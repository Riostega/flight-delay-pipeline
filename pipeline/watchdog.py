"""Liveness and freshness checks that run outside Airflow.

The on_failure_callback in the DAGs covers exactly one failure shape: a task
that ran and failed. It cannot report the failures where nothing runs at all —
a wedged scheduler, a DAG that stopped being scheduled, an import error that
removes a DAG from the dagbag, or data quietly going stale while every task
reports success. In all of those the pipeline is broken and Slack stays quiet,
which is indistinguishable from healthy.

This runs on a systemd timer independent of Airflow and alerts on those cases.
It deliberately does NOT re-report individual task failures; the callback owns
that, and duplicating it would train the channel to be ignored.

Alerts are throttled through a small state file: a condition that stays broken
re-alerts at most once every REALERT_HOURS, and a recovery is announced once,
retried on every tick until Slack accepts it. Without the throttle, a single
outage would post every time the timer fires.

The watchdog imports two modules it shares with the DAGs, pipeline.notify and
pipeline.thresholds. A bad edit to either removes both DAGs from the dagbag AND
would stop this file from even starting, which is the one failure it most needs
to report. So those imports are guarded: if they fail, a small self-contained
fallback (_import_failure_alarm) posts to Slack and fails the heartbeat instead
of the watchdog dying with nothing but a journal line.
"""

import csv
import json
import os
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

STATE_FILE = REPO_ROOT / ".watchdog_state.json"
REALERT_HOURS = 6

# The airports in scope. The same file the extract reads and dbt loads as a seed,
# read here directly so the per-airport freshness check expects exactly the airports
# the pipeline is supposed to be collecting.
AIRPORTS_FILE = REPO_ROOT / "dbt" / "seeds" / "dim_airports.csv"


def _import_failure_alarm(exc):
    """Raise the alarm when the shared modules won't import. Standard library only.

    Everything else in this file depends on pipeline.notify (Slack, .env reading)
    and pipeline.thresholds, so this cannot use any of it. It reads .env itself,
    posts one fixed message (at most once per REALERT_HOURS, so a broken deploy
    doesn't post every 30 minutes), and pings the heartbeat with /fail so the
    external monitor alerts at once instead of after its grace period. It never
    prints a URL: the webhook and the ping URL are both credentials.
    """
    import urllib.request

    def env(name):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
        try:
            for line in (REPO_ROOT / ".env").read_text().splitlines():
                key, _, val = line.strip().partition("=")
                if key.strip() == name:
                    return val.strip().strip("'\"")
        except OSError:
            pass
        return ""

    message = (
        f"the watchdog cannot import its shared modules ({type(exc).__name__}: {str(exc)[:200]}). "
        "Both DAGs import the same module, so they are probably missing from the dagbag too"
    )
    print(f"watchdog: {message}")

    try:
        state = json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        state = {}
    webhook = env("SLACK_WEBHOOK_URL")
    if webhook and time.time() - state.get("_import_error_alerted_at", 0) > REALERT_HOURS * 3600:
        body = json.dumps({"text": f":rotating_light: *pipeline watchdog* ({socket.gethostname()})\n> {message}"})
        request = urllib.request.Request(
            webhook, data=body.encode(), headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    state["_import_error_alerted_at"] = time.time()
                    STATE_FILE.write_text(json.dumps(state, indent=2))
        except Exception as post_exc:
            print(f"watchdog: could not reach Slack ({type(post_exc).__name__})")

    # Both heartbeats get /fail: a watchdog that cannot run is not alive in any useful sense.
    for name in ("HEARTBEAT_URL", "HEARTBEAT_CHECKS_URL"):
        url = env(name)
        if url:
            try:
                urllib.request.urlopen(url.rstrip("/") + "/fail", timeout=10).close()
            except Exception:
                print(f"watchdog: {name} ping failed")


try:
    from pipeline.notify import (
        _env_value,
        _webhook_url,
        redacted_traceback,
        SLACK_TIMEOUT,
    )
    # Thresholds live in pipeline/thresholds.py so the watchdog and the dashboard
    # cannot drift apart from each other or from the DAG schedules.
    from pipeline.thresholds import (
        FLIGHTS_STALE_HOURS,
        WEATHER_STALE_HOURS,
        WEATHER_STALE_MINUTES,
    )
    import requests
except Exception as _import_exc:
    _import_failure_alarm(_import_exc)
    sys.exit(1)

# The liveness checks are local and free, so they run on every tick. The
# freshness check is not: each query wakes the warehouse for its 60s minimum
# billing period, and at a 30-minute cadence that costs ~24 credits/month
# against a pipeline that consumes ~13. Monitoring should not cost more than
# the thing it monitors. Two hours cuts that to ~6. The price is detection lag:
# a stale condition is noticed up to two hours after it crosses its threshold.
# That is small against the 54-hour flights threshold, but large against the
# 3-hour weather one, so a weather gap of a few hours that recovers on its own
# between two checks is never reported. The per-task failure alerts still are.
FRESHNESS_INTERVAL_HOURS = 2

# Timeout for the heartbeat ping. Short: a heartbeat that hangs would delay the
# checks it is reporting on, and a missed ping is the signal anyway.
HEARTBEAT_TIMEOUT = 10

# The scheduler liveness check reads Airflow's SQLite database, which can be briefly
# locked. One retry after a short pause keeps a momentary lock from looking like a
# dead scheduler and triggering a needless restart.
SCHEDULER_CHECK_ATTEMPTS = 2
SCHEDULER_RETRY_SECONDS = 30

# Automatic restarts (see restart_airflow_once): at most one per outage and none within
# this many hours of the last; never in the window around the 09:00 UTC flights run; and
# never while airflow.service is younger than STARTUP_GRACE_MINUTES.
RESTART_COOLDOWN_HOURS = 3
FLIGHTS_WINDOW_UTC = ("08:50", "09:30")
STARTUP_GRACE_MINUTES = 10


def _load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {}


def _save_state(state):
    # Written to a temp file and swapped in, so a watchdog killed mid-write can't leave
    # half a file behind. _load_state reads a corrupt file as {}, which would forget the
    # restart guards (_last_restart, _restarted_this_outage) along with everything else.
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, STATE_FILE)
    except OSError:
        pass  # A read-only disk is itself a problem, but not one to crash on.


def _signature(detail):
    """A problem's wording with the ages taken out, for spotting a CHANGED problem.

    Freshness details carry ages ("flights are 57h stale"), and those change every
    time the check runs. Comparing raw text would treat each new number as news and
    re-alert every two hours instead of every REALERT_HOURS. Only the ages ("57h",
    "7.5h") are blanked, so everything else that changes still counts as news:
    "restarted automatically" becoming "needs a human", a second airport going
    stale, or a different error code or broken-file count.
    """
    return re.sub(r"\d+(\.\d+)?h\b", "Nh", detail or "")


def check_airflow_service():
    """Airflow's own service state, per systemd."""
    try:
        out = subprocess.run(
            ["systemctl", "is-active", "airflow.service"],
            capture_output=True, text=True, timeout=15,
        ).stdout.strip()
        if out != "active":
            return f"airflow.service is `{out}`, not active"
    except Exception as exc:
        return f"could not query airflow.service: {exc}"
    return None


def minutes_since_airflow_started():
    """How long airflow.service has been running, or None if systemd can't say.

    Both systemd's monotonic timestamp and time.monotonic() count from boot on the
    same clock, so their difference is the service's age.
    """
    try:
        out = subprocess.run(
            ["systemctl", "show", "airflow.service", "-p", "ActiveEnterTimestampMonotonic", "--value"],
            capture_output=True, text=True, timeout=15,
        ).stdout.strip()
        return (time.monotonic() - int(out) / 1_000_000) / 60
    except Exception:
        return None


def check_scheduler_alive(airflow_bin, airflow_home):
    """Is a scheduler actually running and heartbeating, not just the service around it?

    `airflow standalone` runs the scheduler as a child process and does not restart it
    if it dies. On 2026-10-04 the scheduler crashed on a SQLite "database is locked"
    error while its parent kept running, so systemd reported airflow.service as active
    and nothing was scheduled for five and a half hours. check_airflow_service() could
    not see that.

    `airflow jobs check` asks Airflow's own database whether a scheduler on this host
    has heartbeated in the last 30 seconds, which catches a dead scheduler and a hung
    one alike. A failure is retried once, 30 seconds later, so a brief stall doesn't
    count.

    Returns (problem, definitely_dead). definitely_dead is True only when Airflow
    answered "No alive jobs found" both times. Anything else (a timeout, a missing
    binary, a locked database, two schedulers found) means the CHECK was inconclusive,
    not that the scheduler is dead, and must never trigger a restart.
    """
    command = [airflow_bin, "jobs", "check", "--job-type", "SchedulerJob", "--local"]
    env = dict(os.environ, AIRFLOW_HOME=airflow_home)
    answers = []
    for attempt in range(SCHEDULER_CHECK_ATTEMPTS):
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=120, env=env)
            if proc.returncode == 0:
                return None, False
            output = "\n".join(part.strip() for part in (proc.stdout, proc.stderr) if part.strip())
            answers.append(output.splitlines()[-1] if output else f"exit {proc.returncode}")
        except Exception as exc:
            answers.append(f"could not run airflow jobs check: {exc}")
        if attempt < SCHEDULER_CHECK_ATTEMPTS - 1:
            time.sleep(SCHEDULER_RETRY_SECONDS)

    if all("No alive jobs found" in answer for answer in answers):
        return "airflow.service is active but its scheduler is dead", True
    return f"scheduler check inconclusive: {answers[-1][:150]}", False


def in_flights_window(now):
    """True around the 09:00 UTC flights run, when a restart could kill it mid-flight.

    The flights run spends API quota as it goes and has no retries, so killing it
    loses that day's flights for good. A dead scheduler at this hour isn't running the
    flights anyway, and once restarted it still creates the missed run.
    """
    start, end = FLIGHTS_WINDOW_UTC
    return start <= now.strftime("%H:%M") < end


def restart_airflow_once(detail, state, now):
    """Restart Airflow for a dead scheduler, at most once per outage, and say what was done.

    A scheduler that dies again soon after a restart has a problem a restart won't fix,
    and restarting on every tick would hide that behind a loop. So there is one
    automatic restart per outage, and none within RESTART_COOLDOWN_HOURS of the last
    one. After that it is left for a human.

    The restart time is saved to the state file immediately, before restarting. If the
    watchdog itself died partway through this tick, an unsaved stamp would let the next
    tick restart again.
    """
    last = state.get("_last_restart")
    if state.get("_restarted_this_outage"):
        return f"{detail}. An automatic restart was already tried for this outage; needs a human"
    if last:
        try:
            if now - datetime.fromisoformat(last) < timedelta(hours=RESTART_COOLDOWN_HOURS):
                when = last[:16].replace("T", " ")
                return f"{detail}. Airflow was already restarted automatically at {when} UTC; needs a human"
        except ValueError:
            pass
    if in_flights_window(now):
        return f"{detail}. Restart deferred until after the 09:00 UTC flights run"

    state["_last_restart"] = now.isoformat()
    state["_restarted_this_outage"] = True
    _save_state(state)
    try:
        # sudo -n fails instead of prompting. It works because the Ubuntu image gives the
        # ubuntu user password-free sudo; on a host without that, this reports FAILED.
        proc = subprocess.run(
            ["sudo", "-n", "systemctl", "restart", "airflow.service"],
            capture_output=True, text=True, timeout=180,
        )
        error = None if proc.returncode == 0 else (proc.stderr.strip() or f"exit {proc.returncode}")
    except Exception as exc:
        error = str(exc)
    if error:
        return f"{detail}. Automatic restart FAILED: {error[:150]}"
    return f"{detail}. Restarted Airflow automatically"


def check_dag_health(airflow_python, airflow_home):
    """Import errors and missing DAGs — both make runs silently stop happening."""
    script = (
        "import sys; sys.path.insert(0, %r)\n"
        # Airflow 3.3 moved DagBag to airflow.dag_processing.dagbag (the old path
        # still works but is deprecated). The fallback keeps an older Airflow working.
        "try:\n"
        "    from airflow.dag_processing.dagbag import DagBag\n"
        "except ImportError:\n"
        "    from airflow.models.dagbag import DagBag\n"
        "db = DagBag(%r)\n"
        "import json\n"
        # The END of each traceback, not the start. Airflow 3 stores the whole
        # traceback, and its first 200 characters are "Traceback (most recent call
        # last): File "<frozen importlib...", while the actual error (the
        # ImportError, or the SyntaxError and its line) is at the bottom.
        "print(json.dumps({'errors': {k: str(v).strip()[-300:] for k, v in db.import_errors.items()},\n"
        "                  'dags': sorted(db.dags)}))\n"
        % (str(REPO_ROOT), str(REPO_ROOT / "dags"))
    )
    try:
        env = dict(os.environ, AIRFLOW_HOME=airflow_home)
        proc = subprocess.run(
            [airflow_python, "-c", script],
            capture_output=True, text=True, timeout=120, cwd=str(REPO_ROOT), env=env,
        )
        payload = None
        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                payload = json.loads(line)
        if payload is None:
            return f"could not parse the dagbag (exit {proc.returncode})"

        errors = payload["errors"]
        if errors:
            path, error = next(iter(errors.items()))
            # Squashed onto one line so it stays inside the Slack quote block.
            error = " ".join(error.split())
            more = f" (+{len(errors) - 1} more broken file(s))" if len(errors) > 1 else ""
            return f"DAG import error in {Path(path).name}{more}: ...{error}"

        expected = {"flight_pipeline_daily", "weather_hourly"}
        missing = expected - set(payload["dags"])
        if missing:
            return f"DAG(s) missing from the dagbag: {', '.join(sorted(missing))}"
    except Exception as exc:
        return f"dagbag check failed: {exc}"
    return None


SNOWFLAKE_SETTINGS = (
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
)

# When the flights pull happened, read from the S3 key that stg_flights keeps as
# source_file (raw/flights/<date>/<IATA>_<HHMMSS>_<micro>.json). Used by both the
# overall and the per-airport flights queries below.
#
# Anchored on the IATA code, NOT on ".json". The time used to be the last six
# digits before the extension, so '_([0-9]{6})\.json' worked — until microseconds
# were appended to the key to stop two runs in the same second overwriting each
# other. After that the same pattern matched the MICROSECONDS, and
# to_timestamp_ntz raised on '2026-09-09 158798', taking the whole freshness
# check down. Both key shapes put the time immediately after the airport code,
# so that is the stable anchor.
#
# try_to_timestamp_ntz, not to_timestamp_ntz: a filename this code does not
# control must not be able to raise. Anything unparseable — the pre-IATA keys
# still sitting in the raw zone, or the next key-format change — becomes NULL
# and is skipped by max() instead of failing the check that exists to notice
# failures.
FILE_TIME_SQL = """try_to_timestamp_ntz(
    regexp_substr(source_file, '([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)
    || ' ' ||
    regexp_substr(source_file, '[A-Z]{3}_([0-9]{6})', 1, 1, 'e', 1),
    'YYYY-MM-DD HH24MISS')"""

# The airport code in a source_file key. The same pattern stg_weather uses to
# derive iata_code, so the two can't disagree about which airport a file is for.
FILE_AIRPORT_SQL = "regexp_substr(source_file, '/([A-Z]{3})_', 1, 1, 'e', 1)"


def expected_airports():
    """The IATA codes in dim_airports.csv, i.e. the airports that should be delivering."""
    with open(AIRPORTS_FILE) as fh:
        return [row["iata_code"].strip() for row in csv.DictReader(fh)]


def stale_airports(ages, airports, limit, describe):
    """The airports whose newest data is older than limit, or that have none at all.

    Starts from the airports that SHOULD exist, not the ones Snowflake returned. An
    airport that never delivered (a typo'd seed code, a key-format change) has no
    row in the query result, and must count as stale rather than simply not appear.
    """
    stale = []
    for code in airports:
        age = ages.get(code)
        if age is None:
            stale.append(f"{code} (no data)")
        elif age > limit:
            stale.append(f"{code} ({describe(age)})")
    return stale


def check_freshness():
    """Data actually arriving — the check that survives every task passing.

    Asks three questions on one Snowflake connection. The cost is the warehouse
    waking up for its 60-second minimum, not the number of queries, so the extra
    queries are effectively free:

      1. Is weather arriving at all, and is it arriving for EVERY airport?
      2. Are flight files arriving at all, and for EVERY airport?
      3. Are those files actually adding new flights to fct_flight_events?

    The per-airport questions exist because the overall ones take the newest row
    across all airports. One airport could stop delivering for weeks (a bad seed
    code, a persistent API error for that airport) while the other four kept the
    overall number fresh, and the regression would quietly lose a comparison
    airport. The extract deliberately exits 0 when only some airports fail, so the
    four good files still load; this check is what reports the fifth.
    """
    try:
        import snowflake.connector
    except ImportError:
        # NOT None. None is this module's encoding for "checked, and fine", so
        # returning it here reported a check that never ran as a passing one:
        # main() stamped _last_freshness, set freshness_ran, saw no freshness
        # problem, and posted a green "freshness: recovered" to Slack while
        # deleting any outstanding problem from the state file. Every later tick
        # then reported healthy forever, having never once reached Snowflake —
        # silently disabling the single failure mode this watchdog exists for.
        #
        # The watchdog is deployed against a venv that has this dependency, so
        # an ImportError here is a broken deployment, and a broken deployment of
        # the monitor is worth an alert in its own right.
        return "snowflake connector not importable — the watchdog cannot check freshness"

    # Checked before connecting, so a missing or unreadable .env is reported as
    # what it is, rather than as "could not reach Snowflake" (which would send
    # someone off to debug a warehouse that is fine). Only key names are reported.
    settings = {key: _env_value(key) for key in SNOWFLAKE_SETTINGS}
    missing = [key for key, value in settings.items() if not value]
    if missing:
        return f"freshness check not run: {', '.join(missing)} missing from the environment and .env"
    try:
        airports = expected_airports()
    except (OSError, KeyError, ValueError) as exc:
        return f"freshness check not run: could not read {AIRPORTS_FILE.name} ({exc})"

    conn = None
    try:
        connect_args = dict(
            account=settings["SNOWFLAKE_ACCOUNT"], user=settings["SNOWFLAKE_USER"],
            password=settings["SNOWFLAKE_PASSWORD"], warehouse=settings["SNOWFLAKE_WAREHOUSE"],
            database=settings["SNOWFLAKE_DATABASE"], schema=settings["SNOWFLAKE_SCHEMA"],
            login_timeout=60,
            # The watchdog is the last line of defence; it must not be the thing
            # that hangs. login_timeout covers authentication only, so a query
            # against a warehouse that cannot resume would block main() before
            # it ever reached the heartbeat, and the dead-man's switch would
            # fire — correct, but far slower and less specific than failing here.
            network_timeout=120,
        )
        # Optional. Unset, the session uses the user's default role, as it always has.
        if _env_value("SNOWFLAKE_ROLE"):
            connect_args["role"] = _env_value("SNOWFLAKE_ROLE")
        conn = snowflake.connector.connect(**connect_args)
        cur = conn.cursor()
        cur.execute("alter session set statement_timeout_in_seconds = 120")

        def ages_by_airport(sql):
            cur.execute(sql)
            return {code: age for code, age in cur.fetchall() if code}

        # sysdate(), not current_timestamp(). observed_at is TIMESTAMP_NTZ holding
        # UTC, while current_timestamp() returns TIMESTAMP_LTZ in the session's
        # timezone (America/Los_Angeles, Snowflake's default for a new account;
        # nothing here overrides it). Comparing them shifts every age by the UTC
        # offset (7 hours in summer, 8 in winter), making it negative, so the
        # staleness test could never be true and the check would have reported
        # healthy forever. sysdate() is UTC and matches how the data is stored.
        #
        # Minutes, not hours. datediff('hour') counts hour BOUNDARIES crossed, so
        # an observation at 20:00:15 was "0 hours old" until 21:00 and the 3-hour
        # rule really needed three missed runs plus the boundary. Minutes match
        # the dashboard, which reads the same WEATHER_STALE_MINUTES.
        cur.execute("select datediff('minute', max(observed_at), sysdate()) from stg_weather")
        weather_age = cur.fetchone()[0]
        weather_by_airport = ages_by_airport(
            "select iata_code, datediff('minute', max(observed_at), sysdate()) "
            "from stg_weather where iata_code is not null group by iata_code"
        )

        # stg_flights has no load timestamp, so freshness comes from the newest
        # source FILE that contained at least one flight. That measures whether
        # the pipeline is delivering — the flights' own timestamps would only say
        # how recent the *flights* were (question 3 below asks that separately).
        #
        # This reads stg_flights, NOT stg_flights_raw, on purpose. The lateral
        # flatten in stg_flights drops a file whose data array is empty, so a run
        # that lands only empty responses does not reset this clock and still
        # shows up as stale. Pointing this at stg_flights_raw would make an
        # all-empty pipeline look healthy forever.
        cur.execute(f"select datediff('hour', max({FILE_TIME_SQL}), sysdate()) from stg_flights")
        flights_age = cur.fetchone()[0]
        flights_by_airport = ages_by_airport(
            f"select {FILE_AIRPORT_SQL} as code, datediff('hour', max({FILE_TIME_SQL}), sysdate()) "
            f"from stg_flights where code is not null group by code"
        )

        # Files landing is not the same as new flights arriving. A pull that only
        # returns flights already captured (a stalled or cached API snapshot), or
        # only out-of-scope arrivals, still resets the file clock above while
        # fct_flight_events stops growing. This is the dashboard's own measure, so
        # the two can no longer disagree silently. Measured on 2026-10-04, the
        # newest arrival trailed the newest pull by about an hour, well inside the
        # threshold's six hours of slack. (Per airport it trails by up to ~5h, which
        # is why this one is NOT checked per airport.)
        cur.execute("select datediff('hour', max(arrival_actual_utc), sysdate()) from fct_flight_events")
        arrivals_age = cur.fetchone()[0]

        # A NULL age is its own failure and gets its own wording. It means no row
        # produced a usable timestamp at all. Reporting that as "None hours
        # stale" reads like a formatting bug and buries the cause.
        #
        # The per-airport results are only reported while the overall one is
        # fine. When everything is stale, listing all five airports again is
        # noise; the per-airport check exists for the case the overall one hides.
        problems = []
        if weather_age is None:
            problems.append("weather age is unknown — stg_weather has no usable observed_at")
        elif weather_age > WEATHER_STALE_MINUTES:
            problems.append(
                f"weather is {weather_age / 60:.1f}h stale (expected under {WEATHER_STALE_HOURS}h)"
            )
        else:
            stale = stale_airports(weather_by_airport, airports, WEATHER_STALE_MINUTES,
                                   lambda age: f"{age / 60:.1f}h")
            if stale:
                problems.append(
                    f"weather stale for {', '.join(stale)} (expected under {WEATHER_STALE_HOURS}h)"
                )

        if flights_age is None:
            problems.append(
                "flights age is unknown — no stg_flights row has a parseable source_file "
                "(an empty table, e.g. a new account before its first load, or the S3 key format changed)"
            )
        elif flights_age > FLIGHTS_STALE_HOURS:
            problems.append(f"flights are {flights_age}h stale (expected under {FLIGHTS_STALE_HOURS}h)")
        else:
            stale = stale_airports(flights_by_airport, airports, FLIGHTS_STALE_HOURS,
                                   lambda age: f"{age}h")
            if stale:
                problems.append(
                    f"flights stale for {', '.join(stale)} (expected under {FLIGHTS_STALE_HOURS}h)"
                )
            # Only asked while files ARE landing; otherwise it repeats the line above.
            if arrivals_age is None:
                problems.append("fct_flight_events has no arrivals (has dbt_build run on this account?)")
            elif arrivals_age > FLIGHTS_STALE_HOURS:
                problems.append(
                    f"no new flights in fct_flight_events for {arrivals_age}h although files are "
                    "landing (check dbt_build, or whether pulls only return flights already captured)"
                )
        return "; ".join(problems) if problems else None
    except Exception as exc:
        return f"freshness check failed (Snowflake query or connection): {str(exc)[:150]}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _ping(name, url, healthy):
    target = url.rstrip("/") if healthy else url.rstrip("/") + "/fail"
    try:
        requests.get(target, timeout=HEARTBEAT_TIMEOUT)
    except Exception:
        print(f"watchdog: {name} ping failed")


def heartbeat(healthy):
    """Ping an external dead-man's switch.

    Every other check in this file runs on the host it monitors, which means
    none of them can report that host being stopped, unreachable, or shut down
    — the alerts would have to originate from the very machine that is gone,
    and the symptom is silence rather than a message. Silence is
    indistinguishable from healthy, so it needs inverting: an external service
    expects a ping on a schedule and alerts when one fails to arrive.

    Nothing here is specific to a provider. Any service exposing a ping URL
    (healthchecks.io, Better Stack, Cronitor, a self-hosted equivalent) works,
    and the /fail suffix convention is shared by all of them.

    One URL or two:

      HEARTBEAT_URL only         (the original setup) It gets /fail while any
                                 problem is outstanding. The catch: a long,
                                 expected problem (flights stale for days after
                                 the monthly budget runs out) holds the external
                                 check in its "down" state the whole time, so if
                                 the instance then stopped, the service would see
                                 no change and send nothing new.

      + HEARTBEAT_CHECKS_URL     HEARTBEAT_URL becomes pure liveness ("the host
                                 and this timer are running") and is always
                                 pinged plain, so a stopped host is caught however
                                 broken the pipeline already is. The checks'
                                 healthy-or-/fail state goes to the second URL.

    Failures to ping are swallowed: the watchdog's own checks matter more than
    its ability to report to a third party, and a heartbeat outage should not
    take the local checks down with it.
    """
    liveness_url = _env_value("HEARTBEAT_URL")
    checks_url = _env_value("HEARTBEAT_CHECKS_URL")
    if checks_url:
        if liveness_url:
            _ping("HEARTBEAT_URL", liveness_url, healthy=True)
        _ping("HEARTBEAT_CHECKS_URL", checks_url, healthy)
    elif liveness_url:
        _ping("HEARTBEAT_URL", liveness_url, healthy)


def post(text):
    """Send one Slack message. Returns True only if Slack actually accepted it.

    The caller stamps the re-alert throttle from this. Stamping on a merely
    attempted post loses the FIRST alert of an outage whenever Slack is briefly
    unreachable — and that is a correlated failure, because the conditions that
    break the pipeline are the ones most likely to break its network too. The
    problem would then stay silent for the full re-alert window.
    """
    webhook = _webhook_url()
    if not webhook:
        print("watchdog: SLACK_WEBHOOK_URL unset — would have posted:", text)
        return False
    try:
        r = requests.post(webhook, json={"text": text}, timeout=SLACK_TIMEOUT)
        if r.status_code != 200:
            print(f"watchdog: Slack returned {r.status_code}")
            return False
        return True
    except Exception:
        print("watchdog: could not reach Slack")
        print(redacted_traceback(webhook, _env_value("HEARTBEAT_URL"), _env_value("HEARTBEAT_CHECKS_URL")))
        return False


# An unannounced recovery is retried on every tick until Slack takes it, but not
# forever: with the webhook gone for good, they would pile up in the state file.
RECOVERY_RETRY_HOURS = 24


def main():
    airflow_python = os.environ.get("WATCHDOG_AIRFLOW_PYTHON", "/home/ubuntu/airflow-venv/bin/python")
    airflow_home = os.environ.get("AIRFLOW_HOME", "/home/ubuntu/airflow")

    state = _load_state()
    now = datetime.now(timezone.utc)
    host = socket.gethostname()

    # The shared modules imported, so any earlier import-failure alarm is over. Clearing
    # the stamp lets a later breakage alert at once instead of waiting out its throttle.
    state.pop("_import_error_alerted_at", None)

    # The airflow CLI lives next to the venv's python.
    airflow_bin = str(Path(airflow_python).parent / "airflow")

    # A stopped service is only reported, never restarted: someone may have stopped it
    # on purpose, and systemd's own Restart=always already handles a crash of the service.
    # The automatic restart is only for the case systemd can't see, where the service is
    # running but the scheduler inside it is dead.
    #
    # A service that started in the last few minutes is skipped: its scheduler hasn't
    # heartbeated yet, so it would look dead and be restarted for no reason.
    airflow_problem = check_airflow_service()
    age = minutes_since_airflow_started()
    if airflow_problem is None and (age is None or age >= STARTUP_GRACE_MINUTES):
        airflow_problem, definitely_dead = check_scheduler_alive(airflow_bin, airflow_home)
        if definitely_dead:
            airflow_problem = restart_airflow_once(airflow_problem, state, now)
        elif airflow_problem is None:
            # A scheduler confirmed alive ends any outage, so the next one may be
            # restarted again (once the cooldown has passed).
            state.pop("_restarted_this_outage", None)

    checks = {
        "scheduler": airflow_problem,
        "dags": check_dag_health(airflow_python, airflow_home),
    }

    # Only reach for Snowflake when enough time has passed to justify waking the
    # warehouse. On the ticks in between, the LAST freshness result is carried
    # forward and treated exactly like a result from this tick.
    #
    # It used to be dropped instead, so on three ticks out of four an open
    # freshness problem was simply absent. If Slack had refused the alert, nothing
    # was in the state file either, so the heartbeat pinged healthy on those ticks
    # and flapped fail/ok/ok/ok, which most dead-man services read as "resolved".
    # Carrying it forward keeps the heartbeat failing on every tick, and lets a
    # failed Slack post be retried on the next tick like any other check's.
    last_fresh = state.get("_last_freshness")
    due = True
    if last_fresh:
        try:
            due = now - datetime.fromisoformat(last_fresh) > timedelta(hours=FRESHNESS_INTERVAL_HOURS)
        except ValueError:
            due = True
    if due:
        result = check_freshness()
        state["_last_freshness"] = now.isoformat()
        if result:
            state["_freshness_result"] = result
        else:
            state.pop("_freshness_result", None)
        freshness_ran = True
    else:
        freshness_ran = False
    checks["freshness"] = state.get("_freshness_result")

    problems = {k: v for k, v in checks.items() if v}

    for name, detail in problems.items():
        last = state.get(name)
        due = True
        if last:
            try:
                due = now - datetime.fromisoformat(last) > timedelta(hours=REALERT_HOURS)
            except ValueError:
                due = True
        # A problem whose wording changed is news, whatever the throttle says. Without
        # this, "restarted Airflow automatically" followed half an hour later by "needs a
        # human" would stay silent for the whole re-alert window. Compared without the
        # numbers (see _signature), so an age ticking up is not mistaken for news.
        if _signature(state.get(f"_detail:{name}")) != _signature(detail):
            due = True
        if due:
            # Only stamp the throttle when Slack actually took the message, so a
            # failed delivery is retried on the next tick (freshness included,
            # since its result is carried forward) instead of being suppressed
            # for the whole re-alert window.
            if post(f":warning: *pipeline watchdog* ({host})\n> {name}: {detail}"):
                state[name] = now.isoformat()
                state[f"_detail:{name}"] = detail

    for name in list(state):
        # Keys prefixed with "_" are bookkeeping, not conditions.
        if name.startswith("_"):
            continue
        # A freshness check that did not run this tick says nothing about whether it
        # recovered. With the result carried forward this mostly can't happen, but a
        # state file written before that change has an open "freshness" key and no
        # carried result, and announcing recovery there would clear a live problem.
        if name == "freshness" and not freshness_ran:
            continue
        if name not in problems:
            # The condition is cleared at once, whatever Slack does, so the heartbeat
            # and the throttle reflect reality. The announcement is queued separately.
            state.pop(name, None)
            state.pop(f"_detail:{name}", None)
            state[f"_unannounced_recovery:{name}"] = now.isoformat()

    # Announce queued recoveries. Previously the recovery was posted once and the key
    # dropped whatever happened, so a Slack blip at the moment of recovery left the
    # channel's last word on that problem a :warning: indefinitely.
    for key in [k for k in state if k.startswith("_unannounced_recovery:")]:
        name = key.split(":", 1)[1]
        if name in problems or name in state:
            # It broke again before the recovery got out; the warning path owns it now.
            state.pop(key, None)
            continue
        try:
            expired = now - datetime.fromisoformat(state[key]) > timedelta(hours=RECOVERY_RETRY_HOURS)
        except ValueError:
            expired = True
        if expired or post(f":white_check_mark: *pipeline watchdog* ({host})\n> {name}: recovered"):
            state.pop(key, None)

    _save_state(state)

    # Ping last, so the heartbeat reflects the checks that just ran. A failing
    # ping tells the external service to alert immediately rather than waiting
    # for the grace period to lapse.
    # Outstanding problems in the state file count too, not just this tick's: a
    # problem whose alert Slack accepted stays open until a check says otherwise.
    outstanding = {k for k in state if not k.startswith("_")}
    heartbeat(healthy=not problems and not outstanding)

    # The journal line says the same thing the heartbeat just did. It used to print
    # "all checks passed (freshness skipped)" on ticks between freshness checks while
    # a freshness problem was still open, which read as resolved in journalctl.
    parts = []
    for name, detail in problems.items():
        carried = name == "freshness" and not freshness_ran
        parts.append(f"{name}: {detail}" + (" (carried from the last check)" if carried else ""))
    for name in sorted(outstanding - set(problems)):
        parts.append(f"{name}: still open (last alerted {state[name][:16]}), not re-checked this tick")
    status = "; ".join(parts) or "all checks passed"
    if not freshness_ran and "freshness" not in problems and "freshness" not in outstanding:
        status += " (freshness not due; its last check was clean)"
    print(f"watchdog {now.isoformat()} — {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
