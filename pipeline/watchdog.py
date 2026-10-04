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
re-alerts at most once every REALERT_HOURS, and a recovery is announced once.
Without that, a single outage would post every time the timer fires.
"""

import json
import os
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.notify import (  # noqa: E402
    _env_value,
    _webhook_url,
    redacted_traceback,
    SLACK_TIMEOUT,
)

import requests  # noqa: E402

STATE_FILE = REPO_ROOT / ".watchdog_state.json"
REALERT_HOURS = 6

# Thresholds live in pipeline/thresholds.py so the watchdog and the dashboard
# cannot drift apart from each other or from the DAG schedules.
from pipeline.thresholds import WEATHER_STALE_HOURS, FLIGHTS_STALE_HOURS  # noqa: E402

# The liveness checks are local and free, so they run on every tick. The
# freshness check is not: each query wakes the warehouse for its 60s minimum
# billing period, and at a 30-minute cadence that costs ~24 credits/month
# against a pipeline that consumes ~13. Monitoring should not cost more than
# the thing it monitors. Two hours keeps detection well inside the staleness
# thresholds above while cutting that to ~6.
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
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except OSError:
        pass  # A read-only disk is itself a problem, but not one to crash on.


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
        "from airflow.models.dagbag import DagBag\n"
        "db = DagBag(%r)\n"
        "import json\n"
        "print(json.dumps({'errors': {k: str(v)[:200] for k, v in db.import_errors.items()},\n"
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

        if payload["errors"]:
            first = next(iter(payload["errors"].items()))
            return f"DAG import error in {Path(first[0]).name}: {first[1]}"

        expected = {"flight_pipeline_daily", "weather_hourly"}
        missing = expected - set(payload["dags"])
        if missing:
            return f"DAG(s) missing from the dagbag: {', '.join(sorted(missing))}"
    except Exception as exc:
        return f"dagbag check failed: {exc}"
    return None


def check_freshness():
    """Data actually arriving — the check that survives every task passing."""
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

    def env(key):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
        for line in (REPO_ROOT / ".env").read_text().splitlines():
            k, _, v = line.strip().partition("=")
            if k.strip() == key:
                return v.strip().strip("'\"")
        return ""

    conn = None
    try:
        conn = snowflake.connector.connect(
            account=env("SNOWFLAKE_ACCOUNT"), user=env("SNOWFLAKE_USER"),
            password=env("SNOWFLAKE_PASSWORD"), warehouse=env("SNOWFLAKE_WAREHOUSE"),
            database=env("SNOWFLAKE_DATABASE"), schema=env("SNOWFLAKE_SCHEMA"),
            login_timeout=60,
            # The watchdog is the last line of defence; it must not be the thing
            # that hangs. login_timeout covers authentication only, so a query
            # against a warehouse that cannot resume would block main() before
            # it ever reached the heartbeat, and the dead-man's switch would
            # fire — correct, but far slower and less specific than failing here.
            network_timeout=120,
        )
        cur = conn.cursor()
        cur.execute("alter session set statement_timeout_in_seconds = 120")

        # sysdate(), not current_timestamp(). observed_at is TIMESTAMP_NTZ holding
        # UTC, while current_timestamp() returns TIMESTAMP_LTZ in the session's
        # timezone (US/Central here). Comparing them subtracts a 6-hour offset,
        # which made every age six hours too low — negative, in fact, so the
        # staleness test could never be true and the check would have reported
        # healthy forever. sysdate() is UTC and matches how the data is stored.
        cur.execute("select datediff('hour', max(observed_at), sysdate()) from stg_weather")
        weather_age = cur.fetchone()[0]

        # stg_flights has no load timestamp, so freshness comes from when the
        # newest source file landed. That measures whether the pipeline is
        # delivering, which is the question here — the flights' own timestamps
        # would only say how recent the *flights* were.
        #
        # Anchored on the IATA code, NOT on ".json". The time used to be the last
        # six digits before the extension, so '_([0-9]{6})\.json' worked — until
        # microseconds were appended to the key to stop two runs in the same
        # second overwriting each other. After that the same pattern matched the
        # MICROSECONDS, and to_timestamp_ntz raised on '2026-09-09 158798',
        # taking the whole freshness check down. Both key shapes put the time
        # immediately after the airport code, so that is the stable anchor.
        #
        # try_to_timestamp_ntz, not to_timestamp_ntz: a filename this code does
        # not control must not be able to raise. Anything unparseable — the
        # pre-IATA keys still sitting in the raw zone, or the next key-format
        # change — becomes NULL and is skipped by max() instead of failing the
        # check that exists to notice failures.
        cur.execute("""
            select datediff('hour', max(try_to_timestamp_ntz(
                       regexp_substr(source_file, '([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)
                       || ' ' ||
                       regexp_substr(source_file, '[A-Z]{3}_([0-9]{6})', 1, 1, 'e', 1),
                       'YYYY-MM-DD HH24MISS')), sysdate())
            from stg_flights
        """)
        flights_age = cur.fetchone()[0]

        # A NULL age is its own failure and gets its own wording. It means no row
        # produced a usable timestamp at all — an empty table, or a source_file
        # shape the parser above no longer recognises. Reporting that as
        # "None hours stale" reads like a formatting bug and buries the cause.
        problems = []
        if weather_age is None:
            problems.append("weather age is unknown — stg_weather has no usable observed_at")
        elif weather_age > WEATHER_STALE_HOURS:
            problems.append(f"weather is {weather_age}h stale (expected <{WEATHER_STALE_HOURS}h)")

        if flights_age is None:
            problems.append(
                "flights age is unknown — no source_file parsed to a timestamp, "
                "which usually means the S3 key format changed"
            )
        elif flights_age > FLIGHTS_STALE_HOURS:
            problems.append(f"flights are {flights_age}h stale (expected <{FLIGHTS_STALE_HOURS}h)")
        return "; ".join(problems) if problems else None
    except Exception as exc:
        return f"freshness check could not reach Snowflake: {str(exc)[:150]}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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

    Failures to ping are swallowed: the watchdog's own checks matter more than
    its ability to report to a third party, and a heartbeat outage should not
    take the local checks down with it.
    """
    base = _env_value("HEARTBEAT_URL")
    if not base:
        return
    url = base.rstrip("/") if healthy else base.rstrip("/") + "/fail"
    try:
        requests.get(url, timeout=HEARTBEAT_TIMEOUT)
    except Exception:
        print("watchdog: heartbeat ping failed")


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
        print(redacted_traceback(webhook, _env_value("HEARTBEAT_URL")))
        return False


def main():
    airflow_python = os.environ.get("WATCHDOG_AIRFLOW_PYTHON", "/home/ubuntu/airflow-venv/bin/python")
    airflow_home = os.environ.get("AIRFLOW_HOME", "/home/ubuntu/airflow")

    state = _load_state()
    now = datetime.now(timezone.utc)
    host = socket.gethostname()

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
    # warehouse. When it is skipped, any outstanding freshness problem is left
    # in the state file untouched rather than being treated as recovered.
    last_fresh = state.get("_last_freshness")
    due = True
    if last_fresh:
        try:
            due = now - datetime.fromisoformat(last_fresh) > timedelta(hours=FRESHNESS_INTERVAL_HOURS)
        except ValueError:
            due = True
    if due:
        checks["freshness"] = check_freshness()
        state["_last_freshness"] = now.isoformat()
        freshness_ran = True
    else:
        freshness_ran = False

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
        # human" would stay silent for the whole re-alert window.
        if state.get(f"_detail:{name}") != detail:
            due = True
        if due:
            # Only stamp the throttle when Slack actually took the message,
            # so a failed delivery is retried on the next tick instead of
            # being suppressed for the whole re-alert window.
            if post(f":warning: *pipeline watchdog* ({host})\n> {name}: {detail}"):
                state[name] = now.isoformat()
                state[f"_detail:{name}"] = detail

    for name in list(state):
        # Keys prefixed with "_" are bookkeeping, not conditions. And a check
        # that did not run this tick says nothing about whether it recovered —
        # announcing recovery there would clear a problem still outstanding.
        if name.startswith("_"):
            continue
        if name == "freshness" and not freshness_ran:
            continue
        if name not in problems:
            post(f":white_check_mark: *pipeline watchdog* ({host})\n> {name}: recovered")
            state.pop(name, None)
            state.pop(f"_detail:{name}", None)

    _save_state(state)

    # Ping last, so the heartbeat reflects the checks that just ran. A failing
    # ping tells the external service to alert immediately rather than waiting
    # for the grace period to lapse.
    # Outstanding problems in the state file count, not just the ones detected on
    # THIS tick. Freshness is only checked every couple of hours, so on a skipped
    # tick it is absent from `problems` even while still broken — pinging healthy
    # there resolved the external incident and the dead-man's switch went green
    # with the problem still live.
    outstanding = {k for k in state if not k.startswith("_")}
    heartbeat(healthy=not problems and not outstanding)

    status = "; ".join(f"{k}: {v}" for k, v in problems.items()) or "all checks passed"
    if not freshness_ran:
        status += " (freshness skipped — not due)"
    print(f"watchdog {now.isoformat()} — {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
