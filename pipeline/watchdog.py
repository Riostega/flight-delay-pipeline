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
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.notify import _webhook_url, SLACK_TIMEOUT  # noqa: E402

import requests  # noqa: E402

STATE_FILE = REPO_ROOT / ".watchdog_state.json"
REALERT_HOURS = 6

# Weather runs hourly; flights daily. The thresholds allow one missed run plus
# slack, so a single transient failure that self-heals on retry stays quiet.
WEATHER_STALE_HOURS = 3
FLIGHTS_STALE_HOURS = 30

# The liveness checks are local and free, so they run on every tick. The
# freshness check is not: each query wakes the warehouse for its 60s minimum
# billing period, and at a 30-minute cadence that costs ~24 credits/month
# against a pipeline that consumes ~13. Monitoring should not cost more than
# the thing it monitors. Two hours keeps detection well inside the staleness
# thresholds above while cutting that to ~6.
FRESHNESS_INTERVAL_HOURS = 2


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


def check_scheduler():
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
        return None  # Wrong venv; the other checks still ran.

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
        )
        cur = conn.cursor()

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
        cur.execute("""
            select datediff('hour', max(to_timestamp_ntz(
                       regexp_substr(source_file, '([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)
                       || ' ' ||
                       regexp_substr(source_file, '_([0-9]{6})\\.json', 1, 1, 'e', 1),
                       'YYYY-MM-DD HH24MISS')), sysdate())
            from stg_flights
        """)
        flights_age = cur.fetchone()[0]

        problems = []
        if weather_age is None or weather_age > WEATHER_STALE_HOURS:
            problems.append(f"weather is {weather_age}h stale (expected <{WEATHER_STALE_HOURS}h)")
        if flights_age is None or flights_age > FLIGHTS_STALE_HOURS:
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


def post(text):
    webhook = _webhook_url()
    if not webhook:
        print("watchdog: SLACK_WEBHOOK_URL unset — would have posted:", text)
        return
    try:
        r = requests.post(webhook, json={"text": text}, timeout=SLACK_TIMEOUT)
        if r.status_code != 200:
            print(f"watchdog: Slack returned {r.status_code}")
    except Exception:
        print("watchdog: could not reach Slack")
        traceback.print_exc()


def main():
    airflow_python = os.environ.get("WATCHDOG_AIRFLOW_PYTHON", "/home/ubuntu/airflow-venv/bin/python")
    airflow_home = os.environ.get("AIRFLOW_HOME", "/home/ubuntu/airflow")

    state = _load_state()
    now = datetime.now(timezone.utc)
    host = socket.gethostname()

    checks = {
        "scheduler": check_scheduler(),
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
        if due:
            post(f":warning: *pipeline watchdog* ({host})\n> {name}: {detail}")
            state[name] = now.isoformat()

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

    _save_state(state)

    status = "; ".join(f"{k}: {v}" for k, v in problems.items()) or "all checks passed"
    if not freshness_ran:
        status += " (freshness skipped — not due)"
    print(f"watchdog {now.isoformat()} — {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
