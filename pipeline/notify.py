"""Failure alerting for the pipeline DAGs.

Wired in as an `on_failure_callback`, so Airflow calls it whenever a task
finishes in a failed state — including the flights extract being refused by
its own monthly budget guard, which exits with a distinct code so the alert
can say it is expected (see EXIT_HINTS).

Repeat failures of the same task are throttled: the first failure posts, then
the same failure at most once every REALERT_HOURS, with a count of the ones in
between. An hourly DAG would otherwise turn one Snowflake outage (or the trial
expiring) into 24 identical alerts a day, burying anything else in the noise.
slack_recovered, wired in as `on_success_callback`, posts one line when a task
that had alerted succeeds again.

Three rules govern everything here, and all three exist because this code runs
at the exact moment something is already broken:

1. It must never raise. An exception inside an on_failure_callback is logged
   against the callback, not the task, so a bug here would bury the original
   failure underneath a second, unrelated stack trace — the alerting turning
   into the thing that hides the outage.
2. It must never block. In Airflow 3 the callback runs in the failing task's
   own process, so a Slack outage without a timeout would leave that process
   hanging instead of letting the task finish failing.
3. It must never print the webhook URL. Anyone holding that URL can post into
   the channel, so it is a credential and belongs in .env with the others.
"""

import json
import os
import re
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# Short enough that a hung Slack costs seconds, not the run.
SLACK_TIMEOUT = 10

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"

# One small JSON file per (dag, task) that has alerted, so the throttle survives
# between task runs. One file per task rather than one shared file, because the
# two DAGs' callbacks can fire at the same moment (both load at 09:00) and would
# overwrite each other's updates to a shared file.
ALERT_STATE_DIR = REPO_ROOT / ".alert_state"
REALERT_HOURS = 6

# What an exit code means, for the tasks that use them (extract_pipeline.py
# defines the codes). Airflow's BashOperator reports every failure as the same
# sentence, "Bash command failed. The command returned a non-zero exit code N.",
# so without this an expected budget refusal and a revoked API key look identical
# in Slack.
EXIT_HINTS = {
    ("extract_flights", 3): "monthly AviationStack budget used up. Expected; nothing to do until the 1st (UTC)",
    ("extract_flights", 4): "budget guard could not list S3, so the run was refused. No requests spent",
    ("extract_flights", 5): "no flights collected from any airport. Check the AviationStack key and quota",
    ("extract_weather", 5): "no weather collected from any airport. Check the OpenWeatherMap key",
}


def redacted_traceback(*secrets):
    """The current traceback with credentials scrubbed out.

    traceback.print_exc() is not safe to call after a failed HTTP request to a
    secret URL. requests embeds the full URL in its exception message —
    "Max retries exceeded with url: /services/T.../B.../token" — so printing the
    traceback writes the webhook straight into the Airflow task log, which is
    exactly what this module promises never to do.

    Scrubs the known secret values, then falls back to pattern-matching the
    provider URL shapes so a credential this function was not handed still does
    not survive. The traceback is worth keeping: without it a broken alerter is
    invisible, and the frames are the diagnostic value.
    """
    text = traceback.format_exc()

    for secret in secrets:
        if not secret:
            continue
        text = text.replace(secret, "<redacted>")
        # requests does not report the full URL. It reports the host and the
        # path in separate parts of one message — "host=\'hooks.slack.com\'"
        # then "with url: /services/T.../B.../token" — so replacing the whole
        # URL matches nothing and the credential survives. Scrub the path too.
        path = urllib.parse.urlsplit(secret).path
        if len(path) > 1:
            text = text.replace(path, "/<redacted>")

    # Belt and braces: scrub the provider-shaped paths even when the caller did
    # not hand us the value, so a future secret is not leaked by omission.
    text = re.sub(r"/services/[A-Za-z0-9_\-/]+", "/services/<redacted>", text)
    text = re.sub(r"/ping/[A-Za-z0-9_\-]+", "/ping/<redacted>", text)
    text = re.sub(r"hc-ping\.com/\S+", "hc-ping.com/<redacted>", text)
    return text


def _env_value(name):
    """Read one key from the environment, falling back to .env.

    The callback runs inside Airflow's own process, which systemd starts with
    only four Environment= lines — it never sources .env. Rather than add an
    EnvironmentFile (systemd's parser has its own quoting rules, and feeding it
    a file full of unrelated secrets to obtain one value is a poor trade), this
    reads the single key it needs. .env stays the one place credentials live,
    which is the same rule the extract and load scripts follow.
    """
    from_env = (os.getenv(name) or "").strip()
    if from_env:
        return from_env

    try:
        with ENV_FILE.open() as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                if key.strip() == name:
                    return value.strip().strip("\'\"")
    except OSError:
        # No .env on this host is a normal state for a fresh clone.
        pass
    return ""


def _webhook_url():
    return _env_value("SLACK_WEBHOOK_URL")


def _field(context, key, default="unknown"):
    value = context.get(key)
    return value if value is not None else default


def _state_path(dag_id, task_id):
    return ALERT_STATE_DIR / f"{dag_id}.{task_id}.json"


def _read_state(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _write_state(path, state):
    """Write via a temp file and a rename, so a crash can't leave half a file."""
    if not ALERT_STATE_DIR.exists():
        ALERT_STATE_DIR.mkdir()
        # A .gitignore inside the directory ignoring everything, itself included,
        # keeps this local state out of `git status` without touching the repo's.
        (ALERT_STATE_DIR / ".gitignore").write_text("*\n")
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    os.replace(tmp, path)


def _is_repeat(state, reason, now):
    """True when this exact failure already alerted within the last REALERT_HOURS.

    A different reason (a different exit code, say) is news and always posts.
    """
    try:
        last = datetime.fromisoformat(state["last_posted_at"])
    except (KeyError, TypeError, ValueError):
        return False
    return state.get("reason") == reason and now - last < timedelta(hours=REALERT_HOURS)


def slack_alert(context):
    """Post a failure summary to Slack. Silent no-op when unconfigured."""
    webhook = _webhook_url()
    if not webhook:
        # Not an error: the repo ships without a webhook, and a fresh clone
        # should run without one rather than failing on a missing secret.
        print("notify: SLACK_WEBHOOK_URL unset — skipping alert")
        return

    try:
        ti = _field(context, "task_instance", None)
        dag_run = _field(context, "dag_run", None)
        exception = context.get("exception")

        dag_id = getattr(ti, "dag_id", "unknown-dag")
        task_id = getattr(ti, "task_id", "unknown-task")
        try_number = getattr(ti, "try_number", "?")
        logical_date = getattr(dag_run, "logical_date", None)

        # For these BashOperator tasks the exception is always Airflow's one fixed
        # sentence, "Bash command failed ... exit code N". The real detail (the
        # dbt failure summary, the API error) is in the task log, not here. So the
        # alert carries a hint mapped from the exit code, and the log link.
        # splitlines() on an empty or whitespace-only string returns [], so
        # indexing [0] raised IndexError for `raise SomeError()` with no message.
        # That exception was then swallowed by the outer handler and the alert
        # was dropped entirely — a task failed and nothing reached Slack.
        _lines = str(exception).strip().splitlines() if exception else []
        reason = _lines[0][:300] if _lines else (
            type(exception).__name__ if exception else "no exception recorded"
        )
        code = re.search(r"exit code (\d+)", reason)
        hint = EXIT_HINTS.get((task_id, int(code.group(1)))) if code else None

        # The throttle. Any problem reading or writing its state must fall through
        # to posting: a broken throttle must never be what hides a failure.
        now = datetime.now(timezone.utc)
        path = _state_path(dag_id, task_id)
        state = {}
        try:
            state = _read_state(path)
            if _is_repeat(state, reason, now):
                state["suppressed"] = state.get("suppressed", 0) + 1
                _write_state(path, state)
                print(f"notify: same failure already alerted within {REALERT_HOURS}h "
                      f"for {dag_id}.{task_id}; not posting again")
                return
        except Exception:
            print("notify: alert throttle unavailable, posting anyway")

        text = (
            f":rotating_light: *{dag_id}* — task `{task_id}` failed\n"
            f"> attempt {try_number} · run {logical_date}\n"
            f"> {reason}"
        )
        if hint:
            text += f"\n> {hint}"
        suppressed = state.get("suppressed", 0)
        if suppressed:
            text += (f"\n> {suppressed} more failure(s) of this task since the last alert "
                     f"(repeats post at most every {REALERT_HOURS}h)")
        try:
            # Points at the Airflow UI, which is only reachable through the SSH tunnel.
            log_url = getattr(ti, "log_url", None)
            if log_url:
                text += f"\n> log (via the SSH tunnel): {log_url}"
        except Exception:
            pass

        response = requests.post(webhook, json={"text": text}, timeout=SLACK_TIMEOUT)
        if response.status_code != 200:
            # Deliberately does not include the URL in the log line.
            print(f"notify: Slack returned {response.status_code} {response.text[:120]}")
        else:
            print(f"notify: alerted Slack for {dag_id}.{task_id}")
            # Stamped only once Slack accepted it. Stamping a failed post would
            # throttle away the first alert of an outage, the same rule the
            # watchdog follows.
            try:
                _write_state(path, {"last_posted_at": now.isoformat(), "reason": reason, "suppressed": 0})
            except Exception:
                print("notify: could not save the alert throttle state")

    except Exception:
        # Rule 1. Swallow everything, but leave a trace in the task log so a
        # broken alerter is discoverable rather than merely quiet.
        print("notify: alerting failed, original task failure stands")
        print(redacted_traceback(webhook))


def slack_recovered(context):
    """on_success_callback: say so once when a task that had alerted succeeds again.

    Does nothing for a task with no alert on record, which is nearly every call.
    The state is cleared before posting, so a Slack failure here loses only the
    "recovered" line, and the next failure of this task alerts straight away
    instead of being throttled against the old one. Same three rules as above.
    """
    try:
        ti = _field(context, "task_instance", None)
        dag_id = getattr(ti, "dag_id", "unknown-dag")
        task_id = getattr(ti, "task_id", "unknown-task")
        path = _state_path(dag_id, task_id)
        if not path.exists():
            return
        suppressed = _read_state(path).get("suppressed", 0)
        path.unlink()

        webhook = _webhook_url()
        if not webhook:
            return
        text = f":white_check_mark: *{dag_id}* — task `{task_id}` succeeded again"
        if suppressed:
            text += f" ({suppressed} more failure(s) after the last alert were not posted)"
        response = requests.post(webhook, json={"text": text}, timeout=SLACK_TIMEOUT)
        if response.status_code != 200:
            print(f"notify: Slack returned {response.status_code} for the recovery message")
    except Exception:
        print("notify: recovery message failed")
        print(redacted_traceback(_webhook_url()))
