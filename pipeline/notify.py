"""Failure alerting for the pipeline DAGs.

Wired in as an `on_failure_callback`, so Airflow calls it whenever a task
finishes in a failed state — including a task that failed because
extract_pipeline.py exited non-zero on an exhausted API quota.

Three rules govern everything here, and all three exist because this code runs
at the exact moment something is already broken:

1. It must never raise. An exception inside an on_failure_callback is logged
   against the callback, not the task, so a bug here would bury the original
   failure underneath a second, unrelated stack trace — the alerting turning
   into the thing that hides the outage.
2. It must never block. The scheduler waits on this callback, so a Slack
   outage without a timeout would wedge the run rather than just going unheard.
3. It must never print the webhook URL. Anyone holding that URL can post into
   the channel, so it is a credential and belongs in .env with the others.
"""

import os
import traceback
from pathlib import Path

import requests

# Short enough that a hung Slack costs seconds, not the run.
SLACK_TIMEOUT = 10

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


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

        # The exception can be long (a full dbt failure summary). Slack will
        # accept it, but a phone notification shows only the first line, so the
        # useful detail goes first and the rest is truncated deliberately.
        reason = str(exception).strip().splitlines()[0][:300] if exception else "no exception recorded"

        text = (
            f":rotating_light: *{dag_id}* — task `{task_id}` failed\n"
            f"> attempt {try_number} · run {logical_date}\n"
            f"> {reason}"
        )

        response = requests.post(webhook, json={"text": text}, timeout=SLACK_TIMEOUT)
        if response.status_code != 200:
            # Deliberately does not include the URL in the log line.
            print(f"notify: Slack returned {response.status_code} {response.text[:120]}")
        else:
            print(f"notify: alerted Slack for {dag_id}.{task_id}")

    except Exception:
        # Rule 1. Swallow everything, but leave a trace in the task log so a
        # broken alerter is discoverable rather than merely quiet.
        print("notify: alerting failed, original task failure stands")
        traceback.print_exc()
