"""Every-other-day flight pipeline: extract -> load -> transform -> test.

Runs the full ELT chain on odd-numbered days of the month at 09:00 UTC. (The
dag_id still says "daily", from before the schedule changed; renaming it would
orphan the run history.) The ordering is the reason this is an Airflow DAG
rather than four cron entries: dbt must not run if the Snowflake load failed,
or it would silently model stale data and report success.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Derived from this file's location so the same DAG works on the laptop and on
# the EC2 host without edits.
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = f"{PROJECT_DIR}/dbt"

# The tasks shell out to a separate interpreter, but the failure callback runs
# inside Airflow's own process, so this one module has to be importable here.
# It depends only on os/requests, both of which Airflow already provides.
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)
from pipeline.notify import slack_alert, slack_recovered

# Airflow runs in its own virtualenv, which deliberately does not have this
# project's dependencies (Airflow and dbt pin conflicting versions of jinja2,
# pydantic and requests). Tasks therefore shell out to a separate interpreter
# rather than importing the pipeline. The service definition overrides these
# per host; the defaults are the macOS paths.
PYTHON = os.environ.get("PIPELINE_PYTHON", "/usr/local/bin/python3")
DBT = os.environ.get("PIPELINE_DBT", "/Library/Frameworks/Python.framework/Versions/3.12/bin/dbt")

default_args = {
    # Attached to default_args rather than to one task, so every task in
    # the DAG alerts — a failed load matters as much as a failed extract.
    # Repeats are throttled inside slack_alert, and slack_recovered posts one
    # line when a task that alerted succeeds again.
    "on_failure_callback": slack_alert,
    "on_success_callback": slack_recovered,

    # Without this a task that HANGS rather than fails blocks every later run
    # forever: max_active_runs=1 means the stuck run holds the only slot, and
    # nothing else bounds it. A hang is also invisible to the failure callback,
    # which only fires on a task that actually finishes badly.
    # dbt build over a cold warehouse is the slow step; 45 minutes is far above
    # any observed run (the longest was under 6) and far below the 24-48 hour
    # gap to the next scheduled run.
    "execution_timeout": timedelta(minutes=45),

    # No retries by default, because extract_flights spends quota: five
    # requests per attempt out of a 90-request monthly budget
    # (MONTHLY_REQUEST_BUDGET in extract_pipeline.py, kept under AviationStack's
    # 100). A retry minutes later would fetch nearly the same rows, so it trades
    # real quota for duplicate data. The tasks that spend no quota opt back in
    # below. (No retry_delay here: it does nothing while retries is 0.)
    "retries": 0,
}

with DAG(
    dag_id="flight_pipeline_daily",
    description="Extract landed flights, load to Snowflake, rebuild and test dbt models",
    start_date=datetime(2026, 9, 1),
    # Every other day, not daily. AviationStack's free tier allows 100 requests
    # a MONTH, and one run spends five (one per airport). Daily would need 150
    # and would exhaust the quota around the 20th, leaving ten days of failing
    # runs.
    #
    # "*/2" means odd days of the month: 14-16 runs, 70-80 requests, against
    # the 90-request budget. A 31-day month (October, for one) fires on the
    # 31st and again on the 1st, a 24-hour back-to-back pair, and leaves only
    # 10 spare requests: two manual runs. Every manual run beyond that makes
    # the budget guard refuse a scheduled run at the end of the month, which
    # is how Sep 26-29 came to have no flights.
    #
    # Skipping days costs data in proportion. Each pull returns arrivals from
    # well under the past day, so pulls a day or more apart never overlap:
    # every scheduled pull so far has been 100% new flights, and daily runs
    # would collect about twice as much. (An earlier note here said only ~36%
    # were new at a 24-hour gap; that compared manual pulls a few hours apart.)
    # The schedule is set purely by the quota.
    schedule="0 9 */2 * *",
    # AviationStack's free tier is a live snapshot with no historical endpoint,
    # so a missed interval cannot be recovered by replaying it — re-running a
    # missed 09:00 run hours later fetches that later snapshot. Backfilling
    # would write wrong rows rather than recovering absent ones.
    catchup=False,
    # One run at a time. Scheduled runs are a day or two apart and finish in
    # minutes, so what this really stops is a manual trigger (or a hung run
    # still inside its execution_timeout) overlapping another run, which would
    # spend five more requests and run two dbt builds at once.
    #
    # It does NOT stop concurrent loads. weather_hourly's load_weather runs the
    # same snowflake_load.sql, COPY INTO stg_flights_raw included, every hour,
    # so at 09:00 it runs alongside load_to_snowflake below. Snowflake's
    # per-table load history is what stops a file being loaded twice, so any
    # step added after the load must be safe to run twice or concurrently too.
    max_active_runs=1,
    default_args=default_args,
    tags=["flights", "elt"],
) as dag:

    extract_flights = BashOperator(
        task_id="extract_flights",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipeline/extract_pipeline.py flights",
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        # snowflake_load.sql, not snowflake_setup.sql: the daily job copies new
        # files and nothing more. Recreating stages every day would be wasteful,
        # and it would require AWS credentials on the host — the load path
        # deliberately needs none, so the EC2 box carries no AWS keys.
        #
        # Idempotent: COPY INTO tracks load history per table, so this loads
        # only files landed since the last run.
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipeline/run_snowflake_setup.py snowflake_load.sql",
        # Retries, unlike the DAG default: a COPY spends no API quota and is
        # safe to repeat, and without a retry a brief Snowflake blip at 09:00
        # left the mart unrebuilt until the next run, one or two days later.
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # build, not run-then-test. build tests each model as it is constructed
        # and stops there, so a staging model that fails its tests never becomes
        # the input to the fact table. Running everything first and testing
        # afterwards leaves a corrupted mart in place while reporting the
        # failure, which is the worse of the two outcomes.
        bash_command=f"cd {DBT_DIR} && {DBT} build",
        # One retry for a transient Snowflake fault. Only one, because a failing
        # dbt TEST is deterministic: it fails the same way on every attempt, so
        # more retries would only delay the alert and repeat the build's cost.
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    extract_flights >> load_to_snowflake >> dbt_build
