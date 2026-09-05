"""Daily flight pipeline: extract -> load -> transform -> test.

Runs the full ELT chain once a day. The ordering is the reason this is an
Airflow DAG rather than four cron entries: dbt must not run if the Snowflake
load failed, or it would silently model stale data and report success.
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
from pipeline.notify import slack_alert

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
    "on_failure_callback": slack_alert,
    # No retries on this DAG. A retry spends another five requests out of a
    # hundred-per-month budget, and buys almost nothing: the endpoint returns
    # whatever landed recently, so a retry five minutes later fetches nearly
    # the same rows the failed attempt would have. The next scheduled run
    # recovers the gap at no extra cost. Retrying here trades real quota for
    # duplicate data.
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_pipeline_daily",
    description="Extract landed flights, load to Snowflake, rebuild and test dbt models",
    start_date=datetime(2026, 9, 1),
    # Every other day, not daily. AviationStack's free tier allows 100 requests
    # a MONTH, and one run spends five (one per airport). Daily would need 150
    # and would exhaust the quota around the 20th, leaving ten days of failing
    # runs. Every other day fits in 75 with margin for a manual run.
    #
    # The data cost is smaller than it looks: the endpoint returns a live
    # snapshot of recent arrivals, so consecutive pulls overlap heavily. At a
    # 24-hour gap only ~36% of returned flights were new. Wider spacing raises
    # novelty per request, so half the runs collect far more than half the data.
    schedule="0 9 */2 * *",
    # AviationStack's free tier is a live snapshot with no historical endpoint,
    # so a missed interval cannot be recovered by replaying it — re-running a
    # missed 3am task at 9am fetches 9am data. Backfilling would write wrong
    # rows rather than recovering absent ones.
    catchup=False,
    # Prevent a slow run and the next scheduled run from overlapping and
    # double-loading the same files.
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
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # build, not run-then-test. build tests each model as it is constructed
        # and stops there, so a staging model that fails its tests never becomes
        # the input to the fact table. Running everything first and testing
        # afterwards leaves a corrupted mart in place while reporting the
        # failure, which is the worse of the two outcomes.
        bash_command=f"cd {DBT_DIR} && {DBT} build",
    )

    extract_flights >> load_to_snowflake >> dbt_build
