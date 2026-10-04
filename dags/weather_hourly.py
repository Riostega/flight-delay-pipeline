"""Hourly weather collection.

Separate from the flight DAG because the two APIs have quotas differing by
orders of magnitude. AviationStack's 100 requests a month allow a flights pull
only every other day; OpenWeatherMap allows far more, and dense weather observations are what make the eventual
flight/weather join meaningful rather than matching every flight to a single
coarse daily reading.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = os.environ.get("PIPELINE_PYTHON", "/usr/local/bin/python3")

# The tasks shell out to a separate interpreter, but the failure callback runs
# inside Airflow's own process, so this one module has to be importable here.
# It depends only on os/requests, both of which Airflow already provides.
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)
from pipeline.notify import slack_alert, slack_recovered

with DAG(
    dag_id="weather_hourly",
    description="Fetch current conditions for each airport in dim_airports",
    start_date=datetime(2026, 9, 1),
    schedule="0 * * * *",
    # A missed weather observation is gone permanently: the free endpoint
    # returns current conditions only, with no history to query.
    catchup=False,
    max_active_runs=1,
    default_args={
        # Attached to default_args rather than to one task, so every task in
        # the DAG alerts — a failed load matters as much as a failed extract.
        # slack_alert throttles repeats: an hourly DAG failing all night (say,
        # Snowflake down or the trial expired) posts once, then at most every
        # six hours, instead of 24 times a day. slack_recovered posts one line
        # when a task that alerted succeeds again.
        "on_failure_callback": slack_alert,
        "on_success_callback": slack_recovered,

        # Without this a task that HANGS rather than fails blocks every later run
        # forever: max_active_runs=1 means the stuck run holds the only slot, and
        # nothing else bounds it. A hang is also invisible to the failure callback,
        # which only fires on a task that actually finishes badly.
        # Runs hourly, so anything still going after 15 minutes has hung rather than
        # slowed, and would block the next run under max_active_runs=1.
        "execution_timeout": timedelta(minutes=15),

        # Weather calls are cheap against the quota, so retry more freely than
        # the flights DAG does.
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["weather"],
) as dag:

    extract_weather = BashOperator(
        task_id="extract_weather",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipeline/extract_pipeline.py weather",
    )

    load_weather = BashOperator(
        task_id="load_weather",
        # Loading hourly as well as collecting hourly. Landing in S3 every hour
        # but only loading once a day left Snowflake permanently hours behind
        # the bucket, which made the dashboard's freshness check report an
        # outage every day between loads — an alarm that fires daily during
        # healthy operation is worse than none, because it trains you to ignore
        # it. COPY INTO skips files it has already seen, so the extra runs cost
        # one brief warehouse wake-up.
        #
        # Despite the name, this loads FLIGHTS too: snowflake_load.sql runs both
        # COPY statements. So new flight files reach Snowflake within the hour
        # whichever DAG loads first, and at 09:00 this runs at the same moment as
        # the flights DAG's own load (COPY's load history keeps that safe).
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipeline/run_snowflake_setup.py snowflake_load.sql",
    )

    extract_weather >> load_weather
