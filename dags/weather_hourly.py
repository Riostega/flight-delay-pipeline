"""Hourly weather collection.

Separate from the flight DAG because the two APIs have quotas differing by
orders of magnitude. AviationStack caps flights at roughly daily; OpenWeatherMap
allows far more, and dense weather observations are what make the eventual
flight/weather join meaningful rather than matching every flight to a single
coarse daily reading.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = os.environ.get("PIPELINE_PYTHON", "/usr/local/bin/python3")

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
