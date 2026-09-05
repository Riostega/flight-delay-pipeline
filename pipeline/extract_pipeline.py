import requests
import boto3
import os
import csv
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

# Both APIs are called from a scheduled task. Without a timeout a hung request
# blocks the Airflow task indefinitely — no failure, no retry, just data
# quietly ceasing to arrive.
REQUEST_TIMEOUT = 30

# Single source of truth for which airports are in scope. dbt loads this same
# file as a seed (dim_airports), so the pipeline and the warehouse cannot
# disagree about scope.
AIRPORTS_FILE = REPO_ROOT / "dbt" / "seeds" / "dim_airports.csv"

# AviationStack free tier caps a single request at 100 records. One request
# costs the same whether it returns 5 rows or 100, so always ask for the max.
FLIGHTS_PER_REQUEST = 100

# AviationStack's free tier allows 100 requests per calendar month, and one
# flights run spends one request per airport. This budget leaves headroom under
# that ceiling for a manual run or an unplanned retry.
MONTHLY_REQUEST_BUDGET = 90


def flight_requests_used_this_month(s3, bucket):
    """Count this month's flight pulls from the raw zone.

    Each successful request writes exactly one file, so the object count is the
    request count. It undercounts when a request fails before landing a file —
    a failed call still spends quota — which is why the budget above sits below
    the real ceiling rather than at it.
    """
    prefix = f"raw/flights/{datetime.now(timezone.utc):%Y-%m}"
    used = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        used += len(page.get("Contents", []))
    return used


def load_airports():
    with open(AIRPORTS_FILE) as f:
        return list(csv.DictReader(f))


def fetch_flights(arrival_iata):
    api_key = os.getenv("FLIGHT_API_KEY")

    url = "http://api.aviationstack.com/v1/flights"

    params = {
        "access_key": api_key,
        "arr_iata": arrival_iata,
        "flight_status": "landed",
        "limit": FLIGHTS_PER_REQUEST,
    }

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

    if response.status_code != 200:
        print(f"  flights {arrival_iata}: request failed {response.status_code} {response.text[:120]}")
        return None

    data = response.json()

    # AviationStack reports quota exhaustion, a rejected key, and rate limiting
    # with HTTP 200 and an "error" object in the body rather than a 4xx status.
    # Treating that payload as a successful pull defeats the exit-code guard at
    # the bottom of this file: the error JSON lands in S3, COPY INTO loads it,
    # `lateral flatten` over the absent `data` key yields zero rows, and the DAG
    # reports success while the warehouse quietly stops growing. Quota
    # exhaustion is the expected end-of-month state, not an edge case.
    if "error" in data:
        err = data["error"] if isinstance(data["error"], dict) else {}
        print(f"  flights {arrival_iata}: API error "
              f"{err.get('code', data['error'])} — {err.get('message', '')}")
        return None

    # A 200 carrying neither "error" nor "data" is not a real response either.
    # Landing it would write a file that flattens to nothing, which downstream
    # is indistinguishable from an airport that truly had no landed flights.
    if "data" not in data:
        print(f"  flights {arrival_iata}: unexpected response shape {str(data)[:120]}")
        return None

    count = len(data["data"])
    print(f"  flights {arrival_iata}: {count} landed flights")
    return data


def fetch_weather(lat, lon, iata):
    api_key = os.getenv("WEATHER_API_KEY")

    url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "imperial",
    }

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

    if response.status_code != 200:
        print(f"  weather {iata}: request failed {response.status_code} {response.text[:120]}")
        return None
    else:
        data = response.json()
        print(f"  weather {iata}: {data['main']['temp']:.0f}F {data['weather'][0]['main']}")
        return data


def upload_to_s3(data, prefix, iata):
    # Credentials are passed explicitly when present and fall through to
    # boto3's own chain when absent — which is how the EC2 host reaches S3 via
    # its IAM role with no keys on disk.
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    # UTC, not local time. datetime.now() follows the host's timezone, so the
    # same instant landed under two different date partitions depending on
    # whether the laptop (CDT) or the EC2 host (UTC) ran the extract, and two
    # files could share an HHMMSS while being hours apart.
    now = datetime.now(timezone.utc)

    # Airport goes in the key for legibility only. The raw JSON is landed
    # exactly as received — the airport is recoverable from the payload itself
    # (arrival.iata for flights, coord for weather), so nothing depends on
    # parsing this filename.
    key = f"{prefix}/{now.strftime('%Y-%m-%d')}/{iata}_{now.strftime('%H%M%S')}.json"

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=key,
        Body=json.dumps(data)
    )

    print(f"    uploaded s3://{os.getenv('S3_BUCKET_NAME')}/{key}")


def check_flight_budget(airports):
    """Refuse to start a run that would overrun the monthly request budget.

    Without this, exceeding the quota is only discovered by the API refusing
    the call — after the requests have already been spent. Checking first makes
    the limit something the pipeline observes rather than discovers.
    """
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
        used = flight_requests_used_this_month(s3, os.getenv("S3_BUCKET_NAME"))
    except Exception as exc:
        # A budget check that cannot run is not a reason to block collection.
        print(f"  budget check unavailable ({exc}); proceeding")
        return True

    needed = len(airports)
    print(f"  quota: {used}/{MONTHLY_REQUEST_BUDGET} used this month, this run needs {needed}")
    if used + needed > MONTHLY_REQUEST_BUDGET:
        print(f"  REFUSING: would reach {used + needed}, over the {MONTHLY_REQUEST_BUDGET} budget")
        return False
    return True


def run_flights(airports):
    """Daily task — AviationStack's free quota is the binding constraint.

    Returns the number of airports collected.
    """
    print("FLIGHTS")
    collected = 0
    for airport in airports:
        iata = airport["iata_code"]
        data = fetch_flights(iata)
        if data:
            upload_to_s3(data, "raw/flights", iata)
            collected += 1
    return collected


def run_weather(airports):
    """Hourly task — OpenWeatherMap's quota is generous, and dense weather
    observations are what make the flight/weather join meaningful.

    Returns the number of airports collected.
    """
    print("WEATHER")
    collected = 0
    for airport in airports:
        iata = airport["iata_code"]
        data = fetch_weather(airport["latitude"], airport["longitude"], iata)
        if data:
            upload_to_s3(data, "raw/weather", iata)
            collected += 1
    return collected


if __name__ == "__main__":
    # The mode is required rather than defaulting. "flights" spends five of
    # roughly a hundred monthly AviationStack calls, and a command that costs
    # quota should not be what you get by typing nothing.
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    if mode not in ("flights", "weather", "all"):
        sys.exit("Usage: python3 pipeline/extract_pipeline.py <flights|weather|all>")

    airports = load_airports()
    print(f"{len(airports)} airports in scope: {', '.join(a['iata_code'] for a in airports)}")

    attempted = 0
    collected = 0
    if mode in ("flights", "all"):
        if not check_flight_budget(airports):
            sys.exit("FAILED: monthly AviationStack budget reached — no requests spent")
        attempted += len(airports)
        collected += run_flights(airports)
    if mode in ("weather", "all"):
        attempted += len(airports)
        collected += run_weather(airports)

    # Exit non-zero when nothing at all was collected, so Airflow fails the task
    # and retries rather than reporting a green run. Without this, an exhausted
    # API quota — the expected failure roughly three weeks into each month —
    # looks identical to a successful run: no files land, no alarm is raised,
    # and the only symptom is data quietly ceasing to grow.
    #
    # A partial failure is logged but tolerated: one airport failing is not a
    # reason to discard the four that succeeded, and the next run will catch up.
    print(f"collected {collected}/{attempted}")
    if collected == 0:
        sys.exit("FAILED: no data collected from any airport — check API quota and credentials")
