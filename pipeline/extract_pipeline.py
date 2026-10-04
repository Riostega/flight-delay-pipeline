import requests
import boto3
import os
import csv
import re
import sys
import calendar
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

# AviationStack's free tier allows 100 requests a month, and one flights run
# spends one request per airport. This budget leaves headroom under that ceiling
# for a manual run, and for requests the count below cannot see.
#
# The count assumes the quota resets on the 1st of each calendar month (UTC).
# That is an ASSUMPTION, not something read off AviationStack: if the plan
# actually resets on the signup anniversary, the guard's window is misaligned.
# Check the reset date on the AviationStack dashboard (it costs no requests)
# before relying on it.
MONTHLY_REQUEST_BUDGET = 90

# The flights DAG's schedule, "0 9 */2 * *": 09:00 UTC on odd days of the month.
# Only used to print how much of the budget the rest of the month's scheduled runs
# will need; if the schedule in dags/flight_pipeline_daily.py changes, change this.
SCHEDULED_RUN_HOUR_UTC = 9

# Exit codes, so a Slack alert can say WHICH failure this was (pipeline/notify.py
# maps them to a hint). Airflow's BashOperator only reports "exit code N".
# 99 is avoided: BashOperator treats it as "skip", not "fail".
EXIT_NO_DATA = 5          # a whole source collected nothing: check the API and keys
                          # (not 1: Python exits 1 on any crash, which would get the wrong hint)
EXIT_BUDGET_REFUSED = 3   # the monthly budget is used up: expected, resumes on the 1st
EXIT_BUDGET_UNKNOWN = 4   # S3 could not be listed, so the budget is unknown: nothing spent


def make_key(prefix, iata, now):
    """The S3 key for one landed response: <prefix>/<YYYY-MM-DD>/<IATA>_<HHMMSS>_<micro>.json.

    Three other places parse this shape, so it is built in exactly one spot:
    stg_weather.sql takes the airport from it, the watchdog takes the pull time
    and the airport from it, and the budget below counts files by its month prefix.
    """
    return f"{prefix}/{now:%Y-%m-%d}/{iata}_{now:%H%M%S_%f}.json"


def month_prefix(now):
    """The raw/flights prefix that holds every flights file landed in now's month."""
    return f"raw/flights/{now:%Y-%m}"


def flight_requests_used_this_month(s3, bucket):
    """Count this month's flight pulls from the raw zone.

    This counts FILES LANDED, which is not quite requests made. A request whose
    file never lands still spent quota, and is invisible here:
      - an error response (non-200, or a 200 carrying an "error" body);
      - a read timeout after AviationStack had already counted the call;
      - an upload that failed after a successful, billed response;
      - the process being killed between the response and the upload.
    Deleting a file from raw/flights also lowers the count, as if the quota had
    been refunded (it hasn't, and the bucket is versioned anyway), so raw files
    are never deleted. The gap between MONTHLY_REQUEST_BUDGET and the real cap
    of 100 is what absorbs these.
    """
    prefix = month_prefix(datetime.now(timezone.utc))
    used = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        used += len(page.get("Contents", []))
    return used


def s3_client():
    """An S3 client, with keys from .env when present and the IAM role when not.

    Credentials are passed explicitly when set and fall through to boto3's own
    chain when absent, which is how the EC2 host reaches S3 via its IAM role
    with no keys on disk. `or None` matters: a BLANK "AWS_ACCESS_KEY_ID=" line
    gives "", and boto3 treats "" as a real (empty) key and signs with it instead
    of falling back to the role, so every S3 call fails.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
        region_name=os.getenv("AWS_REGION") or None,
    )


def load_airports():
    with open(AIRPORTS_FILE) as f:
        return list(csv.DictReader(f))


# Both APIs take their key as a query parameter, and requests puts the full URL,
# query string included, into the message of a ConnectionError or Timeout. So
# printing an exception as-is would write the key into the Airflow task log.
_KEY_PARAM = re.compile(r"(access_key|appid)=[^&\s'\")]+", re.IGNORECASE)


def _redact(text):
    """text with both API keys removed: by value, then by parameter name as a fallback."""
    for name in ("FLIGHT_API_KEY", "WEATHER_API_KEY"):
        secret = (os.getenv(name) or "").strip()
        if secret:  # guard: replace("", ...) would put <redacted> between every character
            text = text.replace(secret, "<redacted>")
    return _KEY_PARAM.sub(r"\1=<redacted>", text)


def _safe_error(exc):
    """An exception as printable text, with API keys removed."""
    return _redact(f"{type(exc).__name__}: {exc}")


def fetch_flights(arrival_iata):
    """One AviationStack request for one arrival airport.

    Returns (data, body): data is the parsed JSON, used to check the response;
    body is the raw bytes exactly as received, which is what lands in S3.
    Returns (None, None) when the response must not be landed.
    """
    api_key = os.getenv("FLIGHT_API_KEY")

    # Plain HTTP, not HTTPS, because AviationStack's free plan serves HTTP only
    # (HTTPS is a paid feature; on the free plan an https request comes back as a
    # 200 with an "https_access_restricted" error, which the check below catches).
    # So this key crosses the network unencrypted. Accepted: it is a free key,
    # capped at 100 requests a month and rotatable, and the scheduled runs go out
    # from the EC2 host inside AWS. Switch to https:// if the plan is upgraded.
    url = "http://api.aviationstack.com/v1/flights"

    params = {
        "access_key": api_key,
        "arr_iata": arrival_iata,
        "flight_status": "landed",
        "limit": FLIGHTS_PER_REQUEST,
    }

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

    if response.status_code != 200:
        print(f"  flights {arrival_iata}: request failed {response.status_code} {_redact(response.text[:120])}")
        return None, None

    data = response.json()

    # AviationStack reports quota exhaustion, a rejected key, and rate limiting
    # with HTTP 200 and an "error" object in the body rather than a 4xx status.
    # Treating that payload as a successful pull defeats the exit-code guard at
    # the bottom of this file: the error JSON lands in S3, COPY INTO loads it,
    # `lateral flatten` over the absent `data` key yields zero rows, and the DAG
    # reports success while the warehouse quietly stops growing.
    #
    # The budget guard (check_flight_budget) normally stops runs before the quota
    # runs out, so this should be rare. It covers what the guard can't see: a
    # revoked key, rate limiting, a plan change, or quota spent by requests the
    # guard never counted (see flight_requests_used_this_month).
    if "error" in data:
        err = data["error"] if isinstance(data["error"], dict) else {}
        print(f"  flights {arrival_iata}: API error "
              f"{err.get('code', data['error'])} — {err.get('message', '')}")
        return None, None

    # A 200 carrying neither "error" nor "data" is not a real response, and is
    # not landed. (A 200 whose "data" list is EMPTY is different: it is a real
    # response and is landed, but run_flights does not count it as collected.)
    if not isinstance(data.get("data"), list):
        print(f"  flights {arrival_iata}: unexpected response shape {_redact(str(data)[:120])}")
        return None, None

    count = len(data["data"])
    print(f"  flights {arrival_iata}: {count} landed flights")
    return data, response.content


def fetch_weather(lat, lon, iata):
    """One OpenWeatherMap request. Returns (data, body) like fetch_flights."""
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
        print(f"  weather {iata}: request failed {response.status_code} {_redact(response.text[:120])}")
        return None, None
    else:
        data = response.json()
        print(f"  weather {iata}: {data['main']['temp']:.0f}F {data['weather'][0]['main']}")
        return data, response.content


def upload_to_s3(body, prefix, iata):
    """Land one response in S3, byte for byte as the API sent it.

    body is the raw response bytes, not the parsed dict. This used to write
    json.dumps(data), a parse-and-re-encode round trip that turned "Cancún" into
    "Canc\\u00fan" and normalised whitespace. The content was the same, but the raw
    zone is the source of truth and is described as untouched, so it now is.
    (Files landed before 2026-10-04 are those json.dumps re-serialisations.)
    """
    s3 = s3_client()
    # UTC, not local time. datetime.now() follows the host's timezone, so the
    # same instant landed under two different date partitions depending on
    # whether the laptop (CDT) or the EC2 host (UTC) ran the extract, and two
    # files could share an HHMMSS while being hours apart.
    now = datetime.now(timezone.utc)

    # The airport in this key is LOad-BEARING for weather. OpenWeatherMap's
    # response carries coordinates but no airport code, so stg_weather derives
    # iata_code by regex over source_file. Changing this key shape breaks that
    # join. (It already did once: pre-IATA keys of the form
    # raw/weather/<date>/HHMMSS.json cannot be matched, and those rows are still
    # in the warehouse with a null iata_code.) Flights genuinely do not depend
    # on it — arrival.iata is in the payload.
    # Microseconds, not just seconds. A whole 5-airport run finishes inside a
    # second or two, so a manual run overlapping the scheduled one produced the
    # same key for the same airport and put_object silently overwrote the
    # earlier file — losing raw data from the zone that is meant to be the
    # immutable source of truth.
    key = make_key(prefix, iata, now)

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    print(f"    uploaded s3://{os.getenv('S3_BUCKET_NAME')}/{key}")


def scheduled_runs_left(now):
    """How many scheduled flights runs are still to come this month, after now."""
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    runs = 0
    for day in range(1, days_in_month + 1, 2):  # odd days: */2 in the cron schedule
        run_time = now.replace(day=day, hour=SCHEDULED_RUN_HOUR_UTC, minute=0, second=0, microsecond=0)
        if run_time > now:
            runs += 1
    return runs


def check_flight_budget(airports):
    """Refuse to start a run that would overrun the monthly request budget.

    Without this, exceeding the quota is only discovered by the API refusing
    the call — after the requests have already been spent. Checking first makes
    the limit something the pipeline observes rather than discovers.

    Returns None when the run may go ahead, otherwise the exit code to stop with.
    """
    try:
        s3 = s3_client()
        used = flight_requests_used_this_month(s3, os.getenv("S3_BUCKET_NAME"))
    except Exception as exc:
        # Fail CLOSED. This used to carry on, which left the guard blind in exactly
        # the outage that spends quota without counting it: if S3 can't be listed,
        # it very likely can't be written either, so every request would be paid
        # for and nothing would land. Refusing costs one run; nothing is spent.
        print(f"  budget check unavailable ({_safe_error(exc)})")
        print("  REFUSING: S3 is unreachable, so the budget is unknown and nothing could be landed anyway")
        return EXIT_BUDGET_UNKNOWN

    needed = len(airports)
    # Worded as what it is: files counted in S3 against a budget this project set
    # itself, NOT AviationStack's own usage figure.
    print(f"  S3 flight pulls counted this calendar month (UTC): {used}/{MONTHLY_REQUEST_BUDGET} "
          f"self-imposed budget (AviationStack's cap is 100); this run needs {needed}")
    if used + needed > MONTHLY_REQUEST_BUDGET:
        print(f"  REFUSING: would reach {used + needed}, over the {MONTHLY_REQUEST_BUDGET} budget")
        return EXIT_BUDGET_REFUSED

    # Information only; never changes the decision. The schedule spends 70-80
    # requests a month, so only the first two or three manual runs are free. Each
    # one after that makes the guard refuse a scheduled run at the end of the
    # month — which is how Sep 26-29 ended up with no flights at all.
    now = datetime.now(timezone.utc)
    runs_left = scheduled_runs_left(now)
    headroom = MONTHLY_REQUEST_BUDGET - used - needed - runs_left * needed
    print(f"  {runs_left} scheduled run(s) left this month after this one; "
          f"budget left after all of them: {headroom}")
    if headroom < 0:
        print("  WARNING: if this is a manual run, a scheduled run at the end of the month will be refused")
    return None


def run_flights(airports):
    """Every-other-day task — AviationStack's free quota is the binding constraint.

    Returns the number of airports collected.
    """
    print("FLIGHTS")
    collected = 0
    for airport in airports:
        iata = airport["iata_code"]
        # Per-airport boundary. requests can raise (read timeout, DNS blip,
        # connection reset) and a payload can be shaped unexpectedly; neither is
        # caught by the status-code and body checks in fetch_*. Without this an
        # error on airport 2 of 5 aborted the run and the remaining three were
        # never attempted, and with retries=0 on the flights DAG their data for
        # that window was lost: the next run, 48 hours later, fetches a different
        # snapshot.
        #
        # One airport failing is not a reason to discard the others. A whole
        # source failing is still caught by the per-source guard at the bottom.
        try:
            data, body = fetch_flights(iata)
            if data:
                # Landed even when it holds no flights: the raw zone keeps what was
                # received, and the budget guard counts files, so skipping it would
                # undercount a request that was really spent.
                upload_to_s3(body, "raw/flights", iata)
                if data["data"]:
                    collected += 1
                else:
                    # For these five hubs an empty list is never real (every
                    # production response so far reported hundreds to thousands of
                    # matches). Not counting it means five empty airports fail the
                    # run instead of reporting "collected 5/5".
                    total = (data.get("pagination") or {}).get("total")
                    print(f"  flights {iata}: WARNING 0 flights returned (pagination.total={total}); "
                          "landed but not counted as collected")
        except Exception as exc:
            print(f"  flights {iata}: unhandled error, skipping — {_safe_error(exc)}")
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
        try:
            data, body = fetch_weather(airport["latitude"], airport["longitude"], iata)
            if data:
                upload_to_s3(body, "raw/weather", iata)
                collected += 1
        except Exception as exc:
            print(f"  weather {iata}: unhandled error, skipping — {_safe_error(exc)}")
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
    # Tracked per source, not just in total. Summing them and testing the sum
    # against zero masks a whole-source failure in "all" mode: five successful
    # weather pulls and zero flights gives collected == 5, the guard below stays
    # quiet, and the run reports success having collected no flights at all.
    failed_sources = []

    refusal = None
    if mode in ("flights", "all"):
        refusal = check_flight_budget(airports)
        if refusal:
            print("FAILED: flights run refused by the budget guard — no requests spent", file=sys.stderr)
            if mode == "flights":
                sys.exit(refusal)
            # In "all" mode a flights refusal must not cost the weather pull too,
            # so weather still runs and the refusal is reported at the end.
        else:
            attempted += len(airports)
            got = run_flights(airports)
            collected += got
            if got == 0:
                failed_sources.append("flights")

    if mode in ("weather", "all"):
        attempted += len(airports)
        got = run_weather(airports)
        collected += got
        if got == 0:
            failed_sources.append("weather")

    if refusal:
        sys.exit(refusal)

    # Exit non-zero when a source collected nothing, so Airflow marks the task
    # failed and the Slack callback fires, instead of showing a green run while
    # data stops growing. A rejected key, or quota spent outside the guard's
    # view, would otherwise look identical to success. The weather DAG then
    # retries; the flights DAG deliberately does not, because every retry spends
    # five more requests. (Late in a month, flights runs are normally stopped
    # earlier, by the budget guard's exit above, before any request is spent.)
    #
    # A partial failure is logged but tolerated: one airport failing is not a
    # reason to discard the four that succeeded. That airport's data for this
    # run is lost for good, though: both APIs return current data only, so a
    # later run fetches a different window rather than the missed one. The
    # watchdog's per-airport freshness check is what alerts if one airport keeps
    # failing.
    print(f"collected {collected}/{attempted}")
    if failed_sources:
        print(
            f"FAILED: no data collected for {', '.join(failed_sources)} "
            "— check API quota and credentials",
            file=sys.stderr,
        )
        sys.exit(EXIT_NO_DATA)
