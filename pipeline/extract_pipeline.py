import requests
import boto3
import os
import csv
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Single source of truth for which airports are in scope. dbt loads this same
# file as a seed (dim_airports), so the pipeline and the warehouse cannot
# disagree about scope.
AIRPORTS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dbt", "seeds", "dim_airports.csv"
)

# AviationStack free tier caps a single request at 100 records. One request
# costs the same whether it returns 5 rows or 100, so always ask for the max.
FLIGHTS_PER_REQUEST = 100


def load_airports():
    with open(AIRPORTS_FILE) as f:
        return list(csv.DictReader(f))


def fetch_flights(arrival_iata):
    api_key = os.getenv("FLIGHT_API_KEY")

    url = "http://api.aviationstack.com/v1/flights"

    params = {
        "access_key" : api_key,
        "arr_iata" : arrival_iata,
        "flight_status" : "landed",
        "limit" : FLIGHTS_PER_REQUEST
    }

    response = requests.get(url, params = params)

    if response.status_code != 200:
        print(f"  flights {arrival_iata}: request failed {response.status_code} {response.text[:120]}")
        return None
    else:
        data = response.json()
        count = len(data.get("data", []))
        print(f"  flights {arrival_iata}: {count} landed flights")
        return data


def fetch_weather(lat, lon, iata):
    api_key = os.getenv("WEATHER_API_KEY")

    url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "imperial"
    }

    response = requests.get(url, params = params)

    if response.status_code != 200:
        print(f"  weather {iata}: request failed {response.status_code} {response.text[:120]}")
        return None
    else:
        data = response.json()
        print(f"  weather {iata}: {data['main']['temp']:.0f}F {data['weather'][0]['main']}")
        return data


def upload_to_s3(data, prefix, iata):
    s3 = boto3.client(
        "s3",
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = os.getenv("AWS_REGION")
    )
    now = datetime.now()

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


def run_flights(airports):
    """Daily task — AviationStack's free quota is the binding constraint."""
    print("FLIGHTS")
    for airport in airports:
        iata = airport["iata_code"]
        data = fetch_flights(iata)
        if data:
            upload_to_s3(data, "raw/flights", iata)


def run_weather(airports):
    """Hourly task — OpenWeatherMap's quota is generous, and dense weather
    observations are what make the flight/weather join meaningful."""
    print("WEATHER")
    for airport in airports:
        iata = airport["iata_code"]
        data = fetch_weather(airport["latitude"], airport["longitude"], iata)
        if data:
            upload_to_s3(data, "raw/weather", iata)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode not in ("flights", "weather", "all"):
        sys.exit(f"Usage: python3 pipeline/extract_pipeline.py [flights|weather|all]")

    airports = load_airports()
    print(f"{len(airports)} airports in scope: {', '.join(a['iata_code'] for a in airports)}")

    if mode in ("flights", "all"):
        run_flights(airports)
    if mode in ("weather", "all"):
        run_weather(airports)
