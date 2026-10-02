"""Loading and preparing flight data, shared by every analysis notebook.

    from flights_data import load_flights, prepare

    flights = load_flights()   # every row of fct_flight_events
    clean = prepare(flights)   # suspect rows removed, analysis columns added
    weather = load_weather()   # every hourly weather reading (stg_weather)
"""

import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import find_dotenv, load_dotenv

# Searches upward from the current folder, so it finds the repository's .env from analysis/.
load_dotenv(find_dotenv(usecwd=True))

BACKUP_DIR = Path.home() / "flight-pipeline-backup" / "warehouse_export"
BACKUP_CSV = BACKUP_DIR / "fct_flight_events_2026-10-02.csv"
WEATHER_BACKUP_CSV = BACKUP_DIR / "stg_weather_2026-10-02.csv"

# Operators matched by name. Simple keyword lists built by reading the carrier names in the
# data: they cover what's there today, but a new operator would need adding.
CARGO_WORDS = ("Cargo|Fedex|Dhl|Ups|Abx Air|Atlas Air|Air Transport International|Amerijet|Kalitta"
               "|Aeronaves Tsm|Aerologic|Aitheras")

# Private jets and air ambulances fly on demand, not to a published airline schedule, so
# "late against the schedule" doesn't mean the same thing for them.
NON_SCHEDULED_WORDS = ("Flexjet|Netjets|Wheels Up|Vistajet|Selectjet|Reach Air Medical"
                       "|Pegasus Elite|K&R Aviation|Revv Aviation|Ati Jet|Neajets|Baker Aviation"
                       "|Airsprint|Planesense|Private Owner|Qatar Executive")

TIME_OF_DAY_LABELS = ["night (0-5)", "morning (6-11)", "afternoon (12-17)", "evening (18-23)"]


def read_backup_csv(reason, path=BACKUP_CSV):
    """Fall back to a CSV export, and say so loudly so stale data is never mistaken for live."""
    print(f"*** {reason}")
    print(f"*** Using the CSV backup instead: {path.name}")
    return pd.read_csv(path)


def read_table(table, backup_csv):
    """Read a whole Snowflake table, or its CSV backup if Snowflake can't be reached."""
    # Only a missing or failed connection falls back to the CSV. A broken query should
    # fail loudly rather than quietly analysing old data.
    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        return read_backup_csv("No Snowflake credentials found (is there a .env file?).", backup_csv)
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            login_timeout=30,
        )
    except snowflake.connector.errors.Error as error:
        return read_backup_csv(f"Snowflake unavailable ({error}).", backup_csv)
    with conn:
        rows = conn.cursor().execute(f"select * from {table}").fetch_pandas_all()
    print(f"Loaded {table} live from Snowflake")
    return rows


def load_flights():
    """Every row of fct_flight_events, with column names lowercased and timestamps parsed."""
    flights = read_table("fct_flight_events", BACKUP_CSV)
    flights.columns = flights.columns.str.lower()
    for column in ["flight_date", "arrival_scheduled_local", "arrival_scheduled_utc",
                   "arrival_actual_utc", "departure_scheduled_utc"]:
        flights[column] = pd.to_datetime(flights[column]).astype("datetime64[ns]")
    print(f"{len(flights):,} flights")
    return flights


def load_weather():
    """Every hourly weather reading from stg_weather, timestamps in UTC."""
    weather = read_table("stg_weather", WEATHER_BACKUP_CSV)
    weather.columns = weather.columns.str.lower()
    weather["observed_at"] = pd.to_datetime(weather["observed_at"]).astype("datetime64[ns]")
    return weather.dropna(subset=["iata_code"])


def wind_at_scheduled_arrival(flights, weather, max_lag_minutes=120):
    """Wind speed from the latest reading at or before each flight's SCHEDULED arrival.

    fct_flight_events matches weather to the ACTUAL arrival, so a late flight gets a later
    reading than it would have if on time. Matching on the schedule fixes the reading before
    any delay happens.
    """
    readings = (
        weather[["iata_code", "observed_at", "wind_speed"]]
        .rename(columns={"iata_code": "arrival_airport", "wind_speed": "wind_at_schedule"})
        .sort_values("observed_at")
    )
    ordered = flights.sort_values("arrival_scheduled_utc").reset_index()
    matched = pd.merge_asof(
        ordered, readings,
        left_on="arrival_scheduled_utc", right_on="observed_at", by="arrival_airport",
        direction="backward", tolerance=pd.Timedelta(minutes=max_lag_minutes),
    )
    return matched.set_index("index")["wind_at_schedule"].reindex(flights.index)


def prepare(flights):
    """Remove suspect rows and add the columns the analysis uses."""
    # Rows whose schedule belongs to a different flight are excluded from all analysis.
    clean = flights[~flights["has_suspect_times"]].copy()

    # Time of day uses the SCHEDULED arrival, not the actual one. A late flight lands later,
    # so using the actual time would push late flights into later time slots and make
    # those slots look worse than they are.
    clean["arrival_hour"] = clean["arrival_scheduled_local"].dt.hour
    clean["time_of_day"] = pd.cut(clean["arrival_hour"], bins=[0, 6, 12, 18, 24],
                                  right=False, labels=TIME_OF_DAY_LABELS)
    clean["is_sfo"] = clean["arrival_airport"] == "SFO"

    # Some carriers appear under livery names, e.g. "Alaska Airlines (Oneworld Livery)".
    # Dropping the bracketed part merges them back into one carrier.
    clean["carrier"] = clean["operating_carrier_name"].str.replace(r"\s*\(.*\)", "", regex=True)
    clean["is_cargo"] = clean["carrier"].str.contains(CARGO_WORDS, case=False, na=False)
    clean["is_non_scheduled"] = clean["carrier"].str.contains(NON_SCHEDULED_WORDS, case=False, na=False)

    # Which API pull captured the flight. The regular pull runs at 09:00 UTC and returns
    # flights that landed in the hours before it, so a flight belongs to the next 09:00 UTC
    # after it landed. Adding 15 hours and keeping the date does exactly that, for any pull
    # that looks back less than 24 hours (the real ones look back about 15). This matched
    # the raw files for every regular pull; it is not meaningful for the manual pulls on
    # 4-5 September.
    clean["pull_date"] = (clean["arrival_actual_utc"] + pd.Timedelta(hours=15)).dt.normalize()

    # How long before the pull the flight was scheduled to land. A late flight scheduled
    # shortly before the pull hasn't landed yet, so the API never returns it. Flights
    # scheduled close to the pull therefore look far more punctual than they are.
    pull_time = clean["pull_date"] + pd.Timedelta(hours=9)
    clean["hours_before_pull"] = (pull_time - clean["arrival_scheduled_utc"]).dt.total_seconds() / 3600

    clean["scheduled_duration_hours"] = (
        clean["arrival_scheduled_utc"] - clean["departure_scheduled_utc"]
    ).dt.total_seconds() / 3600
    return clean
