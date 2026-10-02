"""Loading and preparing flight data, shared by every analysis notebook.

    from flights_data import load_flights, prepare

    flights = load_flights()   # every row of fct_flight_events
    clean = prepare(flights)   # suspect rows removed, analysis columns added
"""

import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import find_dotenv, load_dotenv

# Searches upward from the current folder, so it finds the repository's .env from analysis/.
load_dotenv(find_dotenv(usecwd=True))

BACKUP_CSV = Path.home() / "flight-pipeline-backup" / "warehouse_export" / "fct_flight_events_2026-10-02.csv"

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


def read_backup_csv(reason):
    """Fall back to the CSV export, and say so loudly so stale data is never mistaken for live."""
    print(f"*** {reason}")
    print(f"*** Using the CSV backup instead: {BACKUP_CSV.name}")
    return pd.read_csv(BACKUP_CSV)


def load_flights():
    """Read fct_flight_events from Snowflake, or from the CSV backup if Snowflake can't be reached."""
    # Only a missing or failed connection falls back to the CSV. A broken query should
    # fail loudly rather than quietly analysing old data.
    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        flights = read_backup_csv("No Snowflake credentials found (is there a .env file?).")
    else:
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
            flights = read_backup_csv(f"Snowflake unavailable ({error}).")
        else:
            with conn:
                flights = conn.cursor().execute("select * from fct_flight_events").fetch_pandas_all()
            print("Loaded live data from Snowflake")

    flights.columns = flights.columns.str.lower()
    for column in ["flight_date", "arrival_scheduled_local", "arrival_scheduled_utc",
                   "arrival_actual_utc", "departure_scheduled_utc"]:
        flights[column] = pd.to_datetime(flights[column])
    print(f"{len(flights):,} flights")
    return flights


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
