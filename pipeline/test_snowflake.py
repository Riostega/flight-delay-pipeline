"""Connection smoke test — the first thing to run after filling in .env.

Prints the Snowflake version on success. On failure it says which piece of
configuration is wrong, because this is where a new setup fails first and the
driver's own errors are not self-explanatory: an empty username produces a
message about OAUTH_AUTHORIZATION_CODE and WORKLOAD_IDENTITY rather than
"you have not filled in .env".

    python3 pipeline/test_snowflake.py
"""

import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

REQUIRED = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

missing = [name for name in REQUIRED if not (os.getenv(name) or "").strip()]
if missing:
    sys.exit(
        "Snowflake settings are missing from .env:\n"
        + "".join(f"  {name}\n" for name in missing)
        + "\nCopy .env.example to .env and fill these in. The account identifier is the\n"
        "ORGNAME-ACCOUNTNAME pair from your Snowflake URL, not the numeric locator."
    )

try:
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT").strip(),
        user=os.getenv("SNOWFLAKE_USER").strip(),
        password=os.getenv("SNOWFLAKE_PASSWORD").strip(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE").strip(),
        database=os.getenv("SNOWFLAKE_DATABASE").strip(),
        schema=os.getenv("SNOWFLAKE_SCHEMA").strip(),
    )
except Exception as e:
    sys.exit(
        f"Could not connect to Snowflake:\n  {e}\n\n"
        "Check SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER and SNOWFLAKE_PASSWORD in .env.\n"
        "Note that the warehouse and database do not need to exist yet — "
        "pipeline/run_snowflake_setup.py creates them."
    )

with conn:
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
    version, account, warehouse, database = cur.fetchone()
    cur.close()

print("Connected to Snowflake")
print(f"  version   : {version}")
print(f"  account   : {account}")
print(f"  warehouse : {warehouse or 'none set — run_snowflake_setup.py will create it'}")
print(f"  database  : {database or 'none set — run_snowflake_setup.py will create it'}")
