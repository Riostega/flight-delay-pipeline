"""Execute a Snowflake SQL file one statement at a time.

    python3 pipeline/run_snowflake_setup.py                      # snowflake_setup.sql
    python3 pipeline/run_snowflake_setup.py snowflake_load.sql   # daily load

Environment-specific values live in .env, never in the .sql file (which is
tracked in git). Placeholders written as <VAR_NAME> are substituted here at
runtime, so .env stays the single source of truth.

Only the variables a given file actually references are required. That is what
lets snowflake_load.sql run on the EC2 host, which has no AWS credentials at
all: the stages already hold what Snowflake needs to reach S3, so the daily
load asks for nothing the box does not have.
"""

import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# SQL files sit beside this script, so they resolve relative to it rather than
# to the working directory — the DAG invokes this from the repository root.
SQL_DIR = Path(__file__).resolve().parent
SETUP_FILE = SQL_DIR / (sys.argv[1] if len(sys.argv) > 1 else "snowflake_setup.sql")

# Placeholders substituted into the SQL before execution.
PLACEHOLDER_VARS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "S3_BUCKET_NAME",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

SECRET_VARS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "SNOWFLAKE_PASSWORD"}


def env(name):
    """Read a required environment variable, stripping stray whitespace."""
    value = (os.getenv(name) or "").strip()
    if not value:
        sys.exit(f"Missing required environment variable in .env: {name}")
    return value


def clean_statement(stmt):
    lines = [line for line in stmt.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines).strip()


def load_statements(path):
    with open(path) as f:
        sql_text = f.read()

    # Resolve only the placeholders this file actually uses, so a file needing
    # no credentials can run on a host that has none.
    for name in PLACEHOLDER_VARS:
        token = f"<{name}>"
        if token in sql_text:
            sql_text = sql_text.replace(token, env(name))

    statements = [clean_statement(s) for s in sql_text.split(";")]
    return [s for s in statements if s]


def redact(text):
    """Strip secret values out of text before it is printed."""
    for name in SECRET_VARS:
        value = (os.getenv(name) or "").strip()
        if value:
            text = text.replace(value, f"<{name}>")
    return text


statements = load_statements(SETUP_FILE)

conn = snowflake.connector.connect(
    account=env("SNOWFLAKE_ACCOUNT"),
    user=env("SNOWFLAKE_USER"),
    password=env("SNOWFLAKE_PASSWORD"),
    warehouse=env("SNOWFLAKE_WAREHOUSE"),
    database=env("SNOWFLAKE_DATABASE"),
    schema=env("SNOWFLAKE_SCHEMA"),
)
cur = conn.cursor()

failed = False
for i, statement in enumerate(statements, start=1):
    preview = redact(statement.splitlines()[0])[:80]
    print(f"[{i}/{len(statements)}] {preview}")
    try:
        cur.execute(statement)
        if statement.upper().startswith("SELECT"):
            for row in cur.fetchall():
                print("   ", row)
    except Exception as e:
        print(f"\nFailed on statement {i}:\n{redact(statement)}\n{e}")
        failed = True
        break

cur.close()
conn.close()

# Creating the objects does not populate them: the COPY INTO statements live in
# snowflake_load.sql so the daily path needs no AWS credentials. Say so, because
# a rebuild that stops here produces empty tables and models that build cleanly
# over nothing.
if not failed and SETUP_FILE.name == "snowflake_setup.sql":
    print("\nObjects created but empty. Populate them next:")
    print("  python3 pipeline/run_snowflake_setup.py snowflake_load.sql")

sys.exit(1 if failed else 0)
