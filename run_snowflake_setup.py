"""Execute snowflake_setup.sql one statement at a time.

Environment-specific values live in .env, never in the .sql file (which is
tracked in git). Placeholders written as <VAR_NAME> are substituted here at
runtime, so .env stays the single source of truth.
"""

import os
import sys

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SETUP_FILE = "snowflake_setup.sql"

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

    for name in PLACEHOLDER_VARS:
        sql_text = sql_text.replace(f"<{name}>", env(name))

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

sys.exit(1 if failed else 0)
