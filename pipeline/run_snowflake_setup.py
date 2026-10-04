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

The session uses the user's default role unless SNOWFLAKE_ROLE is set in .env.
snowflake_setup.sql switches to ACCOUNTADMIN itself; snowflake_load.sql does not
switch at all, so the host can run it as the least-privilege PIPELINE_ROLE.
"""

import io
import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv
# The connector's own statement splitter. It understands comments and quoted
# strings, which a plain split(";") does not (see load_statements).
from snowflake.connector.util_text import split_statements

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
    "SNOWFLAKE_USER",
]

SECRET_VARS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "SNOWFLAKE_PASSWORD"}


def env(name):
    """Read a required environment variable, stripping stray whitespace."""
    value = (os.getenv(name) or "").strip()
    if not value:
        sys.exit(f"Missing required environment variable in .env: {name}")
    return value


def load_statements(path):
    with open(path) as f:
        sql_text = f.read()

    # Resolve only the placeholders this file actually uses, so a file needing
    # no credentials can run on a host that has none.
    for name in PLACEHOLDER_VARS:
        token = f"<{name}>"
        if token in sql_text:
            sql_text = sql_text.replace(token, env(name))

    # Not sql_text.split(";"). That split on every semicolon, including one inside
    # a comment ("-- run this; then that") or a string literal, and handed
    # Snowflake a fragment it rejects. Both SQL files are full of prose comments,
    # so it was one innocent comment away from breaking every hourly load.
    statements = [stmt.strip() for stmt, _is_put_get in
                  split_statements(io.StringIO(sql_text), remove_comments=True)]
    return [s for s in statements if s]


def redact(text):
    """Strip secret values out of text before it is printed."""
    for name in SECRET_VARS:
        value = (os.getenv(name) or "").strip()
        if value:
            text = text.replace(value, f"<{name}>")
    return text


def print_copy_summary(cur):
    """Say what one COPY INTO actually did: how many new files, how many rows.

    COPY returns one row per file it looked at (file, status, rows_loaded, ...),
    or a single "Copy executed with 0 files processed." row. These used to be
    thrown away, so a task log could not show whether a run loaded 0 files or 5.
    """
    rows = cur.fetchall()
    columns = [col[0].lower() for col in cur.description]
    if "file" not in columns:
        print("    0 new files")
        return
    file_i, status_i = columns.index("file"), columns.index("status")
    rows_i = columns.index("rows_loaded") if "rows_loaded" in columns else None
    loaded = [r for r in rows if r[status_i] == "LOADED"]
    row_count = sum(r[rows_i] or 0 for r in loaded) if rows_i is not None else "?"
    print(f"    {len(loaded)} new files, {row_count} rows loaded")
    error_i = columns.index("first_error") if "first_error" in columns else None
    for r in rows:
        if r[status_i] != "LOADED":
            error = r[error_i] if error_i is not None else ""
            print(f"    NOT LOADED: {r[file_i]} {r[status_i]} {error}")


# Setup statements depend on each other (USE DATABASE before CREATE TABLE, the
# stage before its grants), so setup stops at the first failure. Anything else,
# i.e. snowflake_load.sql, runs every statement and fails at the end. Its two
# COPYs are independent, and stopping at the first meant a broken flights stage
# also stopped every hourly weather load.
STOP_ON_FIRST_FAILURE = SETUP_FILE.name == "snowflake_setup.sql"

statements = load_statements(SETUP_FILE)

connect_args = dict(
    account=env("SNOWFLAKE_ACCOUNT"),
    user=env("SNOWFLAKE_USER"),
    password=env("SNOWFLAKE_PASSWORD"),
    # Bounded so a half-open connection or a warehouse that will not resume
    # fails the task instead of hanging it. Without these the only backstop is
    # the server-side statement timeout, which defaults to two days.
    login_timeout=60,
    network_timeout=300,
    warehouse=env("SNOWFLAKE_WAREHOUSE"),
    database=env("SNOWFLAKE_DATABASE"),
    schema=env("SNOWFLAKE_SCHEMA"),
)
# Optional, unlike everything above: unset means the user's default role.
if (os.getenv("SNOWFLAKE_ROLE") or "").strip():
    connect_args["role"] = os.getenv("SNOWFLAKE_ROLE").strip()
conn = snowflake.connector.connect(**connect_args)
cur = conn.cursor()

failed = False
for i, statement in enumerate(statements, start=1):
    preview = redact(statement.splitlines()[0])[:80]
    print(f"[{i}/{len(statements)}] {preview}")
    try:
        cur.execute(statement)
        if statement.upper().startswith("COPY"):
            print_copy_summary(cur)
        elif statement.upper().startswith("SELECT"):
            for row in cur.fetchall():
                print("   ", row)
    except Exception as e:
        # The exception text is redacted too. Snowflake does not echo the
        # failing SQL in its error message today, but that is a property of the
        # server's error format rather than a guarantee, and this statement can
        # carry AWS credentials for CREATE STAGE.
        print(f"\nFailed on statement {i}:\n{redact(statement)}\n{redact(str(e))}")
        failed = True
        if STOP_ON_FIRST_FAILURE:
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
