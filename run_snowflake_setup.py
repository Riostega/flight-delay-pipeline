import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()


def clean_statement(stmt):
    lines = [line for line in stmt.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines).strip()


def load_statements(path):
    with open(path) as f:
        sql_text = f.read()

    sql_text = sql_text.replace("<your AWS_ACCESS_KEY_ID>", os.getenv("AWS_ACCESS_KEY_ID"))
    sql_text = sql_text.replace("<your AWS_SECRET_ACCESS_KEY>", os.getenv("AWS_SECRET_ACCESS_KEY"))

    statements = [clean_statement(s) for s in sql_text.split(";")]
    return [s for s in statements if s]


conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cur = conn.cursor()
statements = load_statements("snowflake_setup.sql")

for i, statement in enumerate(statements, start=1):
    preview = statement.splitlines()[0][:80]
    print(f"[{i}/{len(statements)}] {preview}")
    try:
        cur.execute(statement)
        if statement.strip().upper().startswith("SELECT"):
            for row in cur.fetchall():
                print(row)
    except Exception as e:
        print(f"Failed on statement {i}: {statement}")
        print(e)
        break

cur.close()
conn.close()
