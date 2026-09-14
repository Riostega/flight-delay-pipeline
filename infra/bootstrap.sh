#!/usr/bin/env bash
#
# Turn a bare Ubuntu host into a working pipeline host.
#
#   provision_ec2.py --launch     creates the instance
#   git clone <repo> && cd it
#   cp /somewhere/.env .           the one thing this cannot create
#   bash infra/bootstrap.sh        everything else
#
# Before this existed, that middle section was typed by hand and lived only on
# the running machine. The data was recoverable from S3 and the warehouse was
# rebuildable with dbt, but the HOST was not reproducible from the repository —
# so "the pipeline is recoverable" was only true for half of it.
#
# Safe to re-run. Every step checks whether it has already been done, so this
# doubles as a repair tool when one piece of a host has drifted.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="$(id -un)"
PIPELINE_VENV="${HOME}/pipeline-venv"
AIRFLOW_VENV="${HOME}/airflow-venv"
export AIRFLOW_HOME="${AIRFLOW_HOME:-${HOME}/airflow}"

# Pinned deliberately. Airflow's own constraints file is published per version,
# so the version and the constraints URL must move together.
AIRFLOW_VERSION="3.3.1"
PYTHON_TAG="3.12"

say() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

# ---------------------------------------------------------------- preflight
say "Checking prerequisites"

if [[ ! -f "${REPO_ROOT}/requirements.txt" ]]; then
    echo "  requirements.txt not found — run this from inside the repository." >&2
    exit 1
fi

# .env holds every credential and is gitignored, so a fresh clone will not have
# it. Failing here with a clear message beats failing later inside dbt with an
# authentication error that looks like a Snowflake problem.
if [[ ! -f "${REPO_ROOT}/.env" ]]; then
    echo "  .env not found at ${REPO_ROOT}/.env" >&2
    echo "  Copy it across before running this. See .env.example for the keys." >&2
    exit 1
fi

command -v python3 >/dev/null || { echo "  python3 missing" >&2; exit 1; }
echo "  repo:    ${REPO_ROOT}"
echo "  user:    ${RUN_USER}"
echo "  python:  $(python3 --version)"

# ------------------------------------------------------- the pipeline venv
# Two virtualenvs, not one. Airflow and dbt pin conflicting versions of jinja2,
# pydantic and requests, so installing them together breaks one of them. The
# DAGs shell out to this interpreter rather than importing the project, which is
# why dags/ can reference a package Airflow's own venv never installs.
say "Pipeline virtualenv (extract, dbt, watchdog)"

if [[ -x "${PIPELINE_VENV}/bin/python" ]]; then
    echo "  already exists at ${PIPELINE_VENV}"
else
    python3 -m venv "${PIPELINE_VENV}"
    echo "  created ${PIPELINE_VENV}"
fi
"${PIPELINE_VENV}/bin/pip" install --quiet --upgrade pip
"${PIPELINE_VENV}/bin/pip" install --quiet -r "${REPO_ROOT}/requirements.txt"
echo "  dependencies installed"

# --------------------------------------------------------- the airflow venv
say "Airflow virtualenv"

if [[ -x "${AIRFLOW_VENV}/bin/airflow" ]]; then
    echo "  already exists at ${AIRFLOW_VENV}"
else
    python3 -m venv "${AIRFLOW_VENV}"
    "${AIRFLOW_VENV}/bin/pip" install --quiet --upgrade pip
    # The constraints file is what makes an Airflow install reproducible. Without
    # it pip resolves whatever is newest and the result differs by install date.
    "${AIRFLOW_VENV}/bin/pip" install --quiet \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_TAG}.txt"
    echo "  installed Airflow ${AIRFLOW_VERSION}"
fi

# --------------------------------------------------------------- dbt profile
# dbt reads ~/.dbt/profiles.yml and cannot read .env, so these credentials exist
# in two places. Generating the profile FROM .env is what stops them drifting:
# a mismatch otherwise shows up as test_snowflake.py passing while dbt debug
# fails, which is a confusing way to learn you edited only one of them.
say "dbt profile (generated from .env, so it cannot drift)"

mkdir -p "${HOME}/.dbt"
"${PIPELINE_VENV}/bin/python" - "$REPO_ROOT" "$HOME" <<'PY'
import sys, pathlib
from dotenv import dotenv_values

repo, home = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
env = dotenv_values(repo / ".env")


def need(key):
    value = (env.get(key) or "").strip()
    if not value:
        sys.exit(f"  {key} is missing from .env — cannot write a dbt profile without it")
    return value


# Quoted, and the password is never interpolated bare. A value starting with *
# or & is a YAML anchor, one containing ': ' fails to parse, and an all-digit
# password loads as an int rather than a str. Those fail silently.
profile = f"""flight_delay_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{need('SNOWFLAKE_ACCOUNT')}"
      user: "{need('SNOWFLAKE_USER')}"
      password: "{need('SNOWFLAKE_PASSWORD')}"
      role: ACCOUNTADMIN
      database: "{need('SNOWFLAKE_DATABASE')}"
      warehouse: "{need('SNOWFLAKE_WAREHOUSE')}"
      schema: "{need('SNOWFLAKE_SCHEMA')}"
      threads: 1
"""

target = home / ".dbt" / "profiles.yml"
target.write_text(profile)
target.chmod(0o600)          # it holds the warehouse password
print(f"  wrote {target}")
PY

# ------------------------------------------------------------ systemd units
# The units are templated on /home/ubuntu. Substituting the real user and repo
# path means a host that clones somewhere else still works, instead of starting
# a service that points at a directory which does not exist.
say "systemd units"

for unit in airflow.service pipeline-watchdog.service pipeline-watchdog.timer; do
    sed -e "s|/home/ubuntu/Flight_Delay_Pipeline|${REPO_ROOT}|g" \
        -e "s|/home/ubuntu|${HOME}|g" \
        -e "s|^User=ubuntu$|User=${RUN_USER}|" \
        -e "s|^Group=ubuntu$|Group=${RUN_USER}|" \
        "${REPO_ROOT}/infra/systemd/${unit}" \
        | sudo tee "/etc/systemd/system/${unit}" >/dev/null
    echo "  installed ${unit}"
done

sudo systemctl daemon-reload

# ------------------------------------------------------------------ start up
say "Starting services"

sudo systemctl enable --now airflow.service
sudo systemctl enable --now pipeline-watchdog.timer

# Airflow builds its metadata database on first start. Until that finishes the
# DAGs are not registered, so reporting success immediately would be misleading.
echo "  waiting for Airflow to come up..."
for _ in $(seq 1 30); do
    if "${AIRFLOW_VENV}/bin/airflow" dags list >/dev/null 2>&1; then
        break
    fi
    sleep 5
done

# -------------------------------------------------------------------- verify
say "Verifying"

echo "  airflow.service:          $(systemctl is-active airflow.service)"
echo "  pipeline-watchdog.timer:  $(systemctl is-active pipeline-watchdog.timer)"

registered=$("${AIRFLOW_VENV}/bin/airflow" dags list 2>/dev/null | grep -cE 'flight_pipeline_daily|weather_hourly' || true)
echo "  DAGs registered:          ${registered}/2"

if (cd "${REPO_ROOT}/dbt" && "${PIPELINE_VENV}/bin/dbt" debug >/dev/null 2>&1); then
    echo "  dbt connection:           OK"
else
    echo "  dbt connection:           FAILED — check the Snowflake values in .env"
fi

cat <<EOF

Host is ready.

  Airflow UI       ssh -L 8080:localhost:8080 ${RUN_USER}@<this-host>, then http://localhost:8080
  admin password   ${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated

If the warehouse is also new, populate it before the first dbt run:

  ${PIPELINE_VENV}/bin/python pipeline/run_snowflake_setup.py
  ${PIPELINE_VENV}/bin/python pipeline/run_snowflake_setup.py snowflake_load.sql
  cd dbt && ${PIPELINE_VENV}/bin/dbt build
EOF
