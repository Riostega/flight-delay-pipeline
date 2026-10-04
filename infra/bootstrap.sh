#!/usr/bin/env bash
#
# Turn a bare Ubuntu host into a working pipeline host.
#
#   provision_ec2.py --launch     creates the instance
#   git clone <repo> && cd it
#   copy .env across               the one thing this cannot create, then
#                                  DELETE its AWS_ACCESS_KEY_ID and
#                                  AWS_SECRET_ACCESS_KEY lines (see preflight)
#   bash infra/bootstrap.sh        everything else
#
# Before this existed, that middle section was typed by hand and lived only on
# the running machine. The data was recoverable from S3 and the warehouse was
# rebuildable with dbt, but the HOST was not reproducible from the repository —
# so "the pipeline is recoverable" was only true for half of it.
#
# Safe to re-run. Every step checks whether it has already been done, so this
# doubles as a repair tool when one piece of a host has drifted. A re-run
# restarts Airflow only if its unit file or airflow.cfg actually changed, and
# never during the 09:00 UTC flights run. It exits non-zero, and does not say
# "Host is ready", if any check at the end fails.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="$(id -un)"
PIPELINE_VENV="${HOME}/pipeline-venv"
AIRFLOW_VENV="${HOME}/airflow-venv"
export AIRFLOW_HOME="${AIRFLOW_HOME:-${HOME}/airflow}"
AIRFLOW_CFG="${AIRFLOW_HOME}/airflow.cfg"
PIPELINE_DAGS=(flight_pipeline_daily weather_hourly)

# Pinned deliberately. Airflow's own constraints file is published per version,
# so the version and the constraints URL must move together.
AIRFLOW_VERSION="3.3.1"

say() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

# Same window the watchdog refuses to restart in (FLIGHTS_WINDOW_UTC in
# pipeline/watchdog.py). Restarting Airflow kills any running task, and a lost
# flights pull cannot be re-fetched: the API quota is spent and there is no
# backfill.
in_flights_window() {
    local now
    now=$(date -u +%H%M)
    (( 10#$now >= 850 && 10#$now <= 930 ))
}

# Prints "<dag_id> <is_paused>" for each pipeline DAG Airflow has registered.
# Airflow prints warnings on stdout ahead of the JSON, so only the line that
# starts with "[" is parsed.
pipeline_dags() {
    "${AIRFLOW_VENV}/bin/airflow" dags list -o json 2>/dev/null \
        | "${AIRFLOW_VENV}/bin/python" -c '
import json, sys
wanted = sys.argv[1:]
rows = next((json.loads(line) for line in sys.stdin if line.startswith("[")), [])
# The list can repeat a DAG (one row per stored version), so keep the first row
# per dag_id. Counting rows would let "10/2 registered" pass with a DAG missing.
seen = set()
for r in rows:
    if r["dag_id"] in wanted and r["dag_id"] not in seen:
        seen.add(r["dag_id"])
        print(r["dag_id"], r["is_paused"])
' "${PIPELINE_DAGS[@]}" || true
}

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
# scp keeps the laptop file's mode, which is often 0644. Only this user needs it.
chmod 600 "${REPO_ROOT}/.env"

# The host gets S3 access from its IAM instance role, so it must hold no AWS
# keys. extract_pipeline.py passes AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY to
# boto3 explicitly, and explicit keys win over the role: a straight copy of the
# laptop .env would put long-lived keys on an internet-facing box and quietly
# stop the role being used. A BLANK line is no better — boto3 then signs with
# empty keys and every upload fails. So the lines must be gone entirely.
# Only whether the lines exist is checked; their values are never printed.
if grep -Eq '^[[:space:]]*(export[[:space:]]+)?AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)[[:space:]]*=' "${REPO_ROOT}/.env"; then
    echo "  .env contains AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY lines." >&2
    echo "  The host uses its IAM instance role; keys in .env would override it." >&2
    echo "  Delete both lines, then re-run:" >&2
    echo "    sed -i -E '/^[[:space:]]*(export[[:space:]]+)?AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)[[:space:]]*=/d' ${REPO_ROOT}/.env" >&2
    exit 1
fi

command -v python3 >/dev/null || { echo "  python3 missing" >&2; exit 1; }

# This script installs units with sudo, and the watchdog restarts a dead
# Airflow with `sudo -n systemctl restart airflow.service`. Both rely on the
# password-free sudo the Ubuntu cloud image gives its default user.
if ! sudo -n true 2>/dev/null; then
    echo "  ${RUN_USER} needs password-free sudo (the Ubuntu cloud image's default user has it)." >&2
    exit 1
fi

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

# Set when Airflow has to be restarted for a change made below to take effect.
# The need is also written to a marker file the moment it is found, and only
# cleared once a restart has actually happened. Otherwise a restart postponed
# for the flights window, or lost to a failure later in this script, would be
# forgotten: on the next run the files already match, nothing looks changed,
# and the host keeps running the old config while this script says it is ready.
restart_airflow=0
RESTART_MARKER="${HOME}/.airflow_restart_pending"
need_restart() {
    restart_airflow=1
    touch "${RESTART_MARKER}"
}
if [[ -f "${RESTART_MARKER}" ]]; then
    echo "  an earlier run left an Airflow restart pending"
    restart_airflow=1
fi

if [[ ! -x "${AIRFLOW_VENV}/bin/python" ]]; then
    python3 -m venv "${AIRFLOW_VENV}"
    echo "  created ${AIRFLOW_VENV}"
fi

# Compare the INSTALLED version with the pinned one, rather than only checking
# that the venv exists. Otherwise bumping AIRFLOW_VERSION above and re-running
# prints "already exists" and keeps the old Airflow.
installed=$("${AIRFLOW_VENV}/bin/pip" show apache-airflow 2>/dev/null | awk '/^Version:/{print $2}' || true)
if [[ "${installed}" == "${AIRFLOW_VERSION}" ]]; then
    echo "  Airflow ${AIRFLOW_VERSION} already installed"
else
    if systemctl is-active --quiet airflow.service; then
        # Swapping packages under a running Airflow can break the task that is
        # running, so stop it first. It is started again further down.
        if in_flights_window; then
            echo "  Airflow ${installed} needs upgrading to ${AIRFLOW_VERSION}, but the 09:00 UTC flights" >&2
            echo "  run may be in progress. Re-run after 09:30 UTC." >&2
            exit 1
        fi
        echo "  stopping airflow.service to change Airflow ${installed} -> ${AIRFLOW_VERSION}"
        sudo systemctl stop airflow.service
    fi
    # The constraints file must match the venv's Python, not whatever python3
    # happens to be newest on the box.
    PYTHON_TAG=$("${AIRFLOW_VENV}/bin/python" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
    "${AIRFLOW_VENV}/bin/pip" install --quiet --upgrade pip
    # The constraints file is what makes an Airflow install reproducible. Without
    # it pip resolves whatever is newest and the result differs by install date.
    "${AIRFLOW_VENV}/bin/pip" install --quiet \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_TAG}.txt"
    echo "  installed Airflow ${AIRFLOW_VERSION} (Python ${PYTHON_TAG})"
fi

# ------------------------------------------------------------ airflow.cfg
# Airflow's defaults look for DAGs in $AIRFLOW_HOME/dags (which does not exist
# here) and load dozens of example DAGs. The first host only worked because
# airflow.cfg was edited by hand, so a host rebuilt from the repo would have
# started Airflow with zero pipeline DAGs. These settings go in airflow.cfg
# itself, not in the systemd unit, so that `airflow ...` typed over SSH and the
# CLI calls in this script see the same DAG folder as the service does.
say "Airflow config (${AIRFLOW_CFG})"

if [[ ! -f "${AIRFLOW_CFG}" ]]; then
    # Almost any airflow command writes the default airflow.cfg when none
    # exists. (Not `airflow version`, which is deliberately exempt.)
    "${AIRFLOW_VENV}/bin/airflow" config get-value core dags_folder >/dev/null 2>&1 || true
    [[ -f "${AIRFLOW_CFG}" ]] || { echo "  could not create ${AIRFLOW_CFG}" >&2; exit 1; }
    echo "  created default config"
fi

cfg_before=$(sha256sum "${AIRFLOW_CFG}")
# dags_are_paused_at_creation only applies to a DAG Airflow has never seen, so
# on a fresh host both DAGs start running on their own, while a DAG someone
# deliberately paused on an existing host stays paused.
sed -i -E \
    -e "s|^dags_folder = .*|dags_folder = ${REPO_ROOT}/dags|" \
    -e "s|^load_examples = .*|load_examples = False|" \
    -e "s|^dags_are_paused_at_creation = .*|dags_are_paused_at_creation = False|" \
    "${AIRFLOW_CFG}"
for line in "dags_folder = ${REPO_ROOT}/dags" "load_examples = False" "dags_are_paused_at_creation = False"; do
    if ! grep -qxF "${line}" "${AIRFLOW_CFG}"; then
        echo "  could not set '${line}' in ${AIRFLOW_CFG} — set it under [core] by hand" >&2
        exit 1
    fi
    echo "  ${line}"
done
if [[ "$(sha256sum "${AIRFLOW_CFG}")" != "${cfg_before}" ]]; then
    echo "  airflow.cfg changed"
    need_restart
fi

# --------------------------------------------------------------- dbt profile
# dbt reads ~/.dbt/profiles.yml and cannot read .env, so these credentials exist
# in two places. Generating the profile FROM .env is what stops them drifting:
# a mismatch otherwise shows up as test_snowflake.py passing while dbt debug
# fails, which is a confusing way to learn you edited only one of them.
say "dbt profile (generated from .env, so it cannot drift)"

mkdir -p "${HOME}/.dbt"
"${PIPELINE_VENV}/bin/python" - "$REPO_ROOT" "$HOME" <<'PY'
import json, os, sys, pathlib
import yaml
from dotenv import dotenv_values

repo, home = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
env = dotenv_values(repo / ".env")


def need(key):
    value = (env.get(key) or "").strip()
    if not value:
        sys.exit(f"  {key} is missing from .env — cannot write a dbt profile without it")
    # dbt renders profiles.yml through Jinja before reading it as YAML, so these
    # would be evaluated as a template rather than used as text.
    if any(t in value for t in ("{{", "{%", "{#")):
        sys.exit("  " + key + " contains {{, {% or {#, which dbt would treat as a template. Change it in .env")
    return value


def quoted(key):
    # json.dumps produces a double-quoted string with every " and \ escaped,
    # which is also a valid YAML double-quoted string. Pasting the raw value
    # between quotes is not safe: a " ends the string early, and \n or \t
    # silently turns into a different password that Snowflake then rejects.
    return json.dumps(need(key))


# role: SNOWFLAKE_ROLE from .env, defaulting to the trial account's admin role
# until the least-privilege PIPELINE_ROLE in pipeline/snowflake_setup.sql is
# applied. While it is ACCOUNTADMIN, the host's .env and this file are sensitive
# (see the provision_ec2.py docstring), not just convenient.
role = json.dumps((env.get("SNOWFLAKE_ROLE") or "").strip() or "ACCOUNTADMIN")
profile = f"""flight_delay_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: {quoted('SNOWFLAKE_ACCOUNT')}
      user: {quoted('SNOWFLAKE_USER')}
      password: {quoted('SNOWFLAKE_PASSWORD')}
      role: {role}
      database: {quoted('SNOWFLAKE_DATABASE')}
      warehouse: {quoted('SNOWFLAKE_WAREHOUSE')}
      schema: {quoted('SNOWFLAKE_SCHEMA')}
      threads: 1
"""

# Read it back before writing, so an escaping mistake fails here rather than
# as a Snowflake authentication error later. Nothing is printed on mismatch.
dev = yaml.safe_load(profile)["flight_delay_pipeline"]["outputs"]["dev"]
for field, key in [("account", "SNOWFLAKE_ACCOUNT"), ("user", "SNOWFLAKE_USER"),
                   ("password", "SNOWFLAKE_PASSWORD"), ("database", "SNOWFLAKE_DATABASE"),
                   ("warehouse", "SNOWFLAKE_WAREHOUSE"), ("schema", "SNOWFLAKE_SCHEMA")]:
    if dev[field] != need(key):
        sys.exit(f"  {key} did not survive the round trip through YAML — profile not written")

# Created 0600 from the start (it holds the warehouse password), rather than
# written world-readable and tightened afterwards. fchmod covers a file that
# already exists from an earlier run.
target = home / ".dbt" / "profiles.yml"
fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.fchmod(fd, 0o600)
with os.fdopen(fd, "w") as f:
    f.write(profile)
print(f"  wrote {target}")
PY

# ------------------------------------------------------------ systemd units
# The units are templated on /home/ubuntu. Substituting the real user and repo
# path means a host that clones somewhere else still works, instead of starting
# a service that points at a directory which does not exist.
#
# Each unit is rendered to a temp file and compared with the installed copy, so
# a re-run knows whether anything changed. `systemctl enable --now` does not
# restart a service that is already running, so without this a corrected unit
# would sit on disk unused while the script reported success.
say "systemd units"

changed_units=()
for unit in airflow.service pipeline-watchdog.service pipeline-watchdog.timer; do
    rendered=$(mktemp)
    sed -e "s|/home/ubuntu/Flight_Delay_Pipeline|${REPO_ROOT}|g" \
        -e "s|/home/ubuntu|${HOME}|g" \
        -e "s|^User=ubuntu$|User=${RUN_USER}|" \
        -e "s|^Group=ubuntu$|Group=${RUN_USER}|" \
        "${REPO_ROOT}/infra/systemd/${unit}" > "${rendered}"
    if sudo cmp -s "${rendered}" "/etc/systemd/system/${unit}"; then
        echo "  ${unit} unchanged"
    else
        sudo install -m 0644 "${rendered}" "/etc/systemd/system/${unit}"
        changed_units+=("${unit}")
        echo "  installed ${unit}"
    fi
    rm -f "${rendered}"
done

sudo systemctl daemon-reload

if [[ " ${changed_units[*]} " == *" airflow.service "* ]]; then
    need_restart
fi

# ------------------------------------------------------------------ start up
say "Starting services"

restart_pending=0
if [[ ${restart_airflow} -eq 1 ]] && systemctl is-active --quiet airflow.service; then
    if in_flights_window; then
        echo "  airflow.service needs a restart for the changes above, but the 09:00 UTC" >&2
        echo "  flights run may be in progress. Re-run this script after 09:30 UTC." >&2
        restart_pending=1
    else
        echo "  restarting airflow.service so the changes above take effect"
        sudo systemctl restart airflow.service
    fi
fi
# Starts whatever is not running yet (a fresh host, or Airflow stopped for an
# upgrade above); a no-op for a unit that is already up.
sudo systemctl enable --now airflow.service
# Airflow is now running with the current config (restarted, or freshly started),
# unless the restart was postponed above.
if [[ ${restart_pending} -eq 0 ]]; then
    rm -f "${RESTART_MARKER}"
fi
sudo systemctl enable --now pipeline-watchdog.timer
if [[ " ${changed_units[*]} " == *" pipeline-watchdog.timer "* ]]; then
    sudo systemctl restart pipeline-watchdog.timer
fi
# pipeline-watchdog.service is a oneshot the timer starts, so daemon-reload is
# all it needs.

# Airflow builds its metadata database on first start, and the DAG processor
# then has to parse dags/. Wait for the DAGs themselves, not just for the CLI to
# answer, or a slow first start reads as "0/2 registered".
echo "  waiting for Airflow to register the pipeline DAGs (up to 5 minutes)..."
for _ in $(seq 1 30); do
    if [[ "$(pipeline_dags | wc -l)" -ge 2 ]]; then
        break
    fi
    sleep 10
done

# The UI password Airflow generated on first start. It is the only thing
# guarding the UI, so keep it private to this user.
chmod 600 "${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated" 2>/dev/null || true

# -------------------------------------------------------------------- verify
say "Verifying"

failed=0

for u in airflow.service pipeline-watchdog.timer; do
    state=$(systemctl is-active "${u}" || true)
    printf '  %-26s%s\n' "${u}:" "${state}"
    [[ "${state}" == "active" ]] || failed=1
done

dags=$(pipeline_dags)
registered=$(printf '%s' "${dags}" | grep -c . || true)
echo "  DAGs registered:          ${registered}/2"
[[ "${registered}" -ge 2 ]] || failed=1
# A paused DAG never runs. Reported rather than unpaused, because on an existing
# host someone may have paused it on purpose.
while read -r dag paused; do
    if [[ "${paused}" == "True" ]]; then
        echo "  ${dag} is PAUSED — unpause it in the UI or with: airflow dags unpause ${dag}"
    fi
done <<< "${dags}"

if [[ ${restart_pending} -eq 1 ]]; then
    echo "  airflow.service restart:  PENDING — re-run after 09:30 UTC"
    failed=1
fi

if (cd "${REPO_ROOT}/dbt" && "${PIPELINE_VENV}/bin/dbt" debug >/dev/null 2>&1); then
    echo "  dbt connection:           OK"
else
    echo "  dbt connection:           FAILED — check the Snowflake values in .env"
    failed=1
fi

cat <<EOF

  Airflow UI       ssh -L 8080:localhost:8080 ${RUN_USER}@<this-host>, then http://localhost:8080
  admin password   ${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated

If the warehouse is also new, populate it before the first dbt run. Do that
from the LAPTOP: snowflake_setup.sql needs AWS keys to create the S3 stages,
and this host deliberately has none.

  python3 pipeline/run_snowflake_setup.py
  python3 pipeline/run_snowflake_setup.py snowflake_load.sql
  cd dbt && dbt seed && dbt build
EOF

if [[ ${failed} -ne 0 ]]; then
    echo
    echo "Host is NOT ready. Fix the lines above and re-run (journalctl -u airflow.service for Airflow)." >&2
    exit 1
fi

echo
echo "Host is ready."
