-- ============================================
-- Snowflake Setup — Flight Delay Pipeline
-- Creates compute, storage, stages and staging tables. Creates them EMPTY —
-- run snowflake_load.sql afterwards to populate them from S3.
--
-- Run with:  python3 pipeline/run_snowflake_setup.py snowflake_setup.sql
--
-- Needs AWS credentials, because Snowflake reads S3 with its own keys rather
-- than through any IAM role. Run rarely, from a trusted machine (the laptop),
-- as a user that holds ACCOUNTADMIN. The scheduled pipeline uses
-- snowflake_load.sql, which needs no AWS credentials, and can run as the
-- least-privilege PIPELINE_ROLE created in section 6.
--
-- Values in <ANGLE_BRACKETS> are placeholders substituted at runtime from .env
-- by run_snowflake_setup.py. Never hardcode credentials in this file — it is
-- tracked in git.
--
-- Safe to re-run: every statement is idempotent.
-- ============================================

-- Run as ACCOUNTADMIN (or a role with sufficient privileges)
USE ROLE ACCOUNTADMIN;

-- ============================================
-- 1. Compute — the warehouse that executes queries
-- AUTO_SUSPEND/INITIALLY_SUSPENDED keep trial credits from burning while idle.
-- ============================================
CREATE WAREHOUSE IF NOT EXISTS <SNOWFLAKE_WAREHOUSE>
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE <SNOWFLAKE_WAREHOUSE>;

-- ============================================
-- 2. Storage — the database/schema holding stages and staging tables
-- The USE statements make session context explicit rather than relying on
-- Snowflake's implicit switch after CREATE.
-- ============================================
CREATE DATABASE IF NOT EXISTS <SNOWFLAKE_DATABASE>;

USE DATABASE <SNOWFLAKE_DATABASE>;

-- Created, not assumed. Only PUBLIC exists automatically in a new database, so
-- with SNOWFLAKE_SCHEMA set to anything else this script failed here — on the
-- documented trial-rebuild path, which is exactly when it is needed most and
-- .env.example actively invites a different value.
CREATE SCHEMA IF NOT EXISTS <SNOWFLAKE_SCHEMA>;

USE SCHEMA <SNOWFLAKE_SCHEMA>;

-- ============================================
-- 3. File format — tells Snowflake to expect JSON
-- (shared by both flights and weather)
-- ============================================
CREATE OR REPLACE FILE FORMAT flight_pipeline_json_format
  TYPE = JSON;

-- ============================================
-- 4a. Stage — pointer to the flights S3 folder
-- ============================================
CREATE OR REPLACE STAGE raw_flights_stage
  URL = 's3://<S3_BUCKET_NAME>/raw/flights/'
  CREDENTIALS = (AWS_KEY_ID = '<AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT = flight_pipeline_json_format;

-- 5a. Staging table — holds raw flight JSON, one row per file
CREATE TABLE IF NOT EXISTS stg_flights_raw (
  raw_data    VARIANT,
  source_file STRING
);



-- ============================================
-- 4b. Stage — pointer to the weather S3 folder
-- ============================================
CREATE OR REPLACE STAGE raw_weather_stage
  URL = 's3://<S3_BUCKET_NAME>/raw/weather/'
  CREDENTIALS = (AWS_KEY_ID = '<AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT = flight_pipeline_json_format;

-- 5b. Staging table — holds raw weather JSON, one row per file
CREATE TABLE IF NOT EXISTS stg_weather_raw (
  raw_data    VARIANT,
  source_file STRING
);


-- ============================================
-- 6. Least privilege: PIPELINE_ROLE, for everything that runs unattended
--
-- The scheduled load, dbt and the watchdog used to run as ACCOUNTADMIN, so a
-- leaked host password meant full control of the account: dropping databases,
-- creating users, spending credits. This role can do what the pipeline needs
-- and little else:
--   - use the warehouse, the database and the schema
--   - read the two stages and the file format (COPY INTO)
--   - own the tables and views in the schema, so COPY can insert, dbt can
--     create and replace its models and seed, and the watchdog can read them
--
-- Ownership, not just grants, because dbt REPLACES its models on every build,
-- and only an owner can replace an object. FUTURE ownership makes every table
-- or view created later in this schema belong to PIPELINE_ROLE too, whoever
-- creates it, so a rebuild run from the laptop as ACCOUNTADMIN cannot leave
-- objects the pipeline's role can't replace.
--
-- PIPELINE_ROLE is granted to SYSADMIN, so SYSADMIN, and ACCOUNTADMIN above
-- it, inherit everything PIPELINE_ROLE owns. Admin sessions (the laptop, CI)
-- keep working exactly as before.
--
-- These statements come AFTER the file format and stages on purpose: CREATE
-- OR REPLACE drops an object's grants, so re-running this file re-creates the
-- stages and then grants on them again.
--
-- Applying this changes nothing by itself. The pipeline keeps running as its
-- user's default role until .env / profiles.yml say otherwise (see the
-- deploy notes in the README).
-- ============================================
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;

GRANT ROLE PIPELINE_ROLE TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE <SNOWFLAKE_WAREHOUSE> TO ROLE PIPELINE_ROLE;
GRANT USAGE ON DATABASE <SNOWFLAKE_DATABASE> TO ROLE PIPELINE_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA> TO ROLE PIPELINE_ROLE;

GRANT USAGE ON FILE FORMAT flight_pipeline_json_format TO ROLE PIPELINE_ROLE;
GRANT USAGE ON STAGE raw_flights_stage TO ROLE PIPELINE_ROLE;
GRANT USAGE ON STAGE raw_weather_stage TO ROLE PIPELINE_ROLE;

-- Existing objects (the raw tables, and any dbt models and seed already built),
-- then everything created from now on. COPY CURRENT GRANTS keeps any grants
-- other roles already had on them.
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>
  TO ROLE PIPELINE_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>
  TO ROLE PIPELINE_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>
  TO ROLE PIPELINE_ROLE;
GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA <SNOWFLAKE_DATABASE>.<SNOWFLAKE_SCHEMA>
  TO ROLE PIPELINE_ROLE;

-- The user running this file gets the role too, so the laptop can test the
-- pipeline as PIPELINE_ROLE (SNOWFLAKE_ROLE=PIPELINE_ROLE in .env) before the
-- host is switched over.
GRANT ROLE PIPELINE_ROLE TO USER <SNOWFLAKE_USER>;

-- Not run by this file, because it needs a password, and passwords never go
-- in a tracked file: a separate service user for the host and CI, whose
-- default role is PIPELINE_ROLE and which holds nothing else. Without it the
-- host's password still belongs to a user that can switch to ACCOUNTADMIN, so
-- the role alone only limits what the pipeline does by default, not what a
-- leaked password can do. Run once by hand, as ACCOUNTADMIN, in a worksheet:
--
--   CREATE USER IF NOT EXISTS PIPELINE_SVC
--     PASSWORD = '<a new long random password>'
--     DEFAULT_ROLE = PIPELINE_ROLE
--     DEFAULT_WAREHOUSE = <SNOWFLAKE_WAREHOUSE>
--     TYPE = LEGACY_SERVICE
--   GRANT ROLE PIPELINE_ROLE TO USER PIPELINE_SVC
--
-- (TYPE = LEGACY_SERVICE is what lets a non-human user sign in with a
-- password while Snowflake enforces MFA on people. Key-pair auth is the
-- longer-term replacement.)
