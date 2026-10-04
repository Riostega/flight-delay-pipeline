-- ============================================
-- Snowflake Load — Flight Delay Pipeline
-- Copies newly landed S3 files into the staging tables.
--
-- Run with:  python3 pipeline/run_snowflake_setup.py snowflake_load.sql
--
-- Deliberately contains no credentials and no <PLACEHOLDER> tokens. The stages
-- created by snowflake_setup.sql already hold everything needed to reach S3, so
-- this file can run on a host that has no AWS keys at all, which is what lets
-- the EC2 box run the load without AWS credentials. (The box still holds other
-- secrets: the Snowflake login and the API keys in .env and ~/.dbt/profiles.yml,
-- protected by file permissions.)
--
-- No USE ROLE. This used to start with USE ROLE ACCOUNTADMIN, so every hourly
-- load ran with full control of the account. Now the session keeps whatever role
-- it connected with: the user's default role, or SNOWFLAKE_ROLE from .env. That
-- can be the least-privilege PIPELINE_ROLE created in snowflake_setup.sql, which
-- can copy into these two tables and do nothing much else.
--
-- Safe to re-run: COPY INTO tracks load history per table, so already-loaded
-- files are skipped and only new ones are picked up. Both DAGs run this file
-- (weather_hourly every hour, the flights DAG after each pull), sometimes at
-- the same moment; that load history is also what makes that safe.
--
-- Weather first. The runner carries on past a failed statement here and fails
-- at the end, so either COPY failing no longer stops the other.
-- ============================================

COPY INTO stg_weather_raw (raw_data, source_file)
  FROM (
    SELECT $1, metadata$filename
    FROM @raw_weather_stage (FILE_FORMAT => flight_pipeline_json_format)
  );

COPY INTO stg_flights_raw (raw_data, source_file)
  FROM (
    SELECT $1, metadata$filename
    FROM @raw_flights_stage (FILE_FORMAT => flight_pipeline_json_format)
  );

-- Totals across all time, one row per file ever loaded. Not what THIS run
-- loaded; the runner prints that after each COPY.
SELECT 'stg_flights_raw' AS table_name, COUNT(*) AS total_files_ever_loaded FROM stg_flights_raw
UNION ALL
SELECT 'stg_weather_raw', COUNT(*) FROM stg_weather_raw;
