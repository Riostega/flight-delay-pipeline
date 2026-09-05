-- ============================================
-- Snowflake Load — Flight Delay Pipeline
-- Copies newly landed S3 files into the staging tables.
--
-- Run with:  python3 run_snowflake_setup.py snowflake_load.sql
--
-- Deliberately contains no credentials and no <PLACEHOLDER> tokens. The stages
-- created by snowflake_setup.sql already hold everything needed to reach S3, so
-- this file can run on a host that has no AWS keys at all — which is what lets
-- the EC2 box run the daily pipeline while carrying no secrets on disk.
--
-- Safe to re-run: COPY INTO tracks load history per table, so already-loaded
-- files are skipped and only new ones are picked up.
-- ============================================

USE ROLE ACCOUNTADMIN;

COPY INTO stg_flights_raw (raw_data, source_file)
  FROM (
    SELECT $1, metadata$filename
    FROM @raw_flights_stage (FILE_FORMAT => flight_pipeline_json_format)
  );

COPY INTO stg_weather_raw (raw_data, source_file)
  FROM (
    SELECT $1, metadata$filename
    FROM @raw_weather_stage (FILE_FORMAT => flight_pipeline_json_format)
  );

SELECT 'stg_flights_raw' AS table_name, COUNT(*) AS files_loaded FROM stg_flights_raw
UNION ALL
SELECT 'stg_weather_raw', COUNT(*) FROM stg_weather_raw;
