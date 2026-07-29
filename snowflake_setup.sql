-- ============================================
-- Snowflake Staging Setup — Flight Delay Pipeline
-- Loads raw JSON from S3 into Snowflake staging tables
-- ============================================

-- Run once as ACCOUNTADMIN (or a role with sufficient privileges)
USE ROLE ACCOUNTADMIN;

-- ============================================
-- 1. File format — tells Snowflake to expect JSON
-- (shared by both flights and weather)
-- ============================================
CREATE OR REPLACE FILE FORMAT flight_pipeline_json_format
  TYPE = JSON;

-- ============================================
-- 2a. Stage — connection to the flights S3 folder
-- ============================================
CREATE OR REPLACE STAGE raw_flights_stage
  URL = 's3://flight-delay-pipeline-josh/raw/flights/'
  CREDENTIALS = (AWS_KEY_ID = '<your AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY = '<your AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT = flight_pipeline_json_format;

-- 3a. Staging table — holds raw flight JSON, one row per file
CREATE OR REPLACE TABLE stg_flights_raw (
  raw_data VARIANT
);

-- 4a. Load flight files from S3 into the staging table
COPY INTO stg_flights_raw
  FROM @raw_flights_stage
  FILE_FORMAT = (FORMAT_NAME = flight_pipeline_json_format);

-- ============================================
-- 2b. Stage — connection to the weather S3 folder
-- ============================================
CREATE OR REPLACE STAGE raw_weather_stage
  URL = 's3://flight-delay-pipeline-josh/raw/weather/'
  CREDENTIALS = (AWS_KEY_ID = '<your AWS_ACCESS_KEY_ID>' AWS_SECRET_KEY = '<your AWS_SECRET_ACCESS_KEY>')
  FILE_FORMAT = flight_pipeline_json_format;

-- 3b. Staging table — holds raw weather JSON, one row per file
CREATE OR REPLACE TABLE stg_weather_raw (
  raw_data VARIANT
);

-- 4b. Load weather files from S3 into the staging table
COPY INTO stg_weather_raw
  FROM @raw_weather_stage
  FILE_FORMAT = (FORMAT_NAME = flight_pipeline_json_format);

-- ============================================
-- Verification queries
-- ============================================
SELECT * FROM stg_flights_raw;
SELECT * FROM stg_weather_raw;