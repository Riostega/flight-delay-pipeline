-- ============================================
-- Snowflake Setup — Flight Delay Pipeline
-- Creates compute, storage, stages and staging tables.
--
-- Run with:  python3 run_snowflake_setup.py snowflake_setup.sql
--
-- Needs AWS credentials, because Snowflake reads S3 with its own keys rather
-- than through any IAM role. Run rarely, from a trusted machine. The daily
-- pipeline uses snowflake_load.sql, which needs no credentials at all.
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
