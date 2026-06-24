-- Phase 3 -- Snowflake setup. Run ONCE in a Snowflake worksheet before loading.
CREATE WAREHOUSE IF NOT EXISTS chi_311_wh
  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS chi_311;
CREATE SCHEMA IF NOT EXISTS chi_311.raw;
CREATE SCHEMA IF NOT EXISTS chi_311.analytics;

CREATE TABLE IF NOT EXISTS chi_311.raw.sr_311 (
  sr_number STRING, sr_type STRING, sr_short_code STRING,
  owner_department STRING, status STRING, origin STRING,
  created_ts TIMESTAMP_NTZ, closed_ts TIMESTAMP_NTZ, last_modified_ts TIMESTAMP_NTZ,
  resolution_hours FLOAT, is_closed BOOLEAN, has_valid_resolution BOOLEAN,
  community_area NUMBER, ward NUMBER, zip_code STRING,
  latitude FLOAT, longitude FLOAT, has_valid_geo BOOLEAN,
  city_flagged_duplicate BOOLEAN, legacy_record BOOLEAN,
  created_date_only DATE, created_year NUMBER, created_month NUMBER
);
