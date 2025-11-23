-- Databricks notebook source
-- Bronze: ingest & constraint
-- Ingests raw taxi trip rows from the samples.nyctaxi.trips table and casts columns to the correct types for downstream use.
-- Defines an inline data-quality expectation that drops any row where trip_distance is NULL or ≤ 0, keeping the bronze layer clean.

CREATE LIVE TABLE bronze_trips
(
  CONSTRAINT valid_trip_distance
    EXPECT (trip_distance IS NOT NULL AND trip_distance > 0)
    ON VIOLATION DROP ROW
)
COMMENT 'Bronze: batch taxi trips from samples.nyctaxi.trips (non-streaming)'
AS
SELECT
  CAST(tpep_pickup_datetime AS TIMESTAMP)        AS tpep_pickup_datetime,
  CAST(tpep_dropoff_datetime AS TIMESTAMP)       AS tpep_dropoff_datetime,
  CAST(trip_distance AS DOUBLE)                  AS trip_distance,
  CAST(fare_amount AS DOUBLE)                    AS fare_amount,
  CAST(pickup_zip AS INT)                        AS pickup_zip,
  CAST(dropoff_zip AS INT)                       AS dropoff_zip
FROM samples.nyctaxi.trips;


-- COMMAND ----------

-- Silver: suspicious rides
-- Reads from the live bronze table, computes fare_per_mile and marks rides as suspicious_flag when the ratio exceeds the threshold (100).
-- Keeps derived fields in the silver layer so analysts can quickly filter or investigate high-fare anomalies.

CREATE LIVE TABLE silver_suspicious_rides
COMMENT 'Silver: compute fare_per_mile and flag suspicious rides where fare_per_mile > 100 (example threshold)'
AS
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip,
  CASE
    WHEN trip_distance IS NULL OR trip_distance = 0 THEN NULL
    ELSE fare_amount / trip_distance
  END AS fare_per_mile,
  CASE
    WHEN trip_distance IS NULL OR trip_distance = 0 THEN FALSE
    WHEN (fare_amount / trip_distance) > 100 THEN TRUE
    ELSE FALSE
  END AS suspicious_flag
FROM LIVE.bronze_trips;


-- COMMAND ----------

-- Silver: weekly aggregates
-- Aggregates bronze trips by year and week to produce weekly metrics: number of rides, total fare, and average distance.
-- Provides a compact analytical view used for dashboards and to validate that ingestion matches expected volumes.

CREATE LIVE TABLE silver_weekly_aggregates
COMMENT 'Silver: weekly aggregates - number of rides, total fare, average trip distance'
AS
SELECT
  year(tpep_pickup_datetime) AS year,
  weekofyear(tpep_pickup_datetime) AS week,
  COUNT(*) AS total_rides,
  SUM(fare_amount) AS total_fare,
  AVG(trip_distance) AS avg_trip_distance
FROM LIVE.bronze_trips
GROUP BY year(tpep_pickup_datetime), weekofyear(tpep_pickup_datetime);


-- COMMAND ----------

-- Gold — top-3 fares per day (materialized)
-- Materializes the top-3 highest-fare rides per day by ranking bronze rows (ROW_NUMBER partitioned by date) and storing the top 3.
-- This is the final curated view for reporting / downstream consumers (e.g., “top fares of each day”) and can be validated against bronze.

CREATE MATERIALIZED VIEW gold_top3_fares_per_day
COMMENT 'Gold: top-3 highest fare rides per day with pickup/dropoff and zip info'
AS
SELECT
  date,
  tpep_pickup_datetime AS pickup_ts,
  tpep_dropoff_datetime AS dropoff_ts,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip,
  rn
FROM (
  SELECT
    DATE(tpep_pickup_datetime) AS date,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    pickup_zip,
    dropoff_zip,
    ROW_NUMBER() OVER (PARTITION BY DATE(tpep_pickup_datetime) ORDER BY fare_amount DESC) AS rn
  FROM LIVE.bronze_trips
) sub
WHERE rn <= 3;
