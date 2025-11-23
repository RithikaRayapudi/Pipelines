-- Databricks notebook source
-- 1) Count rows in Bronze (raw data)
SELECT COUNT(*) AS bronze_count
FROM rithika.default.bronze_trips;

-- COMMAND ----------

-- 2) Count rows in Silver suspicious_rides
SELECT COUNT(*) AS silver_suspicious_count
FROM rithika.default.silver_suspicious_rides;

-- COMMAND ----------

-- 3) Sum of weekly rides (silver_weekly_aggregates)
SELECT SUM(total_rides) AS weekly_total_rides
FROM rithika.default.silver_weekly_aggregates;

-- COMMAND ----------

-- 4) Compare bronze row count vs weekly aggregated total rides
SELECT
  (SELECT COUNT(*) FROM rithika.default.bronze_trips) AS bronze_count,
  (SELECT SUM(total_rides) FROM rithika.default.silver_weekly_aggregates) AS weekly_sum;

-- COMMAND ----------

-- 5) Ensure no invalid trip_distance exists in Bronze
SELECT COUNT(*) AS invalid_trip_distance_rows
FROM rithika.default.bronze_trips
WHERE trip_distance IS NULL OR trip_distance <= 0;

-- COMMAND ----------

-- 6) Validate suspicious_flag logic
SELECT COUNT(*) AS wrong_flag_rows
FROM rithika.default.silver_suspicious_rides
WHERE suspicious_flag = TRUE
  AND (fare_amount / trip_distance) <= 100;

-- COMMAND ----------

-- 7) Check Gold top-3 fares per day consistency
SELECT date, COUNT(*) AS records_per_day
FROM rithika.default.gold_top3_fares_per_day
GROUP BY date
ORDER BY date;