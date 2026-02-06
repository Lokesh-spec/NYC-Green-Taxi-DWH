-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Gold
-- Script: Star Schema Incremental Load
-- Purpose:
--   1. Incrementally load Gold dimensions
--   2. Incrementally MERGE latest trips into fact table
-- =====================================================

-- -----------------------------------------------------
-- Dimension: Locations
-- -----------------------------------------------------
INSERT INTO `glass-chemist-483110-u0.nyc_taxi_gold.dim_locations`
  (
    location_key,
    location_id,
    borough,
    zone,
    service_zone,
    ingestion_ts)
SELECT
  CAST(FARM_FINGERPRINT(CAST(LocationID AS STRING)) AS STRING) AS location_key,
  CAST(LocationID AS INT64) AS location_id,
  Borough AS borough,
  Zone AS zone,
  service_zone,
  ingestion_ts
FROM `glass-chemist-483110-u0.nyc_taxi_silver.taxi_zone_lookup`
WHERE
  ingestion_ts > (
    SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1970-01-01'))
    FROM `glass-chemist-483110-u0.nyc_taxi_gold.dim_locations`
  );

-- -----------------------------------------------------
-- Dimension: Payments
-- -----------------------------------------------------
INSERT INTO `glass-chemist-483110-u0.nyc_taxi_gold.dim_payments`
  (
    payment_key,
    payment_type)
SELECT
  FARM_FINGERPRINT(payment_type) AS payment_key,
  payment_type
FROM
  (
    SELECT DISTINCT payment_type
    FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
  );

-- -----------------------------------------------------
-- Dimension: Date
-- -----------------------------------------------------
INSERT INTO `glass-chemist-483110-u0.nyc_taxi_gold.dim_dates`
  (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend)
WITH
  dates AS (
    SELECT DISTINCT DATE(pickup_datetime) AS full_date
    FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
    WHERE pickup_datetime IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT DATE(dropoff_datetime) AS full_date
    FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
    WHERE dropoff_datetime IS NOT NULL
  )
SELECT
  CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) AS date_key,
  full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  EXTRACT(QUARTER FROM full_date) AS quarter,
  EXTRACT(MONTH FROM full_date) AS month,
  FORMAT_DATE('%B', full_date) AS month_name,
  EXTRACT(WEEK FROM full_date) AS week_of_year,
  EXTRACT(DAY FROM full_date) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
  FORMAT_DATE('%A', full_date) AS day_name,
  EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) AS is_weekend
FROM dates
WHERE
  full_date NOT IN (
    SELECT DISTINCT full_date
    FROM `glass-chemist-483110-u0.nyc_taxi_gold.dim_dates`
  );

-- -----------------------------------------------------
-- Fact: Trips (Latest-record MERGE)
-- -----------------------------------------------------
MERGE INTO `glass-chemist-483110-u0.nyc_taxi_gold.fact_trips` gt
USING (
  WITH
    ranked_trips AS (
      SELECT
        t.trip_id AS trip_key,
        dpu.date_key AS pickup_date_key,
        ddo.date_key AS dropoff_date_key,
        pu.location_key AS pickup_location_key,
        dl.location_key AS dropoff_location_key,
        dp.payment_key AS payment_type_key,
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.total_amount,
        TIMESTAMP_DIFF(
          t.dropoff_datetime,
          t.pickup_datetime,
          SECOND) AS trip_duration_seconds,
        t.ingestion_ts,
        t.ingestion_date,
        ROW_NUMBER()
          OVER (
            PARTITION BY t.trip_id
            ORDER BY t.ingestion_ts DESC
          ) AS rn
      FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata` t
      LEFT JOIN `glass-chemist-483110-u0.nyc_taxi_gold.dim_dates` dpu
        ON dpu.full_date = DATE(t.pickup_datetime)
      LEFT JOIN `glass-chemist-483110-u0.nyc_taxi_gold.dim_dates` ddo
        ON ddo.full_date = DATE(t.dropoff_datetime)
      LEFT JOIN `glass-chemist-483110-u0.nyc_taxi_gold.dim_locations` pu
        ON pu.location_id = t.pickup_location_id
      LEFT JOIN `glass-chemist-483110-u0.nyc_taxi_gold.dim_locations` dl
        ON dl.location_id = t.dropoff_location_id
      LEFT JOIN `glass-chemist-483110-u0.nyc_taxi_gold.dim_payments` dp
        ON dp.payment_type = t.payment_type
      WHERE
        t.ingestion_ts >= (
          SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1970-01-01'))
          FROM `glass-chemist-483110-u0.nyc_taxi_gold.fact_trips`
        )
    )
  SELECT *
  FROM ranked_trips
  WHERE rn = 1
) st
ON
  gt.trip_key = st.trip_key
    WHEN MATCHED THEN UPDATE
SET
  pickup_date_key = st.pickup_date_key,
  dropoff_date_key = st.dropoff_date_key,
  pickup_location_key = st.pickup_location_key,
  dropoff_location_key = st.dropoff_location_key,
  payment_type_key = st.payment_type_key,
  passenger_count = st.passenger_count,
  trip_distance = st.trip_distance,
  fare_amount = st.fare_amount,
  extra = st.extra,
  mta_tax = st.mta_tax,
  tip_amount = st.tip_amount,
  tolls_amount = st.tolls_amount,
  improvement_surcharge = st.improvement_surcharge,
  congestion_surcharge = st.congestion_surcharge,
  total_amount = st.total_amount,
  trip_duration_seconds = st.trip_duration_seconds,
  ingestion_ts = st.ingestion_ts,
  ingestion_date = st.ingestion_date
    WHEN NOT MATCHED
      THEN
        INSERT(
          trip_key,
          pickup_date_key,
          dropoff_date_key,
          pickup_location_key,
          dropoff_location_key,
          payment_type_key,
          passenger_count,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          improvement_surcharge,
          congestion_surcharge,
          total_amount,
          trip_duration_seconds,
          ingestion_ts,
          ingestion_date)
          VALUES(
            trip_key,
            pickup_date_key,
            dropoff_date_key,
            pickup_location_key,
            dropoff_location_key,
            payment_type_key,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            total_amount,
            trip_duration_seconds,
            ingestion_ts,
            ingestion_date);
