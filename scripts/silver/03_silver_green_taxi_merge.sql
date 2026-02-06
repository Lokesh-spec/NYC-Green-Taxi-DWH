-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Silver
-- Script: Green Taxi Incremental Merge
-- Purpose:
--   1. Incrementally MERGE cleaned Green Taxi trip data
--      from Bronze into Silver
--   2. Incrementally load Taxi Zone lookup data
-- =====================================================

-- -----------------------------------------------------
-- Merge Green Taxi Trip Data (Bronze → Silver)
-- -----------------------------------------------------
MERGE INTO `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata` st
USING (
  SELECT
    trip_id,

    -- Vendor
    VendorID AS vendor_id,

    -- Timestamps
    DATETIME(lpep_pickup_datetime) AS pickup_datetime,
    DATETIME(lpep_dropoff_datetime) AS dropoff_datetime,

    -- Store and forward flag (normalized)
    CASE
      WHEN LOWER(store_and_fwd_flag) = 'y' THEN 'Yes'
      WHEN LOWER(store_and_fwd_flag) = 'n' THEN 'No'
      ELSE 'Unknown'
      END AS store_and_fwd_flag,

    -- Rate code (decoded)
    CASE
      WHEN RatecodeID = 1 THEN 'Standard rate'
      WHEN RatecodeID = 2 THEN 'JFK'
      WHEN RatecodeID = 3 THEN 'Newark'
      WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
      WHEN RatecodeID = 5 THEN 'Negotiated fare'
      WHEN RatecodeID = 6 THEN 'Group ride'
      ELSE 'Unknown'
      END AS rate_code,

    -- Locations
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,

    -- Passenger count (cleaned)
    CASE
      WHEN SAFE_CAST(passenger_count AS INT64) IS NULL THEN NULL
      WHEN SAFE_CAST(passenger_count AS INT64) <= 0 THEN NULL
      ELSE SAFE_CAST(passenger_count AS INT64)
      END AS passenger_count,

    -- Passenger count quality flag
    CASE
      WHEN passenger_count IS NULL THEN 'missing'
      WHEN SAFE_CAST(passenger_count AS INT64) IS NULL THEN 'invalid'
      WHEN SAFE_CAST(passenger_count AS INT64) = 0 THEN 'zero_reported'
      WHEN SAFE_CAST(passenger_count AS INT64) BETWEEN 1 AND 6 THEN 'valid'
      ELSE 'out_of_range'
      END AS passenger_count_status,

    -- Trip distance
    CASE
      WHEN trip_distance < 0 THEN NULL
      WHEN trip_distance = 0 THEN 0
      ELSE trip_distance
      END AS trip_distance,

    -- Fare components
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,

    -- Congestion surcharges
    CASE
      WHEN congestion_surcharge < 0 THEN NULL
      ELSE congestion_surcharge
      END
      AS congestion_surcharge,
    CASE
      WHEN cbd_congestion_fee < 0 THEN NULL
      ELSE cbd_congestion_fee
      END
      AS cbd_congestion_fee,

    -- Total amount
    CASE WHEN total_amount < 0 THEN NULL ELSE total_amount END AS total_amount,

    -- Payment type
    CASE
      WHEN payment_type = 1 THEN 'Credit card'
      WHEN payment_type = 2 THEN 'Cash'
      WHEN payment_type = 3 THEN 'No charge'
      WHEN payment_type = 4 THEN 'Dispute'
      ELSE 'Unknown'
      END AS payment_type,

    -- Trip type
    CASE
      WHEN trip_type = 1 THEN 'Street-hail'
      WHEN trip_type = 2 THEN 'Dispatch'
      ELSE 'Unknown'
      END AS trip_type,

    -- Metadata
    ingestion_ts,
    DATE(ingestion_ts) AS ingestion_date
  FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
  WHERE
    NOT (payment_type IN (1, 2) AND total_amount < 0)
    AND lpep_dropoff_datetime >= lpep_pickup_datetime
    AND ingestion_ts >= (
      SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1970-01-01'))
      FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
    )
) bt
ON
  bt.trip_id = st.trip_id
    WHEN MATCHED
      THEN
        UPDATE
SET
  vendor_id = bt.vendor_id,
  pickup_datetime = bt.pickup_datetime,
  dropoff_datetime = bt.dropoff_datetime,
  store_and_fwd_flag = bt.store_and_fwd_flag,
  rate_code = bt.rate_code,
  pickup_location_id = bt.pickup_location_id,
  dropoff_location_id = bt.dropoff_location_id,
  passenger_count = bt.passenger_count,
  passenger_count_status = bt.passenger_count_status,
  trip_distance = bt.trip_distance,
  fare_amount = bt.fare_amount,
  extra = bt.extra,
  mta_tax = bt.mta_tax,
  tip_amount = bt.tip_amount,
  tolls_amount = bt.tolls_amount,
  improvement_surcharge = bt.improvement_surcharge,
  congestion_surcharge = bt.congestion_surcharge,
  cbd_congestion_fee = bt.cbd_congestion_fee,
  total_amount = bt.total_amount,
  payment_type = bt.payment_type,
  trip_type = bt.trip_type,
  ingestion_ts = bt.ingestion_ts,
  ingestion_date = bt.ingestion_date
    WHEN NOT MATCHED
      THEN
        INSERT(
          trip_id,
          vendor_id,
          pickup_datetime,
          dropoff_datetime,
          store_and_fwd_flag,
          rate_code,
          pickup_location_id,
          dropoff_location_id,
          passenger_count,
          passenger_count_status,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          improvement_surcharge,
          congestion_surcharge,
          total_amount,
          payment_type,
          trip_type,
          ingestion_ts,
          ingestion_date)
          VALUES(
            trip_id,
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            store_and_fwd_flag,
            rate_code,
            pickup_location_id,
            dropoff_location_id,
            passenger_count,
            passenger_count_status,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            total_amount,
            payment_type,
            trip_type,
            ingestion_ts,
            ingestion_date);

-- -----------------------------------------------------
-- Incremental Load: Taxi Zone Lookup (Bronze → Silver)
-- -----------------------------------------------------
INSERT INTO `glass-chemist-483110-u0.nyc_taxi_silver.taxi_zone_lookup`
  (
    location_id,
    borough,
    zone,
    service_zone,
    ingestion_ts)
SELECT DISTINCT
  LocationID AS location_id,
  TRIM(Borough) AS borough,
  TRIM(Zone) AS zone,
  TRIM(service_zone) AS service_zone,
  ingestion_ts
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE
  ingestion_ts > (
    SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1970-01-01'))
    FROM `glass-chemist-483110-u0.nyc_taxi_silver.taxi_zone_lookup`
  );
