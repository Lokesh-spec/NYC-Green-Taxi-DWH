-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Silver
-- Purpose: Create cleansed & standardized tables
-- =====================================================

-- -----------------------------------------------------
-- Silver: Green Taxi Trip Data
-- -----------------------------------------------------
CREATE OR REPLACE TABLE `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata` (
  trip_id STRING,

  -- Vendor
  vendor_id INT64,

  -- Timestamps (standardized)
  pickup_datetime DATETIME,
  dropoff_datetime DATETIME,

  -- Flags / codes (normalized)
  store_and_fwd_flag STRING,
  rate_code STRING,

  -- Locations
  pickup_location_id INT64,
  dropoff_location_id INT64,

  -- Passenger info
  passenger_count INT64,
  passenger_count_status STRING,

  -- Trip metrics
  trip_distance FLOAT64,

  -- Fare components
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  congestion_surcharge FLOAT64,
  cbd_congestion_fee FLOAT64,
  total_amount FLOAT64,

  -- Trip / payment attributes
  payment_type STRING,
  trip_type STRING,

  -- Technical metadata
  ingestion_ts TIMESTAMP,
  ingestion_date DATE
)
OPTIONS (
  description = "Silver layer table containing cleaned and standardized NYC Green Taxi trip data"
);

-- -----------------------------------------------------
-- Silver: Taxi Zone Lookup
-- -----------------------------------------------------
CREATE OR REPLACE TABLE `glass-chemist-483110-u0.nyc_taxi_silver.taxi_zone_lookup` (
  location_id INT64,
  borough STRING,
  zone STRING,
  service_zone STRING,
  ingestion_ts TIMESTAMP
)
OPTIONS (
  description = "Silver layer taxi zone lookup table for enriching pickup and dropoff locations"
);
