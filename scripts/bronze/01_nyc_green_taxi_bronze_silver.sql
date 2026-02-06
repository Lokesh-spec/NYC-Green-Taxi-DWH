-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Bronze 
-- Purpose:
--   Bronze: raw NYC Green Taxi trip data
--   Bronze: taxi zone lookup reference table
-- =====================================================

-- -----------------------------------------------------
-- Bronze Table: Raw Green Taxi Trips
-- -----------------------------------------------------
CREATE OR REPLACE TABLE `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata` (
  trip_id STRING,
  VendorID INT64,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  RatecodeID FLOAT64,
  PULocationID INT64,
  DOLocationID INT64,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type FLOAT64,
  trip_type FLOAT64,
  congestion_surcharge FLOAT64,
  cbd_congestion_fee FLOAT64,
  ingestion_ts TIMESTAMP
)
OPTIONS (
  description = "Bronze layer table for NYC Green Taxi LPEP trip records"
);

-- -----------------------------------------------------
-- Bronze Table: Taxi Zone Lookup
-- -----------------------------------------------------
CREATE OR REPLACE TABLE `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup` (
  LocationID INT64,
  Borough STRING,
  Zone STRING,
  service_zone STRING,
  ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
  description = "Bronze layer reference table for NYC taxi zones"
);
