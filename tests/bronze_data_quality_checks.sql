-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Bronze
-- Script: bronze_data_quality_checks.sql
-- Purpose: Run basic data quality checks on Bronze layer tables
-- =====================================================

-- -----------------------------------------------------
-- 1. Vendor ID Check (No Nulls)
-- -----------------------------------------------------
SELECT DISTINCT VendorID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- -----------------------------------------------------
-- 2. Pickup/Dropoff Timestamps
-- -----------------------------------------------------
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
WHERE lpep_pickup_datetime IS NULL
   OR lpep_dropoff_datetime IS NULL
   OR lpep_dropoff_datetime < lpep_pickup_datetime;

-- -----------------------------------------------------
-- 3. Store and Forward Flag
-- -----------------------------------------------------
SELECT DISTINCT store_and_fwd_flag
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- -----------------------------------------------------
-- 4. RatecodeID
-- -----------------------------------------------------
SELECT DISTINCT RatecodeID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- -----------------------------------------------------
-- 5. Pickup and Dropoff Location IDs
-- -----------------------------------------------------
SELECT DISTINCT PULocationID, DOLocationID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
WHERE DOLocationID IS NULL OR PULocationID IS NULL;

-- -----------------------------------------------------
-- 6. Passenger Count
-- -----------------------------------------------------
SELECT DISTINCT passenger_count
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

SELECT DISTINCT passenger_count
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
WHERE passenger_count <= 0;

-- -----------------------------------------------------
-- 7. Taxi Zone Lookup - Null and Trim Checks
-- -----------------------------------------------------
SELECT DISTINCT LocationID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE LocationID IS NULL;

SELECT DISTINCT LocationID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE TRIM(Borough) != Borough;

SELECT DISTINCT LocationID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE TRIM(Zone) != Zone;

SELECT DISTINCT LocationID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE TRIM(service_zone) != service_zone;
