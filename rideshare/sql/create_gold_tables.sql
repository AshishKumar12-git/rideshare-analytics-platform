CREATE TABLE IF NOT EXISTS rideshare.gold.driver_availability
(
    city STRING,
    status STRING,
    driver_count BIGINT
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/driver_availability/';


CREATE TABLE IF NOT EXISTS rideshare.gold.driver_utilization
(
    city STRING,
    available BIGINT,
    busy BIGINT,
    offline BIGINT,
    driver_utilization DOUBLE
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/driver_utilization/';


CREATE TABLE IF NOT EXISTS rideshare.gold.ride_demand
(
    pickup_city STRING,
    request_status STRING,
    ride_count BIGINT
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/ride_demand/';


CREATE TABLE IF NOT EXISTS rideshare.gold.supply_demand
(
    city STRING,
    available_drivers BIGINT,
    waiting_requests BIGINT,
    supply_ratio DOUBLE
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/supply_demand/';


CREATE TABLE IF NOT EXISTS rideshare.gold.dim_driver
(
    driver_key BIGINT GENERATED ALWAYS AS IDENTITY,
    driver_id INT,
    driver_name STRING,
    city STRING,
    vehicle_type STRING,
    join_date DATE,
    updated_at TIMESTAMP,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current STRING
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/dim_driver';

CREATE TABLE IF NOT EXISTS rideshare.gold.dim_rider
(
    rider_key BIGINT GENERATED ALWAYS AS IDENTITY,

    rider_id INT,
    rider_name STRING,
    city STRING,
    signup_date DATE,
    updated_at TIMESTAMP,

    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current STRING
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/dim_rider';

CREATE TABLE IF NOT EXISTS rideshare.gold.dim_date
(
    date_key INT,

    full_date DATE,

    day INT,
    month INT,
    month_name STRING,

    quarter INT,

    year INT,

    week_of_year INT,

    day_of_week INT,
    day_name STRING,

    is_weekend STRING,

    is_month_end STRING
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/dim_date/';


CREATE TABLE IF NOT EXISTS rideshare.gold.fact_trip
(
    trip_id BIGINT,

    driver_key BIGINT,
    rider_key BIGINT,
    date_key INT,

    pickup_city STRING,
    drop_city STRING,

    distance_km DOUBLE,
    trip_duration_minutes DOUBLE,

    base_fare DOUBLE,
    surge_multiplier DOUBLE,
    final_fare DOUBLE,

    trip_rating DOUBLE,

    trip_status STRING,

    payment_method STRING,
    payment_status STRING,
    gold_ingested_date TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@ridesharedevstorage.dfs.core.windows.net/fact_trip/';
