CREATE TABLE if not exists rideshare.silver.current_driver_state
(
    event_id BIGINT,
    driver_id BIGINT,
    event_type STRING,
    status STRING,
    city STRING,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    file_name STRING
)
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/current_driver_state/';


DROP TABLE rideshare.silver.current_ride_request_state;

CREATE TABLE if not exists rideshare.silver.current_ride_request_state
(
    request_id BIGINT,
    rider_id BIGINT,
    pickup_city STRING,
    ride_type STRING,
    request_status STRING,
    request_timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    file_name STRING
)
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/current_ride_request_state/';


