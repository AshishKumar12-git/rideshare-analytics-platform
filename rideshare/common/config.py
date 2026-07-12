# Unity Catalog

CATALOG_NAME = 'rideshare'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'

# Storage

STORAGE = 'ridesharedevstorage'

# File Paths

BRONZE_DRIVER_EVENT_PATH = f'abfss://bronze@{STORAGE}.dfs.core.windows.net/driver_status_events/'
BRONZE_RIDE_REQUEST_PATH = f'abfss://bronze@{STORAGE}.dfs.core.windows.net/rider_status_events/'

# Checkpoints

SILVER_DRIVER_EVENT_CHECKPOINT = f'abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/driver_events/'
SILVER_RIDE_REQUEST_CHECKPOINT = f'abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/ride_request_events/'

# Delta Table Paths

SILVER_DRIVER_EVENT_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/driver_events/"
SILVER_RIDE_REQUEST_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/ride_request_events/"


# State File Paths

STATE_DRIVER_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/current_driver_state/"
SILVER_DRIVER_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/drivers/"
STATE_RIDE_REQUEST_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/current_ride_request_state/"

# State Checkpoints Path 

STATE_DRIVER_CHECKPOINT = f"abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/current_driver_state/"

STATE_RIDE_REQUEST_CHECKPOINT = f"abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/current_ride_request_state/"


# Gold Delta Paths 

GOLD_DRIVER_AVAILABILITY_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/driver_availability/"


GOLD_DRIVER_UTILIZATION_PATH =  f"abfss://gold@{STORAGE}.dfs.core.windows.net/driver_utilization/"


GOLD_RIDE_DEMAND_PATH =  f"abfss://gold@{STORAGE}.dfs.core.windows.net/ride_demand/"


GOLD_SUPPLY_DEMAND_PATH =  f"abfss://gold@{STORAGE}.dfs.core.windows.net/supply_demand/"

GOLD_DIM_DRIVER_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/dim_driver"

GOLD_DIM_RIDER_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/dim_rider"

GOLD_DIM_DATE_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/dim_date"

GOLD_FACT_TRIP_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/fact_trip"

# Gold Checkpoint Paths


GOLD_DRIVER_AVAILABILITY_CHECKPOINT = f"abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/driver_availability/"


GOLD_DRIVER_UTILIZATION_CHECKPOINT = f"abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/driver_utilization/"


GOLD_RIDE_DEMAND_CHECKPOINT =  f"abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/ride_demand/"


GOLD_SUPPLY_DEMAND_CHECKPOINT =  f"abfss://managed@{STORAGE}.dfs.core.windows.net/checkpoints/supply_demand/"


# HISTORICAL SILVER PATHS

SILVER_DRIVER_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/drivers/"
SILVER_RIDER_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/riders/"
SILVER_PAYMENT_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/payments/"
SILVER_TRIP_PATH = f"abfss://silver@{STORAGE}.dfs.core.windows.net/trips/"




