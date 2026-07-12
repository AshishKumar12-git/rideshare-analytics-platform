CREATE TABLE IF NOT EXISTS rideshare.bronze.drivers
USING PARQUET 
LOCATION 'abfss://bronze@ridesharedevstorage.dfs.core.windows.net/drivers';

CREATE TABLE IF NOT EXISTS rideshare.bronze.riders
USING PARQUET
LOCATION 'abfss://bronze@ridesharedevstorage.dfs.core.windows.net/riders';

CREATE TABLE IF NOT EXISTS rideshare.bronze.payments
USING PARQUET
LOCATION 'abfss://bronze@ridesharedevstorage.dfs.core.windows.net/payments';

CREATE TABLE IF NOT EXISTS rideshare.bronze.trips
USING PARQUET
LOCATION 'abfss://bronze@ridesharedevstorage.dfs.core.windows.net/trips';