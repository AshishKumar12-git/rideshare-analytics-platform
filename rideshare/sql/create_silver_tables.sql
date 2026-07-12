CREATE TABLE IF NOT EXISTS rideshare.silver.driver_events
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/driver_events/';

CREATE TABLE IF NOT EXISTS rideshare.silver.ride_request_events
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/ride_request_events/';

CREATE TABLE IF NOT EXISTS rideshare.silver.drivers
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/drivers';

CREATE TABLE IF NOT EXISTS rideshare.silver.riders
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/riders';

CREATE TABLE IF NOT EXISTS rideshare.silver.payments
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/payments';

CREATE TABLE IF NOT EXISTS rideshare.silver.trips
USING DELTA
LOCATION 'abfss://silver@ridesharedevstorage.dfs.core.windows.net/trips';