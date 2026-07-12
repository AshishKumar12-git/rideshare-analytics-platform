from rideshare.common import config
from rideshare.common.schemas import *
from rideshare.common.transformations import (
    add_metadata_columns,
    filter_required_columns,
    convert_timestamp_columns,
    clean_string_columns
)

from pyspark.sql.functions import * 

ride_events = (spark.readStream.format("cloudFiles")
               .option('cloudFiles.format','json')
               .option('multiLine','true')
               .schema(ride_request_schema)
               .load(config.BRONZE_RIDE_REQUEST_PATH)
               )
ride_events = filter_required_columns(ride_events,['request_id','rider_id'])

ride_events = convert_timestamp_columns(ride_events,['request_timestamp','updated_at'])

ride_events = clean_string_columns(ride_events,['ride_type','request_status'],['pickup_city'])

ride_events = add_metadata_columns(ride_events)

ride_events.printSchema()

ride_events_query = (
            ride_events.writeStream.format('delta')
            .outputMode('append')
            .option('checkpointLocation',config.SILVER_RIDE_REQUEST_CHECKPOINT)
            .option('path',config.SILVER_RIDE_REQUEST_PATH)
            .trigger(processingTime="10 seconds")
            .start()
            )

ride_events_query.awaitTermination()