from rideshare.common import config
from rideshare.common.schemas import *
from pyspark.sql.functions import *
from rideshare.common.transformations import (
    add_metadata_columns,
    filter_required_columns,
    convert_timestamp_columns,
    clean_string_columns
)
# Reading the Streaming Files 

driver_events = (spark.readStream.format('cloudFiles')
                .option('cloudFiles.format','json')
                .option("multiLine", "true")
                .schema(driver_event_schema)
                .load(config.BRONZE_DRIVER_EVENT_PATH))

driver_events = filter_required_columns(driver_events,['event_id','driver_id'])

driver_events = convert_timestamp_columns(driver_events,['event_timestamp'])

driver_events = clean_string_columns(driver_events,['event_type','status'],['city'])

driver_events = add_metadata_columns(driver_events)

driver_events.printSchema()

driver_events_query = (
    driver_events.writeStream.format('delta')
    .outputMode('append')
    .option('checkpointLocation',config.SILVER_DRIVER_EVENT_CHECKPOINT)
    .option('path',config.SILVER_DRIVER_EVENT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
    )
driver_events_query.awaitTermination()
# print(driver_events_query.lastProgress)
# display(
#     spark.read.format("delta")
#     .load(config.SILVER_DRIVER_EVENT_PATH)
# )
# display(
#     spark.read
#          .format("json")
#          .option("multiLine", "true")
#          .schema(driver_event_schema)
#          .load(config.BRONZE_DRIVER_EVENT_PATH)
# )