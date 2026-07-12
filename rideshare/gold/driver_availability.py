from rideshare.common import config
from pyspark.sql.functions import *
driver_state = spark.readStream.table(f"{config.CATALOG_NAME}.{config.SILVER_SCHEMA}.current_driver_state")

driver_availability = (
    driver_state.groupBy('city','status').count().withColumnRenamed('count','driver_count')
)

driver_query = (
    driver_availability.writeStream.format('delta')
    .outputMode('complete')
    .option('checkpointLocation',config.GOLD_DRIVER_AVAILABILITY_CHECKPOINT)
    .option('path',config.GOLD_DRIVER_AVAILABILITY_PATH)
    .trigger(processingTime='30 seconds')
    .start()
)
driver_query.awaitTermination()