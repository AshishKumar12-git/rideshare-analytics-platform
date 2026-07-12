from rideshare.common import config
from pyspark.sql.functions import *

driver_event = spark.readStream.table(f'{config.CATALOG_NAME}.{config.SILVER_SCHEMA}.current_driver_state')

driver_utilization = (
    driver_event
        .groupBy("city")
        .agg(
            sum(when(col("status") == "available", 1).otherwise(0)).alias("available"),
            sum(when(col("status") == "busy", 1).otherwise(0)).alias("busy"),
            sum(when(col("status") == "offline", 1).otherwise(0)).alias("offline")
        )
        .withColumn(
            "driver_utilization",
            when(
                (col("busy") + col("available")) == 0,
                lit(0.0)
            ).otherwise(
                col("busy") / (col("busy") + col("available"))
            )
        )
)

driver_query = (
    driver_utilization.writeStream.format('delta')
    .outputMode('complete')
    .option('checkpointLocation',config.GOLD_DRIVER_UTILIZATION_CHECKPOINT)
    .option('path',config.GOLD_DRIVER_UTILIZATION_PATH)
    .trigger(processingTime='30 seconds')
    .start()
)
driver_query.awaitTermination()