from rideshare.common import config

ride_state = spark.readStream.table(f"{config.CATALOG_NAME}.{config.SILVER_SCHEMA}.current_ride_request_state")

ride_demand = (
    ride_state.groupBy('pickup_city','request_status').count().withColumnRenamed('count','ride_count')
)

ride_query = (
    ride_demand.writeStream.format('delta')
    .outputMode('complete')
    .option('checkpointLocation',config.GOLD_RIDE_DEMAND_CHECKPOINT)
    .option('path',config.GOLD_RIDE_DEMAND_PATH)
    .trigger(processingTime='30 seconds')
    .start()
)
ride_query.awaitTermination()