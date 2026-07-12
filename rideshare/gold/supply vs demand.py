from rideshare.common import config
from pyspark.sql.functions import *


driver_state_stream = (
    spark.readStream.table(
        f"{config.CATALOG_NAME}.{config.SILVER_SCHEMA}.current_driver_state"
    )
)

def compute_supply_demand(batch_df, batch_id):

    
    driver_state = spark.read.table(
        f"{config.CATALOG_NAME}.{config.SILVER_SCHEMA}.current_driver_state"
    )

    
    ride_state = spark.read.table(
        f"{config.CATALOG_NAME}.{config.SILVER_SCHEMA}.current_ride_request_state"
    )

    
    driver_supply = (
        driver_state
            .groupBy("city")
            .agg(
                sum(
                    when(col("status") == "available", 1)
                    .otherwise(0)
                ).alias("available_drivers")
            )
    )

    
    ride_demand = (
        ride_state
            .groupBy("pickup_city")
            .agg(
                sum(
                    when(col("request_status") == "requested", 1)
                    .otherwise(0)
                ).alias("waiting_requests")
            )
    )

    
    supply_demand = (
        driver_supply.alias("d")
            .join(
                ride_demand.alias("r"),
                col("d.city") == col("r.pickup_city"),
                "full"
            )
            .select(
                coalesce(col("d.city"), col("r.pickup_city")).alias("city"),
                coalesce(col("available_drivers"), lit(0)).alias("available_drivers"),
                coalesce(col("waiting_requests"), lit(0)).alias("waiting_requests")
            )
            .withColumn(
                "supply_ratio",
                when(
                    col("waiting_requests") == 0,
                    lit(0.0)
                ).otherwise(
                    col("available_drivers") / col("waiting_requests")
                )
            )
    )

    (
        supply_demand.write
            .format("delta")
            .mode("overwrite")
            .save(config.GOLD_SUPPLY_DEMAND_PATH)
    )

supply_query = (
    driver_state_stream.writeStream
        .foreachBatch(compute_supply_demand)
        .option(
            "checkpointLocation",
            config.GOLD_SUPPLY_DEMAND_CHECKPOINT
        )
        .trigger(processingTime="30 seconds")
        .start()
)

supply_query.awaitTermination()