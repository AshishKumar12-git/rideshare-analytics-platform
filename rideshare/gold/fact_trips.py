from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


gold_table = DeltaTable.forName(spark, "rideshare.gold.fact_trip")

initial_load = (
    spark.table("rideshare.gold.fact_trip")
    .limit(1)
    .count() == 0
)



dim_driver = (
    spark.table("rideshare.gold.dim_driver")
    .select(
        "driver_key",
        "driver_id",
        "effective_from",
        "effective_to"
    )
)

dim_rider = (
    spark.table("rideshare.gold.dim_rider")
    .select(
        "rider_key",
        "rider_id",
        "effective_from",
        "effective_to"
    )
)

dim_date = (
    spark.table("rideshare.gold.dim_date")
    .select(
        "date_key",
        "full_date"
    )
)


payments = (
    spark.table("rideshare.silver.payments")
    .select(
        "trip_id",
        "payment_method",
        "payment_status"
    )
)


if initial_load:

    trips = spark.table("rideshare.silver.trips")

else:

    last_processed_silver_timestamp = "2026-07-12 12:00:00"

    trips = (
        spark.table("rideshare.silver.trips")
        .filter(
            F.col("silver_ingested_date")
            > F.lit(last_processed_silver_timestamp)
        )
    )



window_spec = (
    Window.partitionBy("trip_id")
    .orderBy(F.col("updated_at").desc())
)

trips = (
    trips
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)


trip_payment_df = (
    trips.alias("t")
    .join(
        payments.alias("p"),
        on="trip_id",
        how="left"
    )
)


driver_df = (
    trip_payment_df.alias("t")
    .join(
        dim_driver.alias("d"),
        (
            (F.col("t.driver_id") == F.col("d.driver_id"))
            &
            (F.col("t.trip_start_time") >= F.col("d.effective_from"))
            &
            (
                (
                    F.col("t.trip_start_time")
                    < F.col("d.effective_to")
                )
                |
                (F.col("d.effective_to").isNull())
            )
        ),
        "left"
    )
)


rider_df = (
    driver_df.alias("t")
    .join(
        dim_rider.alias("r"),
        (
            (F.col("t.rider_id") == F.col("r.rider_id"))
            &
            (F.col("t.trip_start_time") >= F.col("r.effective_from"))
            &
            (
                (
                    F.col("t.trip_start_time")
                    < F.col("r.effective_to")
                )
                |
                (F.col("r.effective_to").isNull())
            )
        ),
        "left"
    )
)


fact_trip_df = (
    rider_df.alias("t")
    .join(
        dim_date.alias("dt"),
        F.to_date(F.col("t.trip_start_time"))
        == F.col("dt.full_date"),
        "left"
    )
)


fact_trip_df = (
    fact_trip_df
    .select(

        F.col("trip_id"),

        F.col("driver_key").alias("driver_key"),

        F.col("rider_key").alias("rider_key"),

        F.col("date_key").alias("date_key"),

        F.col("pickup_city"),

        F.col("drop_city"),

        F.col("distance_km"),

        (
            (
                F.unix_timestamp("trip_end_time")
                -
                F.unix_timestamp("trip_start_time")
            ) / 60.0
        ).alias("trip_duration_minutes"),

        F.col("base_fare"),

        F.col("surge_multiplier"),

        F.col("final_fare"),

        F.col("trip_rating"),

        F.col("trip_status"),

        F.col("payment_method"),

        F.col("payment_status"),

        F.current_timestamp().alias("gold_ingested_date")
    )
)


print(f"Fact Trip Count : {fact_trip_df.count()}")

display(fact_trip_df)


if initial_load:

    (
        fact_trip_df
        .writeTo("rideshare.gold.fact_trip")
        .append()
    )

    print("Initial Load Completed")


else:

    (
        gold_table.alias("gold")
        .merge(
            fact_trip_df.alias("updates"),
            "gold.trip_id = updates.trip_id"
        )

        .whenMatchedUpdate(
            set={

                "driver_key": "updates.driver_key",

                "rider_key": "updates.rider_key",

                "date_key": "updates.date_key",

                "pickup_city": "updates.pickup_city",

                "drop_city": "updates.drop_city",

                "distance_km": "updates.distance_km",

                "trip_duration_minutes": "updates.trip_duration_minutes",

                "base_fare": "updates.base_fare",

                "surge_multiplier": "updates.surge_multiplier",

                "final_fare": "updates.final_fare",

                "trip_rating": "updates.trip_rating",

                "trip_status": "updates.trip_status",

                "payment_method": "updates.payment_method",

                "payment_status": "updates.payment_status",

                "gold_ingested_date": "updates.gold_ingested_date"

            }
        )

        .whenNotMatchedInsert(
            values={

                "trip_id": "updates.trip_id",

                "driver_key": "updates.driver_key",

                "rider_key": "updates.rider_key",

                "date_key": "updates.date_key",

                "pickup_city": "updates.pickup_city",

                "drop_city": "updates.drop_city",

                "distance_km": "updates.distance_km",

                "trip_duration_minutes": "updates.trip_duration_minutes",

                "base_fare": "updates.base_fare",

                "surge_multiplier": "updates.surge_multiplier",

                "final_fare": "updates.final_fare",

                "trip_rating": "updates.trip_rating",

                "trip_status": "updates.trip_status",

                "payment_method": "updates.payment_method",

                "payment_status": "updates.payment_status",

                "gold_ingested_date": "updates.gold_ingested_date"

            }
        )

        .execute()
    )

    print("Incremental Load Completed")
