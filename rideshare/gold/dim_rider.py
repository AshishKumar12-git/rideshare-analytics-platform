from rideshare.common import config
from pyspark.sql import functions as F
from delta.tables import DeltaTable


gold_table = DeltaTable.forName(spark, "rideshare.gold.dim_rider")


riders = spark.table("rideshare.silver.riders")


initial_load = spark.table("rideshare.gold.dim_rider").limit(1).count() == 0

if initial_load:

    initial_rider_df = (
        riders.select(
            "rider_id",
            "rider_name",
            "city",
            "signup_date",
            "updated_at"
        )
        .withColumn(
            "effective_from",
            F.col("updated_at")
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            F.lit("Y")
        )
    )

    print("Initial Load Count :", initial_rider_df.count())

    display(initial_rider_df)

    (
        initial_rider_df
        .writeTo("rideshare.gold.dim_rider")
        .append()
    )

else:

    last_processed_silver_timestamp = "2026-07-12 12:00:00"

    incremental_riders_df = (
        riders
        .filter(
            F.col("silver_ingested_date") >
            F.lit(last_processed_silver_timestamp)
        )
    )

    current_riders_df = (
        spark.table("rideshare.gold.dim_rider")
        .filter(
            F.col("is_current") == "Y"
        )
        .select(
            "rider_key",
            "rider_id",
            "rider_name",
            "city",
            "signup_date",
            "updated_at",
            "effective_from",
            "effective_to",
            "is_current"
        )
    )

    riders_compare_df = (
        incremental_riders_df.alias("silver")
        .join(
            current_riders_df.alias("gold"),
            on="rider_id",
            how="left"
        )
    )

    display(riders_compare_df)

    new_rider_df = (
        riders_compare_df
        .filter(
            F.col("gold.rider_key").isNull()
        )
    )

    changed_rider_df = (
        riders_compare_df
        .filter(
            F.col("gold.rider_key").isNotNull()
            &
            (
                (F.col("silver.rider_name") != F.col("gold.rider_name"))
                |
                (F.col("silver.city") != F.col("gold.city"))
            )
        )
    )

    unchanged_rider_df = (
        riders_compare_df
        .filter(
            F.col("gold.rider_key").isNotNull()
            &
            (F.col("silver.rider_name") == F.col("gold.rider_name"))
            &
            (F.col("silver.city") == F.col("gold.city"))
        )
    )

    expire_df = (
        changed_rider_df
        .select(
            F.col("silver.rider_id").alias("rider_id"),
            F.col("silver.updated_at").alias("updated_at")
        )
    )

    (
        gold_table.alias("gold")
        .merge(
            expire_df.alias("updates"),
            "gold.rider_id = updates.rider_id AND gold.is_current = 'Y'"
        )
        .whenMatchedUpdate(
            set={
                "effective_to": "updates.updated_at",
                "is_current": "'N'"
            }
        )
        .execute()
    )

    changed_insert_df = (
        changed_rider_df
        .select(
            F.col("silver.rider_id").alias("rider_id"),
            F.col("silver.rider_name").alias("rider_name"),
            F.col("silver.city").alias("city"),
            F.col("silver.signup_date").alias("signup_date"),
            F.col("silver.updated_at").alias("updated_at")
        )
        .withColumn(
            "effective_from",
            F.col("updated_at")
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            F.lit("Y")
        )
    )

    new_insert_df = (
        new_rider_df
        .select(
            F.col("silver.rider_id").alias("rider_id"),
            F.col("silver.rider_name").alias("rider_name"),
            F.col("silver.city").alias("city"),
            F.col("silver.signup_date").alias("signup_date"),
            F.col("silver.updated_at").alias("updated_at")
        )
        .withColumn(
            "effective_from",
            F.col("updated_at")
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            F.lit("Y")
        )
    )

    final_insert_df = (
        changed_insert_df
        .unionByName(new_insert_df)
    )

    (
        final_insert_df
        .writeTo("rideshare.gold.dim_rider")
        .append()
    )