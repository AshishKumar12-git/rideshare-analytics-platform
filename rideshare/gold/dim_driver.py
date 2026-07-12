from rideshare.common import config
from pyspark.sql import functions as F
from delta.tables import DeltaTable

gold_table = DeltaTable.forName(spark,"rideshare.gold.dim_driver")

drivers = spark.table('rideshare.silver.drivers')
initial_load = spark.table('rideshare.gold.dim_driver').limit(1).count() == 0

if initial_load:
    initial_driver_df = (drivers.select(
        'driver_id',
        'driver_name',
        'city',
        'vehicle_type',
        'join_date',
        'updated_at'
    ).withColumn(
        'effective_from',F.col('updated_at')
    ).withColumn(
        'effective_to',F.lit(None).cast('timestamp')
    ).withColumn('is_current',F.lit('Y'))
    )
    print(initial_driver_df.count())
    display(initial_driver_df)
    (initial_driver_df.writeTo('rideshare.gold.dim_driver').append())
else:
    last_processed_silver_timestamp = "2026-07-12 12:00:00"
    incremental_drivers_df = (drivers
                              .filter(F.col('silver_ingested_date')>F.lit(last_processed_silver_timestamp))
    )
    current_drivers_df = (
        spark.table('rideshare.gold.dim_driver')
        .filter(F.col('is_current') == 'Y')
        .select('driver_key','driver_id','driver_name','city','vehicle_type','join_date','updated_at','effective_from','effective_to','is_current')
    )
    drivers_compare_df = (
        incremental_drivers_df.alias('silver')
        .join(current_drivers_df.alias('gold'), on='driver_id', how='left')
    )
    display(drivers_compare_df)
    new_driver_df = (
        drivers_compare_df.filter(F.col('gold.driver_key').isNull())
    )
    changed_driver_df = (
    drivers_compare_df.filter(
        F.col("gold.driver_key").isNotNull()
        &
        (
            (F.col("silver.driver_name") != F.col("gold.driver_name"))
            |
            (F.col("silver.city") != F.col("gold.city"))
            |
            (F.col("silver.vehicle_type") != F.col("gold.vehicle_type"))
        )
    )
    )
    unchanged_driver_df = (
    drivers_compare_df.filter(
        F.col("gold.driver_key").isNotNull()
        &
        (F.col("silver.driver_name") == F.col("gold.driver_name"))
        &
        (F.col("silver.city") == F.col("gold.city"))
        &
        (F.col("silver.vehicle_type") == F.col("gold.vehicle_type"))
    )
    )
    expire_df = (
    changed_driver_df
    .select(
        F.col("silver.driver_id").alias("driver_id"),
        F.col("silver.updated_at").alias("updated_at")
    )
    )
    (
    gold_table.alias("gold")
    .merge(
        expire_df.alias("updates"),
        "gold.driver_id = updates.driver_id AND gold.is_current='Y'"
    )
    .whenMatchedUpdate(
        set={
            "effective_to":"updates.updated_at",
            "is_current":"'N'"
        }
    )
    .execute()
    )
    changed_insert_df = (
    changed_driver_df
    .select(
        F.col("silver.driver_id").alias("driver_id"),
        F.col("silver.driver_name").alias("driver_name"),
        F.col("silver.city").alias("city"),
        F.col("silver.vehicle_type").alias("vehicle_type"),
        F.col("silver.join_date").alias("join_date"),
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
    new_driver_df
    .select(
        F.col("silver.driver_id").alias("driver_id"),
        F.col("silver.driver_name").alias("driver_name"),
        F.col("silver.city").alias("city"),
        F.col("silver.vehicle_type").alias("vehicle_type"),
        F.col("silver.join_date").alias("join_date"),
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
    .writeTo("rideshare.gold.dim_driver")
    .append()
    )   