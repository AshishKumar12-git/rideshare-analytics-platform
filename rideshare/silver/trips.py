from rideshare.common import config
from pyspark.sql import functions as F
trips = (
    spark.table('rideshare.bronze.trips').select('trip_id','driver_id','rider_id','pickup_city','drop_city','trip_start_time','trip_end_time','distance_km','base_fare','surge_multiplier','final_fare','trip_rating','trip_status','updated_at','ingestion_date')
)

trips.printSchema()

trips_cast = (
    trips.withColumn('trip_start_time',F.date_trunc("second", F.to_timestamp("trip_start_time")))
    .withColumn('trip_end_time',F.date_trunc("second", F.to_timestamp("trip_end_time")))
    .withColumn('updated_at',F.to_timestamp('updated_at'))
)
trips_trim = (
    trips_cast.withColumn('pickup_city',F.initcap(F.trim(F.col('pickup_city'))))
    .withColumn('drop_city',F.initcap(F.trim(F.col('drop_city'))))
    .withColumn('trip_status',F.initcap(F.trim(F.col('trip_status'))))
)
trip_valid = (
    trips_trim
    .filter(
        F.col("trip_id").isNotNull()
        & F.col("driver_id").isNotNull()
        & F.col("rider_id").isNotNull()
        & F.col("pickup_city").isNotNull()
        & F.col("drop_city").isNotNull()
        & F.col("trip_start_time").isNotNull()
        & F.col("trip_end_time").isNotNull()
        & F.col("distance_km").isNotNull()
        & F.col("base_fare").isNotNull()
        & F.col("surge_multiplier").isNotNull()
        & F.col("final_fare").isNotNull()
        & F.col("trip_rating").isNotNull()
        & F.col("trip_status").isNotNull()
        & F.col("updated_at").isNotNull()
        & F.col("ingestion_date").isNotNull()
    )
)

valid_cities = [
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Pune",
    "Chennai"
]

valid_trip_status = [
    "Completed",
    "In_progress",
    "Cancelled"
]
trips_valid = (
    trip_valid.filter(F.col('pickup_city').isin(valid_cities)
                      & F.col('drop_city').isin(valid_cities)
                       & F.col('trip_status').isin(valid_trip_status))
)
    

trips_valid = (
    trips_valid.filter( (F.col('trip_start_time') <= F.current_timestamp())
                        & (F.col('trip_end_time') <= F.current_timestamp())
                        & (F.col("trip_start_time") <= F.col("trip_end_time"))
                        )
)

trips_valid = (
    trips_valid.filter(
      (F.col("distance_km") > 0)
        & (F.col("base_fare") > 0)
        & (F.col("surge_multiplier") >= 1)
        & (F.col("final_fare") >= F.col("base_fare"))
        & (
        F.col("final_fare")
        == F.col("base_fare") * F.col("surge_multiplier")
            )
    )
)       
        

trips_final = trips_valid.dropDuplicates()
trips_silver = trips_final.withColumnRenamed('ingestion_date','bronze_ingested_date')
trips_silver = trips_silver.withColumn('silver_ingested_date',F.current_timestamp())

trips_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(config.SILVER_TRIP_PATH)

