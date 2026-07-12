from rideshare.common import config
from pyspark.sql import functions as F

riders = (spark.table('rideshare.bronze.riders')
          .select('rider_id','rider_name','city','signup_date','updated_at','ingestion_date')
          )
riders_cast = (
    riders.withColumn('signup_date',F.to_date(F.col('signup_date')))
    .withColumn('updated_at',F.to_timestamp(F.col('updated_at')))
)
riders_trim = (
    riders_cast.withColumn('rider_name',F.initcap(F.trim(F.col('rider_name'))))
    .withColumn('city',F.initcap(F.trim(F.col('city'))))
)
riders_valid = (
    riders_trim.filter(
        F.col('rider_id').isNotNull()
        & F.col('rider_name').isNotNull()
        & F.col('city').isNotNull()
        & F.col('signup_date').isNotNull()
        & F.col('updated_at').isNotNull()
        & F.col('ingestion_date').isNotNull()
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

riders_valid = (
    riders_valid.filter(
        F.col("city").isin(valid_cities)
    )
)
riders_valid = riders_valid.filter(F.col('signup_date')<= F.current_date())
riders_silver = riders_valid.dropDuplicates()
riders_silver = riders_silver.withColumnRenamed('ingestion_date','bronze_ingested_date')
riders_silver = riders_silver.withColumn('silver_ingestion_date',F.current_timestamp())
riders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(config.SILVER_RIDER_PATH)

