from rideshare.common import config
from pyspark.sql import functions as F

drivers = (spark.table('rideshare.bronze.drivers')
           .select('driver_id','driver_name','city','vehicle_type',
                   'join_date','updated_at','ingestion_date'))
drivers_cast = (
    drivers.withColumn('join_date',F.to_date(F.col('join_date')))
    .withColumn('updated_at',F.to_timestamp(F.col('updated_at')))
)

drivers_trim = (
    drivers_cast.withColumn('city',F.initcap(F.trim(F.col('city'))))
    .withColumn('driver_name',F.initcap(F.trim(F.col('driver_name'))))
    .withColumn('vehicle_type',F.initcap(F.trim(F.col('vehicle_type'))))
)

drivers_valid = (
    drivers_trim.filter(F.col('driver_id').isNotNull()
             & F.col('driver_name').isNotNull()
             & F.col('city').isNotNull()
             & F.col('vehicle_type').isNotNull()
             & F.col('join_date').isNotNull()
             & F.col('updated_at').isNotNull()
             & F.col('ingestion_date').isNotNull())
)

valid_cities = [
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Pune",
    "Chennai"
]
valid_vehicle_types = [
    "Mini",
    "Premium"
]
drivers_valid = (
    drivers_valid.filter(F.col('city').isin(valid_cities)
                         & F.col('vehicle_type').isin(valid_vehicle_types)
                         )
)

drivers_valid = (
    drivers_valid.filter(F.col('join_date')<= F.current_date())
)

drivers_silver = drivers_valid.dropDuplicates()
drivers_silver = drivers_silver.withColumnRenamed('ingestion_date','bronze_ingested_date')
drivers_silver = drivers_silver.withColumn('silver_ingested_date',F.current_timestamp())
# drivers_silver.explain('formatted')
drivers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(config.SILVER_DRIVER_PATH)














# drivers_trim.explain('formatted')
# display(drivers)
# drivers_trim.explain('formatted')

# filtered = (
#     drivers_trim
#     .filter(F.col("city") == "Bangalore")
#     .select("driver_id
# )
# # drivers.explain('formatted')

# print(drivers.count())

# drivers.printSchema()

# print(drivers.rdd.getNumPartitions())

# filtered = (
#     drivers
#     .filter(F.col("city") == "Bangalore")
#     .select("driver_id", "driver_name", "city")
# )
# filtered.explain("formatted")



