from rideshare.common import config
from pyspark.sql import functions as F


payments = (
    spark.table("rideshare.bronze.payments")
    .select(
        "payment_id",
        "trip_id",
        "payment_method",
        "payment_amount",
        "payment_status",
        "payment_timestamp",
        "updated_at",
        "ingestion_date"
    )
)


payments_cast = (
    payments
    .withColumn("payment_amount", F.col("payment_amount").cast("double"))
    .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)



payments_clean = (
    payments_cast
    .withColumn(
        "payment_method",
        F.initcap(F.trim("payment_method"))
    )
    .withColumn(
        "payment_status",
        F.initcap(F.trim("payment_status"))
    )
)


payments_valid = (
    payments_clean
    .filter(
        F.col("payment_id").isNotNull()
        & F.col("trip_id").isNotNull()
        & F.col("payment_method").isNotNull()
        & F.col("payment_amount").isNotNull()
        & F.col("payment_status").isNotNull()
        & F.col("payment_timestamp").isNotNull()
        & F.col("updated_at").isNotNull()
        & F.col("ingestion_date").isNotNull()
    )
)


valid_payment_methods = [
    "Credit Card",
    "Cash",
    "Upi",
    "Debit Card"
]

valid_payment_status = [
    "Success",
    "Pending",
    "Refunded"
]

payments_valid = (
    payments_valid
    .filter(
        (F.col("payment_method").isin(valid_payment_methods))
        &
        (F.col("payment_status").isin(valid_payment_status))
    )
)

payments_valid = (
    payments_valid.filter(F.col("payment_amount") > 0)
)

payments_valid = (
    payments_valid.filter(F.col("payment_timestamp") <= F.current_timestamp())
)

payments_silver = payments_valid.dropDuplicates()


payments_silver = (
    payments_silver
    .withColumnRenamed(
        "ingestion_date",
        "bronze_ingested_date"
    )
    .withColumn(
        "silver_ingestion_date",
        F.current_timestamp()
    )
)

payments_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(config.SILVER_PAYMENT_PATH)



    