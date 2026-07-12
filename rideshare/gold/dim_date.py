from pyspark.sql import functions as F


start_date = "2024-01-01"
end_date = "2028-12-31"

dim_date_df = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS full_date
        """
    )
)


dim_date_df = (
    dim_date_df
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )

    
    .withColumn(
        "day",
        F.dayofmonth("full_date")
    )

    
    .withColumn(
        "month",
        F.month("full_date")
    )

    
    .withColumn(
        "month_name",
        F.date_format("full_date", "MMMM")
    )

    
    .withColumn(
        "quarter",
        F.quarter("full_date")
    )

   
    .withColumn(
        "year",
        F.year("full_date")
    )

    
    .withColumn(
        "week_of_year",
        F.weekofyear("full_date")
    )

    
    .withColumn(
        "day_of_week",
        F.dayofweek("full_date")
    )

   
    .withColumn(
        "day_name",
        F.date_format("full_date", "EEEE")
    )

    
    .withColumn(
        "is_weekend",
        F.when(
            F.dayofweek("full_date").isin(1, 7),
            "Y"
        ).otherwise("N")
    )

   
    .withColumn(
        "is_month_end",
        F.when(
            F.last_day("full_date") == F.col("full_date"),
            "Y"
        ).otherwise("N")
    )

    .select(
        "date_key",
        "full_date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "week_of_year",
        "day_of_week",
        "day_name",
        "is_weekend",
        "is_month_end"
    )
)


print(f"Total Dates : {dim_date_df.count()}")

display(dim_date_df)

(
    dim_date_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("rideshare.gold.dim_date")
)
