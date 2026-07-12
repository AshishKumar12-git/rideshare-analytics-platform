from pyspark.sql.functions import * 
from functools import reduce

def add_metadata_columns(df):
    return ( df.withColumn('ingestion_timestamp',current_timestamp())
            .withColumn('file_name',input_file_name())
            )

def filter_required_columns(df,required_columns):
    condition = reduce(lambda x,y : x & y, [col(column).isNotNull() for column in required_columns])
    return df.filter(condition)

def convert_timestamp_columns(df,timestamp_columns):
    for column in timestamp_columns:
        df = df.withColumn(column,to_timestamp(col(column)))
    return df

def clean_string_columns(df,string_columns,trim_columns):
    for column in string_columns:
        df = df.withColumn(column,lower(trim(col(column))))
    for column in trim_columns:
        df = df.withColumn(column,trim(col(column)))
    return df
    