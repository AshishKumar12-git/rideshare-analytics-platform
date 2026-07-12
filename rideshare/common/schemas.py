from pyspark.sql.types import * 

# ----- Driver Event Schema -----

driver_event_schema = StructType([
    StructField('event_id',LongType(),nullable = False),
    StructField('driver_id',LongType(),nullable = False),
    StructField('event_type',StringType(),nullable = True),
    StructField('status',StringType(),nullable = True),
    StructField('city',StringType(),nullable = True),
    StructField('event_timestamp',StringType(),nullable = True)
])

# ----- Ride Request Schema -----

ride_request_schema = StructType([
    StructField('request_id',LongType(), nullable = False),
    StructField('rider_id',LongType(), nullable = False),
    StructField('pickup_city',StringType(), nullable = True),
    StructField('ride_type',StringType(), nullable = True),
    StructField('request_status',StringType(), nullable = True),
    StructField('request_timestamp',StringType(), nullable = True),
    StructField('updated_at',StringType(), nullable = True)
])