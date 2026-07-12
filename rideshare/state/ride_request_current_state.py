from rideshare.common import config
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

ride_request_events = (spark.readStream.format('delta')
                       .load(config.SILVER_RIDE_REQUEST_PATH)
                       )

ride_request_events.printSchema()

def merge_ride_events(batch_df,batch_id):
    window = Window.partitionBy('request_id').orderBy(col('updated_at').desc())
    latest_events = batch_df.withColumn('row_num',row_number().over(window)).filter(col('row_num')== 1).drop('row_num')
    target_table = DeltaTable.forPath(spark,config.STATE_RIDE_REQUEST_PATH)
    (
        target_table.alias('target').merge(latest_events.alias('source'),'target.request_id = source.request_id')
        .whenMatchedUpdate(set =
                              {
                                  'rider_id' : 'source.rider_id',
                                  'pickup_city': 'source.pickup_city',
                                  'ride_type' : 'source.ride_type',
                                  'request_status' : 'source.request_status',
                                  'request_timestamp': 'source.request_timestamp',
                                  'updated_at': 'source.updated_at',
                                  'ingestion_timestamp': 'source.ingested_timestamp',
                                  'file_name': 'source.file_name'
                              })
        .whenNotMatchedInsert(values=
                              {
                                'request_id' : 'source.request_id',
                                'rider_id': 'source.rider_id',
                                'pickup_city': 'source.pickup_city',
                                'ride_type' : 'source.ride_type',
                                'request_status' : 'source.request_status',
                                'request_timestamp': 'source.request_timestamp',
                                'updated_at': 'source.updated_at',
                                'ingestion_timestamp': 'source.ingested_timestamp',
                                'file_name': 'source.file_name' 
                              })
        .execute()
    )

ride_query = (
        ride_request_events.writeStream
        .foreachBatch(merge_ride_events)
        .option("checkpointLocation",config.STATE_RIDE_REQUEST_CHECKPOINT)
        .trigger(availableNow = True)
        .start()
)
ride_query.awaitTermination()
