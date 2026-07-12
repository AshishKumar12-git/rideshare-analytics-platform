from rideshare.common import config
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

driver_events = (spark.readStream.format('delta')
          .load(config.SILVER_DRIVER_EVENT_PATH))

driver_events.printSchema()

def merge_driver_state(batch_df,batch_id):
    windows = Window.partitionBy(col('driver_id')).orderBy(col('event_timestamp').desc())
    latest_driver_events = (batch_df.withColumn('row_num',row_number().over(windows))
                    .filter(col('row_num')==1).drop('row_num'))
    target_table = DeltaTable.forPath(spark,config.STATE_DRIVER_PATH)
    (target_table.alias('target').merge(latest_driver_events.alias('source'),'target.driver_id = source.driver_id')
        .whenMatchedUpdate(set =
                              {
                                  'event_id' : 'source.event_id',
                                  'event_type': 'source.event_type',
                                  'status' : 'source.status',
                                  'city' : 'source.city',
                                  'event_timestamp': 'source.event_timestamp',
                                  'ingestion_timestamp': 'source.ingestion_timestamp',
                                  'file_name': 'source.file_name'
                              })
        .whenNotMatchedInsert(values =
                              {
                                  'event_id' : 'source.event_id',
                                  'driver_id' : 'source.driver_id',
                                  'event_type': 'source.event_type',
                                  'status' : 'source.status',
                                  'city' : 'source.city',
                                  'event_timestamp': 'source.event_timestamp',
                                  'ingestion_timestamp': 'source.ingestion_timestamp',
                                  'file_name': 'source.file_name'
                                  
                              })
        .execute()
    )
    
driver_events_query = (
            driver_events.writeStream
            .foreachBatch(merge_driver_state)
            .option('checkpointLocation',config.STATE_DRIVER_CHECKPOINT)
            .trigger(availableNow = True)
            .start()
)
driver_events_query.awaitTermination()