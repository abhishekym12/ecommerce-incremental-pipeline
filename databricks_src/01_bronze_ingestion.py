from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# 1. Define schemas
payment_schema = StructType([
    StructField("method", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("error_code", StringType(), True)
])

bronze_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True), # Strict enforcement
    StructField("subscription_plan", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("payment_details", payment_schema, True)
])

# 2. Define paths (Replace 'workspace' if your catalog differs)
source_volume_path = "/Volumes/workspace/default/landing_zone/"
checkpoint_path = "/Volumes/workspace/default/checkpoints/bronze_events/"
dlq_path = "/Volumes/workspace/default/dlq/"
target_table_name = "workspace.default.bronze_events"

# 3. Read incrementally using Auto Loader
raw_stream_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(bronze_schema)
    .option("badRecordsPath", dlq_path) 
    .load(source_volume_path)
)

# 4. Write using AvailableNow trigger (Micro-batch)
query = (raw_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table_name)
)

query.awaitTermination()
print("Bronze ingestion complete. Bad records routed to DLQ.")