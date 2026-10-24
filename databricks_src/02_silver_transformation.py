from pyspark.sql.functions import col

# 1. Read incrementally from the Bronze table
bronze_stream_df = spark.readStream.format("delta").table("workspace.default.bronze_events")

# 2. Flatten the nested structure and drop invalid rows
flattened_df = bronze_stream_df.select(
    col("event_id"),
    col("user_id"),
    col("subscription_plan"),
    col("transaction_status"),
    col("timestamp"),
    col("payment_details.method").alias("payment_method"),
    col("payment_details.amount").alias("payment_amount"),
    col("payment_details.error_code").alias("error_code")
).dropna(subset=["user_id", "event_id"])

# 3. Define the Silver checkpoint
silver_checkpoint = "/Volumes/workspace/default/checkpoints/silver_events/"

# 4. Route data based on transaction status
def route_transactions(batch_df, batch_id):
    batch_df.persist()
    
    # Success branch
    success_df = batch_df.filter(col("transaction_status") == 'success')
    success_df.write.format("delta").mode("append").saveAsTable("workspace.default.silver_subscriptions")
    
    # Failure branch
    failed_df = batch_df.filter(col("transaction_status") == 'failed')
    failed_df.write.format("delta").mode("append").saveAsTable("workspace.default.silver_failed_transactions")
    
    batch_df.unpersist()

# 5. Execute stream
query = (flattened_df.writeStream
    .foreachBatch(route_transactions)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("Silver transformation complete. Data routed to target tables.")
