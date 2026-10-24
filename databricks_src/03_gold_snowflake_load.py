import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# 1. Fetch credentials & load Silver data
sf_password = dbutils.secrets.get(scope="snowflake", key="password")
silver_success_df = spark.read.table("workspace.default.silver_subscriptions")

pdf = silver_success_df.toPandas()
pdf.columns = [col.upper() for col in pdf.columns]
pdf.attrs = {}

# 2. Connect to Snowflake
conn = snowflake.connector.connect(
    user="you_snowflake_username",
    password=sf_password,
    account="LJOVIWZ-HX64351", 
    database="ECOMMERCE",
    schema="GOLD",
    warehouse="COMPUTE_WH"
)

# 3. Load into Staging
conn.cursor().execute("DROP TABLE IF EXISTS ECOMMERCE.GOLD.STG_SUBSCRIPTIONS")
success, num_chunks, num_rows, _ = write_pandas(
    conn=conn,
    df=pdf,
    table_name="STG_SUBSCRIPTIONS",
    database="ECOMMERCE",
    schema="GOLD",
    auto_create_table=True,
)
print(f"Data successfully staged in Snowflake! ({num_rows} rows)")

# 4. Execute SCD Type 2 MERGE
merge_sql = """
MERGE INTO ECOMMERCE.GOLD.DIM_SUBSCRIPTIONS AS target
USING (
    SELECT 
        USER_ID, 
        SUBSCRIPTION_PLAN, 
        PAYMENT_METHOD, 
        PAYMENT_AMOUNT, 
        TO_TIMESTAMP_NTZ(TIMESTAMP, 9) AS EVENT_TIME,
        'INSERT' AS MERGE_ACTION
    FROM ECOMMERCE.GOLD.STG_SUBSCRIPTIONS
    
    UNION ALL
    
    SELECT 
        target.USER_ID, 
        target.SUBSCRIPTION_PLAN, 
        target.PAYMENT_METHOD, 
        target.PAYMENT_AMOUNT, 
        TO_TIMESTAMP_NTZ(stage.TIMESTAMP, 9) AS EVENT_TIME,
        'UPDATE' AS MERGE_ACTION
    FROM ECOMMERCE.GOLD.DIM_SUBSCRIPTIONS AS target
    JOIN ECOMMERCE.GOLD.STG_SUBSCRIPTIONS AS stage
      ON target.USER_ID = stage.USER_ID
    WHERE target.IS_ACTIVE = TRUE 
      AND target.SUBSCRIPTION_PLAN != stage.SUBSCRIPTION_PLAN
) AS source
ON target.USER_ID = source.USER_ID 
   AND target.IS_ACTIVE = TRUE 
   AND source.MERGE_ACTION = 'UPDATE'
WHEN MATCHED THEN 
    UPDATE SET target.IS_ACTIVE = FALSE, target.END_TIME = source.EVENT_TIME
WHEN NOT MATCHED THEN 
    INSERT (USER_ID, SUBSCRIPTION_PLAN, PAYMENT_METHOD, PAYMENT_AMOUNT, IS_ACTIVE, START_TIME, END_TIME)
    VALUES (source.USER_ID, source.SUBSCRIPTION_PLAN, source.PAYMENT_METHOD, source.PAYMENT_AMOUNT, TRUE, source.EVENT_TIME, NULL);
"""

conn.cursor().execute(merge_sql)
conn.close()
print("Gold Layer updated successfully! End-to-end batch complete.")