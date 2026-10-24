print("Starting E-Commerce Incremental Pipeline...")

try:
    # Run Bronze Layer (Timeout after 600 seconds)
    print("1. Running Bronze Ingestion...")
    dbutils.notebook.run("./01_bronze_ingestion", 600)
    
    # Run Silver Layer
    print("2. Running Silver Transformation...")
    dbutils.notebook.run("./02_silver_transformation", 600)
    
    # Run Gold Layer
    print("3. Running Gold Snowflake Load...")
    dbutils.notebook.run("./03_gold_snowflake_load", 600)
    
    print("Pipeline executed successfully end-to-end!")
    
except Exception as e:
    print(f"Pipeline failed! Error: {e}")