# 🛒 Incremental Data Pipeline for Subscription E-Commerce

## 📌 Project Overview
An end-to-end, incremental Data Engineering pipeline designed to process streaming e-commerce subscription events. This project simulates a live API generating transaction data, ingests it incrementally using Databricks Auto Loader, processes the data through a Medallion Architecture (Bronze, Silver, Gold), and loads the final dimensions into Snowflake. 

The pipeline is built to handle real-world edge cases, including malformed data routing (Dead Letter Queues) and historical state tracking (SCD Type 2), proving readiness for production-level cloud data environments.

## 🛠️ Tech Stack
* **Language:** Python, SQL
* **Data Processing:** PySpark, Databricks (Auto Loader, Unity Catalog Volumes)
* **Cloud Data Warehouse:** Snowflake
* **Libraries:** `faker`, `snowflake-connector-python`, `pandas`

## 🏗️ Pipeline Architecture
1. **Data Generation (Mock API):** A Python script uses the `Faker` library to generate continuous JSON payloads representing user subscriptions and payments. It intentionally injects schema violations (nulls, datatype mismatches) in 10% of the data to simulate real-world messy data.
2. **Landing Zone (Databricks Unity Catalog):** Data is streamed directly into a secure Unity Catalog Volume via the Databricks REST API.
3. **Bronze Layer (Ingestion & Schema Enforcement):** Databricks Auto Loader (`cloudFiles`) incrementally reads new JSON files. Strict `StructType` schemas are enforced. Valid records are saved to a Delta table, while malformed records are automatically routed to a **Dead Letter Queue (DLQ)** without halting the pipeline.
4. **Silver Layer (Transformation & Routing):** PySpark flattens nested JSON structures (`payment_details`). A `foreachBatch` function splits the micro-batch, routing successful transactions to the main Silver table and isolating failed transactions into a separate table for downstream analysis.
5. **Gold Layer (Snowflake & SCD Type 2):** Clean data is pushed to a Snowflake Staging table using the Snowflake Python Connector. A native SQL `MERGE` statement is then executed to update the final `DIM_SUBSCRIPTIONS` table, implementing **Slowly Changing Dimensions (SCD Type 2)** to track user upgrades and downgrades over time.

## 🔄 Data Flow Architecture

```mermaid
graph TD
    %% 1. Define Custom Colors (Required before applying)
    classDef source fill:#f9f2f4,stroke:#d0a9b5,stroke-width:2px,color:#000
    classDef storage fill:#fff3e0,stroke:#ffb74d,stroke-width:2px,color:#000
    classDef databricks fill:#e6f3ff,stroke:#82b1ff,stroke-width:2px,color:#000
    classDef alert fill:#ffebee,stroke:#ef5350,stroke-width:2px,color:#000
    classDef snowflake fill:#e6f7fa,stroke:#4dd0e1,stroke-width:2px,color:#000

    %% 2. Define Nodes, Connections, and Apply Styles in one pass
    A([Mock API Generator]):::source -->|Streams JSON| B[(Landing Zone)]:::storage
    
    B -->|Reads incrementally| C{Databricks Auto Loader}:::databricks
    
    C -->|Schema Valid| D[(Bronze: Raw Events)]:::databricks
    C -->|Schema Mismatch| E[(Dead Letter Queue)]:::alert
    
    D -->|Micro-batch Stream| F[PySpark foreachBatch]:::databricks
    
    F -->|Status: Success| G[(Silver: Clean Data)]:::databricks
    F -->|Status: Failed| H[(Silver: Failed Payments)]:::databricks
    
    G -->|toPandas payload| I[Snowflake Python Connector]:::snowflake
    
    I -->|Truncate & Load| J[(Staging: STG_SUBSCRIPTIONS)]:::snowflake
    
    J -->|SCD Type 2| K{SQL MERGE}:::snowflake
    
    K -->|Maintains History| L[(Gold: DIM_SUBSCRIPTIONS)]:::snowflake

## 💡 Key Engineering Features Highlighted
* **Cost-Optimized Micro-Batching:** Utilized Databricks Auto Loader with `Trigger.AvailableNow=True` to process continuous data in scheduled, cost-efficient batches rather than running expensive 24/7 streaming clusters.
* **Resilient Error Handling:** Implemented PySpark's `badRecordsPath` to capture and quarantine schema violations into a DLQ, ensuring pipeline stability and zero data loss.
* **Historical Data Tracking (CDC):** Designed an SCD Type 2 architecture in Snowflake using `MERGE` and `UNION ALL` statements to preserve the full timeline of user subscription changes (e.g., tracking exactly when a user upgraded from Basic to Premium).
* **Cross-Platform Integration:** Successfully navigated cloud security boundaries by using Databricks Secrets for credential management and bridging Databricks Unity Catalog with Snowflake.

## 📂 Repository Structure
* `/data_generator` - Contains the `mock_api_generator.py` script that streams data to Databricks.
* `/databricks_src` - Contains the PySpark notebook handling the Bronze, Silver, and Gold transformations.
* `/snowflake_sql` - Contains the DDL scripts used to build the target Snowflake environment.
