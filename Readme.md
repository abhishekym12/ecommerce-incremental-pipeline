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

## 🔄 Data Flow Architecture

```mermaid
graph TD
    %% Define styles
    classDef source fill:#f9f2f4,stroke:#d0a9b5,stroke-width:2px;
    classDef databricks fill:#e6f3ff,stroke:#82b1ff,stroke-width:2px;
    classDef snowflake fill:#e6f7fa,stroke:#4dd0e1,stroke-width:2px;
    classDef storage fill:#fff3e0,stroke:#ffb74d,stroke-width:2px;
    classDef alert fill:#ffebee,stroke:#ef5350,stroke-width:2px;

    %% Nodes
    A([Mock API / Python Generator])
    B[(Landing Zone: JSON files)]
    C{Auto Loader}
    D[(Bronze Table: Raw Events)]
    E[(Dead Letter Queue)]
    F[PySpark foreachBatch]
    G[(Silver Table: Success)]
    H[(Silver Table: Failures)]
    I[Snowflake Python Connector]
    J[(Staging: STG_SUBSCRIPTIONS)]
    K[SQL MERGE: SCD Type 2]
    L[(Gold: DIM_SUBSCRIPTIONS)]

    %% Flow
    A -->|"Streams events"| B
    B -->|"Reads incrementally"| C
    C -->|"Schema valid"| D
    C -->|"Schema mismatch"| E
    
    D -->|"Micro-batch stream"| F
    F -->|"Status = 'success'"| G
    F -->|"Status = 'failed'"| H
    
    G -->|"toPandas() micro-batch"| I
    I -->|"Auto-creates/Truncates"| J
    J -->|"INSERT / UPDATE"| K
    K -->|"Maintains History"| L

    %% Apply Styles
    class A source;
    class B storage;
    class C,D,F,G,H databricks;
    class E alert;
    class I,J,K,L snowflake;

## 💡 Key Engineering Features Highlighted
* **Cost-Optimized Micro-Batching:** Utilized Databricks Auto Loader with `Trigger.AvailableNow=True` to process continuous data in scheduled, cost-efficient batches rather than running expensive 24/7 streaming clusters.
* **Resilient Error Handling:** Implemented PySpark's `badRecordsPath` to capture and quarantine schema violations into a DLQ, ensuring pipeline stability and zero data loss.
* **Historical Data Tracking (CDC):** Designed an SCD Type 2 architecture in Snowflake using `MERGE` and `UNION ALL` statements to preserve the full timeline of user subscription changes (e.g., tracking exactly when a user upgraded from Basic to Premium).
* **Cross-Platform Integration:** Successfully navigated cloud security boundaries by using Databricks Secrets for credential management and bridging Databricks Unity Catalog with Snowflake.

## 📂 Repository Structure
* `/data_generator` - Contains the `mock_api_generator.py` script that streams data to Databricks.
* `/databricks_src` - Contains the PySpark notebook handling the Bronze, Silver, and Gold transformations.
* `/snowflake_sql` - Contains the DDL scripts used to build the target Snowflake environment.