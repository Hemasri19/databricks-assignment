
Project Overview
-------------------------------------------------------------------------------------------------------------------
This project implements an end-to-end data analytics pipeline for a global retail organization using Python, Pandas, Airflow, and Databricks.

The system ingests raw sales data, validates and cleans it, stores intermediate “silver” data in Databricks, and finally generates aggregated “gold” tables to support dashboards and analytics queries.

The goal is to provide a centralized, automated, and scalable analytics platform that consolidates sales data from multiple sources and delivers actionable insights.

Architecture & Data Flow
--------------------------------------------------------------------------------------------------------------------
1. Data Ingestion (Raw → Silver)
    Input Data: CSV and Excel files from various sales channels and regions.
    Processing Tool: Pandas
    Tasks:
        File Validation: Ensures supported extensions (.csv, .xlsx).
        Data Cleaning: Removes duplicates, validates required columns.
        Parquet Conversion: Converts clean data into silver-level Parquet files for optimized processing.
    Airflow Component: run_pandas_ingestion Python task

2. Upload to Databricks (Silver Storage)
    Destination: Databricks DBFS (/FileStore/silver/)
    Method: Airflow’s DatabricksHook for secure API-based upload
    Tasks:
        Read the silver Parquet file locally.
        Upload to DBFS in streaming mode (chunked upload).
    Airflow Component: upload_to_dbfs Python task

Notes: Databricks connection credentials are managed via Airflow Connections.

3. Databricks Job Triggering & Monitoring (Silver → Gold)
    Job Purpose: Transform silver Parquet files into structured fact and dimension tables, and aggregate gold-level tables.
    Steps:
        Fact & Dimension Table Creation
            Fact tables: Sales transactions, revenue, quantities
            Dimension tables: Products, Customers
        Gold-Level Aggregations
            Aggregate tables for analytics and reporting
            Stored in Databricks Catalog (catalog_name.schema_name)
        Airflow Component: monitor_databricks_job Python task
        Logic:
            Task triggers the Databricks job and retrieves the run_id internally.
            Polls Databricks REST API until the job completes.
            If the job fails, sends email notification and fails the task.
            On success, returns True and continues DAG flow.


4. Analytics & Dashboard
    Data Source: Aggregated tables in Databricks Catalog
    Purpose: Run SQL queries to generate dashboards and reports
    Tools: Databricks SQL or BI tools (e.g., Power BI)

---------------------------------------------------------------------------------------------------------------------
                                                    Airflow DAG Flow
----------------------------------------------------------------------------------------------------------------------
                                                    run_pandas_ingestion
                                                            ↓
                                                    upload_to_dbfs(Databricks Filesystem via API)
                                                            ↓
                                                    monitor_databricks_job (triggers & monitors Databricks job)

Retry Policy: 2 retries with 5-minute intervals
Schedule: Daily (@daily)
Start Date: Jan 1, 2024
Failure Handling: Email notifications on ingestion or Databricks job failures (Using Slack Webhook)

---------------------------------------------------------------------------------------------------------------------
Technology Stack
---------------------------------------------------------------------------------------------------------------------
Orchestration: Apache Airflow
Data Processing: Python, Pandas, PySpark
Storage & Processing: Databricks DBFS, Catalog & Tables
Data Format: CSV / Excel → Parquet → Delta Tables
Monitoring: Airflow Logs, Email Notifications
BI & Analytics: SQL Queries, Dashboards (Power BI / Databricks SQL)