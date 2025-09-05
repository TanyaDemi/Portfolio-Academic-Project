# Data Engineering Project №6:
# Final Project — Analytical Data Warehouse 

# Project Overview

This project demonstrates the development of an analytical Data Warehouse using 
Apache Airflow, PostgreSQL, and Vertica.
The solution was implemented inside a Docker environment, with DAGs orchestrating data extraction, transformation, and loading (ETL) processes.

The work focused on creating staging and DWH layers, testing incremental loads, and delivering aggregated financial metrics.

## Project Workflow

1. Environment Setup
    Pulled and ran a Docker image with Airflow and database connections.
    Installed required Python libraries inside the container (psycopg3, vertica-python, tqdm).
    Configured connections to PostgreSQL and Vertica in Airflow.

2. DAGs
    0_data_init_table: created schemas and tables in Vertica from SQL files (1_stg.sql, 2_dwh.sql).
    1_data_import: loaded data from PostgreSQL (currencies, transactions) into Vertica staging tables.
    2_datamart_update: transformed staging data into DWH tables (dimensions, facts, metrics).
    2_datamart_update_2022-10-31: test DAG for loading a single date to validate logic faster.

3. Database Layers
    Staging (STG): raw data from PostgreSQL.
    DWH:
        dim_rates — exchange rates by date.
        accounts — accounts with country details.
        fact_transactions — transactions with amounts in source currency and USD.
        global_metrics — aggregated metrics by currency (total amounts, transaction counts, averages).

### Technologies Used
    Apache Airflow — DAG orchestration
    Docker — containerized environment
    PostgreSQL — source database
    Vertica — Data Warehouse
    Python (ETL scripting, SQL execution)
    SQL (DDL/DML, data transformations)

### Results
    Implemented a working ETL pipeline from PostgreSQL to Vertica.
    Built staging and DWH schemas with dimension and fact tables.
    Validated incremental and full data loads through Airflow DAGs.
    Delivered a data mart with exchange rates, transactions, accounts, and global financial metrics ready for analytical queries.
