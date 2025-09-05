# Data Engineering Project №3:
# Analytical Data Warehouse Development

# Project Overview

This project demonstrates the development of an analytical database using Apache 
Airflow, Vertica, and SQL. The pipeline ingests raw CSV data from Amazon S3, 
loads it into the staging layer of Vertica, and then processes it into a 
Data Vault model (DDS) and business-oriented tables.

The implementation was part of a training sprint and focused on hands-on practice 
with ETL orchestration, staging schema design, and Data Vault modeling.

## Repository Structure
    ├── src/
    │   ├── dags/
    │   │   ├── dags_for_groups.py                # Loads raw CSV files from S3 to local machine for inspection
    │   │   ├── Vertica_init_dag_STG.py           # Creates schema and staging tables in Vertica (suffix "_csv")
    │   │   ├── Vertica_dag_by_YP_users_csv.py    # Loads users CSV data from S3 into Vertica STG
    │   │   ├── Vertica_dag_by_YP_groups_csv.py   # Loads groups CSV data from S3 into Vertica STG
    │   │   ├── Vertica_dag_by_YP_dialogs_csv.py  # Loads dialogs CSV data from S3 into Vertica STG
    │   │   └── Vertica_dag_by_YP_group_log_csv.py# Loads group logs CSV data from S3 into Vertica STG
    │   │
    │   ├── sql/
    │   │   ├── 6_STG_tables_users_groups_dialogs_group_log.sql   # Creates staging tables with filters and keys
    │   │   ├── 7_DDS_tables_in_data_Vault.sql                    # DDS layer tables (Data Vault model, sprint)
    │   │   ├── 8_DDS_tables_in_data_Vault_project.sql            # DDS layer tables (Data Vault model, project)
    │   │   └── 9_CTE_tables_for_business.sql                     # Business CTEs and analytical queries
    │
    └── README.md

##  ETL Workflow

    1. Extract
        Airflow DAGs download CSV files from Amazon S3 into a temporary folder.
        Data inspected locally for delimiters, structure, and volume.
    2. Load (STG Layer)
        Initial DAG (Vertica_init_dag_STG.py) creates schemas and staging tables in Vertica.
        Additional DAGs insert raw CSV data into staging tables (suffix _csv).
    3. Transform (DDS Layer)
        SQL scripts implement Data Vault methodology (Hubs, Links, Satellites).
        Two versions: sprint practice (7_DDS_tables_in_data_Vault.sql) and project version (8_DDS_tables_in_data_Vault_project.sql).
    4. Business Layer (CDM)
        CTE-based transformations in 9_CTE_tables_for_business.sql.
        Provides aggregated data structures for business use cases.

##  Tech Stack
    Apache Airflow – DAG orchestration
    Vertica – Data Warehouse (STG, DDS, CDM layers)
    SQL – table creation, transformations, CTE queries
    Amazon S3 – data source (CSV files)
    Python – Airflow DAG implementation

##  Key Deliverables
    ETL pipeline to load data from S3 → Vertica STG → DDS → CDM
    Implementation of Data Vault model (Hubs, Links, Satellites) in SQL
    Business transformations using CTEs for analytical queries
    Reusable Airflow DAGs for staging layer ingestion
