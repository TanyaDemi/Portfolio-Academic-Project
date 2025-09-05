 _____________________________________________________________________________
|                         Data Engineering Project №1:                        |
|                                                                             |
|             ETL Pipelines and Courier Payments Data Mart                    |
|_____________________________________________________________________________|

### Project Overview

This academic project demonstrates the design and implementation of an 
ETL pipeline using Apache Airflow and Python for data orchestration. 
Data was ingested from multiple sources (PostgreSQL, MongoDB, API), 
processed through a multi-layered architecture (staging, DDS, CDM), 
and transformed into a data mart for courier payments.

# ETL Processes

Developed Airflow DAGs in Python for orchestrating data ingestion, transformation, and loading.
Implemented schema creation and incremental data loading for PostgreSQL, MongoDB, and API sources.
Configured DAGs for each layer:
    Staging (stg): raw data ingestion from sources.
    DDS (Data Delivery Store): normalized data with hubs, dimensions, and fact tables.
    CDM (Consumer Data Mart): aggregated data for analytical use cases.
Handled workflow state tracking via service tables (srv_wf_settings).

## Data Sources

PostgreSQL: bonus system events, users, and ranks.
MongoDB: restaurant and order data.
API: couriers, deliveries, and restaurants.
Data stored in JSON format for API and MongoDB extractions.

## Data Warehouse Architecture

The DWH follows a snowflake schema:
Fact table: dds.fct_api_courier_payments
Dimensions:
    dds.dm_users (customers)
    dds.dm_restaurants (restaurants)
    dds.dm_timestamps (time dimensions)
    dds.api_dm_couriers (couriers)
    dds.dm_deliveries (deliveries)
    dds.dm_products (restaurant products)

##  Courier Payments Data Mart (cdm.dm_courier_ledger)

Aggregated courier performance and compensation:
orders_count — number of unique orders
orders_total_sum — total order value
rate_avg — average courier rating
order_processing_fee — commission (25% of order value)
courier_order_sum — calculated payout based on rating
courier_tips_sum — sum of tips
courier_reward_sum — total reward = payout + 95% of tips

## Tech Stack

- **Airflow** (DAG orchestration)  
- **Python** (ETL scripts)  
- **PostgreSQL** (relational data source)  
- **MongoDB** (NoSQL data source)  
- **REST API** (external data source)  
- **JSON, SQL, API integration** (data formats)
- **Snowflake schema (fact + dimensions), multi-layer DWH (stg → dds → cdm)** (modeling)

##  Key Deliverables

Automated DAGs for end-to-end ETL workflows.
Incremental and batch data loading from heterogeneous sources.
Normalized DDS layer and analytical CDM layer.
A functional data mart for courier payouts with aggregated financial metrics.

###  This project highlights practical skills in ETL pipeline development, 
### data warehousing, and workflow orchestration using real-world data integration scenarios.
