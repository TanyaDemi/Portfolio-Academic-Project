# Выполненная работа

# ETL Pipelines and Courier Payments Data Mart

## Project Description

This project demonstrates the development of an ETL pipeline for building a Data Warehouse (DWH) to support courier payments analytics.  
The workflows are orchestrated with **Apache Airflow** and process data from multiple sources (PostgreSQL, MongoDB, API) into a multi-layer DWH (stg → dds → cdm).

---

## ETL Processes

- **Schema Initialization**  
  Creation of schemas and tables for data loading into the DWH.  
  *Source code:*  
  `dags/examples/stg/init_schema_dag`

- **PostgreSQL → Staging Layer**  
  Load of bonus system data (ranks) into the staging schema.  
  *Source code:*  
  `dags/examples/stg/bonus_system_ranks_dag`

- **MongoDB → Staging Layer**  
  Load of restaurant and order system data into the staging schema.  
  *Source code:*  
  `dags/examples/stg/order_system_restaurants_dag`

- **API → Staging Layer**  
  Load of external API data into the staging schema.  
  *Source code:*  
  `dags/examples/stg/project/1_project_stg_ddl_api_loader_dags.py`

- **Staging → DDS Layer**  
  Transformation of staging data into normalized DDS structures (hubs, dimensions, facts).  
  *Source code:*  
  `dags/examples/stg/project/2_project_stg_dds_api_tables_dags.py`

- **DDS → CDM Layer**  
  Aggregation of DDS data into business-oriented marts for courier payments analytics.  
  *Source code:*  
  `dags/examples/stg/project/3_project_dwh_tables_dags.py`

---

## Data Warehouse Architecture

- **Staging (stg):** raw data from source systems.  
- **DDS (Data Delivery Store):** normalized data with dimensions and facts.  
- **CDM (Consumer Data Mart):** aggregated data prepared for analytical queries.

---

## Tech Stack

- **PostgreSQL** – relational data source  
- **MongoDB** – NoSQL data source  
- **REST API** – external data integration  
- **Airflow** – workflow orchestration  
- **Python** – ETL scripting  
- **JSON** – data exchange format  

# ETL-процессы

Для загрузки данных в DWH используются DAG-оркестраторы Airflow на языке Python. 
DAG содержит задачи на соответствующую загрузку данных из источников в staging-слой
и дальнейшую обработку в DDS и CDM слоях. Источники данных:
    PostgreSQL,
    MongoDB,
    API.
 
Описание проeкта:
Процесс создания таблиц для загрузки данных в DWH реализован с помощью Airflow.
Исходный код в директории 
ETL Pipelines and Courier Payments Data Mart/dags/examples/stg/init_schema_dag 'https://github.com/TanyaDemi/Portfolio-Academic-Project/edit/master/ETL%20Pipelines%20and%20Courier%20Payments%20Data%20Mart/README.md'

Процесс загрузки данных из источника PostgreSQL в staging-слой реализован с помощью Airflow.
Исходный код в директории 
ETL Pipelines and Courier Payments Data Mart\dags\examples\stg\bonus_system_ranks_dag

Процесс загрузки данных из источника MongoDB в staging-слой реализован с помощью Airflow.
Исходный код в директории 
ETL Pipelines and Courier Payments Data Mart\dags\examples\stg\order_system_restaurants_dag

Процесс загрузки данных из источника API в staging-слой реализован с помощью Airflow.
Исходный код в директории 
ETL Pipelines and Courier Payments Data Mart\dags\examples\stg\project\1_project_stg_ddl_api_loader_dags.py


Процесс загрузки данных в источнике PostgreSQL в dds слой реализован с помощью Airflow.
Исходный код в директории 
ETL Pipelines and Courier Payments Data Mart\dags\examples\stg\project\2_project_stg_dds_api_tables_dags.py

Процесс загрузки данных в источнике PostgreSQL в cdm слой реализован с помощью Airflow.
Исходный код в директории 
ETL Pipelines and Courier Payments Data Mart\dags\examples\stg\project\3_project_dwh_tables_dags.py


Архитектура и модель данных:
Слой stg: хранение необработанных данных из источников.
Слой dds: нормализация данных, создание измерений и фактов.
Слой cdm: агрегированные данные, предназначенные для аналитики.

Используемый стек технологий: PostgreSQL, Airflow, Python, API-интеграции, JSON-формат данных, MongoDB.

Витрина данных о выплатах курьерам

Состав витрины cdm.dm_courier_ledger
Логика наполнения витрины
1. Выбор исходных данных. Источник: таблицы слоя dds:
dds.dm_timestamps
dds.api_dm_couriers
dds.fct_api_courier_payments

2. Агрегация данных 
orders_count: количество уникальных order_key.
orders_total_sum: сумма order_sum.
rate_avg: средний рейтинг курьера.
order_processing_fee: orders_total_sum * 0.25.
courier_order_sum: расчёт на основе рейтинга.
courier_tips_sum: сумма чаевых.
courier_reward_sum: courier_order_sum + courier_tips_sum * 0.95.

3. Вставка данных с ON CONFLICT
Источники данных
Таблицы DDS
dds.dm_users — информация о заказчиках.
dds.dm_restaurants — информация о ресторанах.
dds.dm_timestamps — временные измерения (год, месяц) добавлены и обновлены на основе загрузок из двух источников API и MongoDB.
dds.api_dm_couriers — информация о курьерах.
dds.api_dm_couriers_orders_sum — суммы заказов, рейтинги, чаевые
dds.dm_deliveries — информация о доставках.
dds.dm_products - информация о товарах в меню ресторанов
dds.dm_orders — информация о стоимости, и курьерах по заказам.
dds.fct_product_sales - таблица фактов, содержащая информацию о продажах ресторанов.
dds.fct_api_courier_payments — основная таблица фактов, содержащая информацию о выплатах курьерам.

Архитектура модели данных (снежинка)
Центральная фактовая таблица:
dds.fct_api_courier_payments

Измерения:
dds.dm_users
dds.dm_restaurants
dds.dm_timestamps
dds.api_dm_couriers
dds.api_dm_couriers_orders_sum
dds.dm_deliveries

Загрузка данных 

Из PostgreSQL:
    stg.bonussystem_ranks
    stg.bonussystem_users
    stg.bonussystem_events
Из MongoDB:
    stg.ordersystem_restaurants
    stg.ordersystem_orders
    stg.ordersystem_users
Из API:
    stg.api_restaurants
    stg.api_couriers
    stg.api_deliveries

Таблицы для записи положения курсора в слоях stg и dds:
    stg.srv_wf_settings
    dds.srv_wf_settings
    cdm.srv_wf_settings
