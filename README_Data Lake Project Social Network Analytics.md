# Data Engineering Project №2:
# Data Lake Project: Social Network Analytics

# Project Overview

This project demonstrates the design of a Data Lake architecture for a social network. 
Raw event data from different sources was ingested, processed with Apache Spark on YARN, 
and transformed into analytical data marts for user profiles, geo-based activity, 
and friend recommendations.
All workflows were tested on a Yandex.Cloud Dataproc cluster using HDFS storage.

## Data Sources
    Events: raw user activity logs (/user/master/data/geo/events/) with nested JSON structure (messages, reactions, subscriptions).
    Geo reference data: city directory (geo.csv) with corrected column names, delimiters, and formats.
    Test dataset: 10% sample of event data for Q1 2022, stored in HDFS.

##  Data Architecture
    Raw layer (staging): unprocessed event data partitioned by date.
    DWH layer (Parquet): structured data marts with partitioning for optimized queries.
    Formats: Parquet (processed data), CSV (reference).

##  Data Marts

1. User Profiles (/user/.../geo/user_profiles)
    Aggregated user home and active cities.
    Travel history with count and list of visited cities.
    Local time for last activity.
Schema:
    user_id
    act_city
    home_city
    travel_count
    travel_array
    local_time

2. Geo Zones Analytics (/user/.../geo/user_activity_zones)
    Aggregated metrics by week and month:
        messages
        reactions
        subscriptions
        active users
    Schema:
        month, week, zone_id
        week_message, week_reaction, week_subscription, week_user
        month_message, month_reaction, month_subscription, month_user

3. Friend Recommendations (/user/.../geo/friend_recommendations)
    Suggested friend connections based on activity zones.
    Includes processing timestamp and local time.

    Schema:
        user_left, user_right
        processed_dttm
        zone_id
        local_time

##  Automation

    PySpark jobs for data processing (stored in src/scripts/).
    Airflow DAG (dag_bash.py) for orchestration of data processing and data mart creation.
    Partitioning by date=YYYY-MM-DD for scalable storage in HDFS.

##  Tech Stack

    Processing: PySpark, Apache Spark (local & YARN modes)
    Orchestration: Apache Airflow
    Storage: HDFS, Yandex.Cloud Dataproc
    Formats: JSON (raw), CSV (geo reference), Parquet (processed)
    Languages: Python

##  Key Deliverables

    Configured Spark environment for both local and YARN cluster modes.
    Built data marts for user profiles, geo-based activity, and friend recommendations.
    Automated ETL jobs with Airflow DAGs.
    Implemented partitioned Parquet storage for analytical queries.

##  This project highlights practical skills in data lake architecture, distributed data processing with Spark, and ETL automation with Airflow.
