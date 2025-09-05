from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

# Устанавливаем переменные окружения
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    dag_id="geo_analytics_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


start_date = "2022-01-01"
stop_date = "2022-03-31"
events_path = "/user/tanyademi1/data/stg/geo/sample_events"
cities_path = "/user/tanyademi1/data/geo/cities/geo.csv"
output_path_1 = "/user/tanyademi1/data/dwh/geo/user_profiles"
output_path_2 = "/user/tanyademi1/data/dwh/geo/user_activity_zones"
output_path_3 = "/user/tanyademi1/data/dwh/geo/friend_recommendations"


# Задача 1: user_profiles.py
user_profiles = SparkSubmitOperator(
    task_id='user_profiles',
    dag=dag,
    application='/lessons/dags/step_2_user_profiles.py',
    conn_id='yarn_spark',
    application_args=[
        "--events_path", events_path,
        "--cities_path", cities_path,
        "--output_path", output_path_1,
        "--date_from", start_date,
        "--date_to", stop_date
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)


# Задача 2: geo_zones.py
geo_zones = SparkSubmitOperator(
    task_id='geo_zones',
    dag=dag,
    application='/lessons/dags/step_3_geo_zones.py',
    conn_id='yarn_spark',
    application_args=[
        "--events_path", events_path,
        "--cities_path", cities_path,
        "--profiles_path", output_path_1,
        "--output_path", output_path_2,
        "--date_from", start_date,
        "--date_to", stop_date
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)


# Задача 3: friend_recommendations.py
friend_recommendations = SparkSubmitOperator(
    task_id='friend_recommendations',
    dag=dag,
    application='/lessons/dags/step_4_friend_recommendations.py',
    conn_id='yarn_spark',
    application_args=[
        "--events_path", events_path,
        "--cities_path", cities_path,
        "--output_path", output_path_3,
        "--date_from", start_date,
        "--date_to", stop_date
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)


# Зависимости
user_profiles >> geo_zones >> friend_recommendations
