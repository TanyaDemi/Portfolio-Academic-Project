from pathlib import Path
import csv
import os
import logging
import contextlib
from typing import Dict, List, Optional
from datetime import timedelta

import pandas as pd
import boto3
import pendulum
import vertica_python

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

def get_s3_client():
    conn = BaseHook.get_connection('YANDEX_S3_CONNECTION')
    return boto3.client(
        's3',
        endpoint_url=conn.extra_dejson.get('endpoint_url'),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

def get_vertica_conn():
    conn = BaseHook.get_connection("VERTICA_CONNECTION")
    return vertica_python.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        autocommit=True
    )

# Универсальная функция загрузки CSV файла в Vertica
def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None
):
    df = pd.read_csv(
        dataset_path,
        keep_default_na=True,
        na_values=["", "null", "NULL", "NaN"]
    )

    if type_override:
        for col, dtype in type_override.items():
            try:
                # Приведение колонки с поддержкой пропусков (Int64 позволяет хранить NA)
                df[col] = pd.array(df[col], dtype=dtype)
            except Exception as e:
                log.warning(f"Не удалось привести столбец {col} к типу {dtype}: {e}")

    num_rows = len(df)
    chunk_size = max(num_rows // 100, 1)

    with contextlib.closing(get_vertica_conn()) as conn:
        with contextlib.closing(conn.cursor()) as cur:
            # TRUNCATE таблицы перед загрузкой
            cur.execute(f"TRUNCATE TABLE {schema}.{table}")
            
            columns_sql = ', '.join(columns)
            copy_expr = f"""
            COPY {schema}.{table} ({columns_sql})
            FROM STDIN DELIMITER ',' NULL AS '\\N'
            """
            for start in range(0, num_rows, chunk_size):
                end = min(start + chunk_size, num_rows)
                chunk = df.iloc[start:end]
                chunk = chunk.astype(str)
                chunk = chunk.replace({'nan': '\\N', 'NaT': '\\N'})
                csv_data = chunk.to_csv(
                    index=False,
                    header=False,
                    na_rep='\\N',
                    quoting=csv.QUOTE_NONE,
                    escapechar='\\'
                )
                cur.copy(copy_expr, csv_data)
                log.info(f"Загружены строки {start}-{end} в таблицу {table}")
        conn.commit()
        log.info(f"Загрузка таблицы {table} завершена.")

@dag(
    dag_id='s3_to_vertica_group_log',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['s6', 'yandex'],
    is_paused_upon_creation=True,
)
def s3_to_vertica_group_log():

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def download_group_log_file() -> str:
        file_name = 'group_log.csv'
        s3_client = get_s3_client()
        output_dir = Path("/tmp")
        output_dir.mkdir(exist_ok=True)
        file_path = output_dir / file_name

        try:
            s3_client.head_object(Bucket='sprint6', Key=file_name)
            s3_client.download_file(Bucket='sprint6', Key=file_name, Filename=str(file_path))
            if not file_path.exists():
                raise AirflowException(f"Файл {file_path} не найден после загрузки.")
            log.info(f"Файл {file_name} успешно загружен.")
            return str(file_path)
        except Exception as e:
            log.error(f"Ошибка при загрузке {file_name}: {e}")
            raise AirflowException(f"Ошибка загрузки {file_name}: {e}")

    @task(retries=3, retry_delay=timedelta(minutes=3))
    def load_group_log_to_vertica(file_path: str):
        load_dataset_file_to_vertica(
            dataset_path=file_path,
            schema='STV202502272__STAGING',
            table='group_log_csv',
            columns=['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],
            type_override={'user_id_from': 'Int64'}
        )

    file_path = download_group_log_file()
    load_task = load_group_log_to_vertica(file_path)

    start >> file_path >> load_task >> end

dag = s3_to_vertica_group_log()
