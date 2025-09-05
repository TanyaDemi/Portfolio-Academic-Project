import logging
import boto3
import os
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from pathlib import Path

log = logging.getLogger(__name__)

@dag(
    schedule_interval=None,  
    start_date=datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['s6', 'yandex'],
    dag_id='dags_for_groups',
    is_paused_upon_creation=True
)
def s3_download_dags_for_groups():
    def get_s3_client():
        conn = BaseHook.get_connection('YANDEX_S3_CONNECTION')
        return boto3.client(
            's3',
            endpoint_url=conn.extra_dejson.get('endpoint_url'),
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password
        )

    @task
    def download_file_from_s3(file_name: str):
        try:
            s3_client = get_s3_client()
            output_dir = Path('/data')
            output_dir.mkdir(exist_ok=True)
            file_path = output_dir / file_name

            # Проверяем существование файла в S3
            s3_client.head_object(Bucket='sprint6', Key=file_name)
            
            # Скачиваем файл
            s3_client.download_file(
                Bucket='sprint6',
                Key=file_name,
                Filename=str(file_path)
            )
            
            if not os.path.exists(file_path):
                raise AirflowException(f"Файл {file_path} не был создан!")
            
            log.info(f"Файл {file_name} успешно загружен в {file_path}!")
            log.info(f"Файл сохранён по пути: {file_path}")
            return str(file_path)
            
        except Exception as e:
            log.error(f"Ошибка при загрузке файла {file_name}: {e}")
            raise AirflowException(f"Не удалось загрузить файл {file_name}: {e}")

    # Создаем задачи для каждого файла
    download_groups = download_file_from_s3.override(task_id='download_groups')('groups.csv')
    download_users = download_file_from_s3.override(task_id='download_users')('users.csv')
    download_dialogs = download_file_from_s3.override(task_id='download_dialogs')('dialogs.csv')
    download_group_log = download_file_from_s3.override(task_id='download_group_log')('group_log.csv')

dag = s3_download_dags_for_groups()