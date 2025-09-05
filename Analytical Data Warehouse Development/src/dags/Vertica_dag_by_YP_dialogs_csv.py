from pathlib import Path
import os
import logging
import csv
from typing import List
import boto3
import pendulum
import vertica_python
import contextlib

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

TABLE_NAME = "STV202502272__STAGING.dialogs_csv"
S3_BUCKET = "sprint6"
S3_FILE_KEY = "dialogs.csv"

def get_s3_client():
    conn = BaseHook.get_connection('YANDEX_S3_CONNECTION')
    return boto3.client(
        's3',
        endpoint_url=conn.extra_dejson.get('endpoint_url'),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

def get_vertica_conn(autocommit=False):
    conn = BaseHook.get_connection("VERTICA_CONNECTION")
    return vertica_python.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        autocommit=autocommit
    )

def execute_sql_script(conn, sql_script):
    with conn.cursor() as cur:
        try:
            for statement in sql_script.split(';'):
                if statement.strip():
                    cur.execute(statement)
        except Exception as e:
            conn.rollback()
            raise AirflowException(f"SQL execution error: {e}")

@task()
def download_dialogs_file() -> str:
    file_path = Path("/tmp") / S3_FILE_KEY
    s3_client = get_s3_client()

    try:
        log.info(f"Скачиваем файл {S3_FILE_KEY} в {file_path}")
        s3_client.head_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY)
        s3_client.download_file(Bucket=S3_BUCKET, Key=S3_FILE_KEY, Filename=str(file_path))
        if not os.path.exists(file_path):
            raise AirflowException(f"Файл {file_path} не найден после загрузки.")
        log.info(f"Файл успешно загружен: {file_path}")
        return str(file_path)
    except Exception as e:
        log.error(f"Ошибка загрузки {S3_FILE_KEY}: {e}")
        raise AirflowException(f"Ошибка загрузки {S3_FILE_KEY}: {e}")

@task()
def preprocess_dialogs_file(input_file: str) -> str:
    """
    Предварительное преобразование: читаем исходный CSV и очищаем колонку message от опасных символов.
    Записываем результат в новый файл.
    """
    log.info("Начинаем препроцессинг файла для очистки опасных символов")
    input_path = Path(input_file)
    output_path = input_path.with_name("dialogs_clean.csv")
    
    try:
        with open(input_path, "r", encoding="utf-8", errors="replace") as infile, \
             open(output_path, "w", newline="", encoding="utf-8") as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
            
            # Читаем заголовок и записываем его без изменений
            header = next(reader, None)
            if header:
                writer.writerow(header)
            
            for row in reader:
                if len(row) < 6:
                    # Если строка имеет меньше 6 колонок, пропускаем или логируем
                    log.warning(f"Строка с недостаточным количеством колонок: {row}")
                    continue
                    
                # Предполагаем, что колонка message имеет индекс 4.
                message = row[4]
                # Заменяем опасные символы на пробел или другой подходящий символ
                cleaned_message = message.replace("\x01", " ").replace("\0", " ").replace("\r", " ").replace("\n", " ")
                row[4] = cleaned_message
                writer.writerow(row)
        
        log.info(f"Препроцессинг завершён. Создан файл: {output_path}")
        return str(output_path)
    except Exception as e:
        log.error(f"Ошибка препроцессинга файла: {e}")
        raise AirflowException(str(e))

@task()
def load_dialogs_to_vertica(file_path: str):
    log.info(f"Загружаем файл напрямую в таблицу {TABLE_NAME}: {file_path}")
    
    schema, table = TABLE_NAME.split('.')  # Разбиваем на схему и имя таблицы

    try:
        with contextlib.closing(get_vertica_conn(autocommit=True)) as conn:
            with contextlib.closing(conn.cursor()) as cur:
                log.info(f"Выполняем TRUNCATE TABLE {schema}.{table}")
                cur.execute(f"TRUNCATE TABLE {schema}.{table}")
                
                sql = f"""
                COPY {schema}.{table} 
                (message_id, message_ts, message_from, message_to, message, message_group)
                FROM LOCAL '{file_path}' 
                DELIMITER ',' 
                ENCLOSED BY '"' 
                ESCAPE AS '\\' 
                SKIP 1;
                """
                log.info(f"Выполняем загрузку COPY в {schema}.{table}")
                cur.execute(sql)
                log.info("Данные успешно загружены")
    except Exception as e:
        log.error(f"Ошибка при загрузке: {e}")
        raise AirflowException(str(e))

@dag(
    dag_id='s3_to_vertica_dialogs',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['s6', 'yandex', 'super_csv'],
    is_paused_upon_creation=True,
)
def s3_to_vertica_dialogs():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Скачиваем исходный файл
    raw_file = download_dialogs_file()
    # Применяем предварительный preprocessing для очистки опасных символов в колонке message
    clean_file = preprocess_dialogs_file(raw_file)
    # Загружаем очищенный файл в Vertica
    load = load_dialogs_to_vertica(clean_file)

    start >> raw_file >> clean_file >> load >> end

dag = s3_to_vertica_dialogs()
