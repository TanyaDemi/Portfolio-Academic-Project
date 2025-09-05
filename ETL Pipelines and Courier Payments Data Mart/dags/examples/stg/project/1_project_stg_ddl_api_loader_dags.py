import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from lib import ConnectionBuilder, PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from datetime import datetime
from typing import List, Dict, Any
import requests
import json
from examples.stg.stg_settings_repository import StgEtlSetting, StgEtlSettingsRepository
from bson.objectid import ObjectId
from bson import json_util

# Логирование
log = logging.getLogger(__name__)

# Заголовки для API
HEADERS = {
    "X-Nickname": "tani-astroblog",
    "X-Cohort": "1",
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
}

# Подключение к БД
dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

# Класс для работы с API и загрузки данных в PostgreSQL
class ApiToPostgresLoader:
    def __init__(self, api_url: str, api_headers: Dict[str, str], pg_conn: Connection):
        self.api_url = api_url
        self.api_headers = api_headers
        self.pg_conn = pg_conn
        self.settings_repository = StgEtlSettingsRepository()

    # Получение данных с API. Выполняет запрос к API и возвращает данные.
    def fetch_data_from_api(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            url = f"{self.api_url}{endpoint}"
            response = requests.get(url, headers=self.api_headers, params=params)
            response.raise_for_status()  # Проверка на ошибки HTTP
            return response.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Ошибка при запросе к API: {e}")
            raise

    # Получение всех данных с API с постраничным чтением. Выполняет постраничное чтение данных из API.
    def fetch_all_data_from_api(self, endpoint: str, params: Dict[str, Any], workflow_key: str) -> List[Dict[str, Any]]:
        all_data = []
        offset = 0
        limit = params.get("limit", 50)  # По умолчанию limit=50

        while True:
            # Устанавливаем текущий offset
            params["offset"] = offset

            # Загружаем данные с API
            data = self.fetch_data_from_api(endpoint, params)
            if not data:
                break  # Если данных больше нет, выходим из цикла

            # Добавляем загруженные данные в общий список
            all_data.extend(data)

            # Сохраняем текущий offset в таблицу stg.srv_wf_settings
            self.save_last_loaded_ts(workflow_key, {"last_offset": offset, "last_loaded_ts": datetime.now().isoformat()})

            # Увеличиваем offset для следующей итерации
            offset += limit

        return all_data

    # Извлечение restaurant_id из таблицы stg.api_restaurants.
    def get_restaurant_ids(self) -> List[str]:
        with self.pg_conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT object_id FROM stg.api_restaurants;")
                rows = cur.fetchall()
                return [row[0] for row in rows]  # Возвращаем список restaurant_id

    # Сохранение данных в PostgreSQL.
    def save_to_postgres(self, table_name: str, data: List[Dict[str, Any]]) -> None:      
        with self.pg_conn.connection() as conn:          
            with conn.cursor() as cur:
                for record in data:
                    str_val = json2str(record)
                    cur.execute(
                        f"""
                        INSERT INTO {table_name} (object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                        """,
                        {
                            "id": record.get("_id"),
                            "val": str_val,
                            "update_ts": datetime.now()
                        }
                    )
                conn.commit()

    # Загрузка данных о доставках.
    def save_to_postgres_delivery(self, table_name: str, data: List[Dict[str, Any]], restaurant_id: str) -> None:
        with self.pg_conn.connection() as conn:
            with conn.cursor() as cur:
                for record in data:
                    object_id = record.get("order_id")
                    if not object_id:  # Проверка на null или отсутствие order_id
                        log.warning(f"Пропущена запись с отсутствующим order_id: {record}")
                        continue

                    # Преобразуем весь объект в JSON-строку для object_value
                    str_val = json2str(record)

                    cur.execute(
                        f"""
                        INSERT INTO {table_name} (restaurant_id, object_id, object_value, update_ts)
                        VALUES (%(restaurant_id)s, %(object_id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                        """,
                        {
                            "restaurant_id": restaurant_id,  # Передаем restaurant_id
                            "object_id": object_id,  # Используем order_id
                            "val": str_val,  # Весь объект в формате JSON
                            "update_ts": datetime.now()  # Текущее время
                        }
                    )
                conn.commit()

    # Сохранение даты последней загрузки в таблицу stg.srv_wf_settings.
    def save_last_loaded_ts(self, workflow_key: str, last_loaded_ts: Dict[str, Any]) -> None:
        with self.pg_conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%(workflow_key)s, %(workflow_settings)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET
                        workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    {
                        "workflow_key": workflow_key,
                        "workflow_settings": json.dumps(
                            {k: (v.isoformat() if isinstance(v, (datetime, pendulum.DateTime)) else v) 
                            for k, v in last_loaded_ts.items()}
                        )
                    }
                )
            conn.commit()



# Создаем загрузчик API → Postgres
api_loader = ApiToPostgresLoader(
    api_url="https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net",
    api_headers=HEADERS,
    pg_conn=dwh_pg_connect
)

@dag(
    schedule_interval='10 0 * * *',  # Запуск каждый день в 00:10 UTC или schedule_interval='0 0 * * 1' запуск каждую неделю в понедельник в 00:00 UTC
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "api", "postgres"],
    is_paused_upon_creation=True
)
def sprint12_project_stg_api_data_loading():

    @task()
    def load_restaurants():
        log.info("Начало загрузки данных о ресторанах")
        last_loaded_ts = pendulum.now("UTC")

        data = api_loader.fetch_all_data_from_api(
            endpoint="/restaurants",
            params={"sort_field": "name", "sort_direction": "asc", "limit": 50},
            workflow_key="api_restaurants_workflow"
        )
        log.info(f"Загружено {len(data)} записей о ресторанах")
        api_loader.save_to_postgres("stg.api_restaurants", data)
        api_loader.save_last_loaded_ts("api_restaurants_workflow", {"last_loaded_ts": last_loaded_ts.isoformat()})
        log.info("Данные о ресторанах сохранены в PostgreSQL")

    @task()
    def load_couriers():
        log.info("Начало загрузки данных о курьерах")
        last_loaded_ts = pendulum.now("UTC")

        data = api_loader.fetch_all_data_from_api(
            endpoint="/couriers",
            params={"sort_field": "name", "sort_direction": "asc", "limit": 50},
            workflow_key="api_couriers_workflow"
        )
        log.info(f"Загружено {len(data)} записей о курьерах")
        api_loader.save_to_postgres("stg.api_couriers", data)
        api_loader.save_last_loaded_ts("api_couriers_workflow", {"last_loaded_ts": last_loaded_ts.isoformat()})

        log.info("Данные о курьерах сохранены в PostgreSQL")

    @task()
    def load_deliveries():
        log.info("Начало загрузки данных о доставках")
        last_loaded_ts = pendulum.now("UTC")

        end_date = pendulum.now("UTC")
        start_date = end_date.subtract(days=7)
        
        log.info(f"Загрузка данных о доставках за период с {start_date} по {end_date}")

        restaurant_ids = api_loader.get_restaurant_ids()
        log.info(f"Извлечены restaurant_id: {restaurant_ids}")

        for restaurant_id in restaurant_ids:
            log.info(f"Загрузка данных для ресторана {restaurant_id}")
            data = api_loader.fetch_all_data_from_api(
                endpoint="/deliveries",
                params={
                    "restaurant_id": restaurant_id,
                    "from": start_date.to_datetime_string(),
                    "to": end_date.to_datetime_string(),
                    "sort_field": "date",
                    "sort_direction": "asc",
                    "limit": 50
                },
                workflow_key=f"api_deliveries_workflow_{restaurant_id}"
            )
            log.info(f"Загружено {len(data)} записей о доставках для ресторана {restaurant_id}")
            api_loader.save_to_postgres_delivery("stg.api_deliveries", data, restaurant_id)

        api_loader.save_last_loaded_ts("api_deliveries_workflow", {
            "last_loaded_ts": last_loaded_ts.isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        })
        log.info("Данные о доставках сохранены в PostgreSQL")

    load_restaurants() >> load_deliveries()
    load_couriers()

sprint12_project_stg_api_data_loading_dag = sprint12_project_stg_api_data_loading()