import logging
import os
import sys
from pathlib import Path

import pendulum
import psycopg
from pydantic import BaseModel

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader, UserLoader, EventLoader
from lib import ConnectionBuilder, PgConnect
from lib.dict_util import json2str

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint5_example_stg_bonus_system_ranks_dag():
    try:
        # Подключения к базам данных
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    except Exception as e:
        log.error(f"Ошибка подключения к БД: {e}")
        raise

    @task(task_id="ranks_load")
    def load_ranks():
        """Загрузка рангов"""
        try:
            rank_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
            rank_loader.load_ranks()
        except Exception as e:
            log.error(f"Ошибка загрузки рангов: {e}")
            raise

    @task(task_id="users_load")
    def load_users():
        """Загрузка пользователей"""
        try:
            user_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
            user_loader.load_users()
        except Exception as e:
            log.error(f"Ошибка загрузки пользователей: {e}")
            raise

    @task(task_id="events_load")
    def events_load():
        """Загрузка событий"""
        try:
            event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
            event_loader.load_events()
        except Exception as e:
            log.error(f"Ошибка загрузки событий: {e}")
            raise

    # Создаём зависимости
    ranks_task = load_ranks()
    users_task = load_users()
    events_task = events_load()

    ranks_task >> users_task >> events_task  # Сначала загружаем ранги, затем пользователей

stg_bonus_system_ranks_dag = sprint5_example_stg_bonus_system_ranks_dag()