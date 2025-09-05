import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaverRestaurant, PgSaverUser, PgSaverOrder
from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader, UserLoader, OrderLoader
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader, UserReader, OrderReader
from lib import ConnectionBuilder, MongoConnect
import sys
import os

sys.path.insert(0, "/lessons/dags/examples/stg")

from lib import PgConnect  # Теперь импорт должен работать


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_order_system_restaurants():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverRestaurant()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_users():
        pg_saver = PgSaverUser()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        user_reader = UserReader(mongo_connect)
        user_loader = UserLoader(user_reader, dwh_pg_connect, pg_saver, log)
        user_loader.run_copy()

    @task()
    def load_orders():
        pg_saver = PgSaverOrder()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        order_reader = OrderReader(mongo_connect)
        order_loader = OrderLoader(order_reader, dwh_pg_connect, pg_saver, log)
        order_loader.run_copy()

    restaurant_loader = load_restaurants()
    user_loader = load_users()
    order_loader = load_orders()

    restaurant_loader >> user_loader >> order_loader

order_stg_dag = sprint5_example_stg_order_system_restaurants()
