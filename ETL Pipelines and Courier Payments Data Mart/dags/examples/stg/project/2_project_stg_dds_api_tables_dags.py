import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib.dict_util import json2str

# Импортируем классы загрузчиков
from examples.stg.project.project_stg_dds_api_dds_loader import (
    DmUserLoader, DmRestaurantLoader, DmTimestampLoader, 
    DmCourierLoader, DmCourierOrdersLoader, DmDeliveryLoader, 
    DmProductLoader, DmOrderLoader, FctProductSaleLoader, FctCourierPaymentsLoader
)

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага.
    catchup=False,  # Не запускать даг за предыдущие периоды.
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен при создании.
)
def sprint12_project_insert_dds_tables_dag():   # Инициализация подключения к БД
    try:
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        origin_pg_connect = dwh_pg_connect
    except Exception as e:
        log.error(f"Ошибка подключения к БД: {e}")
        raise

    @task(task_id="dm_users_load")    # Загрузка пользователей
    def dm_load_users():
        try:
            user_loader = DmUserLoader(origin_pg_connect, dwh_pg_connect, log)
            user_loader.dm_load_users()
        except Exception as e:
            log.error(f"Ошибка загрузки пользователей: {e}")
            raise

    @task(task_id="dm_restaurant_load")     # Загрузка ресторанов
    def dm_load_restaurants():
        try:
            restaurant_loader = DmRestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
            restaurant_loader.dm_load_restaurants()
        except Exception as e:
            log.error(f"Ошибка загрузки ресторанов: {e}")
            raise

    @task(task_id="dm_load_timestamps")    # Загрузка временных меток
    def dm_load_timestamps():
        try:
            timestamp_loader = DmTimestampLoader(origin_pg_connect, dwh_pg_connect, log)
            timestamp_loader.dm_load_timestamps()
        except Exception as e:
            log.error(f"Ошибка загрузки временных меток: {e}")
            raise

    @task(task_id="api_dm_couriers_load")     # Загрузка курьеров
    def dm_load_couriers():
        try:
            courier_loader = DmCourierLoader(origin_pg_connect, dwh_pg_connect, log)
            courier_loader.dm_load_couriers()
        except Exception as e:
            log.error(f"Ошибка загрузки курьеров: {e}")
            raise

    @task(task_id="api_dm_courier_orders_loader")     # Загрузка курьерских заказов
    def dm_load_courier_orders():
        try:
            delivery_loader = DmCourierOrdersLoader(origin_pg_connect, dwh_pg_connect, log)
            delivery_loader.dm_load_courier_orders()
        except Exception as e:
            log.error(f"Ошибка загрузки курьерских заказов: {e}")
            raise

    @task(task_id="api_dm_deliveries_load")     # Загрузка доставок
    def dm_load_api_deliveries():
        try:
            delivery_loader = DmDeliveryLoader(origin_pg_connect, dwh_pg_connect, log)
            delivery_loader.dm_load_deliveries()
        except Exception as e:
            log.error(f"Ошибка загрузки доставок: {e}")
            raise

    @task(task_id="dm_load_products")     # Загрузка продуктов
    def dm_load_products():
        try:
            product_loader = DmProductLoader(origin_pg_connect, dwh_pg_connect, log)
            product_loader.dm_load_products()
        except Exception as e:
            log.error(f"Ошибка загрузки продуктов: {e}")
            raise

    @task(task_id="dm_orders_loader")     # Загрузка заказов
    def dm_load_orders():
        try:
            order_loader = DmOrderLoader(origin_pg_connect, dwh_pg_connect, log)
            order_loader.dm_load_orders()
        except Exception as e:
            log.error(f"Ошибка загрузки заказов: {e}")
            raise

    @task(task_id="dm_load_fct_product_sales")     # Загрузка таблицы фактов
    def dm_load_fct_product_sales():
        try:
            product_sale_loader = FctProductSaleLoader(origin_pg_connect, dwh_pg_connect, log)
            product_sale_loader.dm_load_fct_product_sales()
        except Exception as e:
            log.error(f"Ошибка загрузки таблицы фактов: {e}")
            raise

    @task(task_id="load_fct_api_courier_payments_load")     # Загрузка таблицы фактов курьерских оплат
    def dm_load_fct_courier_payments():
        try:
            delivery_loader = FctCourierPaymentsLoader(origin_pg_connect, dwh_pg_connect, log)
            delivery_loader.dm_load_fct_courier_payments()
        except Exception as e:
            log.error(f"Ошибка загрузки таблицы фактов курьерских оплат: {e}")
            raise

    # Определяем задачи
    users_task = dm_load_users()
    restaurants_task = dm_load_restaurants()
    timestamps_task = dm_load_timestamps()
    couriers_task = dm_load_couriers()
    courier_orders_task = dm_load_courier_orders()
    deliveries_task = dm_load_api_deliveries()
    products_task = dm_load_products()
    orders_task = dm_load_orders()
    product_sales_task = dm_load_fct_product_sales()
    courier_payments_task = dm_load_fct_courier_payments()

    # Задаем порядок выполнения задач
    users_task >> products_task
    restaurants_task >> products_task
    timestamps_task >> products_task
    couriers_task >> products_task
    courier_orders_task >> products_task
    deliveries_task >> products_task
    products_task >> orders_task
    orders_task >> product_sales_task
    orders_task >> courier_payments_task

# Создаем объект DAG
dds_tables_dag = sprint12_project_insert_dds_tables_dag()