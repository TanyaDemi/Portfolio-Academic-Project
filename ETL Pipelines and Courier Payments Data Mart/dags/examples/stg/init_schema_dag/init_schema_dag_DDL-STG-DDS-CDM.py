import logging
import os
from pathlib import Path
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder, PgConnect

log = logging.getLogger(__name__)


class SchemaDdl:
    def __init__(self, pg: PgConnect, log: logging.Logger) -> None:
        self._db = pg
        self.log = log

    def init_schema(self, path_to_scripts: Path, table_order: list) -> None:
        """
        Инициализирует схему, выполняя SQL-скрипты из указанной директории.
        :param path_to_scripts: Путь к директории с SQL-скриптами.
        :param table_order: Порядок выполнения скриптов на основе имен таблиц.
        """
        if not path_to_scripts.exists() or not path_to_scripts.is_dir():
            self.log.error(f"Directory {path_to_scripts} does not exist or is not a directory.")
            return

        files = [f for f in os.listdir(path_to_scripts) if f.endswith(".sql")]
        file_paths = sorted((path_to_scripts / f for f in files), key=lambda x: self._sort_key(x.name, table_order))

        self.log.info(f"Found {len(file_paths)} SQL files to apply changes.")

        for i, fp in enumerate(file_paths, start=1):
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            script = fp.read_text()

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)
                conn.commit()

            self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")

    def _sort_key(self, filename: str, table_order: list) -> int:
        """
        Определяет порядок выполнения файла, основываясь на имени таблицы.
        :param filename: Имя файла.
        :param table_order: Порядок таблиц.
        :return: Индекс для сортировки.
        """
        for idx, table in enumerate(table_order):
            if table in filename:
                return idx
        return len(table_order)  # Файлы, не соответствующие таблицам, идут в конец


def create_schema_dag(schema_name: str, ddl_path: str, table_order: list, tags: list, dag_id: str):
    """
    Создает DAG для инициализации схемы.

    :param schema_name: Имя схемы (например, 'stg', 'dds', 'cdm').
    :param ddl_path: Путь к директории с SQL-скриптами.
    :param table_order: Порядок выполнения скриптов.
    :param tags: Теги для DAG.
    :param dag_id: Уникальный идентификатор DAG.
    :return: Созданный DAG.
    """
    @dag(
        schedule_interval='0/15 * * * *',  # Каждый 15 минут
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=tags,
        is_paused_upon_creation=True,
        dag_id=dag_id
    )
    def schema_creation_dag():
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        ddl_path_obj = Path(__file__).parent / ddl_path

        @task(task_id=f"create_{schema_name}_schema")
        def create_schema():
            with dwh_pg_connect.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                conn.commit()
            log.info(f"Schema '{schema_name}' checked/created successfully.")

        @task(task_id=f"{schema_name}_schema_init")
        def schema_init():
            schema_loader = SchemaDdl(dwh_pg_connect, log)
            schema_loader.init_schema(ddl_path_obj, table_order)

        create_schema_task = create_schema()
        init_schema_task = schema_init()
        create_schema_task >> init_schema_task

    return schema_creation_dag()


# ================== STG SCHEMA CREATION ==================
stg_dag = create_schema_dag(
    schema_name="stg",
    ddl_path="ddl",
    table_order=[
        "schema-create", "srv_wf_settings", 
        "bonussystem_ranks", "bonussystem_users", "bonussystem_events",
        "ordersystem_restaurants", "ordersystem_orders", "ordersystem_users",
        "api_restaurants", "api_couriers", "api_deliveries"
    ],
    tags=['sprint5', 'stg', 'schema', 'ddl', 'example'],
    dag_id="schema1_creating_stg_layers_tables_dag"
)

# ================== DDS SCHEMA CREATION ==================
dds_dag = create_schema_dag(
    schema_name="dds",
    ddl_path="dds",
    table_order=[
        "schema-create", "srv_wf_settings", 
        "dm_users", "dm_restaurants", "dm_timestamps", "api_dm_couriers",
        "api_dm_couriers_orders_sum", "api_dm_deliveries", 
        "dm_products", "dm_orders", "fct_product_sales", "fct_api_courier_payments"
    ],
    tags=['sprint5', 'dds', 'schema', 'ddl', 'example'],
    dag_id="schema2_creating_dds_layers_tables_dag"
)

# ================== CDM SCHEMA CREATION ==================
cdm_dag = create_schema_dag(
    schema_name="cdm",
    ddl_path="cdm",
    table_order=["schema-create", "srv_wf_settings", "dm_settlement_report", "dm_courier_ledger"],
    tags=['sprint5', 'cdm', 'schema', 'ddl', 'example'],
    dag_id="schema3_creating_cdm_layers_tables_dag"
)