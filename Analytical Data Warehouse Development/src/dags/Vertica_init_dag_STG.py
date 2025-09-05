import logging
import vertica_python
import os
import pendulum
from pathlib import Path
from airflow.decorators import dag, task
from DB_connection import ConnectionBuilder, VerticaConnect


log = logging.getLogger(__name__)


class SchemaDdl:
    def __init__(self, pg: VerticaConnect, log: logging.Logger) -> None:
        self._db = pg
        self.log = log

        """
        Проверяет SQL-скрипт на наличие неподдерживаемых конструкций.
        :param file_path: Путь к SQL-файлу
        :raises ValueError: Если найдены неподдерживаемые конструкции
        """
    def validate_sql(self, file_path: Path) -> None:
        with open(file_path) as f:
            sql = f.read().upper()
            if "IDENTITY(" in sql and "AUTO_INCREMENT" not in sql:
                raise ValueError(f"File {file_path} uses IDENTITY() which might not be supported in your Vertica version")

    def init_schema(self, path_to_scripts: Path, table_order: list) -> None:
        """
        Инициализирует схему если ее нет, выполняя SQL-скрипты из директории.
        :param path_to_scripts: Путь к директории с SQL-скриптами.
        :param table_order: Порядок выполнения скриптов на основе имен таблиц.
        """
        if not path_to_scripts.exists() or not path_to_scripts.is_dir():
            self.log.error(f"Directory {path_to_scripts} does not exist or is not a directory.")
            raise FileNotFoundError(f"Directory {path_to_scripts} not found")

        files = [f for f in os.listdir(path_to_scripts) if f.endswith(".sql")]
        file_paths = sorted((path_to_scripts / f for f in files), key=lambda x: self._sort_key(x.name, table_order))

        self.log.info(f"Found {len(file_paths)} SQL files to apply changes.")

        for i, fp in enumerate(file_paths, start=1):
            try:
                self.log.info(f"Iteration {i}. Validating file {fp.name}")
                self.validate_sql(fp)

                self.log.info(f"Iteration {i}. Applying file {fp.name}")
                script = fp.read_text()

                with self._db.connection() as conn:
                    with conn.cursor() as cur:
                        self.log.debug(f"Executing SQL from {fp.name}:\n{script}")
                        cur.execute(script)
                    conn.commit()

                self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")
            
            except ValueError as e:
                self.log.error(f"Validation error in file {fp.name}: {str(e)}")
                raise
            except vertica_python.errors.VerticaSyntaxError as e:
                self.log.error(f"SQL syntax error in file {fp.name}: {str(e)}")
                raise
            except Exception as e:
                self.log.error(f"Unexpected error executing file {fp.name}: {str(e)}")
                raise

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
    :param schema_name: Имя схемы.
    :param ddl_path: Путь к директории с SQL-скриптами.
    :param table_order: Порядок выполнения скриптов.
    :param tags: Теги для DAG.
    :param dag_id: Уникальный идентификатор DAG.
    :return: Созданный DAG.
    """
    @dag(
        schedule_interval='0 0 * * *',  # Daily at midnight
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=tags,
        dag_id=dag_id
    )
    def schema_creation_dag():
        vertica_connect = ConnectionBuilder.vertica_conn("VERTICA_CONNECTION")
        ddl_path_obj = Path(__file__).parent / ddl_path

        @task(task_id="check_vertica_version")
        def check_version():
            with vertica_connect.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
                    log.info(f"Vertica version: {version}")     

        @task(task_id=f"create_{schema_name}_schema")
        def create_schema():
            try:
                with vertica_connect.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                    conn.commit()
                log.info(f"Schema '{schema_name}' checked/created successfully.")
            except Exception as e:
                log.error(f"Error creating schema {schema_name}: {str(e)}")
                raise

        @task(task_id=f"{schema_name}_schema_init")
        def schema_init():
            try:
                schema_loader = SchemaDdl(vertica_connect, log)
                schema_loader.init_schema(ddl_path_obj, table_order)
            except Exception as e:
                log.error(f"Error initializing schema: {str(e)}")
                raise

        check_version() >> create_schema() >> schema_init()   

    return schema_creation_dag()


# ================== STG SCHEMA CREATION ==================
stg_dag = create_schema_dag(
    schema_name="STV202502272__STAGING",
    ddl_path="stg_init_tables",
    table_order=["schema", "users_csv", "groups_csv", "dialogs_csv", "group_log_csv"],
    tags=['sprint13', 'stg', 'schema', 'ddl'],
    dag_id="stg_schema_creation_tables_dag"
)
