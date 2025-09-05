from pathlib import Path
import pendulum
import logging
from airflow.decorators import dag, task
from DB_connection import ConnectionBuilder, VerticaConnect


log = logging.getLogger(__name__)


class SchemaDDL:
    def __init__(self, vertica: VerticaConnect, logger: logging.Logger):
        self._db = vertica
        self._log = logger

    @staticmethod
    def _validate_sql(sql_text: str, file_name: str) -> None:
        if "IDENTITY(" in sql_text.upper() and "AUTO_INCREMENT" not in sql_text.upper():
            raise ValueError(
                f"{file_name}: keyword IDENTITY() не поддерживается текущей версией Vertica"
            )

    def apply_folder(self, folder: Path, file_mask: list[str]) -> None:
        if not folder.is_dir():
            raise FileNotFoundError(f"Directory {folder} not found")

        sql_files = sorted(
            [p for p in folder.glob("*.sql") if any(m in p.name for m in file_mask)],
            key=lambda p: p.name
        )

        self._log.info(f"Found {len(sql_files)} SQL files")
        
        for fp in sql_files:
            sql_text = fp.read_text(encoding="utf-8")
            self._validate_sql(sql_text, fp.name)
            
            try:
                with self._db.connection() as conn, conn.cursor() as cur:
                    cur.execute(sql_text)
                self._log.info(f"Successfully applied {fp.name}")
            except Exception as e:
                self._log.error(f"Error applying {fp.name}: {str(e)}")
                raise

@dag(
    dag_id="0_data_init_table",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["staging", "dwh", "Final_Project"]
)

def init_all_schemas():
    vertica_conn = ConnectionBuilder.vertica_conn("Vertica_DB")
    sql_dir = Path(__file__).parent.parent / "sql"
    loader = SchemaDDL(vertica_conn, log)

    def execute_sql(sql: str):
        try:
            with vertica_conn.connection() as conn, conn.cursor() as cur:
                cur.execute(sql)
            return True
        except Exception as e:
            log.error(f"SQL execution failed: {str(e)}")
            raise

    @task
    def create_schema_stg():
        execute_sql("CREATE SCHEMA IF NOT EXISTS STV202506166__STAGING")

    @task
    def create_schema_dwh():
        execute_sql("CREATE SCHEMA IF NOT EXISTS STV202506166__DWH")

    @task
    def apply_stg():
        loader.apply_folder(sql_dir, ["1_stg"])

    @task
    def apply_dwh():
        loader.apply_folder(sql_dir, ["2_dwh"])

    # Порядок выполнения
    create_stg_task = create_schema_stg()
    create_dwh_task = create_schema_dwh()
    
    apply_stg_task = apply_stg()
    apply_dwh_task = apply_dwh()

    create_stg_task >> apply_stg_task
    create_dwh_task >> apply_dwh_task

init_all_schemas_dag = init_all_schemas()
