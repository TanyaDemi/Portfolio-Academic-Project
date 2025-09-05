import json
import logging
import time
from datetime import datetime
from typing import Dict, Generator, List, Optional, Tuple
from contextlib import contextmanager

import vertica_python
import psycopg
from tqdm import tqdm
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

class VerticaConnect:
    def __init__(self, conn_id: str = "Vertica_DB") -> None:
        self.conn_id = conn_id
        self.retry_count = 3
        self.retry_delay = 5

    def _conn_info(self) -> dict:
        conn = BaseHook.get_connection(self.conn_id)
        extra = {}
        try:
            extra = json.loads(conn.extra) if conn.extra else {}
        except json.JSONDecodeError:
            log.warning(f"Invalid JSON in extra for connection {self.conn_id}")
        
        return {
            "host": conn.host,
            "port": conn.port or 5433,
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
            "connection_timeout": extra.get("connection_timeout", 30),
            "read_timeout": extra.get("read_timeout", 60),
            "ssl": extra.get("ssl", False),
            "backup_server_node": extra.get("backup_server_node", []),
            **extra
        }

    @contextmanager
    def connection(self):
        conn = None
        last_error = None
        
        for attempt in range(self.retry_count):
            try:
                conn = vertica_python.connect(**self._conn_info())
                yield conn
                return
            except Exception as e:
                last_error = e
                log.error(f"Vertica connection attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
            finally:
                if conn:
                    conn.close()
        
        log.error(f"All connection attempts failed for Vertica")
        raise vertica_python.errors.ConnectionError(
            f"Failed to establish Vertica connection after {self.retry_count} attempts. Last error: {last_error}"
        )

class PgConnect:
    def __init__(
        self,
        host: str,
        port: str,
        db_name: str,
        user: str,
        pw: str,
        sslmode: str = "require",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.db_name} "
            f"user={self.user} "
            f"password={self.pw} "
            f"target_session_attrs=read-write "
            f"sslmode={self.sslmode}"
        )

    def _get_row_count(self, query: str) -> int:        
        count_query = f"SELECT COUNT(*) FROM ({query}) AS subquery"
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(count_query)
                return cur.fetchone()[0]

    def _get_date_ranges(self, date_column: str, table: str) -> List[Tuple[datetime, datetime]]:
        """Возвращает список диапазонов дат для пакетной обработки"""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MIN({date_column}), MAX({date_column}) 
                    FROM {table}
                """)
                min_date, max_date = cur.fetchone()
                
                if not min_date or not max_date:
                    return []
                
                # Разбиваем на месячные интервалы
                ranges = []
                current = min_date
                while current <= max_date:
                    next_month = (current.replace(day=1) + timedelta(days=32))
                    end = min(next_month.replace(day=1) - timedelta(days=1), max_date)
                    ranges.append((current, end))
                    current = end + timedelta(days=1)
                
                return ranges

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = None
        try:
            conn = psycopg.connect(self.url())
            yield conn
            conn.commit()
        except Exception as e:
            log.error(f"Postgres connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

class ConnectionBuilder:
    @staticmethod
    def vertica_conn(conn_id: str = "Vertica_DB") -> VerticaConnect:
        return VerticaConnect(conn_id)

    @staticmethod
    def pg_conn(conn_id: str = "Postgres_DB") -> PgConnect:
        conn = BaseHook.get_connection(conn_id)
        extra = {}
        try:
            extra = json.loads(conn.extra) if conn.extra else {}
        except json.JSONDecodeError:
            log.warning(f"Invalid JSON in extra for connection {conn_id}")
        
        sslmode = extra.get("sslmode", "require")

        return PgConnect(
            host=str(conn.host),
            port=str(conn.port),
            db_name=str(conn.schema),
            user=str(conn.login),
            pw=str(conn.password),
            sslmode=sslmode,
        )

class DataTransfer:
    def __init__(
        self,
        pg: PgConnect,
        vt: VerticaConnect,
        logger: logging.Logger = log,
    ):
        self.pg = pg
        self.vt = vt
        self.log = logger

    # Динамически определяет размер батча в зависимости от объема данных
    def _get_batch_size(self, row_count: int) -> int:
        if row_count < 10_000:
            return 10_000
        elif row_count < 100_000:
            return 50_000
        elif row_count < 1_000_000:
            return 100_000
        else:
            return 200_000

    def copy_table_pg_to_vertica(
        self,
        pg_query: str,
        vertica_table: str,
        vertica_columns: List[str],
        delimiter: str = ",",
        batch_size: Optional[int] = None,
        vertica_truncate: bool = False,
    ) -> None:
        try:
            total_rows = self.pg._get_row_count(pg_query)
            batch_size = batch_size or self._get_batch_size(total_rows)
            
            self.log.info(f"Starting transfer of {total_rows:,} rows to {vertica_table} (batch size: {batch_size:,})")
            
            col_list = ", ".join(vertica_columns)
            copy_to_sql = f"COPY ({pg_query}) TO STDOUT WITH (FORMAT csv, DELIMITER '{delimiter}')"
            copy_from_sql = f"COPY {vertica_table} ({col_list}) FROM STDIN DELIMITER '{delimiter}' NULL '' DIRECT"

            with tqdm(total=total_rows, desc="Transferring rows") as pbar:
                with self.pg.connection() as pg_conn, self.vt.connection() as vt_conn:
                    with pg_conn.cursor() as pg_cur, vt_conn.cursor() as vt_cur:
                        if vertica_truncate:
                            self.log.info("Truncating target table")
                            vt_cur.execute(f"TRUNCATE TABLE {vertica_table}")

                        pg_copy = pg_cur.copy(copy_to_sql)
                        vt_copy = vt_cur.copy(copy_from_sql)

                        rows_transferred = 0
                        while True:
                            data = pg_copy.read(batch_size)
                            if not data:
                                break
                            vt_copy.write(data)
                            chunk_rows = len(data.decode('utf-8').split('\n')) - 1
                            rows_transferred += chunk_rows
                            pbar.update(chunk_rows)

                        vt_copy.close()
                        self.log.info(f"Completed: {rows_transferred:,} rows transferred")
                        
        except Exception as e:
            self.log.error(f"Error during data transfer: {e}")
            raise

    def upsert_table_pg_to_vertica(
        self,
        pg_query: str,
        target_table: str,
        merge_keys: List[str],
        columns: List[str],
        column_types: Dict[str, str],
        delimiter: str = ",",
        staging_schema: Optional[str] = None,
        batch_by_date: Optional[str] = None,
    ) -> None:
        try:
            total_rows = self.pg._get_row_count(pg_query)
            self.log.info(f"Starting upsert of {total_rows:,} rows to {target_table}")

            if batch_by_date and total_rows > 100_000:
                # Пакетная обработка по датам для больших таблиц
                date_ranges = self.pg._get_date_ranges(batch_by_date, f"({pg_query}) AS subq")
                
                if date_ranges:
                    self.log.info(f"Processing in {len(date_ranges)} date batches")
                    
                    for start_date, end_date in date_ranges:
                        batch_query = f"""
                            SELECT * FROM ({pg_query}) AS subq
                            WHERE {batch_by_date} BETWEEN '{start_date}' AND '{end_date}'
                        """
                        self._single_upsert(
                            batch_query, target_table, merge_keys, 
                            columns, column_types, delimiter, staging_schema
                        )
                    return
            
            # Обычная обработка для маленьких таблиц
            self._single_upsert(
                pg_query, target_table, merge_keys, 
                columns, column_types, delimiter, staging_schema
            )
            
        except Exception as e:
            self.log.error(f"Upsert operation failed: {e}")
            raise

    def _single_upsert(
        self,
        pg_query: str,
        target_table: str,
        merge_keys: List[str],
        columns: List[str],
        column_types: Dict[str, str],
        delimiter: str,
        staging_schema: Optional[str],
    ) -> None:
        """Выполняет единичный upsert без пакетной обработки"""
        staging_tbl = (
            f"{staging_schema}.tmp_{target_table.replace('.', '_')}"
            f"_{int(datetime.now().timestamp())}"
            if staging_schema
            else f"v_temp_schema.tmp_{target_table.replace('.', '_')}"
            f"_{int(datetime.now().timestamp())}"
        )
        
        col_defs = ", ".join([f"{col} {column_types[col]}" for col in columns])
        col_list = ", ".join(columns)
        key_cond = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        upd_set = ", ".join([f"{col} = s.{col}" for col in columns if col not in merge_keys])

        # Создаем временную таблицу с явными типами
        with self.vt.connection() as vt_conn, vt_conn.cursor() as cur:
            self.log.info(f"Creating staging table {staging_tbl}")
            cur.execute(f"DROP TABLE IF EXISTS {staging_tbl}")
            cur.execute(f"CREATE TEMP TABLE {staging_tbl} ({col_defs}) ON COMMIT PRESERVE ROWS")

        # Копируем данные
        self.copy_table_pg_to_vertica(
            pg_query=pg_query,
            vertica_table=staging_tbl,
            vertica_columns=columns,
            delimiter=delimiter,
        )

        # Выполняем MERGE
        merge_sql = f"""
        MERGE INTO {target_table} AS t
        USING {staging_tbl} AS s
        ON {key_cond}
        WHEN MATCHED THEN UPDATE SET {upd_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES (s.{col_list})
        """

        with self.vt.connection() as vt_conn, vt_conn.cursor() as cur:
            try:
                self.log.info(f"Executing MERGE into {target_table}")
                cur.execute(merge_sql)
                self.log.info(f"MERGE finished, {cur.rowcount} row(s) affected")
                cur.execute(f"DROP TABLE IF EXISTS {staging_tbl}")
            except Exception as e:
                self.log.error(f"Error during MERGE operation: {e}")
                raise