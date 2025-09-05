import logging
import json
import pendulum
from datetime import date
from typing import List
from pydantic import BaseModel
from airflow.decorators import dag, task
from lib import ConnectionBuilder, PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from airflow.models.variable import Variable
from logging import Logger

from examples.stg.stg_settings_repository import CdmEtlSetting, CdmEtlSettingsRepository

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 0 10 * *',  # Задаем расписание выполнения дага каждый месяц 10 го числа.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dwh', 'origin', 'example'],
    is_paused_upon_creation=True
)
def project_dwh_cdm_tables_dag_loader():
    try:
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        origin_pg_connect = dwh_pg_connect
    except Exception as e:
        log.error(f"Ошибка подключения к БД: {e}")
        raise
    
    @task(task_id="cdm_settlement_report_load")
    def cdm_load_settlement_report():
        try:
            report_loader = ReportLoader(origin_pg_connect, dwh_pg_connect, log)
            report_loader.cdm_load_reports()
        except Exception as e:
            log.error(f"Ошибка загрузки отчетов о количестве заказов за период (месяц): {e}")
            raise

 
    @task(task_id="cdm_courier_ledger_load")
    def cdm_courier_ledger_load():
        try:
            courier_loader = CourierLedgerLoader(origin_pg_connect, dwh_pg_connect, log)
            courier_loader.cdm_load_courier_ledger()
        except Exception as e:
            log.error(f"Ошибка загрузки отчетов о расчетах с курьерами за период (месяц): {e}")
            raise

    settlement_report_task = cdm_load_settlement_report()
    courier_ledger_task = cdm_courier_ledger_load()

    settlement_report_task >> courier_ledger_task

dwh_tables_dag_loader = project_dwh_cdm_tables_dag_loader()

class ReportObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float

class ReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_report(self, report_threshold: int, limit: int) -> List[ReportObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    o.id,
                    r.restaurant_id,
                    r.restaurant_name,
                    t.date AS settlement_date,
                    COUNT(DISTINCT o.id) AS orders_count,
                    SUM(f.total_sum) AS orders_total_sum,
                    SUM(f.bonus_payment) AS orders_bonus_payment_sum,
                    SUM(f.bonus_grant) AS orders_bonus_granted_sum,
                    SUM(f.total_sum) * 0.25 AS order_processing_fee,
                    SUM(f.total_sum - (f.total_sum * 0.25) - f.bonus_payment) AS restaurant_reward_sum
                FROM dds.dm_orders o
                JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                JOIN dds.fct_product_sales f ON o.id = f.order_id
                WHERE o.order_status = 'CLOSED'
                AND o.id > %(threshold)s
                GROUP BY o.id, r.restaurant_id, r.restaurant_name, t.date
                ORDER BY o.id ASC
                LIMIT %(limit)s;
                """, 
                {"threshold": report_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [ReportObj(
            id=obj[0],
            restaurant_id=obj[1],
            restaurant_name=obj[2],
            settlement_date=obj[3],
            orders_count=obj[4],
            orders_total_sum=obj[5],
            orders_bonus_payment_sum=obj[6],
            orders_bonus_granted_sum=obj[7],
            order_processing_fee=obj[8],
            restaurant_reward_sum=obj[9]
        ) for obj in objs]

class ReportDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_report(self, conn: Connection, report: ReportObj) -> None:
        with conn.cursor() as cur:        
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report (
                    id, restaurant_id, restaurant_name, settlement_date, orders_count,
                    orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
                    order_processing_fee, restaurant_reward_sum
                )
                VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s,
                        %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s,
                        %(order_processing_fee)s, %(restaurant_reward_sum)s)
                ON CONFLICT (restaurant_id, settlement_date)
                DO UPDATE SET 
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                report.dict()
            )

class ReportLoader:
    WF_KEY = "cdm_settlement_report"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.origin = ReportOriginRepository(pg_origin)
        self.dest = ReportDestRepository(pg_dest)
        self.settings_repo = CdmEtlSettingsRepository()
        self.log = log
        self.pg_dest = pg_dest

    def cdm_load_reports(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.cdm_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = CdmEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_report(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} reports to load.")
            if not load_queue:
                self.log.info("Quitting.") 
                return

            for report in load_queue:
                self.dest.insert_report(conn, report)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.cdm_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class CourierLedgerObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class CourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledger(self, courier_ledger_threshold: int, limit: int) -> List[CourierLedgerObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                WITH aggregated_data AS (
                    SELECT
                        fct.courier_id,
                        c.courier_name,
                        fct.settlement_year,
                        fct.settlement_month,
                        COUNT(DISTINCT fct.order_id) AS orders_count,
                        SUM(fct.order_sum) AS orders_total_sum,
                        AVG(fct.rate) AS rate_avg,
                        SUM(fct.tip_sum) AS courier_tips_sum
                    FROM
                        dds.fct_api_courier_payments fct
                    JOIN
                        dds.api_dm_couriers c ON fct.courier_id = c.id
                    WHERE (fct.settlement_year, fct.settlement_month) = 
                        CASE 
                            WHEN EXTRACT(MONTH FROM CURRENT_DATE) = 1 
                            THEN (EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - 1, 12)
                            ELSE (EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER, EXTRACT(MONTH FROM CURRENT_DATE)::INTEGER - 1)
                        END
                    GROUP BY 
                        fct.courier_id, c.courier_name, fct.settlement_year, fct.settlement_month
                )
                SELECT
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    orders_total_sum * 0.25 AS order_processing_fee,
                    CASE
                        WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
                        WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
                        WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
                        ELSE GREATEST(orders_total_sum * 0.1, 200)
                    END AS courier_order_sum,
                    courier_tips_sum,
                    (courier_tips_sum * 0.95) + 
                    CASE
                        WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
                        WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
                        WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
                        ELSE GREATEST(orders_total_sum * 0.1, 200)
                    END AS courier_reward_sum
                FROM
                    aggregated_data
                WHERE courier_id > %(threshold)s            
                LIMIT %(limit)s;
                """, 
                {"threshold": courier_ledger_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [CourierLedgerObj(
            courier_id=obj[0],
            courier_name=obj[1],
            settlement_year=obj[2],
            settlement_month=obj[3],
            orders_count=obj[4],
            orders_total_sum=obj[5],
            rate_avg=obj[6],
            order_processing_fee=obj[7],
            courier_order_sum=obj[8],
            courier_tips_sum=obj[9],
            courier_reward_sum=obj[10]
        ) for obj in objs]

class CourierLedgerDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_courier_ledger(self, conn: Connection, courier_ledger: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_courier_ledger (
                    courier_id, courier_name, settlement_year, settlement_month,
                    orders_count, orders_total_sum, rate_avg, order_processing_fee,
                    courier_order_sum, courier_tips_sum, courier_reward_sum
                )
                VALUES (
                    %(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s,
                    %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s,
                    %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s
                )
                ON CONFLICT (courier_id, settlement_year, settlement_month) 
                DO UPDATE SET
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                courier_ledger.dict()
            )

class CourierLedgerLoader:
    WF_KEY = "cdm_courier_ledger"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierLedgerOriginRepository(pg_origin)
        self.dest = CourierLedgerDestRepository(pg_dest)
        self.settings_repo = CdmEtlSettingsRepository()
        self.log = log

    def cdm_load_courier_ledger(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.cdm_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = CdmEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_courier_ledger(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier ledgers to load.")
            if not load_queue:
                self.log.info("Quitting.") 
                return

            for courier_ledger in load_queue:
                self.dest.insert_courier_ledger(conn, courier_ledger)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.courier_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.cdm_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
