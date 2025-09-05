import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

@dag(
    dag_id="1_data_import",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule="0 1 * * *",          # 01:00 UTC
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["simple","pg→vt"],
)
def pg2vt():
    pg_hook = PostgresHook("Postgres_DB")
    vt_hook = VerticaHook("Vertica_DB")

    @task
    def copy_table(pg_sql: str, vt_table: str, cols: list[str]):
        col_list = ", ".join(cols)

        copy_to = f"COPY ({pg_sql}) TO STDOUT WITH CSV"
        copy_from = f"COPY {vt_table} ({col_list}) FROM STDIN DELIMITER ','"

        with pg_hook.get_conn() as pg_conn, vt_hook.get_conn() as vt_conn:
            with pg_conn.cursor() as pc, vt_conn.cursor() as vc:
                vc.execute(f"TRUNCATE TABLE {vt_table}")
                pc.copy_expert(copy_to, open('/tmp/tmp.csv','wb'))
                with open('/tmp/tmp.csv','rb') as f:
                    vc.copy(copy_from, f)

    # currencies
    currencies = copy_table.override(task_id="currencies")(
        pg_sql="""
            SELECT
              date_update::date,
              currency_code,
              currency_code_with,
              currency_with_div
            FROM public.currencies
        """,
        vt_table="STV202506166__STAGING.currencies",
        cols=[
            "rate_dt","currency_code_from","currency_code_to","currency_with_div"
        ],
    )

    # transactions
    transactions = copy_table.override(task_id="transactions")(
        pg_sql="""
            SELECT
              operation_id,
              account_number_from,
              account_number_to,
              currency_code,
              country,
              status,
              transaction_type,
              amount::numeric(18,2),
              transaction_dt
            FROM public.transactions
        """,
        vt_table="STV202506166__STAGING.transactions",
        cols=[
            "operation_id","account_number_from","account_number_to",
            "currency_code","country","status","transaction_type",
            "amount","transaction_dt"
        ],
    )

pg2vertica_simple = pg2vt()

