import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.vertica.hooks.vertica import VerticaHook


VT_CONN_ID = "Vertica_DB"
STG = "STV202506166__STAGING"
DWH = "STV202506166__DWH"

# 1. Обновление курсов валют (dim_rates)
SQL_DIM_RATES = """
DELETE FROM {DWH}.dim_rates
WHERE rate_dt = '{date}';

INSERT INTO {DWH}.dim_rates
(
    rate_dt,
    currency,
    rate_to_usd,
    rate_year,
    rate_month,
    rate_day,
    load_dt
)
SELECT
    rate_dt::DATE,
    CASE WHEN currency_code_to = 420 THEN currency_code_from
         ELSE currency_code_to
    END AS currency,
    CASE
        WHEN currency_code_to = 420 THEN 1 / currency_with_div
        ELSE currency_with_div
    END::NUMERIC(18,6) AS rate_to_usd,
    EXTRACT(year FROM rate_dt) AS rate_year,
    EXTRACT(month FROM rate_dt) AS rate_month,
    EXTRACT(day FROM rate_dt) AS rate_day,
    CURRENT_TIMESTAMP AS load_dt
FROM {STG}.currencies
WHERE rate_dt::DATE = '{date}'
  AND (currency_code_from = 420 OR currency_code_to = 420);
"""

# 2. Обновление счетов (accounts)
SQL_ACCOUNTS = """
DELETE FROM {DWH}.accounts
WHERE account_number IN (
    SELECT DISTINCT account_number_from
    FROM {STG}.transactions
    WHERE transaction_dt::DATE = '{date}'

    UNION

    SELECT DISTINCT account_number_to
    FROM {STG}.transactions
    WHERE transaction_dt::DATE = '{date}'
);

INSERT INTO {DWH}.accounts (account_number, country, load_dt)
SELECT DISTINCT account_number, country, CURRENT_TIMESTAMP
FROM (
    SELECT account_number_from AS account_number, country
    FROM {STG}.transactions
    WHERE transaction_dt::DATE = '{date}'

    UNION ALL

    SELECT account_number_to AS account_number, country
    FROM {STG}.transactions
    WHERE transaction_dt::DATE = '{date}'
) t;
"""

# 3. Обновление транзакций (fact_transactions)
SQL_FACT_TRX = """
DELETE FROM {DWH}.fact_transactions
WHERE trx_date = '{date}';

INSERT INTO {DWH}.fact_transactions
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    amount_src,
    amount_usd,
    trx_date, trx_year, trx_month, trx_day,
    load_dt
)
SELECT
    tr.operation_id,
    tr.account_number_from,
    tr.account_number_to,
    tr.currency_code,
    tr.amount AS amount_src,
    tr.amount * COALESCE(dr.rate_to_usd, 1) AS amount_usd,
    tr.transaction_dt::DATE AS trx_date,
    EXTRACT(year FROM tr.transaction_dt) AS trx_year,
    EXTRACT(month FROM tr.transaction_dt) AS trx_month,
    EXTRACT(day FROM tr.transaction_dt) AS trx_day,
    CURRENT_TIMESTAMP AS load_dt
FROM {STG}.transactions tr
LEFT JOIN {DWH}.dim_rates dr
       ON dr.rate_dt  = tr.transaction_dt::DATE
      AND dr.currency = tr.currency_code
WHERE tr.status = 'done'
  AND tr.transaction_dt::DATE = '{date}';
"""

# 4. Обновление метрик (global_metrics)
SQL_GLOBAL_METRICS = """
DELETE FROM {DWH}.global_metrics
WHERE date_update = '{date}';

INSERT INTO {DWH}.global_metrics
(
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions,
    load_dt
)
SELECT
    '{date}' AS date_update,
    currency_code AS currency_from,
    SUM(amount_usd) AS amount_total,
    COUNT(*) AS cnt_transactions,
    CASE WHEN COUNT(DISTINCT account_number_from) > 0
         THEN SUM(amount_usd) / COUNT(DISTINCT account_number_from)
         ELSE 0
    END AS avg_transactions_per_account,
    COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions,
    CURRENT_TIMESTAMP AS load_dt
FROM {DWH}.fact_transactions
WHERE trx_date = '{date}'
GROUP BY currency_code;
"""

@dag(
    dag_id="2_datamart_update",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1)
    },
    tags=["dwh", "daily"],
)
def build_dwh():
    vt_hook = VerticaHook(VT_CONN_ID)

    def exec_sql(sql: str):
        with vt_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                print(f"Executed SQL; {cur.rowcount} rows affected")

    # Возвращает строку даты (YYYY‑MM‑DD) за день до logical_date
    def yesterday() -> str:
        context = get_current_context()
        logical_date = context["logical_date"]
        return (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

    def render(template: str) -> str:
        return template.format(
            date=yesterday(),
            STG=STG,
            DWH=DWH,
        )

    @task
    def dim_rates():
        exec_sql(render(SQL_DIM_RATES))

    @task
    def accounts():
        exec_sql(render(SQL_ACCOUNTS))

    @task
    def fact_transactions():
        exec_sql(render(SQL_FACT_TRX))

    @task
    def global_metrics():
        exec_sql(render(SQL_GLOBAL_METRICS))

    # порядок выполнения
    dim_rates_task = dim_rates()
    accounts_task = accounts()
    fact_transactions_task = fact_transactions()
    metrics_task = global_metrics()

    [dim_rates_task, accounts_task] >> fact_transactions_task >> metrics_task


datamart_update_dag = build_dwh()
