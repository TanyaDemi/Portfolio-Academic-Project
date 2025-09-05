CREATE SCHEMA IF NOT EXISTS STV202506166__DWH;

CREATE TABLE IF NOT EXISTS STV202506166__DWH.fact_transactions (
    operation_id VARCHAR(36) NOT NULL,
    account_number_from INT NOT NULL,
    account_number_to INT NOT NULL,
    currency_code INT NOT NULL,
    amount_src NUMERIC(18,2) NOT NULL, 
    amount_usd NUMERIC(18,2) NOT NULL, 
    trx_date DATE NOT NULL,
    trx_year INT NOT NULL,
    trx_month INT NOT NULL,
    trx_day INT NOT NULL, 
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_transactions PRIMARY KEY (operation_id)
)
SEGMENTED BY HASH(trx_date, operation_id) ALL NODES;

CREATE TABLE IF NOT EXISTS STV202506166__DWH.dim_rates (
    rate_dt DATE NOT NULL,
    rate_year INT NOT NULL,
    rate_month INT NOT NULL,
    rate_day INT NOT NULL,
    currency INT NOT NULL,
    rate_to_usd NUMERIC(18,6) NOT NULL,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_dim_rates PRIMARY KEY (rate_dt, currency)
)
SEGMENTED BY HASH(rate_dt, currency) ALL NODES;

CREATE TABLE IF NOT EXISTS STV202506166__DWH.accounts (
    account_number INT NOT NULL,
    country VARCHAR(30) NOT NULL,    
    load_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT accounts_pk PRIMARY KEY (account_number)
)
SEGMENTED BY HASH(account_number) ALL NODES;

CREATE TABLE IF NOT EXISTS STV202506166__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INT NOT NULL,
    amount_total NUMERIC(38,2) NOT NULL,
    cnt_transactions INT NOT NULL,
    avg_transactions_per_account NUMERIC(38,4) NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_global_metrics PRIMARY KEY (date_update, currency_from)
)
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
