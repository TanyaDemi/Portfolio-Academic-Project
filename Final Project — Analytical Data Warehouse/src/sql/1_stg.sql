CREATE SCHEMA IF NOT EXISTS STV202506166__STAGING;

CREATE TABLE IF NOT EXISTS STV202506166__STAGING.transactions (
    operation_id VARCHAR(36) NOT NULL,
    account_number_from INT NOT NULL,
    account_number_to INT NOT NULL,
    currency_code INT NOT NULL,
    country VARCHAR(30) NOT NULL,
    status VARCHAR(30) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    transaction_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT transactions_pk PRIMARY KEY (operation_id)
)
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;

CREATE TABLE IF NOT EXISTS STV202506166__STAGING.currencies (
    rate_dt DATE NOT NULL,
    currency_code_from INT NOT NULL,
    currency_code_to INT NOT NULL,
    currency_with_div NUMERIC(5,3) NOT NULL,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT u_currencies UNIQUE (rate_dt, currency_code_from, currency_code_to)
)
SEGMENTED BY HASH(rate_dt, currency_code_from) ALL NODES;