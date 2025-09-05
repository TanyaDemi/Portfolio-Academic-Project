CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR  NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date TIMESTAMP NOT NULL,
    orders_count INT NOT NULL,
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    orders_bonus_payment_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    orders_bonus_granted_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0,
    restaurant_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    CONSTRAINT dm_settlement_report_uniq UNIQUE (restaurant_id, settlement_date)
);