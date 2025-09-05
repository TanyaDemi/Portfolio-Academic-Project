CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id INTEGER NOT NULL,
    courier_name TEXT NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INTEGER NOT NULL,
    orders_count NUMERIC(19, 5) NOT NULL,
    orders_total_sum NUMERIC(19, 5) NOT NULL,
    rate_avg NUMERIC(5, 2) NOT NULL,
    order_processing_fee NUMERIC(19, 5) NOT NULL,
    courier_order_sum NUMERIC(19, 5) NOT NULL,
    courier_tips_sum NUMERIC(19, 5) NOT NULL,
    courier_reward_sum NUMERIC(19, 5) NOT NULL,
    UNIQUE (courier_id, settlement_year, settlement_month)
);