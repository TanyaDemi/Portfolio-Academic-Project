CREATE TABLE IF NOT EXISTS dds.fct_api_courier_payments (
    id SERIAL PRIMARY KEY,
    courier_id INTEGER NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INTEGER NOT NULL,
    orders_count NUMERIC(19, 5) NOT NULL,
    orders_total_sum NUMERIC(19, 5) NOT NULL,
    rate_avg NUMERIC(4, 2) NOT NULL,
    courier_tips_sum NUMERIC(19, 5) NOT NULL,
    UNIQUE (courier_id, settlement_year, settlement_month)
);
