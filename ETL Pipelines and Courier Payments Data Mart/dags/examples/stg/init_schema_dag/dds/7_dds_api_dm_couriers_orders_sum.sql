CREATE TABLE IF NOT EXISTS dds.api_dm_couriers_orders_sum (
    id SERIAL NOT NULL,
    order_id VARCHAR PRIMARY KEY,
    order_sum NUMERIC(19, 5) NOT NULL,
    order_tip_sum NUMERIC(19, 5) NOT NULL,
    rate NUMERIC(4, 2)
);
