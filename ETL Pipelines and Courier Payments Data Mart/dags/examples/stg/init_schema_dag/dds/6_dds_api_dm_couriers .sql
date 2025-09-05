CREATE TABLE IF NOT EXISTS dds.api_dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,
    CONSTRAINT api_couriers_courier_id_key UNIQUE (courier_id)
);