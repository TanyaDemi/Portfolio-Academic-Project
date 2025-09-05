CREATE TABLE IF NOT EXISTS stg.api_deliveries (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    restaurant_id TEXT NOT NULL,
    object_id VARCHAR NOT NULL UNIQUE,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);