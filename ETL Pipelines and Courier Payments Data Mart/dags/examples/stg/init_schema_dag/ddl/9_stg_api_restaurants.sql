CREATE TABLE IF NOT EXISTS stg.api_restaurants (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);