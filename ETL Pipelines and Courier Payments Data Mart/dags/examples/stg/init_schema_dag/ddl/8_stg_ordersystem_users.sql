CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id SERIAL PRIMARY KEY,
    object_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    user_login TEXT NOT NULL UNIQUE,
    update_ts TIMESTAMP NOT NULL
);