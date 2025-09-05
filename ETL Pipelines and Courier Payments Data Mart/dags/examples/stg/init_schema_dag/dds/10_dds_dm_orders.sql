CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id SERIAL PRIMARY KEY,
    order_key VARCHAR NOT NULL UNIQUE,
    order_status VARCHAR NOT NULL,
    restaurant_id INT NOT NULL,
    timestamp_id INT NOT NULL,
    user_id INT NOT NULL,
    courier_id INT,
    CONSTRAINT fk_restaurant_id FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT fk_timestamp_id FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
    CONSTRAINT fk_courier_id FOREIGN KEY (courier_id) REFERENCES dds.api_dm_couriers(id)
);
