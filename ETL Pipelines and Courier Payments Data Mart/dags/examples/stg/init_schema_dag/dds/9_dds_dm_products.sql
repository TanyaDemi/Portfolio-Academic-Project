CREATE TABLE IF NOT EXISTS dds.dm_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR NOT NULL,
    product_name TEXT NOT NULL,
    product_price NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (product_price >= 0),
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT NULL DEFAULT '2099-12-31 00:00:00.000',
    restaurant_id INT NOT NULL,
    CONSTRAINT dm_products_unique_product_id UNIQUE (product_id),
    CONSTRAINT fk_restaurant FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);