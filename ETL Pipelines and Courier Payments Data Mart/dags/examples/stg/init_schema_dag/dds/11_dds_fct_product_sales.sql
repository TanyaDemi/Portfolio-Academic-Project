CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    order_id INT NOT NULL,
    count INT NOT NULL,
    price NUMERIC(19, 5) NOT NULL,
    total_sum NUMERIC(19, 5) NOT NULL,
    bonus_payment NUMERIC(19, 5),
    bonus_grant NUMERIC(19, 5),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dds.dm_products(id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_product_sales_product_order_uniq UNIQUE (product_id, order_id)  -- Уникальный ключ
);