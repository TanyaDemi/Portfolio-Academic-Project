CREATE TABLE IF NOT EXISTS dds.api_dm_deliveries (
	id SERIAL NOT NULL,
	delivery_id varchar NOT NULL,
	delivery_address varchar NOT NULL,
	CONSTRAINT api_dm_deliveries_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT api_dm_deliveries_pkey PRIMARY KEY (id)
);
