CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id text NOT NULL,
	restaurant_name text NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id),
	CONSTRAINT dm_restaurants_unique_restaurant_id UNIQUE (restaurant_id)
);