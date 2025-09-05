CREATE TABLE IF NOT EXISTS dds.dm_users (
    id serial4 NOT NULL,
    user_id text NOT NULL,
    user_name text NOT NULL,
    user_login text NOT NULL,
    CONSTRAINT dm_users_pkey PRIMARY KEY (id),
    CONSTRAINT dm_users_user_id_key UNIQUE (user_id) 
    );
 
