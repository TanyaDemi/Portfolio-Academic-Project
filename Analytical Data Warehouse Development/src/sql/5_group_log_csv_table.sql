CREATE TABLE IF NOT EXISTS STV202502272__STAGING.group_log_csv (
    group_id int PRIMARY KEY,
    user_id int,
    user_id_from varchar DEFAULT NULL,
    event varchar,
    datetime timestamp(6)
)
;