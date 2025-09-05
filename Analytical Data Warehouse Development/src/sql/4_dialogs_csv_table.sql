CREATE TABLE IF NOT EXISTS STV202502272__STAGING.dialogs_csv
(
    message_id int PRIMARY KEY,
    message_ts timestamp,
    message_from int,
    message_to int,
    message varchar(1000) DEFAULT NULL,
    message_group int DEFAULT NULL
)
;