CREATE TABLE IF NOT EXISTS STV202502272__STAGING.groups_csv
(
    id int PRIMARY KEY,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private BOOLEAN DEFAULT FALSE
)
;