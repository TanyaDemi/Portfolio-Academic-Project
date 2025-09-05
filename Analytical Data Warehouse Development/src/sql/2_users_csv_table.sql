CREATE TABLE IF NOT EXISTS STV202502272__STAGING.users_csv (
    id int PRIMARY KEY,
    chat_name varchar(200),
    registration_dt timestamp,
    country varchar(200),
    age int
);
