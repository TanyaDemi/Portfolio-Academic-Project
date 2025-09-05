"""
Таблицы стейждингового слоя, которые были созданы в процессе обучения в рамках спринта
сгруппированы и добавлены ключи после загрузки таблиц с приставкой '_csv'
Код для GROUP_LOG смотрите на строке 76.
"""

-- USERS
DROP TABLE IF EXISTS STV202502272__STAGING.users;

CREATE TABLE IF NOT EXISTS STV202502272__STAGING.users (
    id int PRIMARY KEY,
    chat_name varchar(200),
    registration_dt timestamp,
    country varchar(200),
    age int
)
ORDER BY id
SEGMENTED BY hash(id) ALL NODES;

INSERT INTO STV202502272__STAGING.users (
    id, chat_name, registration_dt, country, age
)
SELECT
    id, chat_name, registration_dt, country, age
FROM STV202502272__STAGING.users_csv;

-- GROUPS
DROP TABLE IF EXISTS STV202502272__STAGING.groups;
CREATE TABLE IF NOT EXISTS STV202502272__STAGING.groups (
    id int PRIMARY KEY,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private BOOLEAN DEFAULT FALSE,
    CONSTRAINT fk_admin_id FOREIGN KEY (admin_id) REFERENCES STV202502272__STAGING.users(id)
)
ORDER BY id, admin_id
SEGMENTED BY hash(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

INSERT INTO STV202502272__STAGING.groups (
    id, admin_id, group_name, registration_dt, is_private
)
SELECT
    id, admin_id, group_name, registration_dt, is_private
FROM STV202502272__STAGING.groups_csv;


-- DIALOGS
DROP TABLE IF EXISTS STV202502272__STAGING.dialogs;

CREATE TABLE IF NOT EXISTS STV202502272__STAGING.dialogs (
    message_id int PRIMARY KEY,
    message_ts timestamp,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int,
    CONSTRAINT fk_message_from FOREIGN KEY (message_from) REFERENCES STV202502272__STAGING.users(id),
    CONSTRAINT fk_message_to FOREIGN KEY (message_to) REFERENCES STV202502272__STAGING.users(id),
    CONSTRAINT fk_message_group FOREIGN KEY (message_group) REFERENCES STV202502272__STAGING.groups(id)
)
ORDER BY message_id
SEGMENTED BY hash(message_id) ALL NODES
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

INSERT INTO STV202502272__STAGING.dialogs (
    message_id, message_ts, message_from, message_to, message, message_group
)
SELECT
    message_id, message_ts, message_from, message_to, message, message_group
FROM STV202502272__STAGING.dialogs_csv;

-- GROUP_LOG
DROP TABLE IF EXISTS STV202502272__STAGING.group_log;
CREATE TABLE IF NOT EXISTS STV202502272__STAGING.group_log (
    group_id int,
    user_id int,
    user_id_from int DEFAULT NULL,
    event varchar(100),
    datetime timestamp(6),
    PRIMARY KEY (group_id, user_id),
    CONSTRAINT fk_group_id FOREIGN KEY (group_id) REFERENCES STV202502272__STAGING.groups(id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES STV202502272__STAGING.users(id),
    CONSTRAINT fk_user_id_from FOREIGN KEY (user_id_from) REFERENCES STV202502272__STAGING.users(id)
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id) ALL NODES
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

-- Загрузка данных из CSV-таблицы с преобразованием varchar → int
INSERT INTO STV202502272__STAGING.group_log (
    group_id, user_id, user_id_from, event, datetime
)
SELECT
    group_id,
    user_id,
    CASE 
        WHEN TRIM(user_id_from) IN ('', '<NA>', 'NA', 'NULL', 'null') THEN NULL 
        ELSE user_id_from::int 
    END,
    event,
    datetime
FROM STV202502272__STAGING.group_log_csv;