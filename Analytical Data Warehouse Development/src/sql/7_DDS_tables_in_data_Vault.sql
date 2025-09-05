"""
Таблицы слоя DDS - аналитического хранилища по методу Data Vault.
Код для спринта, код для проектной работы в файле 7_DDS_tables_in_data_Vault_project.sql
"""

-- Создаём хабы
drop table if exists STV202502272__DWH.h_users CASCADE;
create table STV202502272__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV202502272__STAGING.users
where hash(id) not in (select hk_user_id from STV202502272__DWH.h_users);

drop table if exists STV202502272__DWH.h_groups CASCADE;
create table STV202502272__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV202502272__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV202502272__STAGING.groups
where hash(id) not in (select hk_group_id from STV202502272__DWH.h_groups);


drop table if exists STV202502272__DWH.h_dialogs CASCADE;
create table STV202502272__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      int,
    message_ts datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id as message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV202502272__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV202502272__DWH.h_dialogs);


-- Таблица связки пользователей и сообщений
drop table if exists STV202502272__DWH.l_user_message CASCADE;
create table STV202502272__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id bigint not null 
        constraint fk_l_user_message_user references STV202502272__DWH.h_users (hk_user_id),
    hk_message_id bigint not null 
        constraint fk_l_user_message_message references STV202502272__DWH.h_dialogs (hk_message_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
SELECT
    hash(hu.hk_user_id, hd.hk_message_id),
    hu.hk_user_id,
    hd.hk_message_id,
    NOW() as load_dt,
    's3' as load_src
FROM STV202502272__STAGING.dialogs d
JOIN STV202502272__DWH.h_users hu ON d.message_from = hu.user_id
JOIN STV202502272__DWH.h_dialogs hd ON d.message_id = hd.message_id
LEFT JOIN STV202502272__DWH.l_user_message lum 
    ON hash(hu.hk_user_id, hd.hk_message_id) = lum.hk_l_user_message
WHERE lum.hk_l_user_message IS NULL;

-- Таблица связки групп и сообщений
drop table if exists STV202502272__DWH.l_groups_dialogs;
create table STV202502272__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_message_id bigint not null 
        constraint fk_l_groups_dialogs_message references STV202502272__DWH.h_dialogs (hk_message_id),
    hk_group_id bigint not null 
        constraint fk_l_groups_dialogs_group references STV202502272__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
SELECT
    hash(hd.hk_message_id, hg.hk_group_id),
    hd.hk_message_id,
    hg.hk_group_id,
    NOW(),
    's3'
FROM STV202502272__STAGING.dialogs AS d
JOIN STV202502272__DWH.h_groups AS hg ON d.message_group = hg.group_id
JOIN STV202502272__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
LEFT JOIN STV202502272__DWH.l_groups_dialogs AS lgd
    ON hash(hd.hk_message_id, hg.hk_group_id) = lgd.hk_l_groups_dialogs
WHERE lgd.hk_l_groups_dialogs IS NULL;

-- Таблица связки админов и групп
drop table if exists STV202502272__DWH.l_admins CASCADE;
create table STV202502272__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id bigint not null 
        constraint fk_l_admins_user references STV202502272__DWH.h_users (hk_user_id),
    hk_group_id bigint not null 
        constraint fk_l_admins_group references STV202502272__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV202502272__STAGING.groups as g
left join STV202502272__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV202502272__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV202502272__DWH.l_admins);

-- Таблица сателлитов админов
drop table if exists STV202502272__DWH.s_admins;
create table STV202502272__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV202502272__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV202502272__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV202502272__DWH.l_admins as la
left join STV202502272__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


-- Таблица сателлитов пользователей и диалогов
DROP TABLE IF EXISTS STV202502272__DWH.s_user_chatinfo;
CREATE TABLE STV202502272__DWH.s_user_chatinfo (
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_s_user_chat_info_h_users REFERENCES STV202502272__DWH.h_users (hk_user_id),
    chat_name VARCHAR(1000),
    load_dt TIMESTAMP,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
SELECT hu.hk_user_id, u.chat_name, NOW(), 's3'
FROM STV202502272__STAGING.users u
JOIN STV202502272__DWH.h_users hu ON u.id = hu.user_id;


-- Таблица сателлитов пользователей и демографии
DROP TABLE IF EXISTS STV202502272__DWH.s_user_socdem;
CREATE TABLE STV202502272__DWH.s_user_socdem (
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV202502272__DWH.h_users (hk_user_id),
    country VARCHAR,
    age INT,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
SELECT hu.hk_user_id, u.country, u.age, NOW(), 's3'
FROM STV202502272__STAGING.users u
JOIN STV202502272__DWH.h_users hu ON u.id = hu.user_id;


-- Таблица сателлитов групп и статуса
DROP TABLE IF EXISTS STV202502272__DWH.s_group_private_status;
CREATE TABLE STV202502272__DWH.s_group_private_status (
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_s_group_private_status_h_groups REFERENCES STV202502272__DWH.h_groups (hk_group_id),
    is_private BOOLEAN,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
SELECT hg.hk_group_id, g.is_private, NOW(), 's3'
FROM STV202502272__STAGING.groups g
JOIN STV202502272__DWH.h_groups hg ON g.id = hg.group_id;

-- Таблица сателлитов групп и названия
DROP TABLE IF EXISTS STV202502272__DWH.s_group_name;
CREATE TABLE STV202502272__DWH.s_group_name (
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202502272__DWH.h_groups (hk_group_id),
    group_name VARCHAR(100),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
SELECT hg.hk_group_id, g.group_name, NOW(), 's3'
FROM STV202502272__STAGING.groups g
JOIN STV202502272__DWH.h_groups hg ON g.id = hg.group_id;


-- Таблица сателлитов диалогов
DROP TABLE IF EXISTS STV202502272__DWH.s_dialog_info;
CREATE TABLE STV202502272__DWH.s_dialog_info (
    hk_message_id BIGINT NOT NULL CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV202502272__DWH.h_dialogs (hk_message_id),
    message VARCHAR(1000),
    message_from INT,
    message_to INT,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV202502272__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
SELECT hd.hk_message_id, d.message, d.message_from, d.message_to, NOW(), 's3'
FROM STV202502272__STAGING.dialogs d
JOIN STV202502272__DWH.h_dialogs hd ON d.message_id = hd.message_id;