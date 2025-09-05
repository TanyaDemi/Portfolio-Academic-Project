"""
Таблицы слоя DDS - аналитического хранилища по методу Data Vault.
"""

-- ПРОЕКТ СПРИНТ 13
-- Создать таблицу связи пользователей и групп
DROP TABLE IF EXISTS STV202502272__DWH.l_user_group_activity;
CREATE TABLE STV202502272__DWH.l_user_group_activity (
    hk_l_user_group_activity INT PRIMARY KEY,
    hk_user_id INT NOT NULL,
    hk_group_id INT NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(20) NOT NULL,
    FOREIGN KEY (hk_user_id) REFERENCES STV202502272__DWH.h_users (hk_user_id),
    FOREIGN KEY (hk_group_id) REFERENCES STV202502272__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- Создать скрипты миграции в таблицу связи
INSERT INTO STV202502272__DWH.l_user_group_activity(
    hk_l_user_group_activity,
    hk_user_id,
    hk_group_id,
    load_dt,
    load_src
)
SELECT DISTINCT
    HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    CURRENT_TIMESTAMP AS load_dt,
    'group_log' AS load_src
FROM STV202502272__STAGING.group_log AS gl
LEFT JOIN STV202502272__DWH.h_users hu ON gl.user_id = hu.user_id
LEFT JOIN STV202502272__DWH.h_groups hg ON gl.group_id = hg.group_id
WHERE hu.hk_user_id IS NOT NULL AND hg.hk_group_id IS NOT NULL;


-- Создать и наполнить сателлит групп и привлеченных пользователей
DROP TABLE IF EXISTS STV202502272__DWH.s_auth_history;
CREATE TABLE STV202502272__DWH.s_auth_history (
    hk_l_user_group_activity INT NOT NULL,
    user_id_from INT,
    event VARCHAR(100) NOT NULL,
    event_dt DATETIME NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(20) NOT NULL,
    FOREIGN KEY (hk_l_user_group_activity) REFERENCES STV202502272__DWH.l_user_group_activity (hk_l_user_group_activity)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV202502272__DWH.s_auth_history(
    hk_l_user_group_activity,
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src
)
SELECT 
    luga.hk_l_user_group_activity,
    CASE 
        WHEN gl.event = 'add' AND gl.user_id_from IS NOT NULL THEN gl.user_id_from
        ELSE NULL
    END AS user_id_from,
    gl.event AS event,
    gl.datetime AS event_dt,
    CURRENT_TIMESTAMP AS load_dt,
    'group_log' AS load_src
FROM STV202502272__STAGING.group_log AS gl
LEFT JOIN STV202502272__DWH.h_groups AS hg 
    ON gl.group_id = hg.group_id
LEFT JOIN STV202502272__DWH.h_users AS hu 
    ON gl.user_id = hu.user_id
LEFT JOIN STV202502272__DWH.l_user_group_activity AS luga 
    ON hg.hk_group_id = luga.hk_group_id 
    AND hu.hk_user_id = luga.hk_user_id
WHERE luga.hk_l_user_group_activity IS NOT NULL;