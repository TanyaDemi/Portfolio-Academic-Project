"""
Таблицы СТЕ для бизнеса. В топ-10 самых ранних групп, средняя доля пользователей, 
которые начали активное общение после вступления в группу, превышает 270%. 
Это значит, что в одной группе один пользователь может писать много сообщений или 
даже создавать несколько сообщений как разные идентификаторы (hk_user_id). Это повод
уточнить, правильно ли агрегируются данные и как именно считаются уникальные авторы сообщений.

Но если все корректно — это очень хороший показатель вовлечённости! Люди вступают и активно общаются. 
Лучше всего конвертируют группы с ID: 
7757992142189260835 - "ох чистая романтика !!!" , 
7279971728630971062 - "собачники для нас подписка 500р" и 
5568963519328366880 - "детективные истории новая эпоха VIP".
"""
-- Шаг 7.1. Подготовить CTE user_group_messages
with user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV202502272__DWH.l_user_message lum
    join STV202502272__DWH.l_groups_dialogs lgd 
        on lum.hk_message_id = lgd.hk_message_id
    group by lgd.hk_group_id
)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;

"""
Вывод:

hk_group_id        |cnt_users_in_group_with_messages|
-------------------+--------------------------------+
3573821581643801282|                               5|
6210160447255813510|                              36|
8764442430478790635|                              49|
1319692034796300830|                              69|
4038157487054977717|                              80|
6640162946765271748|                              83|
1594722103625592852|                              96|
5504994290564188728|                             130|
7271205554897122496|                             144|
4651097343657001468|                             148|

"""

-- Шаг 7.2. Подготовить CTE user_group_log
with top_10_oldest_groups as (
    select hk_group_id
    from STV202502272__DWH.h_groups
    order by registration_dt
    limit 10
),
user_group_log as (
    select 
        lg.hk_group_id,
        count(distinct sah.user_id_from) as cnt_added_users
    from STV202502272__DWH.s_auth_history sah
    join STV202502272__DWH.l_user_group_activity lg 
        on sah.hk_l_user_group_activity = lg.hk_l_user_group_activity
    join top_10_oldest_groups t10 
        on lg.hk_group_id = t10.hk_group_id
    where sah.event = 'add'
    group by lg.hk_group_id
)
select hk_group_id,
       cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;

"""
Вывод:

hk_group_id        |cnt_added_users|
-------------------+---------------+
7279971728630971062|            279|
7757992142189260835|            365|
6014017525933240454|            628|
2461736748292367987|            694|
9183043445192227260|            720|
3214410852649090659|            725|
5568963519328366880|            791|
4350425024258480878|            848|
 206904954090724337|            901|
7174329635764732197|            930|

"""

-- Шаг 7.3. Написать запрос и ответить на вопрос бизнеса
with user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV202502272__DWH.l_user_message lum
    join STV202502272__DWH.l_groups_dialogs lgd 
        on lum.hk_message_id = lgd.hk_message_id
    group by lgd.hk_group_id
),
top_10_oldest_groups as (
    select hk_group_id
    from STV202502272__DWH.h_groups
    order by registration_dt
    limit 10
),
user_group_log as (
    select 
        lg.hk_group_id,
        count(distinct sah.user_id_from) as cnt_added_users
    from STV202502272__DWH.s_auth_history sah
    join STV202502272__DWH.l_user_group_activity lg 
        on sah.hk_l_user_group_activity = lg.hk_l_user_group_activity
    join top_10_oldest_groups t10 
        on lg.hk_group_id = t10.hk_group_id
    where sah.event = 'add'
    group by lg.hk_group_id
)
select 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    coalesce(ugm.cnt_users_in_group_with_messages, 0) as cnt_users_in_group_with_messages,
    coalesce(ugm.cnt_users_in_group_with_messages * 1.0 / nullif(ugl.cnt_added_users, 0), 0) as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm 
    on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;

"""
Вывод:

hk_group_id        |cnt_added_users|cnt_users_in_group_with_messages|group_conversion     |
-------------------+---------------+--------------------------------+---------------------+
7757992142189260835|            365|                            1136|3.1123287671232876712|
7279971728630971062|            279|                             861|3.0860215053763440860|
5568963519328366880|            791|                            2387|3.0176991150442477876|
7174329635764732197|            930|                            2757|2.9645161290322580645|
3214410852649090659|            725|                            2126|2.9324137931034482759|
9183043445192227260|            720|                            2045|2.8402777777777777778|
2461736748292367987|            694|                            1967|2.8342939481268011527|
6014017525933240454|            628|                            1778|2.8312101910828025478|
4350425024258480878|            848|                            2360|2.7830188679245283019|
 206904954090724337|            901|                            2448|2.7169811320754716981|

"""
