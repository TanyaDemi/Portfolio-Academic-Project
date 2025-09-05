
##### СОБИРАЕМ ПРИЛОЖЕНИЕ   #####
## 1 ## Запускаем докер 
## 2 ## Запускаем WSL
## 3 ## Проверяем и запускаем если не запущены yc, kubectl, helm и далее команды по списку ниже.
## 4 ## Собирем докер образ и пушим его...

yc init
          Pick desired action:
          [1] Re-initialize this profile 'default' with new settings
          Please enter your numeric choice: 1

          Please go to https://oauth.yandex.ru/authorize?response_type=token&client_id=1a6990aa636648e9b2ef855fa7bec2fb in order to obtain OAuth token.
          Please enter OAuth token: [y0__xCR6Y-gBR***************************NOPnwB3iVBTeQ] y0__xCR6Y-gBRjB3RMghZTbyhORUFasnpKIWRUkgNOPnwB3iVBTeQ
          Please select cloud to use: 1
          Please choose folder to use: 1
          [1] account-136 (id = b1gv2p2akfgnu9b6j8vr)
          Please enter your numeric choice: 1
          Your current folder has been set to 'account-136' (id = b1gv2p2akfgnu9b6j8vr).
          Do you want to configure a default Compute zone? [Y/n] y
          Which zone do you want to use as a profile default?
          [3] ru-central1-d
          Please enter your numeric choice: 3
          Your profile default Compute zone has been set to 'ru-central1-d'.

export KUBECONFIG=/mnt/c/Users/user/.kube/config

kubectl version
          Client Version: v1.32.2
          Kustomize Version: v5.5.0
          Server Version: v1.27.3
          WARNING: version difference between client (1.32) and server (1.27) exceeds the supported minor version skew of +/-1

kubectl config get-contexts
            CURRENT   NAME                      CLUSTER                   AUTHINFO               NAMESPACE
                      docker-desktop            docker-desktop            docker-desktop
            *         practicum-data-engineer   practicum-data-engineer   c03-demidova-tatyana   c03-demidova-tatyana

kubectl config use-context practicum-data-engineer
            Switched to context "practicum-data-engineer".

kubectl get pods -n c03-demidova-tatyana
        NAME                           READY   STATUS             RESTARTS             AGE
        cdm-service-7d67564cc6-wrrhc   1/1     Running            0                    22m
        dds-service-866bb4d476-b84g5   1/1     Running            0                    37m

helm version
            version.BuildInfo{Version:"v3.18.3", GitCommit:"6838ebcf265a3842d1433956e8a622e3290cf324", GitTreeState:"clean", GoVersion:"go1.24.4"}

 yc container registry configure-docker
            docker configured to use yc --profile "default" for authenticating "cr.yandex" container registries
            Credential helper is configured in '/home/user/.docker/config.json'

# Настройки Kafka, в том числе топик, можно оставить прежними, нет необходимости что-либо пересоздавать.
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/delete_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "stg-demidova"
}
EOF

curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "stg-demidova",
    "kafka_connect": {
        "host": "rc1a-3tcu3d0dj8bm3blo.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "order-service_orders",
        "producer_name": "producer_consumer",
        "producer_password": "Tanya-159487"
    }
}
EOF

# Для загрузки пользователей выполните команду:
curl -X POST https://redis-data-service.sprint9.tgcloudenv.ru/load_users \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "redis":{
        "host": "c-c9qrta44sapt0mnmf6o6.rw.mdb.yandexcloud.net",
        "port": 6380,
        "password": "Tanya-159487"
    }
}
EOF

# Для загрузки ресторанов выполните команду:
curl -X POST https://redis-data-service.sprint9.tgcloudenv.ru/load_restaurants \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "redis":{
        "host": "c-c9qrta44sapt0mnmf6o6.rw.mdb.yandexcloud.net",
        "port": 6380,
        "password": "Tanya-159487"
    }
}
EOF


# # # # dds_service запуск

cd /mnt/c/Users/user/9-project_yandex_cloud/Project/solution/service_dds

## 1. Сборка
docker build . -t cr.yandex/crp586ae3cqor6r77a7h/dds_service:v2025-07-01-r1

# 2. Пуш в ЯК
docker push cr.yandex/crp586ae3cqor6r77a7h/dds_service:v2025-07-01-r1

# 3. Деплой
helm upgrade --install dds-service ./app \
  -n c03-demidova-tatyana \
  --set image.repository=cr.yandex/crp586ae3cqor6r77a7h/dds_service \
  --set image.tag=v2025-07-01-r1\
  --set image.pullPolicy=Always \
  --atomic \
  --timeout 15m \
  --debug \
  --force

# 4. Логи
kubectl get pods -n c03-demidova-tatyana
kubectl logs -n c03-demidova-tatyana dds-service-5c5ffd4795-wws59
kubectl rollout restart deployment stg-service -n c03-demidova-tatyana

# 5. Почистить все helm
helm uninstall dds-service -n c03-demidova-tatyana || echo "Helm release not found, continuing..."
helm uninstall cdm-service -n c03-demidova-tatyana || echo "Helm release not found, continuing..."


# # # # cdm_service

cd /mnt/c/Users/user/9-project_yandex_cloud/Project/solution/service_cdm

## 1. Сборка
docker build . -t cr.yandex/crp586ae3cqor6r77a7h/cdm_service:v2025-07-01-r2

# 2. Пуш в ЯК
docker push cr.yandex/crp586ae3cqor6r77a7h/cdm_service:v2025-07-01-r2


# 3. Деплой

helm upgrade --install cdm-service ./app \
  -n c03-demidova-tatyana \
  --set image.repository=cr.yandex/crp586ae3cqor6r77a7h/cdm_service \
  --set image.tag=v2025-07-01-r2\
  --set image.pullPolicy=Always \
  --atomic \
  --timeout 15m \
  --debug \
  --force

# 4. Логи
kubectl get pods -n c03-demidova-tatyana
kubectl rollout restart deployment cdm-service -n c03-demidova-tatyana


kubectl get pods -n c03-demidova-tatyana
            NAME                           READY   STATUS             RESTARTS             AGE
            cdm-service-7d67564cc6-wrrhc   1/1     Running            0                    22m
            dds-service-866bb4d476-b84g5   1/1     Running            0                    37m

# Показывает структуру с глубиной 3 уровня
  tree -L 5  

# SQL созданные таблицы, в которые получилось загрузить дату

------------------------------------------------------------------
------------------------------------------------------------------
----------------------Cоздаём схему cdm---------------------------
------------------------------------------------------------------
------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS cdm;
--  user product
DROP TABLE IF EXISTS cdm.user_product_counters;
CREATE SCHEMA IF NOT EXISTS cdm.user_product_counters (
    id          SERIAL PRIMARY KEY,
    user_id     UUID   NOT NULL,
    product_id  UUID   NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    order_cnt   INTEGER NOT NULL DEFAULT 0 CHECK (order_cnt >= 0),
    CONSTRAINT uq_user_product UNIQUE (user_id, product_id)
);

--  user category
DROP TABLE IF EXISTS cdm.user_category_counters;
CREATE SCHEMA IF NOT EXISTS cdm.user_category_counters (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID   NOT NULL,
    category_id   UUID   NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    order_cnt     INTEGER NOT NULL DEFAULT 0 CHECK (order_cnt >= 0),
    CONSTRAINT uq_user_category UNIQUE (user_id, category_id)
);

------------------------------------------------------------------
------------------------------------------------------------------
----------------------Cоздаём схему stg---------------------------
------------------------------------------------------------------
------------------------------------------------------------------

-- Cоздаём схему stg
CREATE SCHEMA IF NOT EXISTS stg;
DROP TABLE IF EXISTS stg.order_events;
CREATE TABLE IF NOT EXISTS stg.order_events (
    id SERIAL PRIMARY KEY,
    object_id integer NOT NULL UNIQUE,
    object_type VARCHAR(100) NOT NULL,
    sent_dttm TIMESTAMP WITHOUT TIME ZONE,
    payload json NOT NULL
);
------------------------------------------------------------------
------------------------------------------------------------------
----------------------Cоздаём схему dds---------------------------
------------------------------------------------------------------
------------------------------------------------------------------
-- Cоздаём схему dds
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF EXISTS dds CASCADE;
-- Таблица-хаб пользователей (Hub User)
------------------------------------------------------------------
------------------------------- Хабы -----------------------------
------------------------------------------------------------------
DROP TABLE IF EXISTS dds.h_user CASCADE;
CREATE TABLE IF NOT EXISTS dds.h_user (
    h_user_pk UUID PRIMARY KEY,              -- хэш-ключ (UUID)
    user_id VARCHAR NOT NULL UNIQUE,         -- бизнес-ключ из источника (с уникальностью)
    load_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,     -- время загрузки (UTC)
    load_src VARCHAR NOT NULL                         -- источник («orders-system-kafka»)
);

-- Хаб категорий (Hub Category)
DROP TABLE IF EXISTS dds.h_category CASCADE;
CREATE TABLE IF NOT EXISTS dds.h_category (
    h_category_pk UUID PRIMARY KEY NOT NULL,          -- суррогатный ключ (UUID, хэш от category_id)
    category_name VARCHAR NOT NULL UNIQUE,            -- бизнес-ключ; здесь – имя категории из источника (с уникальностью)
    load_dt       TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- время загрузки (UTC)
    load_src      VARCHAR NOT NULL                    -- источник, напр. 'orders-system-kafka'
);

DROP TABLE IF EXISTS dds.h_product CASCADE;
CREATE TABLE IF NOT EXISTS dds.h_product (
    h_product_pk UUID PRIMARY KEY,            -- суррогатный ключ (генерируется на основе product_id)
    product_id VARCHAR NOT NULL UNIQUE,       -- бизнес-ключ продукта (тип character varying) (с уникальностью)
    load_dt TIMESTAMP NOT NULL,               -- время загрузки записи
    load_src VARCHAR NOT NULL                 -- источник загрузки, например 'orders-system-kafka'
);

DROP TABLE IF EXISTS dds.h_restaurant CASCADE;
CREATE TABLE IF NOT EXISTS dds.h_restaurant (
    h_restaurant_pk UUID PRIMARY KEY,       -- суррогатный ключ (генерируется на основе restaurant_id)
    restaurant_id VARCHAR NOT NULL UNIQUE,   -- бизнес-ключ ресторана (с уникальностью)
    load_dt TIMESTAMP NOT NULL,              -- время загрузки записи
    load_src VARCHAR NOT NULL                -- источник загрузки
);

DROP TABLE IF EXISTS dds.h_order CASCADE;
CREATE TABLE IF NOT EXISTS dds.h_order (
    h_order_pk UUID PRIMARY KEY,       -- суррогатный ключ заказа
    order_id integer NOT NULL UNIQUE,  -- бизнес-ключ заказа (с уникальностью)
    order_dt TIMESTAMP NOT NULL,       -- время совершения заказа
    load_dt TIMESTAMP NOT NULL,        -- время загрузки записи
    load_src VARCHAR NOT NULL          -- источник загрузки
);

------------------------------------------------------------------
------------------------------- Линки ----------------------------
------------------------------------------------------------------
-- Линк «Заказ ↔ Продукт»
DROP TABLE IF EXISTS dds.l_order_product CASCADE;
CREATE TABLE IF NOT EXISTS dds.l_order_product (
    hk_order_product_pk UUID PRIMARY KEY NOT NULL,         -- суррогатный ключ линка
    h_order_pk   UUID NOT NULL,                            -- FK на hub заказа
    h_product_pk UUID NOT NULL,                            -- FK на hub продукта
    load_dt  TIMESTAMP WITHOUT TIME ZONE NOT NULL,         -- дата-время загрузки (UTC)
    load_src VARCHAR NOT NULL,                             -- источник ('orders-system-kafka')
    -- Внешние ключи на соответствующие хабы
    CONSTRAINT fk_l_order_product__order
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk),
    CONSTRAINT fk_l_order_product__product
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk),
    -- Уникальность пары «order–product»
    CONSTRAINT ux_l_order_product__order_product UNIQUE (h_order_pk, h_product_pk)
);

-- Линк «Продукт ↔ Ресторан»
DROP TABLE IF EXISTS dds.l_product_restaurant CASCADE;
CREATE TABLE IF NOT EXISTS dds.l_product_restaurant (
    hk_product_restaurant_pk UUID PRIMARY KEY NOT NULL,      -- суррогатный ключ линка
    h_product_pk    UUID NOT NULL,                           -- FK на хаб продукта
    h_restaurant_pk UUID NOT NULL,                           -- FK на хаб ресторана
    load_dt  TIMESTAMP WITHOUT TIME ZONE NOT NULL,           -- дата-время загрузки (UTC)
    load_src VARCHAR NOT NULL,                               -- источник, напр. 'orders-system-kafka'
    CONSTRAINT fk_l_product_restaurant__product
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
    CONSTRAINT fk_l_product_restaurant__restaurant
        FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk),
    -- Уникальность пары «продукт–ресторан»
    CONSTRAINT ux_l_product_restaurant__product_restaurant UNIQUE (h_product_pk, h_restaurant_pk)
);

-- Линк «Продукт ↔ Категория»
DROP TABLE IF EXISTS dds.l_product_category CASCADE;
CREATE TABLE IF NOT EXISTS dds.l_product_category (
    hk_product_category_pk UUID PRIMARY KEY NOT NULL,          -- суррогатный ключ линка
    h_product_pk  UUID NOT NULL,                               -- FK на хаб продукта
    h_category_pk UUID NOT NULL,                               -- FK на хаб категории
    load_dt  TIMESTAMP WITHOUT TIME ZONE NOT NULL,             -- время загрузки (UTC)
    load_src VARCHAR NOT NULL,                                 -- источник ('orders-system-kafka')
    CONSTRAINT fk_l_product_category__product
        FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
    CONSTRAINT fk_l_product_category__category
        FOREIGN KEY (h_category_pk) REFERENCES dds.h_category (h_category_pk),
    -- Уникальность пары «product–category»
    CONSTRAINT ux_l_product_category__product_category UNIQUE (h_product_pk, h_category_pk)
);

-- Линк «Заказ ↔ Пользователь»
DROP TABLE IF EXISTS dds.l_order_user CASCADE;
CREATE TABLE IF NOT EXISTS dds.l_order_user (
    hk_order_user_pk UUID PRIMARY KEY NOT NULL,               -- суррогатный ключ линка
    h_order_pk UUID NOT NULL,                                 -- FK на хаб заказа
    h_user_pk  UUID NOT NULL,                                 -- FK на хаб пользователя
    load_dt  TIMESTAMP WITHOUT TIME ZONE NOT NULL,            -- время загрузки (UTC)
    load_src VARCHAR NOT NULL,                                -- источник, напр. 'orders-system-kafka'
    CONSTRAINT fk_l_order_user__order
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
    CONSTRAINT fk_l_order_user__user
        FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk),
    -- Уникальность пары «order–user»
    CONSTRAINT ux_l_order_user__order_user UNIQUE (h_order_pk, h_user_pk)
);

------------------------------------------------------------------
---------------------------- Сателлиты ---------------------------
------------------------------------------------------------------
-- Сателлит имён пользователя
DROP TABLE IF EXISTS dds.s_user_names CASCADE;
CREATE TABLE IF NOT EXISTS dds.s_user_names (
    h_user_pk UUID NOT NULL,
    username  VARCHAR,
    userlogin VARCHAR,
    load_dt   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    load_src VARCHAR NOT NULL,                -- источник, напр. 'orders-system-kafka'
    hk_user_names_hashdiff UUID NOT NULL,     -- хэш-дифф записи
    CONSTRAINT pk_s_user_names PRIMARY KEY (h_user_pk, load_dt),
    CONSTRAINT fk_s_user_names_user FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk),
    -- Уникальность хэш-диффа
    CONSTRAINT ux_s_user_names__hashdiff UNIQUE (hk_user_names_hashdiff)
);

-- Сателлит: Имена продуктов
DROP TABLE IF EXISTS dds.s_product_names CASCADE;
CREATE TABLE IF NOT EXISTS dds.s_product_names (
    h_product_pk UUID NOT NULL,                    -- FK на хаб продукта
    name VARCHAR NOT NULL,                         -- имя продукта
    load_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,  -- время загрузки (UTC)
    load_src VARCHAR NOT NULL,                     -- источник данных
    hk_product_names_hashdiff UUID NOT NULL,       -- хэш-дифф строки
    CONSTRAINT pk_s_product_names PRIMARY KEY (h_product_pk, load_dt),
    CONSTRAINT fk_s_product_names__product
        FOREIGN KEY (h_product_pk)
        REFERENCES dds.h_product (h_product_pk),
    -- Уникальность хэш-диффа
    CONSTRAINT ux_s_product_names__hashdiff UNIQUE (hk_product_names_hashdiff)
);

-- Сателлит: Стоимость заказа
DROP TABLE IF EXISTS dds.s_order_cost CASCADE;
CREATE TABLE IF NOT EXISTS dds.s_order_cost (
    h_order_pk UUID NOT NULL,                            -- FK на хаб заказа
    cost   DECIMAL(19,5) NOT NULL CHECK (cost   >= 0),   -- стоимость заказа
    payment DECIMAL(19,5) NOT NULL CHECK (payment >= 0), -- сумма оплаты
    load_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,        -- время загрузки (UTC)
    load_src VARCHAR NOT NULL,                           -- источник данных
    hk_order_cost_hashdiff UUID NOT NULL,                -- хэш-дифф строки
    CONSTRAINT pk_s_order_cost PRIMARY KEY (h_order_pk, load_dt),
    CONSTRAINT fk_s_order_cost__order
        FOREIGN KEY (h_order_pk)
        REFERENCES dds.h_order (h_order_pk),
    -- Уникальность хэш-диффа
    CONSTRAINT ux_s_order_cost__hashdiff UNIQUE (hk_order_cost_hashdiff)
);

-- Сателлит: Статус заказа
DROP TABLE IF EXISTS dds.s_order_status CASCADE;
CREATE TABLE IF NOT EXISTS dds.s_order_status (
    h_order_pk UUID NOT NULL,                            -- FK на хаб заказа
    status    VARCHAR NOT NULL,                          -- статус заказа
    load_dt   TIMESTAMP WITHOUT TIME ZONE NOT NULL,      -- время загрузки (UTC)
    load_src  VARCHAR NOT NULL,                          -- источник данных
    hk_order_status_hashdiff UUID NOT NULL,              -- хэш-дифф строки
    CONSTRAINT pk_s_order_status PRIMARY KEY (h_order_pk, load_dt),
    CONSTRAINT fk_s_order_status__order
        FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
    -- Уникальность хэш-диффа
    CONSTRAINT ux_s_order_status__hashdiff UNIQUE (hk_order_status_hashdiff)
);

-- Сателлит: Имена ресторанов
DROP TABLE IF EXISTS dds.s_restaurant_names CASCADE;
CREATE TABLE IF NOT EXISTS dds.s_restaurant_names (
    h_restaurant_pk UUID NOT NULL,                -- FK на хаб ресторана
    name VARCHAR NOT NULL,                        -- наименование ресторана
    load_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- время загрузки (UTC)
    load_src VARCHAR NOT NULL,                    -- источник данных
    hk_restaurant_names_hashdiff UUID NOT NULL,   -- хэш-дифф строки
    CONSTRAINT pk_s_restaurant_names PRIMARY KEY (h_restaurant_pk, load_dt),
    CONSTRAINT fk_s_restaurant_names__restaurant
        FOREIGN KEY (h_restaurant_pk)
        REFERENCES dds.h_restaurant (h_restaurant_pk),
    -- Уникальность хэш-диффа
    CONSTRAINT ux_s_restaurant_names__hashdiff UNIQUE (hk_restaurant_names_hashdiff)
);