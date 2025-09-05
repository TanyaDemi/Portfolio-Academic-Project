#### Выполненная работа.

## Пункт 1. Отправляем и читаем данные из топика Kafka:
_____________________________________________________________________________________________________________________________

docker exec -it dd31f4bb45cb bash
kafkacat

# Запуск консьюмерa Kafka. Tерминал 1.
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="de-student" \
  -X sasl.password="ltcneltyn" \
  -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
  -t tani-astroblog \
  -K: \
  -P

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}

# Ctrl+D

kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="de-student" \
  -X sasl.password="ltcneltyn" \
  -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
  -t tani-astroblog \
  -C \
  -o beginning \
  -f 'Key: %k\nValue: %s\n---\n' 

============================================================================================================================
## Пункт 2. Инсталлируем в докере на ВМ postgresql-client и создаем в Рostgres тестовые таблицы:
 1. public.subscribers_restaurants,
 2. public.subscribers_feedback.
____________________________________________________________________________________________________________________________
sudo apt update
sudo apt install postgresql-client

psql -h localhost -p 5432 -U jovyan -d de

DROP TABLE IF EXISTS public.subscribers_restaurants;
CREATE TABLE public.subscribers_restaurants (
    id BIGSERIAL PRIMARY KEY,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL
);

INSERT INTO public.subscribers_restaurants (client_id, restaurant_id) VALUES
('223e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('323e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('423e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('523e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('623e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('723e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('823e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('923e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001'),
('023e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('123e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');

CREATE INDEX IF NOT EXISTS idx_subscribers_restaurants_client_id ON subscribers_restaurants (client_id);
CREATE INDEX IF NOT EXISTS idx_subscribers_restaurants_restaurant_id ON subscribers_restaurants (restaurant_id);

=========================================================

DROP TABLE IF EXISTS public.subscribers_feedback;
CREATE TABLE public.subscribers_feedback (
    id BIGSERIAL PRIMARY KEY,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    processing_date timestamp NOT NULL,
    feedback varchar NULL
);

CREATE INDEX IF NOT EXISTS idx_restaurant_id ON subscribers_feedback (restaurant_id);
CREATE INDEX IF NOT EXISTS idx_client_id ON subscribers_feedback (client_id);

============================================================================================================================
## Пункт 3. По шагам, указанным в уроке для подготовки проекта, постепенно подготовлен скрипт kafka_stream.py

Проверка выполнения кода:
____________________________________________________________________________________________________________________________

1. Проверка через Spark выходного топика показала:

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaCheck") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Чтение из выходного топика
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.scram.ScramLoginModule required username="XXXXXXXXXX" password="XXXXXXXXXX";') \
    .option("subscribe", "tani-astroblog_out") \
    .option("startingOffsets", "earliest") \
    .load()

# Преобразование бинарных данных в строку
messages = kafka_df.selectExpr("CAST(value AS STRING)").limit(5)
messages.show(truncate=False)

[Stage 0:>                                                          (0 + 1) / 1]
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1659203516,"adv_campaign_datetime_end":2659207116,"datetime_created":1659131516,"id":3,"client_id":"423e4567-e89b-12d3-a456-426614174000","processing_date":"2025-05-31"} |
|{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1659203516,"adv_campaign_datetime_end":2659207116,"datetime_created":1659131516,"id":6,"client_id":"723e4567-e89b-12d3-a456-426614174000","processing_date":"2025-05-31"} |
|{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1659203516,"adv_campaign_datetime_end":2659207116,"datetime_created":1659131516,"id":2,"client_id":"323e4567-e89b-12d3-a456-426614174000","processing_date":"2025-05-31"} |
|{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1659203516,"adv_campaign_datetime_end":2659207116,"datetime_created":1659131516,"id":10,"client_id":"123e4567-e89b-12d3-a456-426614174000","processing_date":"2025-05-31"}|
|{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1659203516,"adv_campaign_datetime_end":2659207116,"datetime_created":1659131516,"id":1,"client_id":"223e4567-e89b-12d3-a456-426614174000","processing_date":"2025-05-31"} |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

================================================================

Проверка PostgreSQL вручную показала:

psql -h localhost -U jovyan -d de -c "SELECT COUNT(*) FROM subscribers_feedback;"
psql -h localhost -U jovyan -d de -c "SELECT * FROM subscribers_feedback LIMIT 5;"

Password for user jovyan: 
 count 
-------
    42
(1 row)

Password for user jovyan: 
 id |            restaurant_id             |           adv_campaign_id            | adv_campaign_content |  adv_campaign_
owner   | adv_campaign_owner_contact | adv_campaign_datetime_start | adv_campaign_datetime_end | datetime_created |      
        client_id               |   processing_date   | feedback 
----+--------------------------------------+--------------------------------------+----------------------+---------------
--------+----------------------------+-----------------------------+---------------------------+------------------+------
--------------------------------+---------------------+----------
  1 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Iv
anovich | iiivanov@restaurant_id     |                  1659203516 |                2659207116 |       1659131516 | 423e4
567-e89b-12d3-a456-426614174000 | 2025-05-31 00:00:00 | 
  2 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Iv
anovich | iiivanov@restaurant_id     |                  1659203516 |                2659207116 |       1659131516 | 723e4
567-e89b-12d3-a456-426614174000 | 2025-05-31 00:00:00 | 
  3 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Iv
anovich | iiivanov@restaurant_id     |                  1659203516 |                2659207116 |       1659131516 | 323e4
567-e89b-12d3-a456-426614174000 | 2025-05-31 00:00:00 | 
  4 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Iv
anovich | iiivanov@restaurant_id     |                  1659203516 |                2659207116 |       1659131516 | 123e4
567-e89b-12d3-a456-426614174000 | 2025-05-31 00:00:00 | 
  5 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Iv
anovich | iiivanov@restaurant_id     |                  1659203516 |                2659207116 |       1659131516 | 223e4
--More--

Сессия завершена!
