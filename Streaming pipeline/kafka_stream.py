import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, struct, col, current_date, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StringType, LongType, IntegerType

# Константы
TOPIC_NAME_IN = 'XXXXX'
TOPIC_NAME_OUT = 'XXXXX'
KAFKA_SERVERS = 'XXXXX'
KAFKA_USERNAME = 'XXXXX'
KAFKA_PASSWORD = 'XXXXX'
POSTGRES_URL = 'jdbc:postgresql://localhost:5432/de'
POSTGRES_USER = 'XXXXX'
POSTGRES_PASSWORD = 'XXXXX'

# Foreach batch метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(batch_df, batch_id):
    try:
        count = batch_df.count()
        if count == 0:
            return

        print(f"\nProcessing batch {batch_id} ({count} records)")
        batch_df.persist()

        # Удаление id, если он есть
        if "id" in batch_df.columns:
            batch_df = batch_df.drop("id")

        # Запись в PostgreSQL
        batch_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "subscribers_feedback") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

        # Отправка сообщения в результирующий топик Kafka без поля feedback
        kafka_ready_df = batch_df.drop("feedback") \
                                 .select(to_json(struct("*")).alias("value"))

        kafka_ready_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";') \
            .option("topic", TOPIC_NAME_OUT) \
            .save()

    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")
    # очищаем память от df    
    finally:
        batch_df.unpersist()


# Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

# Создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
print("\n1. Инициализация Spark сессии...")
spark = SparkSession.builder \
    .appName("Step8") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark сессия успешно создана!")

# Читаем из топика Kafka сообщения с акциями от ресторанов
print("\n2. Подключение к Kafka и чтение данных...")
restaurant_read_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";') \
    .option("subscribe", TOPIC_NAME_IN) \
    .option("startingOffsets", "earliest") \
    .load()


# Определяем схему входного сообщения для json
incomming_message_schema = StructType() \
    .add("restaurant_id", StringType()) \
    .add("adv_campaign_id", StringType()) \
    .add("adv_campaign_content", StringType()) \
    .add("adv_campaign_owner", StringType()) \
    .add("adv_campaign_owner_contact", StringType()) \
    .add("adv_campaign_datetime_start", LongType()) \
    .add("adv_campaign_datetime_end", LongType()) \
    .add("datetime_created", LongType()) \
    .add("client_id", StringType()) \
    .add("processing_date", IntegerType()) \
    .add("feedback", StringType())

# Десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
print("\n3. Обработка данных из Kafka...")
json_df = restaurant_read_stream_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), incomming_message_schema).alias("data")) \
    .select("data.*") \
    .dropDuplicates(["restaurant_id", "adv_campaign_id"])

# Фильтрация активных кампаний. Определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

# Десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = json_df.filter(
    (col("adv_campaign_datetime_start") <= current_timestamp_utc) & 
    (col("adv_campaign_datetime_end") >= current_timestamp_utc)
)

# Вычитываем всех пользователей с подпиской на рестораны
print("\n4. Чтение subscribers_restaurants из PostgreSQL...")
subscribers_restaurant_df = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "subscribers_restaurants") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .load()

subscribers_restaurant_df = subscribers_restaurant_df.drop("client_id")

# Джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(
    subscribers_restaurant_df,
    on="restaurant_id",
    how="inner"
).withColumn("processing_date", current_date())

# Удаление потенциальных дубликатов после JOIN
final_df = result_df.dropDuplicates(["restaurant_id", "client_id", "adv_campaign_id"])

# Запускаем стримминг
print("\n5. Запуск стриминга...")
try:
    query = final_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/feedback_checkpoint") \
        .start()

    query.awaitTermination(600)
finally:
    print("Остановка стриминга и Spark.")
    query.stop()
    spark.stop()