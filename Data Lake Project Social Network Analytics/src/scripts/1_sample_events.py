from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Project_1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .getOrCreate()

# Чтение исходных данных
events_df = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events")

# Создание тестовой выборки (10% данных за 31 день)
sample_df = events_df.filter(F.col("date").between("2022-01-01", "2022-03-31")) \
                    .sample(0.1)

# Сохранение тестовых данных
sample_df.write.parquet(
    "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/stg/geo/sample_events",
    mode="overwrite",
    partitionBy="date"
)