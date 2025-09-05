import os
import argparse
import sys
import traceback
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import broadcast, current_timestamp, col, when, coalesce, date_format, from_utc_timestamp, lit, expr


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    logger = logging.getLogger(__name__)
    logger.error("Uncaught exception:", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_uncaught_exception
logger = setup_logging()


def init_spark(app_name: str) -> SparkSession:
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

    return SparkSession.builder \
        .master("local[4]") \
        .appName(app_name) \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "128") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .config("spark.yarn.executor.memoryOverhead", "1024") \
        .config("spark.sql.broadcastTimeout", "1200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
        .getOrCreate()


@contextmanager
def cached_df(spark, df, cache_name):
    if df.is_cached:
        logger.warning(f"DataFrame {cache_name} уже закэширован")
    df.createOrReplaceTempView(cache_name)
    spark.sql(f"CACHE TABLE {cache_name}")
    try:
        yield df
    finally:
        try:
            spark.sql(f"UNCACHE TABLE IF EXISTS {cache_name}")
            spark.sql(f"DROP VIEW IF EXISTS {cache_name}")
            logger.debug(f"Кэш для {cache_name} успешно очищен")
        except Exception as e:
            logger.error(f"Ошибка при очистке кэша {cache_name}: {str(e)}")


def generate_event_paths(base_path: str, date_from: str, date_to: str) -> list:
    date_fmt = "%Y-%m-%d"
    start = datetime.strptime(date_from, date_fmt)
    end = datetime.strptime(date_to, date_fmt)
    
    paths = []
    while start <= end:
        path = os.path.join(base_path, f"date={start.strftime(date_fmt)}")
        paths.append(path)
        start += timedelta(days=1)
    return paths


def load_data(spark: SparkSession, path_events: str, path_cities: str, date_from: str, date_to: str) -> tuple:
    logger.info(f"Loading events from {date_from} to {date_to} from {path_events}")
    paths = generate_event_paths(path_events, date_from, date_to)

    message_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("message_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("message_to", StringType(), True),
        StructField("date_time_utc", TimestampType(), True)
    ])

    subscription_schema = StructType([
        StructField("channel_id", StringType(), True),
        StructField("user_id", StringType(), True)
    ])

    try:
        if paths:
            messages = spark.read.parquet(*paths)

            if "event" in messages.columns:
                messages = messages \
                    .filter(
                        (F.col("event_type") == "message") &
                        F.col("lat").isNotNull() &
                        F.col("lon").isNotNull()
                    ) \
                    .select(
                        F.col("event_type"),
                        F.col("lat"),
                        F.col("lon"),
                        F.col("event.message_id").alias("message_id"),
                        F.col("event.message_from").alias("user_id"),
                        F.col("event.message_to").alias("message_to"), 
                        F.col("event.message_ts").alias("date_time_utc")
                    ) \
                    .filter(
                        F.col("user_id").isNotNull() & 
                        F.col("message_id").isNotNull() & 
                        F.col("date_time_utc").isNotNull()
                    )

                logger.info(f"Loaded {messages.count()} messages")
            else:
                logger.warning("No valid paths found for events.")
                messages = spark.createDataFrame([], message_schema)
    except Exception as e:
        logger.error(f"Error reading messages: {str(e)}")
        messages = spark.createDataFrame([], message_schema)

    try:
        subscriptions = spark.read.parquet(*paths) \
            .filter(F.col("event_type") == "subscription") \
            .select(
                F.col("event.subscription_channel").alias("channel_id"),
                F.col("event.user").alias("user_id")
            ) \
            .filter(F.col("channel_id").isNotNull() & F.col("user_id").isNotNull())

        logger.info(f"Loaded {subscriptions.count()} subscriptions")
    except Exception as e:
        logger.error(f"Error reading subscriptions: {str(e)}")
        subscriptions = spark.createDataFrame([], schema=subscription_schema)

    city_schema = StructType([
        StructField("city_id", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])

    try:
        cities = spark.read.csv(path_cities, header=True, sep=",") \
            .filter(
                F.col("lat").isNotNull() &
                F.col("lon").isNotNull() &
                ~F.isnan(F.col("lat")) &
                ~F.isnan(F.col("lon"))
            ) \
            .withColumn("lat", F.col("lat").cast("double")) \
            .withColumn("lon", F.col("lon").cast("double")) \
            .withColumnRenamed("id", "city_id") \
            .withColumnRenamed("city", "city_name")
        
        # Добавляем временную зону timezone на основе city_name
        cities = cities.withColumn(
            "timezone",
            F.concat(F.lit("Australia/"), F.regexp_replace(F.col("city_name"), " ", "_"))
        )
        logger.info(f"Loaded {cities.count()} cities")
    except Exception as e:
        logger.error(f"Error reading cities: {str(e)}")
        cities = spark.createDataFrame([], schema=city_schema)

    return messages, subscriptions, cities


def process_user_locations(messages: DataFrame) -> DataFrame:
    window = Window.partitionBy("user_id").orderBy(F.col("date_time_utc").desc())
    
    last_locations = messages.withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .select(
            "user_id",
            F.col("lat").alias("last_lat"),
            F.col("lon").alias("last_lon"),
            F.col("date_time_utc").alias("location_ts")
        )

    logger.info(f"Уникальных пользователей: {last_locations.select('user_id').distinct().count()}")
    
    return last_locations


def get_common_subscriptions(subscriptions: DataFrame) -> DataFrame:
    logger.info("Входные подписки для get_common_subscriptions:")
    
    subscriptions_df = (subscriptions.alias("s1")
        .join(subscriptions.alias("s2"),
              (F.col("s1.channel_id") == F.col("s2.channel_id")) & 
              (F.col("s1.user_id") < F.col("s2.user_id")))
        .select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
            F.col("s1.channel_id").alias("channel_id"))
    )
    
    logger.info(f"Всего пар: {subscriptions_df.count()}")
    
    return subscriptions_df


def filter_existing_conversations(pairs_df: DataFrame, messages: DataFrame) -> DataFrame:
    logger.info("Проверка существующих диалогов:")
    messages.select("user_id", "message_to").show(5)

    existing_conv = messages.select(
            F.col("user_id").alias("user_left"),
            F.col("message_to").alias("user_right")
        ).distinct()
    
    existing_conv_reverse = messages.select(
            F.col("message_to").alias("user_left"),
            F.col("user_id").alias("user_right")
        ).distinct()
    
    all_existing = existing_conv.union(existing_conv_reverse).distinct()

    filter_result = pairs_df.join(all_existing, ["user_left", "user_right"], "left_anti")
    
    logger.info(f"Пар после фильтрации: {filter_result.count()}")
    
    return filter_result


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_rad, lon1_rad = F.radians(lat1), F.radians(lon1)
    lat2_rad, lon2_rad = F.radians(lat2), F.radians(lon2)
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = F.sin(dlat / 2)**2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2)**2
    c = 2 * F.asin(F.sqrt(a))
    return R * c


def add_distance_filter(pairs_df: DataFrame, last_locations: DataFrame, max_distance_km: float = 1.0) -> DataFrame:
    logger.info("Проверка locations для distance_filter:")

    loc_left = last_locations.select(
        F.col("user_id").alias("user_left"),
        F.col("last_lat").alias("lat_left"),
        F.col("last_lon").alias("lon_left")
    )
    
    loc_right = last_locations.select(
        F.col("user_id").alias("user_right"),
        F.col("last_lat").alias("lat_right"),
        F.col("last_lon").alias("lon_right"))
    
    joined = pairs_df \
        .join(broadcast(loc_left), "user_left") \
        .join(broadcast(loc_right), "user_right")
    
    result = joined.withColumn(
        "distance",
        calculate_distance(F.col("lat_left"), F.col("lon_left"), 
                         F.col("lat_right"), F.col("lon_right"))
    ).filter(F.col("distance") <= max_distance_km)

    logger.info(f"Пар после фильтра расстояния: {result.count()}")   
    
    return result.drop("lat_left", "lon_left", "lat_right", "lon_right")

def create_data_recommendations(nearby_pairs: DataFrame, cities: DataFrame, last_locations: DataFrame) -> DataFrame:
    required_cols = {'last_lat', 'last_lon', 'user_id', 'location_ts'}
    if not required_cols.issubset(set(last_locations.columns)):
        raise ValueError(f"last_locations must contain columns: {required_cols}")
    
    logger.info("Creating final data mart...")
    
    left_user = last_locations.alias("left_loc") \
        .join(cities.alias("left_city"), 
              (F.abs(F.col("left_loc.last_lat") - F.col("left_city.lat")) < 0.1) &
              (F.abs(F.col("left_loc.last_lon") - F.col("left_city.lon")) < 0.1),
              "left") \
        .select(
            F.col("left_loc.user_id").alias("user_left"),
            F.col("left_city.timezone").alias("zone_left"),
            F.col("left_loc.location_ts").alias("ts_left")
        )
    
    right_user = last_locations.alias("right_loc") \
        .join(cities.alias("right_city"), 
              (F.abs(F.col("right_loc.last_lat") - F.col("right_city.lat")) < 0.1) &
              (F.abs(F.col("right_loc.last_lon") - F.col("right_city.lon")) < 0.1),
              "left") \
        .select(
            F.col("right_loc.user_id").alias("user_right"),
            F.col("right_city.timezone").alias("zone_right"),
            F.col("right_loc.location_ts").alias("ts_right")
        )
    
    result = nearby_pairs.join(left_user, "user_left", "left") \
        .join(right_user, "user_right", "left") \
        .withColumn("zone_id", F.coalesce(F.col("zone_left"), F.col("zone_right"), F.lit("Australia/Brisbane"))) \
        .withColumn("location_ts", F.coalesce(F.col("ts_left"), F.col("ts_right"))) \
        .withColumn("local_time", F.date_format(
            F.from_utc_timestamp(F.col("location_ts"), F.col("zone_id")),
            "yyyy-MM-dd HH:mm:ss"
        )) \
        .withColumn("processed_dttm", F.current_timestamp()) \
        .select(
            "user_left",
            "user_right",
            "processed_dttm",
            "zone_id",
            "local_time"
        ) \
        .drop("zone_left", "zone_right", "ts_left", "ts_right", "location_ts")
    
    logger.info(f"Final data mart created with {result.count()} records")
    return result


def main(args):
    spark = None
    try:
        logger.info("Starting Spark session...")
        spark = init_spark("Friend_Recommendations")
        
        logger.info(f"Processing data from {args.date_from} to {args.date_to}")
        messages, subscriptions, cities = load_data(
            spark, 
            args.events_path,
            args.cities_path,
            args.date_from, 
            args.date_to
        )

        with cached_df(spark, messages, "messages_cache"):
            last_locations = process_user_locations(messages)
        
        with cached_df(spark, subscriptions, "subscriptions_cache"):
            common_subs = get_common_subscriptions(subscriptions)
        
        with cached_df(spark, messages, "messages_cache_2"):
            filtered_pairs = filter_existing_conversations(common_subs, messages)
        
        nearby_pairs = add_distance_filter(filtered_pairs, last_locations)
        
        data_mart = create_data_recommendations(nearby_pairs, cities, last_locations)
        
        logger.info(f"Writing results to {args.output_path}")
        data_mart.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(args.output_path)        
        logger.info("Результаты успешно сохранены!")
        
    except Exception as e:
        logger.error(f"Ошибка в основном потоке выполнения:: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
            

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--events_path', required=True, help='Path to input events directory')
    parser.add_argument('--cities_path', required=True, help='Path to cities CSV')
    parser.add_argument('--output_path', required=True, help='Output path for results')
    parser.add_argument('--date_from', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    main(args)