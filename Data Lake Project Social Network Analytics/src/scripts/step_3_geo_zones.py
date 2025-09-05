import os
import argparse
import logging
from typing import Tuple, Dict
from pyspark.sql import SparkSession, Window, DataFrame, functions as F
from pyspark.sql.functions import (
    col, when, row_number, to_timestamp, radians, sin, cos, asin, sqrt, lit,
    weekofyear, date_format, broadcast, count, coalesce
)
from pyspark import StorageLevel
from typing import List 
from pyspark.sql import DataFrame


def setup_logger():
    logger = logging.getLogger("GeoZonesMart")
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)
    return logger
logger = setup_logger()


def init_spark(app_name: str) -> SparkSession:
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[4]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.executor.memoryOverhead", "512") \
        .config("spark.sql.broadcastTimeout", "1200") \
        .getOrCreate()


def load_data(spark: SparkSession, paths: Dict[str, str]) -> Tuple[DataFrame, DataFrame, DataFrame]:
    logger.info("Loading events from %s", paths['events'])
    events = spark.read.parquet(paths['events'])

    logger.info("Loading cities from %s", paths['cities'])
    cities = spark.read.option("header", True).csv(paths['cities']) \
        .withColumn("lat", col("lat").cast("double")) \
        .withColumn("lon", col("lon").cast("double")) \
        .withColumnRenamed("city_id", "zone_id")

    logger.info("Loading profiles from %s", paths['profiles'])
    profiles = spark.read.parquet(paths['profiles'])
    return events, cities, profiles


def check_required_columns(df: DataFrame, required_cols: List[str]) -> None:
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")


def get_event_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "event_datetime",
        F.coalesce(
            F.to_timestamp(col("event.datetime")),
            F.to_timestamp(col("event.message_ts"))
        )
    ).withColumn(
        "date",
        F.to_date(col("event_datetime")))


def filter_events_by_date(events: DataFrame, date_from: str, date_to: str) -> DataFrame:
    events_with_ts = get_event_timestamp(events)
    return events_with_ts.filter(
        (col("event_datetime") >= F.lit(date_from)) & 
        (col("event_datetime") < F.lit(date_to))
    )


def enrich_events(events: DataFrame, profiles: DataFrame) -> DataFrame:
    events_with_ts = get_event_timestamp(events)
    
    last_msg = events_with_ts.filter(col("event_type") == "message") \
        .withColumn("rn", row_number().over(
            Window.partitionBy(col("event.message_from"))
                  .orderBy(F.desc("event_datetime"))
        )) \
        .filter(col("rn") == 1) \
        .select(
            col("event.message_from").alias("user_id"),
            col("lat").alias("last_lat"),
            col("lon").alias("last_lon")
        )  
    
    enriched = events.join(last_msg, events["event.message_from"] == last_msg["user_id"], "left") \
        .withColumn("lat", when(col("lat").isNull(), col("last_lat")).otherwise(col("lat"))) \
        .withColumn("lon", when(col("lon").isNull(), col("last_lon")).otherwise(col("lon"))) \
        .withColumn("lat", col("lat").cast("double")) \
        .withColumn("lon", col("lon").cast("double")) \
        .drop("user_id", "last_lat", "last_lon")
    return enriched


def generate_registrations(events_df: DataFrame) -> DataFrame:
    events_with_ts = get_event_timestamp(events_df)    
    return events_with_ts.filter(F.col("event_type") == "message") \
        .groupBy("event.message_from") \
        .agg(
            F.min("event_datetime").alias("event_datetime"),
            F.first("lat").alias("lat"),
            F.first("lon").alias("lon")
        ) \
        .withColumn("event_type", F.lit("registration")) \
        .withColumnRenamed("event.message_from", "user_id")


def assign_city(events: DataFrame, cities: DataFrame) -> DataFrame:
    logger.info("Assigning cities to events...")

    events = events.filter(col("lat").isNotNull() & col("lon").isNotNull())
    events = events.withColumn("lat_rad", radians(col("lat"))) \
                   .withColumn("lon_rad", radians(col("lon")))

    cities = cities.withColumn("lat_center_rad", radians(col("lat"))) \
                   .withColumn("lon_center_rad", radians(col("lon")))

    cities = cities.persist(StorageLevel.MEMORY_AND_DISK)
    events = events.persist(StorageLevel.MEMORY_AND_DISK)

    joined = events.crossJoin(broadcast(cities)) \
        .withColumn("dlat", col("lat_center_rad") - col("lat_rad")) \
        .withColumn("dlon", col("lon_center_rad") - col("lon_rad")) \
        .withColumn("a", sin(col("dlat") / 2) ** 2 + cos(col("lat_rad")) * cos(col("lat_center_rad")) * sin(col("dlon") / 2) ** 2) \
        .withColumn("c", 2 * asin(sqrt(col("a")))) \
        .withColumn("distance", col("c") * lit(6371))

    window = Window.partitionBy("event.message_from", "event.message_ts").orderBy(col("distance"))
    nearest = joined.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
    return nearest.select("event_type", "event_datetime", F.col("id").alias("zone_id"))


def aggregate_geo(events_geo: DataFrame) -> DataFrame:
    logger.info("Aggregating events by geo zones...")

    result = events_geo.withColumn("zone_id", coalesce(col("zone_id"), lit(1))) \
                       .withColumn("zone_id", col("zone_id").cast("int")) \
                       .withColumn("week", weekofyear(col("event_datetime"))) \
                       .withColumn("month", date_format(col("event_datetime"), "yyyy-MM"))

    weekly = result.groupBy("zone_id", "week").agg(
        count(when(col("event_type") == "message", True)).alias("week_message"),
        count(when(col("event_type") == "reaction", True)).alias("week_reaction"),
        count(when(col("event_type") == "subscription", True)).alias("week_subscription"),
        count(when(col("event_type") == "registration", True)).alias("week_user")
    )

    monthly = result.groupBy("zone_id", "month").agg(
        count(when(col("event_type") == "message", True)).alias("month_message"),
        count(when(col("event_type") == "reaction", True)).alias("month_reaction"),
        count(when(col("event_type") == "subscription", True)).alias("month_subscription"),
        count(when(col("event_type") == "registration", True)).alias("month_user")
    )

    weekly_with_month = result.select("zone_id", "week", "month").distinct().join(
        weekly, on=["zone_id", "week"], how="left"
    )

    geo_zones_mart = weekly_with_month.join(monthly, on=["zone_id", "month"], how="left") \
        .select(
            "month", "week", "zone_id",
            "week_message", "week_reaction", "week_subscription", "week_user",
            "month_message", "month_reaction", "month_subscription", "month_user"
        )
    
    return geo_zones_mart 


def main(args):
    spark = init_spark("GeoZonesMart")
    try:
        paths = {
            'events': args.events_path,
            'cities': args.cities_path,
            'profiles': args.profiles_path
        }

        events, cities, profiles = load_data(spark, paths)

        event_fields = events.select("event.*").columns
        if "datetime" not in event_fields and "message_ts" not in event_fields:
            raise ValueError("Neither event.datetime nor event.message_ts found in events data")

        events = filter_events_by_date(events, args.date_from, args.date_to)
        events = enrich_events(events, profiles)


        required_event_fields = ["message_ts", "message_from"]
        event_fields = events.select("event.*").columns
        missing = [f for f in required_event_fields if f not in event_fields]
        if missing:
            raise ValueError(f"Missing fields in 'event': {missing}")

        required_flat_fields = ["lat", "lon"]
        missing_flat = [f for f in required_flat_fields if f not in events.columns]
        if missing_flat:
            raise ValueError(f"Missing flat fields: {missing_flat}")        

        registrations = generate_registrations(events)

        registrations_padded = registrations.select([
            F.lit(None).cast(events.schema[col].dataType).alias(col)
            if col not in registrations.columns else registrations[col]
            for col in events.columns
        ])

        events_all = events.unionByName(registrations_padded)

        events_geo = assign_city(events_all, cities)
        
        geo_zones = aggregate_geo(events_geo)
        
        geo_zones.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(args.output_path)

        logger.info("Витрина geo_zones успешно создана")
        logger.info(f"Всего записей в geo_zones: {geo_zones.count()}")
        geo_zones.show(10, truncate=False)        
        logger.info("Process complete.")
    finally:
        spark.stop()      
        
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--events_path', required=True, help='Path to input events')
    parser.add_argument('--cities_path', required=True, help='Path to cities CSV')
    parser.add_argument('--profiles_path', required=True, help='Path to user profiles')
    parser.add_argument('--output_path', required=True, help='Output path for geo zones mart')
    parser.add_argument("--date_from", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date_to", type=str, required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()
    main(args)

    