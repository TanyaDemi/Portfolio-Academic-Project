import os
import argparse
import logging
from pyspark.sql import SparkSession, functions as F, Window
from datetime import datetime, timedelta
from pyspark.sql.types import StringType, ArrayType
from pyspark import StorageLevel

logger = logging.getLogger(__name__)

def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger

def safe_stop_spark(spark):
    try:
        spark.stop()
    except Exception as e:
        logger.warning(f"Error stopping SparkContext: {e}")

def init_spark(app_name: str) -> SparkSession:
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

    return SparkSession.builder \
        .master("local[4]") \
        .appName(app_name) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "128") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.yarn.executor.memoryOverhead", "1024") \
        .config("spark.sql.broadcastTimeout", "1200") \
        .getOrCreate()

def calculate_distance(lat1_deg, lon1_deg, lat2_deg, lon2_deg):
    R = 6371.0

    lat1 = F.radians(lat1_deg)
    lon1 = F.radians(lon1_deg)
    lat2 = F.radians(lat2_deg)
    lon2 = F.radians(lon2_deg)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.asin(F.sqrt(a))
    return c * F.lit(R)

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

def prepare_cities(spark, cities_raw):
    logger.info("Preparing city data")
    
    valid_zones = [
        z for z in spark.sparkContext._jvm.java.util.TimeZone.getAvailableIDs()
        if z.startswith("Australia/")
    ]
    valid_zones_broadcast = spark.sparkContext.broadcast(valid_zones)
    
    cities = cities_raw.withColumn(
        "timezone_candidate",
        F.concat(F.lit("Australia/"), F.regexp_replace(F.col("city"), " ", "_")))
    
    cities = cities.withColumn(
        "timezone",
        F.when(
            F.col("timezone_candidate").isin(valid_zones),
            F.col("timezone_candidate")
        ).otherwise(F.lit("Australia/Sydney"))
    ).drop("timezone_candidate")
    
    return cities, valid_zones_broadcast

def load_data(spark, path_events, path_cities, date_from, date_to):
    logger.info(f"Loading events from {date_from} to {date_to} from {path_events}")
    paths = generate_event_paths(path_events, date_from, date_to)

    events = None
    cities_raw = None
    
    try:
        events = spark.read.parquet(*paths) \
            .filter(
                (F.col("event_type") == "message") &
                F.col("lat").isNotNull() &
                F.col("lon").isNotNull() &
                ~F.isnan("lat") &
                ~F.isnan("lon") &
                F.col("event").isNotNull() &
                F.col("event.message_from").isNotNull()
            ) \
            .withColumn("lat", F.col("lat").cast("double")) \
            .withColumn("lon", F.col("lon").cast("double")) \
            .withColumn("message_ts", F.coalesce(
                F.col("event.message_ts").cast("timestamp"),
                F.to_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss")
            )) \
            .withColumn("time_utc", F.col("message_ts")) \
            .withColumn("date", F.to_date("message_ts"))
            
        events = events.persist(StorageLevel.DISK_ONLY)
        logger.info(f"Loaded {events.count()} events")
        
    except Exception as e:
        logger.error(f"Error reading parquet files: {str(e)}")
        schema = "event_type string, lat double, lon double, event struct<message_from:string,message_ts:string,datetime:string>, message_ts timestamp, time_utc timestamp, date date"
        events = spark.createDataFrame([], schema=schema) 

    try:
        cities_raw = spark.read.csv(path_cities, header=True, sep=",") \
            .filter(
                (F.col("lat") != "") & (F.col("lon") != "")
            ) \
            .withColumn("lat", F.col("lat").cast("double")) \
            .withColumn("lon", F.col("lon").cast("double")) \
            .filter(
                F.col("lat").isNotNull() & ~F.isnan(F.col("lat")) &
                F.col("lon").isNotNull() & ~F.isnan(F.col("lon"))
            )
        logger.info(f"Loaded {cities_raw.count()} cities")
            
    except Exception as e:
        logger.error(f"Error reading cities reference: {str(e)}")
        cities_raw = spark.createDataFrame([], schema="city string, lat double, lon double") 

    return events, cities_raw

def enrich_events(events, cities):
    logger.info("Calculating distances and determining nearest cities")
    if events.rdd.isEmpty() or cities.rdd.isEmpty():
        logger.warning("Empty input data, skipping enrichment")
        return events.sparkSession.createDataFrame([], schema="user_id string, city string, message_ts timestamp, time_utc timestamp, timezone string, date date")

    try:
        events_prep = events.select(
            F.col("event.message_from").alias("user_id"),
            F.col("lat"),
            F.col("lon"),
            F.col("message_ts"),
            F.col("time_utc"),
            F.col("date")
        )

        approx_distance = 100
        approx_deg = approx_distance / 111

        cities_prepared = cities.select(
            F.col("city").alias("city"),
            F.col("lat").alias("c_lat"),
            F.col("lon").alias("c_lon"),
            F.col("timezone")
        )

        filtered_events = events_prep.alias("e").join(
            cities_prepared.alias("c"),
            (F.abs(F.col("e.lat") - F.col("c.c_lat")) <= approx_deg) &
            (F.abs(F.col("e.lon") - F.col("c.c_lon")) <= approx_deg),
            how="inner"
        )

        enriched = filtered_events.withColumn("distance", calculate_distance(
            F.col("lat"), F.col("lon"),
            F.col("c_lat"), F.col("c_lon")
        ))

        window = Window.partitionBy("user_id", "message_ts").orderBy(F.col("distance"))

        events_geo_final = enriched.withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .drop("rn") \
            .select(
                "user_id",
                F.col("city"),
                "message_ts",
                "time_utc",
                "timezone",
                "date"
            ).withColumn("timezone", F.coalesce("timezone", F.lit("Australia/Sydney")))

        events_geo_final = events_geo_final.persist(StorageLevel.DISK_ONLY)
        logger.info(f"After geo filter count: {events_geo_final.count()}")
        return events_geo_final

    except Exception as e:
        logger.error(f"Error enriching events with geo data: {e}")
        return events.sparkSession.createDataFrame([], schema="user_id string, city string, message_ts timestamp, time_utc timestamp, timezone string, date date")

def calculate_user_profiles(events_geo, valid_zones_broadcast):
    logger.info("Calculating user profiles")
    
    if events_geo.rdd.isEmpty():
        logger.warning("Empty events_geo dataframe, skipping profile calculation")
        schema = "user_id string, act_city string, home_city string, travel_count int, travel_array array<string>, local_time string"
        return events_geo.sparkSession.createDataFrame([], schema=schema)

    try:
        window_latest = Window.partitionBy("user_id").orderBy(F.col("message_ts").desc())
        latest_event = events_geo.withColumn("rn", F.row_number().over(window_latest)) \
            .filter(F.col("rn") == 1) \
            .select(
                "user_id",
                F.col("city").alias("act_city"),
                "timezone",
                "time_utc"
            )
        
        home_city_window = Window.partitionBy("user_id", "city").orderBy("date")
        home_city_df = events_geo.select("user_id", "city", "date").distinct() \
            .withColumn("days_in_city", F.count("*").over(home_city_window)) \
            .filter(F.col("days_in_city") >= 27) \
            .groupBy("user_id", "city") \
            .agg(F.max("days_in_city").alias("days_in_city")) \
            .withColumn("rn", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("days_in_city")))) \
            .filter(F.col("rn") == 1) \
            .select("user_id", F.col("city").alias("home_city"))
        
        travel_window = Window.partitionBy("user_id").orderBy("message_ts")
        travel_df = events_geo.withColumn("city_sequence", F.collect_list("city").over(travel_window)) \
            .groupBy("user_id") \
            .agg(
                F.countDistinct("city").alias("travel_count"),
                F.first("city_sequence").alias("travel_array")
            )
        
        valid_zones = valid_zones_broadcast.value
        user_profiles = latest_event \
            .join(home_city_df, "user_id", "left") \
            .join(travel_df, "user_id", "left") \
            .withColumn("local_time", 
                F.date_format(
                    F.from_utc_timestamp("time_utc", 
                        F.when(
                            F.col("timezone").isin(valid_zones),
                            F.col("timezone")
                        ).otherwise(F.lit("Australia/Sydney"))
                    ),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ) \
            .select(
                "user_id",
                "act_city",
                F.coalesce("home_city", "act_city").alias("home_city"),
                F.coalesce("travel_count", F.lit(1)).alias("travel_count"),
                F.coalesce("travel_array", F.array("act_city")).alias("travel_array"),
                "local_time"
            )
        
        logger.info(f"User profiles ready: {user_profiles.count()} rows")
        return user_profiles
        
    except Exception as e:
        logger.error(f"Error calculating user profiles: {e}")
        schema = "user_id string, act_city string, home_city string, travel_count int, travel_array array<string>, local_time string"
        return events_geo.sparkSession.createDataFrame([], schema=schema)

def save_user_profiles(df, output_path):
    logger.info(f"Saving result to {output_path}")
    try:
        df.write.mode("overwrite").parquet(output_path)
        logger.info("Successfully saved user profiles")
    except Exception as e:
        logger.error(f"Error saving user profiles: {e}")

def main(path_events, path_cities, output_path, date_from, date_to):
    setup_logger()
    spark = init_spark("User Profiles Calculation")
    try:
        events, cities_raw = load_data(spark, path_events, path_cities, date_from, date_to)
        if events.rdd.isEmpty() or cities_raw.rdd.isEmpty():
            logger.error("No data to process, exiting.")
            return

        cities, valid_zones_broadcast = prepare_cities(spark, cities_raw)
        events_geo = enrich_events(events, cities)
        user_profiles = calculate_user_profiles(events_geo, valid_zones_broadcast)

        if not user_profiles.rdd.isEmpty():
            logger.info(f"Sample user profiles:")
            user_profiles.show(10, truncate=False)
            save_user_profiles(user_profiles, output_path)
        else:
            logger.warning("No user profiles generated, nothing to save")
            
    except Exception as e:
        logger.error(f"ERROR in main: {str(e)}", exc_info=True)
    finally:
        safe_stop_spark(spark)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate user profiles based on geo events")
    parser.add_argument("--events_path", type=str, required=True, help="Path to geo events (base folder with date=YYYY-MM-DD)")
    parser.add_argument("--cities_path", type=str, required=True, help="Path to cities CSV")
    parser.add_argument("--output_path", type=str, required=True, help="Output path for user profiles")
    parser.add_argument("--date_from", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date_to", type=str, required=True, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()
    main(args.events_path, args.cities_path, args.output_path, args.date_from, args.date_to)