# Параметры джобы для local

!spark-submit \
  --master local[4] \
  --deploy-mode client \
  --num-executors 1 \
  --executor-memory 8g \
  --driver-memory 4g \
  step_2_user_profiles.py \
  --events_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/stg/geo/sample_events \
  --cities_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/geo/cities/geo.csv \
  --output_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/dwh/geo/user_profiles \
  --date_from 2022-01-01 \
  --date_to 2022-03-31 \
  > my_job1.log 2>&1

!spark-submit \
  --master local[4] \
  --deploy-mode client \
  --num-executors 1 \
  --executor-memory 8g \
  --driver-memory 4g \
  step_3_geo_zones.py \
  --events_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/stg/geo/sample_events \
  --cities_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/geo/cities/geo.csv \
  --profiles_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/dwh/geo/user_profiles \
  --output_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/dwh/geo/user_activity_zones \
  --date_from 2022-01-01 \
  --date_to 2022-03-31 \
  > my_job2.log 2>&1

!spark-submit \
  --master local[4] \
  --deploy-mode client \
  --num-executors 1 \
  --executor-memory 8g \
  --driver-memory 4g \
  step_4_friend_recommendations.py \
  --events_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/stg/geo/sample_events \
  --cities_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/geo/cities/geo.csv \
  --output_path hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/tanyademi1/data/dwh/geo/friend_recommendations \
  --date_from 2022-01-01 \
  --date_to 2022-03-31 \
  > my_job3.log 2>&1
