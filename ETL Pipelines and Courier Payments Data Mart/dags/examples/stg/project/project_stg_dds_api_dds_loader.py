from logging import Logger
from typing import List, Optional
from datetime import datetime, date, time
import json

from examples.stg.stg_settings_repository import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str

import psycopg
from psycopg import Connection
from pydantic import BaseModel
import logging

class DmUserObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str

class DmUserOriginRepository:
    def __init__(self, pg: PgConnect)  -> None:
        self._db = pg

    def list_users(self, user_threshold: int, limit: int) -> List[DmUserObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, object_id, user_name, user_login
                FROM stg.ordersystem_users
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": user_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmUserObj(id=obj[0], user_id=obj[1], user_name=obj[2], user_login=obj[3]) for obj in objs]

class DmUserDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_user(self, conn: Connection, user: DmUserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_users (id, user_id, user_name, user_login)
                VALUES (%(id)s, %(user_id)s, %(user_name)s, %(user_login)s)
                ON CONFLICT (user_id) DO UPDATE
                SET
                    user_name = EXCLUDED.user_name,
                    user_login = EXCLUDED.user_login;
                """,
                {"id": user.id, "user_id": user.user_id, "user_name": user.user_name, "user_login": user.user_login} 
            )

class DmUserLoader:
    WF_KEY = "dm_users_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmUserOriginRepository(pg_origin)
        self.dest = DmUserDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_users(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.dest.insert_user(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class DmRestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

class DmRestaurantOriginRepository:
    def __init__(self, pg: PgConnect)  -> None:
        self._db = pg

    def list_restaurants(self, last_loaded_ts: datetime, limit: int) -> List[DmRestaurantObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    object_id AS restaurant_id,
                    (object_value::json ->> 'name') AS restaurant_name,
                    update_ts AS active_from,
                    '2099-12-31 00:00:00'::TIMESTAMP AS active_to
                FROM stg.ordersystem_restaurants    
                WHERE update_ts > %(last_loaded_ts)s
                ORDER BY update_ts ASC
                LIMIT %(limit)s;   
                """,
                {"last_loaded_ts": last_loaded_ts, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmRestaurantObj(id=obj[0], restaurant_id=obj[1], restaurant_name=obj[2], active_from=obj[3], active_to=obj[4]) for obj in objs]

class DmRestaurantDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_restaurants(self, conn: Connection, restaurant: DmRestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to)
                VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                ON CONFLICT (restaurant_id) DO UPDATE
                SET
                    restaurant_name = EXCLUDED.restaurant_name,
                    active_from = EXCLUDED.active_from;
                """,
                restaurant.dict()
            )

class DmRestaurantLoader:
    _LOG_THRESHOLD = 5
    WF_KEY = "dm_restaurants_loader"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmRestaurantOriginRepository(pg_origin)
        self.dest = DmRestaurantDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_restaurants(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            
            # Проверяем, есть ли настройки, если нет — создаем начальные
            if not wf_setting or not wf_setting.workflow_settings:
                wf_setting = DdsEtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )

            # Проверяем, есть ли ключ 'last_loaded_ts' в settings, если нет — задаем начальное значение
            if self.LAST_LOADED_TS_KEY not in wf_setting.workflow_settings:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = datetime(2022, 1, 1).isoformat()

            # Получаем дату последней загрузки
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)

            self.log.info(f"Starting to load restaurants from last checkpoint: {last_loaded_ts}")

            # Загружаем рестораны, обновленные после last_loaded_ts
            load_queue = self.origin.list_restaurants(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to sync.")

            if not load_queue:
                self.log.info("No new records found. Exiting.")
                return 0

            for i, restaurant in enumerate(load_queue, start=1):
                self.dest.insert_restaurants(conn, restaurant)

                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} of {len(load_queue)} restaurants.")

            last_update_ts = max([r.active_from for r in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_update_ts.isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)

            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Finished processing. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
        
class DmTimestampObj(BaseModel):
    ts: datetime
    year: int
    month: int
    day: int    
    date: date
    time: time

class DmTimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, last_loaded_ts: datetime, limit: int) -> List[DmTimestampObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
            """
            SELECT
                ts,
                EXTRACT(YEAR FROM ts) AS year,
                EXTRACT(MONTH FROM ts) AS month,
                EXTRACT(DAY FROM ts) AS day,
                ts::DATE AS date,
                ts::TIME AS time
            FROM (
                -- Выбираем даты из ordersystem_orders
                SELECT 
                    (jsonb_path_query_first(s.object_value::jsonb, 
                        '$.statuses[*] ? (@.status == "CLOSED" || @.status == "CANCELLED").dttm') 
                    #>> '{}')::TIMESTAMP AS ts
                FROM stg.ordersystem_orders s
                WHERE jsonb_path_query_first(s.object_value::jsonb, 
                    '$.statuses[*] ? (@.status == "CLOSED" || @.status == "CANCELLED").dttm') 
                IS NOT NULL
                
                UNION
                
                -- Выбираем order_ts из api_deliveries, если он есть, иначе delivery_ts
                SELECT 
                    COALESCE(
                        (jsonb_path_query_first(d.object_value::jsonb, '$.order_ts') #>> '{}')::TIMESTAMP,
                        (jsonb_path_query_first(d.object_value::jsonb, '$.delivery_ts') #>> '{}')::TIMESTAMP
                    ) AS ts
                FROM stg.api_deliveries d
                WHERE jsonb_path_query_first(d.object_value::jsonb, '$.order_ts') IS NOT NULL
                OR jsonb_path_query_first(d.object_value::jsonb, '$.delivery_ts') IS NOT NULL
            ) AS timestamps
            ORDER BY ts ASC
            LIMIT %(limit)s;            ;
            """,
                {"last_loaded_ts": last_loaded_ts, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmTimestampObj(
            ts=obj[0],
            year=obj[1],
            month=obj[2],
            day=obj[3],
            date=obj[4],
            time=obj[5]
        ) for obj in objs]

class DmTimestampDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_timestamps(self, conn: Connection, timestamp: DmTimestampObj) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    ON CONFLICT (ts) DO UPDATE SET
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                    """,
                    timestamp.dict()
                )
        except Exception as e:
            self.log.error(f"Failed to insert timestamp {timestamp.ts}: {e}")
            raise

class DmTimestampLoader:
    _LOG_THRESHOLD = 5000
    WF_KEY = "dm_timestamps_loader"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmTimestampOriginRepository(pg_origin)
        self.dest = DmTimestampDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_timestamps(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            
            # Проверяем, есть ли настройки, если нет — создаем начальные
            if not wf_setting or not wf_setting.workflow_settings:
                wf_setting = DdsEtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )

            # Проверяем, есть ли ключ 'last_loaded_ts' в settings, если нет — задаем начальное значение
            if self.LAST_LOADED_TS_KEY not in wf_setting.workflow_settings:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = datetime(2022, 1, 1).isoformat()

            # Получаем дату последней загрузки
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)

            self.log.info(f"Starting to load timestamps from last checkpoint: {last_loaded_ts}")

            # Загружаем данные, обновленные после last_loaded_ts
            load_queue = self.origin.list_timestamps(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to sync.")

            if not load_queue:
                self.log.info("No new records found. Exiting.")
                return 0

            for i, timestamp in enumerate(load_queue, start=1):
                self.dest.insert_timestamps(conn, timestamp)

                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} of {len(load_queue)} timestamps.")

            last_update_ts = max([r.ts for r in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_update_ts.isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)

            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Finished processing. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

class DmCourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str
    update_ts: datetime

class DmCourierOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int, limit: int) -> List[DmCourierObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    object_id AS courier_id,
                    (object_value::jsonb ->> 'name') AS courier_name,
                    update_ts
                FROM stg.api_couriers
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s 
                """,
                {"threshold": courier_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmCourierObj(id=obj[0], courier_id=obj[1], courier_name=obj[2], update_ts=obj[3]) for obj in objs]

class DmCourierDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_couriers(self, conn: Connection, courier: DmCourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.api_dm_couriers (courier_id, courier_name, update_ts)
                VALUES (%(courier_id)s, %(courier_name)s, %(update_ts)s)
                ON CONFLICT (courier_id) DO UPDATE SET
                    courier_name = EXCLUDED.courier_name,
                    update_ts = EXCLUDED.update_ts;
                """,
                courier.dict()
            )

class DmCourierLoader:
    WF_KEY = "dm_couriers_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmCourierOriginRepository(pg_origin)
        self.dest = DmCourierDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_couriers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier in load_queue:
                self.dest.insert_couriers(conn, courier)

            # Обновляем last_loaded_id
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class DmCourierOrdersObj(BaseModel):
    id: int
    order_id: str
    order_sum: float
    order_tip_sum: float
    rate: float

class DmCourierOrdersOriginRepository:
    def __init__(self, pg: PgConnect)  -> None:
        self._db = pg

    def list_courier_orders(self, courier_orders_threshold: datetime, limit: int) -> List[DmCourierOrdersObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    id,
                    d.object_id AS order_id,
                    COALESCE((object_value::jsonb ->> 'sum')::NUMERIC(19, 5), 0) AS order_sum,
                    COALESCE((object_value::jsonb ->> 'tip_sum')::NUMERIC(19, 5), 0) AS order_tip_sum,
                    COALESCE((object_value::jsonb ->> 'rate')::NUMERIC(4, 2), 0) AS rate  
                FROM stg.api_deliveries d
                WHERE object_value::jsonb ->> 'order_id' IS NOT NULL 
                AND id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;   
                """,
                {"threshold": courier_orders_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmCourierOrdersObj(id=obj[0], order_id=obj[1], order_sum=obj[2], order_tip_sum=obj[3], rate=obj[4]) for obj in objs]

class DmCourierOrdersDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_courier_orders(self, conn: Connection, courier_orders: DmCourierOrdersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.api_dm_couriers_orders_sum (order_id, order_sum, order_tip_sum, rate)
                VALUES (%(order_id)s, %(order_sum)s, %(order_tip_sum)s, %(rate)s)
                ON CONFLICT (order_id) 
                DO UPDATE SET
                    order_sum = EXCLUDED.order_sum,
                    order_tip_sum = EXCLUDED.order_tip_sum,
                    rate = EXCLUDED.rate;
                """,
                courier_orders.dict()
            )

class DmCourierOrdersLoader:
    WF_KEY = "dm_courier_orders_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmCourierOrdersOriginRepository(pg_origin)
        self.dest = DmCourierOrdersDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_courier_orders(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_courier_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier_orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier_orders in load_queue:
                self.dest.insert_courier_orders(conn, courier_orders)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class DmDeliveryObj(BaseModel):
    id: int
    delivery_id: str
    delivery_address: str

class DmDeliveryOriginRepository:
    def __init__(self, pg: PgConnect)  -> None:
        self._db = pg

    def list_delivery(self, delivery_threshold: int, limit: int) -> List[DmDeliveryObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    (d.object_value::jsonb)->>'delivery_id' AS delivery_id,
                    (d.object_value::jsonb)->>'address' AS delivery_address
                FROM stg.api_deliveries d
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": delivery_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmDeliveryObj(id=obj[0], delivery_id=obj[1], delivery_address=obj[2]) for obj in objs]

class DmDeliveryDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_delivery(self, conn: Connection, delivery: DmDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.api_dm_deliveries (id, delivery_id, delivery_address)
                VALUES (%(id)s, %(delivery_id)s, %(delivery_address)s)
                ON CONFLICT (delivery_id)
                DO UPDATE SET
                    delivery_address = EXCLUDED.delivery_address;                
                """,
                delivery.dict()
            )

class DmDeliveryLoader:
    WF_KEY = "dm_deliveries_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliveryOriginRepository(pg_origin)
        self.dest = DmDeliveryDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_delivery(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.dest.insert_delivery(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class DmProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int

class DmProductOriginRepository:
    def __init__(self, pg: PgConnect)  -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[DmProductObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    ROW_NUMBER() OVER () AS id,
                    item->>'_id' AS product_id,
                    item->>'name' AS product_name,
                    (item->>'price')::NUMERIC(19, 5) AS product_price,
                    stg.ordersystem_restaurants.update_ts AS active_from,
                    '2099-12-31 00:00:00.000'::TIMESTAMP AS active_to,
                    r.id AS restaurant_id
                FROM stg.ordersystem_restaurants
                CROSS JOIN LATERAL jsonb_array_elements(stg.ordersystem_restaurants.object_value::jsonb->'menu') AS item  -- Извлекаем элементы из массива 'menu'
                JOIN dds.dm_restaurants r ON r.restaurant_id = stg.ordersystem_restaurants.object_id
                WHERE stg.ordersystem_restaurants.id > %(threshold)s
                ORDER BY stg.ordersystem_restaurants.id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": product_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmProductObj(
            id=obj[0],
            product_id=obj[1],
            product_name=obj[2],
            product_price=obj[3],
            active_from=obj[4],
            active_to=obj[5],
            restaurant_id=obj[6]
        ) for obj in objs]

class DmProductDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_products(self, conn: Connection, product: DmProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_products (id, product_id, product_name, product_price, active_from, active_to, restaurant_id)
                VALUES (%(id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_price = EXCLUDED.product_price,
                    active_from = EXCLUDED.active_from,
                    active_to = EXCLUDED.active_to,
                    restaurant_id = EXCLUDED.restaurant_id;
                """,
                product.dict()
            )

class DmProductLoader:
    WF_KEY = "dm_products_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmProductOriginRepository(pg_origin)
        self.dest = DmProductDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_products(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for product in load_queue:
                self.dest.insert_products(conn, product)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class DmOrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    courier_id: int

class DmOrderOriginRepository:
    def __init__(self, pg: PgConnect)  -> None:
        self._db = pg


    def list_orders(self, order_threshold: int, limit: int) -> List[DmOrderObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    o.id,
                    o.object_id AS order_key,
                    o.object_value::json->>'final_status' AS order_status,
                    r.id AS restaurant_id,
                    t.id AS timestamp_id,
                    u.id AS user_id,
                    c.id AS courier_id
                FROM stg.ordersystem_orders o
                LEFT JOIN LATERAL (
                    SELECT (jsonb_path_query_first(o.object_value::jsonb, '$.statuses[*] ? (@.status == "CLOSED" || @.status == "CANCELLED").dttm') #>> '{}')::TIMESTAMP AS ts
                ) AS extracted_ts ON true
                LEFT JOIN dds.dm_timestamps t 
                    ON t.ts = extracted_ts.ts
                JOIN dds.dm_restaurants r 
                    ON r.restaurant_id = (o.object_value::json->'restaurant'->'id'->>'$oid')::VARCHAR
                JOIN dds.dm_users u 
                    ON u.user_id = (o.object_value::json->'user'->'id'->>'$oid')::VARCHAR
                LEFT JOIN stg.api_deliveries d 
                    ON d.object_value::json->>'order_id' = o.object_id
                LEFT JOIN dds.api_dm_couriers c 
                    ON c.courier_id = d.object_value::json->>'courier_id'
                WHERE  o.id > %(threshold)s
                AND t.id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM dds.dm_orders existing
                    WHERE existing.order_key = o.object_id
                    AND existing.restaurant_id = r.id
                    AND (existing.timestamp_id IS NOT DISTINCT FROM t.id)
                    AND existing.user_id = u.id
                    AND (existing.courier_id IS NOT DISTINCT FROM c.id)
                )
                ORDER BY o.id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": order_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [DmOrderObj(
            id=obj[0],
            order_key=obj[1],
            order_status=obj[2],
            restaurant_id=obj[3],
            timestamp_id=obj[4],
            user_id=obj[5],
            courier_id=obj[6]
        ) for obj in objs]

class DmOrderDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_orders(self, conn: Connection, order: DmOrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id, courier_id)
                VALUES (%(id)s, %(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s, %(courier_id)s)
                ON CONFLICT (order_key) DO UPDATE SET
                    order_status = EXCLUDED.order_status,
                    restaurant_id = EXCLUDED.restaurant_id,
                    timestamp_id = EXCLUDED.timestamp_id,
                    user_id = EXCLUDED.user_id,
                    courier_id = EXCLUDED.courier_id;
                """,
                order.dict()
            )

class DmOrderLoader:
    WF_KEY = "dm_orders_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrderOriginRepository(pg_origin)
        self.dest = DmOrderDestRepository(pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_orders(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                self.dest.insert_orders(conn, order)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class FctProductSaleObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class FctProductSaleOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_product_sales(self, product_sales_threshold: int, limit: int) -> List[FctProductSaleObj]:
        with self._db.connection() as conn:  # Получаем подключение
            with conn.cursor() as cur:      # Создаем курсор
                cur.execute(
                    """
                    WITH extracted_data AS (
                        SELECT 
                            (event_value::json->>'order_id') AS order_key,
                            event_value::jsonb->'product_payments' AS product_payments
                        FROM stg.bonussystem_events
                        WHERE event_value::jsonb ? 'product_payments'
                    ),
                    parsed_data AS (
                        SELECT 
                            ed.order_key,
                            (pd->>'product_id') AS product_id_text,
                            (pd->>'quantity')::INT AS count,
                            (pd->>'price')::NUMERIC(19,5) AS price,
                            ((pd->>'quantity')::INT * (pd->>'price')::NUMERIC(19,5)) AS total_sum,
                            COALESCE((pd->>'bonus_payment')::NUMERIC(19,5), 0) AS bonus_payment,
                            COALESCE((pd->>'bonus_grant')::NUMERIC(19,5), 0) AS bonus_grant
                        FROM extracted_data ed
                        CROSS JOIN LATERAL jsonb_array_elements(
                            CASE WHEN jsonb_typeof(ed.product_payments) = 'array' THEN ed.product_payments ELSE '[]'::jsonb END
                        ) AS pd
                    )
                    SELECT
                        dp.id AS product_id,
                        doo.id AS order_id,
                        pd.count,
                        pd.price,
                        pd.total_sum,
                        pd.bonus_payment,
                        pd.bonus_grant
                    FROM parsed_data pd
                    JOIN dds.dm_products dp ON pd.product_id_text = dp.product_id
                    JOIN dds.dm_orders doo ON pd.order_key = doo.order_key
                    WHERE doo.id > %(threshold)s
                    ORDER BY doo.id ASC
                    LIMIT %(limit)s;
                    """,
                    {"threshold": product_sales_threshold, "limit": limit}
                )
                objs = cur.fetchall()
        return [FctProductSaleObj(
            product_id=obj[0],
            order_id=obj[1],
            count=obj[2],
            price=obj[3],
            total_sum=obj[4],
            bonus_payment=obj[5],
            bonus_grant=obj[6]
        ) for obj in objs]

class FctProductSaleDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_fct_product_sales(self, product_sale: FctProductSaleObj) -> None:
        with self._db.connection() as conn:  # Получаем подключение
            with conn.cursor() as cur:      # Создаем курсор
                cur.execute(
                    """
                    INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (product_id, order_id) 
                    DO UPDATE SET 
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                    """,
                    product_sale.dict()
                )

class FctProductSaleLoader:
    WF_KEY = "dm_fct_product_sales_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctProductSaleOriginRepository(pg_origin)
        self.dest = FctProductSaleDestRepository(self.pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_fct_product_sales(self):
        with self.pg_dest.connection() as conn:  # Получаем подключение
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fct_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct product sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for product_sale in load_queue:
                self.dest.insert_fct_product_sales(product_sale)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_id for t in load_queue])
            wf_setting_json = json.dumps(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class FctCourierPaymentsObj(BaseModel):
    courier_id: int
    settlement_year: int
    settlement_month: int
    orders_count: float
    orders_total_sum: float
    rate_avg: float
    courier_tips_sum: float

class FctCourierPaymentsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_courier_payments(self, courier_payments_threshold: int, limit: int) -> List[FctCourierPaymentsObj]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH courier_data AS (
                        SELECT
                            c.id AS courier_id,
                            EXTRACT(YEAR FROM t.ts) AS settlement_year,
                            EXTRACT(MONTH FROM t.ts) AS settlement_month,
                            o.order_key,
                            cos.order_sum,
                            cos.rate,
                            cos.order_tip_sum    
                        FROM dds.dm_orders o
                        JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                        JOIN dds.api_dm_couriers c ON o.courier_id = c.id
                        JOIN dds.api_dm_couriers_orders_sum cos ON o.order_key = cos.order_id
                    ),
                    aggregated_data AS (
                        SELECT
                            courier_id,
                            settlement_year,
                            settlement_month,
                            COUNT(DISTINCT order_key) AS orders_count,
                            SUM(order_sum) AS orders_total_sum,
                            AVG(rate::NUMERIC(4, 2)) AS rate_avg,
                            SUM(order_tip_sum) AS courier_tips_sum
                        FROM courier_data
                        GROUP BY
                            courier_id, settlement_year, settlement_month
                    )
                    SELECT
                        courier_id,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        courier_tips_sum   
                    FROM aggregated_data
                    WHERE courier_id > %(threshold)s
                    ORDER BY courier_id ASC
                    LIMIT %(limit)s;
                    """,
                    {"threshold": courier_payments_threshold, "limit": limit}
                )
                objs = cur.fetchall()
        return [FctCourierPaymentsObj(
            courier_id=obj[0],
            settlement_year=obj[1],
            settlement_month=obj[2],
            orders_count=obj[3],
            orders_total_sum=obj[4],
            rate_avg=obj[5],
            courier_tips_sum=obj[6]
        ) for obj in objs]

class FctCourierPaymentsDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_fct_courier_payments(self, courier_payments: FctCourierPaymentsObj) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.fct_api_courier_payments (courier_id, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, courier_tips_sum)
                    VALUES (%(courier_id)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(courier_tips_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) 
                    DO UPDATE SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        courier_tips_sum = EXCLUDED.courier_tips_sum;
                    """,
                    courier_payments.dict()
                )

class FctCourierPaymentsLoader:
    WF_KEY = "dm_fct_courier_payments_loader"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctCourierPaymentsOriginRepository(pg_origin)
        self.dest = FctCourierPaymentsDestRepository(self.pg_dest)
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_fct_courier_payments(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repo.dds_get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fct_courier_payments(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct courier_payments to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier_payments in load_queue:
                self.dest.insert_fct_courier_payments(courier_payments)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.courier_id for t in load_queue])
            wf_setting_json = json.dumps(wf_setting.workflow_settings)
            self.settings_repo.dds_save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
