from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection
import json
from bson.objectid import ObjectId
from bson import json_util


class PgSaverRestaurant:

    def save_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_restaurants(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )


class PgSaverUser:
    def save_object(self, conn: Connection, object_id: str, user_name: str, user_login: str, update_ts: datetime):
        """
        Сохраняет или обновляет пользователя в таблице stg.ordersystem_users
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.ordersystem_users (object_id, user_name, user_login, update_ts)
                VALUES (%(object_id)s, %(user_name)s, %(user_login)s, %(update_ts)s)
                ON CONFLICT (user_login) DO UPDATE
                SET
                    object_id = EXCLUDED.object_id,
                    user_name = EXCLUDED.user_name,
                    update_ts = EXCLUDED.update_ts;
                """,
                {
                    "object_id": object_id,
                    "user_name": user_name,
                    "user_login": user_login,
                    "update_ts": update_ts
                }
            )


class PgSaverOrder:
    def save_object(self, conn, id, update_ts, val):
        str_val = json_util.dumps(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.ordersystem_orders (object_id, object_value, update_ts)
                VALUES (%(id)s, %(val)s, %(update_ts)s)
                ON CONFLICT (object_id) DO UPDATE 
                SET 
                    object_value = EXCLUDED.object_value, 
                    update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )


