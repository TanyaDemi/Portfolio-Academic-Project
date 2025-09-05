import uuid
import hashlib
from datetime import datetime
from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    # UUID и hashdiff helpers
    def _generate_uuid(self, *args) -> uuid.UUID:
        key_str = "_".join(map(str, args))
        return uuid.uuid5(uuid.NAMESPACE_DNS, key_str)

    def _generate_hashdiff(self, *args) -> uuid.UUID:
        joined = "|".join(map(str, args))
        return uuid.uuid5(uuid.NAMESPACE_DNS, hashlib.sha256(joined.encode()).hexdigest())

    # Hubs
    def get_or_create_h_user(self, user_id: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_user_pk = self._generate_uuid("h_user", user_id)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_user_pk) DO NOTHING;
                """, (h_user_pk, user_id, load_dt, load_src))

        return h_user_pk

    def get_or_create_h_order(self, order_id: int, order_dt: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_order_pk = self._generate_uuid("h_order", order_id)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (h_order_pk) DO NOTHING;
                """, (h_order_pk, order_id, order_dt, load_dt, load_src))

        return h_order_pk

    def get_or_create_h_product(self, product_id: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_product_pk = self._generate_uuid("h_product", product_id)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_product_pk) DO NOTHING;
                """, (h_product_pk, product_id, load_dt, load_src))

        return h_product_pk

    def get_or_create_h_restaurant(self, restaurant_id: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_restaurant_pk = self._generate_uuid("h_restaurant", restaurant_id)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_restaurant_pk) DO NOTHING;
                """, (h_restaurant_pk, restaurant_id, load_dt, load_src))

        return h_restaurant_pk

    def get_or_create_h_category(self, category_name: str, load_dt: datetime, load_src: str) -> uuid.UUID:
        h_category_pk = self._generate_uuid("h_category", category_name)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_category_pk) DO NOTHING;
                """, (h_category_pk, category_name, load_dt, load_src))

        return h_category_pk

    # Links
    def insert_l_order_product(self, h_order_pk, h_product_pk, load_dt, load_src):
        pk = self._generate_uuid("l_order_product", h_order_pk, h_product_pk)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_product_pk) DO NOTHING;
                """, (pk, h_order_pk, h_product_pk, load_dt, load_src))

    def insert_l_order_user(self, h_order_pk, h_user_pk, load_dt, load_src):
        pk = self._generate_uuid("l_order_user", h_order_pk, h_user_pk)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_user_pk) DO NOTHING;
                """, (pk, h_order_pk, h_user_pk, load_dt, load_src))

    def insert_l_product_category(self, h_product_pk, h_category_pk, load_dt, load_src):
        pk = self._generate_uuid("l_product_category", h_product_pk, h_category_pk)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_category_pk) DO NOTHING;
                """, (pk, h_product_pk, h_category_pk, load_dt, load_src))

    def insert_l_product_restaurant(self, h_product_pk, h_restaurant_pk, load_dt, load_src):
        pk = self._generate_uuid("l_product_restaurant", h_product_pk, h_restaurant_pk)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                """, (pk, h_product_pk, h_restaurant_pk, load_dt, load_src))

    # Satellites
    def insert_s_order_cost(self, h_order_pk, cost, payment, load_dt, load_src):
        hashdiff = self._generate_hashdiff(cost, payment)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (h_order_pk, cost, payment, load_dt, load_src, hashdiff))

    def insert_s_order_status(self, h_order_pk, status, load_dt, load_src):
        hashdiff = self._generate_hashdiff(status)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (h_order_pk, status, load_dt, load_src, hashdiff))

    def insert_s_user_names(self, h_user_pk, name, login, load_dt, load_src):
        hashdiff = self._generate_hashdiff(name, login)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (h_user_pk, name, login, load_dt, load_src, hashdiff))

    def insert_s_product_names(self, h_product_pk, name, load_dt, load_src):
        hashdiff = self._generate_hashdiff(name)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (h_product_pk, name, load_dt, load_src, hashdiff))

    def insert_s_restaurant_names(self, h_restaurant_pk, name, load_dt, load_src):
        hashdiff = self._generate_hashdiff(name)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (h_restaurant_pk, name, load_dt, load_src, hashdiff))