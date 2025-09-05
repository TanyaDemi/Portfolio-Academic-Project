from datetime import datetime
from lib.pg import PgConnect
from uuid import UUID


class CdmRepository:
    def __init__(self, db, logger):
        self._db = db
        self._logger = logger

    def increment_user_product_counter(
        self,
        user_id: UUID | str,
        product_id: UUID | str,
        product_name: str,
        qty: int,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, product_id)
                    DO UPDATE SET
                        order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt, product_name = EXCLUDED.product_name;
                """, (user_id, product_id, product_name, qty))

    def increment_user_category_counter(
        self,
        user_id: UUID | str,
        category_id: UUID | str,
        category_name: str,
        qty: int,
    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, category_id)
                    DO UPDATE SET
                        order_cnt = cdm.user_category_counters.order_cnt + EXCLUDED.order_cnt, category_name = EXCLUDED.category_name;;
                """, (user_id, category_id, category_name, qty))
