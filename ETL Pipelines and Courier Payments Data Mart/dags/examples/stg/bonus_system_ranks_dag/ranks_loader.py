from logging import Logger
from typing import List
from datetime import datetime, timezone
import json

from examples.stg.stg_settings_repository import StgEtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str

import psycopg
from psycopg import Connection
from pydantic import BaseModel
import logging


class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float

class UserObj(BaseModel):
    id: int
    order_user_id: str

class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str

class RanksOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ranks(self, rank_threshold: int, limit: int) -> List[RankObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, name, bonus_percent, min_payment_threshold
                FROM ranks
                WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, 
                {"threshold": rank_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [RankObj(id=obj[0], name=obj[1], bonus_percent=obj[2], min_payment_threshold=obj[3]) for obj in objs]

class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, user_threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, order_user_id
                FROM users
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": user_threshold, "limit": limit}
            )
            objs = cur.fetchall()
        return [UserObj(id=obj[0], order_user_id=obj[1]) for obj in objs]

class OutboxRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, event_threshold: int, limit: int) -> List[EventObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, event_ts, event_type, event_value
                FROM outbox
                WHERE id > %(threshold)s
                ORDER BY id ASC
                LIMIT %(limit)s;
                """,
                {"threshold": event_threshold, "limit": limit}  # Добавлен лимит
            )
            objs = cur.fetchall()
        return [EventObj(id=obj[0], event_ts=obj[1], event_type=obj[2], event_value=obj[3]) for obj in objs]

class RankDestRepository:
    def insert_rank(self, conn: Connection, rank: RankObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                    VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold;
                """,
                {
                    "id": rank.id,
                    "name": rank.name,
                    "bonus_percent": rank.bonus_percent,
                    "min_payment_threshold": rank.min_payment_threshold
                },
            )

class UserDestRepository:
    def insert_user(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.bonussystem_users (id, order_user_id)
                VALUES (%(id)s, %(order_user_id)s)
                ON CONFLICT (id) DO UPDATE
                SET order_user_id = EXCLUDED.order_user_id
                WHERE stg.bonussystem_users.order_user_id IS DISTINCT FROM EXCLUDED.order_user_id;
                """,
                {"id": user.id, "order_user_id": user.order_user_id} 
            )

class EventDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s::JSON)
                ON CONFLICT (id) 
                DO UPDATE 
                SET event_ts = EXCLUDED.event_ts, 
                    event_type = EXCLUDED.event_type, 
                    event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                }
            )

class RankLoader:
    WF_KEY = "example_ranks_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RanksOriginRepository(pg_origin)
        self.stg = RankDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log


    def load_ranks(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = StgEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_ranks(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rank in load_queue:
                self.stg.insert_rank(conn, rank)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class UserLoader:
    WF_KEY = "example_users_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UsersOriginRepository(pg_origin)
        self.stg = UserDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_users(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = StgEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            for user in load_queue:
                self.stg.insert_user(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class EventLoader:
    WF_KEY = "example_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OutboxRepository(pg_origin)
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log


    def load_events(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = StgEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_events(last_loaded, self.BATCH_LIMIT)
            load_queue.sort(key=lambda event: event.id)
            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            for event in load_queue:
                 self.stg.insert_event(conn, event)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
