from datetime import datetime
from logging import Logger

from examples.stg.stg_settings_repository import StgEtlSetting, StgEtlSettingsRepository
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaverRestaurant, PgSaverUser, PgSaverOrder
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader, UserReader, OrderReader
from lib import PgConnect
from lib.dict_util import json2str
import json


class RestaurantLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_restaurants_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: RestaurantReader, pg_dest: PgConnect, pg_saver: PgSaverRestaurant, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = StgEtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_restaurants(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from restaurants collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing restaurants.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
        
class UserLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_users_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: UserReader, pg_dest: PgConnect, pg_saver: PgSaverUser, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = StgEtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load users from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_users(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to sync.")

            if not load_queue:
                self.log.info("No new users. Exiting.")
                return 0

            i = 0
            for d in load_queue:
                # Проверка на наличие _id
                if "_id" not in d:
                    self.log.error(f"Skipping user without _id: {d}")
                    continue

                self.log.info(f"Saving user: {d}")
                self.pg_saver.save_object(conn, str(d["_id"]), d["name"], d["login"], d["update_ts"])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} users of {len(load_queue)}.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing user sync. Last checkpoint: {wf_setting_json}")

            return len(load_queue)


class OrderLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_orders_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: OrderReader, pg_dest: PgConnect, pg_saver: PgSaverOrder, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = StgEtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load orders from last checkpoint: {last_loaded_ts}")

            # Загружаем заказы из MongoDB
            load_queue = self.collection_loader.get_orders(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to sync.")

            if not load_queue:
                self.log.info("No new orders. Exiting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} orders of {len(load_queue)}.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing order sync. Last checkpoint: {wf_setting_json}")

            return len(load_queue)