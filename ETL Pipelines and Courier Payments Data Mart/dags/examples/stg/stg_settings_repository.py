from typing import Union, Optional, Dict
from psycopg import Connection
from pydantic import BaseModel
from psycopg.rows import class_row
from psycopg.types.json import Json
from lib import PgConnect
from lib import MongoConnect
import json


class StgEtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    """Класс для работы с настройками в зависимости от базы данных"""
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[StgEtlSetting]:
        with conn.cursor(row_factory=class_row(StgEtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )


class DdsEtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict[str, Union[int, str]]  # Может содержать int или str

    @classmethod
    def from_dict(cls, data: Dict) -> "DdsEtlSetting":
        return cls(
            id=data["id"],
            workflow_key=data["workflow_key"],
            workflow_settings=json.loads(data["workflow_settings"])
        )


class DdsEtlSettingsRepository:
    """Класс для работы в слое dds c настройками в зависимости от базы данных"""
    def dds_get_setting(self, conn: Connection, etl_key: str) -> Optional[DdsEtlSetting]:
        with conn.cursor(row_factory=class_row(DdsEtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def dds_save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )


class CdmEtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict[str, Union[int, str]]  # Может содержать int или str

    @classmethod
    def from_dict(cls, data: Dict) -> "CdmEtlSetting":
        return cls(
            id=data["id"],
            workflow_key=data["workflow_key"],
            workflow_settings=json.loads(data["workflow_settings"])
        )


class CdmEtlSettingsRepository:
    """Класс для работы в слое cdm c настройками в зависимости от базы данных"""
    def cdm_get_setting(self, conn: Connection, etl_key: str) -> Optional[CdmEtlSetting]:
        with conn.cursor(row_factory=class_row(CdmEtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM cdm.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def cdm_save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )