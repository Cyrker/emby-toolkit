from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from ..db.engine import normalize_sqlalchemy_url

_DYNAMIC_CONFIG_KEY = "dynamic_app_config"


class PostgresLegacyConfigStore:
    """psycopg 3 adapter for legacy ``app_settings`` configuration records."""

    def __init__(self, database_url: str) -> None:
        normalized = normalize_sqlalchemy_url(database_url)
        self._database_url = normalized.set(drivername="postgresql").render_as_string(hide_password=False)
        self._safe_database_url = normalized.render_as_string(hide_password=True)

    def __repr__(self) -> str:
        return f"PostgresLegacyConfigStore(database_url={self._safe_database_url!r})"

    def load_dynamic_config(self) -> Mapping[str, Any] | None:
        value = self.load_setting(_DYNAMIC_CONFIG_KEY)
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise TypeError("dynamic_app_config must contain a JSON object")
        return deepcopy(dict(value))

    def save_dynamic_config(self, values: Mapping[str, Any]) -> None:
        self.save_setting(_DYNAMIC_CONFIG_KEY, dict(values))

    def merge_dynamic_config(self, updates: Mapping[str, Any]) -> Mapping[str, Any]:
        """Atomically merge updates so concurrent snapshots cannot erase each other."""

        statement = """
            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (setting_key) DO UPDATE SET
                value_json = COALESCE(app_settings.value_json, '{}'::jsonb) || EXCLUDED.value_json,
                last_updated_at = NOW()
            RETURNING value_json
        """
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, (_DYNAMIC_CONFIG_KEY, Jsonb(dict(updates))))
                row = cursor.fetchone()
        if row is None or not isinstance(row[0], Mapping):
            raise RuntimeError("database did not return merged dynamic configuration")
        return deepcopy(dict(row[0]))

    def load_setting(self, key: str) -> Any:
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT value_json FROM app_settings WHERE setting_key = %s",
                    (key,),
                )
                row = cursor.fetchone()
        return deepcopy(row[0]) if row is not None else None

    def save_setting(self, key: str, value: Any) -> None:
        statement = """
            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (setting_key) DO UPDATE SET
                value_json = EXCLUDED.value_json,
                last_updated_at = NOW()
        """
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, (key, Jsonb(value)))
