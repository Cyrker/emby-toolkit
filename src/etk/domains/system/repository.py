from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from copy import deepcopy
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyAppSettingsRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load(self, key: str) -> Any:
        value: Any = None
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
                {"key": key},
            )
        return deepcopy(value)

    def save(self, key: str, value: Any) -> None:
        self.save_many({key: value})

    def save_many(self, values: Mapping[str, Any]) -> None:
        with self._unit_of_work() as uow:
            for key, value in values.items():
                uow.session.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """
                    ),
                    {"key": key, "value": json.dumps(value, ensure_ascii=False)},
                )
            uow.commit()

    def delete(self, key: str) -> bool:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("DELETE FROM app_settings WHERE setting_key = :key"),
                {"key": key},
            )
            uow.commit()
        return True
