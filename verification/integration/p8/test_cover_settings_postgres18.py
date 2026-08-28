from __future__ import annotations

import json
from typing import Any, cast

from sqlalchemy import text

from etk.domains.collection.service import COVER_DEFAULTS, CollectionService
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime, EngineConfig

SETTING_KEY = "cover_generator_config"
CONSTRAINT = "verification_p11_cover_settings"


def test_cover_settings_replace_and_roll_back_through_native_postgres18_repository(
    migrated_p8_postgres_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p8_postgres_url,
            application_name="etk-p11-cover-settings",
        )
    )
    snapshot: dict[str, Any] | None = None
    mutated = False
    try:
        runtime.start()
        with runtime.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """
                    SELECT setting_key, value_json, last_updated_at
                    FROM app_settings
                    WHERE setting_key = :key
                    """
                    ),
                    {"key": SETTING_KEY},
                )
                .mappings()
                .one_or_none()
            )
            snapshot = dict(row) if row is not None else None

        mutated = True
        with runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key = :key"), {"key": SETTING_KEY})

        repository = SqlAlchemyAppSettingsRepository(runtime.unit_of_work)
        service = CollectionService(
            cast(Any, object()),
            repository,
            cast(Any, object()),
            cast(Any, object()),
            cast(Any, object()),
        )
        assert service.cover_config().payload == COVER_DEFAULTS

        saved = {"enabled": True, "cover_style": "fixture-style", "opaque": {"kept": True}}
        assert service.save_cover_config(saved).payload == {"message": "配置已成功保存！"}
        with runtime.engine.connect() as connection:
            assert (
                connection.scalar(
                    text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
                    {"key": SETTING_KEY},
                )
                == saved
            )
        assert service.cover_config().payload == {**COVER_DEFAULTS, **saved}

        with runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
            connection.execute(
                text(
                    f"""
                    ALTER TABLE app_settings ADD CONSTRAINT {CONSTRAINT}
                    CHECK (
                        setting_key <> '{SETTING_KEY}'
                        OR value_json ->> 'reject' IS DISTINCT FROM 'true'
                    )
                    """
                )
            )
        failed = service.save_cover_config({"reject": True})
        assert failed.status_code == 500
        assert failed.payload == {"error": "保存配置失败"}
        assert repository.load(SETTING_KEY) == saved
    finally:
        try:
            if mutated:
                with runtime.engine.begin() as connection:
                    connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
                    connection.execute(
                        text("DELETE FROM app_settings WHERE setting_key = :key"),
                        {"key": SETTING_KEY},
                    )
                    if snapshot is not None:
                        connection.execute(
                            text(
                                """
                                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                                VALUES (:key, CAST(:value AS jsonb), :last_updated_at)
                                """
                            ),
                            {
                                "key": snapshot["setting_key"],
                                "value": json.dumps(snapshot["value_json"], ensure_ascii=False),
                                "last_updated_at": snapshot["last_updated_at"],
                            },
                        )
        finally:
            runtime.shutdown()
