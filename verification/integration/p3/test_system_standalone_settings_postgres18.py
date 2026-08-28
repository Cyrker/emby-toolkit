from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.domains.system.standalone import (
    CUSTOM_THEME_SETTING,
    MENU_CONFIG_SETTING,
    StandaloneSettingsService,
)
from etk.platform.db import DatabaseRuntime, EngineConfig


class _UnusedDefaults:
    def telegram_templates(self) -> Mapping[str, Any]:
        raise AssertionError("theme/menu ownership must not load Telegram defaults")

    def ai_prompts(self) -> Mapping[str, Any]:
        raise AssertionError("theme/menu ownership must not load AI defaults")


def test_theme_and_menu_round_trip_through_native_postgres18_repository(
    migrated_postgres18_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_postgres18_url,
            application_name="etk-p11-system-standalone-settings",
        )
    )
    keys = (CUSTOM_THEME_SETTING, MENU_CONFIG_SETTING)
    snapshots: list[dict[str, Any]] = []
    mutated = False
    try:
        runtime.start()
        with runtime.engine.begin() as connection:
            snapshots = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json, last_updated_at
                        FROM app_settings
                        WHERE setting_key = ANY(:keys)
                        """
                    ),
                    {"keys": list(keys)},
                ).mappings()
            ]
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = ANY(:keys)"),
                {"keys": list(keys)},
            )
        mutated = True
        repository = SqlAlchemyAppSettingsRepository(runtime.unit_of_work)
        service = StandaloneSettingsService(repository, _UnusedDefaults())

        service.save_custom_theme({"accent": "blue", "nested": {"enabled": True}})
        service.save_menu_config([{"key": "library", "visible": True}])

        assert repository.load(CUSTOM_THEME_SETTING) == {
            "accent": "blue",
            "nested": {"enabled": True},
        }
        assert service.get_menu_config() == [{"key": "library", "visible": True}]
        with runtime.engine.connect() as connection:
            rows = {
                str(row["setting_key"]): row["value_json"]
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json
                        FROM app_settings
                        WHERE setting_key = ANY(:keys)
                        """
                    ),
                    {"keys": list(keys)},
                ).mappings()
            }
        assert rows == {
            CUSTOM_THEME_SETTING: {"accent": "blue", "nested": {"enabled": True}},
            MENU_CONFIG_SETTING: [{"key": "library", "visible": True}],
        }

        assert service.delete_custom_theme() is True
        service.reset_menu_config()
        with runtime.engine.connect() as connection:
            assert (
                connection.scalar(
                    text("SELECT count(*) FROM app_settings WHERE setting_key = ANY(:keys)"),
                    {"keys": list(keys)},
                )
                == 0
            )
    finally:
        try:
            if mutated:
                with runtime.engine.begin() as connection:
                    connection.execute(
                        text("DELETE FROM app_settings WHERE setting_key = ANY(:keys)"),
                        {"keys": list(keys)},
                    )
                    for snapshot in snapshots:
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
