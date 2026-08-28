from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.domains.system.standalone import (
    AI_PROMPTS_SETTING,
    CUSTOM_THEME_SETTING,
    MENU_CONFIG_SETTING,
    TELEGRAM_TEMPLATES_SETTING,
    PlaintextStandaloneSettingsDefaults,
    StandaloneSettingsService,
)
from etk.platform.db import DatabaseRuntime, EngineConfig
from etk.policy_defaults import DEFAULT_AI_PROMPTS, DEFAULT_TELEGRAM_NOTIFICATION_TEMPLATES


def test_standalone_settings_round_trip_through_native_postgres18_repository(
    migrated_postgres18_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_postgres18_url,
            application_name="etk-p11-system-standalone-settings",
        )
    )
    keys = (
        AI_PROMPTS_SETTING,
        CUSTOM_THEME_SETTING,
        MENU_CONFIG_SETTING,
        TELEGRAM_TEMPLATES_SETTING,
    )
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
        service = StandaloneSettingsService(repository, PlaintextStandaloneSettingsDefaults())

        service.save_custom_theme({"accent": "blue", "nested": {"enabled": True}})
        service.save_menu_config([{"key": "library", "visible": True}])
        templates = service.save_telegram_templates({"templates": {"library_new": "fixture-template"}})
        service.save_ai_prompts({"fast_mode": "fixture-fast"})

        assert repository.load(CUSTOM_THEME_SETTING) == {
            "accent": "blue",
            "nested": {"enabled": True},
        }
        assert service.get_menu_config() == [{"key": "library", "visible": True}]
        assert templates["library_new"] == "fixture-template"
        assert repository.load(TELEGRAM_TEMPLATES_SETTING) == templates
        assert repository.load(AI_PROMPTS_SETTING) == {"fast_mode": "fixture-fast"}
        assert service.get_ai_prompts()["fast_mode"] == "fixture-fast"
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
            AI_PROMPTS_SETTING: {"fast_mode": "fixture-fast"},
            CUSTOM_THEME_SETTING: {"accent": "blue", "nested": {"enabled": True}},
            MENU_CONFIG_SETTING: [{"key": "library", "visible": True}],
            TELEGRAM_TEMPLATES_SETTING: templates,
        }

        assert service.delete_custom_theme() is True
        service.reset_menu_config()
        service.reset_ai_prompts()
        assert repository.delete(TELEGRAM_TEMPLATES_SETTING) is True
        assert service.get_ai_prompts() == DEFAULT_AI_PROMPTS
        assert service.get_telegram_templates() == DEFAULT_TELEGRAM_NOTIFICATION_TEMPLATES
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
