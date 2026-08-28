from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

from etk.domains.p115.local_config_service import (
    P115_DEFAULT_STREAM_CONFIG_SETTING,
    RELEASE_GROUP_MAPPING_SETTING,
    P115LocalSettingsService,
)
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime, EngineConfig


def test_local_settings_round_trip_through_native_postgres18_repository(
    migrated_p6_postgres_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p6_postgres_url,
            application_name="etk-p11-p115-local-settings",
        )
    )
    keys = (P115_DEFAULT_STREAM_CONFIG_SETTING, RELEASE_GROUP_MAPPING_SETTING)
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
        service = P115LocalSettingsService(repository)

        saved = service.save_default_stream_config(
            {
                "audio_lang": "eng",
                "audio_lang_priority": ["JPN", "eng", "JPN"],
                "subtitle_lang_priority": "CHT",
                "audio_priority_order": ["feature"],
                "realtime_sub_detect": True,
            }
        )
        assert saved.status_code == 200
        loaded = service.get_default_stream_config().payload
        assert isinstance(loaded, dict)
        assert isinstance(loaded.get("data"), Mapping)
        expected_stream = dict(loaded["data"])
        assert expected_stream["audio_lang_priority"] == ["jpn", "eng"]
        assert expected_stream["subtitle_lang_priority"] == ["cht"]
        assert expected_stream["audio_priority_order"] == ["feature", "param"]
        assert repository.load(P115_DEFAULT_STREAM_CONFIG_SETTING) == expected_stream

        repository.save(
            RELEASE_GROUP_MAPPING_SETTING,
            {"Zulu": ["Z"], "alpha": ["A"]},
        )
        assert service.get_release_groups().payload == {
            "success": True,
            "data": [
                {"label": "alpha", "value": "alpha"},
                {"label": "Zulu", "value": "Zulu"},
            ],
        }
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
