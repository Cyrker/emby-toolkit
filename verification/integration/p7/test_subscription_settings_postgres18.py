from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from importlib import import_module
from typing import Any, cast

from sqlalchemy import text

from etk.domains.subscription.actor import (
    ACTOR_DEFAULT_CONFIG_KEY,
    ActorSubscriptionService,
)
from etk.domains.subscription.cleanup import (
    CLEANUP_CONFIG_SETTING,
    CleanupService,
)
from etk.domains.subscription.watchlist import (
    SHARED_RESOURCE_CONFIG_SETTING,
    WATCHLIST_CONFIG_SETTING,
    WatchlistService,
)
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime, EngineConfig

LEGACY_CLEANUP_RULES_SETTING = "media_cleanup_rules"
LEGACY_CLEANUP_LIBRARY_IDS_SETTING = "media_cleanup_library_ids"
LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING = "media_cleanup_keep_one_per_res"
LEGACY_CLEANUP_DELETE_DELAY_SETTING = "media_cleanup_delete_delay"
CLEANUP_CONFIG_CUTOVER_SQL = str(
    import_module("migrations.versions.0020_p11_cleanup_config_cutover").CLEANUP_CONFIG_CUTOVER_SQL
)

SETTING_KEYS = (
    ACTOR_DEFAULT_CONFIG_KEY,
    WATCHLIST_CONFIG_SETTING,
    CLEANUP_CONFIG_SETTING,
    SHARED_RESOURCE_CONFIG_SETTING,
    LEGACY_CLEANUP_RULES_SETTING,
    LEGACY_CLEANUP_LIBRARY_IDS_SETTING,
    LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING,
    LEGACY_CLEANUP_DELETE_DELAY_SETTING,
)
CONSTRAINT = "verification_p11_subscription_settings"


class _ActorRepository:
    def __init__(self) -> None:
        self.created_config: dict[str, Any] | None = None

    def create_subscription(
        self,
        *,
        tmdb_person_id: object,
        actor_name: object,
        profile_path: object,
        config: Mapping[str, Any],
    ) -> int:
        del tmdb_person_id, actor_name, profile_path
        self.created_config = deepcopy(dict(config))
        return 701


def test_subscription_settings_use_canonical_rows_and_preserve_retired_source_rows(
    migrated_p7_postgres_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p7_postgres_url,
            application_name="etk-p11-subscription-settings",
        )
    )
    snapshots: list[dict[str, Any]] = []
    mutated = False
    try:
        runtime.start()
        with runtime.engine.connect() as connection:
            assert int(connection.scalar(text("SHOW server_version_num")) or 0) // 10000 == 18
            snapshots = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json, last_updated_at
                        FROM app_settings
                        WHERE setting_key = ANY(CAST(:keys AS text[]))
                        """
                    ),
                    {"keys": list(SETTING_KEYS)},
                ).mappings()
            ]

        mutated = True
        with runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
                {"keys": list(SETTING_KEYS)},
            )

        settings = SqlAlchemyAppSettingsRepository(runtime.unit_of_work)
        actor_defaults = {
            "start_year": 2001,
            "media_types": ["Movie", "TV"],
            "genres_include_json": [18],
            "genres_exclude_json": [],
            "min_rating": 7.2,
            "main_role_only": True,
            "min_vote_count": 25,
        }
        settings.save(ACTOR_DEFAULT_CONFIG_KEY, actor_defaults)
        actor_repository = _ActorRepository()
        actor = ActorSubscriptionService(
            repository=cast(Any, actor_repository),
            default_settings=settings,
            search=cast(Any, object()),
            refresh_intents=cast(Any, object()),
            runtime=cast(Any, object()),
        )
        created = actor.create_subscription(
            {
                "tmdb_person_id": 701,
                "actor_name": "P11 fixture actor",
                "profile_path": None,
            }
        )
        assert created.status_code == 201
        assert actor_repository.created_config == actor_defaults

        shared_fallback = {
            "p115_etk_intro_detection_enabled": "on",
            "legacy_only": {"kept": True},
        }
        settings.save(SHARED_RESOURCE_CONFIG_SETTING, shared_fallback)
        watchlist = WatchlistService(
            cast(Any, object()),
            settings,
            cast(Any, object()),
            cast(Any, object()),
        )
        migrated = watchlist.get_settings()
        assert migrated["intro_detection_trigger_mode"] == "import"
        assert settings.load(WATCHLIST_CONFIG_SETTING) == {"intro_detection_trigger_mode": "import"}
        watchlist.save_settings(
            {
                "douban_count_correction": True,
                "intro_detection_library_ids": ["library-a", "", "library-a", 42],
            }
        )
        watchlist_config = settings.load(WATCHLIST_CONFIG_SETTING)
        assert watchlist_config == {
            "intro_detection_trigger_mode": "import",
            "douban_count_correction": True,
            "revival_check_days": 365,
            "tg_channel_tracking": False,
            "intro_detection_credits_enabled": False,
            "intro_detection_library_ids": ["library-a", "42"],
        }
        assert settings.load(SHARED_RESOURCE_CONFIG_SETTING) == shared_fallback

        cleanup_fallbacks = {
            LEGACY_CLEANUP_RULES_SETTING: [
                {"id": "resolution", "enabled": True, "priority": ["2160p", "1080p"]},
            ],
            LEGACY_CLEANUP_LIBRARY_IDS_SETTING: ["legacy-library"],
            LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING: True,
            LEGACY_CLEANUP_DELETE_DELAY_SETTING: 12,
        }
        settings.save_many(cleanup_fallbacks)
        with runtime.engine.begin() as connection:
            connection.execute(text(CLEANUP_CONFIG_CUTOVER_SQL))
        cleanup = CleanupService(
            cast(Any, object()),
            settings,
            cast(Any, object()),
            cast(Any, object()),
            cast(Any, object()),
        )
        fallback_config = cleanup.get_settings()
        assert fallback_config["rules"][0]["priority"] == ["4K", "1080p", "480p"]
        assert fallback_config["library_ids"] == ["legacy-library"]
        assert fallback_config["keep_one_per_res"] is True
        assert fallback_config["delete_delay"] == 12
        cleanup.save_settings(
            {
                "rules": [{"id": "runtime", "enabled": False}],
                "library_ids": ["canonical-library"],
                "keep_one_per_res": False,
                "delete_delay": 3,
                "manual_delete_without_confirm": True,
            }
        )
        cleanup_config = {
            "rules": [{"id": "runtime", "enabled": False}],
            "library_ids": ["canonical-library"],
            "keep_one_per_res": False,
            "delete_delay": 3,
            "manual_delete_without_confirm": True,
        }
        assert settings.load(CLEANUP_CONFIG_SETTING) == cleanup_config
        assert settings.load_many(tuple(cleanup_fallbacks)) == cleanup_fallbacks
        canonical_effective = cleanup.get_settings()
        assert canonical_effective["rules"][0]["id"] == "runtime"
        assert canonical_effective["rules"][0]["enabled"] is False
        assert canonical_effective["library_ids"] == ["canonical-library"]
        assert canonical_effective["keep_one_per_res"] is False
        assert canonical_effective["delete_delay"] == 3
        assert canonical_effective["manual_delete_without_confirm"] is True

        with runtime.engine.connect() as connection:
            canonical_rows = {
                str(row["setting_key"]): row["value_json"]
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json
                        FROM app_settings
                        WHERE setting_key = ANY(CAST(:keys AS text[]))
                        """
                    ),
                    {
                        "keys": [
                            ACTOR_DEFAULT_CONFIG_KEY,
                            WATCHLIST_CONFIG_SETTING,
                            CLEANUP_CONFIG_SETTING,
                        ]
                    },
                ).mappings()
            }
        assert canonical_rows == {
            ACTOR_DEFAULT_CONFIG_KEY: actor_defaults,
            WATCHLIST_CONFIG_SETTING: watchlist_config,
            CLEANUP_CONFIG_SETTING: cleanup_config,
        }

        with runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
            connection.execute(
                text(
                    f"""
                    ALTER TABLE app_settings ADD CONSTRAINT {CONSTRAINT}
                    CHECK (
                        setting_key <> '{ACTOR_DEFAULT_CONFIG_KEY}'
                        OR value_json ->> 'reject' IS DISTINCT FROM 'true'
                    )
                    """
                )
            )
        failed = actor.save_default_config({"reject": True})
        assert failed.status_code == 500
        assert failed.payload == {"error": "保存默认配置时发生服务器内部错误"}
        assert settings.load(ACTOR_DEFAULT_CONFIG_KEY) == actor_defaults
    finally:
        try:
            if mutated:
                with runtime.engine.begin() as connection:
                    connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
                    connection.execute(
                        text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
                        {"keys": list(SETTING_KEYS)},
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
