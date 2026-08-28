from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.p115.washing_priority import (
    P115_WASHING_PRIORITY_CONFIG_SETTING,
    P115_WASHING_PRIORITY_RECALCULATE_SETTING,
    P115WashingPriorityService,
)
from etk.domains.p115.washing_priority_repository import SqlAlchemyP115WashingPriorityRepository
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime, EngineConfig


def test_washing_priority_uow_state_revision_and_rollback_on_postgres18(
    migrated_p6_postgres_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p6_postgres_url,
            application_name="etk-p11-p115-washing-priority",
        )
    )
    setting_keys = (
        P115_WASHING_PRIORITY_CONFIG_SETTING,
        P115_WASHING_PRIORITY_RECALCULATE_SETTING,
        "p115_rename_config",
    )
    setting_snapshots: list[dict[str, Any]] = []
    group_snapshots: list[dict[str, Any]] = []
    sequence_snapshot: dict[str, Any] | None = None
    mutated = False
    try:
        runtime.start()
        with runtime.engine.begin() as connection:
            assert int(connection.scalar(text("SHOW server_version_num")) or 0) // 10000 == 18
            setting_snapshots = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json, last_updated_at
                        FROM app_settings
                        WHERE setting_key = ANY(:keys)
                        """
                    ),
                    {"keys": list(setting_keys)},
                ).mappings()
            ]
            group_snapshots = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT id, name, media_type, target_cids, priorities, sort_order
                        FROM washing_priority_groups
                        ORDER BY id
                        """
                    )
                ).mappings()
            ]
            sequence_snapshot = dict(
                connection.execute(
                    text(
                        """
                        SELECT last_value, is_called
                        FROM washing_priority_groups_id_seq
                        """
                    )
                )
                .mappings()
                .one()
            )
            connection.execute(text("DELETE FROM washing_priority_groups"))
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = ANY(:keys)"),
                {"keys": list(setting_keys)},
            )
        mutated = True

        repository = SqlAlchemyP115WashingPriorityRepository(lambda: runtime.unit_of_work())
        settings = SqlAlchemyAppSettingsRepository(runtime.unit_of_work)
        service = P115WashingPriorityService(settings=settings, repository=repository)

        assert repository.replace_groups([]) == ({}, {})
        assert settings.load(P115_WASHING_PRIORITY_RECALCULATE_SETTING) is None

        saved = service.save_config(
            {
                "conflict_mode": "keep",
                "skip_scope": "all",
                "version_slots_enabled": True,
                "version_slots": [
                    {
                        "id": "Main 4K",
                        "name": "主版本",
                        "match": {
                            "resolution": "4k",
                            "source": ["REMUX", "REMUX"],
                            "clean_version": True,
                        },
                    }
                ],
            }
        )
        assert saved.status_code == 200
        assert isinstance(saved.payload, Mapping)
        saved_data = saved.payload.get("data")
        assert isinstance(saved_data, Mapping)
        assert saved_data["conflict_mode"] == "keep_both"
        assert saved_data["skip_scope"] == "library"
        version_slots = saved_data["version_slots"]
        assert isinstance(version_slots, list)
        assert version_slots[0]["id"] == "Main_4K"
        assert repository.load_recalculate_state() == {
            "revision": 1,
            "media_types": {
                "Movie": {"all": True, "target_cids": []},
                "Series": {"all": True, "target_cids": []},
            },
        }

        group = {
            "name": "电影规则",
            "media_type": "Movie",
            "target_cids": ["100"],
            "priorities": [{"resolution": ["4k"]}],
        }
        replaced = service.replace_groups([group])
        assert replaced.status_code == 200
        assert isinstance(replaced.payload, Mapping)
        replaced_data = replaced.payload.get("data")
        assert isinstance(replaced_data, Mapping)
        assert replaced_data["affected_scope"] == {"Movie": {"all": False, "target_cids": ["100"]}}
        assert replaced_data["pending_scope"] == {
            "Movie": {"all": True, "target_cids": []},
            "Series": {"all": True, "target_cids": []},
        }
        rows = repository.list_groups()
        assert len(rows) == 1
        assert rows[0]["name"] == "电影规则"
        assert rows[0]["sort_order"] == 0
        assert rows[0]["target_cids"] == ["100"]
        state_before_failure = repository.load_recalculate_state()
        assert state_before_failure["revision"] == 2

        with pytest.raises(KeyError):
            repository.replace_groups(
                [
                    group,
                    {"media_type": "Series", "target_cids": [], "priorities": []},
                ]
            )
        assert repository.list_groups() == rows
        assert repository.load_recalculate_state() == state_before_failure

        changed = repository.replace_groups(
            [
                {
                    **group,
                    "target_cids": ["200"],
                }
            ]
        )
        assert changed[0] == {"Movie": {"all": False, "target_cids": ["100", "200"]}}
        latest_state = repository.load_recalculate_state()
        assert latest_state["revision"] == 3
        assert (
            repository.acknowledge_recalculate_scope(
                2,
                {"Movie": {"all": True, "target_cids": []}},
            )
            is False
        )
        assert repository.load_recalculate_state() == latest_state
        assert (
            repository.acknowledge_recalculate_scope(
                3,
                {"Movie": {"all": True, "target_cids": []}},
            )
            is True
        )
        assert repository.load_recalculate_state() == {
            "revision": 3,
            "media_types": {"Series": {"all": True, "target_cids": []}},
        }

        persisted_config = settings.load(P115_WASHING_PRIORITY_CONFIG_SETTING)
        assert isinstance(persisted_config, Mapping)
        assert persisted_config["conflict_mode"] == "keep_both"
    finally:
        try:
            if mutated:
                with runtime.engine.begin() as connection:
                    connection.execute(text("DELETE FROM washing_priority_groups"))
                    for snapshot in group_snapshots:
                        connection.execute(
                            text(
                                """
                                INSERT INTO washing_priority_groups (
                                    id,
                                    name,
                                    media_type,
                                    target_cids,
                                    priorities,
                                    sort_order
                                )
                                VALUES (
                                    :id,
                                    :name,
                                    :media_type,
                                    CAST(:target_cids AS jsonb),
                                    CAST(:priorities AS jsonb),
                                    :sort_order
                                )
                                """
                            ),
                            {
                                "id": snapshot["id"],
                                "name": snapshot["name"],
                                "media_type": snapshot["media_type"],
                                "target_cids": json.dumps(snapshot["target_cids"], ensure_ascii=False),
                                "priorities": json.dumps(snapshot["priorities"], ensure_ascii=False),
                                "sort_order": snapshot["sort_order"],
                            },
                        )
                    connection.execute(
                        text("DELETE FROM app_settings WHERE setting_key = ANY(:keys)"),
                        {"keys": list(setting_keys)},
                    )
                    for snapshot in setting_snapshots:
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
                    if sequence_snapshot is not None:
                        connection.execute(
                            text(
                                """
                                SELECT setval(
                                    'washing_priority_groups_id_seq',
                                    :last_value,
                                    :is_called
                                )
                                """
                            ),
                            sequence_snapshot,
                        )
        finally:
            runtime.shutdown()
