from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .washing_priority import (
    P115_WASHING_PRIORITY_CONFIG_SETTING,
    P115_WASHING_PRIORITY_RECALCULATE_SETTING,
    build_washing_priority_recalculate_scope,
    effective_washing_priority_config,
    effective_washing_priority_recalculate_state,
    merge_washing_priority_recalculate_state,
    normalize_washing_priority_config_for_save,
    normalize_washing_priority_recalculate_scope,
)

_LOCK_SCOPE = "p115:washing-priority"


class SqlAlchemyP115WashingPriorityRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def list_groups(self) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT id, name, media_type, target_cids, priorities, sort_order
                    FROM washing_priority_groups
                    ORDER BY sort_order ASC
                    """
                )
            ).mappings()
            return [dict(row) for row in rows]

    def replace_groups(
        self,
        groups: Sequence[object],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        with self._unit_of_work() as uow:
            _lock(uow)
            old_groups = [
                dict(row)
                for row in uow.session.execute(
                    text(
                        """
                        SELECT id, name, media_type, target_cids, priorities, sort_order
                        FROM washing_priority_groups
                        ORDER BY sort_order ASC
                        FOR UPDATE
                        """
                    )
                ).mappings()
            ]
            affected_scope = build_washing_priority_recalculate_scope(old_groups, list(groups))
            current_state = _locked_setting(uow, P115_WASHING_PRIORITY_RECALCULATE_SETTING)
            pending_state = merge_washing_priority_recalculate_state(current_state, affected_scope)

            uow.session.execute(text("DELETE FROM washing_priority_groups"))
            for index, raw_group in enumerate(groups):
                if not isinstance(raw_group, Mapping):
                    raise TypeError("washing priority group must be an object")
                uow.session.execute(
                    text(
                        """
                        INSERT INTO washing_priority_groups (
                            name,
                            media_type,
                            target_cids,
                            priorities,
                            sort_order
                        )
                        VALUES (
                            :name,
                            :media_type,
                            CAST(:target_cids AS jsonb),
                            CAST(:priorities AS jsonb),
                            :sort_order
                        )
                        """
                    ),
                    {
                        "name": raw_group["name"],
                        "media_type": raw_group["media_type"],
                        "target_cids": json.dumps(
                            raw_group.get("target_cids", []),
                            ensure_ascii=False,
                        ),
                        "priorities": json.dumps(
                            raw_group.get("priorities", []),
                            ensure_ascii=False,
                        ),
                        "sort_order": index,
                    },
                )
            if affected_scope:
                _save_setting(uow, P115_WASHING_PRIORITY_RECALCULATE_SETTING, pending_state)
            uow.commit()
        return affected_scope, dict(pending_state.get("media_types", {}))

    def save_config(self, value: Mapping[str, Any]) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            _lock(uow)
            rows = uow.session.execute(
                text(
                    """
                    SELECT setting_key, value_json
                    FROM app_settings
                    WHERE setting_key = ANY(:keys)
                    """
                ),
                {
                    "keys": [
                        P115_WASHING_PRIORITY_CONFIG_SETTING,
                        "p115_rename_config",
                    ]
                },
            ).mappings()
            values = {str(row["setting_key"]): row["value_json"] for row in rows}
            current, _migrated = effective_washing_priority_config(
                values.get(P115_WASHING_PRIORITY_CONFIG_SETTING),
                values.get("p115_rename_config"),
            )
            clean_config = normalize_washing_priority_config_for_save(value, current)
            slots_changed = (
                bool(current.get("version_slots_enabled")) != clean_config["version_slots_enabled"]
                or current.get("version_slots", []) != clean_config["version_slots"]
            )
            _save_setting(uow, P115_WASHING_PRIORITY_CONFIG_SETTING, clean_config)
            if slots_changed:
                current_state = _locked_setting(
                    uow,
                    P115_WASHING_PRIORITY_RECALCULATE_SETTING,
                )
                pending_state = merge_washing_priority_recalculate_state(
                    current_state,
                    {
                        "Movie": {"all": True, "target_cids": []},
                        "Series": {"all": True, "target_cids": []},
                    },
                )
                _save_setting(
                    uow,
                    P115_WASHING_PRIORITY_RECALCULATE_SETTING,
                    pending_state,
                )
            uow.commit()
        return clean_config

    def load_recalculate_state(self) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT value_json
                    FROM app_settings
                    WHERE setting_key = :key
                    """
                ),
                {"key": P115_WASHING_PRIORITY_RECALCULATE_SETTING},
            )
        return effective_washing_priority_recalculate_state(value)

    def acknowledge_recalculate_scope(
        self,
        expected_revision: int,
        processed_scope: object,
    ) -> bool:
        processed = normalize_washing_priority_recalculate_scope(processed_scope)
        if not processed:
            return False
        with self._unit_of_work() as uow:
            _lock(uow)
            current = _locked_setting(uow, P115_WASHING_PRIORITY_RECALCULATE_SETTING)
            if int(current.get("revision") or 0) != int(expected_revision or 0):
                return False
            remaining = normalize_washing_priority_recalculate_scope(current)
            for media_type, entry in processed.items():
                existing = remaining.get(media_type)
                if not existing:
                    continue
                if entry.get("all"):
                    remaining.pop(media_type, None)
                    continue
                if existing.get("all"):
                    continue
                target_cids = sorted(set(existing.get("target_cids", [])) - set(entry.get("target_cids", [])))
                if target_cids:
                    existing["target_cids"] = target_cids
                else:
                    remaining.pop(media_type, None)
            _save_setting(
                uow,
                P115_WASHING_PRIORITY_RECALCULATE_SETTING,
                {
                    "revision": int(expected_revision or 0),
                    "media_types": remaining,
                },
            )
            uow.commit()
        return True


def _lock(uow: SqlAlchemyUnitOfWork) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": _LOCK_SCOPE},
    )


def _locked_setting(uow: SqlAlchemyUnitOfWork, key: str) -> dict[str, Any]:
    value = uow.session.scalar(
        text(
            """
            SELECT value_json
            FROM app_settings
            WHERE setting_key = :key
            FOR UPDATE
            """
        ),
        {"key": key},
    )
    return dict(value) if isinstance(value, Mapping) else {}


def _save_setting(
    uow: SqlAlchemyUnitOfWork,
    key: str,
    value: Mapping[str, Any],
) -> None:
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
        {
            "key": key,
            "value": json.dumps(dict(value), ensure_ascii=False),
        },
    )


__all__ = ["SqlAlchemyP115WashingPriorityRepository"]
