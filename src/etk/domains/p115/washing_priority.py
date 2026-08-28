from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from .models import P115Result

P115_WASHING_PRIORITY_CONFIG_SETTING = "p115_washing_priority_config"
P115_WASHING_PRIORITY_RECALCULATE_SETTING = "p115_washing_priority_recalculate_state"


class P115WashingPrioritySettingsRepository(Protocol):
    def load_many(self, keys: Sequence[str]) -> Mapping[str, Any]: ...

    def save(self, key: str, value: Any) -> None: ...


class P115WashingPriorityRepository(Protocol):
    def list_groups(self) -> list[dict[str, Any]]: ...

    def replace_groups(
        self,
        groups: Sequence[object],
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...

    def save_config(self, value: Mapping[str, Any]) -> dict[str, Any]: ...

    def load_recalculate_state(self) -> dict[str, Any]: ...

    def acknowledge_recalculate_scope(
        self,
        expected_revision: int,
        processed_scope: object,
    ) -> bool: ...


class P115WashingPriorityService:
    def __init__(
        self,
        *,
        settings: P115WashingPrioritySettingsRepository,
        repository: P115WashingPriorityRepository,
    ) -> None:
        self._settings = settings
        self._repository = repository

    def get_config(self) -> P115Result:
        try:
            values = self._settings.load_many(
                (P115_WASHING_PRIORITY_CONFIG_SETTING, "p115_rename_config"),
            )
            config, migrated = effective_washing_priority_config(
                values.get(P115_WASHING_PRIORITY_CONFIG_SETTING),
                values.get("p115_rename_config"),
            )
            if migrated is not None:
                try:
                    self._settings.save(P115_WASHING_PRIORITY_CONFIG_SETTING, migrated)
                except Exception:
                    pass
            return P115Result({"success": True, "data": config})
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)

    def save_config(self, value: object) -> P115Result:
        try:
            payload = dict(value) if isinstance(value, Mapping) else {}
            config = self._repository.save_config(payload)
            return P115Result({"success": True, "message": "洗版配置已保存", "data": config})
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)

    def save_conflict_mode(self, conflict_mode: object) -> None:
        self._repository.save_config({"conflict_mode": conflict_mode})

    def list_groups(self) -> P115Result:
        try:
            return P115Result({"success": True, "data": self._repository.list_groups()})
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)

    def replace_groups(self, groups: object) -> P115Result:
        if not isinstance(groups, list):
            return P115Result({"success": False, "message": "规则组数据格式错误"}, 400)
        try:
            affected_scope, pending_scope = self._repository.replace_groups(groups)
            return P115Result(
                {
                    "success": True,
                    "message": "洗版优先级规则已保存",
                    "data": {
                        "affected_scope": affected_scope,
                        "pending_scope": pending_scope,
                    },
                }
            )
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)

    def load_recalculate_state(self) -> dict[str, Any]:
        return self._repository.load_recalculate_state()

    def acknowledge_recalculate_scope(
        self,
        expected_revision: int,
        processed_scope: object,
    ) -> bool:
        return self._repository.acknowledge_recalculate_scope(expected_revision, processed_scope)


def effective_washing_priority_config(
    value: object,
    legacy_rename_config: object = None,
    *,
    default_conflict_mode: str = "replace",
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    default_mode = normalize_washing_conflict_mode(default_conflict_mode, "replace")
    config = _json_mapping(value)
    mode = config.get("conflict_mode")
    migrated: dict[str, Any] | None = None
    if not mode:
        legacy = _json_mapping(legacy_rename_config)
        mode = legacy.get("conflict_mode")
        if mode:
            config["conflict_mode"] = normalize_washing_conflict_mode(mode, default_mode)
            migrated = dict(config)

    config["conflict_mode"] = normalize_washing_conflict_mode(config.get("conflict_mode"), default_mode)
    config["skip_scope"] = normalize_washing_skip_scope(config.get("skip_scope"), "directory")
    config["version_slots_enabled"] = bool(config.get("version_slots_enabled"))
    config["version_slots"] = normalize_washing_version_slots(config.get("version_slots"))
    return config, migrated


def normalize_washing_priority_config_for_save(
    value: object,
    current: Mapping[str, Any],
) -> dict[str, Any]:
    config = dict(value) if isinstance(value, Mapping) else {}
    return {
        "conflict_mode": normalize_washing_conflict_mode(config.get("conflict_mode"), "replace"),
        "skip_scope": normalize_washing_skip_scope(config.get("skip_scope"), "directory"),
        "version_slots_enabled": bool(
            config.get("version_slots_enabled")
            if "version_slots_enabled" in config
            else current.get("version_slots_enabled")
        ),
        "version_slots": normalize_washing_version_slots(
            config.get("version_slots") if "version_slots" in config else current.get("version_slots")
        ),
    }


def normalize_washing_conflict_mode(value: object, default: str = "replace") -> str:
    mode = str(value or default or "").strip().lower()
    aliases = {
        "overwrite": "replace",
        "washing": "replace",
        "wash": "replace",
        "洗版": "replace",
        "替换": "replace",
        "skip_existing": "skip",
        "跳过": "skip",
        "keep": "keep_both",
        "both": "keep_both",
        "keepboth": "keep_both",
        "keep-both": "keep_both",
        "保留两者": "keep_both",
        "共存": "keep_both",
    }
    mode = aliases.get(mode, mode)
    return mode if mode in {"replace", "keep_both", "skip"} else default


def normalize_washing_skip_scope(value: object, default: str = "directory") -> str:
    scope = str(value or default or "").strip().lower()
    aliases = {
        "dir": "directory",
        "same_dir": "directory",
        "same-directory": "directory",
        "same_directory": "directory",
        "folder": "directory",
        "current": "directory",
        "同目录": "directory",
        "当前目录": "directory",
        "all": "library",
        "global": "library",
        "full": "library",
        "full_library": "library",
        "full-library": "library",
        "all_library": "library",
        "all-library": "library",
        "library": "library",
        "全库": "library",
        "媒体库": "library",
    }
    scope = aliases.get(scope, scope)
    return scope if scope in {"directory", "library"} else default


def normalize_washing_version_slots(value: object) -> list[dict[str, Any]]:
    slots = washing_json_list(value)
    normalized: list[dict[str, Any]] = []
    list_fields = (
        "resolution",
        "source",
        "codec",
        "effect",
        "audio",
        "subtitle",
        "release_group",
    )
    for index, slot in enumerate(slots):
        if not isinstance(slot, Mapping):
            continue
        raw_id = str(slot.get("id") or f"slot_{index + 1}").strip()
        slot_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", raw_id).strip("_") or f"slot_{index + 1}"
        name = str(slot.get("name") or f"版本槽位 {index + 1}").strip()
        suffix = str(slot.get("suffix") or name).strip()
        raw_match = slot.get("match")
        match = raw_match if isinstance(raw_match, Mapping) else {}
        clean_match: dict[str, Any] = {}
        for field in list_fields:
            values = match.get(field)
            if not isinstance(values, list):
                values = [values] if values not in (None, "") else []
            clean_match[field] = list(dict.fromkeys(str(item).strip() for item in values if str(item or "").strip()))
        clean_match["subtitle_effect"] = bool(match.get("subtitle_effect"))
        clean_match["clean_version"] = bool(match.get("clean_version"))
        clean_match["exempt_original_lang"] = bool(match.get("exempt_original_lang"))
        clean_match["min_size_gb"] = match.get("min_size_gb")
        clean_match["max_size_gb"] = match.get("max_size_gb")
        normalized.append(
            {
                "id": slot_id,
                "name": name,
                "suffix": suffix,
                "fallback": bool(slot.get("fallback")),
                "match": clean_match,
            }
        )
    return normalized


def build_washing_priority_recalculate_scope(
    old_groups: object,
    new_groups: object,
) -> dict[str, Any]:
    old_normalized = normalize_washing_priority_groups(old_groups)
    new_normalized = normalize_washing_priority_groups(new_groups)
    explicit_cids = sorted({cid for group in old_normalized + new_normalized for cid in group["target_cids"]})
    affected: dict[str, Any] = {}
    unmatched_cid = "__etk_unmatched_washing_priority_cid__"
    for media_type in ("Movie", "Series"):
        old_global = _effective_washing_priorities(old_normalized, media_type, unmatched_cid)
        new_global = _effective_washing_priorities(new_normalized, media_type, unmatched_cid)
        if old_global != new_global:
            affected[media_type] = {"all": True, "target_cids": []}
            continue
        changed_cids = [
            cid
            for cid in explicit_cids
            if _effective_washing_priorities(old_normalized, media_type, cid)
            != _effective_washing_priorities(new_normalized, media_type, cid)
        ]
        if changed_cids:
            affected[media_type] = {"all": False, "target_cids": changed_cids}
    return affected


def normalize_washing_priority_groups(groups: object) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for group in groups if isinstance(groups, list) else []:
        if not isinstance(group, Mapping):
            continue
        media_type = str(group.get("media_type") or "All").strip().title()
        if media_type not in {"All", "Movie", "Series"}:
            media_type = "All"
        target_cids = sorted(
            {str(cid).strip() for cid in washing_json_list(group.get("target_cids")) if str(cid or "").strip()}
        )
        priorities = [
            canonicalize_washing_rule_value(rule)
            for rule in washing_json_list(group.get("priorities"))
            if isinstance(rule, Mapping)
        ]
        normalized.append(
            {
                "media_type": media_type,
                "target_cids": target_cids,
                "priorities": priorities,
            }
        )
    return normalized


def canonicalize_washing_rule_value(value: object) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): canonicalize_washing_rule_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if key not in {"_uid", "_group_name"}
        }
    if isinstance(value, list):
        normalized = [canonicalize_washing_rule_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
        )
    return value


def normalize_washing_priority_recalculate_scope(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    raw_media_types = value.get("media_types")
    raw_scope = raw_media_types if isinstance(raw_media_types, Mapping) else value
    normalized: dict[str, Any] = {}
    for media_type in ("Movie", "Series"):
        entry = raw_scope.get(media_type)
        if not isinstance(entry, Mapping):
            continue
        is_all = bool(entry.get("all"))
        target_cids = sorted(
            {str(cid).strip() for cid in washing_json_list(entry.get("target_cids")) if str(cid or "").strip()}
        )
        if is_all or target_cids:
            normalized[media_type] = {
                "all": is_all,
                "target_cids": [] if is_all else target_cids,
            }
    return normalized


def merge_washing_priority_recalculate_state(
    current: object,
    added_scope: object,
) -> dict[str, Any]:
    current_mapping = current if isinstance(current, Mapping) else {}
    merged = normalize_washing_priority_recalculate_scope(current_mapping)
    added = normalize_washing_priority_recalculate_scope(added_scope)
    if not added:
        return {
            "revision": int(current_mapping.get("revision") or 0),
            "media_types": merged,
        }
    for media_type, entry in added.items():
        existing = merged.get(media_type, {"all": False, "target_cids": []})
        if existing.get("all") or entry.get("all"):
            merged[media_type] = {"all": True, "target_cids": []}
        else:
            merged[media_type] = {
                "all": False,
                "target_cids": sorted(set(existing.get("target_cids", [])) | set(entry.get("target_cids", []))),
            }
    return {
        "revision": int(current_mapping.get("revision") or 0) + 1,
        "media_types": merged,
    }


def effective_washing_priority_recalculate_state(value: object) -> dict[str, Any]:
    state = value if isinstance(value, Mapping) else {}
    return {
        "revision": int(state.get("revision") or 0),
        "media_types": normalize_washing_priority_recalculate_scope(state),
    }


def washing_json_list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value or "[]")
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _effective_washing_priorities(
    groups: Sequence[Mapping[str, Any]],
    media_type: str,
    target_cid: str,
) -> list[Any]:
    priorities: list[Any] = []
    for group in groups:
        if group["media_type"] not in {media_type, "All"}:
            continue
        target_cids = group["target_cids"]
        if target_cids and target_cid not in target_cids:
            continue
        priorities.extend(group["priorities"])
    return priorities


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {}
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "P115_WASHING_PRIORITY_CONFIG_SETTING",
    "P115_WASHING_PRIORITY_RECALCULATE_SETTING",
    "P115WashingPriorityRepository",
    "P115WashingPriorityService",
    "P115WashingPrioritySettingsRepository",
    "build_washing_priority_recalculate_scope",
    "canonicalize_washing_rule_value",
    "effective_washing_priority_config",
    "effective_washing_priority_recalculate_state",
    "merge_washing_priority_recalculate_state",
    "normalize_washing_conflict_mode",
    "normalize_washing_priority_config_for_save",
    "normalize_washing_priority_groups",
    "normalize_washing_priority_recalculate_scope",
    "normalize_washing_skip_scope",
    "normalize_washing_version_slots",
    "washing_json_list",
]
