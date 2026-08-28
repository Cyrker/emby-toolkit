from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from etk.domains.system.standalone import StandaloneSettingsRepository
from etk.policy_defaults import DEFAULT_RELEASE_GROUP_MAPPING

from .models import P115Result

P115_DEFAULT_STREAM_CONFIG_SETTING = "p115_default_stream_config"
RELEASE_GROUP_MAPPING_SETTING = "release_group_mapping"

_AUDIO_LANGUAGE_ALLOWED = frozenset({"chi", "yue", "original", "eng", "jpn", "kor"})
_SUBTITLE_LANGUAGE_ALLOWED = frozenset({"chs", "cht", "original", "eng", "jpn", "kor"})

DEFAULT_STREAM_CONFIG: dict[str, Any] = {
    "audio_lang": "",
    "subtitle_lang": "",
    "audio_lang_priority": [],
    "subtitle_lang_priority": [],
    "audio_priority_order": ["param", "feature"],
    "audio_features": ["公映", "上译", "京译", "央视", "长译", "八一", "国配", "台配", "国语", "粤语"],
    "audio_param_priority": [
        "atmos",
        "dts_x",
        "truehd",
        "dts_hd_ma",
        "dts_hd_hra",
        "ddp",
        "dts",
        "flac",
        "ac3",
        "aac",
        "7_1",
        "5_1",
        "2_0",
    ],
    "sub_priority": ["effect", "chs", "cht", "chs_eng", "cht_eng", "chs_jpn", "cht_jpn", "chs_kor", "cht_kor"],
    "realtime_sub_detect": False,
}


class P115LocalSettingsService:
    """Own local P115 policy settings without constructing a provider client."""

    def __init__(self, repository: StandaloneSettingsRepository) -> None:
        self._repository = repository

    def get_default_stream_config(self) -> P115Result:
        try:
            value = self._repository.load(P115_DEFAULT_STREAM_CONFIG_SETTING)
            return P115Result({"success": True, "data": normalize_default_stream_config(value)})
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)

    def save_default_stream_config(self, value: object) -> P115Result:
        try:
            config = normalize_default_stream_config(value)
            self._repository.save(P115_DEFAULT_STREAM_CONFIG_SETTING, config)
            return P115Result({"success": True, "message": "默认音轨与字幕配置已保存"})
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)

    def get_release_groups(self) -> P115Result:
        try:
            try:
                value = self._repository.load(RELEASE_GROUP_MAPPING_SETTING)
            except Exception:
                value = None
            mapping = coerce_release_group_mapping(value)
            options = [{"label": str(group), "value": str(group)} for group in mapping if str(group or "").strip()]
            options.sort(key=lambda item: item["label"].lower())
            return P115Result({"success": True, "data": options})
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)


def normalize_default_stream_config(value: object) -> dict[str, Any]:
    config = deepcopy(DEFAULT_STREAM_CONFIG)
    if isinstance(value, Mapping):
        config.update(value)

    config["audio_lang_priority"] = _clean_priority_list(
        config.get("audio_lang_priority"),
        _AUDIO_LANGUAGE_ALLOWED,
        config.get("audio_lang"),
    )
    config["subtitle_lang_priority"] = _clean_priority_list(
        config.get("subtitle_lang_priority"),
        _SUBTITLE_LANGUAGE_ALLOWED,
        config.get("subtitle_lang"),
    )
    config["audio_lang"] = config["audio_lang_priority"][0] if config["audio_lang_priority"] else ""
    config["subtitle_lang"] = config["subtitle_lang_priority"][0] if config["subtitle_lang_priority"] else ""

    raw_group_order = config.get("audio_priority_order")
    if not isinstance(raw_group_order, list):
        raw_group_order = DEFAULT_STREAM_CONFIG["audio_priority_order"]
    group_order: list[str] = []
    for raw_item in raw_group_order:
        item = str(raw_item or "").strip().lower()
        if item in {"param", "feature"} and item not in group_order:
            group_order.append(item)
    for item in DEFAULT_STREAM_CONFIG["audio_priority_order"]:
        if item not in group_order:
            group_order.append(item)
    config["audio_priority_order"] = group_order

    for key in ("audio_features", "audio_param_priority", "sub_priority"):
        raw_items = config.get(key)
        if not isinstance(raw_items, list):
            config[key] = deepcopy(DEFAULT_STREAM_CONFIG[key])
            continue
        cleaned: list[str] = []
        seen: set[str] = set()
        for raw_item in raw_items:
            item = str(raw_item or "").strip()
            if item and item not in seen:
                seen.add(item)
                cleaned.append(item)
        config[key] = cleaned

    config["realtime_sub_detect"] = bool(config.get("realtime_sub_detect", False))
    return config


def coerce_release_group_mapping(value: object) -> dict[str, list[str]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            value = None

    result: dict[str, list[str]] = {}
    if isinstance(value, Mapping):
        for raw_label, raw_aliases in value.items():
            label = str(raw_label or "").strip()
            if not label:
                continue
            aliases = raw_aliases
            if isinstance(aliases, Mapping):
                aliases = aliases.get("aliases") or aliases.get("en") or aliases.get("ids") or []
            elif isinstance(aliases, str):
                aliases = [aliases]
            if not isinstance(aliases, list):
                aliases = []
            result[label] = [str(alias).strip() for alias in aliases if str(alias or "").strip()]
    elif isinstance(value, list):
        for raw_item in value:
            if isinstance(raw_item, str):
                label = raw_item.strip()
                if label:
                    result[label] = []
                continue
            if not isinstance(raw_item, Mapping):
                continue
            label = str(raw_item.get("label") or raw_item.get("name") or "").strip()
            if not label:
                continue
            aliases = raw_item.get("aliases")
            if aliases is None:
                aliases = raw_item.get("en")
            if aliases is None:
                aliases = raw_item.get("ids")
            if isinstance(aliases, str):
                aliases = [part.strip() for part in aliases.split(",")]
            if not isinstance(aliases, list):
                aliases = []
            result[label] = [str(alias).strip() for alias in aliases if str(alias or "").strip()]

    return result or {label: list(aliases) for label, aliases in DEFAULT_RELEASE_GROUP_MAPPING.items()}


def _clean_priority_list(value: object, allowed: frozenset[str], legacy_value: object = "") -> list[str]:
    if isinstance(value, str):
        raw_items = [value] if value.strip() else []
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []

    result: list[str] = []
    for raw_item in raw_items:
        item = str(raw_item or "").strip().lower()
        if item and item in allowed and item not in result:
            result.append(item)
    legacy = str(legacy_value or "").strip().lower()
    if not result and legacy and legacy in allowed:
        result.append(legacy)
    return result


__all__ = [
    "DEFAULT_STREAM_CONFIG",
    "P115_DEFAULT_STREAM_CONFIG_SETTING",
    "P115LocalSettingsService",
    "RELEASE_GROUP_MAPPING_SETTING",
    "coerce_release_group_mapping",
    "normalize_default_stream_config",
]
