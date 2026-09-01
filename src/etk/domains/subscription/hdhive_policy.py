from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

HDHIVE_FILTER_DEFAULTS: dict[str, Any] = {
    "free_only": False,
    "max_points": 10,
    "max_size_gb": 120.0,
    "resolution": "All",
    "zh_sub_only": True,
    "exclude_iso": False,
}


def normalize_hdhive_media_type(media_type: object, item_type: object = None) -> str:
    raw = str(media_type or item_type or "").strip().casefold()
    if raw in {"movie", "movies", "film", "films"}:
        return "movie"
    if raw in {
        "tv",
        "series",
        "season",
        "episode",
        "show",
        "shows",
        "tvshow",
        "tvshows",
        "电视剧",
        "剧集",
        "季",
        "集",
    }:
        return "tv"
    return "movie" if not raw else "tv"


def normalize_filter_config(value: object) -> dict[str, Any]:
    source = value if isinstance(value, Mapping) else {}
    return {
        "free_only": bool(source.get("free_only", False)),
        "max_points": _integer(source.get("max_points"), default=10, minimum=0, maximum=100_000),
        "max_size_gb": _number(source.get("max_size_gb"), default=120.0, minimum=0.0, maximum=100_000.0),
        "resolution": str(source.get("resolution") or "All").strip()[:40] or "All",
        "zh_sub_only": bool(source.get("zh_sub_only", True)),
        "exclude_iso": bool(source.get("exclude_iso", False)),
    }


def filter_hdhive_resources(
    resources: Sequence[Mapping[str, Any]],
    *,
    config: Mapping[str, Any] | None = None,
    target_season: object = None,
    media_type: str | None = None,
    require_complete: bool = False,
) -> list[dict[str, Any]]:
    cfg = {**HDHIVE_FILTER_DEFAULTS, **normalize_filter_config(config)}
    filtered: list[dict[str, Any]] = []
    for candidate in resources:
        if not isinstance(candidate, Mapping):
            continue
        item = dict(candidate)
        points = _integer(item.get("unlock_points"), default=0, minimum=0, maximum=100_000)
        owned = bool(item.get("already_owned"))
        if bool(cfg["free_only"]) and not owned and points > 0:
            continue
        if not bool(cfg["free_only"]) and not owned and points > int(cfg["max_points"]):
            continue
        size_gb = _size_gb(item)
        if size_gb is not None and size_gb > float(cfg["max_size_gb"]):
            continue
        if not _resolution_matches(item, str(cfg["resolution"])):
            continue
        if bool(cfg["zh_sub_only"]) and not _has_chinese_subtitle(item):
            continue
        if bool(cfg["exclude_iso"]) and _is_original_disc(item):
            continue
        item["_effective_points"] = 0 if owned else points
        item["_size_gb"] = size_gb or 0.0
        if media_type == "tv":
            completion = _completion_level(item)
            item["_completion_level"] = completion
            item["_completion_label"] = _completion_label(completion)
        filtered.append(item)

    if media_type == "tv" and target_season not in (None, ""):
        filtered = _rank_for_season(filtered, target_season)
    if media_type == "tv" and require_complete:
        filtered = [item for item in filtered if int(item.get("_completion_level") or 0) >= 30]
    return filtered


def _rank_for_season(resources: Sequence[dict[str, Any]], target_season: object) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for item in resources:
        level = _season_match_level(item, target_season)
        item["_season_match_level"] = level
        item["_season_match_label"] = _season_match_label(level)
        if level < 0:
            continue
        completion = _completion_level(item)
        item["_completion_level"] = completion
        item["_completion_label"] = _completion_label(completion)
        ranked.append(item)
    ranked.sort(
        key=lambda item: (
            -int(item.get("_completion_level") or 0),
            -int(item.get("_season_match_level") or 0),
            int(item.get("_effective_points") or 0),
            0 if str(item.get("pan_type") or "115").casefold() == "115" else 1,
            -float(item.get("_size_gb") or 0),
        )
    )
    return ranked


def _size_gb(item: Mapping[str, Any]) -> float | None:
    raw = next(
        (
            value
            for value in (
                item.get("share_size"),
                item.get("size"),
                item.get("file_size"),
                item.get("total_size"),
            )
            if value is not None
        ),
        None,
    )
    if raw is None:
        return None
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw) / 1024**3 if float(raw) > 10_000 else float(raw)
    match = re.search(r"([\d.]+)\s*(TB|GB|MB|KB|B)?", str(raw).upper().replace(",", ""))
    if match is None:
        return None
    value = float(match.group(1))
    return value * {"TB": 1024, "GB": 1, "MB": 1 / 1024, "KB": 1 / 1024**2, "B": 1 / 1024**3}.get(
        match.group(2) or "GB",
        1,
    )


def _resource_text(item: Mapping[str, Any]) -> str:
    values: list[str] = []

    def append(value: object) -> None:
        if isinstance(value, Mapping):
            for nested in value.values():
                append(nested)
        elif isinstance(value, (list, tuple, set)):
            for nested in value:
                append(nested)
        elif value not in (None, ""):
            values.append(str(value).strip())

    for key in (
        "title",
        "name",
        "remark",
        "description",
        "summary",
        "filename",
        "file_name",
        "resource_name",
        "share_name",
        "subtitle",
        "subtitles",
        "audio",
        "source",
        "video_resolution",
        "video_codec",
        "format",
        "tags",
    ):
        append(item.get(key))
    return " ".join(value for value in values if value)


def _has_chinese_subtitle(item: Mapping[str, Any]) -> bool:
    text = _resource_text(item).casefold()
    if any(
        token in text
        for token in (
            "中字",
            "中文字幕",
            "简中",
            "繁中",
            "简体",
            "繁体",
            "简繁",
            "双语",
            "中英",
            "中日",
            "中韩",
            "中文",
            "粤字",
        )
    ):
        return True
    return any(
        re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text)
        for token in (
            "chs",
            "cht",
            "chi",
            "zho",
            "zh-cn",
            "zh-tw",
            "zh-hk",
            "zh-hans",
            "zh-hant",
            "chinese",
            "mandarin",
            "cantonese",
        )
    )


def _resolution_matches(item: Mapping[str, Any], expected: str) -> bool:
    if not expected or expected == "All":
        return True
    value = item.get("video_resolution")
    values = value if isinstance(value, list) else [value]
    text = " ".join(str(part) for part in values if part).strip() or _resource_text(item)
    lowered = text.casefold()
    return {
        "4K": "4k" in lowered or "2160" in lowered or "uhd" in lowered,
        "1080p": "1080" in lowered,
        "720p": "720" in lowered,
    }.get(expected, True)


def _is_original_disc(item: Mapping[str, Any]) -> bool:
    text = _resource_text(item).upper()
    return any(token in text for token in ("ISO", "BDISO", "BDMV")) or ("原盘" in text and "REMUX" not in text)


def _season_match_level(item: Mapping[str, Any], target_season: object) -> int:
    try:
        target = int(str(target_season).strip())
    except (TypeError, ValueError):
        return 0
    text = _resource_text(item)
    if not text:
        return 5
    upper = text.upper()
    if any(
        re.search(pattern, upper, re.IGNORECASE)
        for pattern in (
            rf"(?<![A-Z0-9])S0?{target}(?!\d)",
            rf"\bSEASONS?\s*0?{target}\b",
            rf"第\s*0?{target}\s*季",
            rf"(?<!\d)0?{target}\s*季",
        )
    ):
        return 30
    for pattern in (
        r"(?<![A-Z0-9])S0?(\d{1,2})\s*(?:-|~|–|—|至|到|TO)\s*S?0?(\d{1,2})(?!\d)",
        r"\bSEASONS?\s*0?(\d{1,2})\s*(?:-|~|–|—|TO|至|到)\s*(?:SEASONS?\s*)?0?(\d{1,2})\b",
        r"(?:第)?\s*(\d{1,2})\s*(?:-|~|–|—|至|到)\s*(\d{1,2})\s*季",
    ):
        for match in re.finditer(pattern, upper, re.IGNORECASE):
            start, end = int(match.group(1)), int(match.group(2))
            if start > end:
                start, end = end, start
            return 20 if start <= target <= end else -1
    for match in re.finditer(r"全\s*(\d{1,2})\s*季", text, re.IGNORECASE):
        return 20 if target <= int(match.group(1)) else -1
    if any(
        re.search(pattern, upper, re.IGNORECASE)
        for pattern in (
            r"全季",
            r"全集",
            r"合集",
            r"全套",
            r"COMPLETE\s+SERIES",
            r"\b(?:SERIES|SEASONS?)\s+PACK\b",
            r"\bPACK\b",
        )
    ):
        return 15
    if any(
        re.search(pattern, upper, re.IGNORECASE)
        for pattern in (
            r"(?<![A-Z0-9])S\d{1,2}(?!\d)",
            r"\bSEASONS?\s*\d{1,2}\b",
            r"第\s*\d{1,3}\s*季",
            r"(?<!\d)\d{1,2}\s*季",
        )
    ):
        return -1
    return 5


def _completion_level(item: Mapping[str, Any]) -> int:
    text = _resource_text(item).upper()
    if any(
        re.search(pattern, text, re.IGNORECASE)
        for pattern in (
            r"全集",
            r"全套",
            r"全结",
            r"全劇",
            r"全剧",
            r"完结",
            r"完結",
            r"\d{1,4}\s*集\s*(?:全|完|全结|完结|全劇|全剧|完結)",
            r"全\s*\d{1,4}\s*集",
            r"COMPLETE\s+SERIES",
            r"\bSERIES\s+COMPLETE\b",
        )
    ):
        return 30
    if any(
        re.search(pattern, text, re.IGNORECASE)
        for pattern in (
            r"S\d{1,2}\s*E\d{1,3}\s*(?:-|~|–|—|至|到|TO)\s*E?\d{1,3}",
            r"E\d{1,3}\s*(?:-|~|–|—|至|到|TO)\s*E?\d{1,3}",
            r"第\s*\d{1,3}\s*(?:-|~|–|—|至|到)\s*\d{1,3}\s*集",
        )
    ):
        return 10
    return 0


def _season_match_label(level: int) -> str:
    if level >= 30:
        return "明确命中目标季"
    if level >= 20:
        return "范围/全季覆盖目标季"
    if level >= 15:
        return "合集资源"
    if level >= 5:
        return "未标明季号，弱保留"
    return "明确错季，已排除"


def _completion_label(level: int) -> str:
    if level >= 30:
        return "完整资源"
    if level >= 10:
        return "分段资源"
    return "完整度未知"


def _integer(value: object, *, default: int, minimum: int, maximum: int) -> int:
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError):
        normalized = default
    return max(minimum, min(normalized, maximum))


def _number(value: object, *, default: float, minimum: float, maximum: float) -> float:
    try:
        normalized = float(str(value).strip())
    except (TypeError, ValueError):
        normalized = default
    return max(minimum, min(normalized, maximum))


__all__ = [
    "HDHIVE_FILTER_DEFAULTS",
    "filter_hdhive_resources",
    "normalize_filter_config",
    "normalize_hdhive_media_type",
]
