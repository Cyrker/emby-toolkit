from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class DiscoverHttpResult:
    payload: Any
    status_code: int = 200


class DiscoverSettings(Protocol):
    def load_many(self, keys: Sequence[str]) -> Mapping[str, Any]: ...


class DiscoverDefaults(Protocol):
    def language_mapping(self) -> Sequence[Mapping[str, Any]]: ...

    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]: ...

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]: ...


class DiscoverService:
    _SNAPSHOT_KEYS = (
        "language_mapping",
        "keyword_mapping",
        "studio_mapping",
        "recommendation_pool",
        "recommendation_pool_page",
        "recommendation_theme_index",
    )

    def __init__(self, *, settings: DiscoverSettings, defaults: DiscoverDefaults) -> None:
        self._settings = settings
        self._defaults = defaults

    def languages(self) -> DiscoverHttpResult:
        try:
            raw = self._snapshot().get("language_mapping")
            mapping = raw if isinstance(raw, list) and raw else self._defaults.language_mapping()
            return DiscoverHttpResult(
                [
                    {"label": item["label"], "value": item["value"]}
                    for item in mapping
                    if isinstance(item, Mapping) and item.get("label") and item.get("value")
                ]
            )
        except Exception:
            return DiscoverHttpResult([], 500)

    def keywords(self) -> DiscoverHttpResult:
        try:
            raw = self._snapshot().get("keyword_mapping") or self._defaults.keyword_mapping()
            if isinstance(raw, list):
                options = [
                    {"label": item["label"], "value": item["label"]}
                    for item in raw
                    if isinstance(item, Mapping) and item.get("label")
                ]
            elif isinstance(raw, Mapping):
                options = sorted(
                    ({"label": str(label), "value": str(label)} for label in raw),
                    key=lambda item: item["label"],
                )
            else:
                options = []
            return DiscoverHttpResult(options)
        except Exception:
            return DiscoverHttpResult([], 500)

    def studios(self) -> DiscoverHttpResult:
        try:
            raw = self._snapshot().get("studio_mapping") or self._defaults.studio_mapping()
            if isinstance(raw, Mapping):
                mapping = [
                    {"label": str(label), **dict(value)} for label, value in raw.items() if isinstance(value, Mapping)
                ]
                mapping.sort(key=lambda item: item["label"])
            elif isinstance(raw, list | tuple):
                mapping = [dict(item) for item in raw if isinstance(item, Mapping)]
            else:
                mapping = []
            options: list[dict[str, Any]] = []
            for item in mapping:
                label = str(item.get("label") or "").strip()
                if not label:
                    continue
                supported: list[str] = []
                if item.get("company_ids"):
                    supported.append("movie")
                if item.get("network_ids"):
                    supported.append("tv")
                if not supported and item.get("ids"):
                    supported = ["movie", "tv"]
                options.append({"label": label, "value": label, "types": supported})
            return DiscoverHttpResult(options)
        except Exception:
            return DiscoverHttpResult([], 500)

    def daily_recommendation(self) -> DiscoverHttpResult:
        try:
            snapshot = self._snapshot()
            pool = snapshot.get("recommendation_pool")
            if not pool:
                return DiscoverHttpResult({"error": "推荐池尚未生成或为空。"}, 404)
            mapping = snapshot.get("keyword_mapping") or self._defaults.keyword_mapping()
            if isinstance(mapping, Mapping):
                themes = [
                    {"label": str(label), **dict(value)}
                    for label, value in mapping.items()
                    if isinstance(value, Mapping) and value.get("ids")
                ]
            elif isinstance(mapping, list | tuple):
                themes = [
                    dict(item)
                    for item in mapping
                    if isinstance(item, Mapping) and item.get("label") and item.get("ids")
                ]
            else:
                themes = []
            theme_name = "今日精选"
            theme_index = snapshot.get("recommendation_theme_index")
            if theme_index is not None and themes:
                theme_name = str(themes[int(theme_index) % len(themes)]["label"])
            return DiscoverHttpResult({"theme_name": theme_name, "pool": pool})
        except Exception:
            return DiscoverHttpResult({"error": "获取推荐池失败"}, 500)

    def _snapshot(self) -> Mapping[str, Any]:
        return self._settings.load_many(self._SNAPSHOT_KEYS)


__all__ = [
    "DiscoverDefaults",
    "DiscoverHttpResult",
    "DiscoverService",
    "DiscoverSettings",
]
