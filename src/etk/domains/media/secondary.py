from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SecondaryMediaHttpResult:
    payload: Any
    status_code: int = 200


class SecondaryMediaProjection(Protocol):
    def search_library(self, query: str) -> dict[str, Any]: ...

    def search_studios(self, query: str, *, limit: int = 20) -> list[str]: ...


class SecondaryMediaSettings(Protocol):
    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...


class SecondaryMediaService:
    """P8-local media search and auto-tagging configuration boundary."""

    def __init__(
        self,
        *,
        media: SecondaryMediaProjection,
        settings: SecondaryMediaSettings,
    ) -> None:
        self._media = media
        self._settings = settings

    def search_library(self, query: str) -> SecondaryMediaHttpResult:
        normalized = str(query or "").strip()
        if not normalized:
            return SecondaryMediaHttpResult({"error": "搜索词不能为空"}, 400)
        try:
            return SecondaryMediaHttpResult(self._media.search_library(normalized))
        except Exception:
            return SecondaryMediaHttpResult({"error": "搜索时发生未知服务器错误"}, 500)

    def search_studios(self, query: str) -> SecondaryMediaHttpResult:
        normalized = str(query or "").strip()
        if not normalized:
            return SecondaryMediaHttpResult([])
        try:
            return SecondaryMediaHttpResult(self._media.search_studios(normalized, limit=20))
        except Exception:
            return SecondaryMediaHttpResult({"error": "服务器内部错误"}, 500)

    def auto_tagging_rules(self) -> SecondaryMediaHttpResult:
        try:
            rules = self._settings.load("auto_tagging_rules")
            return SecondaryMediaHttpResult(rules or [])
        except Exception:
            return SecondaryMediaHttpResult({"error": "服务器内部错误"}, 500)

    def save_auto_tagging_rules(self, payload: Any) -> SecondaryMediaHttpResult:
        try:
            self._settings.save("auto_tagging_rules", payload)
            return SecondaryMediaHttpResult({"message": "配置已保存"})
        except Exception:
            return SecondaryMediaHttpResult({"error": "服务器内部错误"}, 500)


def normalized_string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("expected a list")
    result: list[str] = []
    for item in value:
        normalized = str(item or "").strip()
        if normalized:
            result.append(normalized)
    return result


def auto_tagging_job_parameters(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("expected an object")
    tags = normalized_string_list(payload.get("tags"))
    if not tags:
        raise ValueError("tags are required")
    return {
        "library_ids": normalized_string_list(payload.get("library_ids")),
        "tags": tags,
        "rating_filters": normalized_string_list(payload.get("rating_filters")),
    }


__all__ = [
    "SecondaryMediaHttpResult",
    "SecondaryMediaProjection",
    "SecondaryMediaService",
    "SecondaryMediaSettings",
    "auto_tagging_job_parameters",
]
