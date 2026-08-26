from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

from etk.domains.system.standalone import StandaloneSettingsRepository

WATCHLIST_USE_CASE_PROVIDER = "subscription.watchlist"
WATCHLIST_MEDIA_PROJECTION_PROVIDER = "media.watchlist-projection"
WATCHLIST_EXTERNAL_DELETE_PROVIDER = "subscription.watchlist-external-delete"
WATCHLIST_REFRESH_PROVIDER = "subscription.watchlist-refresh"

WATCHLIST_CONFIG_SETTING = "watchlist_config"
SHARED_RESOURCE_CONFIG_SETTING = "shared_resource_config"

WATCHLIST_STATUSES = frozenset({"Watching", "Ended", "Paused", "Pending", "Completed"})
BATCH_WATCHLIST_STATUSES = frozenset({"Watching", "Paused", "Completed"})
_INTRO_DETECTION_TRIGGER_MODES = frozenset({"off", "import", "favorite", "playback"})
_DEFAULT_CONFIG: dict[str, Any] = {
    "douban_count_correction": False,
    "revival_check_days": 365,
    "tg_channel_tracking": False,
    "intro_detection_trigger_mode": "off",
    "intro_detection_credits_enabled": False,
    "intro_detection_library_ids": [],
}


@dataclass(frozen=True, slots=True)
class WatchlistDeleteContext:
    tmdb_id: str
    title: str
    emby_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "tmdb_id": self.tmdb_id,
            "title": self.title,
            "emby_ids": list(self.emby_ids),
        }


@dataclass(frozen=True, slots=True)
class WatchlistRemoval:
    removed: bool
    cleanup: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class WatchlistBatchRemoval:
    removed_count: int
    cleanups: tuple[Mapping[str, Any], ...]
    failures: tuple[Mapping[str, Any], ...]

    @property
    def status_code(self) -> int:
        if self.failures and self.cleanups:
            return 207
        if self.failures:
            return 500
        return 200


class WatchlistRefreshUnavailable(RuntimeError):
    pass


class WatchlistMediaProjectionRepository(Protocol):
    """Narrow P7 port over the P5-owned media projection."""

    def list_watchlist_items(self) -> Sequence[Mapping[str, Any]]: ...

    def add_watchlist_item(self, tmdb_id: str, item_name: str) -> None: ...

    def update_watchlist_status(self, tmdb_id: str, new_status: str) -> bool: ...

    def watchlist_delete_context(self, tmdb_id: str) -> Mapping[str, Any] | None: ...

    def remove_watchlist_item(self, tmdb_id: str) -> bool: ...

    def watchlist_item_name(self, tmdb_id: str) -> str | None: ...

    def force_end_watchlist_seasons(self, tmdb_ids: Sequence[Any]) -> int: ...

    def batch_update_watchlist_status(self, item_ids: Sequence[Any], new_status: str) -> int: ...

    def batch_remove_watchlist_items(self, tmdb_ids: Sequence[Any]) -> int: ...

    def update_watchlist_total_episodes(
        self,
        tmdb_id: Any,
        total_episodes: Any,
        item_type: Any,
    ) -> None: ...


class WatchlistExternalDeletePort(Protocol):
    def delete(self, context: WatchlistDeleteContext) -> Mapping[str, Any]: ...


class WatchlistRefreshPort(Protocol):
    def ready(self) -> bool: ...

    def submit(self, *, tmdb_id: str, item_name: str) -> Any: ...


class WatchlistService:
    """Native watchlist HTTP use case over bounded P5, system, P4, and external ports.

    Repository calls complete before external deletion or refresh submission
    starts; no database transaction is held across either boundary.
    """

    def __init__(
        self,
        repository: WatchlistMediaProjectionRepository,
        settings: StandaloneSettingsRepository,
        external_delete: WatchlistExternalDeletePort,
        refresh: WatchlistRefreshPort,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._external_delete = external_delete
        self._refresh = refresh

    def list_items(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for raw_item in self._repository.list_watchlist_items():
            item = dict(raw_item)
            item["next_episode_to_air"] = item.get("next_episode_to_air_json")
            item.pop("next_episode_to_air_json", None)
            item["missing_info"] = item.get("missing_info_json")
            item.pop("missing_info_json", None)
            for key, value in tuple(item.items()):
                if isinstance(value, (datetime, date)):
                    item[key] = value.isoformat()
            items.append(item)
        return items

    def add(self, tmdb_id: Any, item_name: Any) -> None:
        self._repository.add_watchlist_item(str(tmdb_id), str(item_name))

    def update_status(self, tmdb_id: Any, new_status: str) -> bool:
        return self._repository.update_watchlist_status(str(tmdb_id), new_status)

    def remove(self, tmdb_id: Any) -> WatchlistRemoval:
        normalized_id = str(tmdb_id)
        cleanup = self._cleanup(normalized_id)
        if not cleanup.get("success"):
            return WatchlistRemoval(False, cleanup)
        return WatchlistRemoval(
            self._repository.remove_watchlist_item(normalized_id),
            cleanup,
        )

    def refresh(self, tmdb_id: Any) -> str:
        if not self._refresh.ready():
            raise WatchlistRefreshUnavailable("watchlist processor is not ready")
        normalized_id = str(tmdb_id)
        item_name = self._repository.watchlist_item_name(normalized_id) or "未知剧集"
        self._refresh.submit(tmdb_id=normalized_id, item_name=item_name)
        return item_name

    def force_end(self, tmdb_ids: Sequence[Any]) -> int:
        return self._repository.force_end_watchlist_seasons(tuple(tmdb_ids))

    def batch_update_status(self, item_ids: Sequence[Any], new_status: str) -> int:
        return self._repository.batch_update_watchlist_status(tuple(item_ids), new_status)

    def batch_remove(self, item_ids: Sequence[Any]) -> WatchlistBatchRemoval:
        cleaned_ids: list[Any] = []
        cleanups: list[Mapping[str, Any]] = []
        failures: list[Mapping[str, Any]] = []
        for item_id in item_ids:
            cleanup = self._cleanup(str(item_id))
            if cleanup.get("success"):
                cleaned_ids.append(item_id)
                cleanups.append(cleanup)
            else:
                failures.append(
                    {
                        "tmdb_id": item_id,
                        "error": cleanup.get("error") or "删除外部资源失败",
                        "details": cleanup,
                    }
                )
        removed_count = self._repository.batch_remove_watchlist_items(tuple(cleaned_ids)) if cleaned_ids else 0
        return WatchlistBatchRemoval(
            removed_count,
            tuple(cleanups),
            tuple(failures),
        )

    def update_total_episodes(self, tmdb_id: Any, total_episodes: Any, item_type: Any) -> None:
        self._repository.update_watchlist_total_episodes(tmdb_id, total_episodes, item_type)

    def get_settings(self) -> dict[str, Any]:
        current = self._stored_config()
        if "intro_detection_trigger_mode" not in current and self._legacy_intro_enabled():
            current["intro_detection_trigger_mode"] = "import"
            self._settings.save(WATCHLIST_CONFIG_SETTING, deepcopy(current))
        final = {
            **deepcopy(_DEFAULT_CONFIG),
            **{key: deepcopy(value) for key, value in current.items() if key in _DEFAULT_CONFIG},
        }
        final["intro_detection_trigger_mode"] = _normalize_intro_trigger_mode(final.get("intro_detection_trigger_mode"))
        final["intro_detection_library_ids"] = _normalize_library_ids(final.get("intro_detection_library_ids"))
        return final

    def save_settings(self, new_config: Mapping[str, Any]) -> None:
        current = self._stored_config()
        if (
            "intro_detection_trigger_mode" not in current
            and "intro_detection_trigger_mode" not in new_config
            and self._legacy_intro_enabled()
        ):
            current["intro_detection_trigger_mode"] = "import"

        updated = deepcopy(current)
        for key, default in _DEFAULT_CONFIG.items():
            if key in new_config:
                updated[key] = deepcopy(new_config[key])
            elif key not in updated:
                updated[key] = deepcopy(default)
        updated["intro_detection_trigger_mode"] = _normalize_intro_trigger_mode(
            updated.get("intro_detection_trigger_mode")
        )
        updated["intro_detection_library_ids"] = _normalize_library_ids(updated.get("intro_detection_library_ids"))
        self._settings.save(WATCHLIST_CONFIG_SETTING, deepcopy(updated))

    def _cleanup(self, tmdb_id: str) -> dict[str, Any]:
        raw_context = self._repository.watchlist_delete_context(tmdb_id)
        if raw_context is None:
            return {
                "success": False,
                "status": 404,
                "error": "未在追剧列表中找到该项目",
            }
        context = WatchlistDeleteContext(
            tmdb_id=str(raw_context.get("tmdb_id") or tmdb_id),
            title=str(raw_context.get("title") or tmdb_id),
            emby_ids=tuple(_normalize_emby_ids(raw_context.get("emby_ids"))),
        )
        cleanup = dict(self._external_delete.delete(context))
        cleanup.setdefault("success", False)
        cleanup.setdefault("item", context.to_dict())
        return cleanup

    def _stored_config(self) -> dict[str, Any]:
        value = self._settings.load(WATCHLIST_CONFIG_SETTING)
        return deepcopy(dict(value)) if isinstance(value, Mapping) else {}

    def _legacy_intro_enabled(self) -> bool:
        legacy = self._settings.load(SHARED_RESOURCE_CONFIG_SETTING)
        if not isinstance(legacy, Mapping):
            return False
        return _legacy_bool(legacy.get("p115_etk_intro_detection_enabled"))


def _normalize_intro_trigger_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    return mode if mode in _INTRO_DETECTION_TRIGGER_MODES else "off"


def _normalize_library_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        library_id = str(item or "").strip()
        if library_id and library_id not in seen:
            seen.add(library_id)
            result.append(library_id)
    return result


def _normalize_emby_ids(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    normalized = str(value).strip()
    return [normalized] if normalized else []


def _legacy_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


__all__ = [
    "BATCH_WATCHLIST_STATUSES",
    "SHARED_RESOURCE_CONFIG_SETTING",
    "WATCHLIST_CONFIG_SETTING",
    "WATCHLIST_EXTERNAL_DELETE_PROVIDER",
    "WATCHLIST_MEDIA_PROJECTION_PROVIDER",
    "WATCHLIST_REFRESH_PROVIDER",
    "WATCHLIST_STATUSES",
    "WATCHLIST_USE_CASE_PROVIDER",
    "WatchlistBatchRemoval",
    "WatchlistDeleteContext",
    "WatchlistExternalDeletePort",
    "WatchlistMediaProjectionRepository",
    "WatchlistRefreshPort",
    "WatchlistRefreshUnavailable",
    "WatchlistRemoval",
    "WatchlistService",
]
