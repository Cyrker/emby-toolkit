from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

from etk.domains.operations.jobs import JobSubmissionService
from etk.domains.system.standalone import StandaloneSettingsRepository
from etk.platform import Settings
from etk.policy_defaults import DEFAULT_LANGUAGE_MAPPING, DEFAULT_RELEASE_GROUP_MAPPING

from .actor import ActorRefreshIntent
from .moviepilot import MoviePilotListResult, MoviePilotWriteResult
from .moviepilot_configured import MOVIEPILOT_CONFIG_SETTING
from .resubscribe import ResubscribePolicySnapshot, ResubscribeTaskIntent
from .watchlist import WatchlistDeleteContext

logger = logging.getLogger(__name__)


class MoviePilotWatchlistGateway(Protocol):
    def list_subscriptions(self) -> MoviePilotListResult: ...

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult: ...


class EmbyItemDeletionGateway(Protocol):
    def delete_item(self, item_id: str) -> bool: ...


_ASSISTANT_DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "guard_mode": "balanced",
    "season_cooldown_days": 14,
    "volatility_enabled": True,
    "volatility_window_days": 14,
    "pending_enhanced_enabled": True,
    "pending_use_volatility": False,
    "pending_fake_total_episodes": 99,
    "pause_enhanced_enabled": True,
    "tv_air_pause_days": 14,
    "airing_pause_days": 30,
    "tv_no_download_days": 0,
    "no_download_actions": [],
    "download_monitor_enabled": False,
    "manual_delete_listen": True,
    "tracker_response_listen": True,
    "download_timeout_minutes": 120,
    "download_progress_threshold": 10,
    "download_retry_limit": 3,
    "auto_search_when_delete": True,
    "skip_deletion": True,
    "delete_record_retention_hours": 24,
    "delete_exclude_tags": ["H&R"],
    "tracker_keywords": [
        "torrent not registered with this tracker",
        "torrent banned",
    ],
    "best_version_type": "tv_episode",
    "best_version_episode_to_full": True,
    "best_version_full_consistency_check_enabled": True,
    "full_washing_timeout_hours": 72,
    "subscription_cleanup_history_type": "none",
    "subscription_cleanup_history_scenes": ["completed"],
    "verify_enabled": True,
    "verify_interval_hours": 12,
    "snapshot_retention_days": 180,
}


class StaticAssistantDefaults:
    @staticmethod
    def defaults() -> Mapping[str, Any]:
        return deepcopy(_ASSISTANT_DEFAULTS)


class ConfiguredSubscriptionAvailabilityGateway:
    def __init__(
        self,
        settings: StandaloneSettingsRepository,
        *,
        hdhive_probe: Callable[[], bool] | None = None,
        shared_pool_probe: Callable[[], bool] | None = None,
    ) -> None:
        self._settings = settings
        self._hdhive_probe = hdhive_probe or self._default_hdhive_probe
        self._shared_pool_probe = shared_pool_probe or self._default_shared_pool_probe

    def available_sources(self) -> Sequence[str]:
        status = self.status()
        return ("mp",) if status["mp_configured"] else ()

    def status(self) -> Mapping[str, bool]:
        mp_config = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
        mp_configured = isinstance(mp_config, Mapping) and bool(mp_config.get("moviepilot_url"))
        return {
            "mp_configured": mp_configured,
            "hdhive_configured": False,
            "tg_userbot_configured": False,
            "shared_pool_configured": False,
            "cloud_search_configured": False,
        }

    @staticmethod
    def _default_hdhive_probe() -> bool:
        from handler.hdhive_client import HDHiveClient

        return bool(HDHiveClient().ping())

    @staticmethod
    def _default_shared_pool_probe() -> bool:
        from handler.shared_center_client import shared_center_enabled

        return bool(shared_center_enabled())


class NativeActorSearchGateway:
    """Lazy TMDb adapter; construction never imports or connects to the legacy runtime."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings

    def configured(self) -> bool:
        return bool(self._settings().tmdb_api_key.get_secret_value().strip())

    def search(self, query: str) -> Sequence[Mapping[str, Any]] | None:
        from handler.tmdb import search_person_tmdb

        api_key = self._settings().tmdb_api_key.get_secret_value().strip()
        if not api_key:
            return None
        result = search_person_tmdb(query, api_key)
        if result is None:
            return None
        return tuple(dict(person) for person in result if isinstance(person, Mapping))


class NativeActorRuntimeGateway:
    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings

    @staticmethod
    def processor_ready() -> bool:
        return True

    def detail_context(self) -> Mapping[str, Any]:
        settings = self._settings()
        try:
            import extensions

            server_id = extensions.EMBY_SERVER_ID
        except (AttributeError, ImportError):
            server_id = None
        return {
            "emby_server_url": settings.emby_server_url.rstrip("/"),
            "emby_api_key_for_url": settings.emby_api_key.get_secret_value(),
            "emby_server_id": server_id,
        }


class PersistentActorRefreshGateway:
    def __init__(self, jobs: Callable[[], JobSubmissionService]) -> None:
        self._jobs = jobs

    def submit(self, intent: ActorRefreshIntent) -> bool:
        self._jobs().submit_task(
            "actor-tracking",
            {
                "subscription_id": intent.subscription_id,
                "actor_name": intent.actor_name,
            },
            trigger_type="manual",
            max_attempts=2,
        )
        return True


class PersistentWatchlistRefreshGateway:
    def __init__(self, jobs: Callable[[], JobSubmissionService]) -> None:
        self._jobs = jobs

    @staticmethod
    def ready() -> bool:
        return True

    def submit(self, *, tmdb_id: str, item_name: str) -> Any:
        return self._jobs().submit_task(
            "process-watchlist",
            {"tmdb_id": tmdb_id, "item_name": item_name},
            trigger_type="manual",
            max_attempts=2,
        )


class ConfiguredResubscribePolicy:
    def __init__(self, settings: StandaloneSettingsRepository) -> None:
        self._settings = settings

    def snapshot(self) -> ResubscribePolicySnapshot:
        mp_value = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
        mp_config = dict(mp_value) if isinstance(mp_value, Mapping) else {}
        raw_cap = mp_config.get("resubscribe_daily_cap", 10)
        daily_cap = int(raw_cap) if not isinstance(raw_cap, bool) else 10
        daily_cap = max(daily_cap, 0)
        try:
            delay_seconds = max(float(mp_config.get("resubscribe_delay_seconds", 2.0)), 0.0)
        except (TypeError, ValueError):
            delay_seconds = 2.0

        language_mapping = self._settings.load("language_mapping")
        release_mapping = self._settings.load("release_group_mapping")
        if not isinstance(language_mapping, list) or not language_mapping:
            language_mapping = DEFAULT_LANGUAGE_MAPPING
        if not isinstance(release_mapping, Mapping) or not release_mapping:
            release_mapping = DEFAULT_RELEASE_GROUP_MAPPING
        return ResubscribePolicySnapshot(
            quota_date=datetime.now(ZoneInfo("Asia/Shanghai")).date(),
            daily_cap=daily_cap,
            delay_seconds=delay_seconds,
            language_keywords=_language_keywords(language_mapping),
            release_group_mapping=_release_group_mapping(release_mapping),
        )


class PersistentResubscribeTaskGateway:
    def __init__(self, jobs: Callable[[], JobSubmissionService]) -> None:
        self._jobs = jobs

    def busy(self) -> bool:
        return self._jobs().is_busy()

    def submit(self, intent: ResubscribeTaskIntent) -> bool:
        self._jobs().submit_task(
            intent.task_key,
            {},
            trigger_type="manual",
            max_attempts=2,
            reject_when_busy=True,
        )
        return True


class ConfiguredEmbyLibraryGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def ready(self) -> bool:
        settings = self._settings()
        return bool(
            settings.emby_server_url.strip()
            and settings.emby_api_key.get_secret_value().strip()
            and settings.emby_user_id.strip()
        )

    def list_libraries(self) -> Sequence[Mapping[str, Any]] | None:
        if not self.ready():
            return None
        settings = self._settings()
        base_url = settings.emby_server_url.rstrip("/")
        with self._client_factory() as client:
            response = client.get(
                f"{base_url}/Users/{quote(settings.emby_user_id, safe='')}/Views",
                headers={"X-Emby-Token": settings.emby_api_key.get_secret_value()},
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
            return None
        return tuple(dict(item) for item in payload["Items"] if isinstance(item, Mapping))

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


class HttpxEmbyItemDeletionGateway:
    """One-shot configured-token adapter for Emby's official item-delete contract."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def delete_item(self, item_id: str) -> bool:
        settings = self._settings()
        normalized_id = str(item_id or "").strip()
        base_url = settings.emby_server_url.strip().rstrip("/")
        api_key = settings.emby_api_key.get_secret_value().strip()
        user_id = settings.emby_user_id.strip()
        if not all((normalized_id, base_url, api_key, user_id)):
            return False
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(normalized_id, safe='')}/Delete",
                    headers={"X-Emby-Token": api_key},
                    params={"UserId": user_id},
                )
        except (httpx.TimeoutException, httpx.TransportError):
            logger.exception("Emby item deletion transport failed for item %s", normalized_id)
            return False
        return response.status_code in {200, 204, 404}

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


class NativeWatchlistExternalDeleteGateway:
    def __init__(
        self,
        settings: StandaloneSettingsRepository,
        moviepilot: MoviePilotWatchlistGateway,
        emby: EmbyItemDeletionGateway,
    ) -> None:
        self._settings = settings
        self._moviepilot = moviepilot
        self._emby = emby

    def delete(self, context: WatchlistDeleteContext) -> Mapping[str, Any]:
        item = context.to_dict()
        mp_config = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
        mp_enabled = isinstance(mp_config, Mapping) and bool(mp_config.get("moviepilot_url"))
        mp_deleted = 0
        if mp_enabled:
            try:
                subscriptions = self._moviepilot.list_subscriptions().subscriptions
                for subscription in subscriptions:
                    if str(subscription.get("tmdbid")) != context.tmdb_id:
                        continue
                    subscription_id = subscription.get("id")
                    if not subscription_id:
                        continue
                    result = self._moviepilot.delete_subscription_by_id(subscription_id)
                    if not result.confirmed:
                        raise RuntimeError("MoviePilot deletion was not confirmed")
                    mp_deleted += 1
            except Exception as exc:
                logger.warning("MoviePilot watchlist cleanup failed", exc_info=True)
                return {
                    "success": False,
                    "status": 500,
                    "error": "MoviePilot 取消订阅失败",
                    "error_class": type(exc).__name__,
                    "item": item,
                }

        failed_emby_ids = [item_id for item_id in context.emby_ids if not self._emby.delete_item(item_id)]
        if failed_emby_ids:
            return {
                "success": False,
                "status": 500,
                "error": "Emby 媒体项删除失败",
                "failed_emby_ids": failed_emby_ids,
                "item": item,
            }
        return {
            "success": True,
            "item": item,
            "mp_cancelled": mp_enabled,
            "mp_deleted": mp_deleted,
            "emby_deleted": len(context.emby_ids),
        }


__all__ = [
    "ConfiguredEmbyLibraryGateway",
    "ConfiguredSubscriptionAvailabilityGateway",
    "EmbyItemDeletionGateway",
    "ConfiguredResubscribePolicy",
    "HttpxEmbyItemDeletionGateway",
    "StaticAssistantDefaults",
    "MoviePilotWatchlistGateway",
    "NativeActorRuntimeGateway",
    "NativeActorSearchGateway",
    "NativeWatchlistExternalDeleteGateway",
    "PersistentActorRefreshGateway",
    "PersistentResubscribeTaskGateway",
    "PersistentWatchlistRefreshGateway",
]


def _language_keywords(value: Sequence[Any]) -> dict[str, tuple[str, ...]]:
    by_label: dict[str, tuple[str, ...]] = {}
    normalized_items: list[Mapping[str, Any]] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            continue
        aliases = raw.get("aliases", ())
        if isinstance(aliases, str):
            aliases = aliases.split(",")
        if not isinstance(aliases, Sequence) or isinstance(aliases, (str, bytes, bytearray)):
            aliases = ()
        normalized = tuple(str(alias).strip() for alias in aliases if str(alias).strip())
        label = str(raw.get("label") or "").strip()
        if label:
            by_label[label] = normalized
        normalized_items.append(raw)

    keywords: dict[str, tuple[str, ...]] = {
        "chi": (*by_label.get("国语", ()), "国配", "公映", "台配", "京译", "上译", "央译"),
        "yue": (*by_label.get("粤语", ()), "粤配", "港配", "粤英双语"),
        "eng": by_label.get("英语", ()),
        "jpn": by_label.get("日语", ()),
        "kor": by_label.get("韩语", ()),
        "sub_chi": (
            *by_label.get("国语", ()),
            "CHS",
            "SC",
            "GB",
            "ZHS",
            "ZH-CN",
            "ZH-HANS",
            "简体",
            "简中",
            "Simplified",
        ),
        "sub_yue": (
            *by_label.get("粤语", ()),
            "CHT",
            "TC",
            "BIG5",
            "ZHT",
            "ZH-TW",
            "ZH-HK",
            "ZH-HANT",
            "繁体",
            "繁中",
            "Traditional",
        ),
        "sub_eng": (*by_label.get("英语", ()), "英字"),
        "sub_jpn": (*by_label.get("日语", ()), "日字"),
        "sub_kor": (*by_label.get("韩语", ()), "韩字"),
    }
    primary_labels = {"国语", "粤语", "英语", "日语", "韩语", "无语言"}
    for item in normalized_items:
        label = str(item.get("label") or "").strip()
        if not label or label in primary_labels:
            continue
        aliases = by_label.get(label, ())
        code = next(
            (alias.casefold() for alias in aliases if len(alias) == 3 and alias.isalpha()),
            str(item.get("value") or "").strip().casefold(),
        )
        if code and aliases:
            keywords[code] = aliases
            keywords[f"sub_{code}"] = aliases
    return keywords


def _release_group_mapping(value: Mapping[Any, Any]) -> dict[str, tuple[str, ...]]:
    normalized: dict[str, tuple[str, ...]] = {}
    for raw_name, raw_aliases in value.items():
        name = str(raw_name or "").strip()
        if not name:
            continue
        aliases = raw_aliases
        if isinstance(aliases, Mapping):
            aliases = aliases.get("aliases") or aliases.get("en") or aliases.get("ids") or ()
        if isinstance(aliases, str):
            aliases = (aliases,)
        if not isinstance(aliases, Sequence) or isinstance(aliases, (str, bytes, bytearray)):
            aliases = ()
        normalized[name] = tuple(str(alias).strip() for alias in aliases if str(alias).strip())
    return normalized
