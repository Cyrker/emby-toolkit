from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from urllib.parse import quote

import httpx

from etk.domains.operations.jobs import JobSubmissionService
from etk.domains.system.standalone import StandaloneSettingsRepository
from etk.platform import Settings

from .actor import ActorRefreshIntent
from .moviepilot import MoviePilotListResult, MoviePilotWriteResult
from .moviepilot_configured import MOVIEPILOT_CONFIG_SETTING
from .watchlist import WatchlistDeleteContext

logger = logging.getLogger(__name__)


class MoviePilotWatchlistGateway(Protocol):
    def list_subscriptions(self) -> MoviePilotListResult: ...

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult: ...


class EmbyItemDeletionGateway(Protocol):
    def delete_item(self, item_id: str) -> bool: ...


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
    "EmbyItemDeletionGateway",
    "HttpxEmbyItemDeletionGateway",
    "MoviePilotWatchlistGateway",
    "NativeActorRuntimeGateway",
    "NativeActorSearchGateway",
    "NativeWatchlistExternalDeleteGateway",
    "PersistentActorRefreshGateway",
    "PersistentWatchlistRefreshGateway",
]
