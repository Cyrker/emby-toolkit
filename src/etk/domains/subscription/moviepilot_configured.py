from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import Lock
from typing import Any

from etk.domains.system.standalone import StandaloneSettingsRepository

from .moviepilot import (
    HttpxMoviePilotGateway,
    MoviePilotListResult,
    MoviePilotQueryResult,
    MoviePilotWriteResult,
)

MOVIEPILOT_CONFIG_SETTING = "mp_config"


class ConfiguredMoviePilotGateway:
    """Lazily materializes the typed gateway from the current standalone config."""

    def __init__(
        self,
        settings: StandaloneSettingsRepository,
        *,
        timeout_seconds: float = 20,
        gateway_factory: Callable[[str, str, str, float], HttpxMoviePilotGateway] | None = None,
    ) -> None:
        self._settings = settings
        self._timeout_seconds = float(timeout_seconds)
        self._gateway_factory = gateway_factory or self._create_gateway
        self._lock = Lock()
        self._identity: tuple[str, str, str] | None = None
        self._cached_gateway: HttpxMoviePilotGateway | None = None

    def list_subscriptions(self) -> MoviePilotListResult:
        return self._gateway().list_subscriptions()

    def query_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotQueryResult:
        return self._gateway().query_subscription(tmdb_id, season=season)

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        return self._gateway().create_subscription(payload)

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        return self._gateway().update_subscription(payload)

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult:
        return self._gateway().update_subscription_status(
            tmdb_id,
            status,
            season=season,
            total_episodes=total_episodes,
        )

    def delete_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotWriteResult:
        return self._gateway().delete_subscription(tmdb_id, season=season)

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult:
        return self._gateway().delete_subscription_by_id(subscription_id)

    def _gateway(self) -> HttpxMoviePilotGateway:
        value = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
        config = dict(value) if isinstance(value, Mapping) else {}
        identity = (
            str(config.get("moviepilot_url") or "").strip().rstrip("/"),
            str(config.get("moviepilot_username") or ""),
            str(config.get("moviepilot_password") or ""),
        )
        if not all(identity):
            raise ValueError("MoviePilot is not fully configured")
        with self._lock:
            if self._cached_gateway is None or self._identity != identity:
                self._cached_gateway = self._gateway_factory(*identity, self._timeout_seconds)
                self._identity = identity
            return self._cached_gateway

    @staticmethod
    def _create_gateway(
        base_url: str,
        username: str,
        password: str,
        timeout_seconds: float,
    ) -> HttpxMoviePilotGateway:
        return HttpxMoviePilotGateway(
            base_url,
            username,
            password,
            timeout_seconds=timeout_seconds,
        )


__all__ = ["ConfiguredMoviePilotGateway", "MOVIEPILOT_CONFIG_SETTING"]
