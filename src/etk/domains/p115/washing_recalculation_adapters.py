from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import httpx

from etk.platform import Settings


class HttpxP115EpisodeRuntimeGateway:
    """Narrow read-only TMDb adapter used only by clean-version evaluation."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def episode_runtime_minutes(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
    ) -> float | None:
        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        if not api_key or not base_url:
            return None
        try:
            series_id = int(float(series_tmdb_id))
        except (TypeError, ValueError):
            return None
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/tv/{series_id}/season/{int(season_number)}",
                    params={"api_key": api_key, "language": "zh-CN"},
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        if not isinstance(payload, Mapping):
            return None
        episodes = payload.get("episodes")
        if not isinstance(episodes, list):
            return None
        for episode in episodes:
            if not isinstance(episode, Mapping):
                continue
            try:
                number = int(episode.get("episode_number") or 0)
                runtime = float(episode.get("runtime") or 0)
            except (TypeError, ValueError):
                continue
            if number == int(episode_number) and runtime > 0:
                return runtime
        return None

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


__all__ = ["HttpxP115EpisodeRuntimeGateway"]
