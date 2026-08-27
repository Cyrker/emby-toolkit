from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import httpx

from etk.platform import Settings


class HttpxPortalTmdbAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def movie(self, tmdb_id: int) -> Mapping[str, Any]:
        return self._get(f"/movie/{int(tmdb_id)}")

    def tv(self, tmdb_id: int) -> Mapping[str, Any]:
        return self._get(f"/tv/{int(tmdb_id)}")

    def _get(self, path: str) -> Mapping[str, Any]:
        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        if not api_key or not base_url:
            raise RuntimeError("TMDb is not configured")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    params={"api_key": api_key, "language": "zh-CN"},
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    raise RuntimeError("TMDb media query was rejected")
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("TMDb media query is unavailable") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb media query returned an invalid payload")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


__all__ = ["HttpxPortalTmdbAdapter"]
