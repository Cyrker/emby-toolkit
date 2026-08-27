from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx

from etk.platform import Settings


class HttpxDiscoverProviderAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def discover_movie(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        return self._mapping("/discover/movie", payload)

    def discover_tv(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        return self._mapping("/discover/tv", payload)

    def person_tv_credits(self, person_id: int) -> Mapping[str, Any] | None:
        return self._mapping(f"/person/{int(person_id)}/tv_credits", {})

    def genres(self, media_type: str) -> Sequence[Mapping[str, Any]] | None:
        payload = self._mapping(f"/genre/{media_type}/list", {})
        if payload is None or not isinstance(payload.get("genres"), list):
            return None
        return tuple(dict(item) for item in payload["genres"] if isinstance(item, Mapping))

    def search(
        self,
        *,
        query: str,
        media_type: str,
        page: int,
    ) -> Mapping[str, Any] | None:
        params = {
            "query": query,
            "include_adult": "true",
            "page": page,
            "language": "zh-CN",
        }
        payload = self._mapping(f"/search/{media_type}", params)
        if payload is not None and not payload.get("results"):
            payload = self._mapping(
                f"/search/{media_type}",
                {**params, "language": "en-US"},
            )
        return payload

    def tv_details(self, tv_id: int) -> Mapping[str, Any] | None:
        return self._mapping(f"/tv/{int(tv_id)}", {})

    def _mapping(self, path: str, params: Mapping[str, Any]) -> dict[str, Any] | None:
        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        if not api_key or not base_url:
            return None
        query = {"api_key": api_key, "language": "zh-CN", **dict(params)}
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    params=query,
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    return None
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        return dict(payload) if isinstance(payload, Mapping) else None

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


__all__ = ["HttpxDiscoverProviderAdapter"]
