from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings

from .service import (
    CollectionGatewayConfigurationError,
    CollectionProviderNotFoundError,
    CollectionProviderUnavailableError,
)


class HttpxCollectionQueryAdapter:
    """Narrow read-only adapter for the P8 collection HTTP surface."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def collection_details(self, tmdb_collection_id: int) -> Mapping[str, Any] | None:
        return self._tmdb_mapping(
            f"/collection/{int(tmdb_collection_id)}",
            {},
            not_found_identity=str(tmdb_collection_id),
        )

    def search_collections(self, query: str) -> Sequence[Mapping[str, Any]]:
        payload = self._tmdb_mapping("/search/collection", {"query": query})
        return _mapping_items(payload, "results")

    def movie_details(self, tmdb_id: int) -> Mapping[str, Any]:
        return self._tmdb_mapping(
            f"/movie/{int(tmdb_id)}",
            {},
            not_found_identity=str(tmdb_id),
        )

    def series_details(self, tmdb_id: int) -> Mapping[str, Any]:
        return self._tmdb_mapping(
            f"/tv/{int(tmdb_id)}",
            {},
            not_found_identity=str(tmdb_id),
        )

    def season_details(self, series_tmdb_id: int, season_number: int) -> Mapping[str, Any]:
        return self._tmdb_mapping(
            f"/tv/{int(series_tmdb_id)}/season/{int(season_number)}",
            {},
            not_found_identity=f"{series_tmdb_id}:season:{season_number}",
        )

    def emby_libraries(self, *, cover: bool = False) -> Sequence[Mapping[str, Any]] | None:
        del cover
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        user_id = settings.emby_user_id.strip()
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not user_id or not token:
            raise CollectionGatewayConfigurationError("Emby 服务器配置不完整")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Views",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise CollectionProviderUnavailableError("Emby library query failed") from exc
        if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
            raise CollectionProviderUnavailableError("Emby library query returned an invalid payload")
        return tuple(dict(item) for item in payload["Items"] if isinstance(item, Mapping))

    def tmdb_movie_genres(self) -> Sequence[Mapping[str, Any]] | None:
        return _mapping_items(self._tmdb_mapping("/genre/movie/list", {}), "genres")

    def tmdb_tv_genres(self) -> Sequence[Mapping[str, Any]] | None:
        return _mapping_items(self._tmdb_mapping("/genre/tv/list", {}), "genres")

    def search_tmdb_companies(self, query: str, search_type: str) -> Sequence[Mapping[str, Any]]:
        if search_type != "network":
            return _mapping_items(
                self._tmdb_mapping("/search/company", {"query": query}),
                "results",
            )
        shows = _mapping_items(
            self._tmdb_mapping("/search/tv", {"query": query}),
            "results",
        )[:3]
        networks: dict[str, dict[str, Any]] = {}
        for show in shows:
            show_id = show.get("id")
            if show_id is None:
                continue
            details = self._tmdb_mapping(f"/tv/{int(show_id)}", {})
            for network in _mapping_items(details, "networks"):
                network_id = str(network.get("id") or "").strip()
                if not network_id or network_id in networks:
                    continue
                networks[network_id] = {
                    "id": network.get("id"),
                    "name": network.get("name"),
                    "logo_path": network.get("logo_path"),
                    "origin_country": network.get("origin_country"),
                    "source_show": show.get("name"),
                }
        return tuple(networks.values())

    def search_tmdb_people(self, query: str) -> Sequence[Mapping[str, Any]]:
        people = _mapping_items(
            self._tmdb_mapping("/search/person", {"query": query}),
            "results",
        )
        return tuple(
            {
                "id": person.get("id"),
                "name": person.get("name"),
                "profile_path": person.get("profile_path"),
                "department": person.get("known_for_department"),
                "known_for": "、".join(
                    str(item.get("title") or item.get("name") or "")
                    for item in person.get("known_for", [])
                    if isinstance(item, Mapping) and (item.get("title") or item.get("name"))
                ),
            }
            for person in people
        )

    def _tmdb_mapping(
        self,
        path: str,
        params: Mapping[str, Any],
        *,
        not_found_identity: str | None = None,
    ) -> dict[str, Any]:
        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        if not api_key or not base_url:
            raise CollectionGatewayConfigurationError("TMDb API Key 未配置")
        query = {"api_key": api_key, "language": "zh-CN", **dict(params)}
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    params=query,
                    headers={"Accept": "application/json"},
                )
                if response.status_code == 404 and not_found_identity is not None:
                    raise CollectionProviderNotFoundError(not_found_identity)
                response.raise_for_status()
                payload: Any = response.json()
        except CollectionProviderNotFoundError:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise CollectionProviderUnavailableError("TMDb collection query failed") from exc
        if not isinstance(payload, Mapping):
            raise CollectionProviderUnavailableError("TMDb collection query returned an invalid payload")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


def _mapping_items(payload: Mapping[str, Any], key: str) -> tuple[dict[str, Any], ...]:
    values = payload.get(key)
    if not isinstance(values, list):
        return ()
    return tuple(dict(item) for item in values if isinstance(item, Mapping))


__all__ = ["HttpxCollectionQueryAdapter"]
