from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings

from .activation import (
    CollectionActivationNotFoundError,
    CollectionActivationProviderRejectedError,
    CollectionActivationProviderUnavailableError,
)
from .refresh import NativeCollectionReference


class HttpxNativeCollectionRefreshAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        emby_client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._emby_client_factory = emby_client_factory or client_factory or self._default_emby_client

    def list_collections(self) -> Sequence[NativeCollectionReference]:
        settings = self._settings()
        base_url, token, user_id = _emby_identity(settings)
        selected_libraries = {str(value).strip() for value in settings.libraries_to_process if str(value).strip()}
        with self._emby_client_factory() as client:
            libraries_payload = self._get_json(
                client,
                f"{base_url}/Library/VirtualFolders",
                headers={"Accept": "application/json", "X-Emby-Token": token},
            )
            if not isinstance(libraries_payload, list):
                raise RuntimeError("Emby virtual-folder query returned an invalid payload")
            references: list[NativeCollectionReference] = []
            for library in libraries_payload:
                if (
                    not isinstance(library, Mapping)
                    or str(library.get("CollectionType") or "").strip().casefold() == "boxsets"
                ):
                    continue
                library_id = str(library.get("Id") or "").strip()
                if not library_id or selected_libraries and library_id not in selected_libraries:
                    continue
                payload = self._get_json(
                    client,
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params={
                        "ParentId": library_id,
                        "IncludeItemTypes": "BoxSet",
                        "Recursive": "true",
                        "Fields": "ProviderIds,Name,Id,ImageTags",
                    },
                )
                if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                    raise RuntimeError("Emby native-collection query returned an invalid payload")
                for item in payload["Items"]:
                    if not isinstance(item, Mapping):
                        continue
                    provider_ids = item.get("ProviderIds")
                    tmdb_id = str(provider_ids.get("Tmdb") or "").strip() if isinstance(provider_ids, Mapping) else ""
                    emby_id = str(item.get("Id") or "").strip()
                    if not tmdb_id.isdigit() or not emby_id:
                        continue
                    references.append(
                        NativeCollectionReference(
                            emby_collection_id=emby_id,
                            tmdb_collection_id=tmdb_id,
                            name=str(item.get("Name") or tmdb_id).strip(),
                            library_id=library_id,
                        )
                    )
        unique: dict[str, NativeCollectionReference] = {}
        by_tmdb_id: dict[str, NativeCollectionReference] = {}
        for reference in references:
            existing = unique.get(reference.emby_collection_id)
            if existing is not None and existing != reference:
                raise RuntimeError("Emby native-collection identity is ambiguous")
            tmdb_existing = by_tmdb_id.get(reference.tmdb_collection_id)
            if tmdb_existing is not None and tmdb_existing.emby_collection_id != reference.emby_collection_id:
                raise RuntimeError("Emby native-collection TMDb identity is ambiguous")
            unique[reference.emby_collection_id] = reference
            by_tmdb_id[reference.tmdb_collection_id] = reference
        return tuple(unique.values())

    def collection_details(self, tmdb_collection_id: str) -> Mapping[str, Any]:
        try:
            settings = self._settings()
            base_url = settings.tmdb_api_base_url.strip().rstrip("/")
            api_key = settings.tmdb_api_key.get_secret_value().strip()
            if not base_url or not api_key:
                raise CollectionActivationProviderUnavailableError("TMDb is not configured")
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/collection/{quote(str(tmdb_collection_id), safe='')}",
                    params={"api_key": api_key, "language": "zh-CN"},
                    headers={"Accept": "application/json"},
                )
                if response.status_code == 404:
                    raise CollectionActivationNotFoundError("TMDb collection was not found")
                if 300 <= response.status_code < 400 or response.status_code in {408, 425, 429}:
                    raise CollectionActivationProviderUnavailableError(
                        f"TMDb collection query returned transient HTTP {response.status_code}"
                    )
                if 400 <= response.status_code < 500:
                    raise CollectionActivationProviderRejectedError(
                        f"TMDb collection query was rejected with HTTP {response.status_code}"
                    )
                if response.status_code >= 500:
                    raise CollectionActivationProviderUnavailableError(
                        f"TMDb collection query returned HTTP {response.status_code}"
                    )
                payload: Any = response.json()
        except (
            CollectionActivationNotFoundError,
            CollectionActivationProviderRejectedError,
            CollectionActivationProviderUnavailableError,
        ):
            raise
        except (httpx.HTTPError, OSError, RuntimeError, TypeError, ValueError) as exc:
            raise CollectionActivationProviderUnavailableError("TMDb collection query is unavailable") from exc
        if not isinstance(payload, Mapping):
            raise CollectionActivationProviderUnavailableError("TMDb collection query returned an invalid payload")
        return dict(payload)

    @staticmethod
    def _get_json(
        client: httpx.Client,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, str] | None = None,
    ) -> Any:
        try:
            response = client.get(url, headers=dict(headers), params=dict(params or {}))
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby native-collection query is unavailable") from exc

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=httpx.Timeout(20.0, connect=5.0), trust_env=False)

    @staticmethod
    def _default_emby_client() -> httpx.Client:
        return httpx.Client(timeout=httpx.Timeout(20.0, connect=5.0), trust_env=False)


def _emby_identity(settings: Settings) -> tuple[str, str, str]:
    base_url = settings.emby_server_url.strip().rstrip("/")
    token = settings.emby_api_key.get_secret_value().strip()
    user_id = settings.emby_user_id.strip()
    if not base_url or not token or not user_id:
        raise RuntimeError("Emby is not configured")
    return base_url, token, user_id


__all__ = ["HttpxNativeCollectionRefreshAdapter"]
