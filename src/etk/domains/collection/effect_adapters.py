from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from etk.compatibility.persistent_runtime_state import PersistentRuntimeStateStore
from etk.platform import Settings

from .effects import (
    CollectionRemoteIndeterminate,
    CollectionRemoteRejected,
    RemoteCollectionState,
)


class HttpxCollectionRemoteEffectsAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def read_collection(self, collection_id: str) -> RemoteCollectionState | None:
        normalized_id = _required_text(collection_id, "collection_id")
        base_url, user_id, token = self._configuration()
        details = self._get(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(normalized_id, safe='')}",
            token=token,
            params={"Fields": "Type,ProviderIds,Name"},
            not_found=True,
        )
        if details is None:
            return None
        if str(details.get("Type") or "") != "BoxSet":
            raise CollectionRemoteRejected("Emby collection target is not a BoxSet")
        members = self._get(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items",
            token=token,
            params={"ParentId": normalized_id, "Fields": "Id"},
        )
        if members is None or not isinstance(members.get("Items"), list):
            raise CollectionRemoteIndeterminate("Emby collection members response is invalid")
        member_ids = tuple(
            str(item.get("Id") or "").strip()
            for item in members["Items"]
            if isinstance(item, Mapping) and str(item.get("Id") or "").strip()
        )
        provider_ids = details.get("ProviderIds")
        tmdb_id = str(provider_ids.get("Tmdb") or "").strip() if isinstance(provider_ids, Mapping) else ""
        return RemoteCollectionState(
            collection_id=normalized_id,
            member_ids=member_ids,
            tmdb_collection_id=tmdb_id or None,
        )

    def list_native_collections(self, tmdb_collection_id: str) -> Sequence[RemoteCollectionState]:
        normalized_tmdb_id = _required_text(tmdb_collection_id, "tmdb_collection_id")
        base_url, user_id, token = self._configuration()
        payload = self._get(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items",
            token=token,
            params={
                "IncludeItemTypes": "BoxSet",
                "Recursive": "true",
                "Fields": "ProviderIds,Name",
            },
        )
        if payload is None or not isinstance(payload.get("Items"), list):
            raise CollectionRemoteIndeterminate("Emby native collection response is invalid")
        matches: list[RemoteCollectionState] = []
        for item in payload["Items"]:
            if not isinstance(item, Mapping):
                continue
            provider_ids = item.get("ProviderIds")
            remote_tmdb_id = str(provider_ids.get("Tmdb") or "").strip() if isinstance(provider_ids, Mapping) else ""
            collection_id = str(item.get("Id") or "").strip()
            if remote_tmdb_id != normalized_tmdb_id or not collection_id:
                continue
            state = self.read_collection(collection_id)
            if state is not None:
                matches.append(state)
        return tuple(matches)

    def empty_collection(self, state: RemoteCollectionState) -> None:
        if not state.member_ids:
            return
        base_url, _user_id, token = self._configuration()
        self._write(
            "DELETE",
            f"{base_url}/Collections/{quote(state.collection_id, safe='')}/Items",
            token=token,
            params={"Ids": ",".join(state.member_ids)},
        )

    def delete_collection(self, collection_id: str) -> None:
        normalized_id = _required_text(collection_id, "collection_id")
        base_url, user_id, token = self._configuration()
        self._write(
            "POST",
            f"{base_url}/Items/{quote(normalized_id, safe='')}/Delete",
            token=token,
            params={"UserId": user_id},
            not_found_ok=True,
        )

    def _get(
        self,
        url: str,
        *,
        token: str,
        params: Mapping[str, str],
        not_found: bool = False,
    ) -> dict[str, Any] | None:
        try:
            with self._client_factory() as client:
                response = client.get(
                    url,
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=dict(params),
                )
                if response.status_code == 404 and not_found:
                    return None
                if 300 <= response.status_code < 400 or response.status_code in {408, 425, 429}:
                    raise CollectionRemoteIndeterminate(
                        f"Emby collection read returned transient HTTP {response.status_code}"
                    )
                if 400 <= response.status_code < 500:
                    raise CollectionRemoteRejected(f"Emby rejected collection read with HTTP {response.status_code}")
                if response.status_code >= 500:
                    raise CollectionRemoteIndeterminate(f"Emby collection read returned HTTP {response.status_code}")
                payload: Any = response.json()
        except (CollectionRemoteRejected, CollectionRemoteIndeterminate):
            raise
        except (httpx.TimeoutException, httpx.TransportError, TypeError, ValueError) as exc:
            raise CollectionRemoteIndeterminate("Emby collection read failed") from exc
        if not isinstance(payload, Mapping):
            raise CollectionRemoteIndeterminate("Emby collection read returned an invalid payload")
        return dict(payload)

    def _write(
        self,
        method: str,
        url: str,
        *,
        token: str,
        params: Mapping[str, str],
        not_found_ok: bool = False,
    ) -> None:
        try:
            with self._client_factory() as client:
                response = client.request(
                    method,
                    url,
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=dict(params),
                )
                if response.status_code == 404 and not_found_ok:
                    return
                if 300 <= response.status_code < 400 or response.status_code in {408, 425, 429}:
                    raise CollectionRemoteIndeterminate(
                        f"Emby collection mutation returned transient HTTP {response.status_code}"
                    )
                if 400 <= response.status_code < 500:
                    raise CollectionRemoteRejected(
                        f"Emby rejected collection mutation with HTTP {response.status_code}"
                    )
                if response.status_code >= 500:
                    raise CollectionRemoteIndeterminate(
                        f"Emby collection mutation returned HTTP {response.status_code}"
                    )
        except (CollectionRemoteRejected, CollectionRemoteIndeterminate):
            raise
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise CollectionRemoteIndeterminate("Emby collection mutation outcome is unknown") from exc

    def _configuration(self) -> tuple[str, str, str]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        user_id = settings.emby_user_id.strip()
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not user_id or not token:
            raise CollectionRemoteRejected("Emby collection configuration is incomplete")
        return base_url, user_id, token

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=max(1.0, float(self._settings().emby_api_timeout)))


class LegacyCollectionAssetEffectsAdapter:
    """Worker-only compatibility seam for established local image-cache files."""

    @staticmethod
    def read_archived_image(source_url: str) -> Mapping[str, Any] | None:
        from handler.media_image_cache import _source_record

        value = _source_record(source_url)
        if not isinstance(value, Mapping):
            return None
        path = Path(str(value.get("path") or ""))
        expected_hash = str(value.get("content_hash") or "").strip().casefold()
        expected_size = int(value.get("byte_size") or -1)
        if not path.is_file() or path.stat().st_size != expected_size:
            return None
        with path.open("rb") as source:
            actual_hash = hashlib.file_digest(source, "sha256").hexdigest()
        if actual_hash != expected_hash:
            return None
        return {
            "source_url": str(value.get("source_url") or source_url),
            "content_hash": actual_hash,
            "relative_path": str(value.get("relative_path") or ""),
            "mime_type": str(value.get("mime_type") or ""),
            "byte_size": expected_size,
        }

    @staticmethod
    def archive_image(source_url: str) -> Mapping[str, Any] | None:
        from handler.media_image_cache import cache_remote_image

        value = cache_remote_image(source_url)
        if not isinstance(value, Mapping):
            return None
        return LegacyCollectionAssetEffectsAdapter.read_archived_image(source_url)

    @staticmethod
    def cleanup_placeholder(tmdb_id: str) -> bool:
        from handler.poster_generator import cleanup_placeholder

        cleanup_placeholder(tmdb_id)
        return True

    @staticmethod
    def discard_unreferenced_sources(source_urls: Sequence[str]) -> int:
        from handler.media_image_cache import discard_unreferenced_policy_sources

        normalized = tuple(dict.fromkeys(str(value).strip() for value in source_urls if str(value).strip()))
        discard_unreferenced_policy_sources(normalized)
        return len(normalized)


class PersistentCollectionDeletionSuppression:
    def __init__(self, store: PersistentRuntimeStateStore) -> None:
        self._store = store

    def suppress(self, collection_id: str, *, ttl_seconds: float) -> None:
        self._store.put_ttl_marker(
            "deleting-emby-collections",
            _required_text(collection_id, "collection_id"),
            ttl_seconds,
        )

    def is_suppressed(self, collection_id: str) -> bool:
        return self._store.has_ttl_marker(
            "deleting-emby-collections",
            _required_text(collection_id, "collection_id"),
        )


def _required_text(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


__all__ = [
    "HttpxCollectionRemoteEffectsAdapter",
    "LegacyCollectionAssetEffectsAdapter",
    "PersistentCollectionDeletionSuppression",
]
