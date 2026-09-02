from __future__ import annotations

import hashlib
import ipaddress
import json
import mimetypes
import os
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from sqlalchemy import text

from etk.compatibility.persistent_runtime_state import PersistentRuntimeStateStore
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

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
            name=str(details.get("Name") or "").strip() or None,
        )

    def find_collection_by_name(self, name: str) -> RemoteCollectionState | None:
        normalized_name = _required_text(name, "collection_name")
        base_url, user_id, token = self._configuration()
        payload = self._get(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items",
            token=token,
            params={"IncludeItemTypes": "BoxSet", "Recursive": "true", "Fields": "Name,Id,ProviderIds"},
        )
        if payload is None or not isinstance(payload.get("Items"), list):
            raise CollectionRemoteIndeterminate("Emby collection name query is invalid")
        matches = [
            str(item.get("Id") or "").strip()
            for item in payload["Items"]
            if isinstance(item, Mapping)
            and str(item.get("Name") or "").strip().casefold() == normalized_name.casefold()
            and str(item.get("Id") or "").strip()
        ]
        if len(matches) > 1:
            raise CollectionRemoteRejected("Emby custom collection name is ambiguous")
        return self.read_collection(matches[0]) if matches else None

    def create_collection(self, name: str, member_ids: Sequence[str]) -> RemoteCollectionState:
        normalized_name = _required_text(name, "collection_name")
        base_url, user_id, token = self._configuration()
        payload = {"Name": normalized_name, "UserId": user_id}
        if member_ids:
            payload["Ids"] = ",".join(str(value).strip() for value in member_ids if str(value).strip())
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Collections",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=payload,
                )
                if response.status_code in {408, 425, 429} or response.status_code >= 500:
                    raise CollectionRemoteIndeterminate("Emby collection creation outcome is unknown")
                if response.status_code >= 400:
                    raise CollectionRemoteRejected("Emby rejected custom collection creation")
                body = response.json()
        except (CollectionRemoteRejected, CollectionRemoteIndeterminate):
            raise
        except (httpx.TimeoutException, httpx.TransportError, TypeError, ValueError) as exc:
            raise CollectionRemoteIndeterminate("Emby collection creation outcome is unknown") from exc
        collection_id = str(body.get("Id") or body.get("id") or "").strip() if isinstance(body, Mapping) else ""
        if not collection_id:
            raise CollectionRemoteIndeterminate("Emby collection creation response has no identity")
        state = self.read_collection(collection_id)
        if state is None:
            raise CollectionRemoteIndeterminate("created Emby collection is not readable")
        return state

    def add_members(self, collection_id: str, member_ids: Sequence[str]) -> None:
        self._write(
            "POST",
            f"{self._configuration()[0]}/Collections/"
            f"{quote(_required_text(collection_id, 'collection_id'), safe='')}/Items",
            token=self._configuration()[2],
            params={"Ids": ",".join(str(value).strip() for value in member_ids if str(value).strip())},
        )

    def remove_members(self, collection_id: str, member_ids: Sequence[str]) -> None:
        if not member_ids:
            return
        self._write(
            "DELETE",
            f"{self._configuration()[0]}/Collections/"
            f"{quote(_required_text(collection_id, 'collection_id'), safe='')}/Items",
            token=self._configuration()[2],
            params={"Ids": ",".join(str(value).strip() for value in member_ids if str(value).strip())},
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


class SqlAlchemyCollectionAssetEffectsAdapter:
    """Processor-free owner for the collection image cache and placeholders."""

    _MAX_IMAGE_BYTES = 32 * 1024 * 1024
    _MIME_EXTENSIONS = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/avif": ".avif",
        "image/svg+xml": ".svg",
    }

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: Callable[[], Settings],
        *,
        data_root: Path,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._data_root = data_root.expanduser().resolve()
        self._client_factory = client_factory or self._default_client

    def read_archived_image(self, source_url: str) -> Mapping[str, Any] | None:
        source = _required_text(source_url, "source_url")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT source_url, content_hash, relative_path, mime_type, byte_size
                        FROM media_image_cache
                        WHERE source_url = :source_url
                        """
                    ),
                    {"source_url": source},
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                uow.session.execute(
                    text("UPDATE media_image_cache SET last_accessed_at = NOW() WHERE source_url = :source_url"),
                    {"source_url": source},
                )
            uow.commit()
        if row is None:
            return None
        path = self._cached_path(str(row["relative_path"]))
        expected_hash = str(row["content_hash"]).strip().casefold()
        expected_size = int(row["byte_size"])
        if path is None or not path.is_file() or path.stat().st_size != expected_size:
            return None
        with path.open("rb") as handle:
            actual_hash = hashlib.file_digest(handle, "sha256").hexdigest()
        if actual_hash != expected_hash:
            return None
        return {
            "source_url": source,
            "content_hash": actual_hash,
            "relative_path": str(row["relative_path"]),
            "mime_type": str(row["mime_type"]),
            "byte_size": expected_size,
        }

    def archive_image(self, source_url: str) -> Mapping[str, Any] | None:
        source = _required_text(source_url, "source_url")
        if not bool(self._settings().media_image_archive_enabled):
            return None
        cached = self.read_archived_image(source)
        if cached is not None:
            return cached
        parsed = urlparse(source)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("collection image source URL is invalid")
        _reject_private_literal(parsed.hostname)
        chunks: list[bytes] = []
        size = 0
        with self._client_factory() as client:
            with client.stream(
                "GET",
                source,
                headers={
                    "Accept": "image/*",
                    "User-Agent": str(getattr(self._settings(), "user_agent", "") or "ETK/collection-image"),
                },
            ) as response:
                if response.status_code != 200:
                    raise RuntimeError(f"collection image download returned HTTP {response.status_code}")
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > self._MAX_IMAGE_BYTES:
                    raise ValueError("collection image exceeds the size limit")
                mime_type = str(response.headers.get("Content-Type") or "").partition(";")[0].strip().casefold()
                if not mime_type.startswith("image/"):
                    mime_type = str(mimetypes.guess_type(parsed.path)[0] or "").casefold()
                if not mime_type.startswith("image/"):
                    raise ValueError("collection image response is not an image")
                for chunk in response.iter_bytes(128 * 1024):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > self._MAX_IMAGE_BYTES:
                        raise ValueError("collection image exceeds the size limit")
                    chunks.append(chunk)
        if not chunks:
            raise ValueError("collection image response is empty")
        content = b"".join(chunks)
        content_hash = hashlib.sha256(content).hexdigest()
        extension = self._MIME_EXTENSIONS.get(mime_type) or mimetypes.guess_extension(mime_type) or ".img"
        relative_path = f"{content_hash[:2]}/{content_hash}{extension}"
        final_path = self._cached_path(relative_path)
        if final_path is None:
            raise ValueError("collection image cache path is invalid")
        final_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_dir = self._archive_root() / ".tmp"
        temporary_dir.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=temporary_dir, prefix="collection-", delete=False) as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
                temporary_path = Path(handle.name)
            if not final_path.exists():
                os.replace(temporary_path, final_path)
                temporary_path = None
            with self._unit_of_work() as uow:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO media_image_cache (
                            source_url, content_hash, relative_path, mime_type, byte_size
                        ) VALUES (
                            :source_url, :content_hash, :relative_path, :mime_type, :byte_size
                        )
                        ON CONFLICT (source_url) DO UPDATE SET
                            content_hash = EXCLUDED.content_hash,
                            relative_path = EXCLUDED.relative_path,
                            mime_type = EXCLUDED.mime_type,
                            byte_size = EXCLUDED.byte_size,
                            last_accessed_at = NOW()
                        """
                    ),
                    {
                        "source_url": source,
                        "content_hash": content_hash,
                        "relative_path": relative_path,
                        "mime_type": mime_type,
                        "byte_size": size,
                    },
                )
                uow.commit()
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        return self.read_archived_image(source)

    def cleanup_placeholder(self, tmdb_id: str) -> bool:
        normalized = _required_text(tmdb_id, "tmdb_id")
        if not normalized.isdigit():
            raise ValueError("placeholder TMDb identity is invalid")
        with self._unit_of_work() as uow:
            active = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM media_metadata
                        WHERE tmdb_id = :tmdb_id
                          AND subscription_status IN ('WANTED','SUBSCRIBED','PENDING_RELEASE','PAUSED')
                    )
                    """
                ),
                {"tmdb_id": normalized},
            )
            uow.commit()
        if active:
            return True
        directory = (self._data_root / "cache" / "missing_posters").resolve()
        if not directory.is_relative_to(self._data_root) or not directory.is_dir():
            return True
        for path in directory.glob(f"{normalized}_*.jpg"):
            path.unlink(missing_ok=True)
        return True

    def discard_unreferenced_sources(self, source_urls: Sequence[str]) -> int:
        discarded = 0
        for source in dict.fromkeys(str(value).strip() for value in source_urls if str(value).strip()):
            relative_path: str | None = None
            content_hash: str | None = None
            remove_file = False
            with self._unit_of_work() as uow:
                referenced = uow.session.scalar(
                    text(
                        """
                        SELECT EXISTS(
                            SELECT 1 FROM media_image_policy_cache
                            WHERE images_json @> CAST(:needle AS jsonb)
                        )
                        """
                    ),
                    {
                        "needle": json.dumps(
                            [{"source_url": source}],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    },
                )
                if referenced:
                    uow.commit()
                    continue
                row = (
                    uow.session.execute(
                        text(
                            """
                            DELETE FROM media_image_cache
                            WHERE source_url = :source_url
                            RETURNING content_hash, relative_path
                            """
                        ),
                        {"source_url": source},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is not None:
                    content_hash = str(row["content_hash"]).strip().casefold()
                    relative_path = str(row["relative_path"])
                    token = f"etk-cache://{content_hash}"
                    still_referenced = uow.session.scalar(
                        text(
                            """
                            SELECT EXISTS(
                                SELECT 1 FROM media_image_cache WHERE content_hash = :content_hash
                                UNION ALL
                                SELECT 1 FROM media_metadata
                                WHERE screenshot_hash = :content_hash
                                   OR poster_path = :token OR backdrop_path = :token
                                   OR logo_path = :token OR thumb_path = :token
                            )
                            """
                        ),
                        {"content_hash": content_hash, "token": token},
                    )
                    remove_file = not bool(still_referenced)
                    discarded += 1
                uow.commit()
            if remove_file and relative_path is not None:
                path = self._cached_path(relative_path)
                if path is not None:
                    path.unlink(missing_ok=True)
        return discarded

    def _archive_root(self) -> Path:
        configured = str(self._settings().media_image_archive_path or "").strip()
        if configured:
            candidate = Path(os.path.expandvars(os.path.expanduser(configured)))
            if not candidate.is_absolute():
                candidate = self._data_root / candidate
        else:
            candidate = self._data_root / "media_images"
        return candidate.resolve()

    def _cached_path(self, relative_path: str) -> Path | None:
        relative = Path(str(relative_path).replace("\\", "/"))
        if relative.is_absolute():
            return None
        root = self._archive_root()
        path = (root / relative).resolve()
        return path if path.is_relative_to(root) else None

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(90.0, connect=15.0),
            follow_redirects=False,
            trust_env=False,
        )


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


def _reject_private_literal(hostname: str) -> None:
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return
    if not address.is_global:
        raise ValueError("collection image source cannot use a non-public literal address")


__all__ = [
    "HttpxCollectionRemoteEffectsAdapter",
    "LegacyCollectionAssetEffectsAdapter",
    "PersistentCollectionDeletionSuppression",
    "SqlAlchemyCollectionAssetEffectsAdapter",
]
