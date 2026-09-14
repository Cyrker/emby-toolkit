from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from urllib.parse import parse_qs, quote, unquote, urlparse

import httpx

from etk.domains.operations.jobs import JobSubmissionService
from etk.platform import Settings

from .event_dispatch import EmbyEventOwner, classify_emby_event
from .image_store import MAX_IMAGE_BYTES, SqlAlchemyMediaImageStore
from .models import ImagePolicyKey, ImagePolicyRecord, MediaIdentity, MediaItemType, ServiceResult


class _MediaEventRepository(Protocol):
    def resolve_by_path(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> MediaIdentity | None: ...

    def update_cached_image_path(self, identity: MediaIdentity, image_type: str, value: str) -> int: ...

    def get_image_policy(self, key: ImagePolicyKey) -> ImagePolicyRecord | None: ...

    def replace_image_policy(
        self,
        key: ImagePolicyKey,
        rules: Sequence[Mapping[str, Any]],
        images: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]: ...

    def sync_emby_metadata(self, item_id: str, item: Mapping[str, Any]) -> bool: ...


class MetadataEventProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...


class HttpxP5MediaEventProvider:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def item(self, item_id: str) -> dict[str, Any] | None:
        settings = self._settings()
        base_url, token, user_id = _emby_identity(settings)
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            return None
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(normalized_id, safe='')}",
                    params={
                        "Fields": (
                            "ProviderIds,Type,Name,OriginalTitle,Overview,Tags,TagItems,OfficialRating,CustomRating,"
                            "PremiereDate,ProductionYear,Path,SeriesId,ParentIndexNumber,IndexNumber"
                        )
                    },
                    headers={"X-Emby-Token": token, "Accept": "application/json"},
                )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        return dict(payload) if isinstance(payload, Mapping) else None

    def library_for_path(self, item_path: str) -> Mapping[str, Any] | None:
        normalized_path = os.path.normpath(str(item_path or ""))
        if not normalized_path:
            return None
        settings = self._settings()
        base_url, token, _user_id = _emby_identity(settings)
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Library/VirtualFolders",
                    headers={"X-Emby-Token": token, "Accept": "application/json"},
                    params={"api_key": token},
                )
            response.raise_for_status()
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        if not isinstance(payload, list):
            return None
        matches: list[tuple[int, Mapping[str, Any]]] = []
        for folder in payload:
            if not isinstance(folder, Mapping):
                continue
            for location in folder.get("Locations") or []:
                root = os.path.normpath(str(location or ""))
                if root and (normalized_path == root or normalized_path.startswith(root.rstrip(os.sep) + os.sep)):
                    matches.append((len(root), folder))
        if not matches:
            return None
        matches.sort(key=lambda item: item[0], reverse=True)
        return dict(matches[0][1])

    def image(self, item_id: str, image_type: str) -> tuple[bytes, str] | None:
        settings = self._settings()
        base_url, token, _user_id = _emby_identity(settings)
        suffix = f"Images/{image_type}" if image_type != "Backdrop" else "Images/Backdrop/0"
        try:
            with self._client_factory() as client:
                with client.stream(
                    "GET",
                    f"{base_url}/Items/{quote(str(item_id), safe='')}/{suffix}",
                    headers={"X-Emby-Token": token},
                ) as response:
                    if response.status_code == 404:
                        return None
                    response.raise_for_status()
                    length = response.headers.get("Content-Length")
                    if length and int(length) > MAX_IMAGE_BYTES:
                        return None
                    content = bytearray()
                    for chunk in response.iter_bytes(chunk_size=128 * 1024):
                        content.extend(chunk)
                        if len(content) > MAX_IMAGE_BYTES:
                            return None
                    if not content:
                        return None
                    content_type = str(response.headers.get("Content-Type") or "image/jpeg")
                    return bytes(content), content_type
        except (httpx.HTTPError, TypeError, ValueError):
            return None

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        return httpx.Client(
            timeout=max(1.0, float(settings.emby_api_timeout)),
            follow_redirects=True,
            trust_env=False,
        )


class NativeP5MediaEventGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        repository: _MediaEventRepository,
        jobs: Callable[[], JobSubmissionService],
        provider: HttpxP5MediaEventProvider,
        images: SqlAlchemyMediaImageStore,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._jobs = jobs
        self._provider = provider
        self._images = images

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        del remote_addr
        event_name = str(payload.get("Event") or "").strip()
        if classify_emby_event(event_name) is not EmbyEventOwner.P5_MEDIA:
            raise ValueError(f"event {event_name!r} is not owned by P5 media")
        raw_item = payload.get("Item")
        item = dict(raw_item) if isinstance(raw_item, Mapping) else {}
        item_id = str(item.get("Id") or "").strip()
        item_type = MediaItemType.parse(item.get("Type"), allow_boxset=True)
        if not item_id or item_type is None:
            return ServiceResult({"status": "event_ignored_no_id_or_wrong_type"})
        item_path = str(item.get("Path") or "").strip()
        if item_type is not MediaItemType.BOXSET:
            library = self._provider.library_for_path(item_path)
            library_id = str((library or {}).get("ItemId") or (library or {}).get("Id") or "")
            allowed = {str(value) for value in self._settings().libraries_to_process}
            if library_id and library_id not in allowed:
                return ServiceResult({"status": "ignored_library"})
        if item_type in {MediaItemType.MOVIE, MediaItemType.EPISODE} and item_path.lower().endswith(".strm"):
            if not _is_etk_strm(item_path):
                return ServiceResult({"status": "ignored_non_etk_strm"})
        if event_name == "metadata.update":
            target_id = item_id
            target_name = str(item.get("Name") or "")
            if item_type is MediaItemType.EPISODE:
                current = self._provider.item(item_id) or item
                target_id = str(current.get("SeriesId") or "").strip()
                if not target_id:
                    return ServiceResult({"status": "event_ignored_episode_no_series_id"})
                series = self._provider.item(target_id)
                if series is not None:
                    target_name = str(series.get("Name") or target_name)
            submitted = self._jobs().submit_native_operation(
                "media.metadata-event-sync",
                {"item_id": target_id, "item_name": target_name},
                display_name=f"元数据同步: {target_name or target_id}",
                trigger_type="webhook",
                timeout_seconds=max(120, int(self._settings().emby_api_timeout) + 60),
            )
            return ServiceResult(
                {
                    "status": "metadata_update_task_debounced",
                    "item_id": target_id,
                    "job_id": str(submitted.projection.job.id),
                },
                202,
            )
        return self._observe_image(payload, item, item_type, host_url)

    def _observe_image(
        self,
        payload: Mapping[str, Any],
        item: Mapping[str, Any],
        item_type: MediaItemType,
        host_url: str,
    ) -> ServiceResult:
        item_id = str(item.get("Id") or "")
        image = payload.get("Image")
        image = dict(image) if isinstance(image, Mapping) else {}
        image_type = str(image.get("Type") or "").strip().split("/", 1)[0].title()
        if not image_type:
            return ServiceResult({"status": "image_update_observed", "item_id": item_id})
        season_number = (
            _optional_int(item.get("IndexNumber"))
            if item_type is MediaItemType.SEASON
            else _optional_int(item.get("ParentIndexNumber"))
        )
        episode_number = _optional_int(item.get("IndexNumber")) if item_type is MediaItemType.EPISODE else None
        identity = self._repository.resolve_by_path(
            str(item.get("Path") or ""),
            item_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if identity is None:
            return ServiceResult({"status": "image_update_identity_not_found", "item_id": item_id})
        source_url = str(image.get("Url") or "").strip()
        image_path = _tmdb_image_path(source_url)
        content_hash = ""
        if image_path:
            metadata_value = image_path
            source_url = f"https://image.tmdb.org/t/p/original/{image_path.lstrip('/')}"
        else:
            current = self._provider.image(item_id, image_type)
            if current is None:
                return ServiceResult({"status": "image_update_observed", "item_id": item_id})
            content, mime_type = current
            if not source_url:
                identity_source = str(
                    payload.get("EventId")
                    or payload.get("event_id")
                    or hashlib.sha256(
                        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
                    ).hexdigest()
                )
                source_url = (
                    f"emby-manual-image://{item_type.value}/{identity.tmdb_id}/"
                    f"{season_number or 0}/{episode_number or 0}/{image_type}/{item_id}/{identity_source}"
                )
            try:
                cached = self._images.store(source_url, content, mime_type)
            except (OSError, ValueError):
                return ServiceResult({"status": "image_update_archive_failed", "item_id": item_id})
            content_hash = str(cached["content_hash"])
            metadata_value = self._images.cache_token(content_hash)
        metadata_updated = self._repository.update_cached_image_path(identity, image_type, metadata_value)
        policy_updated = self._update_policy(
            identity,
            image_type,
            source_url,
            content_hash,
            host_url,
        )
        return ServiceResult(
            {
                "status": "image_update_cached",
                "item_id": item_id,
                "updated": max(metadata_updated, policy_updated),
            }
        )

    def _update_policy(
        self,
        identity: MediaIdentity,
        image_type: str,
        source_url: str,
        content_hash: str,
        host_url: str,
    ) -> int:
        key = ImagePolicyKey.create(
            identity.item_type,
            identity.tmdb_id,
            season_number=identity.season_number,
            episode_number=identity.episode_number,
        )
        existing = self._repository.get_image_policy(key)
        rules = [dict(value) for value in (existing.rules if existing else ())]
        if image_type not in {str(rule.get("type") or "") for rule in rules}:
            rules.append({"type": image_type, "limit": 1, "min_width": 0})
        images = [
            dict(value)
            for value in (existing.images if existing else ())
            if not (str(value.get("type") or "") == image_type and int(value.get("index") or 0) == 0)
        ]
        image = {
            "type": image_type,
            "index": 0,
            "source_url": source_url,
            "thumbnail_source_url": source_url,
            "manual": True,
        }
        if content_hash:
            image["content_hash"] = content_hash
            image["url"] = f"{str(host_url).rstrip('/')}/api/image_proxy?cache={content_hash}"
        images.append(image)
        self._repository.replace_image_policy(key, rules, images)
        return 1


class NativeMetadataEventOperation:
    def __init__(self, provider: HttpxP5MediaEventProvider, repository: _MediaEventRepository) -> None:
        self._provider = provider
        self._repository = repository

    def execute(self, parameters: Mapping[str, Any], progress: MetadataEventProgress) -> Mapping[str, Any]:
        item_id = str(parameters.get("item_id") or "").strip()
        if not item_id:
            raise ValueError("metadata event operation requires item_id")
        progress.checkpoint()
        item = self._provider.item(item_id)
        if item is None:
            raise RuntimeError("Emby metadata event item is unavailable")
        if not self._repository.sync_emby_metadata(item_id, item):
            raise RuntimeError("Emby metadata event identity is unavailable or ambiguous")
        progress.update(100, "Emby 元数据更新已同步到本地缓存")
        return {"ok": True, "item_id": item_id}


def _emby_identity(settings: Settings) -> tuple[str, str, str]:
    base_url = settings.emby_server_url.strip().rstrip("/")
    token = settings.emby_api_key.get_secret_value().strip()
    user_id = settings.emby_user_id.strip()
    if not base_url or not token or not user_id:
        raise RuntimeError("Emby authorization is unavailable")
    return base_url, token, user_id


def _is_etk_strm(path: str) -> bool:
    try:
        candidate = os.path.realpath(path)
        if not os.path.isfile(candidate) or os.path.getsize(candidate) <= 0:
            return False
        with open(candidate, encoding="utf-8", errors="ignore") as stream:
            content = stream.read(2048)
    except OSError:
        return False
    return "/api/p115/play/" in content or "/api/p115/virtual-play/" in content


def _tmdb_image_path(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    proxied = parse_qs(parsed.query).get("url", [None])[0]
    if proxied:
        parsed = urlparse(unquote(proxied))
    marker = "/t/p/"
    index = parsed.path.find(marker)
    if index < 0:
        return ""
    return "/" + parsed.path[index + len(marker) :].split("/", 1)[-1].lstrip("/")


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


__all__ = [
    "HttpxP5MediaEventProvider",
    "NativeMetadataEventOperation",
    "NativeP5MediaEventGateway",
]
