from __future__ import annotations

import base64
import hashlib
import mimetypes
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx

from etk.platform import Settings

from .image_editing import (
    EmbyMediaDetailsNotFoundError,
    ImageUpdateCommand,
    ImageUpdateOutcome,
    TmdbIdentityMissingError,
    TmdbImageCandidate,
    TmdbImageCatalog,
    TmdbImagesNotFoundError,
    UnsupportedImageMediaTypeError,
)
from .image_store import MAX_IMAGE_BYTES, SqlAlchemyMediaImageStore
from .models import ImagePolicyKey, ImagePolicyRecord, MediaIdentity, MediaItemType

_IMAGE_LANGUAGE_PARAMETER = "zh-CN,zh-TW,zh,en,null,ja,ko"
_PREVIEW_BASE_URL = "https://image.tmdb.org/t/p/w500"
_ORIGINAL_BASE_URL = "https://image.tmdb.org/t/p/original"
_IMAGE_TYPES = {
    "poster": "Primary",
    "clearlogo": "Logo",
    "fanart": "Backdrop",
    "landscape": "Thumb",
}


class _MediaImageRepository(Protocol):
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


class HttpxMediaImageEditingGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        repository: _MediaImageRepository,
        store: SqlAlchemyMediaImageStore,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        external_client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._store = store
        self._client_factory = client_factory or self._default_client
        self._external_client_factory = external_client_factory or client_factory or self._default_external_client

    def processor_ready(self) -> bool:
        settings = self._settings()
        return bool(
            settings.emby_server_url.strip()
            and settings.emby_api_key.get_secret_value().strip()
            and settings.emby_user_id.strip()
        )

    def tmdb_images(self, item_id: str) -> TmdbImageCatalog:
        item = self._emby_item(item_id)
        if item is None:
            raise EmbyMediaDetailsNotFoundError(item_id)
        providers = item.get("ProviderIds")
        tmdb_id = str(providers.get("Tmdb") or "").strip() if isinstance(providers, Mapping) else ""
        if not tmdb_id:
            raise TmdbIdentityMissingError(item_id)
        item_type = str(item.get("Type") or "").strip()
        if item_type == "Movie":
            media_type = "movie"
        elif item_type == "Series":
            media_type = "tv"
        else:
            raise UnsupportedImageMediaTypeError(item_type)
        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not api_key:
            raise RuntimeError("TMDb API Key 未配置")
        try:
            with self._external_client_factory() as client:
                response = client.get(
                    f"{settings.tmdb_api_base_url.strip().rstrip('/')}/{media_type}/{quote(tmdb_id, safe='')}",
                    params={
                        "api_key": api_key,
                        "language": "zh-CN",
                        "append_to_response": "images",
                        "include_image_language": _IMAGE_LANGUAGE_PARAMETER,
                    },
                    headers={"Accept": "application/json"},
                )
            response.raise_for_status()
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise TmdbImagesNotFoundError(tmdb_id) from exc
        images = payload.get("images") if isinstance(payload, Mapping) else None
        if not isinstance(images, Mapping):
            raise TmdbImagesNotFoundError(tmdb_id)
        return TmdbImageCatalog(
            posters=_format_images(images.get("posters", [])),
            backdrops=_format_images(images.get("backdrops", [])),
            logos=_format_images(images.get("logos", [])),
        )

    def update_image(self, command: ImageUpdateCommand) -> ImageUpdateOutcome:
        normalized_type = str(command.image_type or "").strip().lower()
        emby_image_type = _IMAGE_TYPES.get(normalized_type)
        if emby_image_type is None:
            return ImageUpdateOutcome(False, f"不支持的图片类型: {normalized_type}")
        item = self._emby_item(command.item_id)
        if item is None:
            return ImageUpdateOutcome(False, "无法获取该媒体的 Emby 详情。")
        item_type = MediaItemType.parse(item.get("Type"))
        if item_type is None:
            return ImageUpdateOutcome(False, f"不支持为 {item.get('Type') or '未知类型'} 同步 embytool 图片缓存。")
        path = str(item.get("Path") or "").strip()
        if not path:
            return ImageUpdateOutcome(False, "无法定位该媒体的路径，不能同步 embytool 图片缓存。")
        season_number = (
            _optional_int(item.get("IndexNumber"))
            if item_type is MediaItemType.SEASON
            else _optional_int(item.get("ParentIndexNumber"))
        )
        episode_number = _optional_int(item.get("IndexNumber")) if item_type is MediaItemType.EPISODE else None
        identity = self._repository.resolve_by_path(
            path,
            item_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if identity is None:
            return ImageUpdateOutcome(False, "无法定位该媒体的 embytool 元数据记录，未执行图片更改。")
        try:
            content, mime_type = self._image_content(command)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            return ImageUpdateOutcome(False, str(exc))
        confirmed, indeterminate = self._upload_and_confirm(
            command.item_id,
            emby_image_type,
            content,
            mime_type,
        )
        if not confirmed:
            status = "indeterminate" if indeterminate else ""
            message = "上传图片到 Emby 的结果无法确认。" if indeterminate else "上传图片到 Emby 失败。"
            return ImageUpdateOutcome(False, message, effect_status=status)
        source_url = str(command.image_url or "").strip() or (
            f"manual-upload://{item_type.value}/{identity.tmdb_id}/{season_number or 0}/"
            f"{episode_number or 0}/{emby_image_type}/{uuid4().hex}"
        )
        try:
            cached = self._store.store(source_url, content, mime_type)
            content_hash = str(cached["content_hash"])
            metadata_value = self._store.cache_token(content_hash)
            self._repository.update_cached_image_path(identity, emby_image_type, metadata_value)
            self._update_policy(
                identity,
                emby_image_type,
                source_url,
                content_hash,
            )
        except Exception:
            return ImageUpdateOutcome(
                False,
                "Emby 图片已更新，但 embytool 图片缓存写入结果无法确认。",
                effect_status="indeterminate",
            )
        return ImageUpdateOutcome(True, f"{normalized_type} 替换成功，已同步 Emby 与 embytool 图片仓库。")

    def _emby_item(self, item_id: str) -> dict[str, Any] | None:
        settings = self._settings()
        item = str(item_id or "").strip()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        user_id = settings.emby_user_id.strip()
        if not item or not base_url or not token or not user_id:
            return None
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(item, safe='')}",
                    params={
                        "Fields": "ProviderIds,Type,Name,Path,SeriesId,ParentIndexNumber,IndexNumber,ImageTags,People"
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

    def _image_content(self, command: ImageUpdateCommand) -> tuple[bytes, str]:
        if command.image_bytes:
            content = bytes(command.image_bytes)
            declared = str(command.content_type or "")
            return content, _detect_mime(content, declared, str(command.image_url or ""))
        source_url = str(command.image_url or "").strip()
        parsed = urlsplit(source_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("图片 URL 无效")
        with self._external_client_factory() as client:
            with client.stream(
                "GET",
                source_url,
                headers={"User-Agent": self._settings().user_agent or "Mozilla/5.0"},
            ) as response:
                response.raise_for_status()
                length = response.headers.get("Content-Length")
                if length and int(length) > MAX_IMAGE_BYTES:
                    raise ValueError("图片超过 32MB 限制。")
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes(chunk_size=128 * 1024):
                    size += len(chunk)
                    if size > MAX_IMAGE_BYTES:
                        raise ValueError("图片超过 32MB 限制。")
                    chunks.append(chunk)
                content = b"".join(chunks)
                return content, _detect_mime(content, response.headers.get("Content-Type") or "", source_url)

    def _upload_and_confirm(
        self,
        item_id: str,
        image_type: str,
        content: bytes,
        content_type: str,
    ) -> tuple[bool, bool]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        image_path = f"Images/{image_type}" if image_type != "Backdrop" else "Images/Backdrop/0"
        uncertain = False
        rejected = False
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(item_id, safe='')}/Images/{image_type}",
                    headers={"X-Emby-Token": token, "Content-Type": content_type},
                    content=base64.b64encode(content),
                )
            uncertain = response.status_code >= 500
            rejected = 400 <= response.status_code < 500
        except (httpx.TimeoutException, httpx.TransportError):
            uncertain = True
        try:
            with self._client_factory() as client:
                readback = client.get(
                    f"{base_url}/Items/{quote(item_id, safe='')}/{image_path}",
                    headers={"X-Emby-Token": token},
                )
            if (
                readback.status_code == 200
                and hashlib.sha256(readback.content).digest() == hashlib.sha256(content).digest()
            ):
                return True, False
        except (httpx.HTTPError, TypeError, ValueError):
            uncertain = True
        return False, uncertain or not rejected

    def _update_policy(
        self,
        identity: MediaIdentity,
        image_type: str,
        source_url: str,
        content_hash: str,
    ) -> None:
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
        images.append(
            {
                "type": image_type,
                "index": 0,
                "source_url": source_url,
                "thumbnail_source_url": source_url,
                "manual": True,
                "content_hash": content_hash,
            }
        )
        self._repository.replace_image_policy(key, rules, images)

    def _default_client(self) -> httpx.Client:
        return httpx.Client(
            timeout=httpx.Timeout(90.0, connect=15.0),
            follow_redirects=True,
            trust_env=False,
        )

    def _default_external_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(90.0, connect=15.0),
            follow_redirects=True,
            trust_env=False,
        )


def _format_images(values: object) -> tuple[TmdbImageCandidate, ...]:
    if not isinstance(values, Sequence) or isinstance(values, str | bytes | bytearray):
        raise TypeError("TMDb images must be a sequence")
    result: list[TmdbImageCandidate] = []
    for raw in values:
        if not isinstance(raw, Mapping):
            raise TypeError("TMDb image must be an object")
        path = str(raw.get("file_path") or "").strip()
        if not path:
            continue
        result.append(
            TmdbImageCandidate(
                preview=f"{_PREVIEW_BASE_URL}{path}",
                original=f"{_ORIGINAL_BASE_URL}{path}",
                aspect_ratio=raw.get("aspect_ratio", 1),
                width=_optional_int(raw.get("width")),
                height=_optional_int(raw.get("height")),
            )
        )
    return tuple(result)


def _detect_mime(content: bytes, declared: str, source_url: str) -> str:
    mime_type = str(declared or "").split(";", 1)[0].strip().lower()
    if mime_type.startswith("image/"):
        return mime_type
    guessed = mimetypes.guess_type(urlsplit(source_url).path)[0] if source_url else None
    if guessed and guessed.startswith("image/"):
        return guessed
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if content.startswith(b"RIFF") and content[8:12] == b"WEBP":
        return "image/webp"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    raise ValueError("上传内容不是可识别的图片。")


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


__all__ = ["HttpxMediaImageEditingGateway"]
