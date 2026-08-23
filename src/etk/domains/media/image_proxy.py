from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

IMMUTABLE_IMAGE_CACHE_CONTROL = "public, max-age=31536000, immutable"
DEFAULT_UPSTREAM_CONTENT_TYPE = "text/html; charset=utf-8"
TRANSPARENT_PIXEL_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


@dataclass(frozen=True, slots=True)
class CachedImage:
    path: str
    content_type: str


@dataclass(frozen=True, slots=True)
class UpstreamImage:
    chunks: Iterable[bytes]
    status_code: int
    content_type: str | None


@dataclass(frozen=True, slots=True)
class ImageProxyJsonResponse:
    payload: Mapping[str, str]
    status_code: int


@dataclass(frozen=True, slots=True)
class ImageProxyBytesResponse:
    content: bytes
    status_code: int
    content_type: str


type ImageProxyResult = CachedImage | UpstreamImage | ImageProxyJsonResponse | ImageProxyBytesResponse


class ImageProxyUpstreamError(RuntimeError):
    pass


class ImageProxyGateway(Protocol):
    def processor_ready(self) -> bool: ...

    def cached_image(self, cache_key: str) -> CachedImage | None: ...

    def fetch_external(self, external_url: str) -> UpstreamImage: ...

    def fetch_emby(self, image_path: str, raw_query: bytes) -> UpstreamImage: ...


class MediaImageProxyService:
    """P5 application boundary for cached, external, and Emby image delivery."""

    def __init__(self, gateway: ImageProxyGateway) -> None:
        self._gateway = gateway

    def external_image(
        self,
        *,
        cache_key: object = None,
        external_url: object = None,
    ) -> ImageProxyResult:
        normalized_cache_key = str(cache_key or "")
        if normalized_cache_key:
            cached = self._gateway.cached_image(normalized_cache_key)
            if cached is None:
                return ImageProxyJsonResponse({"error": "图片缓存不存在"}, 404)
            return cached

        normalized_url = str(external_url or "")
        if not normalized_url:
            return ImageProxyJsonResponse({"error": "缺少 'url' 参数"}, 400)
        if not is_supported_external_image_url(normalized_url):
            return _placeholder(status_code=404)
        try:
            return self._gateway.fetch_external(normalized_url)
        except ImageProxyUpstreamError:
            logger.warning("external image proxy upstream request failed")
            return _placeholder(status_code=404)

    def emby_image(self, image_path: str, *, raw_query: bytes) -> ImageProxyResult:
        if not self._gateway.processor_ready():
            return ImageProxyJsonResponse({"error": "核心处理器未就绪。"}, 503)
        try:
            return self._gateway.fetch_emby(str(image_path or ""), raw_query)
        except Exception:
            logger.exception("Emby image proxy upstream request failed")
            return _placeholder(status_code=200)


def is_supported_external_image_url(value: str) -> bool:
    """Match the HTTP(S) schemes accepted by the legacy requests-based proxy."""

    try:
        parsed = urlsplit(value)
        return parsed.scheme.casefold() in {"http", "https"} and bool(parsed.hostname)
    except ValueError:
        return False


def _placeholder(*, status_code: int) -> ImageProxyBytesResponse:
    return ImageProxyBytesResponse(
        TRANSPARENT_PIXEL_PNG,
        status_code,
        "image/png",
    )


__all__ = [
    "CachedImage",
    "DEFAULT_UPSTREAM_CONTENT_TYPE",
    "IMMUTABLE_IMAGE_CACHE_CONTROL",
    "ImageProxyBytesResponse",
    "ImageProxyGateway",
    "ImageProxyJsonResponse",
    "ImageProxyResult",
    "ImageProxyUpstreamError",
    "MediaImageProxyService",
    "TRANSPARENT_PIXEL_PNG",
    "UpstreamImage",
    "is_supported_external_image_url",
]
