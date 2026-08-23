from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

from .image_proxy import CachedImage, ImageProxyUpstreamError, UpstreamImage


class LegacyMediaImageProxyGateway:
    """Lazy compatibility adapter for the current image cache and processor state."""

    def __init__(self, request_get: Callable[..., Any] | None = None) -> None:
        self._request_get = request_get

    @staticmethod
    def processor_ready() -> bool:
        import extensions

        return extensions.media_processor_instance is not None

    @staticmethod
    def cached_image(cache_key: str) -> CachedImage | None:
        from handler.media_image_cache import get_cached_image

        cached = get_cached_image(cache_key)
        if not cached:
            return None
        if not isinstance(cached, Mapping):
            raise TypeError("legacy cached image result must be a mapping")
        return CachedImage(
            path=str(cached["path"]),
            content_type=str(cached.get("mime_type") or "application/octet-stream"),
        )

    def fetch_external(self, external_url: str) -> UpstreamImage:
        import requests

        import config_manager

        parsed_url = urlparse(external_url)
        headers = {
            "User-Agent": config_manager.APP_CONFIG.get("user_agent", "Mozilla/5.0"),
            "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/",
        }
        request_get = self._request_get or requests.get
        response: Any | None = None
        try:
            response = request_get(
                external_url,
                stream=True,
                timeout=15,
                headers=headers,
                proxies=config_manager.get_proxies_for_requests(),
            )
            assert response is not None
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            _close_response(response)
            raise ImageProxyUpstreamError("external_image_fetch_failed") from exc
        return _upstream_image(response)

    def fetch_emby(self, image_path: str, raw_query: bytes) -> UpstreamImage:
        import requests

        import extensions

        processor = extensions.media_processor_instance
        if processor is None:
            raise RuntimeError("media processor became unavailable")
        target_url = f"{str(processor.emby_url).rstrip('/')}/{image_path}"
        query_string = raw_query.decode("utf-8")
        if query_string:
            target_url += f"?{query_string}"
        separator = "&" if "?" in target_url else "?"
        target_url = f"{target_url}{separator}api_key={processor.emby_api_key}"

        request_get = self._request_get or requests.get
        response: Any | None = None
        try:
            response = request_get(target_url, stream=True, timeout=20)
            assert response is not None
            response.raise_for_status()
        except Exception:
            _close_response(response)
            raise
        return _upstream_image(response)


def _upstream_image(response: Any) -> UpstreamImage:
    def chunks():
        try:
            yield from response.iter_content(chunk_size=8192)
        finally:
            _close_response(response)

    return UpstreamImage(
        chunks=chunks(),
        status_code=int(response.status_code),
        content_type=response.headers.get("Content-Type"),
    )


def _close_response(response: Any | None) -> None:
    if response is None:
        return
    close = getattr(response, "close", None)
    if callable(close):
        close()


__all__ = ["LegacyMediaImageProxyGateway"]
