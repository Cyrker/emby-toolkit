from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx

from etk.platform import Settings

from .image_proxy import CachedImage, ImageProxyUpstreamError, UpstreamImage
from .image_store import SqlAlchemyMediaImageStore

logger = logging.getLogger(__name__)


class HttpxMediaImageProxyGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        store: SqlAlchemyMediaImageStore,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        external_client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._client_factory = client_factory or self._default_client
        self._external_client_factory = external_client_factory or client_factory or self._default_external_client

    def processor_ready(self) -> bool:
        settings = self._settings()
        return bool(settings.emby_server_url.strip() and settings.emby_api_key.get_secret_value().strip())

    def cached_image(self, cache_key: str) -> CachedImage | None:
        cached = self._store.get(cache_key)
        if cached is None:
            return None
        return CachedImage(
            path=str(cached["path"]),
            content_type=str(cached.get("mime_type") or "application/octet-stream"),
        )

    def fetch_external(self, external_url: str) -> UpstreamImage:
        parsed = urlsplit(external_url)
        headers = {
            "User-Agent": self._settings().user_agent or "Mozilla/5.0",
            "Referer": f"{parsed.scheme}://{parsed.netloc}/",
        }
        try:
            logger.info("开始下载远端图片：来源=%s，图片=%s", parsed.hostname, parsed.path.rsplit("/", 1)[-1])
            return self._stream(external_url, headers=headers, client_factory=self._external_client_factory)
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("远端图片请求失败：来源=%s，错误类型=%s", parsed.hostname, type(exc).__name__)
            raise ImageProxyUpstreamError("external_image_fetch_failed") from exc

    def fetch_emby(self, image_path: str, raw_query: bytes) -> UpstreamImage:
        settings = self._settings()
        relative = str(image_path or "").strip().lstrip("/")
        parts = tuple(part for part in relative.split("/") if part)
        if not parts or any(part in {".", ".."} for part in parts):
            raise ValueError("Emby image path is invalid")
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise RuntimeError("Emby authorization is unavailable")
        query = [
            (key, value)
            for key, value in parse_qsl(raw_query.decode("utf-8"), keep_blank_values=True)
            if key.casefold() != "api_key"
        ]
        query.append(("api_key", token))
        target = f"{base_url}/{'/'.join(parts)}?{urlencode(query)}"
        return self._stream(target, client_factory=self._client_factory)

    def _stream(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        client_factory: Callable[[], httpx.Client],
    ) -> UpstreamImage:
        client = client_factory()
        response: httpx.Response | None = None
        try:
            request = client.build_request("GET", url, headers=headers)
            response = client.send(request, stream=True)
            response.raise_for_status()
        except BaseException:
            if response is not None:
                response.close()
            client.close()
            raise

        def chunks() -> Iterator[bytes]:
            size = 0
            complete = False
            try:
                for chunk in response.iter_bytes(chunk_size=8192):
                    size += len(chunk)
                    yield chunk
                complete = True
            finally:
                response.close()
                client.close()
                parsed = urlsplit(url)
                if parsed.hostname != urlsplit(self._settings().emby_server_url).hostname:
                    logger.info(
                        "远端图片传输%s：图片=%s，已传输=%s 字节",
                        "完成" if complete else "中断",
                        parsed.path.rsplit("/", 1)[-1],
                        size,
                    )

        return UpstreamImage(
            chunks=chunks(),
            status_code=response.status_code,
            content_type=response.headers.get("Content-Type"),
        )

    def _default_client(self) -> httpx.Client:
        return httpx.Client(
            timeout=httpx.Timeout(20.0, connect=15.0),
            follow_redirects=True,
            trust_env=False,
        )

    def _default_external_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(20.0, connect=15.0),
            follow_redirects=True,
            trust_env=False,
        )


__all__ = ["HttpxMediaImageProxyGateway"]
