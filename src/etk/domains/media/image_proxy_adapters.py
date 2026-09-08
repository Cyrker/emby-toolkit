from __future__ import annotations

from collections.abc import Callable, Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx

from etk.platform import Settings

from .image_proxy import CachedImage, ImageProxyUpstreamError, UpstreamImage
from .image_store import SqlAlchemyMediaImageStore


class HttpxMediaImageProxyGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        store: SqlAlchemyMediaImageStore,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._client_factory = client_factory or self._default_client

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
            return self._stream(external_url, headers=headers)
        except (httpx.HTTPError, ValueError) as exc:
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
        return self._stream(target)

    def _stream(self, url: str, *, headers: dict[str, str] | None = None) -> UpstreamImage:
        client = self._client_factory()
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
            try:
                yield from response.iter_bytes(chunk_size=8192)
            finally:
                response.close()
                client.close()

        return UpstreamImage(
            chunks=chunks(),
            status_code=response.status_code,
            content_type=response.headers.get("Content-Type"),
        )

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(20.0, connect=15.0),
            follow_redirects=True,
        )


__all__ = ["HttpxMediaImageProxyGateway"]
