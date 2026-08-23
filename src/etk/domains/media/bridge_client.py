from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings

PLUGIN_UPDATE_URL = "https://github.com/hbq0405/etk-mediainfo-bridge/releases/latest/download/ETKMediaInfoBridge.dll"


class BridgeWriteOutcome(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"


class BridgeWriteError(RuntimeError):
    def __init__(
        self,
        operation: str,
        outcome: BridgeWriteOutcome,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.outcome = outcome
        self.status_code = status_code


class BridgeWriteRejected(BridgeWriteError):
    def __init__(self, operation: str, status_code: int, message: str) -> None:
        super().__init__(
            operation,
            BridgeWriteOutcome.REJECTED,
            message,
            status_code=status_code,
        )


class BridgeWriteIndeterminate(BridgeWriteError):
    def __init__(self, operation: str, message: str) -> None:
        super().__init__(operation, BridgeWriteOutcome.INDETERMINATE, message)


class BridgeAuthorizationChanged(BridgeWriteError):
    def __init__(self, operation: str) -> None:
        super().__init__(
            operation,
            BridgeWriteOutcome.REJECTED,
            "Emby authorization changed after media effect staging",
        )


@dataclass(frozen=True, slots=True)
class BridgeWriteResult:
    operation: str
    status_code: int
    payload: Mapping[str, Any]
    confirmed: bool = True


class HttpxEmbyBridgeClient:
    """Typed P5 owner for the six ETKMediaInfo Bridge write contracts.

    Writes are deliberately attempted once. A timeout or transport break after the
    request starts has an unknown provider outcome and must be reconciled by the
    caller instead of being replayed automatically.
    """

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def authorization_revision(self) -> str:
        return emby_authorization_revision(self._settings())

    def configure_origin(self, etk_url: str) -> BridgeWriteResult:
        base_url, api_key = self._configuration()
        normalized = str(etk_url or "").strip()
        if not normalized:
            raise ValueError("ETK origin URL is required")
        result = self._post(
            "configure-origin",
            f"{base_url}/ETKMediaInfo/Origin",
            params={"api_key": api_key},
            payload={"Url": normalized},
        )
        return BridgeWriteResult(
            result.operation,
            result.status_code,
            result.payload,
            confirmed=bool(result.payload.get("Configured", result.payload.get("configured"))),
        )

    def configure_discovery_url(self, discovery_url: str) -> BridgeWriteResult:
        base_url, api_key = self._configuration()
        result = self._post(
            "configure-discovery-url",
            f"{base_url}/ETKMediaInfo/DiscoveryUrl",
            params={"api_key": api_key},
            payload={"Url": str(discovery_url or "")},
        )
        return BridgeWriteResult(
            result.operation,
            result.status_code,
            result.payload,
            confirmed=bool(result.payload.get("Configured", result.payload.get("configured"))),
        )

    def apply_mediainfo(
        self,
        item_id: str,
        raw: Any,
        *,
        drop_conflicting_external_streams: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult:
        payload = _normalize_mediainfo_payload(raw)
        payload["DropConflictingExternalStreams"] = bool(drop_conflicting_external_streams)
        return self._item_post(
            "apply-mediainfo",
            item_id,
            "",
            payload,
            expected_authorization_revision=expected_authorization_revision,
        )

    def apply_intro(
        self,
        item_id: str,
        intro_start_ticks: int,
        intro_end_ticks: int,
    ) -> BridgeWriteResult:
        start = int(intro_start_ticks)
        end = int(intro_end_ticks)
        if start < 0 or end <= start:
            raise ValueError("intro ticks must define a positive interval")
        return self._item_post(
            "apply-intro",
            item_id,
            "/Intro",
            {"IntroStartTicks": start, "IntroEndTicks": end},
        )

    def apply_credits(
        self,
        item_id: str,
        credits_start_ticks: int,
    ) -> BridgeWriteResult:
        start = int(credits_start_ticks)
        if start < 0:
            raise ValueError("credits ticks must not be negative")
        return self._item_post(
            "apply-credits",
            item_id,
            "/Credits",
            {"CreditsStartTicks": start},
        )

    def update_chapters(
        self,
        item_id: str,
        *,
        intro_start_ticks: int | None = None,
        intro_end_ticks: int | None = None,
        credits_start_ticks: int | None = None,
        clear_intro: bool = False,
        clear_credits: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult:
        payload: dict[str, Any] = {
            "ClearIntro": bool(clear_intro),
            "ClearCredits": bool(clear_credits),
        }
        if intro_start_ticks is not None:
            payload["IntroStartTicks"] = int(intro_start_ticks)
        if intro_end_ticks is not None:
            payload["IntroEndTicks"] = int(intro_end_ticks)
        if credits_start_ticks is not None:
            payload["CreditsStartTicks"] = int(credits_start_ticks)
        return self._item_post(
            "update-chapters",
            item_id,
            "/Chapters",
            payload,
            expected_authorization_revision=expected_authorization_revision,
        )

    def _item_post(
        self,
        operation: str,
        item_id: str,
        suffix: str,
        payload: Mapping[str, Any],
        *,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult:
        settings = self._settings()
        if (
            expected_authorization_revision is not None
            and emby_authorization_revision(settings) != expected_authorization_revision
        ):
            raise BridgeAuthorizationChanged(operation)
        base_url, api_key = self._configuration(settings)
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            raise ValueError("Emby item id is required")
        return self._post(
            operation,
            f"{base_url}/Items/{quote(normalized_item_id, safe='')}/ETKMediaInfo{suffix}",
            headers={"X-Emby-Token": api_key, "Accept": "application/json"},
            payload=payload,
        )

    def _post(
        self,
        operation: str,
        url: str,
        *,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
    ) -> BridgeWriteResult:
        try:
            with self._client_factory() as client:
                response = client.post(
                    url,
                    headers=dict(headers or {}),
                    params=dict(params or {}),
                    json=dict(payload),
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise BridgeWriteIndeterminate(operation, f"Bridge write outcome is unknown: {exc}") from exc
        if not response.is_success:
            raise BridgeWriteRejected(
                operation,
                response.status_code,
                f"Bridge rejected the write with HTTP {response.status_code}: {response.text[:500]}",
            )
        if not response.content:
            response_payload: Mapping[str, Any] = {}
        else:
            try:
                decoded = response.json()
            except ValueError as exc:
                raise BridgeWriteIndeterminate(operation, "Bridge write succeeded but returned invalid JSON") from exc
            if not isinstance(decoded, Mapping):
                raise BridgeWriteIndeterminate(operation, "Bridge write succeeded but returned a non-object response")
            response_payload = dict(decoded)
        return BridgeWriteResult(operation, response.status_code, response_payload)

    def _configuration(self, settings: Settings | None = None) -> tuple[str, str]:
        settings = settings or self._settings()
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        api_key = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise ValueError("Emby server URL and API key are required")
        return base_url, api_key

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        timeout = max(1.0, float(settings.emby_api_timeout))
        return httpx.Client(proxy=proxy or None, timeout=timeout)


class PluginUpdateError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PluginArtifact:
    chunks: Iterator[bytes]
    content_length: str | None = None


class HttpxPluginUpdateGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def download(self) -> PluginArtifact:
        client = self._client_factory()
        try:
            request = client.build_request(
                "GET",
                PLUGIN_UPDATE_URL,
                headers={"User-Agent": "EmbyToolKit-PluginUpdater"},
            )
            response = client.send(request, stream=True)
            response.raise_for_status()
        except (httpx.HTTPError, ValueError) as exc:
            client.close()
            raise PluginUpdateError("plugin_update_download_failed") from exc

        def chunks() -> Iterator[bytes]:
            try:
                yield from response.iter_bytes(chunk_size=128 * 1024)
            finally:
                response.close()
                client.close()

        return PluginArtifact(chunks(), response.headers.get("Content-Length"))

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(300.0, connect=15.0),
            follow_redirects=True,
        )


def _normalize_mediainfo_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        raw = json.loads(raw)
    while isinstance(raw, list):
        raw = next((item for item in raw if isinstance(item, (dict, list))), None)
        if raw is None:
            break
    if not isinstance(raw, Mapping):
        raise ValueError("ETK media information is not a JSON object")
    source: Any = raw.get("MediaSourceInfo")
    while isinstance(source, list):
        source = next((item for item in source if isinstance(item, Mapping)), None)
    media_sources = raw.get("MediaSources")
    media_streams = raw.get("MediaStreams")
    if source is None and isinstance(media_sources, list):
        source = next((item for item in media_sources if isinstance(item, Mapping)), None)
    if source is None and isinstance(media_streams, list):
        source = raw
    if not isinstance(source, Mapping) or not isinstance(source.get("MediaStreams"), list):
        raise ValueError("ETK media information contains no MediaSourceInfo.MediaStreams")
    chapters = raw.get("Chapters")
    return {
        "MediaSourceInfo": dict(source),
        "Chapters": list(chapters) if isinstance(chapters, list) else [],
    }


def emby_authorization_revision(settings: Settings) -> str:
    # Keep one authorization identity contract across persistent Identity and Media work.
    from etk.domains.identity.post_authorization import identity_authorization_revision

    return identity_authorization_revision(settings)


__all__ = [
    "BridgeAuthorizationChanged",
    "BridgeWriteError",
    "BridgeWriteIndeterminate",
    "BridgeWriteOutcome",
    "BridgeWriteRejected",
    "BridgeWriteResult",
    "HttpxEmbyBridgeClient",
    "HttpxPluginUpdateGateway",
    "PLUGIN_UPDATE_URL",
    "PluginArtifact",
    "PluginUpdateError",
    "emby_authorization_revision",
]
