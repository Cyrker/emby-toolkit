from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings

from .playback import PlaybackReportError, PlaybackReportErrorKind


class HttpxPlaybackReportingAdapter:
    """Playback Reporting query adapter that never places credentials in URLs or errors."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def report(self, *, user_id: str | None, days: int) -> Sequence[Mapping[str, Any]]:
        base_url, headers = self._authorization()
        api_url = (
            f"{base_url}/user_usage_stats/UserPlaylist"
            if "/emby" in base_url.casefold()
            else f"{base_url}/emby/user_usage_stats/UserPlaylist"
        )
        params = {
            "user_id": str(user_id or ""),
            "days": int(days),
            "aggregate_data": "true",
            "include_stats": "true",
            "limit": 100000,
        }
        try:
            with self._client_factory() as client:
                response = client.get(api_url, headers=headers, params=params)
                if response.status_code == 404:
                    raise PlaybackReportError(PlaybackReportErrorKind.PLUGIN_NOT_INSTALLED)
                if response.status_code != 200:
                    raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE)
                payload: Any = response.json()
        except PlaybackReportError:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE) from exc
        if not isinstance(payload, list):
            raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE)
        return tuple(dict(item) for item in payload if isinstance(item, Mapping))

    def item_details(
        self,
        *,
        user_id: str,
        item_ids: Sequence[str],
    ) -> Sequence[Mapping[str, Any]]:
        normalized_ids = tuple(dict.fromkeys(str(value).strip() for value in item_ids if str(value).strip()))[:20]
        if not normalized_ids:
            return ()
        base_url, headers = self._authorization()
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items",
                    headers=headers,
                    params={
                        "Ids": ",".join(normalized_ids),
                        "Fields": "SeriesName,ParentIndexNumber,IndexNumber,Name",
                    },
                )
                if response.status_code != 200:
                    raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE)
                payload: Any = response.json()
        except PlaybackReportError:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE) from exc
        items = payload.get("Items") if isinstance(payload, Mapping) else payload
        if not isinstance(items, list):
            raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE)
        return tuple(dict(item) for item in items if isinstance(item, Mapping))

    def _authorization(self) -> tuple[str, dict[str, str]]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        api_key = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise PlaybackReportError(PlaybackReportErrorKind.UNAVAILABLE)
        return base_url, {"X-Emby-Token": api_key, "Accept": "application/json"}

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=30)


__all__ = ["HttpxPlaybackReportingAdapter"]
