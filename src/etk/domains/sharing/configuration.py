from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from typing import Any, Protocol

import httpx

from etk.integrations.http_policy import (
    ExternalAdapterError,
    HttpTransportPolicy,
    ProviderDisposition,
    classify_status,
    decode_json_object,
)
from etk.platform import Settings

SHARED_RESOURCE_CONFIG_KEY = "shared_resource_config"
EMBY_SERVER_ID_CACHE_KEY = "emby_server_id_cache"

_DEFAULT_HOME_SECTIONS: tuple[dict[str, Any], ...] = (
    {
        "key": "latest",
        "title": "最新资源",
        "display_type": "all",
        "order_by": "pool_time",
        "limit": 10,
        "enabled": True,
    },
    {
        "key": "popular",
        "title": "热门共享",
        "display_type": "all",
        "order_by": "popular",
        "limit": 10,
        "enabled": True,
    },
    {
        "key": "movies",
        "title": "电影",
        "display_type": "movie",
        "order_by": "pool_time",
        "limit": 10,
        "enabled": True,
    },
    {
        "key": "series",
        "title": "剧集",
        "display_type": "tv",
        "order_by": "pool_time",
        "limit": 10,
        "enabled": True,
    },
)

_DEFAULT_CONFIG: dict[str, Any] = {
    "p115_shared_resource_enabled": False,
    "p115_shared_center_url": "https://shared.55565576.xyz",
    "p115_shared_resource_mode": "rapid",
    "p115_shared_disable_episode_transfer": False,
    "p115_shared_block_clean_version_transfer": False,
    "p115_shared_block_short_drama_transfer": False,
    "p115_shared_intro_enabled": False,
    "p115_shared_auto_share_requests_enabled": False,
    "p115_shared_virtual_import_enabled": False,
    "p115_shared_virtual_auto_promote_episodes": 0,
    "p115_shared_virtual_auto_promote_movie_percent": 0,
    "p115_shared_center_home_sections": [dict(section) for section in _DEFAULT_HOME_SECTIONS],
}
_BOOLEAN_FIELDS = (
    "p115_shared_resource_enabled",
    "p115_shared_disable_episode_transfer",
    "p115_shared_block_clean_version_transfer",
    "p115_shared_block_short_drama_transfer",
    "p115_shared_intro_enabled",
    "p115_shared_auto_share_requests_enabled",
    "p115_shared_virtual_import_enabled",
)


class AppSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...


class SharingConfigurationService:
    """Canonical native owner for the standalone Shared Resource settings."""

    def __init__(self, settings: AppSettingsPort) -> None:
        self._settings = settings

    def load(self) -> dict[str, Any]:
        current = self._settings.load(SHARED_RESOURCE_CONFIG_KEY)
        payload = {**_DEFAULT_CONFIG, **(dict(current) if isinstance(current, Mapping) else {})}
        return _normalize_config(payload)

    def save(self, incoming: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(incoming, Mapping):
            raise TypeError("shared resource config must be an object")
        # The UI submits partial updates in a few flows. Merge against the
        # canonical stored snapshot so an omitted option is not reset to its
        # default by a later save.
        payload = _normalize_config({**self.load(), **dict(incoming)})
        self._settings.save(SHARED_RESOURCE_CONFIG_KEY, payload)
        return payload

    def enable(self) -> dict[str, Any]:
        payload = self.load()
        payload["p115_shared_resource_enabled"] = True
        self._settings.save(SHARED_RESOURCE_CONFIG_KEY, payload)
        return payload


class SharingServerIdentity:
    """Resolve and cache the native Emby Server ID without the legacy registry."""

    def __init__(
        self,
        settings: Callable[[], Settings | Mapping[str, Any]] | Settings | Mapping[str, Any],
        app_settings: AppSettingsPort,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        policy: HttpTransportPolicy | None = None,
    ) -> None:
        self._settings = settings if callable(settings) else lambda: settings
        self._app_settings = app_settings
        self._client_factory = client_factory or self._default_client
        self._policy = policy or HttpTransportPolicy(system="emby_server_identity")

    def server_id_hash(self) -> str:
        return hashlib.sha256(self.server_id().encode("utf-8")).hexdigest()

    def server_id(self) -> str:
        cached = _server_id(self._app_settings.load(EMBY_SERVER_ID_CACHE_KEY))
        if cached:
            return cached

        settings = self._settings()
        base_url = str(_setting(settings, "emby_server_url", default="") or "").strip().rstrip("/")
        token = _secret(_setting(settings, "emby_api_key", default=""))
        if not base_url or not token:
            raise ExternalAdapterError(
                "Emby ServerID is unavailable",
                disposition=ProviderDisposition.REJECTED,
                system="emby_server_identity",
                operation="configuration",
                status_code=400,
            )
        if not base_url.startswith(("http://", "https://")):
            raise ExternalAdapterError(
                "Emby Server URL must use HTTP or HTTPS",
                disposition=ProviderDisposition.REJECTED,
                system="emby_server_identity",
                operation="configuration",
                status_code=400,
            )

        response = self._policy.request(
            lambda: self._request(base_url, token),
            operation="system_info",
            retryable=True,
        )
        if not 200 <= response.status_code < 300:
            raise ExternalAdapterError(
                "Emby System/Info request failed",
                disposition=classify_status(response.status_code, mutation=False),
                system="emby_server_identity",
                operation="system_info",
                status_code=response.status_code,
            )
        payload = decode_json_object(response, system="emby_server_identity", operation="system_info")
        server_id = _server_id(payload)
        if not server_id:
            raise ExternalAdapterError(
                "Emby System/Info did not provide a ServerID",
                disposition=ProviderDisposition.RETRYABLE,
                system="emby_server_identity",
                operation="system_info",
                status_code=response.status_code,
            )
        self._app_settings.save(EMBY_SERVER_ID_CACHE_KEY, server_id)
        return server_id

    def _request(self, base_url: str, token: str) -> httpx.Response:
        with self._client_factory() as client:
            return client.request(
                "GET",
                f"{base_url}/System/Info",
                headers={"Accept": "application/json", "X-Emby-Token": token},
            )

    def _default_client(self) -> httpx.Client:
        timeout = _setting(self._settings(), "emby_api_timeout", default=60)
        try:
            bounded = max(1.0, min(float(timeout), 120.0))
        except (TypeError, ValueError):
            bounded = 60.0
        return httpx.Client(timeout=bounded, follow_redirects=False)


def _normalize_config(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(value)
    for key in _BOOLEAN_FIELDS:
        payload[key] = _boolish(payload.get(key), bool(_DEFAULT_CONFIG[key]))
    payload["p115_shared_center_url"] = str(
        payload.get("p115_shared_center_url") or _DEFAULT_CONFIG["p115_shared_center_url"]
    ).rstrip("/")
    payload["p115_shared_resource_mode"] = "rapid"
    payload.pop("p115_shared_max_active_shares", None)
    payload["p115_shared_virtual_auto_promote_episodes"] = max(
        0,
        _integer(payload.get("p115_shared_virtual_auto_promote_episodes"), 0),
    )
    payload["p115_shared_virtual_auto_promote_movie_percent"] = max(
        0,
        min(100, _integer(payload.get("p115_shared_virtual_auto_promote_movie_percent"), 0)),
    )
    payload["p115_shared_center_home_sections"] = _home_sections(payload.get("p115_shared_center_home_sections"))
    return payload


def _boolish(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().casefold()
    if text in {"1", "true", "yes", "y", "on", "启用", "开启", "是"}:
        return True
    if text in {"0", "false", "no", "n", "off", "停用", "关闭", "否"}:
        return False
    return default


def _integer(value: object, default: int) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError, OverflowError):
        return default


def _home_sections(value: object) -> list[dict[str, Any]]:
    raw = value if isinstance(value, list) else list(_DEFAULT_HOME_SECTIONS)
    result: list[dict[str, Any]] = []
    for index, item in enumerate(raw[:20]):
        if not isinstance(item, Mapping):
            continue
        key = str(item.get("key") or f"custom_{index + 1}").strip()[:64] or f"custom_{index + 1}"
        title = str(item.get("title") or key).strip()[:40] or key
        display_type = str(item.get("display_type") or item.get("item_type") or "all").strip().casefold()
        if display_type not in {"all", "movie", "tv", "series", "season", "pack"}:
            display_type = "all"
        order_by = str(item.get("order_by") or "latest").strip().casefold()
        if order_by == "latest":
            order_by = "pool_time"
        if order_by not in {"pool_time", "release_year", "popular", "size", "name"}:
            order_by = "pool_time"
        tags = item.get("tags")
        candidates = tags if isinstance(tags, list) else str(item.get("status") or "").split(",")
        normalized_tags: list[str] = []
        for tag in candidates:
            text = str(tag or "").strip()
            if re.fullmatch(r"[A-Za-z0-9_:-]{1,40}", text) and text not in normalized_tags:
                normalized_tags.append(text)
        result.append(
            {
                "key": key,
                "title": title,
                "display_type": display_type,
                "order_by": order_by,
                "status": ",".join(["alive", "available", *normalized_tags]),
                "genre_id": str(item.get("genre_id") or "").strip()[:40],
                "tags": normalized_tags,
                "limit": max(1, min(20, _integer(item.get("limit"), 10))),
                "enabled": _boolish(item.get("enabled"), True),
            }
        )
    return result or [dict(section) for section in _DEFAULT_HOME_SECTIONS]


def _server_id(value: object) -> str:
    if isinstance(value, Mapping):
        value = value.get("Id") or value.get("ServerId") or value.get("server_id")
    return str(value or "").strip()


def _setting(source: Any, key: str, *, default: Any) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _secret(value: object) -> str:
    getter = getattr(value, "get_secret_value", None)
    return str(getter() if callable(getter) else value or "").strip()


__all__ = [
    "EMBY_SERVER_ID_CACHE_KEY",
    "SHARED_RESOURCE_CONFIG_KEY",
    "SharingConfigurationService",
    "SharingServerIdentity",
]
