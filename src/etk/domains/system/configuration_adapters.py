from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol
from urllib.parse import quote

import httpx

from etk.domains.media.bridge_client import BridgeWriteError, BridgeWriteResult, HttpxEmbyBridgeClient
from etk.platform import Settings
from etk.platform.internal_urls import derive_internal_service_url, validate_internal_service_url


class _SettingsReader(Protocol):
    def load(self, key: str) -> Any: ...


class _BridgeClient(Protocol):
    def configure_origin(self, etk_url: str) -> BridgeWriteResult: ...

    def configure_discovery_url(self, discovery_url: str) -> BridgeWriteResult: ...


class HttpxConfigurationGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        repository: _SettingsReader,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        bridge: _BridgeClient | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._client_factory = client_factory or self._default_client
        self._bridge = bridge or HttpxEmbyBridgeClient(settings)

    def preview_etk_url(self, config: Mapping[str, Any]) -> str:
        canonical = str(config.get("etk_server_url") or "").strip()
        try:
            return validate_internal_service_url(canonical)
        except ValueError:
            return derive_internal_service_url(str(config.get("emby_server_url") or ""), 5257)

    def preview_proxy_url(self, config: Mapping[str, Any]) -> str:
        if not config.get("proxy_enabled"):
            raise ValueError("请先启用虚拟库反向代理")
        return derive_internal_service_url(
            str(config.get("emby_server_url") or ""),
            int(config.get("proxy_port") or 0),
        )

    @staticmethod
    def validate_internal_url(value: str) -> str:
        return validate_internal_service_url(value)

    def valid_library_ids(self, config: Mapping[str, Any]) -> set[str] | None:
        url = str(config.get("emby_server_url") or "").strip().rstrip("/")
        token = str(config.get("emby_api_key") or "").strip()
        user_id = str(config.get("emby_user_id") or "").strip()
        if not url or not token or not user_id:
            return None
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{url}/emby/Users/{quote(user_id, safe='')}/Views",
                    headers={"X-Emby-Token": token, "Accept": "application/json"},
                    params={"api_key": token},
                )
            response.raise_for_status()
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        items = payload.get("Items") if isinstance(payload, Mapping) else None
        if not isinstance(items, list):
            return None
        return {str(item["Id"]) for item in items if isinstance(item, Mapping) and str(item.get("Id") or "").strip()}

    def server_id(self) -> str | None:
        value = str(self._repository.load("emby_server_id_cache") or "").strip()
        return value or None

    def sync_plugin_origin(self, config: Mapping[str, Any], etk_url: str) -> bool:
        if not etk_url or not config.get("emby_server_url") or not config.get("emby_api_key"):
            return True
        try:
            return self._bridge.configure_origin(etk_url).confirmed
        except (BridgeWriteError, ValueError):
            return False

    def sync_plugin_discovery(self, config: Mapping[str, Any], discovery_url: str) -> bool:
        if not config.get("emby_server_url") or not config.get("emby_api_key"):
            return True
        try:
            return self._bridge.configure_discovery_url(discovery_url).confirmed
        except (BridgeWriteError, ValueError):
            return False

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        return httpx.Client(
            timeout=max(1.0, float(settings.emby_api_timeout)),
            trust_env=False,
        )


__all__ = ["HttpxConfigurationGateway"]
