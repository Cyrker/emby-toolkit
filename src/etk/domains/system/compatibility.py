from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from etk.compatibility.legacy_registry import get_legacy_emby_server_id


class LegacyConfigurationGateway:
    def preview_etk_url(self, config: Mapping[str, Any]) -> str:
        from handler.emby_discovery import resolve_etk_service_url

        return resolve_etk_service_url(dict(config))

    def preview_proxy_url(self, config: Mapping[str, Any]) -> str:
        from handler.emby_discovery import resolve_proxy_discovery_url

        return resolve_proxy_discovery_url(dict(config))

    def validate_internal_url(self, value: str) -> str:
        from handler.emby_discovery import validate_internal_service_url

        return validate_internal_service_url(value)

    def valid_library_ids(self, config: Mapping[str, Any]) -> set[str] | None:
        from handler import emby

        url = str(config.get("emby_server_url") or "")
        token = str(config.get("emby_api_key") or "")
        user_id = str(config.get("emby_user_id") or "")
        if not url or not token or not user_id:
            return None
        libraries = emby.get_emby_libraries(url, token, user_id)
        if libraries is None:
            return None
        return {str(item["Id"]) for item in libraries if item.get("Id")}

    def server_id(self) -> str | None:
        return get_legacy_emby_server_id()

    def sync_plugin_origin(self, config: Mapping[str, Any], etk_url: str) -> bool:
        from handler import emby

        url = str(config.get("emby_server_url") or "")
        token = str(config.get("emby_api_key") or "")
        if not etk_url or not url or not token:
            return True
        return bool(emby.configure_etk_plugin_origin(url, token, etk_url))

    def sync_plugin_discovery(self, config: Mapping[str, Any], discovery_url: str) -> bool:
        from handler import emby

        url = str(config.get("emby_server_url") or "")
        token = str(config.get("emby_api_key") or "")
        if not url or not token:
            return True
        return bool(emby.configure_etk_plugin_discovery_url(url, token, discovery_url))
