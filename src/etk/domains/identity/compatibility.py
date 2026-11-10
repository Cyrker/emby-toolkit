from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.platform import AppContainer, Settings


class LegacyEmbyIdentityGateway:
    """Temporary P3 adapter to existing Emby/plugin clients; P5 replaces it."""

    def authenticate(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        device_id: str,
    ) -> Mapping[str, Any] | None:
        from handler import emby

        return emby.authenticate_emby_user(
            username,
            password,
            base_url=base_url,
            device_id=device_id,
        )

    def get_user(self, base_url: str, access_token: str, user_id: str) -> Mapping[str, Any] | None:
        from handler import emby

        return emby.get_user_info_from_server(base_url, access_token, user_id)

    def test_connection(self, base_url: str, access_token: str) -> Mapping[str, Any]:
        from handler import emby

        return emby.test_connection(base_url, access_token)

    def install_plugin(self, authorization: Mapping[str, Any], settings: Settings) -> Mapping[str, Any]:
        from etk.platform.control_plane import ControlPlaneClient
        from handler.emby_discovery import resolve_etk_service_url, validate_internal_service_url

        manual = str(settings.etk_server_url_manual or "").strip()
        origin = (
            validate_internal_service_url(manual) if manual else resolve_etk_service_url(settings.to_runtime_dict())
        )
        result = ControlPlaneClient().install_emby_plugin(
            emby_url=str(authorization["_normalized_url"]),
            emby_token=str(authorization["AccessToken"]),
            etk_origin=origin,
        )
        plugin_install = result.get("plugin_install")
        if not isinstance(plugin_install, Mapping):
            raise RuntimeError("control-plane response is missing plugin_install")
        return dict(plugin_install)

    def configure_libraries(
        self,
        authorization: Mapping[str, Any],
        plugin_install: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if not plugin_install.get("ok"):
            return {
                "ok": False,
                "status": "skipped",
                "message": "embytool Bridge 插件未就绪，已跳过媒体库加速配置",
            }
        from handler import emby

        return emby.configure_etk_library_defaults(
            str(authorization["_normalized_url"]),
            str(authorization["AccessToken"]),
        )


class RuntimeConfigurationWriter:
    """Validate, persist, reload, and synchronize one process configuration snapshot."""

    def __init__(
        self,
        container: AppContainer,
        supervisor: RuntimeSupervisor,
    ) -> None:
        self._container = container
        self._supervisor = supervisor

    def save(self, updates: Mapping[str, Any]) -> Settings:
        validated = self._container.settings.with_updates(updates)
        self._supervisor.save_config_and_reload(dict(updates))
        runtime_config = self._supervisor.config
        refreshed = validated.with_updates(runtime_config) if runtime_config else validated
        self._container.settings = refreshed
        return refreshed
