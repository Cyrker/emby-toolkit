from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from copy import deepcopy
from typing import Any, Protocol

from pydantic import BaseModel, RootModel

from etk.platform import Settings

from .models import SystemResult

logger = logging.getLogger(__name__)

MASKED_SECRET = "********"
SENSITIVE_CONFIG_KEYS = frozenset({*Settings.secret_keys, "pro_license_key"})
_DERIVED_SUFFIXES = ("_auto_detected", "_auto_detected_error", "_effective")
_EMBY_AUTHORIZATION_KEYS = (
    "emby_server_url",
    "emby_api_key",
    "emby_user_id",
    "emby_auth_mode",
)
_RESPONSE_ONLY_KEYS = {"custom_theme", "emby_server_id", "pro_license_key"}


class ConfigurationUpdateRequest(RootModel[dict[str, Any]]):
    pass


class ConfigurationResponse(RootModel[dict[str, Any]]):
    pass


class ConfigurationSavedResponse(BaseModel):
    message: str


class AppSettingsRepository(Protocol):
    def load(self, key: str) -> Any: ...


class ConfigurationWriter(Protocol):
    def save(self, updates: Mapping[str, Any]) -> Settings: ...


class ConfigurationGateway(Protocol):
    def preview_etk_url(self, config: Mapping[str, Any]) -> str: ...

    def preview_proxy_url(self, config: Mapping[str, Any]) -> str: ...

    def validate_internal_url(self, value: str) -> str: ...

    def valid_library_ids(self, config: Mapping[str, Any]) -> set[str] | None: ...

    def server_id(self) -> str | None: ...

    def sync_plugin_origin(self, config: Mapping[str, Any], etk_url: str) -> bool: ...

    def sync_plugin_discovery(self, config: Mapping[str, Any], discovery_url: str) -> bool: ...


class ConfigurationService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        repository: AppSettingsRepository,
        writer: ConfigurationWriter,
        gateway: ConfigurationGateway,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._writer = writer
        self._gateway = gateway

    def is_configured(self) -> bool:
        settings = self._settings()
        return bool(settings.emby_server_url.strip() and settings.emby_api_key.get_secret_value().strip())

    def read(self) -> SystemResult:
        try:
            payload = self._settings().to_runtime_dict()
            for key in SENSITIVE_CONFIG_KEYS:
                if payload.get(key):
                    payload[key] = MASKED_SECRET
            self._add_discovery_previews(payload)
            payload["emby_server_id"] = self._gateway.server_id()
            payload["custom_theme"] = self._repository.load("custom_theme") or {}
            payload["pro_license_key"] = MASKED_SECRET if self._repository.load("pro_license_key") else ""
            return SystemResult(payload)
        except Exception:
            logger.exception("Reading system configuration failed")
            return SystemResult({"error": "获取配置信息时发生服务器内部错误"}, 500)

    def save(self, incoming: Mapping[str, Any]) -> SystemResult:
        if not incoming:
            return SystemResult({"error": "请求体中未包含配置数据"}, 400)
        settings = self._settings()
        current = settings.to_runtime_dict()
        updates = deepcopy(dict(incoming))
        for key in SENSITIVE_CONFIG_KEYS:
            if updates.get(key) == MASKED_SECRET:
                updates[key] = current.get(key, "")
        for key in tuple(updates):
            if key.endswith(_DERIVED_SUFFIXES) or key in _RESPONSE_ONLY_KEYS:
                updates.pop(key, None)
        for key in _EMBY_AUTHORIZATION_KEYS:
            updates[key] = current.get(key, "")
        update_keys = set(updates)

        user_id = str(updates.get("emby_user_id") or "").strip()
        if not user_id:
            return SystemResult({"error": "Emby User ID 不能为空！"}, 400)
        if re.fullmatch(r"[a-f0-9]{32}", user_id, re.IGNORECASE) is None:
            return SystemResult({"error": "Emby User ID 格式不正确！"}, 400)

        prepared = {**current, **updates}
        manual_etk_url = str(prepared.get("etk_server_url_manual") or "").strip().rstrip("/")
        try:
            prepared["etk_server_url"] = (
                self._gateway.validate_internal_url(manual_etk_url)
                if manual_etk_url
                else self._gateway.preview_etk_url(prepared)
            )
            update_keys.add("etk_server_url")
        except (TypeError, ValueError, RuntimeError) as exc:
            prefix = "手动 ETK 地址无效" if manual_etk_url else "无法自动获取 ETK 内网地址"
            return SystemResult({"error": f"{prefix}：{exc}"}, 400)

        valid_library_ids = self._gateway.valid_library_ids(prepared)
        if valid_library_ids is not None:
            for key in ("libraries_to_process", "proxy_native_view_selection"):
                value = prepared.get(key)
                if isinstance(value, list):
                    prepared[key] = [item for item in value if item in valid_library_ids]

        lock_discovery = bool(prepared.get("proxy_enabled") and prepared.get("proxy_lock_discovery_address"))
        prepared["proxy_lock_discovery_address"] = lock_discovery
        update_keys.add("proxy_lock_discovery_address")
        discovery_url = ""
        if lock_discovery:
            manual_discovery_url = str(prepared.get("proxy_discovery_url_manual") or "").strip()
            try:
                discovery_url = (
                    self._gateway.validate_internal_url(manual_discovery_url)
                    if manual_discovery_url
                    else self._gateway.preview_proxy_url(prepared)
                )
            except (TypeError, ValueError, RuntimeError) as exc:
                return SystemResult({"error": f"无法锁定 Emby 反代地址：{exc}"}, 400)

        allowed_keys = {*Settings.dynamic_keys(), *(settings.__pydantic_extra__ or {})}
        updates = {key: prepared[key] for key in update_keys if key in allowed_keys}
        try:
            self._writer.save(updates)
        except Exception as exc:
            logger.exception("Saving system configuration failed")
            return SystemResult({"error": f"保存配置时发生服务器内部错误: {exc}"}, 500)

        try:
            if not self._gateway.sync_plugin_origin(
                prepared,
                str(prepared.get("etk_server_url") or ""),
            ):
                logger.warning("Configuration saved but ETK plugin origin synchronization failed")
        except Exception:
            logger.warning("Configuration saved but ETK plugin origin synchronization failed", exc_info=True)
        try:
            if not self._gateway.sync_plugin_discovery(prepared, discovery_url):
                logger.warning("Configuration saved but proxy discovery synchronization failed")
        except Exception:
            logger.warning("Configuration saved but proxy discovery synchronization failed", exc_info=True)
        return SystemResult({"message": "配置已成功保存并自动净化！"})

    def _add_discovery_previews(self, payload: dict[str, Any]) -> None:
        for key, resolver in (
            ("etk_server_url_auto_detected", self._gateway.preview_etk_url),
            ("proxy_discovery_url_auto_detected", self._gateway.preview_proxy_url),
        ):
            try:
                payload[key] = resolver(payload)
                payload[f"{key}_error"] = ""
            except (TypeError, ValueError, RuntimeError) as exc:
                payload[key] = ""
                payload[f"{key}_error"] = str(exc)
        payload["etk_server_url_effective"] = (
            str(payload.get("etk_server_url_manual") or "").strip() or payload["etk_server_url_auto_detected"]
        )
        payload["proxy_discovery_url_effective"] = (
            str(payload.get("proxy_discovery_url_manual") or "").strip() or payload["proxy_discovery_url_auto_detected"]
            if payload.get("proxy_lock_discovery_address")
            else ""
        )
