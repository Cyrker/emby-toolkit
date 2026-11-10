from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from urllib.parse import quote, urlsplit

import httpx

from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.platform import AppContainer, Settings
from etk.platform.config import BOOTSTRAP_KEYS
from etk.platform.control_plane import ControlPlaneClient
from etk.platform.internal_urls import derive_internal_service_url, validate_internal_service_url

logger = logging.getLogger(__name__)

_ETK_LIBRARY_ITEM_TYPES = {
    "movies": {"Movie", "BoxSet"},
    "tvshows": {"Series", "Season", "Episode"},
    "boxsets": {"BoxSet"},
    "mixed": {"Movie", "BoxSet", "Series", "Season", "Episode"},
}


class _PluginInstaller(Protocol):
    def install_emby_plugin(
        self,
        *,
        emby_url: str,
        emby_token: str,
        etk_origin: str,
        request_id: str | None = None,
    ) -> dict[str, Any]: ...


class _ConfigurationPersistence(Protocol):
    def merge_and_save(self, settings: Settings, updates: Mapping[str, Any]) -> Settings: ...


class HttpxEmbyIdentityGateway:
    """Native Emby authorization and post-authorization adapter."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        control_plane_factory: Callable[[], _PluginInstaller] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        reconcile_timeout_seconds: float = 10.0,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._control_plane_factory = control_plane_factory or (lambda: ControlPlaneClient(track_legacy_effects=False))
        self._clock = clock
        self._sleeper = sleeper
        self._reconcile_timeout_seconds = max(0.0, float(reconcile_timeout_seconds))

    def authenticate(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        device_id: str,
    ) -> Mapping[str, Any] | None:
        try:
            url = _http_base_url(base_url)
            headers = {
                "Content-Type": "application/json",
                "X-Emby-Authorization": (
                    f'Emby Client="Emby Web", Device="Chrome", DeviceId="{device_id}", Version="4.8.0.80"'
                ),
            }
            with self._client_factory() as client:
                response = client.post(
                    f"{url}/Users/AuthenticateByName",
                    headers=headers,
                    json={"Username": str(username), "LoginType": "Manual", "Pw": str(password or "")},
                )
            if response.status_code != 200:
                return None
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        if not isinstance(payload, Mapping) or not payload.get("AccessToken") or not payload.get("User"):
            return None
        return dict(payload)

    def get_user(self, base_url: str, access_token: str, user_id: str) -> Mapping[str, Any] | None:
        try:
            url = _http_base_url(base_url)
            with self._client_factory() as client:
                response = client.get(
                    f"{url}/Users/{quote(str(user_id or '').strip(), safe='')}",
                    headers={"X-Emby-Token": str(access_token)},
                )
            if response.status_code != 200:
                return None
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return None
        return dict(payload) if isinstance(payload, Mapping) else None

    def test_connection(self, base_url: str, access_token: str) -> Mapping[str, Any]:
        if not str(base_url or "").strip() or not str(access_token or "").strip():
            return {"success": False, "error": "URL 或 API Key 为空"}
        try:
            url = _http_base_url(base_url)
            with self._client_factory() as client:
                response = client.get(
                    f"{url}/emby/System/Info",
                    params={"api_key": str(access_token)},
                )
        except httpx.ConnectTimeout:
            return {"success": False, "error": "连接超时"}
        except httpx.ConnectError:
            return {"success": False, "error": "无法连接到服务器，请检查 URL 或网络"}
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            return {"success": False, "error": str(exc)}
        if response.status_code == 200:
            return {"success": True}
        if response.status_code == 401:
            return {"success": False, "error": "API Key 无效或无权限"}
        if response.status_code == 404:
            return {"success": False, "error": "找不到 Emby 服务器 (404)，请检查 URL"}
        return {"success": False, "error": f"连接失败 (HTTP {response.status_code})"}

    def install_plugin(
        self,
        authorization: Mapping[str, Any],
        settings: Settings,
        request_id: str,
    ) -> Mapping[str, Any]:
        manual = str(settings.etk_server_url_manual or "").strip()
        canonical = str(settings.etk_server_url or "").strip()
        if manual:
            origin = validate_internal_service_url(manual)
        else:
            try:
                origin = validate_internal_service_url(canonical)
            except ValueError:
                origin = derive_internal_service_url(str(authorization["_normalized_url"]), 5257)
        result = self._control_plane_factory().install_emby_plugin(
            emby_url=str(authorization["_normalized_url"]),
            emby_token=str(authorization["AccessToken"]),
            etk_origin=origin,
            request_id=str(request_id),
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
                "certainty": "confirmed-no-effect",
                "message": "embytool Bridge 插件未就绪，已跳过媒体库加速配置",
            }
        base_url = _http_base_url(str(authorization.get("_normalized_url") or ""))
        access_token = str(authorization.get("AccessToken") or "").strip()
        if not access_token:
            return {
                "ok": False,
                "updated": 0,
                "certainty": "confirmed-no-effect",
                "message": "Emby 服务授权缺少 AccessToken",
            }
        try:
            with self._client_factory() as client:
                folders = self._list_libraries(client, base_url, access_token)
                return self._configure_libraries(client, base_url, access_token, folders)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            return {
                "ok": False,
                "updated": 0,
                "certainty": "confirmed-no-effect",
                "message": str(exc),
            }

    def _configure_libraries(
        self,
        client: httpx.Client,
        base_url: str,
        access_token: str,
        folders: list[dict[str, Any]],
    ) -> Mapping[str, Any]:
        updated = 0
        unchanged = 0
        failures: list[str] = []
        unknown = False
        for folder in folders:
            item_types = _library_item_types(folder)
            if not item_types:
                continue
            folder_id = str(folder.get("Id") or folder.get("ItemId") or "").strip()
            name = str(folder.get("Name") or folder_id or "未知媒体库")
            current = folder.get("LibraryOptions")
            if _library_defaults_confirmed(current, item_types):
                unchanged += 1
                continue
            if not folder_id:
                failures.append(f"{name}: 缺少媒体库 ID")
                continue
            desired = _apply_library_defaults(current, item_types)
            request_uncertain = False
            try:
                response = client.post(
                    f"{base_url}/Library/VirtualFolders/LibraryOptions",
                    headers={"X-Emby-Token": access_token, "Accept": "application/json"},
                    params={"api_key": access_token},
                    json={"Id": folder_id, "LibraryOptions": desired},
                )
                request_uncertain = response.status_code >= 500
                rejected = 400 <= response.status_code < 500
            except (httpx.TimeoutException, httpx.TransportError):
                request_uncertain = True
                rejected = False
            confirmed = self._poll_library_defaults(
                client,
                base_url,
                access_token,
                folder_id,
                item_types,
            )
            if confirmed:
                updated += 1
                continue
            if request_uncertain or not rejected:
                unknown = True
                failures.append(f"{name}: 媒体库配置结果无法确认")
            else:
                failures.append(f"{name}: Emby 拒绝媒体库配置")

        configured = updated + unchanged
        if failures:
            certainty = "unknown" if unknown else ("observed-complete" if updated else "confirmed-no-effect")
            return {
                "ok": False,
                "updated": updated,
                "configured": configured,
                "failed": len(failures),
                "certainty": certainty,
                "message": f"已配置 {configured} 个媒体库，{len(failures)} 个失败",
                "errors": failures,
            }
        return {
            "ok": True,
            "updated": updated,
            "configured": configured,
            "failed": 0,
            "certainty": "observed-complete",
            "message": f"已为 {configured} 个媒体库应用 embytool 加速配置",
        }

    def _poll_library_defaults(
        self,
        client: httpx.Client,
        base_url: str,
        access_token: str,
        folder_id: str,
        item_types: set[str],
    ) -> bool:
        deadline = self._clock() + self._reconcile_timeout_seconds
        while True:
            try:
                folders = self._list_libraries(client, base_url, access_token)
                matches = [
                    folder
                    for folder in folders
                    if str(folder.get("Id") or folder.get("ItemId") or "").strip() == folder_id
                ]
                if len(matches) > 1:
                    raise RuntimeError("Emby 媒体库 ID readback 存在歧义")
                if matches and _library_defaults_confirmed(matches[0].get("LibraryOptions"), item_types):
                    return True
            except (httpx.HTTPError, TypeError, ValueError, RuntimeError):
                pass
            remaining = deadline - self._clock()
            if remaining <= 0:
                return False
            self._sleeper(min(0.2, remaining))

    @staticmethod
    def _list_libraries(
        client: httpx.Client,
        base_url: str,
        access_token: str,
    ) -> list[dict[str, Any]]:
        response = client.get(
            f"{base_url}/Library/VirtualFolders",
            headers={"X-Emby-Token": access_token, "Accept": "application/json"},
            params={"api_key": access_token},
        )
        response.raise_for_status()
        payload: Any = response.json()
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise ValueError("Emby 媒体库 readback 返回无效数据")
        return [dict(item) for item in payload]

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        return httpx.Client(
            timeout=max(1.0, float(settings.emby_api_timeout)),
            trust_env=False,
        )


class RuntimeConfigurationWriter:
    """Validate, persist, reload, and synchronize one process configuration snapshot."""

    def __init__(
        self,
        container: AppContainer,
        supervisor: RuntimeSupervisor,
        *,
        persistence: _ConfigurationPersistence | None = None,
    ) -> None:
        self._container = container
        self._supervisor = supervisor
        self._persistence = persistence
        self._lock = threading.Lock()

    def save(self, updates: Mapping[str, Any]) -> Settings:
        bootstrap_updates = sorted(set(updates) & set(BOOTSTRAP_KEYS))
        if bootstrap_updates:
            raise ValueError(f"bootstrap settings require explicit INI persistence: {', '.join(bootstrap_updates)}")

        with self._lock:
            validated = self._container.settings.with_updates(updates)
            if self._persistence is None:
                self._supervisor.save_config_and_reload(dict(updates))
                runtime_config = self._supervisor.config
                refreshed = validated.with_updates(runtime_config) if runtime_config else validated
            else:
                persisted = self._persistence.merge_and_save(validated, updates)
                try:
                    self._supervisor.save_config_and_reload(persisted.to_runtime_dict())
                except Exception:
                    logger.exception("Runtime reload failed after configuration was persisted")
                    refreshed = persisted
                else:
                    runtime_config = self._supervisor.config
                    refreshed = persisted.with_updates(runtime_config) if runtime_config else persisted
            self._container.settings = refreshed
            return refreshed


def _http_base_url(value: str) -> str:
    normalized = str(value or "").strip().rstrip("/")
    parsed = urlsplit(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Emby URL 必须以 http:// 或 https:// 开头")
    return normalized


def _library_item_types(folder: Mapping[str, Any]) -> set[str]:
    collection_type = str(folder.get("CollectionType") or "mixed").strip().lower() or "mixed"
    return set(_ETK_LIBRARY_ITEM_TYPES.get(collection_type, set()))


def _apply_library_defaults(value: object, item_types: set[str]) -> dict[str, Any]:
    updated = dict(value) if isinstance(value, Mapping) else {}
    updated["EnableRealtimeMonitor"] = False
    updated["SaveLocalMetadata"] = False
    updated["MetadataSavers"] = []
    disabled_readers = list(updated.get("DisabledLocalMetadataReaders") or [])
    if "Nfo" not in disabled_readers:
        disabled_readers.append("Nfo")
    updated["DisabledLocalMetadataReaders"] = disabled_readers
    updated["LocalMetadataReaderOrder"] = [
        item for item in (updated.get("LocalMetadataReaderOrder") or []) if item != "Nfo"
    ]
    type_options: list[dict[str, Any]] = []
    configured: set[str] = set()
    for raw in updated.get("TypeOptions") or []:
        option = dict(raw) if isinstance(raw, Mapping) else {}
        item_type = str(option.get("Type") or "")
        if item_type in item_types:
            configured.add(item_type)
            option.update(
                {
                    "MetadataFetchers": ["ETK Metadata"],
                    "MetadataFetcherOrder": ["ETK Metadata"],
                    "ImageFetchers": ["ETK Images"],
                    "ImageFetcherOrder": ["ETK Images"],
                }
            )
        type_options.append(option)
    for item_type in sorted(item_types - configured):
        type_options.append(
            {
                "Type": item_type,
                "MetadataFetchers": ["ETK Metadata"],
                "MetadataFetcherOrder": ["ETK Metadata"],
                "ImageFetchers": ["ETK Images"],
                "ImageFetcherOrder": ["ETK Images"],
            }
        )
    updated["TypeOptions"] = type_options
    return updated


def _library_defaults_confirmed(value: object, item_types: set[str]) -> bool:
    if not isinstance(value, Mapping):
        return False
    if value.get("EnableRealtimeMonitor") is not False:
        return False
    if value.get("SaveLocalMetadata") is not False or list(value.get("MetadataSavers") or []) != []:
        return False
    if "Nfo" not in set(value.get("DisabledLocalMetadataReaders") or []):
        return False
    if "Nfo" in set(value.get("LocalMetadataReaderOrder") or []):
        return False
    raw_options = value.get("TypeOptions")
    if not isinstance(raw_options, list):
        return False
    for item_type in item_types:
        matches = [
            option
            for option in raw_options
            if isinstance(option, Mapping) and str(option.get("Type") or "") == item_type
        ]
        if len(matches) != 1:
            return False
        option = matches[0]
        if any(
            list(option.get(key) or []) != expected
            for key, expected in (
                ("MetadataFetchers", ["ETK Metadata"]),
                ("MetadataFetcherOrder", ["ETK Metadata"]),
                ("ImageFetchers", ["ETK Images"]),
                ("ImageFetcherOrder", ["ETK Images"]),
            )
        ):
            return False
    return True


__all__ = ["HttpxEmbyIdentityGateway", "RuntimeConfigurationWriter"]
