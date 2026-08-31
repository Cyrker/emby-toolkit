from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

import httpx

from etk.platform import Settings
from etk.platform.config.locking import (
    DYNAMIC_CONFIG_LOCK_NAMESPACE,
    P115_AUTH_LOCK_KEY,
)
from etk.platform.jobs import PersistentEffectIndeterminate

from .auth_directory import P115CredentialStore
from .maintenance import P115OperationProgress
from .maintenance_transport import (
    P115PrimaryOperationsGateway,
    item_is_directory,
    item_name,
    provider_succeeded,
)

_SCHEMA_VERSION = 1
_DYNAMIC_CONFIG_SETTING = "dynamic_app_config"
_PLUGIN_NAME = "ETK MediaInfo Bridge"
_ROOT_NAMES = ("ETK媒体库", "ETK待整理", "ETK未识别")
_CATEGORY_CHILDREN = {
    "动漫": ("国漫", "日番", "美漫"),
    "电影": ("国产片", "日韩片", "欧美片"),
    "剧集": ("国产剧", "日韩剧", "欧美剧"),
}
_LIBRARY_DEFINITIONS = (
    ("国漫", "tvshows"),
    ("日番", "tvshows"),
    ("美漫", "tvshows"),
    ("国产片", "movies"),
    ("日韩片", "movies"),
    ("欧美片", "movies"),
    ("国产剧", "tvshows"),
    ("日韩剧", "tvshows"),
    ("欧美剧", "tvshows"),
)


class QuickDeployAppSettings(Protocol):
    def revision(self, key: str) -> str: ...

    def merge_mapping_if_revision(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        expected_revision: str,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]: ...


class QuickDeployLocalConfigurationRepository(Protocol):
    def save_bundle(
        self,
        *,
        rename_config: Mapping[str, Any],
        sorting_rules: Sequence[Mapping[str, Any]],
        account_id: str,
        washing_config: Mapping[str, Any],
        washing_groups: Sequence[object],
    ) -> None: ...


class QuickDeployDirectoryClient(Protocol):
    def list_children(self, parent_id: str) -> Sequence[Mapping[str, Any]]: ...

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]: ...


class P115QuickDeployLock:
    def __init__(self, connection_factory: Callable[[], AbstractContextManager[Any]]) -> None:
        self._connection_factory = connection_factory

    @contextmanager
    def hold(self):
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_lock(%s, %s)",
                    (DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY),
                )
                connection.commit()
                try:
                    yield
                finally:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s, %s)",
                        (DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY),
                    )
                    connection.commit()


class P115QuickDeployIntentService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore,
        app_settings: QuickDeployAppSettings,
        priority: Callable[[], object],
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._app_settings = app_settings
        self._priority = priority

    def parameters(self) -> dict[str, Any]:
        settings = self._settings()
        snapshot = self._credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        if not account_id.isdigit():
            raise ValueError("无法确认稳定的 115 账号身份，请刷新授权状态或重新授权后再试")
        backend = _select_backend(snapshot, self._priority())
        if not backend:
            raise ValueError("115 客户端未初始化，请检查 Token/Cookie 配置")
        _validate_settings(settings)
        return {
            "account_id": account_id,
            "backend": backend,
            "credential_fingerprint": _credential_fingerprint(snapshot, backend),
            "desired_state_hash": quick_deploy_desired_state_hash(settings, account_id),
            "dynamic_config_revision": self._app_settings.revision(_DYNAMIC_CONFIG_SETTING),
            "emby_server_url": _emby_server_url(settings),
            "emby_credential_fingerprint": _emby_credential_fingerprint(settings),
        }


class P115QuickDeployOperation:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore,
        provider: P115PrimaryOperationsGateway,
        account_lock: P115QuickDeployLock,
        app_settings: QuickDeployAppSettings,
        local_configuration: QuickDeployLocalConfigurationRepository,
        emby: HttpxEmbyQuickDeployGateway,
        local_mirror: P115LocalMirror | None = None,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._provider = provider
        self._account_lock = account_lock
        self._app_settings = app_settings
        self._local_configuration = local_configuration
        self._emby = emby
        self._local_mirror = local_mirror or P115LocalMirror()

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: P115OperationProgress,
    ) -> Mapping[str, Any]:
        account_id = str(parameters.get("account_id") or "").strip()
        backend = str(parameters.get("backend") or "").strip().lower()
        credential_fingerprint = str(parameters.get("credential_fingerprint") or "").strip()
        desired_hash = str(parameters.get("desired_state_hash") or "").strip()
        dynamic_revision = str(parameters.get("dynamic_config_revision") or "")
        emby_server_url = str(parameters.get("emby_server_url") or "").strip().rstrip("/")
        emby_credential_fingerprint = str(parameters.get("emby_credential_fingerprint") or "").strip()
        if (
            not account_id.isdigit()
            or backend not in {"openapi", "cookie"}
            or not credential_fingerprint
            or not desired_hash
            or not emby_server_url
            or not emby_credential_fingerprint
        ):
            raise ValueError("quick-deploy provider identity is invalid")

        with self._account_lock.hold():
            settings = self._assert_target(
                account_id,
                backend,
                credential_fingerprint,
                desired_hash,
                dynamic_revision,
                emby_server_url,
                emby_credential_fingerprint,
            )
            progress.checkpoint()
            self._emby.preflight(
                expected_server_url=emby_server_url,
                expected_credential_fingerprint=emby_credential_fingerprint,
            )
            client = self._provider.open(
                expected_account_id=account_id,
                expected_backend=backend,
            )
            if client is None:
                raise RuntimeError("115 客户端未初始化")

            progress.update(5, "正在创建 115 一级目录")
            media_root = self._ensure_directory(client, "0", "ETK媒体库", progress)
            save_root = self._ensure_directory(client, "0", "ETK待整理", progress)
            unrecognized_root = self._ensure_directory(client, "0", "ETK未识别", progress)
            category_dirs: dict[str, dict[str, Any]] = {}
            for parent_name in _CATEGORY_CHILDREN:
                category_dirs[parent_name] = self._ensure_directory(
                    client,
                    media_root["cid"],
                    parent_name,
                    progress,
                )

            progress.update(15, "正在创建 115 二级分类目录")
            for parent_name, child_names in _CATEGORY_CHILDREN.items():
                parent = category_dirs[parent_name]
                parent["children"] = []
                parent["path"] = parent_name
                for child_name in child_names:
                    child = self._ensure_directory(client, parent["cid"], child_name, progress)
                    child["path"] = f"{parent_name}/{child_name}"
                    parent["children"].append(child)
                    category_dirs[child_name] = child

            settings = self._assert_target(
                account_id,
                backend,
                credential_fingerprint,
                desired_hash,
                dynamic_revision,
                emby_server_url,
                emby_credential_fingerprint,
            )
            progress.update(30, "正在创建本地 STRM 镜像目录")
            local_dirs = self._local_mirror.create(settings.local_strm_root, category_dirs)

            progress.update(45, "正在写入 115 基础配置")
            rename_config = _rename_config()
            sorting_rules = _sorting_rules(category_dirs)
            washing_groups = _washing_groups(category_dirs)
            self._local_configuration.save_bundle(
                rename_config=rename_config,
                sorting_rules=sorting_rules,
                account_id=account_id,
                washing_config={"conflict_mode": "replace"},
                washing_groups=washing_groups,
            )

            settings = self._assert_target(
                account_id,
                backend,
                credential_fingerprint,
                desired_hash,
                dynamic_revision,
                emby_server_url,
                emby_credential_fingerprint,
            )
            progress.update(72, "正在创建 Emby 媒体库")
            emby_libraries: list[dict[str, Any]] = []
            for index, (name, collection_type) in enumerate(_LIBRARY_DEFINITIONS, start=1):
                progress.checkpoint()
                self._assert_target(
                    account_id,
                    backend,
                    credential_fingerprint,
                    desired_hash,
                    dynamic_revision,
                    emby_server_url,
                    emby_credential_fingerprint,
                )
                label = f"quick-deploy:emby-library:{name}"
                progress.record_started_effect(label)
                try:
                    library = self._emby.ensure_library(
                        name=name,
                        collection_type=collection_type,
                        path=str(category_dirs[name]["local_path"]),
                        expected_server_url=emby_server_url,
                        expected_credential_fingerprint=emby_credential_fingerprint,
                    )
                except EmbyQuickDeployRejected:
                    progress.record_abandoned_effect(label)
                    raise
                except EmbyQuickDeployIndeterminate as exc:
                    raise PersistentEffectIndeterminate(str(exc)) from exc
                progress.record_committed_effect(label)
                emby_libraries.append(dict(library))
                progress.update(72 + int(index / len(_LIBRARY_DEFINITIONS) * 20), f"已确认 Emby 媒体库：{name}")

            library_ids = [str(item.get("id") or "") for item in emby_libraries if item.get("id")]
            if len(emby_libraries) != 9 or len(library_ids) != 9 or len(set(library_ids)) != 9:
                raise RuntimeError("Emby 媒体库未完整收敛到 9 个唯一资源")

            settings = self._assert_target(
                account_id,
                backend,
                credential_fingerprint,
                desired_hash,
                dynamic_revision,
                emby_server_url,
                emby_credential_fingerprint,
            )
            monitor_paths = list(dict.fromkeys(os.path.normpath(path) for path in settings.monitor_paths if path))
            normalized_root = os.path.normpath(settings.local_strm_root)
            if normalized_root not in monitor_paths:
                monitor_paths.append(normalized_root)
            dynamic_updates = {
                "p115_save_path_cid": save_root["cid"],
                "p115_save_path_name": save_root["name"],
                "p115_unrecognized_cid": unrecognized_root["cid"],
                "p115_unrecognized_name": unrecognized_root["name"],
                "p115_media_root_cid": media_root["cid"],
                "p115_media_root_name": media_root["name"],
                "p115_mp_classify": False,
                "monitor_enabled": True,
                "libraries_to_process": library_ids,
                "monitor_paths": monitor_paths,
            }
            self._app_settings.merge_mapping_if_revision(
                _DYNAMIC_CONFIG_SETTING,
                dynamic_updates,
                expected_revision=dynamic_revision,
            )
            tree = {
                "media_root": {
                    **media_root,
                    "children": [category_dirs["动漫"], category_dirs["电影"], category_dirs["剧集"]],
                },
                "save_root": save_root,
                "unrecognized_root": unrecognized_root,
            }
            progress.update(100, "一键部署完成")
            return {
                "config": dict(dynamic_updates),
                "tree": tree,
                "local_dirs": local_dirs,
                "emby_libraries": emby_libraries,
                "sorting_rules_count": len(sorting_rules),
                "washing_groups_count": len(washing_groups),
                "rename_config": rename_config,
            }

    def _assert_target(
        self,
        account_id: str,
        backend: str,
        credential_fingerprint: str,
        desired_hash: str,
        dynamic_revision: str,
        emby_server_url: str,
        emby_credential_fingerprint: str,
    ) -> Settings:
        settings = self._settings()
        _validate_settings(settings)
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            raise RuntimeError("一键部署绑定的 115 账号在执行期间发生变化")
        if _credential_fingerprint(snapshot, backend) != credential_fingerprint:
            raise RuntimeError("一键部署绑定的 115 provider credential 在执行期间发生变化")
        if quick_deploy_desired_state_hash(settings, account_id) != desired_hash:
            raise RuntimeError("一键部署目标配置在任务开始前发生变化，请重新确认部署")
        if self._app_settings.revision(_DYNAMIC_CONFIG_SETTING) != dynamic_revision:
            raise RuntimeError("一键部署目标配置在任务执行期间发生变化，请重新确认部署")
        if (
            _emby_server_url(settings) != emby_server_url
            or _emby_credential_fingerprint(settings) != emby_credential_fingerprint
        ):
            raise RuntimeError("一键部署绑定的 Emby endpoint 或 credential 在执行期间发生变化")
        return settings

    @staticmethod
    def _ensure_directory(
        client: QuickDeployDirectoryClient,
        parent_id: str,
        name: str,
        progress: P115OperationProgress,
    ) -> dict[str, Any]:
        existing = _unique_directory(client.list_children(parent_id), name)
        if existing is not None:
            return _directory_projection(existing, name=name, created=False)

        label = f"quick-deploy:p115-directory:{parent_id}:{name}"
        progress.record_started_effect(label)
        try:
            response = client.create_directory(parent_id, name)
        except Exception as exc:
            try:
                confirmed = _unique_directory(client.list_children(parent_id), name)
            except Exception as readback_exc:
                raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}") from readback_exc
            if confirmed is None:
                raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}") from exc
            progress.record_committed_effect(label)
            return _directory_projection(confirmed, name=name, created=True)
        if not provider_succeeded(response):
            progress.record_abandoned_effect(label)
            raise RuntimeError(str(response.get("error_msg") or response.get("message") or f"创建目录失败：{name}"))
        try:
            confirmed = _unique_directory(client.list_children(parent_id), name)
        except Exception as exc:
            raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}") from exc
        if confirmed is None:
            raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}")
        progress.record_committed_effect(label)
        return _directory_projection(confirmed, name=name, created=True)


class P115LocalMirror:
    def create(
        self,
        local_root: str,
        category_dirs: Mapping[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        root = Path(str(local_root or "")).expanduser().resolve(strict=False)
        if not str(local_root or "").strip():
            raise RuntimeError("请先在 Emby 配置页填写本地 STRM 根目录")
        local_dirs: list[dict[str, Any]] = []
        ordered_names = tuple(_CATEGORY_CHILDREN) + tuple(
            name for names in _CATEGORY_CHILDREN.values() for name in names
        )
        for name in ordered_names:
            relative = str(category_dirs[name]["path"])
            parts = tuple(part for part in relative.replace("\\", "/").split("/") if part)
            if not parts or any(part in {".", ".."} for part in parts):
                raise RuntimeError(f"STRM 镜像目录路径无效：{relative}")
            target = (root.joinpath(*parts)).resolve(strict=False)
            try:
                target.relative_to(root)
            except ValueError as exc:
                raise RuntimeError(f"STRM 镜像目录越过配置根目录：{relative}") from exc
            target.mkdir(parents=True, exist_ok=True)
            confirmed = target.resolve(strict=True)
            try:
                confirmed.relative_to(root)
            except ValueError as exc:
                raise RuntimeError(f"STRM 镜像目录通过符号链接越过配置根目录：{relative}") from exc
            item = {"name": name, "path": str(confirmed), "category_path": relative}
            local_dirs.append(item)
            if name not in _CATEGORY_CHILDREN:
                category_dirs[name]["local_path"] = str(confirmed)
        return local_dirs


class EmbyQuickDeployRejected(RuntimeError):
    pass


class EmbyQuickDeployIndeterminate(RuntimeError):
    pass


class HttpxEmbyQuickDeployGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        reconcile_timeout_seconds: float = 10.0,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._clock = clock
        self._sleeper = sleeper
        self._reconcile_timeout_seconds = max(0.1, float(reconcile_timeout_seconds))

    def preflight(
        self,
        *,
        expected_server_url: str | None = None,
        expected_credential_fingerprint: str | None = None,
    ) -> None:
        server_url, credential_fingerprint = self._expected_identity(
            expected_server_url,
            expected_credential_fingerprint,
        )
        response = self._request(
            "GET",
            "/emby/System/Info",
            expected_server_url=server_url,
            expected_credential_fingerprint=credential_fingerprint,
        )
        if response.status_code == 404:
            response = self._request(
                "GET",
                "/System/Info",
                expected_server_url=server_url,
                expected_credential_fingerprint=credential_fingerprint,
            )
        response.raise_for_status()
        plugins = self._request(
            "GET",
            "/Plugins",
            expected_server_url=server_url,
            expected_credential_fingerprint=credential_fingerprint,
        )
        plugins.raise_for_status()
        payload = plugins.json()
        if not isinstance(payload, list) or not any(
            isinstance(item, Mapping) and str(item.get("Name") or "").strip() == _PLUGIN_NAME for item in payload
        ):
            raise RuntimeError("一键部署前请先安装 ETK MediaInfo Bridge 插件")

    def ensure_library(
        self,
        *,
        name: str,
        collection_type: str,
        path: str,
        expected_server_url: str | None = None,
        expected_credential_fingerprint: str | None = None,
    ) -> Mapping[str, Any]:
        server_url, credential_fingerprint = self._expected_identity(
            expected_server_url,
            expected_credential_fingerprint,
        )
        normalized_name = str(name or "").strip()
        normalized_type = str(collection_type or "").strip().lower()
        normalized_path = os.path.normpath(str(path or "").strip())
        folders = self._list_libraries(server_url, credential_fingerprint)
        existing = _find_exact_library(folders, normalized_name, normalized_type, normalized_path)
        created = existing is None
        if existing is None:
            payload = {
                "Name": normalized_name,
                "CollectionType": normalized_type,
                "Paths": [normalized_path],
                "RefreshLibrary": True,
                "LibraryOptions": _library_options(normalized_type),
            }
            try:
                response = self._request(
                    "POST",
                    "/Library/VirtualFolders",
                    json=payload,
                    expected_server_url=server_url,
                    expected_credential_fingerprint=credential_fingerprint,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                response = None
                post_error: Exception | None = exc
            else:
                post_error = None
                if response.status_code >= 500:
                    post_error = EmbyQuickDeployIndeterminate(
                        f"Emby 创建媒体库返回 HTTP {response.status_code}: {normalized_name}"
                    )
                elif response.status_code >= 400:
                    raise EmbyQuickDeployRejected(
                        f"Emby 拒绝创建媒体库，HTTP {response.status_code}: {normalized_name}"
                    )
            existing = self._poll_library(
                normalized_name,
                normalized_type,
                normalized_path,
                server_url,
                credential_fingerprint,
            )
            if existing is None:
                raise EmbyQuickDeployIndeterminate(
                    f"Emby 创建媒体库结果无法确认：{normalized_name} -> {normalized_path}"
                ) from post_error

        folder_id = _library_id(existing)
        if not folder_id:
            raise EmbyQuickDeployIndeterminate(f"Emby 媒体库缺少稳定 ID：{normalized_name}")
        desired_options = _apply_library_options(existing.get("LibraryOptions"), normalized_type)
        if desired_options != existing.get("LibraryOptions"):
            try:
                response = self._request(
                    "POST",
                    "/Library/VirtualFolders/LibraryOptions",
                    json={"Id": folder_id, "LibraryOptions": desired_options},
                    expected_server_url=server_url,
                    expected_credential_fingerprint=credential_fingerprint,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                options_error: Exception | None = exc
            else:
                options_error = None
                if response.status_code >= 500:
                    options_error = EmbyQuickDeployIndeterminate(
                        f"Emby 媒体库选项写入结果不确定，HTTP {response.status_code}: {normalized_name}"
                    )
                elif response.status_code >= 400:
                    raise EmbyQuickDeployRejected(
                        f"Emby 拒绝媒体库选项，HTTP {response.status_code}: {normalized_name}"
                    )
            confirmed = self._poll_library(
                normalized_name,
                normalized_type,
                normalized_path,
                server_url,
                credential_fingerprint,
            )
            if confirmed is None or _apply_library_options(
                confirmed.get("LibraryOptions"), normalized_type
            ) != confirmed.get("LibraryOptions"):
                raise EmbyQuickDeployIndeterminate(f"Emby 媒体库选项结果无法确认：{normalized_name}") from options_error
            existing = confirmed
        return {
            "name": normalized_name,
            "path": normalized_path,
            "id": folder_id,
            "created": created,
        }

    def _poll_library(
        self,
        name: str,
        collection_type: str,
        path: str,
        expected_server_url: str,
        expected_credential_fingerprint: str,
    ) -> Mapping[str, Any] | None:
        deadline = self._clock() + self._reconcile_timeout_seconds
        while True:
            try:
                folder = _find_exact_library(
                    self._list_libraries(expected_server_url, expected_credential_fingerprint),
                    name,
                    collection_type,
                    path,
                )
            except (httpx.HTTPError, ValueError):
                folder = None
            if folder is not None:
                return folder
            remaining = deadline - self._clock()
            if remaining <= 0:
                return None
            self._sleeper(min(0.2, remaining))

    def _list_libraries(
        self,
        expected_server_url: str,
        expected_credential_fingerprint: str,
    ) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            "/Library/VirtualFolders",
            expected_server_url=expected_server_url,
            expected_credential_fingerprint=expected_credential_fingerprint,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or any(not isinstance(item, Mapping) for item in payload):
            raise ValueError("Emby virtual-folder response is invalid")
        return [dict(item) for item in payload]

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: object = None,
        expected_server_url: str,
        expected_credential_fingerprint: str,
    ) -> httpx.Response:
        settings = self._settings()
        self._assert_identity(settings, expected_server_url, expected_credential_fingerprint)
        base_url = _emby_server_url(settings)
        api_key = settings.emby_api_key.get_secret_value().strip()
        try:
            with self._client_factory() as client:
                return client.request(
                    method,
                    f"{base_url}{path}",
                    headers={"X-Emby-Token": api_key, "Accept": "application/json"},
                    params={"api_key": api_key},
                    json=json,
                )
        finally:
            self._assert_identity(
                self._settings(),
                expected_server_url,
                expected_credential_fingerprint,
            )

    def _expected_identity(
        self,
        expected_server_url: str | None,
        expected_credential_fingerprint: str | None,
    ) -> tuple[str, str]:
        settings = self._settings()
        _validate_settings(settings)
        server_url = str(expected_server_url or _emby_server_url(settings)).strip().rstrip("/")
        credential_fingerprint = str(expected_credential_fingerprint or _emby_credential_fingerprint(settings)).strip()
        self._assert_identity(settings, server_url, credential_fingerprint)
        return server_url, credential_fingerprint

    @staticmethod
    def _assert_identity(
        settings: Settings,
        expected_server_url: str,
        expected_credential_fingerprint: str,
    ) -> None:
        if (
            _emby_server_url(settings) != expected_server_url
            or _emby_credential_fingerprint(settings) != expected_credential_fingerprint
        ):
            raise RuntimeError("quick-deploy Emby endpoint or credential changed during provider execution")

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


def quick_deploy_desired_state_hash(settings: Settings, account_id: str) -> str:
    desired = {
        "schema_version": _SCHEMA_VERSION,
        "p115_account_id": str(account_id),
        "emby_server_url": settings.emby_server_url.strip().rstrip("/"),
        "local_strm_root": os.path.normpath(settings.local_strm_root),
        "etk_server_url": settings.etk_server_url.strip().rstrip("/"),
        "p115_roots": list(_ROOT_NAMES),
        "emby_libraries": [list(item) for item in _LIBRARY_DEFINITIONS],
    }
    encoded = json.dumps(desired, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_settings(settings: Settings) -> None:
    missing: list[str] = []
    if not settings.emby_server_url.strip():
        missing.append("Emby URL")
    if not settings.emby_api_key.get_secret_value().strip():
        missing.append("Emby API Key")
    if settings.emby_auth_mode != "user_token" or not settings.emby_user_id.strip():
        missing.append("Emby 服务授权")
    if not settings.local_strm_root.strip():
        missing.append("本地 STRM 根目录")
    etk_server_url = settings.etk_server_url.strip()
    if not etk_server_url:
        missing.append("STRM 链接地址")
    elif not etk_server_url.startswith(("http://", "https://")):
        raise RuntimeError("STRM 链接地址必须以 http:// 或 https:// 开头")
    if missing:
        raise RuntimeError("一键部署前请先配置：" + "、".join(missing))


def _select_backend(snapshot: Any, priority: object) -> str:
    preferred = str(priority or "openapi").strip().lower()
    order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
    for backend in order:
        value = snapshot.cookie if backend == "cookie" else snapshot.access_token
        if str(value or "").strip():
            return backend
    return ""


def _credential_fingerprint(snapshot: Any, backend: str) -> str:
    credential = snapshot.access_token if backend == "openapi" else snapshot.cookie
    normalized = str(credential or "").strip()
    if not normalized:
        return ""
    payload = {
        "account_id": str(snapshot.account_id or "").strip(),
        "app_type": str(snapshot.app_type or "").strip() if backend == "cookie" else "",
        "backend": backend,
        "credential": normalized,
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _emby_server_url(settings: Settings) -> str:
    return settings.emby_server_url.strip().rstrip("/")


def _emby_credential_fingerprint(settings: Settings) -> str:
    payload = {
        "api_key": settings.emby_api_key.get_secret_value().strip(),
        "auth_mode": settings.emby_auth_mode.strip(),
        "user_id": settings.emby_user_id.strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _unique_directory(items: Sequence[Mapping[str, Any]], name: str) -> Mapping[str, Any] | None:
    matches = [item for item in items if item_name(item) == name]
    directories = [item for item in matches if item_is_directory(item)]
    if len(matches) != len(directories) or len(directories) > 1:
        raise RuntimeError(f"115 目录名冲突：{name}")
    return directories[0] if directories else None


def _directory_projection(value: Mapping[str, Any], *, name: str, created: bool) -> dict[str, Any]:
    cid = str(value.get("cid") or value.get("fid") or value.get("file_id") or value.get("id") or "").strip()
    if not cid:
        raise RuntimeError(f"115 目录缺少稳定 CID：{name}")
    return {"cid": cid, "name": name, "created": created}


def _rename_config() -> dict[str, Any]:
    return {
        "keep_original_name": False,
        "main_dir_template": "{{title}}{% if year %} ({{year}}){% endif %}{% if tmdbid %} {tmdb={{tmdbid}}}{% endif %}",
        "season_dir_template": "Season {{season_no}}",
        "movie_file_template": "{{title}}{{fileExt}}",
        "tv_file_template": "{{title}}{% if season_episode %} {{season_episode}}{% endif %}{{fileExt}}",
        "file_template": "{{title}}{% if season_episode %} {{season_episode}}{% endif %}{{fileExt}}",
        "main_dir_format": ["title_zh", "sep_space", "year", "sep_space", "tmdb_bracket"],
        "season_dir_format": ["season_name_en"],
        "file_format": ["title_zh", "sep_space", "s_e"],
        "file_tmdb_fmt": "none",
        "video_codec_style": "hevc",
        "hide_audio_channels": False,
        "strm_url_fmt": "standard",
    }


def _sorting_rules(category_dirs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    definitions = (
        ("国漫", "tv", {"genres": [16], "languages": ["国语", "粤语"]}),
        ("日番", "tv", {"genres": [16], "languages": ["日语"]}),
        ("美漫", "tv", {"genres": [16]}),
        ("国产片", "movie", {"languages": ["国语", "粤语"]}),
        ("日韩片", "movie", {"languages": ["日语", "韩语"]}),
        ("欧美片", "movie", {}),
        ("国产剧", "tv", {"languages": ["国语", "粤语"]}),
        ("日韩剧", "tv", {"languages": ["日语", "韩语"]}),
        ("欧美剧", "tv", {}),
    )
    rules: list[dict[str, Any]] = []
    for index, (name, media_type, extra) in enumerate(definitions, start=1):
        folder = category_dirs[name]
        rules.append(
            {
                "id": f"p115_quick_{index}",
                "name": name,
                "cid": folder["cid"],
                "dir_name": name,
                "category_path": folder["path"],
                "enabled": True,
                "match_mode": "and",
                "media_type": media_type,
                "genres": extra.get("genres", []),
                "countries": [],
                "languages": extra.get("languages", []),
                "studios": [],
                "keywords": [],
                "ratings": [],
                "file_extensions": [],
                "actors": [],
                "watching_status": "all",
                "year_min": None,
                "year_max": None,
                "runtime_min": None,
                "runtime_max": None,
                "min_rating": 0,
            }
        )
    return rules


def _washing_groups(category_dirs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    definitions = (
        ("国漫", "Series"),
        ("日番", "Series"),
        ("美漫", "Series"),
        ("国产片", "Movie"),
        ("日韩片", "Movie"),
        ("欧美片", "Movie"),
        ("国产剧", "Series"),
        ("日韩剧", "Series"),
        ("欧美剧", "Series"),
    )
    priorities = (
        {
            "resolution": ["4k"],
            "codec": [],
            "effect": [],
            "audio": [],
            "subtitle": [],
            "subtitle_effect": False,
            "clean_version": False,
            "min_size_gb": None,
            "max_size_gb": None,
            "is_exclude": False,
        },
        {
            "resolution": ["1080p"],
            "codec": [],
            "effect": [],
            "audio": [],
            "subtitle": [],
            "subtitle_effect": False,
            "clean_version": False,
            "min_size_gb": None,
            "max_size_gb": None,
            "is_exclude": False,
        },
    )
    return [
        {
            "id": index,
            "name": f"{name}基础洗版",
            "media_type": media_type,
            "target_cids": [category_dirs[name]["cid"]],
            "priorities": deepcopy(priorities),
        }
        for index, (name, media_type) in enumerate(definitions, start=1)
    ]


def _library_id(folder: Mapping[str, Any]) -> str:
    return str(folder.get("ItemId") or folder.get("Id") or "").strip()


def _library_paths(folder: Mapping[str, Any]) -> set[str]:
    return {os.path.normpath(str(path)) for path in folder.get("Locations") or [] if str(path or "").strip()}


def _find_exact_library(
    folders: Sequence[Mapping[str, Any]],
    name: str,
    collection_type: str,
    path: str,
) -> Mapping[str, Any] | None:
    matches: list[Mapping[str, Any]] = []
    for folder in folders:
        same_name = str(folder.get("Name") or "").strip() == name
        same_path = path in _library_paths(folder)
        if not same_name and not same_path:
            continue
        same_type = str(folder.get("CollectionType") or "mixed").strip().lower() == collection_type
        if same_name and same_path and same_type:
            matches.append(folder)
            continue
        raise EmbyQuickDeployRejected(
            f"Emby 媒体库冲突：name={name!r}, path={path!r}, CollectionType={collection_type!r}"
        )
    if len(matches) > 1:
        raise EmbyQuickDeployRejected(f"Emby 中存在多个完全匹配的媒体库：{name}")
    return matches[0] if matches else None


def _library_options(collection_type: str) -> dict[str, Any]:
    item_types = {
        "movies": ("Movie", "BoxSet"),
        "tvshows": ("Series", "Season", "Episode"),
        "boxsets": ("BoxSet",),
        "mixed": ("Movie", "BoxSet", "Series", "Season", "Episode"),
    }.get(collection_type)
    if item_types is None:
        raise ValueError(f"不支持的 Emby 媒体库类型：{collection_type}")
    return {
        "EnableRealtimeMonitor": False,
        "SaveLocalMetadata": False,
        "MetadataSavers": [],
        "DisabledLocalMetadataReaders": ["Nfo"],
        "LocalMetadataReaderOrder": [],
        "DisabledSubtitleFetchers": [],
        "DisabledLyricsFetchers": [],
        "SaveLyricsWithMedia": False,
        "TypeOptions": [
            {
                "Type": item_type,
                "MetadataFetchers": ["ETK Metadata"],
                "MetadataFetcherOrder": ["ETK Metadata"],
                "ImageFetchers": ["ETK Images"],
                "ImageFetcherOrder": ["ETK Images"],
            }
            for item_type in item_types
        ],
    }


def _apply_library_options(value: object, collection_type: str) -> dict[str, Any]:
    current = dict(value) if isinstance(value, Mapping) else {}
    desired = _library_options(collection_type)
    updated = dict(current)
    for key, item in desired.items():
        updated[key] = deepcopy(item)
    return updated


__all__ = [
    "EmbyQuickDeployIndeterminate",
    "EmbyQuickDeployRejected",
    "HttpxEmbyQuickDeployGateway",
    "P115LocalMirror",
    "P115QuickDeployIntentService",
    "P115QuickDeployLock",
    "P115QuickDeployOperation",
    "quick_deploy_desired_state_hash",
]
