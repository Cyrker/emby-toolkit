from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlparse

from etk.platform.jobs import PersistentEffectIndeterminate

from .auth_directory import P115CredentialStore
from .maintenance_transport import (
    item_is_directory,
    item_name,
    provider_succeeded,
    response_id,
)
from .media_info_adapters import P115DirectUrl
from .models import P115Result

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_DELETE_BATCH_SIZE = 500


class P115MaintenanceFacts(Protocol):
    def speedtest_sample(self, *, account_id: str) -> dict[str, Any] | None: ...

    def cached_unrecognized_file_ids(
        self,
        *,
        account_id: str,
        parent_id: str,
    ) -> frozenset[str]: ...

    def delete_confirmed_unrecognized_facts(
        self,
        file_ids: Sequence[str],
        *,
        account_id: str,
        parent_id: str,
    ) -> int: ...


class P115OperationProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class P115MaintenanceClient(Protocol):
    def list_children(self, parent_id: str) -> Sequence[Mapping[str, Any]]: ...

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]: ...

    def delete_files(self, file_ids: list[str]) -> Mapping[str, Any]: ...

    def clear_recycle_bin(self) -> Mapping[str, Any]: ...

    def recycle_bin_empty(self) -> bool | None: ...


class P115MaintenanceGateway(Protocol):
    def open(
        self,
        *,
        expected_account_id: str | None = None,
        expected_backend: str | None = None,
    ) -> P115MaintenanceClient | None: ...


class P115MaintenanceDirectUrls(Protocol):
    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl | None: ...


class P115MaintenanceDownloads(Protocol):
    def measure(
        self,
        url: str,
        *,
        user_agent: str,
        max_bytes: int,
        max_seconds: int,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class P115ProviderIdentity:
    account_id: str
    backend: str


class P115MaintenanceIntentService:
    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], object],
    ) -> None:
        self._credentials = credentials
        self._priority = priority

    def provider_identity(self) -> P115ProviderIdentity:
        snapshot = self._credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        if not account_id.isdigit():
            raise ValueError("无法确认稳定的 115 账号身份")
        preferred = str(self._priority() or "openapi").strip().lower()
        order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
        for backend in order:
            credential = snapshot.cookie if backend == "cookie" else snapshot.access_token
            if str(credential or "").strip():
                return P115ProviderIdentity(account_id, backend)
        raise ValueError("115 客户端未初始化，请检查 Token/Cookie 配置")

    def mkdir_parameters(self, parent_id: object, name: object) -> dict[str, str]:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("目录名称不能为空")
        if "/" in normalized_name or "\\" in normalized_name or normalized_name in {".", ".."}:
            raise ValueError("目录名称不能包含路径分隔符")
        normalized_parent = str(parent_id or "0").strip() or "0"
        if not normalized_parent.isdigit():
            raise ValueError("115 父目录 ID 必须是数字")
        identity = self.provider_identity()
        return {
            "account_id": identity.account_id,
            "backend": identity.backend,
            "parent_id": normalized_parent,
            "name": normalized_name,
        }

    def recycle_bin_parameters(self) -> dict[str, str]:
        snapshot = self._credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        if not account_id.isdigit() or not str(snapshot.access_token or "").strip():
            raise ValueError("115 OpenAPI 客户端未初始化，无法清空回收站")
        return {"account_id": account_id, "backend": "openapi"}

    def unrecognized_parameters(self, directory_id: object) -> dict[str, str]:
        normalized_directory = str(directory_id or "").strip()
        if not normalized_directory or normalized_directory == "0":
            raise ValueError("未配置未识别目录，无法执行清空。")
        if not normalized_directory.isdigit():
            raise ValueError("未识别目录 ID 必须是数字")
        identity = self.provider_identity()
        return {
            "account_id": identity.account_id,
            "backend": identity.backend,
            "directory_id": normalized_directory,
        }


class P115MkdirOperation:
    def __init__(self, provider: P115MaintenanceGateway) -> None:
        self._provider = provider

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: P115OperationProgress,
    ) -> Mapping[str, Any]:
        account_id, backend = _provider_parameters(parameters)
        parent_id = _numeric_id(parameters.get("parent_id"), "115 parent directory ID")
        name = str(parameters.get("name") or "").strip()
        if not name:
            raise ValueError("115 directory name is required")
        client = _required_client(self._provider, account_id=account_id, backend=backend)
        progress.checkpoint()
        progress.update(10, f"正在检查 115 目录：{name}")
        existing = _find_directory(client.list_children(parent_id), name)
        if existing is not None:
            return _mkdir_result(existing, name=name, created=False)

        label = f"p115-mkdir:{account_id}:{backend}:{parent_id}:{name}"
        progress.record_started_effect(label)
        try:
            response = client.create_directory(parent_id, name)
        except Exception as exc:
            try:
                confirmed = _find_directory(client.list_children(parent_id), name)
            except Exception as readback_exc:
                raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}") from readback_exc
            if confirmed is None:
                raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}") from exc
            progress.record_committed_effect(label)
            progress.update(100, f"115 目录已确认：{name}")
            return {
                "provider_response": {
                    "state": True,
                    "cid": _item_id(confirmed),
                    "reconciled": True,
                },
                "created": True,
                "directory": dict(confirmed),
            }
        if not provider_succeeded(response):
            progress.record_abandoned_effect(label)
            raise RuntimeError(str(response.get("error_msg") or response.get("message") or "创建失败"))
        confirmed = _find_directory(client.list_children(parent_id), name)
        if confirmed is None:
            raise PersistentEffectIndeterminate(f"115 目录创建结果无法确认：{name}")
        progress.record_committed_effect(label)
        progress.update(100, f"115 目录已确认：{name}")
        result = dict(response)
        confirmed_id = _item_id(confirmed)
        if confirmed_id and not response_id(result):
            result["cid"] = confirmed_id
        result["state"] = True
        return {"provider_response": result, "created": True, "directory": dict(confirmed)}


class P115RecycleBinEmptyOperation:
    def __init__(self, provider: P115MaintenanceGateway) -> None:
        self._provider = provider

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: P115OperationProgress,
    ) -> Mapping[str, Any]:
        account_id, backend = _provider_parameters(parameters)
        client = _required_client(self._provider, account_id=account_id, backend=backend)
        progress.checkpoint()
        progress.update(10, "正在读取 115 回收站状态")
        try:
            recycle_bin_state = client.recycle_bin_empty()
        except Exception as exc:
            raise PersistentEffectIndeterminate("115 回收站状态无法确认，已拒绝清空") from exc
        if recycle_bin_state is True:
            return {"ok": True, "already_empty": True}
        if recycle_bin_state is not False:
            raise PersistentEffectIndeterminate("115 回收站状态无法确认，已拒绝清空")

        label = f"p115-recycle-bin-empty:{account_id}:{backend}"
        progress.record_started_effect(label)
        try:
            response = client.clear_recycle_bin()
        except Exception as exc:
            try:
                readback = client.recycle_bin_empty()
            except Exception as readback_exc:
                raise PersistentEffectIndeterminate("115 回收站清空结果无法确认") from readback_exc
            if readback is not True:
                raise PersistentEffectIndeterminate("115 回收站清空结果无法确认") from exc
            progress.record_committed_effect(label)
            progress.update(100, "115 回收站已确认清空")
            return {"ok": True, "already_empty": False, "provider_response": {"state": True, "reconciled": True}}
        if not provider_succeeded(response):
            progress.record_abandoned_effect(label)
            raise RuntimeError(str(response.get("error_msg") or response.get("message") or "清空失败"))
        readback = client.recycle_bin_empty()
        if readback is not True:
            raise PersistentEffectIndeterminate("115 回收站清空结果无法确认")
        progress.record_committed_effect(label)
        progress.update(100, "115 回收站已确认清空")
        return {"ok": True, "already_empty": False, "provider_response": dict(response)}


class P115UnrecognizedEmptyOperation:
    def __init__(
        self,
        *,
        provider: P115MaintenanceGateway,
        facts: P115MaintenanceFacts,
    ) -> None:
        self._provider = provider
        self._facts = facts

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: P115OperationProgress,
    ) -> Mapping[str, Any]:
        account_id, backend = _provider_parameters(parameters)
        directory_id = _numeric_id(parameters.get("directory_id"), "115 unrecognized directory ID")
        if directory_id == "0":
            raise ValueError("115 unrecognized directory ID must not be root")
        client = _required_client(self._provider, account_id=account_id, backend=backend)
        progress.checkpoint()
        initial = client.list_children(directory_id)
        frozen_ids = tuple(
            dict.fromkeys(_item_id(item) for item in initial if _item_id(item) and not item_is_directory(item))
        )
        remote_ids = set(frozen_ids)
        cached_ids = self._facts.cached_unrecognized_file_ids(
            account_id=account_id,
            parent_id=directory_id,
        )
        reconciled = sorted(cached_ids - remote_ids)
        local_deleted = self._facts.delete_confirmed_unrecognized_facts(
            reconciled,
            account_id=account_id,
            parent_id=directory_id,
        )
        deleted = 0
        for offset in range(0, len(frozen_ids), _DELETE_BATCH_SIZE):
            progress.checkpoint()
            batch = list(frozen_ids[offset : offset + _DELETE_BATCH_SIZE])
            progress.update(
                min(95, 10 + int((offset / max(1, len(frozen_ids))) * 80)),
                f"正在删除未识别目录文件：{offset + 1}-{offset + len(batch)}",
            )
            label = f"p115-unrecognized-delete:{account_id}:{backend}:{directory_id}:{offset}:{len(batch)}"
            progress.record_started_effect(label)
            try:
                response = client.delete_files(batch)
            except Exception as exc:
                try:
                    remaining = {
                        _item_id(item)
                        for item in client.list_children(directory_id)
                        if _item_id(item) and not item_is_directory(item)
                    }
                except Exception as readback_exc:
                    raise PersistentEffectIndeterminate(
                        f"115 未识别目录删除结果无法确认：{','.join(batch[:10])}"
                    ) from readback_exc
                if set(batch) & remaining:
                    raise PersistentEffectIndeterminate(
                        f"115 未识别目录删除结果无法确认：{','.join(sorted(set(batch) & remaining)[:10])}"
                    ) from exc
                local_deleted += self._facts.delete_confirmed_unrecognized_facts(
                    batch,
                    account_id=account_id,
                    parent_id=directory_id,
                )
                progress.record_committed_effect(label)
                deleted += len(batch)
                continue
            if not provider_succeeded(response):
                progress.record_abandoned_effect(label)
                raise RuntimeError(
                    str(response.get("error_msg") or response.get("message") or "115 未识别目录删除失败")
                )
            remaining = {
                _item_id(item)
                for item in client.list_children(directory_id)
                if _item_id(item) and not item_is_directory(item)
            }
            unconfirmed = sorted(set(batch) & remaining)
            if unconfirmed:
                raise PersistentEffectIndeterminate(f"115 未识别目录删除结果无法确认：{','.join(unconfirmed[:10])}")
            local_deleted += self._facts.delete_confirmed_unrecognized_facts(
                batch,
                account_id=account_id,
                parent_id=directory_id,
            )
            progress.record_committed_effect(label)
            deleted += len(batch)
        progress.update(100, "未识别目录已确认清空")
        return {
            "ok": True,
            "deleted_count": deleted,
            "local_deleted": local_deleted,
            "reconciled_count": len(reconciled),
            "unrecognized_cid": directory_id,
        }


class P115SpeedtestService:
    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        facts: P115MaintenanceFacts,
        direct_urls: P115MaintenanceDirectUrls,
        downloads: P115MaintenanceDownloads,
        clock: Callable[[], datetime] = datetime.now,
    ) -> None:
        self._credentials = credentials
        self._facts = facts
        self._direct_urls = direct_urls
        self._downloads = downloads
        self._clock = clock

    def run(
        self,
        *,
        max_mb: object,
        seconds: object,
        user_agent: object,
    ) -> P115Result:
        snapshot = self._credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        if not account_id.isdigit():
            return P115Result(
                {"status": "error", "message": "115 客户端未初始化，请检查 Token/Cookie 配置"},
                500,
            )
        sample = self._facts.speedtest_sample(account_id=account_id)
        if not sample or not str(sample.get("pick_code") or "").strip():
            return P115Result(
                {
                    "status": "error",
                    "message": "媒体库中没有可用于测速的 115 pick_code 资源，请先完成媒体库同步或整理。",
                },
                400,
            )
        resolved = self._direct_urls.resolve(
            str(sample["pick_code"]),
            expected_account_id=account_id,
            user_agent=_DEFAULT_USER_AGENT,
        )
        if resolved is None:
            return P115Result({"status": "error", "message": "提取 115 下载直链失败：无法提取下载直链"}, 500)
        try:
            speed = self._downloads.measure(
                resolved.url,
                user_agent=resolved.user_agent or _DEFAULT_USER_AGENT,
                max_bytes=_max_bytes(max_mb),
                max_seconds=_seconds(seconds),
            )
        except Exception as exc:
            return P115Result({"status": "error", "message": str(exc)}, 500)

        public_sample = dict(sample)
        pick_code = str(public_sample.pop("pick_code", "") or "")
        public_sample["pick_code_masked"] = f"{pick_code[:4]}****{pick_code[-4:]}" if len(pick_code) > 8 else "****"
        speed.update(
            {
                "ok": True,
                "backend": resolved.backend or "直链",
                "sample": public_sample,
                "host": urlparse(resolved.url).netloc,
                "tested_at": self._clock().isoformat(timespec="seconds"),
            }
        )
        app_type = str(snapshot.app_type or "web").strip().lower()
        return P115Result(
            {
                "status": "success",
                "data": {
                    "has_token": snapshot.has_openapi,
                    "has_cookie": snapshot.has_cookie,
                    "cookie_app_type": app_type if snapshot.has_cookie else None,
                    "cookie_app_label": self._credentials.app_label(app_type) if snapshot.has_cookie else None,
                    "cookie_valid": None,
                    "valid": True,
                    "msg": f"测速完成：{speed.get('speed_text')}",
                    "user_info": None,
                    "speed_test": speed,
                },
            }
        )


def _required_client(
    gateway: P115MaintenanceGateway,
    *,
    account_id: str,
    backend: str,
) -> P115MaintenanceClient:
    client = gateway.open(expected_account_id=account_id, expected_backend=backend)
    if client is None:
        raise RuntimeError("115 客户端未初始化")
    return client


def _provider_parameters(parameters: Mapping[str, Any]) -> tuple[str, str]:
    account_id = str(parameters.get("account_id") or "").strip()
    backend = str(parameters.get("backend") or "").strip().lower()
    if not account_id.isdigit():
        raise ValueError("stable 115 account identity is required")
    if backend not in {"openapi", "cookie"}:
        raise ValueError("stable 115 provider backend is required")
    return account_id, backend


def _numeric_id(value: object, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized.isdigit():
        raise ValueError(f"{label} must be numeric")
    return normalized


def _item_id(value: Mapping[str, Any]) -> str:
    return str(value.get("fid") or value.get("file_id") or value.get("id") or value.get("cid") or "").strip()


def _find_directory(items: Sequence[Mapping[str, Any]], name: str) -> Mapping[str, Any] | None:
    named = [item for item in items if item_name(item) == name]
    if any(not item_is_directory(item) for item in named):
        raise RuntimeError(f"115 中存在同名文件：{name}")
    if len(named) > 1:
        raise RuntimeError(f"115 中存在多个同名目录：{name}")
    return named[0] if named else None


def _mkdir_result(item: Mapping[str, Any], *, name: str, created: bool) -> dict[str, Any]:
    directory_id = _item_id(item)
    if not directory_id:
        raise RuntimeError(f"115 目录缺少稳定 CID：{name}")
    return {
        "provider_response": {"state": True, "cid": directory_id},
        "created": created,
        "directory": dict(item),
    }


def _max_bytes(value: object) -> int:
    try:
        raw_value = 32.0 if value in (None, "") else float(str(value))
        parsed = int(raw_value * 1024 * 1024)
    except (TypeError, ValueError):
        parsed = 32 * 1024 * 1024
    return max(1024 * 1024, min(parsed, 128 * 1024 * 1024))


def _seconds(value: object) -> int:
    try:
        raw_value = 12.0 if value in (None, "") else float(str(value))
        parsed = int(raw_value)
    except (TypeError, ValueError):
        parsed = 12
    return max(3, min(parsed, 30))


__all__ = [
    "P115MaintenanceFacts",
    "P115MaintenanceIntentService",
    "P115MkdirOperation",
    "P115ProviderIdentity",
    "P115RecycleBinEmptyOperation",
    "P115SpeedtestService",
    "P115UnrecognizedEmptyOperation",
]
