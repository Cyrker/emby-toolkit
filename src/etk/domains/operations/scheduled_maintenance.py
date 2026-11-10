from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import quote

import httpx

from etk.domains.media.plugin_identity import PLUGIN_NAMES, PLUGIN_RELEASE_API, PLUGIN_TASK_KEY
from etk.platform import Settings
from etk.platform.jobs import PersistentEffectIndeterminate

_PLUGIN_TASK_KEY = PLUGIN_TASK_KEY
_LATEST_PLUGIN_RELEASE_API = PLUGIN_RELEASE_API


class ScheduledOperationProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...


class P115PlaybackCleanupPort(Protocol):
    def cleanup(self) -> int: ...

    def is_pool_enabled(self) -> bool: ...


class ScheduledWriteStatus(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class ScheduledWriteResult:
    status: ScheduledWriteStatus
    status_code: int | None = None


class PluginUpdatePort(Protocol):
    def installed_version(self) -> str | None: ...

    def latest_version(self) -> str | None: ...

    def update_task(self, task_id: str = "") -> Mapping[str, Any] | None: ...

    def trigger_update(self, task_id: str) -> ScheduledWriteResult: ...

    def pending_restart(self) -> bool: ...

    def restart(self) -> ScheduledWriteResult: ...


class NativeP115TempCleanupOperation:
    def __init__(self, playback: P115PlaybackCleanupPort) -> None:
        self._playback = playback

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: ScheduledOperationProgress,
    ) -> Mapping[str, Any]:
        unexpected = set(parameters) - {"older_than_hours"}
        if unexpected:
            raise ValueError("P115 temp cleanup parameters are not supported")
        older_than_hours = parameters.get("older_than_hours", 3)
        if isinstance(older_than_hours, bool):
            raise ValueError("older_than_hours must be a positive number")
        try:
            if float(older_than_hours) <= 0:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise ValueError("older_than_hours must be a positive number") from exc
        progress.checkpoint()
        removed = int(self._playback.cleanup())
        progress.update(100, "115 临时播放产物清理已完成")
        return {
            "ok": True,
            "deleted": max(removed, 0),
            "details": [{"deleted": max(removed, 0), "label": "native-playback", "lease_guarded": True}],
            "main_cleanup_enabled": True,
            "play_pool_cleanup_enabled": bool(self._playback.is_pool_enabled()),
        }


class NativePluginUpdateCheckOperation:
    def __init__(
        self,
        gateway: PluginUpdatePort,
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        poll_interval_seconds: float = 2,
        timeout_seconds: float = 420,
    ) -> None:
        self._gateway = gateway
        self._sleep = sleep
        self._monotonic = monotonic
        self._poll_interval_seconds = max(float(poll_interval_seconds), 0)
        self._timeout_seconds = max(float(timeout_seconds), 1)

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: ScheduledOperationProgress,
    ) -> Mapping[str, Any]:
        if parameters:
            raise ValueError("plugin update check does not accept parameters")
        progress.checkpoint()
        current_text = self._gateway.installed_version()
        if not current_text:
            return {"ok": True, "updated": False, "reason": "plugin_not_installed"}
        latest_text = self._gateway.latest_version()
        current = _version_tuple(current_text)
        latest = _version_tuple(latest_text)
        if current is None or latest is None:
            return {"ok": True, "updated": False, "reason": "version_unavailable"}
        if current >= latest:
            return {
                "ok": True,
                "updated": False,
                "reason": "up_to_date",
                "current_version": current_text,
                "latest_version": latest_text,
            }

        task = self._gateway.update_task()
        task_id = str((task or {}).get("Id") or "").strip()
        if not task_id:
            return {"ok": True, "updated": False, "reason": "update_task_unavailable"}
        previous_end = _task_end_time(task)
        saw_running = str((task or {}).get("State") or "").strip().casefold() == "running"
        if not saw_running:
            label = f"plugin-update-trigger:{task_id}:{latest_text}"
            progress.record_started_effect(label)
            triggered = self._gateway.trigger_update(task_id)
            if triggered.status is ScheduledWriteStatus.REJECTED:
                return {"ok": True, "updated": False, "reason": "update_trigger_rejected"}
            if triggered.status is ScheduledWriteStatus.INDETERMINATE:
                observed = self._gateway.update_task(task_id)
                state = str((observed or {}).get("State") or "").strip().casefold()
                if state != "running" and _task_end_time(observed) == previous_end:
                    raise PersistentEffectIndeterminate("Emby plugin update trigger outcome is unknown")
                task = observed
                saw_running = state == "running"
            progress.record_committed_effect(label)

        deadline = self._monotonic() + self._timeout_seconds
        while self._monotonic() < deadline:
            progress.checkpoint()
            task = self._gateway.update_task(task_id)
            if task is None:
                return {"ok": True, "updated": False, "reason": "update_task_disappeared"}
            state = str(task.get("State") or "").strip().casefold()
            if state == "running":
                saw_running = True
                self._sleep(self._poll_interval_seconds)
                continue
            end_time = _task_end_time(task)
            if saw_running or (end_time and end_time != previous_end):
                result = task.get("LastExecutionResult")
                status = str(result.get("Status") or "").strip().casefold() if isinstance(result, Mapping) else ""
                if status != "completed":
                    return {"ok": True, "updated": False, "reason": "update_task_failed", "status": status}
                break
            self._sleep(self._poll_interval_seconds)
        else:
            return {"ok": True, "updated": False, "reason": "update_task_timeout"}

        if not self._gateway.pending_restart():
            return {"ok": True, "updated": False, "plugin_updated": True, "reason": "restart_not_pending"}
        restart_label = f"plugin-update-restart:{task_id}:{latest_text}"
        progress.record_started_effect(restart_label)
        restarted = self._gateway.restart()
        if restarted.status is ScheduledWriteStatus.INDETERMINATE:
            raise PersistentEffectIndeterminate("Emby restart outcome is unknown after plugin update")
        if restarted.status is ScheduledWriteStatus.REJECTED:
            return {"ok": True, "updated": False, "plugin_updated": True, "reason": "restart_rejected"}
        progress.record_committed_effect(restart_label)
        progress.update(100, "embytool Bridge 已更新并触发 Emby 重启")
        return {
            "ok": True,
            "updated": True,
            "current_version": current_text,
            "latest_version": latest_text,
        }


class HttpxScheduledPluginUpdateGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        external_client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._external_client_factory = external_client_factory or client_factory or self._default_external_client

    def installed_version(self) -> str | None:
        payload = self._emby_get("/Plugins")
        if not isinstance(payload, list):
            raise RuntimeError("Emby plugin inventory returned an invalid payload")
        plugin = next(
            (
                value
                for value in payload
                if isinstance(value, Mapping) and str(value.get("Name") or "").strip() in PLUGIN_NAMES
            ),
            None,
        )
        return str(plugin.get("Version") or "").strip() or None if isinstance(plugin, Mapping) else None

    def latest_version(self) -> str | None:
        settings = self._settings()
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "embytool-PluginUpdater",
        }
        token = settings.github_token.get_secret_value().strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        with self._external_client_factory() as client:
            response = client.get(_LATEST_PLUGIN_RELEASE_API, headers=headers)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload: Any = response.json()
        return str(payload.get("tag_name") or "").strip() or None if isinstance(payload, Mapping) else None

    def update_task(self, task_id: str = "") -> Mapping[str, Any] | None:
        payload = self._emby_get("/ScheduledTasks")
        if not isinstance(payload, list):
            raise RuntimeError("Emby scheduled-task inventory returned an invalid payload")
        normalized_id = str(task_id or "").strip()
        matches = [
            value
            for value in payload
            if isinstance(value, Mapping)
            and (
                str(value.get("Id") or "").strip() == normalized_id
                if normalized_id
                else str(value.get("Key") or "").strip() == _PLUGIN_TASK_KEY
            )
        ]
        if len(matches) > 1:
            raise RuntimeError("Emby plugin update task identity is ambiguous")
        return dict(matches[0]) if matches else None

    def trigger_update(self, task_id: str) -> ScheduledWriteResult:
        return self._emby_write(f"/ScheduledTasks/Running/{quote(task_id, safe='')}")

    def pending_restart(self) -> bool:
        payload = self._emby_get("/System/Info")
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby system information returned an invalid payload")
        return payload.get("HasPendingRestart") is True

    def restart(self) -> ScheduledWriteResult:
        return self._emby_write("/System/Restart")

    def _emby_get(self, path: str) -> Any:
        base_url, headers = self._emby_authorization()
        with self._client_factory() as client:
            response = client.get(f"{base_url}{path}", headers=headers)
        response.raise_for_status()
        return response.json()

    def _emby_write(self, path: str) -> ScheduledWriteResult:
        base_url, headers = self._emby_authorization()
        try:
            with self._client_factory() as client:
                response = client.post(f"{base_url}{path}", headers=headers)
        except (httpx.TimeoutException, httpx.TransportError):
            return ScheduledWriteResult(ScheduledWriteStatus.INDETERMINATE)
        if response.status_code in {200, 202, 204}:
            return ScheduledWriteResult(ScheduledWriteStatus.CONFIRMED, response.status_code)
        if 400 <= response.status_code < 500:
            return ScheduledWriteResult(ScheduledWriteStatus.REJECTED, response.status_code)
        return ScheduledWriteResult(ScheduledWriteStatus.INDETERMINATE, response.status_code)

    def _emby_authorization(self) -> tuple[str, dict[str, str]]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise RuntimeError("Emby authorization is not configured")
        return base_url, {"X-Emby-Token": token, "Accept": "application/json"}

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        return httpx.Client(
            timeout=httpx.Timeout(max(float(settings.emby_api_timeout), 20.0), connect=10.0),
            follow_redirects=True,
            trust_env=False,
        )

    def _default_external_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(max(float(settings.emby_api_timeout), 20.0), connect=10.0),
            follow_redirects=True,
            trust_env=False,
        )


def _task_end_time(task: Mapping[str, Any] | None) -> str:
    result = task.get("LastExecutionResult") if isinstance(task, Mapping) else None
    return str(result.get("EndTimeUtc") or "").strip() if isinstance(result, Mapping) else ""


def _version_tuple(value: object) -> tuple[int, int, int, int] | None:
    numbers = [int(part) for part in re.findall(r"\d+", str(value or ""))[:4]]
    return tuple((numbers + [0, 0, 0, 0])[:4]) if numbers else None  # type: ignore[return-value]


__all__ = [
    "HttpxScheduledPluginUpdateGateway",
    "NativeP115TempCleanupOperation",
    "NativePluginUpdateCheckOperation",
    "ScheduledWriteResult",
    "ScheduledWriteStatus",
]
