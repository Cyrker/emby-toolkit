from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.operations import scheduled_maintenance as scheduled_maintenance_module
from etk.domains.operations.scheduled_maintenance import (
    HttpxScheduledPluginUpdateGateway,
    NativeP115TempCleanupOperation,
    NativePluginUpdateCheckOperation,
    ScheduledWriteResult,
    ScheduledWriteStatus,
)
from etk.platform import Settings
from etk.platform.jobs import PersistentEffectIndeterminate


class _Progress:
    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint",))

    def update(self, progress: int, message: str) -> None:
        self.events.append(("update", progress, message))

    def record_started_effect(self, label: str) -> None:
        self.events.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.events.append(("committed", label))


def test_native_p115_temp_cleanup_uses_lease_guarded_playback_owner() -> None:
    playback = SimpleNamespace(cleanup=lambda: 3, is_pool_enabled=lambda: True)
    progress = _Progress()

    result = NativeP115TempCleanupOperation(cast(Any, playback)).execute(
        {"older_than_hours": 3},
        progress,
    )

    assert result == {
        "ok": True,
        "deleted": 3,
        "details": [{"deleted": 3, "label": "native-playback", "lease_guarded": True}],
        "main_cleanup_enabled": True,
        "play_pool_cleanup_enabled": True,
    }
    assert progress.events == [("checkpoint",), ("update", 100, "115 临时播放产物清理已完成")]


def test_native_plugin_update_checks_version_runs_task_and_restarts_with_effect_fences() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.tasks = [
                {
                    "Id": "task-1",
                    "State": "Idle",
                    "LastExecutionResult": {"EndTimeUtc": "before", "Status": "Completed"},
                },
                {"Id": "task-1", "State": "Running", "LastExecutionResult": {"EndTimeUtc": "before"}},
                {
                    "Id": "task-1",
                    "State": "Idle",
                    "LastExecutionResult": {"EndTimeUtc": "after", "Status": "Completed"},
                },
            ]

        def installed_version(self) -> str:
            return "1.2.3"

        def latest_version(self) -> str:
            return "v1.2.4"

        def update_task(self, _task_id: str = "") -> dict[str, Any]:
            return self.tasks.pop(0)

        def trigger_update(self, task_id: str) -> ScheduledWriteResult:
            assert task_id == "task-1"
            return ScheduledWriteResult(ScheduledWriteStatus.CONFIRMED, 204)

        def pending_restart(self) -> bool:
            return True

        def restart(self) -> ScheduledWriteResult:
            return ScheduledWriteResult(ScheduledWriteStatus.CONFIRMED, 204)

    progress = _Progress()
    operation = NativePluginUpdateCheckOperation(
        cast(Any, Gateway()),
        sleep=lambda _seconds: None,
        monotonic=lambda: 0,
    )

    result = operation.execute({}, progress)

    assert result == {
        "ok": True,
        "updated": True,
        "current_version": "1.2.3",
        "latest_version": "v1.2.4",
    }
    assert progress.events == [
        ("checkpoint",),
        ("started", "plugin-update-trigger:task-1:v1.2.4"),
        ("committed", "plugin-update-trigger:task-1:v1.2.4"),
        ("checkpoint",),
        ("checkpoint",),
        ("started", "plugin-update-restart:task-1:v1.2.4"),
        ("committed", "plugin-update-restart:task-1:v1.2.4"),
        ("update", 100, "ETK MediaInfo Bridge 已更新并触发 Emby 重启"),
    ]


def test_native_plugin_update_does_not_replay_an_unreadable_trigger() -> None:
    gateway = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": {
            "Id": "task-1",
            "State": "Idle",
            "LastExecutionResult": {"EndTimeUtc": "before", "Status": "Completed"},
        },
        trigger_update=lambda _task_id: ScheduledWriteResult(ScheduledWriteStatus.INDETERMINATE),
    )

    with pytest.raises(PersistentEffectIndeterminate, match="trigger outcome is unknown"):
        NativePluginUpdateCheckOperation(cast(Any, gateway)).execute({}, _Progress())


@pytest.mark.parametrize(
    "parameters",
    [
        {"unsupported": True},
        {"older_than_hours": True},
        {"older_than_hours": 0},
        {"older_than_hours": "invalid"},
    ],
)
def test_native_p115_temp_cleanup_rejects_invalid_parameters(parameters: Mapping[str, Any]) -> None:
    playback = SimpleNamespace(cleanup=lambda: -3, is_pool_enabled=lambda: False)
    with pytest.raises(ValueError):
        NativeP115TempCleanupOperation(cast(Any, playback)).execute(parameters, _Progress())


def test_native_p115_temp_cleanup_clamps_negative_cleanup_count() -> None:
    playback = SimpleNamespace(cleanup=lambda: -3, is_pool_enabled=lambda: False)
    result = NativeP115TempCleanupOperation(cast(Any, playback)).execute({}, _Progress())
    assert result["deleted"] == 0
    assert result["play_pool_cleanup_enabled"] is False


@pytest.mark.parametrize(
    ("installed", "latest", "expected_reason"),
    [
        (None, "1.2.4", "plugin_not_installed"),
        ("invalid", "1.2.4", "version_unavailable"),
        ("1.2.4", "invalid", "version_unavailable"),
        ("1.2.4", "1.2.4", "up_to_date"),
    ],
)
def test_native_plugin_update_handles_version_boundaries(
    installed: str | None,
    latest: str | None,
    expected_reason: str,
) -> None:
    gateway = SimpleNamespace(installed_version=lambda: installed, latest_version=lambda: latest)
    result = NativePluginUpdateCheckOperation(cast(Any, gateway)).execute({}, _Progress())
    assert result["reason"] == expected_reason

    with pytest.raises(ValueError, match="does not accept parameters"):
        NativePluginUpdateCheckOperation(cast(Any, gateway)).execute({"unexpected": True}, _Progress())


def test_native_plugin_update_handles_unavailable_rejected_and_disappeared_tasks() -> None:
    unavailable = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": None,
    )
    assert NativePluginUpdateCheckOperation(cast(Any, unavailable)).execute({}, _Progress())["reason"] == (
        "update_task_unavailable"
    )

    rejected = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": {
            "Id": "task-1",
            "State": "Idle",
            "LastExecutionResult": {"EndTimeUtc": "before"},
        },
        trigger_update=lambda _task_id: ScheduledWriteResult(ScheduledWriteStatus.REJECTED, 400),
    )
    assert NativePluginUpdateCheckOperation(cast(Any, rejected)).execute({}, _Progress())["reason"] == (
        "update_trigger_rejected"
    )

    tasks: list[Mapping[str, Any] | None] = [
        {"Id": "task-1", "State": "Idle", "LastExecutionResult": {"EndTimeUtc": "before"}},
        None,
    ]
    disappeared = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": tasks.pop(0),
        trigger_update=lambda _task_id: ScheduledWriteResult(ScheduledWriteStatus.CONFIRMED, 204),
    )
    assert (
        NativePluginUpdateCheckOperation(cast(Any, disappeared), monotonic=lambda: 0).execute({}, _Progress())["reason"]
        == "update_task_disappeared"
    )


def test_native_plugin_update_accepts_indeterminate_trigger_after_authoritative_running_readback() -> None:
    tasks = [
        {"Id": "task-1", "State": "Idle", "LastExecutionResult": {"EndTimeUtc": "before"}},
        {"Id": "task-1", "State": "Running", "LastExecutionResult": {"EndTimeUtc": "before"}},
        {
            "Id": "task-1",
            "State": "Idle",
            "LastExecutionResult": {"EndTimeUtc": "after", "Status": "Completed"},
        },
    ]
    gateway = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": tasks.pop(0),
        trigger_update=lambda _task_id: ScheduledWriteResult(ScheduledWriteStatus.INDETERMINATE),
        pending_restart=lambda: False,
    )
    progress = _Progress()
    result = NativePluginUpdateCheckOperation(cast(Any, gateway), monotonic=lambda: 0).execute({}, progress)
    assert result["reason"] == "restart_not_pending"
    assert ("committed", "plugin-update-trigger:task-1:1.2.4") in progress.events


def test_native_plugin_update_reports_failed_timeout_and_restart_outcomes() -> None:
    failed_tasks = [
        {"Id": "task-1", "State": "Running", "LastExecutionResult": {"EndTimeUtc": "before"}},
        {
            "Id": "task-1",
            "State": "Idle",
            "LastExecutionResult": {"EndTimeUtc": "after", "Status": "Failed"},
        },
    ]
    failed = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": failed_tasks.pop(0),
    )
    failed_result = NativePluginUpdateCheckOperation(cast(Any, failed), monotonic=lambda: 0).execute({}, _Progress())
    assert failed_result == {"ok": True, "updated": False, "reason": "update_task_failed", "status": "failed"}

    timeout = SimpleNamespace(
        installed_version=lambda: "1.2.3",
        latest_version=lambda: "1.2.4",
        update_task=lambda _task_id="": {
            "Id": "task-1",
            "State": "Running",
            "LastExecutionResult": {"EndTimeUtc": "before"},
        },
    )
    clocks = iter((0.0, 2.0))
    timeout_result = NativePluginUpdateCheckOperation(
        cast(Any, timeout),
        monotonic=lambda: next(clocks),
        timeout_seconds=1,
    ).execute({}, _Progress())
    assert timeout_result["reason"] == "update_task_timeout"

    for status, expected in (
        (ScheduledWriteStatus.REJECTED, "restart_rejected"),
        (ScheduledWriteStatus.INDETERMINATE, "indeterminate"),
    ):
        tasks = [
            {"Id": "task-1", "State": "Running", "LastExecutionResult": {"EndTimeUtc": "before"}},
            {
                "Id": "task-1",
                "State": "Idle",
                "LastExecutionResult": {"EndTimeUtc": "after", "Status": "Completed"},
            },
        ]
        gateway = SimpleNamespace(
            installed_version=lambda: "1.2.3",
            latest_version=lambda: "1.2.4",
            update_task=lambda _task_id="", tasks=tasks: tasks.pop(0),
            pending_restart=lambda: True,
            restart=lambda status=status: ScheduledWriteResult(status),
        )
        operation = NativePluginUpdateCheckOperation(cast(Any, gateway), monotonic=lambda: 0)
        if expected == "indeterminate":
            with pytest.raises(PersistentEffectIndeterminate, match="restart outcome is unknown"):
                operation.execute({}, _Progress())
        else:
            assert operation.execute({}, _Progress())["reason"] == expected


def _http_gateway(handler: Any, *, settings: Settings | None = None) -> HttpxScheduledPluginUpdateGateway:
    return HttpxScheduledPluginUpdateGateway(
        lambda: (
            settings
            or Settings(
                emby_server_url="https://emby.fixture.invalid",
                emby_api_key=SecretStr("token"),
                github_token=SecretStr("github-token"),
            )
        ),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_httpx_scheduled_plugin_gateway_reads_inventory_release_task_and_restart_state() -> None:
    requests: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "api.github.com":
            return httpx.Response(200, json={"tag_name": "v1.2.4"})
        if request.url.path == "/Plugins":
            return httpx.Response(
                200,
                json=[
                    "invalid",
                    {"Name": "Other", "Version": "1"},
                    {"Name": "ETK MediaInfo Bridge", "Version": "1.2.3"},
                ],
            )
        if request.url.path == "/ScheduledTasks":
            return httpx.Response(
                200,
                json=[
                    "invalid",
                    {"Id": "task-1", "Key": "ETKMediaInfoBridgeUpdate", "State": "Idle"},
                ],
            )
        if request.url.path == "/System/Info":
            return httpx.Response(200, json={"HasPendingRestart": True})
        raise AssertionError(request.url)

    gateway = _http_gateway(transport)
    assert gateway.installed_version() == "1.2.3"
    assert gateway.latest_version() == "v1.2.4"
    assert gateway.update_task() == {"Id": "task-1", "Key": "ETKMediaInfoBridgeUpdate", "State": "Idle"}
    assert gateway.update_task("task-1") == {"Id": "task-1", "Key": "ETKMediaInfoBridgeUpdate", "State": "Idle"}
    assert gateway.pending_restart() is True
    github_request = next(request for request in requests if request.url.host == "api.github.com")
    assert github_request.headers["authorization"] == "Bearer github-token"


def test_httpx_scheduled_plugin_gateway_validates_inventories_and_write_certainty() -> None:
    mode = "plugins-invalid"

    def transport(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            if mode == "transport":
                raise httpx.ConnectError("fixture transport", request=request)
            return httpx.Response(int(mode))
        if request.url.host == "api.github.com":
            return httpx.Response(200, json=[])
        if mode == "plugins-invalid":
            return httpx.Response(200, json={})
        if mode == "tasks-invalid":
            return httpx.Response(200, json={})
        if mode == "tasks-ambiguous":
            return httpx.Response(
                200,
                json=[
                    {"Id": "one", "Key": "ETKMediaInfoBridgeUpdate"},
                    {"Id": "two", "Key": "ETKMediaInfoBridgeUpdate"},
                ],
            )
        if mode == "system-invalid":
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[])

    gateway = _http_gateway(transport)
    with pytest.raises(RuntimeError, match="plugin inventory"):
        gateway.installed_version()
    mode = "ok"
    assert gateway.installed_version() is None
    assert gateway.latest_version() is None
    mode = "tasks-invalid"
    with pytest.raises(RuntimeError, match="scheduled-task inventory"):
        gateway.update_task()
    mode = "tasks-ambiguous"
    with pytest.raises(RuntimeError, match="ambiguous"):
        gateway.update_task()
    mode = "system-invalid"
    with pytest.raises(RuntimeError, match="system information"):
        gateway.pending_restart()

    for status, expected in (
        ("204", ScheduledWriteStatus.CONFIRMED),
        ("400", ScheduledWriteStatus.REJECTED),
        ("503", ScheduledWriteStatus.INDETERMINATE),
        ("transport", ScheduledWriteStatus.INDETERMINATE),
    ):
        mode = status
        assert gateway.trigger_update("task / 1").status is expected
    mode = "204"
    assert gateway.restart().status is ScheduledWriteStatus.CONFIRMED

    missing_auth = _http_gateway(
        transport,
        settings=Settings(emby_server_url="", emby_api_key=SecretStr("")),
    )
    with pytest.raises(RuntimeError, match="authorization is not configured"):
        missing_auth.installed_version()
    with gateway._default_client() as client:
        assert client.follow_redirects is True


def test_scheduled_maintenance_helpers() -> None:
    assert scheduled_maintenance_module._task_end_time(None) == ""
    assert scheduled_maintenance_module._task_end_time({"LastExecutionResult": "invalid"}) == ""
    assert scheduled_maintenance_module._version_tuple("v1.2") == (1, 2, 0, 0)
    assert scheduled_maintenance_module._version_tuple("") is None
