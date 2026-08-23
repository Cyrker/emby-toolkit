from __future__ import annotations

import pytest

from handler import p115_service


def test_persistent_manual_raw_maintenance_is_accepted_before_parent_succeeds(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(p115_service, "_has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        p115_service,
        "spawn_later",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("persistent RAW maintenance must not use a local timer")
        ),
    )
    monkeypatch.setattr(
        "tasks.shared_resource_tasks.trigger_shared_resource_maintenance_task",
        lambda: calls.append("submitted") or True,
    )

    result = p115_service._kick_shared_raw_reupload_detached("fixture", delay=8.0)

    assert calls == ["submitted"]
    assert result == {
        "started": True,
        "persistent": True,
        "message": "已提交持久 Rapid 共享维护",
    }


def test_persistent_manual_raw_maintenance_rejects_parent_success(monkeypatch) -> None:
    monkeypatch.setattr(p115_service, "_has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        "tasks.shared_resource_tasks.trigger_shared_resource_maintenance_task",
        lambda: False,
    )

    with pytest.raises(RuntimeError, match="共享 RAW 重传维护 Job 未被接受"):
        p115_service._kick_shared_raw_reupload_detached("fixture")


def test_legacy_manual_raw_maintenance_keeps_delayed_dispatch(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(p115_service, "_has_persistent_execution", lambda: False)
    monkeypatch.setattr(p115_service, "_LAST_SHARED_RAW_REUPLOAD_AT", 0)
    monkeypatch.setattr(
        p115_service,
        "spawn_later",
        lambda delay, function: calls.append((delay, function)),
    )

    result = p115_service._kick_shared_raw_reupload_detached("fixture", delay=3.0)

    assert result == {"started": True, "message": "已异步触发 Rapid 共享维护"}
    assert len(calls) == 1
    assert calls[0][0] == 3.0
