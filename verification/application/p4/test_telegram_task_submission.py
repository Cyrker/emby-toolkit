from __future__ import annotations

from types import SimpleNamespace

import task_manager
from handler import telegram


def test_telegram_task_menu_submits_through_task_manager_without_local_execution(monkeypatch) -> None:
    calls = []
    messages = []

    def fixture_task(_processor, **_kwargs):
        raise AssertionError("Telegram must not execute the task in Coordinator")

    monkeypatch.setattr(
        "tasks.core.get_task_registry",
        lambda **_kwargs: {"fixture-task": (fixture_task, "Fixture Task", "media")},
    )
    monkeypatch.setattr(
        "task_manager.submit_task",
        lambda *args, **kwargs: calls.append((args, kwargs)) or True,
    )
    monkeypatch.setattr(
        telegram,
        "spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Telegram must not spawn a local task")),
    )
    monkeypatch.setattr(
        telegram,
        "send_telegram_message",
        lambda chat_id, text, **_kwargs: messages.append((chat_id, text)),
    )
    monkeypatch.setitem(
        telegram.APP_CONFIG,
        telegram.constants.CONFIG_OPTION_TELEGRAM_MENU_TASKS,
        ["fixture-task"],
    )

    telegram._execute_task_from_tg("fixture-chat", "fixture-task")

    assert calls == [((fixture_task, "Fixture Task", "media"), {})]
    assert messages[-1][0] == "fixture-chat"
    assert "任务已提交" in messages[-1][1]


def test_telegram_force_flag_is_serialized_with_the_job_submission(monkeypatch) -> None:
    captured = {}

    def fixture_task(_processor, **_kwargs):
        raise AssertionError("task must not run locally")

    monkeypatch.setattr(
        "tasks.core.get_task_registry",
        lambda **_kwargs: {"role-translation": (fixture_task, "角色翻译", "media")},
    )
    monkeypatch.setattr(
        "task_manager.submit_task",
        lambda *args, **kwargs: captured.update({"args": args, "kwargs": kwargs}) or True,
    )
    monkeypatch.setattr(telegram, "send_telegram_message", lambda *_args, **_kwargs: None)
    monkeypatch.setitem(
        telegram.APP_CONFIG,
        telegram.constants.CONFIG_OPTION_TELEGRAM_MENU_TASKS,
        ["role-translation"],
    )

    telegram._execute_task_from_tg("fixture-chat", "role-translation")

    assert captured["kwargs"] == {"force_full_update": False}


def test_telegram_rejects_stale_organize_command_without_approved_plan(monkeypatch) -> None:
    calls = []
    messages = []
    monkeypatch.setattr("task_manager.submit_task", lambda *_args, **_kwargs: calls.append(True))
    monkeypatch.setattr(
        telegram,
        "send_telegram_message",
        lambda chat_id, text, **_kwargs: messages.append((chat_id, text)),
    )

    telegram._execute_task_from_tg("fixture-chat", "scan-organize-115")

    assert calls == []
    assert messages[-1][0] == "fixture-chat"
    assert "immutable Plan" in messages[-1][1]


def test_telegram_rejects_retired_or_unconfigured_tasks_before_submission(monkeypatch) -> None:
    calls = []
    messages = []
    registry = {
        "populate-metadata": (lambda *_args, **_kwargs: None, "同步媒体数据", "media"),
        "hdhive-auto-checkin": (lambda *_args, **_kwargs: None, "影巢签到", "media"),
    }
    monkeypatch.setattr("tasks.core.get_task_registry", lambda **_kwargs: registry)
    monkeypatch.setattr("task_manager.submit_task", lambda *_args, **_kwargs: calls.append(True))
    monkeypatch.setattr(
        telegram,
        "send_telegram_message",
        lambda chat_id, text, **_kwargs: messages.append((chat_id, text)),
    )
    monkeypatch.setitem(
        telegram.APP_CONFIG,
        telegram.constants.CONFIG_OPTION_TELEGRAM_MENU_TASKS,
        ["hdhive-auto-checkin"],
    )

    telegram._execute_task_from_tg("fixture-chat", "hdhive-auto-checkin")
    telegram._execute_task_from_tg("fixture-chat", "populate-metadata")

    assert calls == []
    assert "已下线" in messages[0][1]
    assert "未在 Telegram 快捷任务中启用" in messages[1][1]


def test_telegram_command_registration_excludes_retired_resource_search(monkeypatch) -> None:
    requests = []
    registry = {
        "populate-metadata": (lambda *_args, **_kwargs: None, "同步媒体数据", "media"),
        "hdhive-auto-checkin": (lambda *_args, **_kwargs: None, "影巢签到", "media"),
    }
    monkeypatch.setattr("tasks.core.get_task_registry", lambda **_kwargs: registry)
    monkeypatch.setitem(
        telegram.APP_CONFIG,
        telegram.constants.CONFIG_OPTION_TELEGRAM_MENU_TASKS,
        ["populate-metadata", "hdhive-auto-checkin"],
    )
    monkeypatch.setattr(
        telegram.requests,
        "post",
        lambda *args, **kwargs: requests.append((args, kwargs)) or SimpleNamespace(status_code=200),
    )
    monkeypatch.setattr(telegram, "get_proxies_for_requests", lambda: {})

    telegram._setup_bot_commands("fixture-token")

    commands = requests[0][1]["json"]["commands"]
    assert commands == [
        {"command": "populate_metadata", "description": "🚀 同步媒体数据"},
        {"command": "all_tasks", "description": "📋 查看 Telegram 快捷任务"},
    ]


def test_legacy_task_manager_rejects_organize_callable_before_any_queue_or_gateway(monkeypatch) -> None:
    calls = []

    def legacy_organize(*_args, **_kwargs):
        calls.append("executed")

    legacy_organize.__module__ = "tasks.p115"
    legacy_organize.__name__ = "task_scan_and_organize_115"
    monkeypatch.setattr(task_manager, "_persistent_gateway", None)
    queue_size = task_manager.task_queue.qsize()

    accepted = task_manager.submit_task(legacy_organize, "网盘文件整理", "media")

    assert accepted is False
    assert calls == []
    assert task_manager.task_queue.qsize() == queue_size
