from __future__ import annotations

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

    telegram._execute_task_from_tg("fixture-chat", "role-translation")

    assert captured["kwargs"] == {"force_full_update": False}
