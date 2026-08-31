from __future__ import annotations

from typing import Any

import pytest

import config_manager
import constants
import task_manager
from etk.compatibility import tg_resource_operations as resource_operations
from etk.compatibility.tg_control_bridge import submit_telegram_resource_action
from etk.platform.jobs import InvalidJsonPayloadError


class CaptureGateway:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.calls: list[dict[str, Any]] = []

    def submit(
        self,
        task_function: Any,
        task_name: str,
        processor_type: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        silent: bool,
        max_attempts: int = 1,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
    ) -> bool:
        self.calls.append(
            {
                "task_function": task_function,
                "task_name": task_name,
                "processor_type": processor_type,
                "args": args,
                "kwargs": kwargs,
                "silent": silent,
                "max_attempts": max_attempts,
                "idempotency_key": idempotency_key,
                "idempotency_scope": idempotency_scope,
            }
        )
        return self.accepted

    def submit_key(
        self,
        task_key: str,
        task_name: str,
        processor_type: str,
        parameters: dict[str, Any],
        *,
        silent: bool,
        max_attempts: int = 1,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
    ) -> bool:
        self.calls.append(
            {
                "task_key": task_key,
                "task_name": task_name,
                "processor_type": processor_type,
                "parameters": parameters,
                "silent": silent,
                "max_attempts": max_attempts,
                "idempotency_key": idempotency_key,
                "idempotency_scope": idempotency_scope,
            }
        )
        return self.accepted

    def is_busy(self) -> bool:
        return False

    def status(self) -> dict[str, Any]:
        return {}


def _action(name: str, payload: dict[str, Any], *, update_id: str = "update-1") -> dict[str, Any]:
    return {
        "version": 1,
        "action": name,
        "chat_id": "42",
        "update_id": update_id,
        "source_identity": f"fixture:{name}",
        "payload": payload,
    }


def test_resource_action_submission_requires_strict_json_and_no_runtime_credentials(monkeypatch) -> None:
    gateway = CaptureGateway()
    monkeypatch.setenv("ETK_PROCESS_ROLE", "coordinator")
    task_manager.install_persistent_submission_gateway(gateway)
    payload = {"slug": "fixture", "nested": {"ok": True}}
    try:
        assert submit_telegram_resource_action(_action("hdhive_transfer", payload))
        payload["nested"]["ok"] = False

        assert len(gateway.calls) == 1
        call = gateway.calls[0]
        assert call["task_function"] is resource_operations.task_execute_telegram_resource_action
        assert call["task_name"] == "TG影巢资源转存"
        assert call["processor_type"] == "media"
        assert call["args"] == ()
        assert call["kwargs"]["action"]["payload"]["nested"] == {"ok": True}
        assert call["max_attempts"] == 3
        assert len(call["idempotency_key"]) == 64
        assert call["idempotency_scope"] == "telegram-resource-action:v2"

        with pytest.raises(InvalidJsonPayloadError):
            submit_telegram_resource_action(_action("hdhive_transfer", {"bad": {"not-json"}}))
        with pytest.raises(ValueError, match="cannot persist"):
            submit_telegram_resource_action(_action("hdhive_transfer", {"api_key": "must-not-persist"}))
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)


def test_coordinator_resource_selection_only_submits_worker_actions(monkeypatch) -> None:
    from handler import shared_subscription_service, telegram
    from tasks import hdhive, helpers

    sessions = {
        "shared": {
            "stage": "hdhive_resources",
            "media": {"title": "Shared", "media_type": "movie", "tmdb_id": "100"},
            "all_resources": [
                {
                    "_tg_source": "shared_pool",
                    "source_kind": "movie",
                    "source_id": "shared-1",
                    "title": "Shared",
                }
            ],
        },
        "hdhive": {
            "stage": "hdhive_resources",
            "media": {"title": "Hive", "media_type": "movie", "tmdb_id": "200"},
            "all_resources": [{"_tg_source": "hdhive", "slug": "hive-1", "title": "Hive"}],
        },
        "subscription": {
            "stage": "hdhive_resources",
            "media": {"title": "Series", "media_type": "tv", "tmdb_id": "300", "year": "2026"},
            "all_resources": [],
        },
    }
    current = {"name": "shared"}
    submitted: list[dict[str, Any]] = []
    cleared: list[str] = []
    messages: list[str] = []
    monkeypatch.setattr(telegram, "_tg_get_session", lambda _chat_id: sessions[current["name"]])
    monkeypatch.setattr(telegram, "_tg_clear_session", lambda chat_id: cleared.append(str(chat_id)))
    monkeypatch.setattr(telegram, "_tg_send_plain", lambda _chat_id, text, **_kwargs: messages.append(text) or True)
    monkeypatch.setattr(telegram, "submit_telegram_resource_action", lambda action: submitted.append(action) or True)
    monkeypatch.setattr(
        telegram,
        "spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Coordinator action must not spawn")),
    )
    monkeypatch.setattr(
        shared_subscription_service,
        "consume_center_source_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Coordinator must not consume shared source")),
    )
    monkeypatch.setattr(
        hdhive,
        "task_download_from_hdhive",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Coordinator must not run HDHive task")),
    )
    monkeypatch.setattr(
        helpers,
        "process_subscription_items_and_update_db",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Coordinator must not update subscriptions")),
    )

    telegram._tg_start_hdhive_transfer("42", 1, update_id="update-shared")
    current["name"] = "hdhive"
    telegram._tg_start_hdhive_transfer("42", 1, update_id="update-hdhive")
    current["name"] = "subscription"
    telegram._tg_handle_subscribe("42", update_id="update-subscription")

    assert [item["action"] for item in submitted] == [
        "shared_pool_transfer",
        "hdhive_transfer",
        "subscription",
    ]
    assert [item["update_id"] for item in submitted] == [
        "update-shared",
        "update-hdhive",
        "update-subscription",
    ]
    assert all(item["chat_id"] == "42" and item["source_identity"] for item in submitted)
    assert cleared == ["42", "42", "42"]
    assert all("已提交" in message for message in messages)


def test_coordinator_submission_failure_reports_truth_and_keeps_the_session(monkeypatch) -> None:
    from handler import telegram

    session = {
        "stage": "hdhive_resources",
        "media": {"title": "Hive", "media_type": "movie", "tmdb_id": "200"},
        "all_resources": [{"_tg_source": "hdhive", "slug": "hive-1", "title": "Hive"}],
    }
    messages: list[str] = []
    monkeypatch.setattr(telegram, "_tg_get_session", lambda _chat_id: session)
    monkeypatch.setattr(
        telegram,
        "_tg_clear_session",
        lambda _chat_id: (_ for _ in ()).throw(AssertionError("retriable session must remain")),
    )
    monkeypatch.setattr(telegram, "_tg_send_plain", lambda _chat_id, text, **_kwargs: messages.append(text) or True)
    monkeypatch.setattr(
        telegram,
        "submit_telegram_resource_action",
        lambda _action: (_ for _ in ()).throw(RuntimeError("gateway unavailable")),
    )

    telegram._tg_start_hdhive_transfer("42", 1, update_id="update-failed")

    assert messages == ["❌ 持久任务提交失败：gateway unavailable"]


@pytest.mark.parametrize(
    ("text", "expected_action"),
    [
        ("https://115.com/s/abc123 访问码: a1b2", "p115_share_import"),
        ("magnet:?xt=urn:btih:ABCDEF0123456789", "p115_offline_add"),
        ("ed2k://|file|fixture.mkv|100|ABCDEF|/", "p115_offline_add"),
    ],
)
def test_manual_resource_messages_only_submit_persistent_actions(monkeypatch, text: str, expected_action: str) -> None:
    from handler import p115_service, telegram

    submitted: list[dict[str, Any]] = []
    monkeypatch.setattr(telegram.user_db, "get_admin_telegram_chat_ids", lambda: ["42"])
    monkeypatch.setattr(telegram, "_tg_try_handle_resource_session_input", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(telegram, "_tg_send_plain", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(telegram, "submit_telegram_resource_action", lambda action: submitted.append(action) or True)
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError(f"Coordinator accessed P115 client: {cls}"))),
    )

    telegram._handle_incoming_message(
        {"message_id": 9, "chat": {"id": 42}, "from": {"id": 42}, "text": text},
        update_id="update-manual",
    )

    assert len(submitted) == 1
    action = submitted[0]
    assert action["action"] == expected_action
    assert action["chat_id"] == "42"
    assert action["update_id"] == "update-manual"
    assert action["source_identity"]
    assert "client" not in action["payload"]


def test_worker_shared_pool_and_hdhive_actions_raise_reported_failures(monkeypatch) -> None:
    from tasks import hdhive, shared_resource_tasks

    shared_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        shared_resource_tasks,
        "run_shared_center_transfer_persistent",
        lambda action, source, **kwargs: (
            shared_calls.append({"action": action, "source": source, **kwargs})
            or {"ok": True, "result": {"success_count": 1, "total": 1, "message": "done"}}
        ),
    )
    result = resource_operations.task_execute_telegram_resource_action(
        action=_action(
            "shared_pool_transfer",
            {"source": {"source_kind": "movie", "source_id": "shared-1"}, "mode": "rapid"},
        )
    )
    assert result["ok"] is True
    assert shared_calls == [
        {
            "action": "cloud_download",
            "source": {"source_kind": "movie", "source_id": "shared-1"},
            "mode": "rapid",
            "event_id": "telegram:update-1",
        }
    ]

    monkeypatch.setattr(
        shared_resource_tasks,
        "run_shared_center_transfer_persistent",
        lambda *_args, **_kwargs: {"ok": False, "message": "shared failed"},
    )
    with pytest.raises(RuntimeError, match="shared failed"):
        resource_operations.task_execute_telegram_resource_action(
            action=_action(
                "shared_pool_transfer",
                {"source": {"source_kind": "movie", "source_id": "shared-1"}},
            )
        )

    monkeypatch.setattr(hdhive, "task_download_from_hdhive", lambda **_kwargs: False)
    with pytest.raises(RuntimeError, match="HDHive"):
        resource_operations.task_execute_telegram_resource_action(
            action=_action("hdhive_transfer", {"slug": "hive-1", "tmdb_id": "200", "media_type": "movie"})
        )


def test_worker_subscription_resolves_runtime_config_and_propagates_failure(monkeypatch) -> None:
    from handler import tmdb
    from tasks import helpers

    monkeypatch.setitem(config_manager.APP_CONFIG, constants.CONFIG_OPTION_TMDB_API_KEY, "fixture-key")
    monkeypatch.setattr(
        tmdb,
        "get_tv_details",
        lambda tmdb_id, api_key: {
            "id": tmdb_id,
            "api_key_seen": api_key,
            "seasons": [{"season_number": 0}, {"season_number": 1}, {"season_number": 2}],
        },
    )
    processed_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(resource_operations, "_telegram_emby_username", lambda _chat_id: "Fixture Admin")
    monkeypatch.setattr(
        helpers,
        "process_subscription_items_and_update_db",
        lambda **kwargs: processed_calls.append(kwargs) or {"300_Series"},
    )

    result = resource_operations.task_execute_telegram_resource_action(
        action=_action("subscription", {"tmdb_id": "300", "media_type": "tv", "title": "Series"})
    )

    assert result["result"]["processed_ids"] == ["300_Series"]
    assert processed_calls[0]["tmdb_api_key"] == "fixture-key"
    assert processed_calls[0]["subscription_source"] == {
        "type": "telegram_search",
        "user_id": "42",
        "name": "Fixture Admin",
    }
    assert processed_calls[0]["tmdb_items"] == [
        {"tmdb_id": "300", "media_type": "Series", "season": 1},
        {"tmdb_id": "300", "media_type": "Series", "season": 2},
    ]

    monkeypatch.setattr(
        helpers,
        "process_subscription_items_and_update_db",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("subscription failed")),
    )
    with pytest.raises(RuntimeError, match="subscription failed"):
        resource_operations.task_execute_telegram_resource_action(
            action=_action("subscription", {"tmdb_id": "300", "media_type": "movie", "title": "Movie"})
        )


@pytest.mark.parametrize(
    ("action_name", "organize_reason"),
    [
        ("p115_share_import", "tg_share_import"),
        ("p115_offline_add", "tg_offline_download"),
    ],
)
def test_worker_manual_p115_actions_dispatch_only_opaque_secret_payloads(
    monkeypatch,
    action_name: str,
    organize_reason: str,
) -> None:
    calls: list[dict[str, Any]] = []

    def execute(effect_action, payload, action, **kwargs):
        calls.append(
            {
                "action": effect_action,
                "payload": dict(payload),
                "delivery": dict(action),
                **kwargs,
            }
        )
        return {"message": "done", "organize_submitted": True}

    monkeypatch.setattr(resource_operations, "_execute_p115_secret_effect", execute)
    safe_payload = {
        "secret_ref": "a" * 64,
        "semantic_source_hash": "b" * 64,
        "delivery_key": "c" * 64,
    }

    result = resource_operations.task_execute_telegram_resource_action(action=_action(action_name, safe_payload))

    assert result["ok"] is True
    assert calls[0]["action"] == action_name
    assert calls[0]["payload"] == safe_payload
    assert calls[0]["organize_reason"] == organize_reason


@pytest.mark.parametrize("action_name", ["p115_share_import", "p115_offline_add"])
def test_worker_manual_p115_reported_failure_raises_without_organize(monkeypatch, action_name: str) -> None:
    monkeypatch.setattr(
        resource_operations,
        "_execute_p115_secret_effect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("p115 failed")),
    )
    payload = {
        "secret_ref": "a" * 64,
        "semantic_source_hash": "b" * 64,
        "delivery_key": "c" * 64,
    }

    with pytest.raises(RuntimeError, match="p115 failed"):
        resource_operations.task_execute_telegram_resource_action(action=_action(action_name, payload))


@pytest.mark.parametrize("state", [True, 1, 1.0, "1", "true", "TRUE"])
def test_p115_success_state_compatibility(state: Any) -> None:
    assert resource_operations._p115_write_outcome({"state": state}) == "accepted"
