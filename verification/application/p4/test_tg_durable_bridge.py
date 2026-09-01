from __future__ import annotations

import argparse
import hashlib
import json
import os
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from flask import Flask

import config_manager
import task_manager
from etk.bootstrap import cli
from etk.compatibility import tg_resource_effects
from etk.compatibility.tg_control_bridge import (
    DurableTelegramTaskQueue,
    PersistentTaskGatewayUnavailableError,
    TelegramCommandExecutor,
)
from etk.platform.jobs import InvalidJsonPayloadError

REPO_ROOT = Path(__file__).resolve().parents[3]


class CaptureGateway:
    def __init__(self) -> None:
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
        return True

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
        return True

    def is_busy(self) -> bool:
        return False

    def status(self) -> dict[str, Any]:
        return {}


def _watched_threads() -> set[str]:
    prefixes = ("TG_UserBot_Thread", "TG_Task_Consumer", "etk-compat-", "shared-")
    return {thread.name for thread in threading.enumerate() if thread.name.startswith(prefixes)}


def test_tg_transfer_put_requires_the_persistent_gateway_and_strict_json(monkeypatch) -> None:
    gateway = CaptureGateway()
    queue = DurableTelegramTaskQueue()
    secrets: list[dict[str, Any]] = []
    destroyed: list[tuple[str, str | None]] = []

    def create_secret(payload, *, reference=None, owner_key=None, **_kwargs):
        secrets.append(
            {
                "payload": dict(payload),
                "reference": reference,
                "owner_key": owner_key,
            }
        )
        return str(reference), True

    monkeypatch.setattr(tg_resource_effects, "create_secret", create_secret)
    monkeypatch.setattr(tg_resource_effects, "read_effect_binding", lambda _delivery_key: None)
    monkeypatch.setattr(
        tg_resource_effects,
        "destroy_secret",
        lambda reference, owner_key=None: destroyed.append((reference, owner_key)) or True,
    )
    monkeypatch.setenv("ETK_PROCESS_ROLE", "api")
    task_manager.install_persistent_submission_gateway(gateway)
    before = _watched_threads()
    try:
        raw_url = "magnet:?xt=urn:btih:fixture"
        payload = {
            "type": "offline_download",
            "url": raw_url,
            "raw_text": f"private message {raw_url}",
            "candidate": {"source_chat_id": "-10042", "message_id": 99, "magnet_url": raw_url},
            "nested": {"ok": True},
        }
        assert queue.put(payload)
        payload["nested"]["ok"] = False

        assert len(gateway.calls) == 1
        call = gateway.calls[0]
        assert call["task_function"].__name__ == "task_process_channel_transfer"
        assert call["task_name"] == "TG频道资源转存"
        assert call["args"] == ()
        assert call["kwargs"]["payload"]["nested"] == {"ok": True}
        assert call["kwargs"]["payload"]["resource_kind"] == "p115_offline"
        assert call["kwargs"]["payload"]["secret_ref"] == call["idempotency_key"]
        assert call["max_attempts"] == 3
        assert call["idempotency_scope"] == "telegram-channel-transfer:v2"
        assert secrets == [
            {
                "payload": {"urls": [raw_url]},
                "reference": call["idempotency_key"],
                "owner_key": call["idempotency_key"],
            }
        ]
        serialized_job = json.dumps(call["kwargs"], ensure_ascii=False, sort_keys=True)
        assert raw_url not in serialized_job
        assert "private message" not in serialized_job

        with pytest.raises(InvalidJsonPayloadError):
            queue.put({"type": "offline_download", "bad": {"not-json"}})
        with pytest.raises(InvalidJsonPayloadError):
            queue.put({"type": "offline_download", "bad": lambda: None})
        assert _watched_threads() == before
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)

    with pytest.raises(PersistentTaskGatewayUnavailableError, match="process role 'api'"):
        queue.put({"type": "offline_download", "url": "magnet:?xt=urn:btih:no-gateway"})
    assert destroyed
    assert _watched_threads() == before


def test_channel_delivery_identity_uses_source_chat_and_message(monkeypatch) -> None:
    gateway = CaptureGateway()
    monkeypatch.setattr(tg_resource_effects, "read_effect_binding", lambda _delivery_key: None)
    monkeypatch.setattr(
        tg_resource_effects,
        "create_secret",
        lambda _payload, *, reference=None, **_kwargs: (str(reference), True),
    )
    task_manager.install_persistent_submission_gateway(gateway)
    try:
        first = {
            "type": "offline_download",
            "url": "magnet:?xt=urn:btih:first",
            "candidate": {"source_chat_id": "-10042", "message_id": 99},
        }
        repeated_delivery = {
            "type": "offline_download",
            "url": "magnet:?xt=urn:btih:first",
            "candidate": {"source_chat_id": "-10042", "message_id": 99},
        }
        assert DurableTelegramTaskQueue().put(first)
        assert DurableTelegramTaskQueue().put(repeated_delivery)
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)

    assert len(gateway.calls) == 2
    assert gateway.calls[0]["idempotency_key"] == gateway.calls[1]["idempotency_key"]
    assert gateway.calls[0]["idempotency_scope"] == "telegram-channel-transfer:v2"


def test_tg_control_endpoints_use_only_the_persisted_bridge(monkeypatch) -> None:
    from handler.tg_userbot import TGUserBotManager
    from routes import subscription

    class FakeBridge:
        calls: list[tuple[str, dict[str, Any]]] = []

        def enqueue(self, command_type: str, payload: dict[str, Any] | None = None) -> str:
            self.calls.append((command_type, dict(payload or {})))
            return command_type

        def wait(self, command_id: str, *, timeout_seconds: float) -> dict[str, Any]:
            assert timeout_seconds == 45
            results = {
                "send_code": {"success": True},
                "login": {"success": True},
                "logout": {"success": True},
                "clear_session": {"deleted": ["tg_userbot.session"], "missing": []},
            }
            return {"result": results[command_id]}

        def get_runtime_status(self) -> dict[str, Any]:
            return {"status": "authorized", "updated_at": "2026-08-23T00:00:00+00:00"}

    monkeypatch.setattr(subscription, "TelegramControlBridge", FakeBridge)
    monkeypatch.setattr(
        TGUserBotManager,
        "get_instance",
        classmethod(lambda cls: (_ for _ in ()).throw(AssertionError(f"unexpected manager access: {cls}"))),
    )
    saved_settings: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        subscription.settings_db,
        "save_setting",
        lambda key, value: saved_settings.append((key, value)),
    )
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    app = Flask(__name__)
    app.secret_key = "fixture-only"
    before = _watched_threads()

    with app.test_request_context(
        "/api/subscription/tg_userbot/config",
        method="POST",
        json={"enabled": True, "channels": ["fixture"], "api_hash": "must-not-persist"},
    ):
        assert subscription.save_tg_config().get_json()["success"]
    with app.test_request_context("/api/subscription/tg_userbot/status"):
        assert cast(Any, subscription.tg_userbot_status()).get_json()["data"]["status"] == "authorized"
    with app.test_request_context("/api/subscription/tg_userbot/send_code", method="POST", json={}):
        assert subscription.tg_userbot_send_code().get_json()["success"]
    with app.test_request_context("/api/subscription/tg_userbot/login", method="POST", json={"code": "12345"}):
        assert subscription.tg_userbot_login().get_json()["success"]
    with app.test_request_context("/api/subscription/tg_userbot/logout", method="POST", json={}):
        assert cast(Any, subscription.tg_userbot_logout()).get_json()["success"]
    with app.test_request_context("/api/subscription/tg_userbot/clear_session", method="POST", json={}):
        assert cast(Any, subscription.tg_userbot_clear_session()).get_json()["success"]

    assert FakeBridge.calls == [
        ("send_code", {}),
        ("login", {"code": "12345"}),
        ("logout", {}),
        ("clear_session", {}),
    ]
    assert saved_settings == [("tg_userbot_config", {"enabled": True, "channels": ["fixture"]})]
    assert _watched_threads() == before


def test_coordinator_executor_uses_a_fake_manager_and_persists_results() -> None:
    commands = [
        {"id": "1", "type": "send_code", "payload": {}},
        {"id": "2", "type": "login", "payload": {"code": "12345"}},
        {"id": "3", "type": "logout", "payload": {}},
        {"id": "4", "type": "clear_session", "payload": {}},
        {"id": "5", "type": "search", "payload": {"query": "Fixture"}},
    ]

    class FakeBridge:
        def __init__(self) -> None:
            self.completed: list[tuple[str, dict[str, Any]]] = []
            self.failed: list[tuple[str, str]] = []
            self.status: dict[str, Any] = {}

        def claim_next(self, owner_id: str, *, lease_seconds: int) -> dict[str, Any] | None:
            assert owner_id == "fixture-coordinator"
            assert lease_seconds == 60
            return commands.pop(0) if commands else None

        def complete(self, command_id: str, owner_id: str, result: dict[str, Any]) -> None:
            assert owner_id == "fixture-coordinator"
            self.completed.append((command_id, result))

        def checkpoint_request(self, command_id: str, owner_id: str) -> dict[str, Any]:
            assert owner_id == "fixture-coordinator"
            return next(
                {
                    "id": command_id,
                    "type": command_type,
                    "payload": payload,
                    "status": "requesting",
                }
                for expected_id, command_type, payload in (
                    ("1", "send_code", {}),
                    ("2", "login", {"code": "12345"}),
                    ("3", "logout", {}),
                    ("4", "clear_session", {}),
                )
                if expected_id == command_id
            )

        def fail(self, command_id: str, owner_id: str, error: str) -> None:
            assert owner_id == "fixture-coordinator"
            self.failed.append((command_id, error))

        def update_runtime_status(self, status: dict[str, Any]) -> dict[str, Any]:
            self.status = status
            return status

    class FakeManager:
        def send_login_code(self) -> None:
            return None

        def submit_login_code(self, code: str) -> dict[str, Any]:
            return {"success": code == "12345"}

        def logout(self) -> None:
            return None

        def clear_session_files(self) -> dict[str, Any]:
            return {"deleted": ["fixture.session"], "missing": []}

        def search_channel_resources(self, **payload: Any) -> dict[str, Any]:
            return {"ok": True, "results": [{"title": payload["query"]}]}

        def get_status(self) -> dict[str, Any]:
            return {"status": "authorized"}

    bridge = FakeBridge()
    executor = TelegramCommandExecutor(bridge, owner_id="fixture-coordinator", lease_seconds=60)  # type: ignore[arg-type]

    assert executor.process_pending(FakeManager(), limit=8) == 5
    assert [item[0] for item in bridge.completed] == ["1", "2", "3", "4", "5"]
    assert bridge.completed[-1][1] == {"ok": True, "results": [{"title": "Fixture"}]}
    assert bridge.failed == []
    assert bridge.status == {"status": "authorized"}


def test_login_secret_validation_fails_before_checkpoint_and_destroys_secret() -> None:
    command = {
        "id": "fixture-login",
        "type": "login",
        "payload": {"code_ref": "a" * 64, "code_hash": "0" * 64},
        "idempotency_key_hash": "b" * 64,
    }

    class Bridge:
        def __init__(self) -> None:
            self.claimed = False
            self.checkpoints = 0
            self.failed: list[tuple[str, str]] = []
            self.destroyed: list[tuple[str, str | None]] = []

        def claim_next(self, owner_id: str, *, lease_seconds: int) -> dict[str, Any] | None:
            del owner_id, lease_seconds
            if self.claimed:
                return None
            self.claimed = True
            return dict(command)

        def consume_resource_secret(self, reference: str, owner_key: str) -> dict[str, Any]:
            assert reference == "a" * 64
            assert owner_key == "b" * 64
            return {"code": "12345"}

        def checkpoint_request(self, command_id: str, owner_id: str) -> dict[str, Any]:
            del command_id, owner_id
            self.checkpoints += 1
            return {**command, "status": "requesting"}

        def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool:
            self.destroyed.append((reference, owner_key))
            return True

        def fail(self, command_id: str, owner_id: str, error: str) -> None:
            del owner_id
            self.failed.append((command_id, error))

        def update_runtime_status(self, status: dict[str, Any]) -> dict[str, Any]:
            return status

    class Manager:
        def submit_login_code(self, code: str) -> None:
            raise AssertionError(code)

        def get_status(self) -> dict[str, Any]:
            return {"status": "authorized"}

    bridge = Bridge()
    executor = TelegramCommandExecutor(bridge, owner_id="fixture")  # type: ignore[arg-type]

    assert executor.process_pending(Manager(), limit=1) == 1
    assert bridge.checkpoints == 0
    assert bridge.failed == [("fixture-login", "Telegram login code secret changed")]
    assert bridge.destroyed == [("a" * 64, "b" * 64)]


def test_login_manager_failure_after_checkpoint_is_indeterminate_and_destroys_secret() -> None:
    secret = {"code": "12345"}
    secret_hash = hashlib.sha256(
        json.dumps(secret, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    command = {
        "id": "fixture-login",
        "type": "login",
        "payload": {"code_ref": "a" * 64, "code_hash": secret_hash},
        "idempotency_key_hash": "b" * 64,
    }

    class Bridge:
        def __init__(self) -> None:
            self.claimed = False
            self.checkpoints = 0
            self.indeterminate_calls: list[tuple[str, str]] = []
            self.destroyed = 0

        def claim_next(self, owner_id: str, *, lease_seconds: int) -> dict[str, Any] | None:
            del owner_id, lease_seconds
            if self.claimed:
                return None
            self.claimed = True
            return dict(command)

        def consume_resource_secret(self, reference: str, owner_key: str) -> dict[str, Any]:
            del reference, owner_key
            return dict(secret)

        def checkpoint_request(self, command_id: str, owner_id: str) -> dict[str, Any]:
            del command_id, owner_id
            self.checkpoints += 1
            return {**command, "status": "requesting"}

        def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool:
            del reference, owner_key
            self.destroyed += 1
            return True

        def indeterminate(self, command_id: str, owner_id: str, error: str) -> None:
            del owner_id
            self.indeterminate_calls.append((command_id, error))

        def update_runtime_status(self, status: dict[str, Any]) -> dict[str, Any]:
            return status

    class Manager:
        calls = 0

        def submit_login_code(self, code: str) -> None:
            assert code == "12345"
            self.calls += 1
            raise TimeoutError("provider timeout")

        def get_status(self) -> dict[str, Any]:
            return {"status": "error"}

    bridge = Bridge()
    manager = Manager()
    executor = TelegramCommandExecutor(bridge, owner_id="fixture")  # type: ignore[arg-type]

    assert executor.process_pending(manager, limit=1) == 1
    assert bridge.checkpoints == 1
    assert bridge.indeterminate_calls == [("fixture-login", "TimeoutError")]
    assert bridge.destroyed == 1
    assert manager.calls == 1
    assert executor.process_pending(manager, limit=1) == 0
    assert manager.calls == 1


def test_checkpoint_commit_ambiguity_is_indeterminate_instead_of_failed() -> None:
    command = {"id": "fixture-send-code", "type": "send_code", "payload": {}, "status": "claimed"}

    class Bridge:
        def __init__(self) -> None:
            self.claimed = False
            self.durable = dict(command)
            self.indeterminate_calls: list[tuple[str, str]] = []
            self.failed: list[str] = []

        def claim_next(self, owner_id: str, *, lease_seconds: int) -> dict[str, Any] | None:
            del lease_seconds
            if self.claimed:
                return None
            self.claimed = True
            self.durable.update(claimed_by=owner_id)
            return dict(self.durable)

        def checkpoint_request(self, command_id: str, owner_id: str) -> dict[str, Any]:
            assert (command_id, owner_id) == ("fixture-send-code", "fixture")
            self.durable.update(status="requesting", claimed_by=owner_id)
            raise ConnectionError("commit acknowledgement lost")

        def get_command(self, command_id: str) -> dict[str, Any]:
            assert command_id == "fixture-send-code"
            return dict(self.durable)

        def indeterminate(self, command_id: str, owner_id: str, error: str) -> None:
            del owner_id
            self.indeterminate_calls.append((command_id, error))

        def fail(self, command_id: str, owner_id: str, error: str) -> None:
            del owner_id, error
            self.failed.append(command_id)

        def update_runtime_status(self, status: dict[str, Any]) -> dict[str, Any]:
            return status

    class Manager:
        def send_login_code(self) -> None:
            raise AssertionError("provider must not be called after ambiguous checkpoint")

        def get_status(self) -> dict[str, Any]:
            return {"status": "error"}

    bridge = Bridge()
    executor = TelegramCommandExecutor(bridge, owner_id="fixture")  # type: ignore[arg-type]

    assert executor.process_pending(Manager(), limit=1) == 1
    assert bridge.indeterminate_calls == [("fixture-send-code", "ConnectionError")]
    assert bridge.failed == []


def test_checkpoint_ack_loss_is_indeterminate_even_when_immediate_reread_is_stale() -> None:
    command = {
        "id": "fixture-send-code",
        "type": "send_code",
        "payload": {},
        "status": "claimed",
        "claimed_by": "fixture",
    }

    class Bridge:
        def __init__(self) -> None:
            self.claimed = False
            self.indeterminate_calls: list[tuple[str, str]] = []
            self.failed: list[str] = []

        def claim_next(self, owner_id: str, *, lease_seconds: int) -> dict[str, Any] | None:
            del owner_id, lease_seconds
            if self.claimed:
                return None
            self.claimed = True
            return dict(command)

        def checkpoint_request(self, command_id: str, owner_id: str) -> dict[str, Any]:
            del command_id, owner_id
            raise ConnectionError("commit acknowledgement lost")

        def get_command(self, command_id: str) -> dict[str, Any]:
            del command_id
            return dict(command)

        def indeterminate(self, command_id: str, owner_id: str, error: str) -> None:
            del owner_id
            self.indeterminate_calls.append((command_id, error))

        def fail(self, command_id: str, owner_id: str, error: str) -> None:
            del owner_id, error
            self.failed.append(command_id)

        def update_runtime_status(self, status: dict[str, Any]) -> dict[str, Any]:
            return status

    class Manager:
        def send_login_code(self) -> None:
            raise AssertionError("provider must not be called after ambiguous checkpoint")

        def get_status(self) -> dict[str, Any]:
            return {"status": "error"}

    bridge = Bridge()
    executor = TelegramCommandExecutor(bridge, owner_id="fixture")  # type: ignore[arg-type]

    assert executor.process_pending(Manager(), limit=1) == 1
    assert bridge.indeterminate_calls == [("fixture-send-code", "ConnectionError")]
    assert bridge.failed == []


def test_listener_action_releases_for_retry_then_destroys_secret_after_staging() -> None:
    candidate = {"tmdb_id": "42", "target_link": "https://115.com/s/provider-secret"}
    candidate_hash = hashlib.sha256(
        json.dumps(candidate, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    command = {
        "id": "fixture-listener-action",
        "type": "listener_action",
        "payload": {
            "candidate_ref": "a" * 64,
            "candidate_hash": candidate_hash,
            "occurrence": f"telegram-listener:{'b' * 64}",
            "is_brainless": True,
        },
        "idempotency_key_hash": "c" * 64,
        "status": "claimed",
    }

    class Bridge:
        def __init__(self) -> None:
            self.claims = 0
            self.releases = 0
            self.completed: list[dict[str, Any]] = []
            self.destroyed = 0

        def claim_next(self, owner_id: str, *, lease_seconds: int) -> dict[str, Any] | None:
            del owner_id, lease_seconds
            self.claims += 1
            return dict(command) if self.claims <= 2 else None

        def consume_resource_secret(self, reference: str, owner_key: str) -> dict[str, Any]:
            assert (reference, owner_key) == ("a" * 64, "c" * 64)
            return {"candidate": candidate}

        def release(self, command_id: str, owner_id: str, error_class: str) -> None:
            assert (command_id, owner_id, error_class) == (
                "fixture-listener-action",
                "fixture",
                "ConnectionError",
            )
            self.releases += 1

        def complete(self, command_id: str, owner_id: str, result: dict[str, Any]) -> None:
            assert (command_id, owner_id) == ("fixture-listener-action", "fixture")
            self.completed.append(result)

        def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool:
            assert (reference, owner_key) == ("a" * 64, "c" * 64)
            self.destroyed += 1
            return True

        def update_runtime_status(self, status: dict[str, Any]) -> dict[str, Any]:
            return status

    class Manager:
        def __init__(self) -> None:
            self.calls = 0

        def stage_listener_action(self, candidate_value: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
            assert dict(candidate_value) == candidate
            assert kwargs["occurrence"] == f"telegram-listener:{'b' * 64}"
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("fixture database unavailable")
            return {"success": True, "staged": True}

        def get_status(self) -> dict[str, Any]:
            return {"status": "authorized"}

    bridge = Bridge()
    manager = Manager()
    executor = TelegramCommandExecutor(bridge, owner_id="fixture")  # type: ignore[arg-type]

    assert executor.process_pending(manager, limit=1) == 1
    assert bridge.releases == 1
    assert bridge.destroyed == 0
    assert executor.process_pending(manager, limit=1) == 1
    assert bridge.completed == [{"success": True, "staged": True}]
    assert bridge.destroyed == 1


def test_worker_channel_operation_invokes_one_payload_without_a_consumer_thread(monkeypatch) -> None:
    from handler import tg_userbot
    from tasks import subscriptions

    received: list[dict[str, Any]] = []
    monkeypatch.setattr(
        tg_userbot,
        "process_tg_transfer_payload",
        lambda payload: received.append(payload) or {"ok": True},
    )
    before = _watched_threads()

    result = subscriptions.task_process_channel_transfer(
        object(),
        payload={"type": "offline_download", "url": "magnet:?xt=urn:btih:fixture"},
    )

    assert result == {"ok": True}
    assert received == [{"type": "offline_download", "url": "magnet:?xt=urn:btih:fixture"}]
    assert _watched_threads() == before


@pytest.mark.parametrize("reported", [False, {"ok": False}, {"success": False}, {"state": False}])
def test_hdhive_reported_failure_raises_for_worker_job(monkeypatch, reported: Any) -> None:
    from tasks import subscriptions

    monkeypatch.setattr(subscriptions, "task_download_from_hdhive", lambda *args, **kwargs: reported)

    with pytest.raises(RuntimeError, match="HDHive"):
        subscriptions.task_download_hdhive_resource(slug="fixture")


def test_worker_shared_maintenance_does_not_start_the_listener(monkeypatch) -> None:
    from handler import shared_intro_service
    from tasks import shared_resource_tasks

    monkeypatch.setenv("ETK_PROCESS_ROLE", "worker")
    monkeypatch.setattr(shared_resource_tasks, "_enabled", lambda: True)
    monkeypatch.setattr(
        shared_resource_tasks,
        "ensure_shared_device_event_listener",
        lambda: (_ for _ in ()).throw(AssertionError("Worker must not start the shared listener")),
    )
    for name in (
        "_cleanup_offline_local_sources",
        "_cleanup_unresolvable_sign_holder_sources",
        "_reregister_non_effective_local_sources",
        "_backfill_airing_episode_sources",
        "_backfill_center_display_metadata",
        "_backfill_center_raw_repair_queue",
        "repair_logical_season_share_channels_from_115",
        "reconcile_logical_season_share_channels_full",
        "_sync_center_credit",
    ):
        monkeypatch.setattr(shared_resource_tasks, name, lambda *args, **kwargs: {})
    monkeypatch.setattr(shared_intro_service, "scan_and_upload_local_intro", lambda **kwargs: {})

    result = shared_resource_tasks.task_shared_resource_maintenance(maintenance_silent=True)

    assert result["device_event_listener"] == {"started": False, "owner": "coordinator"}


def test_api_cli_and_compose_set_the_process_role_explicitly(monkeypatch) -> None:
    import uvicorn

    from etk.platform import observability

    observed: dict[str, Any] = {}
    monkeypatch.delenv("ETK_PROCESS_ROLE", raising=False)
    monkeypatch.setattr(observability, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: observed.update(kwargs))

    args = argparse.Namespace(host="127.0.0.1", port=5257, log_level="info", log_format="console")
    assert cli._run_api(args) == 0
    assert observed["host"] == "127.0.0.1"
    assert os.environ["ETK_PROCESS_ROLE"] == "api"

    compose = yaml.safe_load((REPO_ROOT / "docker" / "compose.refactor.yml").read_text(encoding="utf-8"))
    assert compose["services"]["api"]["environment"]["ETK_PROCESS_ROLE"] == "api"
