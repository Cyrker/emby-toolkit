from __future__ import annotations

import builtins
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import text

import task_manager
from etk.compatibility import tg_resource_effects
from etk.compatibility.persistent_runtime_state import PostgresRuntimeStateBackend
from etk.compatibility.persistent_tasks import PersistentTaskManagerGateway
from etk.compatibility.tg_control_bridge import (
    DurableTelegramTaskQueue,
    TelegramCommandIdempotencyConflictError,
    TelegramControlBridge,
    submit_telegram_resource_action,
)
from etk.compatibility.tg_resource_effects import (
    TelegramResourceEffectBindingStore,
    TelegramResourceEffectStore,
    TelegramResourceSecretOwnershipError,
    TelegramResourceSecretStore,
)
from etk.domains.operations.jobs import JobSubmissionService
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import InvalidJsonPayloadError, PostgresJobStore
from etk.platform.tasks import TaskRegistry
from etk.processes.coordinator import CoordinatorRuntime
from etk.processes.runtime_config import TaskRuntimeConfig

SETTING_KEY = "tg_userbot_control_bridge"
TASK_FIXTURE = Path(__file__).resolve().parents[3] / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


def _bridge(runtime: DatabaseRuntime, *, clock: Any = None) -> TelegramControlBridge:
    assert runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(connection_factory=runtime.raw_pool.connection)
    return TelegramControlBridge(
        connection_factory=runtime.raw_pool.connection,
        clock=clock,
        resource_secrets=TelegramResourceSecretStore(backend),
    )


def _bridge_state(runtime: DatabaseRuntime) -> dict[str, Any]:
    with runtime.engine.connect() as connection:
        state = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": SETTING_KEY},
        )
    assert isinstance(state, dict)
    return state


@pytest.fixture(autouse=True)
def clean_tg_bridge_state(p4_runtime: DatabaseRuntime, monkeypatch):
    assert p4_runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(connection_factory=p4_runtime.raw_pool.connection)
    monkeypatch.setattr(tg_resource_effects, "_DEFAULT_STORE", TelegramResourceEffectStore(backend))
    monkeypatch.setattr(
        tg_resource_effects,
        "_DEFAULT_BINDING_STORE",
        TelegramResourceEffectBindingStore(backend),
    )
    monkeypatch.setattr(
        tg_resource_effects,
        "_DEFAULT_SECRET_STORE",
        TelegramResourceSecretStore(backend),
    )
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = :key OR setting_key LIKE :resource_pattern"),
            {"key": SETTING_KEY, "resource_pattern": "runtime.%telegram-resource.%"},
        )
    yield
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = :key OR setting_key LIKE :resource_pattern"),
            {"key": SETTING_KEY, "resource_pattern": "runtime.%telegram-resource.%"},
        )


def test_command_is_claimed_and_completed_across_bridge_instances(p4_runtime: DatabaseRuntime) -> None:
    api_bridge = _bridge(p4_runtime)
    coordinator_bridge = _bridge(p4_runtime)

    command_id = api_bridge.enqueue("login", {"code": "12345"})
    claimed = coordinator_bridge.claim_next("coordinator-a", lease_seconds=30)

    assert claimed is not None
    assert claimed["id"] == command_id
    assert claimed["payload"] == {"code": "12345"}
    coordinator_bridge.complete(command_id, "coordinator-a", {"success": True})

    completed = api_bridge.get_command(command_id)
    assert completed is not None
    assert completed["status"] == "succeeded"
    assert completed["result"] == {"success": True}


def test_expired_claim_is_recovered_by_the_next_coordinator(p4_runtime: DatabaseRuntime) -> None:
    now = [datetime(2026, 8, 23, tzinfo=UTC)]
    bridge = _bridge(p4_runtime, clock=lambda: now[0])
    command_id = bridge.enqueue("logout")

    first = bridge.claim_next("coordinator-a", lease_seconds=10)
    assert first is not None
    assert first["claimed_by"] == "coordinator-a"

    now[0] += timedelta(seconds=11)
    recovered = _bridge(p4_runtime, clock=lambda: now[0]).claim_next("coordinator-b", lease_seconds=10)
    assert recovered is not None
    assert recovered["id"] == command_id
    assert recovered["claimed_by"] == "coordinator-b"


def test_concurrent_claim_has_exactly_one_owner(p4_runtime: DatabaseRuntime) -> None:
    bridge = _bridge(p4_runtime)
    command_id = bridge.enqueue("clear_session")

    def claim(owner: str) -> dict[str, Any] | None:
        return _bridge(p4_runtime).claim_next(owner, lease_seconds=30)

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ("coordinator-a", "coordinator-b")))

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0]["id"] == command_id


def test_native_search_consumer_claims_only_search_commands_without_mutating_control_commands(
    p4_runtime: DatabaseRuntime,
) -> None:
    bridge = _bridge(p4_runtime)
    login_id = bridge.enqueue("login", {"code": "12345"})
    before = _bridge_state(p4_runtime)

    assert bridge.claim_next("native-search", command_types={"search"}) is None
    assert _bridge_state(p4_runtime) == before

    search_id = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key="native-search-filter",
        result_owner_key="a" * 64,
    )
    claimed = bridge.claim_next("native-search", command_types={"search"})

    assert claimed is not None
    assert claimed["id"] == search_id
    login = bridge.get_command(login_id)
    assert login is not None
    assert login["status"] == "pending"


def test_native_coordinator_consumes_search_without_importing_legacy_userbot(
    p4_runtime: DatabaseRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge = _bridge(p4_runtime)
    owner_key = "b" * 64
    command_id = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key="native-coordinator-search",
        result_owner_key=owner_key,
    )

    class Manager:
        def get_status(self) -> dict[str, Any]:
            return {"status": "ready"}

        def search_channel_resources(self, **payload: Any) -> dict[str, Any]:
            assert payload["query"] == "Fixture"
            return {
                "ok": True,
                "results": [{"tmdb_id": "42", "target_link": "https://115.com/s/private"}],
            }

    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime._container = SimpleNamespace(database=p4_runtime)
    runtime._native_telegram_search_manager = Manager()
    runtime._publisher_id = "fixture-native-coordinator"
    runtime.config = TaskRuntimeConfig()
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "handler.tg_userbot" or name.startswith("handler.tg_userbot."):
            raise AssertionError("native Coordinator imported legacy Telegram UserBot")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    runtime._process_native_telegram_search_commands()

    completed = bridge.get_command(command_id)
    assert completed is not None
    assert completed["status"] == "succeeded"
    receipt = completed["result"]
    assert bridge.consume_resource_secret(receipt["search_result_ref"], owner_key)["ok"] is True


def test_bridge_rejects_non_json_command_and_result_payloads(p4_runtime: DatabaseRuntime) -> None:
    bridge = _bridge(p4_runtime)

    with pytest.raises(InvalidJsonPayloadError):
        bridge.enqueue("search", {"bad": {1, 2}})

    command_id = bridge.enqueue("search", {"query": "Fixture"})
    assert bridge.claim_next("coordinator-a", lease_seconds=30) is not None
    with pytest.raises(InvalidJsonPayloadError):
        bridge.complete(command_id, "coordinator-a", {"bad": object()})


def test_replay_safe_command_enqueue_is_stable_and_rejects_request_drift(
    p4_runtime: DatabaseRuntime,
) -> None:
    bridge = _bridge(p4_runtime)
    payload = {"query": "Fixture", "limit": 10}
    owner_key = "a" * 64

    first = bridge.enqueue_once(
        "search",
        payload,
        idempotency_key="auto-subscribe:fixture:search",
        result_owner_key=owner_key,
    )
    replayed = bridge.enqueue_once(
        "search",
        payload,
        idempotency_key="auto-subscribe:fixture:search",
        result_owner_key=owner_key,
    )

    assert replayed == first
    with pytest.raises(TelegramCommandIdempotencyConflictError):
        bridge.enqueue_once(
            "search",
            {"query": "Changed", "limit": 10},
            idempotency_key="auto-subscribe:fixture:search",
            result_owner_key=owner_key,
        )
    with pytest.raises(TelegramCommandIdempotencyConflictError):
        bridge.enqueue_once(
            "search",
            payload,
            idempotency_key="auto-subscribe:fixture:search",
            result_owner_key="f" * 64,
        )
    with p4_runtime.engine.connect() as connection:
        state = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": SETTING_KEY},
        )
    assert len(state["commands"]) == 1
    assert state["commands"][0]["id"] == first
    assert len(state["idempotency"]) == 1
    assert "auto-subscribe:fixture:search" not in json.dumps(state, ensure_ascii=False, sort_keys=True)

    assert bridge.enqueue("search", payload) != bridge.enqueue("search", payload)


def test_replay_safe_command_reuses_claimed_identity_after_lease_expiry(
    p4_runtime: DatabaseRuntime,
) -> None:
    now = [datetime(2026, 8, 23, tzinfo=UTC)]
    bridge = _bridge(p4_runtime, clock=lambda: now[0])
    command_id = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key="auto-subscribe:lease:search",
        result_owner_key="b" * 64,
    )

    claimed = bridge.claim_next("coordinator-a", lease_seconds=10)
    assert claimed is not None
    assert claimed["id"] == command_id
    assert (
        bridge.enqueue_once(
            "search",
            {"query": "Fixture"},
            idempotency_key="auto-subscribe:lease:search",
            result_owner_key="b" * 64,
        )
        == command_id
    )

    now[0] += timedelta(seconds=11)
    recovered = bridge.claim_next("coordinator-b", lease_seconds=10)
    assert recovered is not None
    assert recovered["id"] == command_id
    assert recovered["claimed_by"] == "coordinator-b"


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
def test_replay_safe_command_keeps_terminal_identity_without_mutating_state(
    p4_runtime: DatabaseRuntime,
    terminal_status: str,
) -> None:
    bridge = _bridge(p4_runtime)
    key = f"auto-subscribe:terminal:{terminal_status}:search"
    command_id = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key=key,
        result_owner_key="e" * 64,
    )
    assert bridge.claim_next("coordinator-a", lease_seconds=30) is not None
    if terminal_status == "succeeded":
        bridge.complete(command_id, "coordinator-a", {"ok": False})
    else:
        bridge.fail(command_id, "coordinator-a", "fixture failure")

    before = _bridge_state(p4_runtime)
    replayed = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key=key,
        result_owner_key="e" * 64,
    )
    after = _bridge_state(p4_runtime)

    assert replayed == command_id
    assert after == before
    matching = [item for item in after["commands"] if item["id"] == command_id]
    assert len(matching) == 1
    assert matching[0]["status"] == terminal_status


def test_replay_safe_command_is_not_recreated_after_terminal_command_trim(
    p4_runtime: DatabaseRuntime,
) -> None:
    bridge = _bridge(p4_runtime)
    command_id = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key="auto-subscribe:trimmed:search",
        result_owner_key="f" * 64,
    )
    assert bridge.claim_next("coordinator-a", lease_seconds=30) is not None
    bridge.fail(command_id, "coordinator-a", "fixture failure")

    for index in range(201):
        disposable_id = bridge.enqueue("logout", {"index": index})
        claimed = bridge.claim_next("coordinator-a", lease_seconds=30)
        assert claimed is not None
        assert claimed["id"] == disposable_id
        bridge.complete(disposable_id, "coordinator-a", {"ok": True})

    before = _bridge_state(p4_runtime)
    assert bridge.get_command(command_id) is None
    assert any(item["command_id"] == command_id for item in before["idempotency"])

    replayed = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key="auto-subscribe:trimmed:search",
        result_owner_key="f" * 64,
    )

    assert replayed == command_id
    assert _bridge_state(p4_runtime) == before
    assert bridge.get_command(command_id) is None


def test_replay_safe_search_externalizes_raw_result_to_owner_bound_secret(
    p4_runtime: DatabaseRuntime,
) -> None:
    bridge = _bridge(p4_runtime)
    owner_key = "c" * 64
    raw_result = {
        "ok": True,
        "results": [
            {
                "tmdb_id": "42",
                "text": "raw Telegram message",
                "raw_text": "raw Telegram message",
                "target_link": "https://115.com/s/private-share",
                "receive_code": "2468",
            }
        ],
    }
    command_id = bridge.enqueue_once(
        "search",
        {"query": "Fixture"},
        idempotency_key="auto-subscribe:externalized:search",
        result_owner_key=owner_key,
    )
    assert bridge.claim_next("coordinator-a", lease_seconds=30) is not None

    completed = bridge.complete(command_id, "coordinator-a", raw_result)

    receipt = completed["result"]
    assert receipt["ok"] is True
    assert receipt["candidate_count"] == 1
    assert len(receipt["search_result_ref"]) == 64
    assert len(receipt["search_result_hash"]) == 64
    durable = json.dumps(completed, ensure_ascii=False, sort_keys=True)
    assert "private-share" not in durable
    assert "2468" not in durable
    assert "raw Telegram message" not in durable
    assert bridge.consume_resource_secret(receipt["search_result_ref"], owner_key) == raw_result
    with pytest.raises(TelegramResourceSecretOwnershipError):
        bridge.consume_resource_secret(receipt["search_result_ref"], "d" * 64)


def test_tg_transfer_facade_creates_a_postgres_backed_worker_job(
    p4_runtime: DatabaseRuntime,
    p4_store: PostgresJobStore,
) -> None:
    service = JobSubmissionService(p4_store, TaskRegistry.from_fixture(TASK_FIXTURE))
    gateway = PersistentTaskManagerGateway(service)
    raw_url = "magnet:?xt=urn:btih:fixture"
    payload = {
        "type": "offline_download",
        "url": raw_url,
        "raw_text": f"private {raw_url}",
        "candidate": {"source_chat_id": "-10042", "message_id": 99},
        "metadata": {"source": "test"},
    }
    task_manager.install_persistent_submission_gateway(gateway)
    try:
        assert DurableTelegramTaskQueue().put(payload)
        assert DurableTelegramTaskQueue().put(payload)
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)

    projection = p4_store.get_current_job()
    assert projection is not None
    assert projection.job.queue_name == "legacy.backlog"
    assert projection.job.task_key.startswith("legacy-operation:")
    safe_payload = projection.job.request_payload["parameters"]["payload"]
    assert safe_payload["type"] == "offline_download"
    assert safe_payload["resource_kind"] == "p115_offline"
    assert safe_payload["candidate"] == {"source_chat_id": "-10042", "message_id": 99}
    assert safe_payload["metadata"] == {"source": "test"}
    assert len(safe_payload["delivery_key"]) == 64
    assert safe_payload["secret_ref"] == safe_payload["delivery_key"]
    assert len(safe_payload["semantic_source_hash"]) == 64
    serialized_job = json.dumps(projection.job.request_payload, ensure_ascii=False, sort_keys=True)
    assert raw_url not in serialized_job
    assert "private" not in serialized_job
    assert projection.job.request_payload["legacy_operation"] == {
        "module": "tasks.subscriptions",
        "qualname": "task_process_channel_transfer",
    }
    with p4_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM job_runs")) == 1
        secret_state = connection.execute(
            text("SELECT value_json FROM app_settings WHERE setting_key = :setting_key"),
            {"setting_key": f"runtime.secret.telegram-resource.{safe_payload['secret_ref']}"},
        ).scalar_one()
    assert raw_url in json.dumps(secret_state, ensure_ascii=False, sort_keys=True)


def test_resource_action_submission_is_postgres_idempotent_and_retryable(
    p4_runtime: DatabaseRuntime,
    p4_store: PostgresJobStore,
) -> None:
    service = JobSubmissionService(p4_store, TaskRegistry.from_fixture(TASK_FIXTURE))
    gateway = PersistentTaskManagerGateway(service)
    action = {
        "version": 1,
        "action": "p115_offline_add",
        "chat_id": "42",
        "update_id": "update-1",
        "source_identity": "p115_offline:fixture",
        "payload": {"url": "magnet:?xt=urn:btih:" + "A" * 40},
    }
    task_manager.install_persistent_submission_gateway(gateway)
    try:
        assert submit_telegram_resource_action(action)
        assert submit_telegram_resource_action(action)
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)

    with p4_runtime.engine.connect() as connection:
        counts = (
            connection.execute(
                text(
                    """
                SELECT
                    (SELECT count(*) FROM job_runs) AS jobs,
                    (SELECT count(*) FROM job_attempts) AS attempts,
                    (SELECT count(*) FROM idempotency_records) AS idempotency
                """
                )
            )
            .mappings()
            .one()
        )
        max_attempts = connection.scalar(text("SELECT max_attempts FROM job_runs"))
        request_payload = connection.execute(text("SELECT request_payload_json FROM job_runs")).scalar_one()

    assert dict(counts) == {"jobs": 1, "attempts": 1, "idempotency": 1}
    assert max_attempts == 6
    assert action["payload"]["url"] not in json.dumps(request_payload, ensure_ascii=False, sort_keys=True)
