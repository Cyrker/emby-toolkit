from __future__ import annotations

import builtins
import hashlib
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
    TelegramResourceSecretExpiredError,
    TelegramResourceSecretOwnershipError,
    TelegramResourceSecretStore,
)
from etk.domains.operations.jobs import JobSubmissionService
from etk.domains.subscription.telegram_listener import DurableTelegramRealtimeIngress
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


def test_expired_checkpointed_control_command_is_indeterminate_and_never_reclaimed(
    p4_runtime: DatabaseRuntime,
) -> None:
    now = [datetime(2026, 8, 23, tzinfo=UTC)]
    bridge = _bridge(p4_runtime, clock=lambda: now[0])
    command_id = bridge.enqueue_once(
        "logout",
        {},
        idempotency_key="fixture-logout-no-resend",
    )
    claimed = bridge.claim_next("coordinator-a", lease_seconds=10)
    assert claimed is not None
    checkpointed = bridge.checkpoint_request(command_id, "coordinator-a")
    assert checkpointed["status"] == "requesting"

    now[0] += timedelta(seconds=11)
    assert _bridge(p4_runtime, clock=lambda: now[0]).claim_next("coordinator-b", lease_seconds=10) is None
    recovered = bridge.get_command(command_id)
    assert recovered is not None
    assert recovered["status"] == "indeterminate"
    assert recovered["payload"] == {}
    assert bridge.enqueue_once("logout", {}, idempotency_key="fixture-logout-no-resend") == command_id


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
    runtime._native_telegram_search_manager = Manager()  # type: ignore[assignment]
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


def test_native_coordinator_consumes_control_command_without_importing_legacy_userbot(
    p4_runtime: DatabaseRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge = _bridge(p4_runtime)
    command_id = bridge.enqueue_once(
        "logout",
        {},
        idempotency_key="native-coordinator-logout",
    )

    class Manager:
        def __init__(self) -> None:
            self.logout_calls = 0

        def get_status(self) -> dict[str, Any]:
            return {"status": "unauthorized"}

        def logout(self) -> dict[str, Any]:
            self.logout_calls += 1
            return {"success": True}

    manager = Manager()
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime._container = SimpleNamespace(database=p4_runtime)
    runtime._native_telegram_search_manager = manager  # type: ignore[assignment]
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
    assert manager.logout_calls == 1


def test_native_listener_ingress_externalizes_candidate_and_coordinator_stages_it_once(
    p4_runtime: DatabaseRuntime,
) -> None:
    assert p4_runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(connection_factory=p4_runtime.raw_pool.connection)
    secrets = TelegramResourceSecretStore(backend)
    bridge = TelegramControlBridge(
        connection_factory=p4_runtime.raw_pool.connection,
        resource_secrets=secrets,
    )
    ingress = DurableTelegramRealtimeIngress(bridge)
    occurrence = f"telegram-listener:{'a' * 64}"
    raw_url = "https://115.com/s/provider-secret"

    assert ingress.stage(
        {"tmdb_id": "42", "target_link": raw_url},
        occurrence=occurrence,
        is_brainless=True,
        is_keyword_matched=False,
        is_subscribe=False,
        is_hdhive_push=False,
    )
    state = _bridge_state(p4_runtime)
    assert raw_url not in json.dumps(state, ensure_ascii=False, sort_keys=True)
    command_id = state["commands"][0]["id"]
    with p4_runtime.engine.connect() as connection:
        secret_count = connection.scalar(
            text("SELECT COUNT(*) FROM app_settings WHERE setting_key LIKE 'runtime.secret.telegram-resource.%'")
        )
    assert int(secret_count or 0) == 1

    class Manager:
        def __init__(self) -> None:
            self.calls = 0

        def get_status(self) -> dict[str, Any]:
            return {"status": "authorized"}

        def stage_listener_action(self, candidate: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
            assert candidate["target_link"] == raw_url
            assert kwargs["occurrence"] == occurrence
            self.calls += 1
            return {"success": True, "staged": True}

    manager = Manager()
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime._container = SimpleNamespace(database=p4_runtime)
    runtime._native_telegram_search_manager = manager  # type: ignore[assignment]
    runtime._publisher_id = "fixture-native-listener"
    runtime.config = TaskRuntimeConfig()

    runtime._process_native_telegram_search_commands()

    completed = bridge.get_command(command_id)
    assert completed is not None
    assert completed["status"] == "succeeded"
    assert completed["result"] == {"success": True, "staged": True}
    assert manager.calls == 1
    with p4_runtime.engine.connect() as connection:
        secret_count = connection.scalar(
            text("SELECT COUNT(*) FROM app_settings WHERE setting_key LIKE 'runtime.secret.telegram-resource.%'")
        )
    assert int(secret_count or 0) == 0


def test_listener_action_retry_is_delayed_bounded_and_cleans_candidate_secret(
    p4_runtime: DatabaseRuntime,
) -> None:
    now = [datetime(2026, 8, 23, tzinfo=UTC)]
    bridge = _bridge(p4_runtime, clock=lambda: now[0])
    reference = "a" * 64
    idempotency_key = "telegram-listener-action:bounded-retry"
    owner_key = hashlib.sha256(idempotency_key.encode()).hexdigest()
    command_id = bridge.enqueue_listener_action_once(
        {
            "candidate_ref": reference,
            "candidate_hash": "c" * 64,
            "occurrence": f"telegram-listener:{'d' * 64}",
            "is_brainless": True,
        },
        secret_payload={"candidate": {"target_link": "https://115.com/s/provider-secret"}},
        reference=reference,
        owner_key=owner_key,
        ttl_seconds=3600,
        idempotency_key=idempotency_key,
    )

    for attempt in range(1, 9):
        claimed = bridge.claim_next("coordinator-a", lease_seconds=30, command_types={"listener_action"})
        assert claimed is not None
        assert claimed["id"] == command_id
        assert claimed["attempt_count"] == attempt
        released = bridge.release(command_id, "coordinator-a", "ConnectionError")
        if attempt < 8:
            assert released["status"] == "pending"
            assert (
                bridge.claim_next(
                    "coordinator-a",
                    lease_seconds=30,
                    command_types={"listener_action"},
                )
                is None
            )
            now[0] += timedelta(seconds=301)
        else:
            assert released["status"] == "failed"
            assert released["payload"] == {}

    with pytest.raises(TelegramResourceSecretExpiredError):
        bridge.consume_resource_secret(reference, owner_key)


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


def test_legacy_tg_transfer_facade_fails_closed_without_dynamic_worker_job(
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
        with pytest.raises(RuntimeError, match="persistent task submission was rejected"):
            DurableTelegramTaskQueue().put(payload)
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)

    assert p4_store.get_current_job() is None
    with p4_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM job_runs")) == 0
        assert (
            connection.scalar(
                text("SELECT count(*) FROM app_settings WHERE setting_key LIKE 'runtime.secret.telegram-resource.%'")
            )
            == 0
        )


def test_legacy_resource_action_submission_fails_closed_without_dynamic_job(
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
        with pytest.raises(RuntimeError, match="persistent task submission was rejected"):
            submit_telegram_resource_action(action)
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
        secret_count = connection.scalar(
            text("SELECT count(*) FROM app_settings WHERE setting_key LIKE 'runtime.secret.telegram-resource.%'")
        )

    assert dict(counts) == {"jobs": 0, "attempts": 0, "idempotency": 0}
    assert secret_count == 0
