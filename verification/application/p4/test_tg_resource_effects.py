from __future__ import annotations

import json
from contextlib import nullcontext
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from etk.compatibility import tg_resource_effects
from etk.compatibility import tg_resource_operations as operations
from etk.compatibility.tg_control_bridge import _externalize_resource_action, telegram_resource_action_key
from etk.compatibility.tg_resource_effects import (
    TelegramResourceEffectBindingStore,
    TelegramResourceEffectStore,
    TelegramResourceSecretExpiredError,
    TelegramResourceSecretOwnershipError,
    TelegramResourceSecretStore,
)
from etk.platform.jobs import normalize_json_object


class MemoryEffectBackend:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 23, tzinfo=UTC)
        self.states: dict[str, dict[str, Any]] = {}

    def mutate(self, setting_key, mutation):
        current = deepcopy(self.states.get(setting_key))
        next_state, result, should_write = mutation(current, self.now)
        if should_write:
            if next_state is None:
                self.states.pop(setting_key, None)
            else:
                self.states[setting_key] = normalize_json_object(next_state, field="effect state")
        return deepcopy(result)

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _raw_action(name: str, payload: dict[str, Any], *, update_id: str = "update-1") -> dict[str, Any]:
    return {
        "version": 1,
        "action": name,
        "chat_id": "42",
        "update_id": update_id,
        "source_identity": f"fixture:{name}",
        "payload": payload,
    }


def _install_stores(monkeypatch):
    backend = MemoryEffectBackend()
    effect_store = TelegramResourceEffectStore(backend)
    binding_store = TelegramResourceEffectBindingStore(backend)
    secret_store = TelegramResourceSecretStore(backend)
    monkeypatch.setattr(tg_resource_effects, "_DEFAULT_STORE", effect_store)
    monkeypatch.setattr(tg_resource_effects, "_DEFAULT_BINDING_STORE", binding_store)
    monkeypatch.setattr(tg_resource_effects, "_DEFAULT_SECRET_STORE", secret_store)
    monkeypatch.setattr(operations, "_EFFECT_SETTLE_SECONDS", 0)
    return effect_store, binding_store, secret_store, backend


def _safe_action(name: str, payload: dict[str, Any], *, update_id: str = "update-1") -> dict[str, Any]:
    raw = _raw_action(name, payload, update_id=update_id)
    delivery_key = telegram_resource_action_key(raw)
    safe, _created = _externalize_resource_action(raw, delivery_key)
    return safe


class FakeP115Client:
    account_id = "115001"

    def __init__(self) -> None:
        self.imported = False
        self.import_calls = 0
        self.offline_calls = 0
        self.share_response: Any = {"state": True}
        self.offline_response: Any = {"state": True}
        self.offline_reconcile_status = "unknown"

    def share_info(self, *_args, **_kwargs):
        return {"state": True, "data": {"list": [{"cid": "source-1", "n": "Fixture", "fc": "0"}]}}

    def fs_files(self, _payload):
        items = [{"cid": "target-new", "n": "Fixture", "fc": "0"}] if self.imported else []
        return {"state": True, "data": items}

    def share_import(self, *_args):
        self.import_calls += 1
        if isinstance(self.share_response, dict) and self.share_response.get("state") is True:
            self.imported = True
        return self.share_response

    def offline_add_urls(self, _payload):
        self.offline_calls += 1
        return self.offline_response

    def offline_task_reconcile(self, _url, _target_cid):
        return {"status": self.offline_reconcile_status}


def _install_client(monkeypatch, client: FakeP115Client, current: dict[str, str] | None = None) -> None:
    state = current or {"account_id": client.account_id, "target_cid": "target-1"}

    def resolve(*, expected_account_id=None, expected_target_cid=None):
        if expected_account_id is not None and expected_account_id != state["account_id"]:
            raise RuntimeError("115 account changed")
        if expected_target_cid is not None and expected_target_cid != state["target_cid"]:
            raise RuntimeError("115 target changed")
        client.account_id = state["account_id"]
        return client, state["target_cid"]

    monkeypatch.setattr(operations, "_p115_client_and_target", resolve)


def test_effect_binding_is_semantic_and_secret_store_is_owner_bound_and_expiring(monkeypatch) -> None:
    effect_store, binding_store, secret_store, backend = _install_stores(monkeypatch)
    delivery_key = "a" * 64
    source_hash = "b" * 64
    binding, created = binding_store.bind(
        delivery_key,
        "p115_offline_add",
        source_hash,
        "115001",
        "target-1",
    )
    repeated, repeated_created = binding_store.bind(
        delivery_key,
        "p115_offline_add",
        source_hash,
        "115001",
        "target-1",
    )
    assert created is True
    assert repeated_created is False
    assert repeated.effect_key == binding.effect_key
    assert binding.effect_key != delivery_key

    reference, secret_created = secret_store.create(
        {"urls": ["magnet:?xt=urn:btih:PRIVATE"]},
        reference=delivery_key,
        owner_key=delivery_key,
        ttl_seconds=30,
    )
    assert secret_created is True
    assert secret_store.consume(reference, delivery_key).payload["urls"]
    with pytest.raises(TelegramResourceSecretOwnershipError):
        secret_store.consume(reference, "c" * 64)
    backend.advance(31)
    with pytest.raises(TelegramResourceSecretExpiredError):
        secret_store.consume(reference, delivery_key)

    prepared, _ = effect_store.prepare(
        "d" * 64,
        "p115_offline_add",
        {"source_hash": "e" * 64, "target_cid": "target-1"},
    )
    assert prepared.status == "prepared"
    with pytest.raises(ValueError, match="cannot persist"):
        effect_store.prepare("f" * 64, "p115_share_import", {"share_code": "private"})


def test_share_crash_before_success_receipt_reconciles_without_replay(monkeypatch) -> None:
    effect_store, _binding_store, _secret_store, _backend = _install_stores(monkeypatch)
    client = FakeP115Client()
    _install_client(monkeypatch, client)
    action = _safe_action("p115_share_import", {"share_code": "abc123", "receive_code": "a1b2"})
    real_mark = effect_store.mark_succeeded
    mark_calls = 0

    def crash_once(effect_key, action_name, evidence=None, **kwargs):
        nonlocal mark_calls
        mark_calls += 1
        if mark_calls == 1:
            client.imported = True
            raise RuntimeError("crash before success mark")
        return real_mark(effect_key, action_name, evidence, **kwargs)

    monkeypatch.setattr(tg_resource_effects, "mark_effect_succeeded", crash_once)
    with pytest.raises(RuntimeError, match="crash before success mark"):
        operations.task_execute_telegram_resource_action(action=action)
    result = operations.task_execute_telegram_resource_action(action=action)

    assert result["result"]["reconciled"] is True
    assert result["result"]["organize_submitted"] is False
    assert result["result"]["organize_plan_required"] is True
    assert client.import_calls == 1


@pytest.mark.parametrize(
    ("action_name", "payload", "call_attribute"),
    [
        ("p115_share_import", {"share_code": "abc123", "receive_code": "a1b2"}, "import_calls"),
        ("p115_offline_add", {"url": "magnet:?xt=urn:btih:" + "C" * 40}, "offline_calls"),
    ],
)
def test_success_and_replay_require_an_approved_plan_without_legacy_organize_submission(
    monkeypatch,
    action_name: str,
    payload: dict[str, Any],
    call_attribute: str,
) -> None:
    _effect_store, _binding_store, _secret_store, backend = _install_stores(monkeypatch)
    client = FakeP115Client()
    _install_client(monkeypatch, client)
    action = _safe_action(action_name, payload)

    first = operations.task_execute_telegram_resource_action(action=action)
    replay = operations.task_execute_telegram_resource_action(action=action)

    for result in (first, replay):
        assert result["ok"] is True
        assert result["result"]["organize_submitted"] is False
        assert result["result"]["organize_plan_required"] is True
    assert getattr(client, call_attribute) == 1
    assert not any(key.startswith("runtime.secret.telegram-resource.") for key in backend.states)


def test_share_list_miss_remains_unknown_and_never_replays(monkeypatch) -> None:
    _install_stores(monkeypatch)
    client = FakeP115Client()
    client.share_response = None
    _install_client(monkeypatch, client)
    action = _safe_action("p115_share_import", {"share_code": "abc123", "receive_code": "a1b2"})

    with pytest.raises(operations.TelegramResourceEffectPendingError):
        operations.task_execute_telegram_resource_action(action=action)
    with pytest.raises(operations.TelegramResourceEffectPendingError):
        operations.task_execute_telegram_resource_action(action=action)

    assert client.import_calls == 1


@pytest.mark.parametrize("drift", ["account_id", "target_cid"])
def test_retry_fails_closed_when_account_or_target_drifts(monkeypatch, drift: str) -> None:
    _install_stores(monkeypatch)
    client = FakeP115Client()
    client.offline_response = None
    current = {"account_id": "115001", "target_cid": "target-1"}
    _install_client(monkeypatch, client, current)
    action = _safe_action("p115_offline_add", {"url": "magnet:?xt=urn:btih:" + "A" * 40})

    with pytest.raises(operations.TelegramResourceEffectPendingError):
        operations.task_execute_telegram_resource_action(action=action)
    current[drift] = "115002" if drift == "account_id" else "target-2"
    with pytest.raises(RuntimeError, match="changed"):
        operations.task_execute_telegram_resource_action(action=action)
    assert client.offline_calls == 1


@pytest.mark.parametrize(("reconcile_status", "expected_calls"), [("unknown", 1), ("absent", 2)])
def test_offline_uncertain_response_replays_only_after_explicit_absence(
    monkeypatch,
    reconcile_status: str,
    expected_calls: int,
) -> None:
    _install_stores(monkeypatch)
    client = FakeP115Client()
    client.offline_response = None
    client.offline_reconcile_status = reconcile_status
    _install_client(monkeypatch, client)
    action = _safe_action("p115_offline_add", {"url": "magnet:?xt=urn:btih:" + "B" * 40})

    with pytest.raises(operations.TelegramResourceEffectPendingError):
        operations.task_execute_telegram_resource_action(action=action)
    if reconcile_status == "unknown":
        with pytest.raises(operations.TelegramResourceEffectPendingError):
            operations.task_execute_telegram_resource_action(action=action)
    else:
        client.offline_response = {"state": True}
        assert operations.task_execute_telegram_resource_action(action=action)["ok"] is True
    assert client.offline_calls == expected_calls


def test_job_and_effect_records_never_contain_raw_resource_secrets(monkeypatch) -> None:
    _effect_store, _binding_store, _secret_store, backend = _install_stores(monkeypatch)
    client = FakeP115Client()
    _install_client(monkeypatch, client)
    raw_url = "magnet:?xt=urn:btih:" + "D" * 40
    raw_action = _raw_action("p115_offline_add", {"url": raw_url})
    delivery_key = telegram_resource_action_key(raw_action)
    action, _created = _externalize_resource_action(raw_action, delivery_key)

    serialized_job = json.dumps(action, ensure_ascii=False, sort_keys=True)
    assert raw_url not in serialized_job
    assert "magnet:?" not in serialized_job
    operations.task_execute_telegram_resource_action(action=action)

    durable_nonsecret = {
        key: value for key, value in backend.states.items() if not key.startswith("runtime.secret.telegram-resource.")
    }
    serialized_ledger = json.dumps(durable_nonsecret, ensure_ascii=False, sort_keys=True)
    assert raw_url not in serialized_ledger
    assert "magnet:?" not in serialized_ledger
    assert not any(key.startswith("runtime.secret.telegram-resource.") for key in backend.states)

    repeated, repeated_created = _externalize_resource_action(raw_action, delivery_key)
    assert repeated_created is False
    assert repeated["payload"]["secret_ref"] == delivery_key
    assert not any(key.startswith("runtime.secret.telegram-resource.") for key in backend.states)

    changed_action = _raw_action(
        "p115_offline_add",
        {"url": "magnet:?xt=urn:btih:" + "E" * 40},
    )
    with pytest.raises(RuntimeError, match="changed semantic effect identity"):
        _externalize_resource_action(changed_action, delivery_key)


def test_provider_exception_is_sanitized_while_prepared_receipt_is_retained(monkeypatch) -> None:
    _effect_store, _binding_store, _secret_store, backend = _install_stores(monkeypatch)
    client = FakeP115Client()
    _install_client(monkeypatch, client)
    raw_url = "magnet:?xt=urn:btih:" + "F" * 40

    def fail_with_raw_secret(_request):
        raise RuntimeError(f"provider echoed {raw_url}")

    monkeypatch.setattr(client, "offline_add_urls", fail_with_raw_secret)
    action = _safe_action("p115_offline_add", {"url": raw_url})

    with pytest.raises(operations.TelegramResourceEffectPendingError) as captured:
        operations.task_execute_telegram_resource_action(action=action)

    assert raw_url not in str(captured.value)
    assert captured.value.__cause__ is None
    effect_states = {
        key: value for key, value in backend.states.items() if key.startswith("runtime.effect.telegram-resource.")
    }
    assert effect_states
    assert all(state["status"] == "prepared" for state in effect_states.values())
    assert raw_url not in json.dumps(effect_states, ensure_ascii=False, sort_keys=True)


def test_p115_expected_account_context_propagates_and_conflicts_fail_closed(monkeypatch) -> None:
    from handler import p115_service

    observed: list[str | None] = []
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", lambda: nullcontext())
    monkeypatch.setattr(
        p115_service.P115Service,
        "_get_client_locked",
        classmethod(lambda _cls, expected_account_id=None: observed.append(expected_account_id) or object()),
    )

    with p115_service.p115_expected_account("115001"):
        p115_service.P115Service.get_client()
        p115_service.P115Service.get_client(expected_account_id="115001")
        with pytest.raises(RuntimeError, match="context conflict"):
            p115_service.P115Service.get_client(expected_account_id="115002")
        with pytest.raises(RuntimeError, match="context conflict"):
            with p115_service.p115_expected_account("115002"):
                pass
    p115_service.P115Service.get_client()

    assert observed == ["115001", "115001", None]
