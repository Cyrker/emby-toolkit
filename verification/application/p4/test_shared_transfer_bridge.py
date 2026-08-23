from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from flask import Flask

from etk.platform.jobs import PersistentEffectIndeterminate, PersistentExecutionCancelled
from handler.shared_center_client import SharedCenterClient
from routes import shared_resource, subscription
from tasks import shared_resource_tasks

JOB_ID = UUID("00000000-0000-0000-0000-000000000188")


class FakeEffectStore:
    def __init__(self) -> None:
        self.records = {}

    def prepare(self, key, evidence):
        existing = self.records.get(key)
        if existing is not None:
            return existing, False
        record = SimpleNamespace(status="prepared", evidence=dict(evidence))
        self.records[key] = record
        return record, True

    def mark_succeeded(self, key):
        self.records[key] = SimpleNamespace(status="succeeded", evidence=self.records[key].evidence)
        return self.records[key]

    def mark_indeterminate(self, key, evidence=None):
        current = self.records.get(key) or SimpleNamespace(evidence={})
        self.records[key] = SimpleNamespace(
            status="indeterminate",
            evidence={**current.evidence, **dict(evidence or {})},
        )
        return self.records[key]

    def discard_prepared(self, key):
        record = self.records.get(key)
        if record is None or record.status != "prepared":
            return False
        del self.records[key]
        return True

    def read(self, key):
        return self.records.get(key)


class FakeSecretStore:
    def __init__(self) -> None:
        self.records = {}
        self.destroyed = []

    def create(self, payload, *, owner_key, reference, **_kwargs):
        self.records[reference] = SimpleNamespace(
            reference=reference,
            owner_key=owner_key,
            payload=dict(payload),
        )
        return reference, True

    def consume(self, reference, owner_key):
        record = self.records[reference]
        assert record.owner_key == owner_key
        return record

    def destroy(self, reference, owner_key):
        record = self.records.get(reference)
        if record is not None:
            assert record.owner_key == owner_key
            del self.records[reference]
        self.destroyed.append((reference, owner_key))
        return record is not None


def _persistent_kwargs(action, source, *, mode="rapid", virtual_id=None) -> dict[str, Any]:
    account_id = "account-fixture"
    target_cid = "target-fixture"
    return {
        "intent_id": "intent-fixture",
        "expected_account_id": account_id,
        "expected_target_cid": target_cid,
        "effect_key": shared_resource_tasks._persistent_transfer_effect_identity(
            action,
            source,
            account_id,
            target_cid,
            mode,
            virtual_id,
        ),
    }


def _install_worker_effect_fakes(monkeypatch, store):
    monkeypatch.setattr(shared_resource_tasks, "SharedTransferEffectStore", lambda: store)
    monkeypatch.setattr(shared_resource_tasks, "p115_expected_account", lambda _account: nullcontext())
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "persistent_transfer_target",
        lambda _target, **_kwargs: nullcontext(),
    )


def test_center_import_submits_without_executing_the_transfer_in_api(monkeypatch) -> None:
    monkeypatch.setattr(shared_resource, "_center_import_normalize_source", lambda source: dict(source))
    monkeypatch.setattr(shared_resource, "_center_source_transfer_preflight", lambda _source: {"ok": True})
    monkeypatch.setattr(
        shared_resource,
        "_shared_resource_config_payload",
        lambda: {"p115_shared_virtual_import_enabled": False},
    )
    monkeypatch.setattr(
        shared_resource.shared_tasks,
        "consume_device_event_with_transfer_gate",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not execute transfer")),
    )
    captured = {}

    def submit(action, source, **kwargs):
        captured.update({"action": action, "source": source, **kwargs})
        return SimpleNamespace(job_id=JOB_ID)

    monkeypatch.setattr(shared_resource.shared_tasks, "submit_shared_center_transfer_job", submit)
    app = Flask(__name__)
    source = {"source_kind": "movie", "source_id": "fixture-source"}

    with app.test_request_context(json={"source": source}):
        response = shared_resource.api_center_import.__wrapped__()

    assert response.get_json()["job_id"] == str(JOB_ID)
    assert captured == {"action": "center_import", "source": source, "intent_id": None}


def test_virtual_promote_submission_failure_does_not_change_business_state(monkeypatch) -> None:
    monkeypatch.setattr(
        shared_resource.shared_virtual_db,
        "get_virtual_import",
        lambda _virtual_id: {
            "source_kind": "movie",
            "source_id": "fixture-source",
            "source_payload_json": {"source_kind": "movie", "source_id": "fixture-source"},
        },
    )
    monkeypatch.setattr(
        shared_resource.shared_virtual_db,
        "mark_active_washing_for_virtual_import",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("submission must not change state")),
    )
    monkeypatch.setattr(
        shared_resource.shared_virtual_db,
        "update_virtual_import",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("submission must not change state")),
    )
    monkeypatch.setattr(
        shared_resource.shared_tasks,
        "submit_shared_center_transfer_job",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("queue unavailable")),
    )
    app = Flask(__name__)

    with app.test_request_context():
        response, status = shared_resource.api_promote_virtual_import.__wrapped__(17)

    assert status == 503
    assert response.get_json()["success"] is False


def test_virtual_promote_worker_owns_state_and_rolls_back_on_transfer_failure(monkeypatch) -> None:
    events = []
    store = FakeEffectStore()
    _install_worker_effect_fakes(monkeypatch, store)
    monkeypatch.setattr(
        shared_resource_tasks.shared_virtual_db,
        "mark_active_washing_for_virtual_import",
        lambda virtual_id, enabled: events.append(("washing", virtual_id, enabled)) or 1,
    )
    monkeypatch.setattr(
        shared_resource_tasks.shared_virtual_db,
        "update_virtual_import",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("failed transfer must not promote")),
    )
    monkeypatch.setattr(
        shared_resource_tasks,
        "consume_device_event_with_transfer_gate",
        lambda *_args, **_kwargs: {"ok": False, "message": "fixture transfer failed"},
    )

    source = {"source_kind": "movie", "source_id": "fixture-source"}
    with pytest.raises(PersistentEffectIndeterminate, match="fixture transfer failed"):
        shared_resource_tasks.run_shared_center_transfer_persistent(
            "virtual_promote",
            source,
            virtual_id=17,
            event_id="fixture-event",
            **_persistent_kwargs("virtual_promote", source, virtual_id=17),
        )

    assert events == [("washing", 17, True)]
    assert store.records[next(iter(store.records))].status == "indeterminate"


def test_virtual_promote_worker_sets_promoting_only_after_transfer_succeeds(monkeypatch) -> None:
    events = []
    _install_worker_effect_fakes(monkeypatch, FakeEffectStore())
    monkeypatch.setattr(
        shared_resource_tasks.shared_virtual_db,
        "mark_active_washing_for_virtual_import",
        lambda virtual_id, enabled: events.append(("washing", virtual_id, enabled)) or 1,
    )
    monkeypatch.setattr(
        shared_resource_tasks,
        "consume_device_event_with_transfer_gate",
        lambda event, **_kwargs: events.append(("transfer", event["payload_json"]["_virtual_id"])) or {"ok": True},
    )
    monkeypatch.setattr(
        shared_resource_tasks.shared_virtual_db,
        "update_virtual_import",
        lambda virtual_id, **values: events.append(("update", virtual_id, values)) or {"id": virtual_id, **values},
    )

    source = {"source_kind": "movie", "source_id": "fixture-source"}
    result = shared_resource_tasks.run_shared_center_transfer_persistent(
        "virtual_promote",
        source,
        virtual_id=17,
        event_id="fixture-event",
        **_persistent_kwargs("virtual_promote", source, virtual_id=17),
    )

    assert result["ok"] is True
    assert events == [
        ("washing", 17, True),
        ("transfer", 17),
        ("update", 17, {"status": "promoting", "promoted_at": "NOW()"}),
    ]


def test_virtual_promote_cancel_before_provider_releases_receipt_and_washing(monkeypatch) -> None:
    events = []
    checkpoints = 0
    store = FakeEffectStore()
    _install_worker_effect_fakes(monkeypatch, store)

    def checkpoint():
        nonlocal checkpoints
        checkpoints += 1
        if checkpoints == 2:
            raise PersistentExecutionCancelled("cancel before provider")

    monkeypatch.setattr(shared_resource_tasks.task_manager, "persistent_execution_checkpoint", checkpoint)
    monkeypatch.setattr(
        shared_resource_tasks.shared_virtual_db,
        "mark_active_washing_for_virtual_import",
        lambda virtual_id, enabled: events.append((virtual_id, enabled)) or 1,
    )
    monkeypatch.setattr(
        shared_resource_tasks,
        "consume_device_event_with_transfer_gate",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("provider must not run")),
    )

    source = {"source_kind": "movie", "source_id": "fixture-source"}
    with pytest.raises(PersistentExecutionCancelled, match="cancel before provider"):
        shared_resource_tasks.run_shared_center_transfer_persistent(
            "virtual_promote",
            source,
            virtual_id=17,
            **_persistent_kwargs("virtual_promote", source, virtual_id=17),
        )

    assert events == [(17, True), (17, False)]
    assert store.records == {}


def test_cloud_download_defers_season_expansion_and_transfer_to_worker(monkeypatch) -> None:
    monkeypatch.setattr(subscription, "shared_center_enabled", lambda: True)
    monkeypatch.setattr(
        subscription,
        "SharedCenterClient",
        lambda: (_ for _ in ()).throw(AssertionError("API must not load season children")),
    )
    captured = {}

    def submit(action, source, **kwargs):
        captured.update({"action": action, "source": source, **kwargs})
        return SimpleNamespace(job_id=JOB_ID)

    monkeypatch.setattr(subscription, "submit_shared_center_transfer_job", submit)
    app = Flask(__name__)
    resource = {"source_kind": "season_hub", "source_id": "fixture-season"}

    with app.test_request_context(json={"source_type": "shared_pool", "resource": resource}):
        response = subscription.trigger_cloud_download.__wrapped__()

    assert response.get_json()["job_id"] == str(JOB_ID)
    assert captured == {
        "action": "cloud_download",
        "source": resource,
        "mode": "rapid",
        "intent_id": None,
    }


def test_cloud_download_worker_expands_season_before_transfer(monkeypatch) -> None:
    class Center:
        def list_display_children(self, **_kwargs):
            return {"children": [{"source_id": "episode-1"}]}

    captured = {}
    _install_worker_effect_fakes(monkeypatch, FakeEffectStore())
    monkeypatch.setattr(shared_resource_tasks, "SharedCenterClient", Center)
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "consume_center_source_payload",
        lambda source, **kwargs: captured.update({"source": source, **kwargs}) or {"ok": True},
    )

    source = {"source_kind": "season_hub", "source_id": "fixture-season"}
    result = shared_resource_tasks.run_shared_center_transfer_persistent(
        "cloud_download",
        source,
        mode="share",
        **_persistent_kwargs("cloud_download", source, mode="share"),
    )

    assert result["ok"] is True
    assert captured["mode"] == "share"
    assert captured["source"]["children"] == [{"source_id": "episode-1"}]
    assert captured["source"]["pack_items"] == [{"source_id": "episode-1"}]


def test_shared_transfer_delivery_intent_is_separate_from_effect_identity(monkeypatch) -> None:
    calls = []
    secrets = FakeSecretStore()
    monkeypatch.setattr(shared_resource_tasks, "get_115_account_id", lambda: "account-fixture")
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "_configured_target_cid",
        lambda: "target-fixture",
    )
    monkeypatch.setattr(
        shared_resource_tasks,
        "submit_persistent_background",
        lambda *args, **kwargs: calls.append({"args": args, **kwargs}) or SimpleNamespace(job_id=JOB_ID),
    )
    monkeypatch.setattr(shared_resource_tasks, "SharedTransferSecretStore", lambda: secrets)
    source = {
        "source_kind": "movie",
        "source_id": "fixture-source",
        "title": "Title A",
        "share_channel": {"share_code": "raw-share", "receive_code": "raw-password"},
        "url": "magnet:?xt=urn:btih:PRIVATE",
        "backup": "ed2k://PRIVATE",
        "private_url": "https://tracker.invalid/download?passkey=PRIVATE",
    }

    shared_resource_tasks.submit_shared_center_transfer_job("center_import", source)
    shared_resource_tasks.submit_shared_center_transfer_job("center_import", source)
    shared_resource_tasks.submit_shared_center_transfer_job(
        "center_import",
        {**source, "title": "Title B"},
        intent_id="explicit-intent",
    )

    assert calls[0]["idempotency_key"] != calls[1]["idempotency_key"]
    assert calls[2]["idempotency_key"] == "explicit-intent"
    assert calls[0]["kwargs"]["effect_key"] == calls[1]["kwargs"]["effect_key"] == calls[2]["kwargs"]["effect_key"]
    assert calls[0]["idempotency_scope"] == "shared-transfer-intent:v2:center_import"
    serialized_jobs = repr(calls)
    for raw_value in ("raw-share", "raw-password", "magnet:?", "ed2k://", "passkey=PRIVATE"):
        assert raw_value not in serialized_jobs
    serialized_secrets = repr(secrets.records)
    assert "raw-share" in serialized_secrets
    assert "raw-password" in serialized_secrets
    assert "magnet:?" in serialized_secrets


def test_shared_transfer_authoritative_manifest_changes_effect_but_title_does_not() -> None:
    base = {
        "source_kind": "movie",
        "source_id": "fixture-source",
        "manifest_hash": "manifest-a",
        "title": "Title A",
    }
    title_changed = {**base, "title": "Title B", "status": "available"}
    manifest_changed = {**base, "manifest_hash": "manifest-b"}

    base_key = shared_resource_tasks._persistent_transfer_effect_identity(
        "center_import", base, "account-fixture", "target-fixture"
    )
    assert base_key == shared_resource_tasks._persistent_transfer_effect_identity(
        "center_import", title_changed, "account-fixture", "target-fixture"
    )
    assert base_key != shared_resource_tasks._persistent_transfer_effect_identity(
        "center_import", manifest_changed, "account-fixture", "target-fixture"
    )


def test_shared_transfer_secret_is_destroyed_after_success(monkeypatch) -> None:
    effect_store = FakeEffectStore()
    secret_store = FakeSecretStore()
    _install_worker_effect_fakes(monkeypatch, effect_store)
    monkeypatch.setattr(shared_resource_tasks, "SharedTransferSecretStore", lambda: secret_store)
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "consume_center_source_payload",
        lambda source, **_kwargs: {"ok": True, "used_share_code": source["share_code"]},
    )
    raw_source = {
        "source_kind": "movie",
        "source_id": "fixture-source",
        "share_code": "raw-share",
    }
    semantic_source = shared_resource_tasks._persistent_transfer_semantic_source(raw_source)
    kwargs = _persistent_kwargs("cloud_download", semantic_source)
    effect_key = kwargs["effect_key"]
    reference = "a" * 64
    secret_store.create({"source": raw_source}, owner_key=effect_key, reference=reference)

    result = shared_resource_tasks.run_shared_center_transfer_persistent(
        "cloud_download",
        semantic_source,
        source_secret_ref=reference,
        **kwargs,
    )

    assert result["ok"] is True
    assert secret_store.records == {}
    assert secret_store.destroyed == [(reference, effect_key)]


def test_shared_transfer_secret_is_destroyed_after_explicit_rejection(monkeypatch) -> None:
    effect_store = FakeEffectStore()
    secret_store = FakeSecretStore()
    _install_worker_effect_fakes(monkeypatch, effect_store)
    monkeypatch.setattr(shared_resource_tasks, "SharedTransferSecretStore", lambda: secret_store)
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "consume_center_source_payload",
        lambda *_args, **_kwargs: {"ok": False, "rejected": True, "message": "denied"},
    )
    raw_source = {
        "source_kind": "movie",
        "source_id": "fixture-source",
        "share_code": "raw-share",
    }
    semantic_source = shared_resource_tasks._persistent_transfer_semantic_source(raw_source)
    kwargs = _persistent_kwargs("cloud_download", semantic_source)
    effect_key = kwargs["effect_key"]
    reference = "b" * 64
    secret_store.create({"source": raw_source}, owner_key=effect_key, reference=reference)

    with pytest.raises(PersistentEffectIndeterminate, match="denied"):
        shared_resource_tasks.run_shared_center_transfer_persistent(
            "cloud_download",
            semantic_source,
            source_secret_ref=reference,
            **kwargs,
        )

    assert secret_store.records == {}
    assert secret_store.destroyed == [(reference, effect_key)]


def test_indeterminate_shared_transfer_is_never_replayed(monkeypatch) -> None:
    store = FakeEffectStore()
    _install_worker_effect_fakes(monkeypatch, store)
    calls = []
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "consume_center_source_payload",
        lambda *_args, **_kwargs: calls.append("transfer") or {"ok": False, "message": "provider response lost"},
    )
    source = {"source_kind": "movie", "source_id": "fixture-source"}
    kwargs = _persistent_kwargs("cloud_download", source)

    with pytest.raises(PersistentEffectIndeterminate):
        shared_resource_tasks.run_shared_center_transfer_persistent(
            "cloud_download",
            source,
            **kwargs,
        )
    with pytest.raises(PersistentEffectIndeterminate, match="禁止自动重放"):
        shared_resource_tasks.run_shared_center_transfer_persistent(
            "cloud_download",
            source,
            **{**kwargs, "intent_id": "second-intent"},
        )

    assert calls == ["transfer"]


def test_succeeded_shared_transfer_is_deduplicated_without_replay(monkeypatch) -> None:
    store = FakeEffectStore()
    _install_worker_effect_fakes(monkeypatch, store)
    calls = []
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "consume_center_source_payload",
        lambda *_args, **_kwargs: calls.append("transfer") or {"ok": True, "success_count": 1, "total": 1},
    )
    source = {"source_kind": "movie", "source_id": "fixture-source"}
    kwargs = _persistent_kwargs("cloud_download", source)

    first = shared_resource_tasks.run_shared_center_transfer_persistent("cloud_download", source, **kwargs)
    second = shared_resource_tasks.run_shared_center_transfer_persistent(
        "cloud_download",
        source,
        **{**kwargs, "intent_id": "second-intent"},
    )

    assert first["ok"] is True
    assert second["deduplicated"] is True
    assert calls == ["transfer"]


def test_shared_transfer_organize_child_keeps_frozen_binding(monkeypatch) -> None:
    from etk.compatibility import tg_control_bridge

    service = shared_resource_tasks.shared_subscription_service
    scheduled = []
    submitted = []
    effect_key = "c" * 64
    monkeypatch.setattr(service, "_LAST_ORGANIZE_KICK_AT", 0)
    monkeypatch.setattr(service, "_configured_target_cid", lambda: "target-fixture")
    monkeypatch.setattr(
        service,
        "spawn_later",
        lambda *args, **kwargs: scheduled.append((args, kwargs)) or SimpleNamespace(),
    )
    monkeypatch.setattr(
        tg_control_bridge,
        "submit_persistent_legacy_task",
        lambda task_function, **kwargs: submitted.append((task_function, kwargs)) or True,
    )

    with service.persistent_transfer_target(
        "target-fixture",
        expected_account_id="account-fixture",
        effect_key=effect_key,
    ):
        service._kick_115_organize_detached("rapid:movie:fixture", delay=1)

    args, kwargs = scheduled[0]
    assert kwargs == {}
    assert args[2:] == (
        "rapid:movie:fixture",
        "account-fixture",
        "target-fixture",
        effect_key,
    )
    assert service._run_115_organize(*args[2:]) is True
    task_function, submission = submitted[0]
    assert task_function.__name__ == "task_scan_and_organize_115"
    assert submission["kwargs"] == {
        "expected_account_id": "account-fixture",
        "expected_target_cid": "target-fixture",
    }
    assert submission["idempotency_key"] == effect_key
    assert submission["idempotency_scope"] == "shared-transfer-organize:v1"


def test_persistent_share_timeout_keeps_target_and_disables_rapid_fallback(monkeypatch) -> None:
    class P115:
        def share_import(self, *_args):
            raise TimeoutError("fixture response lost")

    monkeypatch.setattr("task_manager.has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "_completed_share_channel_for_transfer",
        lambda *_args, **_kwargs: {
            "share_code": "redacted-share",
            "receive_code": "redacted-receive",
            "channel_id": "channel-fixture",
        },
    )
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service.P115Service,
        "get_client",
        lambda: P115(),
    )
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "_prepare_share_import_target_dir",
        lambda **_kwargs: {
            "target_cid": "folder-fixture",
            "folder_cid": "folder-fixture",
            "share_import_temp_dir": True,
            "folder_name": "fixture",
        },
    )
    monkeypatch.setattr(
        shared_resource_tasks.shared_subscription_service,
        "_cleanup_rapid_temp_dir",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("indeterminate target must be kept")),
    )

    result = shared_resource_tasks.shared_subscription_service._try_completed_season_share_transfer(
        client=cast(SharedCenterClient, SimpleNamespace()),
        source_id="fixture-source",
        payload={"source_kind": "logical_season", "title": "fixture"},
        files=[{"source_id": "episode-1"}],
        target_cid="target-fixture",
    )

    assert result["ok"] is False
    assert result["indeterminate"] is True
    assert result["share_target"] == {"target_cid": "folder-fixture", "folder_cid": "folder-fixture"}
