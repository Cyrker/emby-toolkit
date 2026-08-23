from __future__ import annotations

from types import SimpleNamespace

import pytest

from handler import p115_service


def test_api_delete_submits_the_complete_payload_without_using_process_memory(monkeypatch) -> None:
    captured = {}
    handle = SimpleNamespace(job_id="fixture-delete-job")
    monkeypatch.setitem(
        p115_service.config_manager.APP_CONFIG,
        p115_service.constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE,
        True,
    )
    monkeypatch.setattr(p115_service, "_has_persistent_execution", lambda: False)
    monkeypatch.setattr(p115_service, "persistent_background_gateway_installed", lambda: True)
    monkeypatch.setattr(
        p115_service.WebhookDeleteBuffer,
        "add",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not use the local delete buffer")),
    )

    def submit(function, **options):
        captured.update({"function": function, **options})
        return handle

    monkeypatch.setattr(p115_service, "submit_persistent_background", submit)

    result = p115_service.delete_115_files_by_webhook(
        ["pc-one", "pc-one"],
        file_snapshots=[{"fid": "fid-one", "parent_id": "parent", "pick_code": "pc-one"}],
    )

    assert result is handle
    assert captured["function"] is p115_service.delete_115_files_by_webhook
    assert captured["args"] == (
        ["pc-one"],
        [{"id": "fid-one", "parent_id": "parent", "pick_code": "pc-one"}],
    )
    assert captured["max_attempts"] == 6
    assert captured["timeout_seconds"] == 900
    assert captured["idempotency_scope"] == "p115-webhook-delete"
    assert len(captured["idempotency_key"]) == 64


def test_worker_delete_runs_synchronously_and_propagates_retry(monkeypatch) -> None:
    monkeypatch.setitem(
        p115_service.config_manager.APP_CONFIG,
        p115_service.constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE,
        True,
    )
    monkeypatch.setattr(p115_service, "_has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        "handler.p115_upload_monitor.delete_local_sources_by_pickcodes",
        lambda _pickcodes: {"matched": 0},
    )
    monkeypatch.setattr(
        p115_service.WebhookDeleteBuffer,
        "_process_batch",
        lambda pickcodes, **_kwargs: list(pickcodes),
    )

    with pytest.raises(RuntimeError, match="requires retry"):
        p115_service.delete_115_files_by_webhook(["pc-retry"])


def test_delete_reconciliation_skips_an_effect_completed_before_the_worker_crash() -> None:
    delete_calls = []

    class Client:
        def fs_files(self, _payload):
            return {"state": True, "data": []}

        def fs_delete(self, _fids):
            delete_calls.append(True)
            return {"state": True}

    assert (
        p115_service._p115_delete_nodes_with_reconciliation(
            Client(),
            {"fid-one": "parent-one"},
            ["fid-one"],
        )
        is True
    )
    assert delete_calls == []


def test_delete_reconciliation_accepts_a_lost_success_response() -> None:
    states = iter([True, False])

    class Client:
        def fs_files(self, _payload):
            exists = next(states)
            return {"state": True, "data": [{"fid": "fid-one"}] if exists else []}

        def fs_delete(self, fids):
            assert fids == ["fid-one"]
            return {"state": False, "message": "fixture response lost"}

    assert (
        p115_service._p115_delete_nodes_with_reconciliation(
            Client(),
            {"fid-one": "parent-one"},
            ["fid-one"],
        )
        is True
    )


def test_delete_reconciliation_retries_when_a_failed_effect_is_still_visible() -> None:
    class Client:
        def fs_files(self, _payload):
            return {"state": True, "data": [{"fid": "fid-one"}]}

        def fs_delete(self, fids):
            assert fids == ["fid-one"]
            return {"state": False, "message": "fixture failure"}

    assert (
        p115_service._p115_delete_nodes_with_reconciliation(
            Client(),
            {"fid-one": "parent-one"},
            ["fid-one"],
        )
        is False
    )
