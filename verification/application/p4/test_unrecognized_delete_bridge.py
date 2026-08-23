from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from uuid import UUID

import pytest
from flask import Flask

from routes import p115 as p115_routes
from tasks import p115 as p115_tasks

JOB_ID = UUID("00000000-0000-0000-0000-000000000199")


def _disable_delete_locks(monkeypatch) -> None:
    monkeypatch.setattr(p115_tasks.settings_db, "p115_unrecognized_delete_lock", nullcontext)
    monkeypatch.setattr(p115_tasks.settings_db, "p115_auth_write_lock", nullcontext)


def test_api_only_submits_the_unrecognized_delete_job(monkeypatch) -> None:
    monkeypatch.setattr(
        p115_routes,
        "get_config",
        lambda: {p115_routes.constants.CONFIG_OPTION_115_UNRECOGNIZED_CID: "fixture-cid"},
    )
    monkeypatch.setattr(p115_routes, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_routes.P115Service,
        "get_client",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not create a 115 client")),
    )
    captured = {}

    def submit(cid, account_id, **kwargs):
        captured.update({"cid": cid, "account_id": account_id, **kwargs})
        return SimpleNamespace(job_id=JOB_ID)

    monkeypatch.setattr(p115_tasks, "submit_empty_unrecognized_files_task", submit)
    app = Flask(__name__)

    with app.test_request_context(headers={"Idempotency-Key": "fixture-intent"}):
        response = p115_routes.empty_unrecognized_files.__wrapped__()

    assert response.get_json()["job_id"] == str(JOB_ID)
    assert captured == {
        "cid": "fixture-cid",
        "account_id": "115001",
        "idempotency_key": "fixture-intent",
    }


def test_worker_preserves_the_failed_batch_and_cleans_only_confirmed_batches(monkeypatch) -> None:
    _disable_delete_locks(monkeypatch)
    monkeypatch.setattr(p115_tasks, "_UNRECOGNIZED_DELETE_BATCH_SIZE", 2)

    class Client:
        def __init__(self) -> None:
            self.delete_calls = []

        def fs_files(self, _payload):
            return {"state": True, "data": [{"fid": "1"}, {"fid": "2"}, {"fid": "3"}]}

        def fs_delete(self, batch):
            self.delete_calls.append(list(batch))
            return {"state": len(self.delete_calls) == 1, "message": "fixture batch failure"}

    client = Client()
    monkeypatch.setattr(
        p115_tasks.P115Service,
        "get_client",
        lambda **_kwargs: client,
    )
    monkeypatch.setattr(p115_tasks, "_cached_unrecognized_fids", lambda _cid: set())
    cleaned = []
    monkeypatch.setattr(
        p115_tasks,
        "_cleanup_confirmed_unrecognized_fids",
        lambda fids: cleaned.append(list(fids)) or len(fids),
    )

    with pytest.raises(RuntimeError, match="fixture batch failure"):
        p115_tasks.run_empty_unrecognized_files_persistent("fixture-cid", "115001")

    assert client.delete_calls == [["1", "2"], ["3"]]
    assert cleaned == [[], ["1", "2"]]


def test_worker_reconciles_a_crash_after_remote_success_before_local_cleanup(monkeypatch) -> None:
    _disable_delete_locks(monkeypatch)

    class Client:
        def fs_files(self, _payload):
            return {"state": True, "data": [{"fid": "3"}]}

        def fs_delete(self, batch):
            assert batch == ["3"]
            return {"state": True}

    monkeypatch.setattr(
        p115_tasks.P115Service,
        "get_client",
        lambda **_kwargs: Client(),
    )
    monkeypatch.setattr(
        p115_tasks,
        "_cached_unrecognized_fids",
        lambda _cid: {"1", "2", "3"},
    )
    cleaned = []
    monkeypatch.setattr(
        p115_tasks,
        "_cleanup_confirmed_unrecognized_fids",
        lambda fids: cleaned.append(list(fids)) or len(fids),
    )

    result = p115_tasks.run_empty_unrecognized_files_persistent("fixture-cid", "115001")

    assert result == {
        "ok": True,
        "deleted_count": 1,
        "local_deleted": 3,
        "reconciled_count": 2,
        "unrecognized_cid": "fixture-cid",
    }
    assert cleaned == [["1", "2"], ["3"]]


def test_submitter_uses_a_retryable_idempotent_strict_json_operation(monkeypatch) -> None:
    captured = {}

    def submit(function, **options):
        captured.update({"function": function, **options})
        return SimpleNamespace(job_id=JOB_ID)

    monkeypatch.setattr(p115_tasks, "submit_persistent_background", submit)

    p115_tasks.submit_empty_unrecognized_files_task(
        "fixture-cid",
        "115001",
        idempotency_key="fixture-intent",
    )

    assert captured["function"] is p115_tasks.run_empty_unrecognized_files_persistent
    assert captured["args"] == ("fixture-cid", "115001")
    assert captured["max_attempts"] == 5
    assert captured["timeout_seconds"] == 1800
    assert captured["idempotency_scope"] == "p115-unrecognized-delete"
    assert captured["idempotency_key"] == "fixture-intent"
