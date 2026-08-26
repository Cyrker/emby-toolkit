from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.domains.p115.deep_delete import (
    P115_DEEP_DELETE_OPERATION,
    P115DeepDeleteExecutionService,
    P115DeepDeleteJob,
    P115DeepDeleteService,
)
from etk.domains.p115.media_info import P115MediaInfoNotFound
from etk.domains.p115.router import router
from etk.platform.jobs import PersistentEffectIndeterminate

SHA1 = "A" * 40
JOB_ID = "00000000-0000-0000-0000-000000000167"
FIXTURE = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/p6-p115-ingestion-delete.json").read_text(
        encoding="utf-8"
    )
)


class FakeIdentities:
    def __init__(self) -> None:
        self.values = {"pc-fixture": SHA1, "pc-other": "B" * 40}
        self.calls: list[str] = []

    def sha1_by_pick_code(self, pick_code: object) -> str:
        normalized = str(pick_code or "")
        self.calls.append(normalized)
        if normalized not in self.values:
            raise P115MediaInfoNotFound("missing")
        return self.values[normalized]


class FakeVerifier:
    def __init__(self) -> None:
        self.item: Mapping[str, Any] | None = {
            "Id": "101",
            "Type": "Movie",
            "Path": "/api/p115/play/pc-fixture",
        }
        self.calls: list[tuple[str, str, str]] = []

    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        self.calls.append((sha1, item_id, expected_pick_code))
        return self.item


class FakeEmby:
    def __init__(self) -> None:
        self.root: Mapping[str, Any] | None = {"Id": "100", "Type": "Movie", "Name": "Fixture"}
        self.children: list[Mapping[str, Any]] = []
        self.root_calls: list[str] = []
        self.child_calls: list[str] = []

    def root_item(self, item_id: str) -> Mapping[str, Any] | None:
        self.root_calls.append(item_id)
        return self.root

    def child_items(self, root_item_id: str) -> Sequence[Mapping[str, Any]]:
        self.child_calls.append(root_item_id)
        return self.children


class FakeScope:
    def __init__(self) -> None:
        self.pickcodes: tuple[str, ...] = ("pc-fixture",)
        self.pick_code = "pc-fixture"
        self.snapshots: tuple[Mapping[str, Any], ...] = ({"pick_code": "pc-fixture", "fid": "501", "parent_id": "500"},)
        self.calls: list[tuple[str, Any]] = []

    def pickcodes_for_deleted_item(self, emby_item_id: str, item_type: str) -> Sequence[str]:
        self.calls.append(("pickcodes", (emby_item_id, item_type)))
        return self.pickcodes

    def pick_code_by_sha1(self, sha1: str) -> str | None:
        self.calls.append(("pick-code", sha1))
        return self.pick_code

    def file_snapshots(self, pickcodes: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        self.calls.append(("snapshots", tuple(pickcodes)))
        return self.snapshots


class FakeSnapshots:
    def __init__(self) -> None:
        self.values: dict[str, dict[str, Any]] = {}
        self.store_calls: list[tuple[str, dict[str, Any]]] = []
        self.claim_calls: list[str] = []

    def store(self, token: str, snapshot: Mapping[str, Any]) -> None:
        copied = dict(snapshot)
        self.store_calls.append((token, copied))
        self.values[token] = copied

    def claim(self, token: str) -> Mapping[str, Any] | None:
        self.claim_calls.append(token)
        return self.values.pop(token, None)


class FakeSubmissions:
    def __init__(self) -> None:
        self.submit_calls: list[dict[str, Any]] = []
        self.lookup_calls: list[dict[str, str]] = []
        self.lookup_job: P115DeepDeleteJob | None = None
        self.error: Exception | None = None

    def submit(
        self,
        operation: str,
        payload: Mapping[str, Any],
        **options: Any,
    ) -> P115DeepDeleteJob:
        copied_payload = dict(payload)
        self.submit_calls.append({"operation": operation, "payload": copied_payload, **options})
        if self.error is not None:
            raise self.error
        return P115DeepDeleteJob(JOB_ID, copied_payload)

    def lookup(self, *, idempotency_scope: str, idempotency_key: str) -> P115DeepDeleteJob | None:
        self.lookup_calls.append({"idempotency_scope": idempotency_scope, "idempotency_key": idempotency_key})
        return self.lookup_job


def _service() -> tuple[
    P115DeepDeleteService,
    FakeIdentities,
    FakeVerifier,
    FakeEmby,
    FakeScope,
    FakeSnapshots,
    FakeSubmissions,
]:
    identities = FakeIdentities()
    verifier = FakeVerifier()
    emby = FakeEmby()
    scope = FakeScope()
    snapshots = FakeSnapshots()
    submissions = FakeSubmissions()
    service = P115DeepDeleteService(
        identities=identities,
        verifier=verifier,
        emby=emby,
        scope=scope,
        snapshots=snapshots,
        submissions=submissions,
        clock=lambda: 1_000.0,
        token_factory=lambda: "fixture-token",
    )
    return service, identities, verifier, emby, scope, snapshots, submissions


def _client(service: P115DeepDeleteService) -> TestClient:
    app = FastAPI()
    app.state.p115_deep_delete_service = service
    app.include_router(router)
    return TestClient(app)


def _case(case_id: str) -> Mapping[str, Any]:
    return next(case for case in FIXTURE["cases"] if case["case_id"] == case_id)


def _snapshot(*, created_at: float = 1_000.0, token: str = "fixture-token") -> dict[str, Any]:
    return {
        "created_at": created_at,
        "delete_token": token,
        "item_id": "100",
        "item_name": "Fixture",
        "item_type": "Movie",
        "series_id": "",
        "pickcodes": ["pc-fixture"],
        "p115_file_snapshots": [{"pick_code": "pc-fixture", "fid": "501", "parent_id": "500"}],
    }


def test_fixture_defines_the_four_auth_none_deep_delete_contracts() -> None:
    routes = [entry for entry in FIXTURE["routes"] if "/deep-delete/" in entry["path"]]
    assert {(route["path"], tuple(route["response_statuses"])) for route in routes} == {
        ("/api/p115/mediainfo/<pick_code>/deep-delete/prepare", (200, 400, 404, 409)),
        ("/api/p115/mediainfo/sha1/<sha1>/deep-delete/prepare", (200, 400, 404, 409)),
        ("/api/p115/mediainfo/<pick_code>/deep-delete/commit", (202, 400, 404, 410, 500)),
        ("/api/p115/mediainfo/sha1/<sha1>/deep-delete/commit", (202, 400, 404, 410, 500)),
    }
    assert all(route["auth"] == "none" for route in routes)


def test_native_openapi_matches_all_six_fixture_status_sets_without_422() -> None:
    app = FastAPI()
    app.include_router(router)
    paths = app.openapi()["paths"]

    for route in FIXTURE["routes"]:
        native_path = route["path"].replace("<pick_code>", "{pick_code}").replace("<sha1>", "{sha1}")
        responses = paths[native_path]["post"]["responses"]
        assert {int(status) for status in responses} == set(route["response_statuses"])


@pytest.mark.parametrize("case_id", ["deep-delete-prepare-pickcode", "deep-delete-prepare-sha1"])
def test_prepare_pickcode_and_sha1_routes_match_fixture_symmetrically(case_id: str) -> None:
    case = _case(case_id)
    service, _identities, verifier, emby, scope, snapshots, submissions = _service()
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert len(verifier.calls) == 1
    assert emby.root_calls == ["100"]
    assert any(call[0] == "pickcodes" for call in scope.calls)
    assert snapshots.store_calls[0][1]["created_at"] == 1_000.0
    assert submissions.submit_calls == []


def test_prepare_validation_failure_matches_fixture_without_any_side_effect_port() -> None:
    case = _case("deep-delete-prepare-invalid-id")
    service, _identities, verifier, emby, scope, snapshots, submissions = _service()
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert verifier.calls == []
    assert emby.root_calls == []
    assert scope.calls == []
    assert snapshots.store_calls == []
    assert snapshots.claim_calls == []
    assert submissions.submit_calls == []


def test_pickcode_prepare_identity_miss_matches_fixture_without_downstream_reads() -> None:
    case = _case("deep-delete-prepare-pickcode-identity-missing")
    service, _identities, verifier, emby, scope, snapshots, _submissions = _service()
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert verifier.calls == []
    assert emby.root_calls == []
    assert scope.calls == []
    assert snapshots.store_calls == []


def test_anchor_identity_failure_matches_fixture_before_root_scope_or_snapshot_effects() -> None:
    case = _case("deep-delete-prepare-anchor-mismatch")
    service, _identities, verifier, emby, scope, snapshots, _submissions = _service()
    verifier.item = None
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert len(verifier.calls) == 1
    assert emby.root_calls == []
    assert scope.calls == []
    assert snapshots.store_calls == []


def test_prepare_series_fallback_freezes_anchor_and_child_pickcodes() -> None:
    service, _identities, verifier, emby, scope, snapshots, _submissions = _service()
    verifier.item = {"Path": "/api/p115/play/pc-fixture"}
    emby.root = {"Type": "Series", "Name": "Series Fixture", "SeriesId": "series-1"}
    emby.children = [{"Path": "/api/p115/play/pc-child"}]
    scope.pickcodes = ()
    scope.pick_code = ""
    scope.snapshots = (
        {"pick_code": "pc-fixture", "fid": "501", "parent_id": "500"},
        {"pick_code": "pc-child", "fid": "502", "parent_id": "500"},
    )

    result = service.prepare_by_sha1(
        SHA1,
        {"root_item_id": "100", "anchor_item_id": "101", "item_type": "Series"},
    )

    assert result.payload == {"ok": True, "token": "fixture-token", "pickcode_count": 2}
    stored = snapshots.store_calls[0][1]
    assert stored["pickcodes"] == ["pc-fixture", "pc-child"]
    assert stored["series_id"] == "series-1"
    assert emby.child_calls == ["100"]


def test_prepare_virtual_item_freezes_local_only_snapshot_without_scope_calls() -> None:
    service, _identities, verifier, _emby, scope, snapshots, _submissions = _service()
    verifier.item = {"Path": f"/api/p115/virtual-play/77/{SHA1}"}

    result = service.prepare_by_sha1(
        SHA1,
        {"root_item_id": "100", "anchor_item_id": "101", "item_type": "Movie"},
    )

    assert result.payload == {
        "ok": True,
        "token": "fixture-token",
        "pickcode_count": 0,
        "local_only": True,
        "virtual_id": 77,
    }
    assert snapshots.store_calls[0][1]["local_only"] is True
    assert snapshots.store_calls[0][1]["pickcodes"] == []
    assert scope.calls == []


@pytest.mark.parametrize(
    "case_id",
    ["deep-delete-commit-pickcode-accepted", "deep-delete-commit-sha1-accepted"],
)
def test_commit_pickcode_and_sha1_routes_submit_exact_internal_operation(case_id: str) -> None:
    case = _case(case_id)
    service, _identities, _verifier, _emby, _scope, snapshots, submissions = _service()
    snapshots.values["fixture-token"] = _snapshot()
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert submissions.submit_calls == [
        {
            "operation": P115_DEEP_DELETE_OPERATION,
            "payload": {"schema_version": 1, "snapshot": _snapshot()},
            "max_attempts": 5,
            "timeout_seconds": 900,
            "idempotency_scope": "deep-delete",
            "idempotency_key": "fixture-token",
        }
    ]


def test_commit_missing_token_uses_manual_400_without_claim_or_submission() -> None:
    case = _case("deep-delete-commit-missing-token")
    service, _identities, _verifier, _emby, _scope, snapshots, submissions = _service()
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert snapshots.claim_calls == []
    assert submissions.submit_calls == []
    assert submissions.lookup_calls == []


def test_expired_snapshot_is_consumed_without_restore_or_submission() -> None:
    case = _case("deep-delete-commit-expired")
    service, _identities, _verifier, _emby, _scope, snapshots, submissions = _service()
    snapshots.values["fixture-expired-token"] = _snapshot(created_at=699.0, token="fixture-expired-token")
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert "fixture-expired-token" not in snapshots.values
    assert snapshots.store_calls == []
    assert submissions.submit_calls == []


def test_missing_snapshot_and_job_match_fixture_404() -> None:
    case = _case("deep-delete-commit-snapshot-missing")
    service, _identities, _verifier, _emby, _scope, snapshots, submissions = _service()
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert snapshots.claim_calls == ["fixture-missing-token"]
    assert submissions.lookup_calls == [
        {"idempotency_scope": "deep-delete", "idempotency_key": "fixture-missing-token"}
    ]
    assert submissions.submit_calls == []


def test_submission_failure_restores_the_exact_original_snapshot_without_extending_ttl() -> None:
    case = _case("deep-delete-commit-submission-failure")
    service, _identities, _verifier, _emby, _scope, snapshots, submissions = _service()
    original = _snapshot()
    original.pop("delete_token")
    snapshots.values["fixture-token"] = dict(original)
    submissions.error = RuntimeError("fixture submission unavailable")
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert snapshots.store_calls == [("fixture-token", original)]
    assert snapshots.values["fixture-token"] == original
    assert snapshots.values["fixture-token"]["created_at"] == 1_000.0


def test_lost_response_replay_looks_up_the_idempotent_job_and_matches_fixture() -> None:
    case = _case("deep-delete-commit-replay")
    service, _identities, _verifier, _emby, _scope, _snapshots, submissions = _service()
    submissions.lookup_job = P115DeepDeleteJob(
        JOB_ID,
        {"schema_version": 1, "snapshot": _snapshot(token="fixture-replay-token")},
    )
    with _client(service) as client:
        response = client.request(
            case["request"]["method"],
            case["request"]["path"],
            json=case["request"]["body"],
        )

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert submissions.lookup_calls == [{"idempotency_scope": "deep-delete", "idempotency_key": "fixture-replay-token"}]
    assert submissions.submit_calls == []


def test_commit_token_remains_a_cross_identity_capability() -> None:
    service, _identities, _verifier, _emby, _scope, snapshots, submissions = _service()
    snapshots.values["fixture-token"] = _snapshot()

    result = service.commit_by_pick_code("pc-other", {"token": "fixture-token"})

    assert result.status_code == 202
    assert result.payload["accepted"] is True
    assert submissions.submit_calls[0]["idempotency_key"] == "fixture-token"


def test_invalid_or_missing_json_is_manually_mapped_without_fastapi_422() -> None:
    service, _identities, _verifier, _emby, _scope, _snapshots, _submissions = _service()
    with _client(service) as client:
        prepare = client.post(
            f"/api/p115/mediainfo/sha1/{SHA1}/deep-delete/prepare",
            content=b"not-json",
            headers={"content-type": "text/plain"},
        )
        commit = client.post(
            f"/api/p115/mediainfo/sha1/{SHA1}/deep-delete/commit",
            content=b"not-json",
            headers={"content-type": "text/plain"},
        )

    assert prepare.status_code == 400
    assert prepare.json() == {"ok": False, "error": "invalid item id"}
    assert commit.status_code == 400
    assert commit.json() == {"ok": False, "error": "missing token"}


class RecordingExecutionPorts:
    def __init__(self) -> None:
        self.events: list[str] = []

    def delete_sources(self, pickcodes: Sequence[str]) -> Mapping[str, Any]:
        del pickcodes
        self.events.append("upload")
        return {"ok": True, "deleted": 1}

    def delete_and_reconcile(
        self,
        pickcodes: Sequence[str],
        file_snapshots: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        del pickcodes, file_snapshots
        self.events.append("p115")
        return {"ok": True, "confirmed": True}

    def cleanup(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        if args:
            self.events.append("shared")
            return {"ok": True, "confirmed": True}
        self.events.append("media")
        assert kwargs["operation_id"] == "fixture-token"
        return {"ok": True, "receipt_state": "local_committed"}


def test_native_execution_service_orders_explicit_effect_ports() -> None:
    ports = RecordingExecutionPorts()
    execution = P115DeepDeleteExecutionService(
        p115=ports,
        upload_sources=ports,
        media=ports,
        shared=ports,
    )

    result = execution.execute({"schema_version": 1, "snapshot": _snapshot()})

    assert result["ok"] is True
    assert ports.events == ["upload", "p115", "media", "shared"]


def test_invalid_execution_payload_has_no_external_side_effects() -> None:
    ports = RecordingExecutionPorts()
    execution = P115DeepDeleteExecutionService(
        p115=ports,
        upload_sources=ports,
        media=ports,
        shared=ports,
    )

    with pytest.raises(ValueError, match="schema"):
        execution.execute({"schema_version": 2, "snapshot": _snapshot()})

    assert ports.events == []


def test_local_only_execution_skips_upload_p115_and_shared_ports() -> None:
    ports = RecordingExecutionPorts()
    execution = P115DeepDeleteExecutionService(
        p115=ports,
        upload_sources=ports,
        media=ports,
        shared=ports,
    )
    snapshot = _snapshot()
    snapshot.update({"pickcodes": [], "p115_file_snapshots": [], "local_only": True})

    result = execution.execute({"schema_version": 1, "snapshot": snapshot})

    assert result["ok"] is True
    assert result["p115_delete"] == {"ok": True, "skipped": True, "local_only": True}
    assert result["shared_cleanup"] == {"ok": True, "skipped": True, "local_only": True}
    assert ports.events == ["media"]


def test_indeterminate_embedded_shared_receipt_is_nonretryable_and_stops_replay() -> None:
    class IndeterminatePorts(RecordingExecutionPorts):
        def cleanup(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
            if args:
                raise AssertionError("explicit shared port must not replay an indeterminate embedded effect")
            self.events.append("media")
            return {
                "ok": False,
                "indeterminate": True,
                "error": "shared cleanup outcome is unknown",
            }

    ports = IndeterminatePorts()
    execution = P115DeepDeleteExecutionService(
        p115=ports,
        upload_sources=ports,
        media=ports,
        shared=ports,
    )

    with pytest.raises(PersistentEffectIndeterminate, match="automatic replay is forbidden"):
        execution.execute({"schema_version": 1, "snapshot": _snapshot()})

    assert ports.events == ["upload", "p115", "media"]
