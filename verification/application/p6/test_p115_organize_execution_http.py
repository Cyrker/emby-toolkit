from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.bootstrap.app_factory import create_app
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115 import (
    LegacyP115OrganizeMutationGateway,
    P115ExecutionConflict,
    P115ExecutionNotFound,
    P115ExecutionPreflightError,
    P115ExecutionReservation,
    P115ExecutionStateError,
    P115ExecutionStatus,
    P115OrganizeExecution,
    P115OrganizeExecutionService,
    P115OrganizePlanService,
    P115OrganizeRecordsPage,
    P115OrganizeRecordsService,
    P115ReconciliationResult,
    P115Result,
    SqlAlchemyP115OrganizeExecutionRepository,
    SqlAlchemyP115OrganizeRecordsRepository,
)
from etk.domains.p115.organize_execution import P115Reconciliation
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p6-p115-organize-execution-records.json"
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
FIXTURE_CASES = {case["case_id"]: case for case in FIXTURE["cases"]}
NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
EXECUTION_ID = UUID("10000000-0000-4000-8000-000000000001")
MISSING_EXECUTION_ID = UUID("10000000-0000-4000-8000-000000000099")
ERROR_EXECUTION_ID = UUID("10000000-0000-4000-8000-000000000098")
JOB_ID = UUID("20000000-0000-4000-8000-000000000001")
RECONCILIATION_ID = UUID("30000000-0000-4000-8000-000000000001")


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class RecordingPlanService(P115OrganizePlanService):
    def __init__(self) -> None:
        self.create_calls: list[object] = []

    def create(self, raw_payload: object) -> P115Result:
        self.create_calls.append(raw_payload)
        return P115Result(
            {
                "success": True,
                "data": {
                    "plan_id": "plan-1",
                    "status": "ready",
                },
            },
            200,
        )

    def get(self, plan_id: str) -> P115Result:
        if plan_id != "plan-1":
            return P115Result({"success": False, "message": "整理计划不存在"}, 404)
        return P115Result(
            {
                "success": True,
                "data": {
                    "plan_id": plan_id,
                    "status": "ready",
                },
            },
            200,
        )


class RecordingExecutionService(P115OrganizeExecutionService):
    def __init__(self) -> None:
        self.prepare_calls: list[dict[str, Any]] = []
        self.get_calls: list[UUID] = []
        self.reconcile_calls: list[dict[str, Any]] = []

    def prepare(
        self,
        plan_id: str,
        approved_intent_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> P115ExecutionReservation:
        self.prepare_calls.append(
            {
                "plan_id": plan_id,
                "approved_intent_id": approved_intent_id,
                "requested_by_user_id": requested_by_user_id,
                "trace_id": trace_id,
            }
        )
        if plan_id == "missing":
            raise P115ExecutionNotFound("整理计划不存在")
        if approved_intent_id == "preflight-failed":
            raise P115ExecutionPreflightError("整理计划前置检查失败")
        if approved_intent_id == "conflict":
            raise P115ExecutionConflict("执行意图冲突")
        if approved_intent_id == "unexpected":
            raise RuntimeError("execution-store-failed")
        return P115ExecutionReservation(
            _execution(approved_intent_id=approved_intent_id),
            approved_intent_id != "replay",
        )

    def get(self, execution_id: UUID) -> P115OrganizeExecution | None:
        self.get_calls.append(execution_id)
        if execution_id == MISSING_EXECUTION_ID:
            return None
        if execution_id == ERROR_EXECUTION_ID:
            raise RuntimeError("execution-read-failed")
        return _execution(execution_id=execution_id)

    def reconcile(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        *,
        actor: str | None = None,
    ) -> P115ReconciliationResult:
        self.reconcile_calls.append(
            {
                "execution_id": execution_id,
                "approved_intent_id": approved_intent_id,
                "actor": actor,
            }
        )
        if execution_id == MISSING_EXECUTION_ID:
            raise P115ExecutionNotFound("整理执行不存在")
        if approved_intent_id == "conflict":
            raise P115ExecutionStateError("整理执行不可对账")
        if approved_intent_id == "unexpected":
            raise RuntimeError("reconciliation-readback-failed")
        execution = _execution(execution_id=execution_id)
        reconciliation = P115Reconciliation(
            reconciliation_id=RECONCILIATION_ID,
            approved_intent_id=approved_intent_id,
            request_hash="c" * 64,
            execution_id=execution_id,
            action_index=0,
            effect_key="effect-1",
            status="completed",
            outcome="applied",
            actor=actor,
            evidence={"readback": {"state": "confirmed"}},
            created_at=NOW,
            finished_at=NOW,
        )
        return P115ReconciliationResult(reconciliation, execution)


class RecordingRecordsRepository:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, Any]] = []
        self.delete_calls: list[int] = []

    def list_records(
        self,
        *,
        page: int,
        per_page: int,
        search: str,
        status: str,
        cid: str,
        fail_reason: str,
    ) -> P115OrganizeRecordsPage:
        self.list_calls.append(
            {
                "page": page,
                "per_page": per_page,
                "search": search,
                "status": status,
                "cid": cid,
                "fail_reason": fail_reason,
            }
        )
        return P115OrganizeRecordsPage(
            items=(
                {
                    "id": 7,
                    "original_name": "Movie.2026.mkv",
                    "renamed_name": "电影.mkv",
                    "status": "success",
                },
            ),
            total=1,
            fail_reasons=({"fail_reason": "识别失败", "count": 2},),
            stats={"total": 8, "success": 5, "unrecognized": 2, "unqualified": 1, "thisWeek": 3},
        )

    def delete_record(self, record_id: int) -> None:
        self.delete_calls.append(record_id)


def _execution(
    *,
    execution_id: UUID = EXECUTION_ID,
    approved_intent_id: str = "execute-1",
) -> P115OrganizeExecution:
    return P115OrganizeExecution(
        execution_id=execution_id,
        plan_id="plan-1",
        approved_intent_id=approved_intent_id,
        request_hash="a" * 64,
        status=P115ExecutionStatus.PREPARED,
        account_id="115001",
        provider_backend="openapi",
        preflight_hash="b" * 64,
        preflight={"checked": True},
        actions=(),
        job_id=JOB_ID,
        result=None,
        error=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _build_app(
    *,
    auth_enabled: bool = False,
) -> tuple[
    FastAPI,
    RecordingExecutionService,
    RecordingRecordsRepository,
    LegacySessionCodec,
    list[str],
]:
    execution_service = RecordingExecutionService()
    plan_service = RecordingPlanService()
    records_repository = RecordingRecordsRepository()
    records_service = P115OrganizeRecordsService(records_repository)
    mutation_provider_calls: list[str] = []

    def mutation_provider(_: AppContainer) -> object:
        mutation_provider_calls.append("materialized")
        return object()

    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: FakePermissions()},
        client_providers={"p115.organize-mutations": mutation_provider},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_organize_plan_service = plan_service
    app.state.p115_organize_execution_service = execution_service
    app.state.p115_organize_records_service = records_service
    codec = LegacySessionCodec.from_secret_key(b"p6-organize-execution-http")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, execution_service, records_repository, codec, mutation_provider_calls


def _set_admin(client: TestClient, codec: LegacySessionCodec) -> None:
    client.cookies.set(
        "session",
        codec.dumps({"emby_user_id": "fixture-admin", "emby_is_admin": False}),
    )


def test_fixture_freezes_the_p64_http_surface_and_no_effect_boundaries() -> None:
    assert FIXTURE["schema_version"] == 1
    assert FIXTURE["captured_live"] is False
    assert {(route["method"], route["path"]) for route in FIXTURE["routes"]} == {
        ("POST", "/api/p115/organize/plans/{plan_id}/execute"),
        ("GET", "/api/p115/organize/executions/{execution_id}"),
        ("POST", "/api/p115/organize/executions/{execution_id}/reconcile"),
        ("GET", "/api/p115/records"),
        ("DELETE", "/api/p115/records/{record_id}"),
    }
    assert FIXTURE["http_contract"]["execute_request_fields"] == ["approved_execution_intent_id"]
    assert FIXTURE["http_contract"]["reconcile_request_fields"] == ["approved_reconciliation_intent_id"]
    assert (
        "Record deletion removes only the local record and filesystem-cache rows; it never calls a 115 delete provider."
        in FIXTURE["invariants"]
    )


def test_admin_denial_precedes_body_validation_and_all_p64_side_effects() -> None:
    app, execution_service, records_repository, _codec, mutation_provider_calls = _build_app(auth_enabled=True)

    with TestClient(app) as client:
        responses = [
            client.post(
                "/api/p115/organize/plans/plan-1/execute",
                content="{",
                headers={"content-type": "application/json"},
            ),
            client.post(
                f"/api/p115/organize/executions/{EXECUTION_ID}/reconcile",
                content="{",
                headers={"content-type": "application/json"},
            ),
            client.get("/api/p115/records?page=not-an-integer"),
            client.delete("/api/p115/records/7"),
        ]

    assert [response.status_code for response in responses] == [403, 403, 403, 403]
    assert all(response.json()["status"] == "error" for response in responses)
    assert execution_service.prepare_calls == []
    assert execution_service.reconcile_calls == []
    assert records_repository.list_calls == []
    assert records_repository.delete_calls == []
    assert mutation_provider_calls == []


def test_execute_transport_is_manual_typed_idempotent_and_threadpool_compatible() -> None:
    app, execution_service, _records_repository, codec, _mutation_provider_calls = _build_app(auth_enabled=True)

    with TestClient(app) as client:
        _set_admin(client, codec)
        invalid = [
            client.post("/api/p115/organize/plans/plan-1/execute", json=value)
            for value in (
                1,
                [],
                {},
                {"approved_execution_intent_id": 1},
                {"approved_execution_intent_id": "execute-1", "unexpected": True},
            )
        ]
        malformed = client.post(
            "/api/p115/organize/plans/plan-1/execute",
            content="{",
            headers={"content-type": "application/json"},
        )
        unsupported = client.post(
            "/api/p115/organize/plans/plan-1/execute",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        accepted = client.post(
            "/api/p115/organize/plans/plan-1/execute",
            json={"approved_execution_intent_id": "execute-1"},
        )
        replay = client.post(
            "/api/p115/organize/plans/plan-1/execute",
            json={"approved_execution_intent_id": "replay"},
        )
        conflict = client.post(
            "/api/p115/organize/plans/plan-1/execute",
            json={"approved_execution_intent_id": "conflict"},
        )
        unexpected = client.post(
            "/api/p115/organize/plans/plan-1/execute",
            json={"approved_execution_intent_id": "unexpected"},
        )

    assert {response.status_code for response in invalid} == {400}
    assert all(response.status_code != 422 for response in invalid)
    assert all(response.json() == {"success": False, "message": "整理执行请求格式错误"} for response in invalid)
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert accepted.status_code == FIXTURE_CASES["execute-accepted"]["status"]
    assert accepted.json()["success"] is True
    assert accepted.headers["x-etk-job-id"] == str(JOB_ID)
    assert replay.status_code == FIXTURE_CASES["execute-replay"]["status"]
    assert replay.json()["data"]["approved_execution_intent_id"] == "replay"
    assert conflict.status_code == 409
    assert conflict.json() == {"success": False, "message": "执行意图冲突"}
    assert unexpected.status_code == 500
    assert unexpected.json() == {"success": False, "message": "execution-store-failed"}
    assert execution_service.prepare_calls[0]["requested_by_user_id"] == "fixture-admin"


def test_http_plan_execute_get_and_reconcile_flow() -> None:
    app, execution_service, _records_repository, _codec, mutation_provider_calls = _build_app()

    with TestClient(app) as client:
        planned = client.post(
            "/api/p115/organize/plans/dry-run",
            json={
                "source_cid": "source-root",
                "request_intent_id": "plan-intent-1",
            },
        )
        plan_id = planned.json()["data"]["plan_id"]
        accepted = client.post(
            f"/api/p115/organize/plans/{plan_id}/execute",
            json={"approved_execution_intent_id": "execute-1"},
        )
        execution_id = accepted.json()["data"]["execution_id"]
        fetched = client.get(f"/api/p115/organize/executions/{execution_id}")
        reconciled = client.post(
            f"/api/p115/organize/executions/{execution_id}/reconcile",
            json={"approved_reconciliation_intent_id": "reconcile-1"},
        )

    assert planned.status_code == 200
    assert accepted.status_code == 202
    assert fetched.status_code == 200
    assert fetched.json()["data"]["execution_id"] == execution_id
    assert reconciled.status_code == 200
    assert reconciled.json()["data"]["execution"]["execution_id"] == execution_id
    assert reconciled.json()["data"]["external_mutations_executed"] == 0
    assert execution_service.prepare_calls[0]["plan_id"] == plan_id
    assert mutation_provider_calls == []


def test_execution_get_and_reconcile_have_legacy_envelopes_without_422() -> None:
    app, execution_service, _records_repository, codec, _mutation_provider_calls = _build_app(auth_enabled=True)

    with TestClient(app) as client:
        _set_admin(client, codec)
        fetched = client.get(f"/api/p115/organize/executions/{EXECUTION_ID}")
        missing = client.get(f"/api/p115/organize/executions/{MISSING_EXECUTION_ID}")
        read_error = client.get(f"/api/p115/organize/executions/{ERROR_EXECUTION_ID}")
        malformed_id = client.get("/api/p115/organize/executions/not-a-uuid")
        invalid = [
            client.post(f"/api/p115/organize/executions/{EXECUTION_ID}/reconcile", json=value)
            for value in (
                1,
                [],
                {},
                {"approved_reconciliation_intent_id": 1},
                {"approved_reconciliation_intent_id": "reconcile-1", "unexpected": True},
            )
        ]
        reconciled = client.post(
            f"/api/p115/organize/executions/{EXECUTION_ID}/reconcile",
            json={"approved_reconciliation_intent_id": "reconcile-1"},
        )
        conflict = client.post(
            f"/api/p115/organize/executions/{EXECUTION_ID}/reconcile",
            json={"approved_reconciliation_intent_id": "conflict"},
        )
        unexpected = client.post(
            f"/api/p115/organize/executions/{EXECUTION_ID}/reconcile",
            json={"approved_reconciliation_intent_id": "unexpected"},
        )

    assert fetched.status_code == 200
    assert fetched.json()["data"]["execution_id"] == str(EXECUTION_ID)
    assert missing.status_code == FIXTURE_CASES["execution-missing"]["status"]
    assert missing.json() == FIXTURE_CASES["execution-missing"]["response"]
    assert read_error.status_code == 500
    assert read_error.json() == {"success": False, "message": "execution-read-failed"}
    assert malformed_id.status_code == 404
    assert {response.status_code for response in invalid} == {400}
    assert all(response.status_code != 422 for response in invalid)
    assert reconciled.status_code == FIXTURE_CASES["reconcile-confirmed"]["status"]
    assert reconciled.json()["data"]["external_mutations_executed"] == 0
    assert reconciled.json()["data"]["reconciliation"]["actor"] == "fixture-admin"
    assert conflict.status_code == 409
    assert unexpected.status_code == 500
    assert unexpected.json() == {"success": False, "message": "reconciliation-readback-failed"}
    assert execution_service.reconcile_calls[0]["actor"] == "fixture-admin"


def test_records_filters_pagination_and_idempotent_delete_never_resolve_a_provider() -> None:
    app, _execution_service, records_repository, _codec, mutation_provider_calls = _build_app()
    query = {
        "page": "2",
        "per_page": "25",
        "search": "Movie",
        "status": "success",
        "cid": "target-movies",
        "fail_reason": "识别失败",
    }

    with TestClient(app) as client:
        listed = client.get("/api/p115/records", params=query)
        deleted_existing = client.delete("/api/p115/records/7")
        deleted_missing = client.delete("/api/p115/records/999")
        deleted_invalid = client.delete("/api/p115/records/not-an-integer")

    assert listed.status_code == FIXTURE_CASES["records-page"]["status"]
    assert listed.json() == {
        "success": True,
        "items": [
            {
                "id": 7,
                "original_name": "Movie.2026.mkv",
                "renamed_name": "电影.mkv",
                "status": "success",
            }
        ],
        "total": 1,
        "fail_reasons": [{"fail_reason": "识别失败", "count": 2}],
        "stats": {"total": 8, "success": 5, "unrecognized": 2, "unqualified": 1, "thisWeek": 3},
    }
    assert records_repository.list_calls == [
        {
            "page": 2,
            "per_page": 25,
            "search": "Movie",
            "status": "success",
            "cid": "target-movies",
            "fail_reason": "识别失败",
        }
    ]
    expected_delete = FIXTURE_CASES["record-delete-idempotent"]
    assert deleted_existing.status_code == expected_delete["status"]
    assert deleted_existing.json() == expected_delete["response"]
    assert deleted_missing.status_code == expected_delete["status"]
    assert deleted_missing.json() == expected_delete["response"]
    assert deleted_invalid.status_code == 404
    assert deleted_invalid.json() == {"success": False, "message": "整理记录不存在"}
    assert records_repository.delete_calls == [7, 999]
    assert mutation_provider_calls == []


def test_default_bootstrap_registers_the_p64_repositories_client_and_services(tmp_path: Path) -> None:
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=b"p6-bootstrap-contract",
    )
    container = app.state.container
    try:
        assert isinstance(
            container.repository("p115.organize-executions"),
            SqlAlchemyP115OrganizeExecutionRepository,
        )
        assert isinstance(
            container.repository("p115.organize-records"),
            SqlAlchemyP115OrganizeRecordsRepository,
        )
        assert isinstance(
            container.client("p115.organize-mutations"),
            LegacyP115OrganizeMutationGateway,
        )
        assert isinstance(
            container.use_case("p115.organize-executions"),
            P115OrganizeExecutionService,
        )
        assert isinstance(
            container.use_case("p115.organize-records"),
            P115OrganizeRecordsService,
        )
    finally:
        container.close()


def test_openapi_freezes_p64_request_fields_statuses_content_types_and_no_422() -> None:
    app, *_ = _build_app()
    schema = app.openapi()
    paths: Mapping[str, Any] = schema["paths"]

    execute = paths["/api/p115/organize/plans/{plan_id}/execute"]["post"]
    reconcile = paths["/api/p115/organize/executions/{execution_id}/reconcile"]["post"]
    records = paths["/api/p115/records"]["get"]
    record_delete = paths["/api/p115/records/{record_id}"]["delete"]
    assert set(execute["requestBody"]["content"]["application/json"]["schema"]["properties"]) == {
        "approved_execution_intent_id"
    }
    assert set(reconcile["requestBody"]["content"]["application/json"]["schema"]["properties"]) == {
        "approved_reconciliation_intent_id"
    }
    assert "422" not in execute["responses"]
    assert "422" not in reconcile["responses"]
    assert "422" not in record_delete["responses"]
    assert {"200", "202", "400", "403", "404", "409", "415", "500"} == set(execute["responses"])
    assert {"200", "400", "403", "404", "409", "415", "500"} == set(reconcile["responses"])
    assert set(execute["responses"]["202"]["content"]) == {"application/json"}
    assert set(execute["responses"]["415"]["content"]) == {"text/html"}
    assert {parameter["name"] for parameter in records["parameters"]} == {
        "page",
        "per_page",
        "search",
        "status",
        "cid",
        "fail_reason",
    }
    assert record_delete["parameters"] == [
        {
            "in": "path",
            "name": "record_id",
            "required": True,
            "schema": {"type": "integer"},
        }
    ]
