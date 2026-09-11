from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.job_router import router
from etk.domains.operations.jobs import JobSubmissionService
from etk.platform import AppContainer, Settings
from etk.platform.jobs import (
    ActiveJobConflictError,
    AttemptRecord,
    AttemptStatus,
    CreateJobResult,
    IdempotencyConflictError,
    JobNotFoundError,
    JobProjection,
    JobRecord,
    JobStatus,
    JobStoreProtocol,
)
from etk.platform.tasks import TaskRegistry
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
ROUTE_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "task-runtime-routes.json"
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
SECRET_KEY = b"p4-job-http-session-key"
JOB_ID = UUID("00000000-0000-0000-0000-000000000001")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000002")
NOW = datetime(2026, 8, 23, tzinfo=UTC)


class Permissions:
    def __init__(self, events: list[str]) -> None:
        self.admins: set[str] = set()
        self.events = events

    def is_admin(self, user_id: str) -> bool:
        self.events.append(f"admin:{user_id}")
        return user_id in self.admins


class Readiness:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.media_available = True
        self.any_available = True
        self.stop_signals = 0

    def media_ready(self) -> bool:
        self.events.append("media-ready")
        return self.media_available

    def any_ready(self) -> bool:
        self.events.append("any-ready")
        return self.any_available

    def signal_stop_all(self) -> None:
        self.events.append("signal-stop-all")
        self.stop_signals += 1


class FakeJobStore:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.active = False
        self.active_answers: list[bool] = []
        self.create_error: Exception | None = None
        self.create_calls: list[dict[str, Any]] = []
        self.jobs: dict[UUID, JobProjection] = {}
        self.current: JobProjection | None = None
        self.cancel_result: JobProjection | None = None
        self.cancelled_job_ids: list[UUID] = []
        self.cancel_current_calls = 0

    def has_active_job(self) -> bool:
        self.events.append("busy")
        if self.active_answers:
            return self.active_answers.pop(0)
        return self.active

    def create_job(self, **values: Any) -> CreateJobResult:
        self.events.append("create-job")
        self.create_calls.append(deepcopy(values))
        if self.create_error is not None:
            raise self.create_error
        projection = _projection(
            status=JobStatus.QUEUED,
            attempt_status=AttemptStatus.CREATED,
            task_key=str(values["task_key"]),
            request_payload=deepcopy(values["payload"]),
            requested_by_user_id=values.get("requested_by_user_id"),
        )
        self.jobs[projection.job.id] = projection
        self.current = projection
        return CreateJobResult(projection=projection, reused=False)

    def get_job(self, job_id: UUID) -> JobProjection | None:
        self.events.append(f"get:{job_id}")
        return self.jobs.get(job_id)

    def get_current_job(self) -> JobProjection | None:
        return self.current

    def request_cancel_current(self) -> JobProjection | None:
        self.events.append("cancel-current")
        self.cancel_current_calls += 1
        return self.current

    def request_cancel(self, job_id: UUID) -> JobProjection:
        self.events.append(f"cancel:{job_id}")
        self.cancelled_job_ids.append(job_id)
        if self.cancel_result is not None:
            return self.cancel_result
        projection = self.jobs.get(job_id)
        if projection is None:
            raise JobNotFoundError(str(job_id))
        return projection


def _projection(
    *,
    status: JobStatus,
    attempt_status: AttemptStatus,
    task_key: str = "populate-metadata",
    request_payload: dict[str, Any] | None = None,
    requested_by_user_id: str | None = "fixture-admin",
) -> JobProjection:
    terminal = status.terminal
    running = status in {JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED}
    job = JobRecord(
        id=JOB_ID,
        task_key=task_key,
        trigger_type="manual",
        queue_name="legacy.serial",
        payload_version=1,
        request_payload=request_payload or {"display_name": "同步媒体数据"},
        requested_by_user_id=requested_by_user_id,
        parent_job_id=None,
        workflow_step=None,
        status=status,
        progress=42 if running else 0,
        progress_message="fixture running" if running else "",
        logs=({"level": "info", "message": "fixture log"},) if running else (),
        max_attempts=1,
        current_attempt=1,
        timeout_seconds=None,
        scheduled_for=NOW,
        next_attempt_at=None,
        cancel_requested_at=NOW if status is JobStatus.CANCEL_REQUESTED else None,
        started_at=NOW if running or terminal else None,
        finished_at=NOW if terminal else None,
        result=None,
        error_summary=None,
        trace_id=None,
        created_at=NOW,
        updated_at=NOW,
    )
    attempt = AttemptRecord(
        id=ATTEMPT_ID,
        job_id=JOB_ID,
        attempt_no=1,
        status=attempt_status,
        celery_task_id=str(ATTEMPT_ID),
        worker_id="fixture-worker" if running else None,
        claimed_at=NOW if running else None,
        started_at=NOW if running else None,
        last_heartbeat_at=NOW if running else None,
        lease_expires_at=None,
        finished_at=NOW if attempt_status.terminal else None,
        retryable=None,
        error_type=None,
        error_message=None,
        error_details=None,
        result=None,
        created_at=NOW,
        updated_at=NOW,
    )
    return JobProjection(job=job, current_attempt=attempt)


def _build_app() -> tuple[FastAPI, Permissions, Readiness, FakeJobStore, LegacySessionCodec]:
    events: list[str] = []
    permissions = Permissions(events)
    readiness = Readiness(events)
    store = FakeJobStore(events)
    service = JobSubmissionService(
        cast(JobStoreProtocol, store),
        TaskRegistry.from_fixture(TASK_FIXTURE),
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
        client_providers={"operations.job-readiness": lambda _: readiness},
        use_case_providers={"operations.jobs": lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, readiness, store, codec


def _set_user(client: TestClient, codec: LegacySessionCodec, user_id: str) -> None:
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id, "emby_is_admin": False}))


def _cases() -> dict[str, dict[str, Any]]:
    payload = json.loads(ROUTE_FIXTURE.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def _assert_case(response: Any, case_id: str) -> None:
    expected = _cases()[case_id]["response"]
    assert response.status_code == expected["status"]
    assert response.headers["content-type"] == expected["content_type"]
    body = expected.get("body")
    if isinstance(body, dict):
        assert response.json() == body
    else:
        assert response.text == body
    for name, value in expected.get("headers", {}).items():
        assert response.headers[name] == value


def test_fixture_freezes_all_four_task_runtime_routes() -> None:
    payload = json.loads(ROUTE_FIXTURE.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["provenance"] == {
        "kind": "static-source-contract",
        "source": "src/etk/domains/operations/job_router.py",
        "captured_live": False,
    }
    assert {(item["method"], item["path"]) for item in payload["routes"]} == {
        ("POST", "/api/tasks/run"),
        ("POST", "/api/trigger_stop_task"),
        ("GET", "/api/jobs/{job_id}"),
        ("POST", "/api/jobs/{job_id}/cancel"),
    }


def test_run_task_guard_order_is_admin_then_busy_then_media_then_body() -> None:
    app, permissions, readiness, store, codec = _build_app()
    events = permissions.events

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-user")
        _assert_case(
            client.post("/api/tasks/run", content=b"{", headers={"content-type": "application/json"}),
            "admin-required",
        )
        assert events == ["admin:fixture-user"]

        permissions.admins.add("fixture-user")
        events.clear()
        store.active = True
        _assert_case(
            client.post("/api/tasks/run", content=b"{", headers={"content-type": "application/json"}),
            "busy",
        )
        assert events == ["admin:fixture-user", "busy"]

        events.clear()
        store.active = False
        readiness.media_available = False
        _assert_case(
            client.post("/api/tasks/run", content=b"{", headers={"content-type": "application/json"}),
            "media-not-ready",
        )
        assert events == ["admin:fixture-user", "busy", "media-ready"]

        events.clear()
        readiness.media_available = True
        _assert_case(client.post("/api/tasks/run", content=b"{}"), "unsupported-media-type")
        assert events == ["admin:fixture-user", "busy", "media-ready"]


def test_run_task_preserves_html_parser_errors_and_json_name_errors() -> None:
    app, permissions, _, _, codec = _build_app()
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        _assert_case(
            client.post("/api/tasks/run", content=b"{", headers={"content-type": "application/json"}),
            "malformed-json",
        )
        _assert_case(client.post("/api/tasks/run", json=[]), "non-object-json")
        _assert_case(client.post("/api/tasks/run", json={}), "missing-task-name")
        _assert_case(client.post("/api/tasks/run", json={"task_name": "fixture-unknown"}), "unknown-task")


def test_run_task_submits_202_with_job_header_and_idempotency_contract() -> None:
    app, permissions, _, store, codec = _build_app()
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        response = client.post(
            "/api/tasks/run",
            json={"task_name": "populate-metadata", "force_full_update": True},
            headers={"idempotency-key": "fixture-request-1"},
        )
        _assert_case(response, "submitted")

        store.create_error = IdempotencyConflictError("fixture conflict")
        _assert_case(
            client.post(
                "/api/tasks/run",
                json={"task_name": "populate-metadata", "force_full_update": False},
                headers={"idempotency-key": "fixture-request-1"},
            ),
            "idempotency-conflict",
        )

    submitted = store.create_calls[0]
    assert submitted["task_key"] == "populate-metadata"
    assert submitted["payload"] == {
        "payload_version": 1,
        "parameters": {"force_full_update": True},
        "processor_type": "media",
        "display_name": "同步媒体数据",
        "silent": False,
    }
    assert submitted["trigger_type"] == "manual"
    assert submitted["requested_by_user_id"] == "fixture-admin"
    assert submitted["idempotency_key"] == "fixture-request-1"
    assert submitted["idempotency_scope"] == "api.tasks.run"


def test_run_task_rejects_organize_without_immutable_plan_execution() -> None:
    app, permissions, _, store, codec = _build_app()
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        response = client.post("/api/tasks/run", json={"task_name": "scan-organize-115"})

    assert response.status_code == 409
    assert response.json() == {"error": "115 网盘整理必须先创建并批准 immutable Plan，再从 Plan 执行入口提交。"}
    assert store.create_calls == []


@pytest.mark.parametrize(
    "task_key",
    (
        "hdhive-auto-checkin",
        "shared-resource-maintenance",
        "share-all-library",
        "shared-share-status-sync",
    ),
)
def test_run_task_rejects_retired_external_resource_tasks(task_key: str) -> None:
    app, permissions, _, store, codec = _build_app()
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        response = client.post("/api/tasks/run", json={"task_name": task_key})

    assert response.status_code == 410
    assert response.json() == {"error": "该任务所属的影巢、TG 频道监听或共享资源中心功能已下线。"}
    assert store.create_calls == []


def test_run_task_submit_race_retains_legacy_500_envelope() -> None:
    app, permissions, _, store, codec = _build_app()
    permissions.admins.add("fixture-admin")
    store.active_answers = [False, False]
    store.create_error = ActiveJobConflictError("fixture active root")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        _assert_case(
            client.post("/api/tasks/run", json={"task_name": "populate-metadata"}),
            "submit-race",
        )
    assert len(store.create_calls) == 1


def test_public_stop_checks_any_processor_then_cancels_and_signals_all() -> None:
    app, _, readiness, store, _ = _build_app()
    store.current = _projection(status=JobStatus.RUNNING, attempt_status=AttemptStatus.RUNNING)

    with TestClient(app) as client:
        readiness.any_available = False
        _assert_case(client.post("/api/trigger_stop_task"), "stop-not-ready")
        assert store.cancel_current_calls == 0
        assert readiness.stop_signals == 0

        readiness.any_available = True
        _assert_case(client.post("/api/trigger_stop_task"), "stop-success")

    assert store.cancel_current_calls == 1
    assert readiness.stop_signals == 1
    assert readiness.events[-3:] == ["any-ready", "cancel-current", "signal-stop-all"]


def test_job_query_and_cancel_are_admin_only_and_preserve_projection_contract() -> None:
    app, permissions, _, store, codec = _build_app()
    running = _projection(status=JobStatus.RUNNING, attempt_status=AttemptStatus.RUNNING)
    cancelled = JobProjection(
        job=replace(running.job, status=JobStatus.CANCEL_REQUESTED, cancel_requested_at=NOW),
        current_attempt=running.current_attempt,
    )
    store.jobs[JOB_ID] = running
    store.cancel_result = cancelled

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-user")
        _assert_case(client.get(f"/api/jobs/{JOB_ID}"), "admin-required")
        _assert_case(client.post(f"/api/jobs/{JOB_ID}/cancel"), "admin-required")

        permissions.admins.add("fixture-admin")
        _set_user(client, codec, "fixture-admin")
        _assert_case(client.get("/api/jobs/not-a-uuid"), "job-not-found")
        _assert_case(
            client.get("/api/jobs/00000000-0000-0000-0000-000000000099"),
            "job-not-found",
        )
        _assert_case(client.get(f"/api/jobs/{JOB_ID}"), "job-running")
        _assert_case(client.post(f"/api/jobs/{JOB_ID}/cancel"), "job-cancel-requested")

        store.cancel_result = None
        store.jobs.clear()
        _assert_case(client.post(f"/api/jobs/{JOB_ID}/cancel"), "job-not-found")

    assert store.cancelled_job_ids == [JOB_ID, JOB_ID]
