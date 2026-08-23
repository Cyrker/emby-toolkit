from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeAdapter, RuntimeRole, RuntimeSupervisor
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations import system_runtime, system_runtime_router
from etk.domains.operations.jobs import JobSubmissionService
from etk.platform import AppContainer, Settings
from etk.platform.jobs import (
    AttemptRecord,
    AttemptStatus,
    CreateJobResult,
    JobProjection,
    JobRecord,
    JobStatus,
    JobStoreProtocol,
)
from etk.platform.tasks import TaskRegistry
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
ROUTE_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "system-runtime-routes.json"
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
SECRET_KEY = b"p4-system-runtime-session-key"
JOB_ID = UUID("00000000-0000-0000-0000-000000000001")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000002")
NOW = datetime(2026, 8, 23, tzinfo=UTC)


class Permissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()
        self.calls: list[str] = []

    def is_admin(self, user_id: str) -> bool:
        self.calls.append(user_id)
        return user_id in self.admins


class UpdateStore:
    def __init__(self) -> None:
        self.active = False
        self.create_error: Exception | None = None
        self.create_calls: list[dict[str, Any]] = []
        self.polls: list[JobProjection | None] = []
        self.get_calls: list[UUID] = []

    def has_active_job(self) -> bool:
        return self.active

    def create_job(self, **values: Any) -> CreateJobResult:
        self.create_calls.append(deepcopy(values))
        if self.create_error is not None:
            raise self.create_error
        return CreateJobResult(
            projection=_projection(JobStatus.QUEUED, AttemptStatus.CREATED),
            reused=False,
        )

    def get_job(self, job_id: UUID) -> JobProjection | None:
        self.get_calls.append(job_id)
        if not self.polls:
            return None
        return self.polls.pop(0)


class RecordingRuntimeAdapter(RuntimeAdapter):
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        self.events = events

    @property
    def config(self) -> Mapping[str, Any]:
        return {}

    def start(self, *, role: RuntimeRole, background: bool) -> None:
        self.events.append(("start", role, background))

    def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
        raise AssertionError(f"unexpected reload: {new_config!r}")

    def stop(self) -> None:
        self.events.append(("stop",))


def _projection(
    status: JobStatus,
    attempt_status: AttemptStatus,
    *,
    message: str = "",
    result: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
) -> JobProjection:
    terminal = status.terminal
    active = status in {JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED}
    job = JobRecord(
        id=JOB_ID,
        task_key="system-auto-update",
        trigger_type="system",
        queue_name="legacy.serial",
        payload_version=1,
        request_payload={"display_name": "系统自动更新"},
        requested_by_user_id="fixture-admin",
        parent_job_id=None,
        workflow_step=None,
        status=status,
        progress=50 if active else 0,
        progress_message=message,
        logs=(),
        max_attempts=1,
        current_attempt=1,
        timeout_seconds=None,
        scheduled_for=NOW,
        next_attempt_at=None,
        cancel_requested_at=None,
        started_at=NOW if active or terminal else None,
        finished_at=NOW if terminal else None,
        result=result,
        error_summary=error,
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
        worker_id="fixture-worker" if active else None,
        claimed_at=NOW if active else None,
        started_at=NOW if active else None,
        last_heartbeat_at=NOW if active else None,
        lease_expires_at=None,
        finished_at=NOW if attempt_status.terminal else None,
        retryable=None,
        error_type=None,
        error_message=None,
        error_details=None,
        result=result,
        created_at=NOW,
        updated_at=NOW,
    )
    return JobProjection(job=job, current_attempt=attempt)


def _build_app() -> tuple[FastAPI, Permissions, UpdateStore, LegacySessionCodec]:
    permissions = Permissions()
    update_store = UpdateStore()
    jobs = JobSubmissionService(
        cast(JobStoreProtocol, update_store),
        TaskRegistry.from_fixture(TASK_FIXTURE),
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={"operations.jobs": lambda _: jobs},
    )
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(system_runtime_router.router)
    return app, permissions, update_store, codec


def _set_user(client: TestClient, codec: LegacySessionCodec, user_id: str) -> None:
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id, "emby_is_admin": False}))


def _cases() -> dict[str, dict[str, Any]]:
    payload = json.loads(ROUTE_FIXTURE.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def _assert_json_case(response: Any, case_id: str) -> None:
    expected = _cases()[case_id]["response"]
    assert response.status_code == expected["status"]
    assert response.headers["content-type"] == expected["content_type"]
    assert response.json() == expected["body"]


def _assert_sse_case(response: Any, case_id: str) -> None:
    expected = _cases()[case_id]["response"]
    assert response.status_code == expected["status"]
    assert response.headers["content-type"] == expected["content_type"]
    for name, value in expected.get("headers", {}).items():
        assert response.headers[name] == value
    events = [
        json.loads(line.removeprefix("data: ")) for line in response.text.splitlines() if line.startswith("data: ")
    ]
    assert events == expected["events"]


def test_fixture_freezes_both_system_runtime_routes() -> None:
    payload = json.loads(ROUTE_FIXTURE.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["provenance"] == {
        "kind": "static-source-contract",
        "source": "src/etk/domains/operations/system_runtime_router.py",
        "captured_live": False,
    }
    assert {(item["method"], item["path"]) for item in payload["routes"]} == {
        ("GET", "/api/system/update/stream"),
        ("POST", "/api/system/restart"),
    }


def test_update_and_restart_recheck_admin_permission() -> None:
    app, permissions, update_store, codec = _build_app()

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-user")
        _assert_json_case(client.get("/api/system/update/stream"), "admin-required")
        _assert_json_case(client.post("/api/system/restart"), "admin-required")

    assert permissions.calls == ["fixture-user", "fixture-user"]
    assert update_store.create_calls == []


def test_update_stream_preserves_busy_and_submission_error_envelopes() -> None:
    app, permissions, update_store, codec = _build_app()
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        update_store.active = True
        _assert_json_case(client.get("/api/system/update/stream"), "update-busy")

        update_store.active = False
        update_store.create_error = RuntimeError("fixture submit failure")
        _assert_json_case(client.get("/api/system/update/stream"), "update-submit-error")


def test_update_stream_submits_then_polls_done_no_update_and_error(monkeypatch) -> None:
    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(system_runtime_router.asyncio, "sleep", no_sleep)
    app, permissions, update_store, codec = _build_app()
    permissions.admins.add("fixture-admin")

    scenarios = (
        (
            "update-done",
            [
                _projection(
                    JobStatus.RUNNING,
                    AttemptStatus.RUNNING,
                    message="正在执行 fixture 更新",
                ),
                _projection(
                    JobStatus.SUCCEEDED,
                    AttemptStatus.SUCCEEDED,
                    result={
                        "status": "updated",
                        "message": "fixture 更新完成",
                        "updated": True,
                        "current_version": "1.0.0",
                        "target_version": "1.1.0",
                    },
                ),
            ],
        ),
        (
            "update-no-update",
            [
                _projection(
                    JobStatus.SUCCEEDED,
                    AttemptStatus.SUCCEEDED,
                    result={
                        "status": "up_to_date",
                        "message": "fixture 已是最新版本",
                        "updated": False,
                        "current_version": "1.1.0",
                        "target_version": "1.1.0",
                    },
                )
            ],
        ),
        (
            "update-error",
            [
                _projection(
                    JobStatus.FAILED,
                    AttemptStatus.FAILED,
                    error={"message": "fixture 更新失败"},
                )
            ],
        ),
        (
            "update-reported-error",
            [
                _projection(
                    JobStatus.SUCCEEDED,
                    AttemptStatus.SUCCEEDED,
                    result={
                        "ok": False,
                        "status": "error",
                        "message": "fixture daemon unavailable",
                        "updated": False,
                    },
                )
            ],
        ),
    )

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        for index, (case_id, polls) in enumerate(scenarios):
            update_store.polls = list(polls)
            headers = {"idempotency-key": "fixture-update-1"} if index == 0 else {}
            _assert_sse_case(client.get("/api/system/update/stream", headers=headers), case_id)

    assert update_store.get_calls == [JOB_ID, JOB_ID, JOB_ID, JOB_ID, JOB_ID]
    assert len(update_store.create_calls) == 4
    first = update_store.create_calls[0]
    assert first["task_key"] == "system-auto-update"
    assert first["payload"] == {
        "payload_version": 1,
        "parameters": {},
        "processor_type": "media",
        "display_name": "系统自动更新",
        "silent": False,
    }
    assert first["trigger_type"] == "system"
    assert first["requested_by_user_id"] == "fixture-admin"
    assert first["idempotency_key"] == "fixture-update-1"
    assert first["idempotency_scope"] == "api.system.update"


def test_restart_submits_durable_worker_operation_and_preserves_http_status() -> None:
    app, permissions, store, codec = _build_app()
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, codec, "fixture-admin")
        _assert_json_case(
            client.post(
                "/api/system/restart",
                headers={"idempotency-key": "fixture-restart-1"},
            ),
            "restart-success",
        )

        store.create_error = RuntimeError("fixture submit failure")
        _assert_json_case(client.post("/api/system/restart"), "restart-submit-error")

    restart_call = store.create_calls[0]
    assert restart_call["queue_name"] == "legacy.background"
    assert restart_call["trigger_type"] == "system"
    assert restart_call["max_attempts"] == 3
    assert restart_call["timeout_seconds"] == 180
    assert restart_call["idempotency_key"] == "fixture-restart-1"
    assert restart_call["idempotency_scope"] == "api.system.restart"
    assert restart_call["payload"]["call_style"] == "direct"
    assert restart_call["payload"]["legacy_operation"] == {
        "module": "etk.domains.operations.system_runtime",
        "qualname": "run_system_restart_persistent",
    }


def test_restart_control_plane_runs_only_inside_worker_operation(monkeypatch) -> None:
    calls = []

    class Gateway:
        def restart(self) -> Mapping[str, Any]:
            calls.append("restart")
            return {
                "ok": True,
                "target_service": "api",
                "container_name": "fixture-etk-api",
                "message": "fixture restart observed",
            }

    monkeypatch.setattr(system_runtime, "ControlPlaneSystemRestartGateway", Gateway)

    assert system_runtime.run_system_restart_persistent() == {
        "ok": True,
        "target_service": "api",
        "container_name": "fixture-etk-api",
        "message": "fixture restart observed",
    }
    assert calls == ["restart"]


def test_api_lifespan_uses_foreground_runtime_and_keeps_database_lazy(tmp_path: Path) -> None:
    runtime_events: list[tuple[Any, ...]] = []
    database_calls: list[str] = []

    def forbidden_database(_: AppContainer) -> Any:
        database_calls.append("database")
        raise AssertionError("API lifecycle must not materialize the database")

    container = AppContainer(Settings(), database_provider=forbidden_database)
    supervisor = RuntimeSupervisor(lambda: RecordingRuntimeAdapter(runtime_events))
    app = create_app(
        start_runtime=True,
        data_dir=tmp_path / "data",
        secret_key=SECRET_KEY,
        supervisor=supervisor,
        container=container,
    )

    with TestClient(app) as client:
        assert client.get("/api/health").json() == {"status": "ok"}
        assert app.state.compatibility_app.flask_app is None
        assert database_calls == []
        assert runtime_events == [("start", RuntimeRole.API, False)]

    assert runtime_events == [("start", RuntimeRole.API, False), ("stop",)]
    assert database_calls == []
    assert container.closed
