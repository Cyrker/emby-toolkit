from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.domains.operations.jobs import JobSubmissionService
from etk.domains.p115.intro_backfill_router import router

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((ROOT / "verification/fixtures/http/p11-p115-intro-backfill.json").read_text(encoding="utf-8"))
JOB_ID = UUID(FIXTURE["success"]["response"]["body"]["job_id"])


class RecordingJobs(JobSubmissionService):
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.failure = failure
        self.calls: list[dict[str, Any]] = []

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> Any:
        self.calls.append(
            {
                "task_key": task_key,
                "parameters": dict(parameters or {}),
                "options": dict(options),
            }
        )
        if self.failure is not None:
            raise self.failure
        return SimpleNamespace(projection=SimpleNamespace(job=SimpleNamespace(id=JOB_ID)))


class FakeContainer:
    def __init__(self, jobs: RecordingJobs) -> None:
        self._jobs = jobs

    def use_case(self, name: str) -> Any:
        assert name == "operations.jobs"
        return self._jobs


def _client(jobs: RecordingJobs) -> TestClient:
    app = FastAPI()
    app.state.container = FakeContainer(jobs)
    app.include_router(router)
    return TestClient(app)


def test_intro_backfill_route_submits_native_job_without_request_process_scan() -> None:
    jobs = RecordingJobs()
    case = FIXTURE["success"]

    with _client(jobs) as client:
        response = client.request(case["request"]["method"], case["request"]["path"])

    assert response.status_code == case["response"]["status"]
    assert response.json() == case["response"]["body"]
    assert jobs.calls == [
        {
            "task_key": "intro-fingerprint-backfill",
            "parameters": {"force": False},
            "options": {
                "trigger_type": "system",
                "max_attempts": 1,
                "trace_id": None,
            },
        }
    ]


def test_intro_backfill_route_submit_failure_preserves_compatibility_envelope() -> None:
    jobs = RecordingJobs(failure=RuntimeError("fixture submit failed"))
    case = FIXTURE["failure"]

    with _client(jobs) as client:
        response = client.request(case["request"]["method"], case["request"]["path"])

    assert response.status_code == case["response"]["status"]
    assert response.json() == case["response"]["body"]


def test_intro_backfill_openapi_has_no_request_body_or_automatic_422() -> None:
    jobs = RecordingJobs()
    with _client(jobs) as client:
        operation = client.get("/openapi.json").json()["paths"][FIXTURE["route"]["path"]]["post"]

    assert "requestBody" not in operation
    assert set(operation["responses"]) == {"202", "503"}
