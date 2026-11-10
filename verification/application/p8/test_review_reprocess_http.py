from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.database_router import effects_router
from etk.domains.operations.jobs import SubmittedJob, TaskBusyError
from etk.domains.operations.review_reprocess import (
    REVIEW_REPROCESS_ALL_TASK_KEY,
    REVIEW_REPROCESS_ONE_TASK_KEY,
    NativeReviewReprocessExecutor,
    ReviewJobPort,
    ReviewReprocessRepository,
    ReviewReprocessService,
)
from etk.platform import AppContainer, Settings
from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-review-reprocess-fixture-key"
JOB_ID = UUID("00000000-0000-0000-0000-000000000808")


class FakePermissions:
    def __init__(self) -> None:
        self.active: set[str] = set()
        self.admins: set[str] = set()

    def is_active(self, user_id: str) -> bool:
        return user_id in self.active

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.active and user_id in self.admins


class FakeRepository:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {
            "item-1": {
                "item_id": "item-1",
                "item_name": "Fixture Movie",
                "reason": "缺失媒体信息",
            }
        }

    def item(self, item_id: str) -> dict[str, Any] | None:
        return self.items.get(item_id)


class FakeJobs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.busy = False
        self.fail = False

    def replay_one(self, *_: Any) -> None:
        return None

    def submit_one(self, **values: Any) -> SubmittedJob:
        return self._submit("one", values)

    def submit_all(self, **values: Any) -> SubmittedJob:
        return self._submit("all", values)

    def _submit(self, kind: str, values: dict[str, Any]) -> SubmittedJob:
        if self.busy:
            raise TaskBusyError("fixture busy")
        if self.fail:
            raise RuntimeError("fixture submission failure")
        self.calls.append((kind, dict(values)))
        return cast(
            SubmittedJob,
            SimpleNamespace(
                projection=SimpleNamespace(job=SimpleNamespace(id=JOB_ID)),
                reused=False,
            ),
        )


def _build_app(emby_item=None, identification=None) -> tuple[FastAPI, FakePermissions, FakeRepository, FakeJobs]:
    permissions = FakePermissions()
    repository = FakeRepository()
    jobs = FakeJobs()
    service = ReviewReprocessService(
        cast(ReviewReprocessRepository, repository),
        cast(ReviewJobPort, jobs),
        emby_item=emby_item,
        identification=identification,
    )
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.review_reprocess_service = service
    app.state.processor_ready = True
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app, permissions, repository, jobs


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_identification_query_is_admin_only_read_only_and_selection_is_validated():
    from etk.domains.media.reprocess_metadata import ReprocessIdentificationError

    queries = []
    def preview(item_id, **query):
        queries.append((item_id, query))
        return {"requires_selection": True, "candidates": []}

    def validate(_item_id, _source, selection):
        if selection["tmdb_id"] == "999":
            raise ReprocessIdentificationError("INVALID_TMDB_ID", "无效 TMDb ID")
        return {"item": {"Name": "当前 Emby 名称"}}

    app, permissions, _, jobs = _build_app(identification=SimpleNamespace(preview=preview, validate=validate))
    permissions.active.add("admin")
    permissions.admins.add("admin")
    route = "/api/actions/reprocess_item/123"
    with TestClient(app) as client:
        assert client.get(route + "/identification").status_code == 403
        _set_user(client, "admin")
        assert client.get(route + "/identification?year=bad").status_code == 422
        assert client.get(route + "/identification?query=作品&year=2020&page=2").json()["requires_selection"]
        assert queries[0][1]["page"] == 2 and not jobs.calls
        payload = {"only_missing": True, "source_job_id": str(JOB_ID),
                   "tmdb_selection": {"tmdb_id": "999", "media_type": "movie", "snapshot": "a" * 64}}
        assert client.post(route, json=payload).status_code == 400
        response = client.post(route, json=payload, headers={"Idempotency-Key": "confirm"})
        assert response.status_code == 400 and response.json()["error_code"] == "INVALID_TMDB_ID"
        assert not jobs.calls
        payload["tmdb_selection"]["tmdb_id"] = "2"
        assert client.post(route, json=payload, headers={"Idempotency-Key": "confirm"}).status_code == 202
        assert jobs.calls[0][1]["tmdb_selection"]["tmdb_id"] == "2"


@pytest.mark.parametrize("route", ["/api/actions/reprocess_item/item-1", "/api/actions/reprocess_all_review_items"])
def test_only_missing_is_validated_and_forwarded(route):
    app, permissions, _, jobs = _build_app()
    permissions.active.add("fixture-admin")
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        for invalid in ("true", 1, None, []):
            assert client.post(route, json={"only_missing": invalid}).status_code == 400
        assert not jobs.calls
        assert client.post(route, json={"only_missing": True}).status_code == 202
    assert jobs.calls[0][1]["only_missing"] is True


def test_search_result_can_reprocess_without_creating_review_record() -> None:
    app, permissions, repository, jobs = _build_app(
        lambda item_id: {"Id": item_id, "Name": "Emby Search Result", "Type": "Series"}
    )
    permissions.active.add("fixture-admin")
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        response = client.post("/api/actions/reprocess_item/123", json={"reason": "N/A"})
    assert response.status_code == 202
    assert jobs.calls[0][1]["item_name"] == "Emby Search Result"
    assert jobs.calls[0][1]["reason"] is None
    assert "123" not in repository.items


@pytest.mark.parametrize(
    "source,code", [(None, 404), ({"Id": "123", "Type": "User"}, 400), ({"Id": "456", "Type": "Series"}, 400)]
)
def test_search_reprocess_rejects_missing_or_wrong_emby_item(source, code) -> None:
    app, permissions, _, jobs = _build_app(lambda _: source)
    permissions.active.add("fixture-admin")
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        assert client.post("/api/actions/reprocess_item/123", json={}).status_code == code
    assert not jobs.calls


def test_review_reprocess_routes_require_current_admin_and_ready_processor() -> None:
    app, permissions, _repository, _jobs = _build_app()
    assert {(method, path) for path, operations in app.openapi()["paths"].items() for method in operations} == {
        ("get", "/api/actions/reprocess_item/{item_id}/identification"),
        ("post", "/api/actions/reprocess_item/{item_id}"),
        ("post", "/api/actions/reprocess_all_review_items"),
    }

    with TestClient(app) as client:
        anonymous = client.post("/api/actions/reprocess_item/item-1", json={})
        assert client.get("/api/actions/reprocess_item/item-1/identification").status_code == 403
        permissions.active.add("fixture-user")
        _set_user(client, "fixture-user")
        non_admin = client.post("/api/actions/reprocess_item/item-1", json={})
        permissions.admins.add("fixture-user")
        app.state.processor_ready = False
        not_ready = client.post("/api/actions/reprocess_item/item-1", json={})

    assert anonymous.status_code == 403
    assert non_admin.status_code == 403
    assert not_ready.status_code == 503


def test_single_review_reprocess_uses_local_review_name_and_traceable_durable_job() -> None:
    app, permissions, _repository, jobs = _build_app()
    permissions.active.add("fixture-admin")
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        wrong_media = client.post(
            "/api/actions/reprocess_item/item-1",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        missing = client.post("/api/actions/reprocess_item/missing", json={})
        invalid = client.post("/api/actions/reprocess_item/item-1", json={"module": "tasks.media"})
        submitted = client.post(
            "/api/actions/reprocess_item/item-1",
            json={"reason": " 手动复核 "},
            headers={"Idempotency-Key": "review-one"},
        )

    assert wrong_media.status_code == 415
    assert missing.status_code == 404
    assert invalid.status_code == 400
    assert submitted.status_code == 202
    assert submitted.headers["location"] == f"/api/jobs/{JOB_ID}"
    assert submitted.json() == {
        "message": "重新处理项目 'Fixture Movie' 的任务已提交。",
        "job_id": str(JOB_ID),
        "status_url": f"/api/jobs/{JOB_ID}",
        "reused": False,
    }
    assert jobs.calls == [
        (
            "one",
            {
                "item_id": "item-1",
                "item_name": "Fixture Movie",
                "reason": "手动复核",
                "requested_by_user_id": "fixture-admin",
                "trace_id": None,
                "idempotency_key": "review-one",
            },
        )
    ]


def test_batch_review_reprocess_validation_busy_and_submission_failure() -> None:
    app, permissions, _repository, jobs = _build_app()
    permissions.active.add("fixture-admin")
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        submitted = client.post(
            "/api/actions/reprocess_all_review_items",
            json={"reason": " 缺失媒体信息 "},
        )
        jobs.busy = True
        busy = client.post("/api/actions/reprocess_all_review_items", json={})
        jobs.busy = False
        jobs.fail = True
        failed = client.post("/api/actions/reprocess_all_review_items", json={})

    assert submitted.status_code == 202
    assert submitted.json()["job_id"] == str(JOB_ID)
    assert busy.status_code == 409
    assert failed.status_code == 500
    assert jobs.calls[0] == (
        "all",
        {
            "reason_filter": "缺失媒体信息",
            "requested_by_user_id": "fixture-admin",
            "trace_id": None,
            "idempotency_key": None,
        },
    )


class ExecutorProgress:
    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint",))

    def update(self, value: int, message: str) -> None:
        self.events.append(("update", value, message))


class ExecutorReprocess:
    def __init__(self, outcomes: list[TaskOutcome]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def execute(self, item_id: str, _progress: Any, **options: Any) -> TaskOutcome:
        self.calls.append({"item_id": item_id, **options})
        return self.outcomes.pop(0)


def test_native_review_executor_routes_single_item_and_rejects_unknown_task() -> None:
    reprocess = ExecutorReprocess([TaskOutcome.succeeded(), TaskOutcome.succeeded()])
    executor = NativeReviewReprocessExecutor(cast(Any, object()), cast(Any, reprocess))

    first = executor.execute(
        REVIEW_REPROCESS_ONE_TASK_KEY,
        {"item_id": " item-1 ", "failure_reason": "缺失媒体信息"},
        ExecutorProgress(),
        operation_key="review",
    )
    second = executor.execute(
        REVIEW_REPROCESS_ONE_TASK_KEY,
        {"item_id": "item-2", "failure_reason": "manual"},
        ExecutorProgress(),
        operation_key="review",
    )

    assert first.status is TaskOutcomeStatus.SUCCEEDED
    assert second.status is TaskOutcomeStatus.SUCCEEDED
    assert reprocess.calls == [
        {"item_id": "item-1", "operation_key": "review:item-1", "media_info_only": True},
        {"item_id": "item-2", "operation_key": "review:item-2", "media_info_only": False},
    ]
    with pytest.raises(LookupError, match="unknown P8 review task key"):
        executor.execute("unknown", {}, ExecutorProgress(), operation_key="review")


def test_native_review_executor_skips_an_empty_batch() -> None:
    progress = ExecutorProgress()
    repository = SimpleNamespace(items=lambda **_options: ())
    executor = NativeReviewReprocessExecutor(cast(Any, repository), cast(Any, object()))

    outcome = executor.execute(
        REVIEW_REPROCESS_ALL_TASK_KEY,
        {"reason_filter": "missing"},
        progress,
        operation_key="review",
    )

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details == {"processed": 0, "failed": 0}
    assert progress.events == [("update", 100, "列表为空。")]


@pytest.mark.parametrize(
    ("outcomes", "expected"),
    [
        ([TaskOutcome.failed(), TaskOutcome.failed()], TaskOutcomeStatus.FAILED),
        ([TaskOutcome.succeeded(), TaskOutcome.failed()], TaskOutcomeStatus.PARTIAL),
        ([TaskOutcome.succeeded(), TaskOutcome.partial()], TaskOutcomeStatus.PARTIAL),
        ([TaskOutcome.succeeded(), TaskOutcome.succeeded()], TaskOutcomeStatus.SUCCEEDED),
    ],
)
def test_native_review_executor_aggregates_batch_outcomes(
    outcomes: list[TaskOutcome],
    expected: TaskOutcomeStatus,
) -> None:
    rows = (
        {"item_id": "item-1", "item_name": "One", "reason": "缺失媒体信息"},
        {"item_id": "item-2", "item_name": "Two", "reason": "manual"},
    )
    repository = SimpleNamespace(items=lambda **_options: rows)
    reprocess = ExecutorReprocess(outcomes)
    progress = ExecutorProgress()
    executor = NativeReviewReprocessExecutor(cast(Any, repository), cast(Any, reprocess))

    outcome = executor.execute(
        REVIEW_REPROCESS_ALL_TASK_KEY,
        {"reason_filter": None},
        progress,
        operation_key="review",
    )

    assert outcome.status is expected
    assert outcome.details["processed"] == 2
    assert reprocess.calls[0]["media_info_only"] is True
    assert reprocess.calls[1]["media_info_only"] is False
    assert progress.events[-1] == ("update", 100, "批量重新处理完成。")
