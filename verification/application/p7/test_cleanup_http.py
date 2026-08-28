from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.subscription.cleanup import (
    CLEANUP_CONFIG_SETTING,
    CLEANUP_USE_CASE_PROVIDER,
    LEGACY_CLEANUP_DELETE_DELAY_SETTING,
    LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING,
    LEGACY_CLEANUP_LIBRARY_IDS_SETTING,
    LEGACY_CLEANUP_RULES_SETTING,
    SCAN_CLEANUP_TASK_KEY,
    CleanupService,
    CleanupVersionNotFoundError,
)
from etk.domains.subscription.cleanup_repository import CleanupExecutionConflictError, CleanupExecutionSubmission
from etk.domains.subscription.cleanup_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p7-cleanup.json"
FIXTURE: Mapping[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"
SECRET_KEY = b"p7-cleanup-fixture-session-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()
        self.calls: list[str] = []

    def is_admin(self, user_id: str) -> bool:
        self.calls.append(user_id)
        return user_id in self.admins


class FakeCleanupRepository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.items: list[Mapping[str, Any]] = []
        self.pending_ids: tuple[int, ...] = ()
        self.ignore_count = 0
        self.delete_count = 0
        self.processed_count = 0
        self.removed_versions: list[str] = []
        self.ignored: list[tuple[Any, ...]] = []
        self.deleted: list[tuple[Any, ...]] = []
        self.fail_on: str | None = None
        self.conflict_on: str | None = None

    def list_pending_tasks(self) -> Sequence[Mapping[str, Any]]:
        self._fail("list")
        self.events.append("repository:list")
        return deepcopy(self.items)

    def pending_task_ids(self) -> Sequence[int]:
        self._fail("pending")
        self.events.append("repository:pending")
        return self.pending_ids

    def ignore_tasks(self, task_ids: Sequence[Any]) -> int:
        self._fail("ignore")
        if self.conflict_on == "ignore":
            raise CleanupExecutionConflictError(("cleanup:1",))
        normalized = tuple(task_ids)
        self.ignored.append(normalized)
        self.events.append(f"repository:ignore:{normalized}")
        return self.ignore_count

    def delete_tasks(self, task_ids: Sequence[Any]) -> int:
        self._fail("delete")
        if self.conflict_on == "delete":
            raise CleanupExecutionConflictError(("cleanup:1",))
        normalized = tuple(task_ids)
        self.deleted.append(normalized)
        self.events.append(f"repository:delete:{normalized}")
        return self.delete_count

    def mark_processed(self, task_ids: Sequence[Any]) -> int:
        self._fail("processed")
        del task_ids
        return self.processed_count

    def remove_deleted_version(self, emby_id: str) -> int:
        self._fail("remove-version")
        self.removed_versions.append(emby_id)
        self.events.append(f"repository:remove-version:{emby_id}")
        return 1

    def _fail(self, operation: str) -> None:
        if self.fail_on == operation:
            raise RuntimeError(f"fixture {operation} failure")


class FakeSettingsRepository:
    def __init__(self, values: Mapping[str, Any] | None, events: list[str]) -> None:
        self.values = deepcopy(dict(values or {}))
        self.events = events
        self.saved: list[tuple[str, Any]] = []
        self.fail_on: tuple[str, str] | None = None

    def load(self, key: str) -> Any:
        self._fail("load", key)
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        self._fail("save", key)
        copied = deepcopy(value)
        self.values[key] = copied
        self.saved.append((key, copied))
        self.events.append(f"settings:save:{key}")

    def delete(self, key: str) -> bool:
        self._fail("delete", key)
        return self.values.pop(key, None) is not None

    def _fail(self, operation: str, key: str) -> None:
        if self.fail_on == (operation, key):
            raise RuntimeError(f"fixture {operation} failure")


class FakeJobs:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.busy_value = False
        self.raise_on_submit: Exception | None = None
        self.submissions: list[tuple[str, Mapping[str, Any], Mapping[str, Any]]] = []

    def is_busy(self) -> bool:
        self.events.append("jobs:busy")
        return self.busy_value

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> object:
        self.events.append(f"jobs:submit:{task_key}")
        if self.raise_on_submit is not None:
            raise self.raise_on_submit
        self.submissions.append((task_key, dict(parameters or {}), dict(options)))
        return object()


class FakeReadiness:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.ready_value = True

    def media_ready(self) -> bool:
        self.events.append("readiness:media")
        return self.ready_value


class FakeExecution:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[Any, ...]] = []
        self.options: list[Mapping[str, Any]] = []
        self.version_calls: list[str] = []
        self.raise_on_submit: Exception | None = None
        self.version_error: Exception | None = None

    def submit_cleanup(self, task_ids: Sequence[Any], **options: Any) -> object:
        normalized = tuple(task_ids)
        self.events.append(f"execution:submit:{normalized}")
        if self.raise_on_submit is not None:
            raise self.raise_on_submit
        self.calls.append(normalized)
        self.options.append(dict(options))
        return CleanupExecutionSubmission(
            UUID("00000000-0000-0000-0000-000000000701"),
            "fixture-cleanup-execution",
            len(normalized),
            reused=True,
        )

    def submit_version_deletion(self, emby_id: str, **options: Any) -> object:
        self.events.append(f"execution:version:{emby_id}")
        if self.version_error is not None:
            raise self.version_error
        self.version_calls.append(emby_id)
        self.options.append(dict(options))
        return CleanupExecutionSubmission(
            UUID("00000000-0000-0000-0000-000000000702"),
            "fixture-cleanup-version",
            1,
            resumed=True,
        )


def _build_app(
    *,
    auth_enabled: bool = False,
    setting_values: Mapping[str, Any] | None = None,
) -> tuple[
    FastAPI,
    FakePermissions,
    FakeCleanupRepository,
    FakeSettingsRepository,
    FakeJobs,
    FakeReadiness,
    FakeExecution,
    list[str],
]:
    events: list[str] = []
    permissions = FakePermissions()
    repository = FakeCleanupRepository(events)
    settings = FakeSettingsRepository(setting_values, events)
    jobs = FakeJobs(events)
    readiness = FakeReadiness(events)
    execution = FakeExecution(events)
    service = CleanupService(repository, settings, jobs, readiness, execution)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={CLEANUP_USE_CASE_PROVIDER: lambda _: service},
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository, settings, jobs, readiness, execution, events


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_fixture_and_openapi_freeze_nine_cleanup_bindings_without_422() -> None:
    assert FIXTURE["schema_version"] == 1
    assert FIXTURE["provenance"]["local_legacy_blueprint_verified"] is True
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    assert len(expected) == 9

    app, *_ = _build_app()
    paths = app.openapi()["paths"]
    actual = {(method, path) for path, operations in paths.items() for method in operations}
    assert actual == expected
    for method, path in expected:
        operation = paths[path][method]
        fixture_route = next(
            route for route in FIXTURE["routes"] if route["method"].lower() == method and route["path"] == path
        )
        assert "422" not in operation["responses"]
        assert set(operation["responses"]) == {str(code) for code in fixture_route["response_statuses"]}
        assert ("requestBody" in operation) is bool(fixture_route["request_body"])


def test_legacy_decorator_order_is_preserved_for_execute_clear_and_scan() -> None:
    app, permissions, _repository, _settings, jobs, readiness, *_ = _build_app(auth_enabled=True)
    jobs.busy_value = True
    readiness.ready_value = False
    with TestClient(app) as client:
        execute_busy = client.post("/api/cleanup/execute", json={"task_ids": [1]})
        jobs.busy_value = False
        execute_unavailable = client.post("/api/cleanup/execute", json={"task_ids": [1]})
        readiness.ready_value = True
        execute_denied = client.post("/api/cleanup/execute", json={"task_ids": [1]})
        jobs.busy_value = True
        clear_denied = client.post("/api/cleanup/clear_all")
        scan_denied = client.post("/api/cleanup/scan")

    assert execute_busy.status_code == 409
    assert execute_unavailable.status_code == 503
    assert execute_denied.status_code == 403
    assert clear_denied.status_code == 403
    assert scan_denied.status_code == 403
    assert permissions.calls == []


def test_verified_admin_can_list_cleanup_projection_and_failures_keep_json_500() -> None:
    app, permissions, repository, *_ = _build_app(auth_enabled=True)
    repository.items = [
        {
            "id": 1,
            "tmdb_id": "100",
            "item_type": "Movie",
            "item_name": "Fixture Movie",
            "versions_info_json": [{"id": "emby-1"}, {"id": "emby-2"}],
            "best_version_json": "emby-1",
        }
    ]
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        listed = client.get("/api/cleanup/tasks")
        repository.fail_on = "list"
        failed = client.get("/api/cleanup/tasks")

    assert listed.status_code == 200
    assert listed.json() == repository.items
    assert failed.status_code == 500
    assert failed.json() == {"error": "获取清理任务失败: fixture list failure"}
    assert permissions.calls == ["admin", "admin"]


@pytest.mark.parametrize(
    "path",
    [
        "/api/cleanup/execute",
        "/api/cleanup/ignore",
        "/api/cleanup/delete",
        "/api/cleanup/delete_version",
    ],
)
def test_object_body_routes_preserve_legacy_html_transport_errors(path: str) -> None:
    app, *_ = _build_app()
    with TestClient(app) as client:
        malformed = client.post(path, content=b"{", headers={"Content-Type": "application/json"})
        array = client.post(path, json=[])
        unsupported = client.post(path, content=b"{}", headers={"Content-Type": "text/plain"})

    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert "400 Bad Request" in malformed.text
    assert array.status_code == 500
    assert array.headers["content-type"].startswith("text/html")
    assert "500 Internal Server Error" in array.text
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")


def test_execute_validates_then_submits_durable_intent_and_submit_failure_is_html_500() -> None:
    app, _permissions, _repository, _settings, _jobs, _readiness, execution, *_ = _build_app()
    with TestClient(app) as client:
        invalid = client.post("/api/cleanup/execute", json={"task_ids": []})
        accepted = client.post("/api/cleanup/execute", json={"task_ids": [1, "2"]})
        execution.raise_on_submit = RuntimeError("fixture execution unavailable")
        failed = client.post("/api/cleanup/execute", json={"task_ids": [3]})

    assert invalid.status_code == 400
    assert invalid.json() == {"error": "缺少或无效的 task_ids 参数"}
    assert accepted.status_code == 202
    assert accepted.json() == {"message": "清理任务已提交到后台执行。"}
    assert execution.calls == [(1, "2")]
    assert failed.status_code == 500
    assert failed.headers["content-type"].startswith("text/html")


def test_ignore_and_delete_keep_counts_validation_and_json_failure_envelopes() -> None:
    app, _permissions, repository, *_ = _build_app()
    repository.ignore_count = 2
    repository.delete_count = 1
    with TestClient(app) as client:
        ignored = client.post("/api/cleanup/ignore", json={"task_ids": [1, 2]})
        deleted = client.post("/api/cleanup/delete", json={"task_ids": [3]})
        invalid = client.post("/api/cleanup/delete", json={})
        repository.fail_on = "ignore"
        failed = client.post("/api/cleanup/ignore", json={"task_ids": [9]})

    assert ignored.json() == {"message": "成功忽略 2 个任务。"}
    assert deleted.json() == {"message": "成功删除 1 个任务。"}
    assert invalid.status_code == 400
    assert invalid.json() == {"error": "缺少或无效的 task_ids 参数"}
    assert failed.status_code == 500
    assert failed.json() == {"error": "忽略任务时失败: fixture ignore failure"}
    assert repository.ignored == [(1, 2)]
    assert repository.deleted == [(3,)]


def test_clear_all_reads_pending_before_submission_without_mutating_state() -> None:
    app, _permissions, repository, _settings, _jobs, _readiness, execution, events = _build_app()
    with TestClient(app) as client:
        empty = client.post("/api/cleanup/clear_all")
        repository.pending_ids = (4, 5)
        submitted = client.post("/api/cleanup/clear_all")
        execution.raise_on_submit = RuntimeError("fixture clear unavailable")
        failed = client.post("/api/cleanup/clear_all")

    assert empty.status_code == 200
    assert empty.json() == {"message": "没有发现待处理的清理任务。"}
    assert submitted.status_code == 202
    assert submitted.json() == {"message": "一键清理任务已提交到后台。"}
    assert failed.status_code == 500
    assert failed.json() == {"error": "一键清理失败: fixture clear unavailable"}
    assert execution.calls == [(4, 5)]
    first_submit = events.index("execution:submit:(4, 5)")
    assert events[first_submit - 1] == "repository:pending"
    assert repository.ignored == [] and repository.deleted == []


def test_settings_legacy_fallback_and_rule_normalization_match_source() -> None:
    app, _permissions, _repository, _settings, *_ = _build_app(
        setting_values={
            "media_cleanup_rules": [
                {"id": "effect", "enabled": False, "priority": ["DOVI", "HDR", "hdr"]},
                {"id": "resolution", "enabled": True, "priority": ["2160p", "1080p"]},
                {"id": "codec", "enabled": True, "priority": ["h265", "AVC", "AVC", "Custom"]},
                {"id": "fixture_rule", "enabled": True, "priority": ["x"]},
            ],
            "media_cleanup_library_ids": ["library-1"],
            "media_cleanup_keep_one_per_res": True,
            "media_cleanup_delete_delay": 12,
        }
    )
    with TestClient(app) as client:
        response = client.get("/api/cleanup/settings")

    assert response.status_code == 200
    body = response.json()
    assert [rule["id"] for rule in body["rules"][:4]] == [
        "effect",
        "resolution",
        "codec",
        "fixture_rule",
    ]
    assert body["rules"][0]["priority"] == [
        "dovi_other",
        "hdr",
        "dovi_p8",
        "dovi_p7",
        "dovi_p5",
        "hdr10+",
        "sdr",
    ]
    assert body["rules"][1]["priority"] == ["4K", "1080p", "480p"]
    assert body["rules"][2]["priority"] == ["HEVC", "H.264", "Custom"]
    assert body["library_ids"] == ["library-1"]
    assert body["keep_one_per_res"] is True
    assert body["delete_delay"] == 12
    assert body["manual_delete_without_confirm"] is False
    assert len(body["rules"]) == 12


def test_cleanup_canonical_object_wins_as_a_whole_over_legacy_fallback_rows() -> None:
    app, _permissions, _repository, _settings, *_ = _build_app(
        setting_values={
            CLEANUP_CONFIG_SETTING: {
                "rules": [],
                "library_ids": [],
                "keep_one_per_res": False,
                "delete_delay": 0,
            },
            LEGACY_CLEANUP_RULES_SETTING: [{"id": "runtime", "enabled": False}],
            LEGACY_CLEANUP_LIBRARY_IDS_SETTING: ["legacy-library"],
            LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING: True,
            LEGACY_CLEANUP_DELETE_DELAY_SETTING: 12,
        }
    )

    with TestClient(app) as client:
        response = client.get("/api/cleanup/settings")

    assert response.status_code == 200
    body = response.json()
    assert len(body["rules"]) == 11
    assert body["rules"][0]["id"] == "runtime"
    assert body["rules"][0]["enabled"] is True
    assert body["library_ids"] == []
    assert body["keep_one_per_res"] is False
    assert body["delete_delay"] == 0


def test_settings_central_read_save_and_manual_validation_contracts() -> None:
    app, _permissions, _repository, settings, *_ = _build_app(
        setting_values={
            CLEANUP_CONFIG_SETTING: {
                "rules": [],
                "library_ids": ["central"],
                "keep_one_per_res": False,
                "delete_delay": 3,
                "manual_delete_without_confirm": True,
            }
        }
    )
    with TestClient(app) as client:
        central = client.get("/api/cleanup/settings")
        saved = client.post(
            "/api/cleanup/settings",
            json={
                "rules": [{"id": "runtime", "enabled": False}],
                "library_ids": ["new"],
                "keep_one_per_res": 1,
                "delete_delay": True,
                "manual_delete_without_confirm": "yes",
                "unknown": "ignored",
            },
        )
        array = client.post("/api/cleanup/settings", json=[])
        invalid_rules = client.post("/api/cleanup/settings", json={"rules": {}, "library_ids": []})
        invalid_libraries = client.post("/api/cleanup/settings", json={"rules": [], "library_ids": {}})
        invalid_delay = client.post(
            "/api/cleanup/settings",
            json={"rules": [], "library_ids": [], "delete_delay": -1},
        )
        malformed = client.post(
            "/api/cleanup/settings",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/cleanup/settings",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )

    assert central.json()["library_ids"] == ["central"]
    assert central.json()["manual_delete_without_confirm"] is True
    assert saved.json() == {"message": "清理设置已成功保存！"}
    assert settings.values[CLEANUP_CONFIG_SETTING] == {
        "rules": [{"id": "runtime", "enabled": False}],
        "library_ids": ["new"],
        "keep_one_per_res": True,
        "delete_delay": 1,
        "manual_delete_without_confirm": True,
    }
    assert array.status_code == 400
    assert array.json() == {"error": "无效的数据格式，必须是一个对象。"}
    assert invalid_rules.json() == {"error": "无效的规则格式，'rules' 必须是一个列表。"}
    assert invalid_libraries.json() == {"error": "无效的媒体库格式，'library_ids' 必须是一个列表。"}
    assert invalid_delay.json() == {"error": "删除延迟必须是非负整数。"}
    assert malformed.status_code == 400 and malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415 and unsupported.headers["content-type"].startswith("text/html")


def test_cleanup_config_has_canonical_ownership_without_absorbing_legacy_fallbacks() -> None:
    manifest = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in manifest["standalone_app_settings"]}

    canonical = settings[CLEANUP_CONFIG_SETTING]
    assert canonical["contract_id"] == "setting-29dc7244e442"
    assert canonical["status"] == "new"
    assert canonical["new_owner"] == "etk.domains.system.repository.SqlAlchemyAppSettingsRepository"
    assert canonical["fixture"] == "verification/fixtures/http/p7-cleanup.json"
    assert canonical["validation_owner"] == "subscription/settings"

    for key in (
        LEGACY_CLEANUP_RULES_SETTING,
        LEGACY_CLEANUP_LIBRARY_IDS_SETTING,
        LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING,
        LEGACY_CLEANUP_DELETE_DELAY_SETTING,
    ):
        assert settings[key]["status"] == "legacy"
        assert settings[key]["new_owner"] is None


def test_settings_repository_failure_keeps_legacy_json_500() -> None:
    app, _permissions, _repository, settings, *_ = _build_app()
    settings.fail_on = ("save", CLEANUP_CONFIG_SETTING)
    with TestClient(app) as client:
        response = client.post("/api/cleanup/settings", json={"rules": [], "library_ids": []})

    assert response.status_code == 500
    assert response.json() == {"error": "保存清理设置时失败: fixture save failure"}


def test_ignore_and_delete_report_active_execution_claim_conflicts() -> None:
    app, _permissions, repository, *_ = _build_app()
    with TestClient(app) as client:
        repository.conflict_on = "ignore"
        ignored = client.post("/api/cleanup/ignore", json={"task_ids": [1]})
        repository.conflict_on = "delete"
        deleted = client.post("/api/cleanup/delete", json={"task_ids": [1]})

    expected = {
        "error": "清理任务已提交执行，不能同时忽略或删除。",
        "conflict_ids": ["cleanup:1"],
    }
    assert ignored.status_code == 409 and ignored.json() == expected
    assert deleted.status_code == 409 and deleted.json() == expected


def test_scan_reports_busy_unavailable_and_only_submits_p8_catalog_seam() -> None:
    app, _permissions, _repository, _settings, jobs, readiness, *_ = _build_app()
    with TestClient(app) as client:
        jobs.busy_value = True
        busy = client.post("/api/cleanup/scan")
        jobs.busy_value = False
        readiness.ready_value = False
        unavailable = client.post("/api/cleanup/scan")
        readiness.ready_value = True
        submitted = client.post("/api/cleanup/scan")
        jobs.raise_on_submit = RuntimeError("fixture scan unavailable")
        failed = client.post("/api/cleanup/scan")

    assert busy.status_code == 409
    assert busy.json() == {"error": "后台有任务正在运行，请稍后再试。"}
    assert unavailable.status_code == 503
    assert unavailable.json() == {"error": "核心处理器未就绪。"}
    assert submitted.status_code == 202
    assert submitted.json() == {"message": "扫描任务已提交到后台执行。"}
    assert jobs.submissions == [
        (
            SCAN_CLEANUP_TASK_KEY,
            {},
            {
                "trigger_type": "manual",
                "requested_by_user_id": None,
                "trace_id": None,
                "reject_when_busy": True,
            },
        )
    ]
    assert failed.status_code == 500
    assert failed.json() == {"error": "提交扫描任务失败: fixture scan unavailable"}


def test_delete_version_stages_one_durable_job_instead_of_running_physical_effects_in_http() -> None:
    app, _permissions, repository, _settings, _jobs, _readiness, execution, events = _build_app()
    with TestClient(app) as client:
        missing_body = client.post("/api/cleanup/delete_version", json={})
        execution.version_error = CleanupVersionNotFoundError("fixture missing")
        not_found = client.post("/api/cleanup/delete_version", json={"emby_id": "missing"})
        execution.version_error = CleanupExecutionConflictError(("version:busy",))
        conflict = client.post("/api/cleanup/delete_version", json={"emby_id": "busy"})
        execution.version_error = None
        success = client.post("/api/cleanup/delete_version", json={"emby_id": "emby-1"})

    assert missing_body.status_code == 400
    assert missing_body.json() == {"error": "缺少 emby_id"}
    assert not_found.status_code == 404
    assert not_found.json() == {"error": "未找到对应的物理文件路径"}
    assert conflict.status_code == 409
    assert conflict.json() == {
        "error": "清理任务已提交执行，不能同时忽略或删除。",
        "conflict_ids": ["version:busy"],
    }
    assert success.status_code == 202
    assert success.json() == {"message": "删除任务已提交到后台。"}
    assert execution.version_calls == ["emby-1"]
    assert repository.removed_versions == []
    assert events[-1] == "execution:version:emby-1"
