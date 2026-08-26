from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.subscription.resubscribe import (
    RESUBSCRIBE_USE_CASE_PROVIDER,
    DuplicateResubscribeRuleError,
    ResubscribeBatchReservation,
    ResubscribeItemKey,
    ResubscribeService,
    ResubscribeTaskIntent,
    UnsupportedResubscribeBatchError,
    _rule_values,
)
from etk.domains.subscription.resubscribe_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p7-resubscribe.json").read_text(encoding="utf-8"))
SECRET_KEY = b"p7-resubscribe-fixture-session-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeRepository:
    def __init__(self) -> None:
        self.rules: list[dict[str, Any]] = []
        self.created: list[Mapping[str, Any]] = []
        self.updated: list[tuple[int, Mapping[str, Any]]] = []
        self.deleted: list[int] = []
        self.orders: list[tuple[Any, ...]] = []
        self.status_updates: list[tuple[tuple[ResubscribeItemKey, ...], str]] = []
        self.staged: list[tuple[ResubscribeItemKey, ...]] = []
        self.library_items: list[dict[str, Any]] = []
        self.fail_duplicate = False
        self.unsupported: tuple[str, ...] = ()

    def list_rules(self) -> list[dict[str, Any]]:
        return list(self.rules)

    def create_rule(self, data: Mapping[str, Any]) -> int:
        if self.fail_duplicate:
            raise DuplicateResubscribeRuleError
        self.created.append(dict(data))
        return 11

    def update_rule(self, rule_id: int, data: Mapping[str, Any]) -> bool:
        self.updated.append((rule_id, dict(data)))
        return rule_id == 11

    def delete_rule(self, rule_id: int) -> bool:
        self.deleted.append(rule_id)
        return rule_id == 11

    def order_rules(self, ordered_ids: Sequence[Any]) -> None:
        self.orders.append(tuple(ordered_ids))

    def list_library_status(self) -> list[dict[str, Any]]:
        return list(self.library_items)

    def batch_update_status(self, keys: Sequence[ResubscribeItemKey], status: str) -> int:
        normalized = tuple(keys)
        self.status_updates.append((normalized, status))
        return len(normalized)

    def stage_replacements(self, keys: Sequence[ResubscribeItemKey]) -> ResubscribeBatchReservation:
        normalized = tuple(keys)
        if self.unsupported:
            raise UnsupportedResubscribeBatchError(self.unsupported)
        self.staged.append(normalized)
        return ResubscribeBatchReservation(())

    def set_active_washing(self, key: ResubscribeItemKey, enabled: bool) -> int:
        del key, enabled
        return 1


class FakeTasks:
    def __init__(self) -> None:
        self.busy_value = False
        self.submissions: list[ResubscribeTaskIntent] = []

    def busy(self) -> bool:
        return self.busy_value

    def submit(self, intent: ResubscribeTaskIntent) -> bool:
        self.submissions.append(intent)
        return True


class FakeLibraries:
    def __init__(self) -> None:
        self.ready_value = True
        self.items: Sequence[Mapping[str, Any]] | None = []

    def ready(self) -> bool:
        return self.ready_value

    def list_libraries(self) -> Sequence[Mapping[str, Any]] | None:
        return self.items


def _build_app(
    *, auth_enabled: bool = False
) -> tuple[FastAPI, FakePermissions, FakeRepository, FakeTasks, FakeLibraries]:
    permissions = FakePermissions()
    repository = FakeRepository()
    tasks = FakeTasks()
    libraries = FakeLibraries()
    service = ResubscribeService(repository, tasks, libraries)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={RESUBSCRIBE_USE_CASE_PROVIDER: lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository, tasks, libraries


def _set_user(client: TestClient, user_id: str) -> None:
    client.cookies.set("session", LegacySessionCodec.from_secret_key(SECRET_KEY).dumps({"emby_user_id": user_id}))


def test_fixture_freezes_ten_routes_without_fastapi_422() -> None:
    app, *_ = _build_app()
    paths = app.openapi()["paths"]
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    actual = {(method, path) for path, operations in paths.items() for method in operations}
    assert actual == expected
    for route in FIXTURE["routes"]:
        operation = paths[route["path"]][route["method"].lower()]
        assert "422" not in operation["responses"]
        assert set(operation["responses"]) == {str(value) for value in route["response_statuses"]}
        assert ("requestBody" in operation) is route["request_body"]


def test_admin_contract_and_rule_round_trip_accepts_frontend_readonly_id() -> None:
    app, permissions, repository, *_ = _build_app(auth_enabled=True)
    repository.rules = [{"id": 11, "name": "规则"}]
    with TestClient(app) as client:
        denied = client.get("/api/resubscribe/rules")
        permissions.admins.add("admin")
        _set_user(client, "admin")
        listed = client.get("/api/resubscribe/rules")
        updated = client.put(
            "/api/resubscribe/rules/11",
            json={"id": 11, "name": "规则", "enabled": False},
        )

    assert denied.status_code == 403
    assert denied.json() == {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}
    assert listed.json() == repository.rules
    assert updated.json() == {"message": "洗版规则已成功更新！"}
    assert repository.updated == [(11, {"id": 11, "name": "规则", "enabled": False})]
    assert _rule_values(repository.updated[0][1]) == {"name": "规则", "enabled": False}


def test_rule_create_order_delete_and_transport_errors_remain_legacy_shaped() -> None:
    app, _permissions, repository, *_ = _build_app()
    with TestClient(app) as client:
        invalid = client.post("/api/resubscribe/rules", json={})
        created = client.post("/api/resubscribe/rules", json={"name": "洗版"})
        repository.fail_duplicate = True
        duplicate = client.post("/api/resubscribe/rules", json={"name": "洗版"})
        ordered = client.post("/api/resubscribe/rules/order", json=[11, "12"])
        order_object = client.post("/api/resubscribe/rules/order", json={})
        deleted = client.delete("/api/resubscribe/rules/11")
        malformed = client.post(
            "/api/resubscribe/rules",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/resubscribe/rules",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )

    assert invalid.status_code == 400
    assert created.status_code == 201 and created.json()["id"] == 11
    assert duplicate.status_code == 409
    assert ordered.json() == {"message": "规则顺序已更新！"}
    assert repository.orders == [(11, "12")]
    assert order_object.status_code == 400
    assert deleted.json() == {"message": "洗版规则已成功删除！"}
    assert malformed.status_code == 400 and malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415 and unsupported.headers["content-type"].startswith("text/html")


def test_task_and_library_routes_keep_busy_readiness_and_filter_contracts() -> None:
    app, _permissions, _repository, tasks, libraries = _build_app()
    libraries.items = [
        {"Name": "电影", "Id": "m", "CollectionType": "movies"},
        {"Name": "音乐", "Id": "a", "CollectionType": "music"},
    ]
    with TestClient(app) as client:
        tasks.busy_value = True
        busy = client.post("/api/resubscribe/refresh_status")
        tasks.busy_value = False
        submitted = client.post("/api/resubscribe/resubscribe_all")
        libraries.ready_value = False
        unavailable = client.get("/api/resubscribe/libraries")
        libraries.ready_value = True
        available = client.get("/api/resubscribe/libraries")

    assert busy.status_code == 409
    assert submitted.status_code == 202
    assert tasks.submissions == [ResubscribeTaskIntent("resubscribe-library", "全库媒体洗版")]
    assert unavailable.status_code == 503
    assert available.json() == [{"label": "电影", "value": "m"}]


def test_batch_action_is_atomic_at_the_supported_adapter_boundary() -> None:
    app, _permissions, repository, *_ = _build_app()
    with TestClient(app) as client:
        empty = client.post("/api/resubscribe/batch_action", json={"item_ids": [], "action": "subscribe"})
        invalid = client.post("/api/resubscribe/batch_action", json={"item_ids": ["1-Movie"], "action": "bad"})
        accepted = client.post(
            "/api/resubscribe/batch_action",
            json={"item_ids": ["1-Movie", "2-Season-S3"], "action": "subscribe"},
        )
        repository.unsupported = ("3-Movie",)
        rejected = client.post(
            "/api/resubscribe/batch_action",
            json={"item_ids": ["3-Movie", "4-Movie"], "action": "subscribe"},
        )

    assert empty.json() == {"message": "当前视图下没有可操作的项目。"}
    assert invalid.status_code == 400
    assert accepted.status_code == 202
    assert repository.staged == [(ResubscribeItemKey("1", "Movie"), ResubscribeItemKey("2", "Season", 3))]
    assert rejected.status_code == 409
    assert rejected.json() == {
        "error": "所选项目包含尚未迁移的删除或 HDHive 处理，已整批拒绝。",
        "unsupported_item_ids": ["3-Movie"],
    }
