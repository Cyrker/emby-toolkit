from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.database import (
    DatabaseReadRepository,
    DatabaseReadService,
    SubscriptionQuotaReader,
)
from etk.domains.operations.database_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-review-stats-fixture-key"
NOW = datetime(2026, 8, 27, 8, 30, tzinfo=UTC)


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeDatabaseRepository:
    def __init__(self) -> None:
        self.failed_items = {"item-a", "item-b"}
        self.raise_on_clear = False
        self.subscription_reads = 0

    def subscription_stats(self) -> dict[str, int]:
        self.subscription_reads += 1
        return {
            "watchlist_active": 2,
            "watchlist_paused": 1,
            "watchlist_completed": 3,
            "actor_subscriptions_active": 4,
            "actor_works_in_library": 5,
            "resubscribe_pending": 6,
            "native_collections_total": 7,
            "native_collections_with_missing": 2,
            "native_collections_missing_items": 8,
            "custom_collections_total": 9,
            "custom_collections_with_missing": 3,
            "custom_collections_missing_items": 10,
        }

    def mark_review_item_processed(self, item_id: str) -> bool:
        if item_id not in self.failed_items:
            return False
        self.failed_items.remove(item_id)
        return True

    def clear_review_items(self) -> int:
        if self.raise_on_clear:
            raise RuntimeError("fixture storage failure")
        count = len(self.failed_items)
        self.failed_items.clear()
        return count


@dataclass(frozen=True)
class FakeQuotaSnapshot:
    available_units: int


class FakeQuotaReader:
    def __init__(self) -> None:
        self.calls: list[tuple[date, int]] = []

    def quota_snapshot(self, quota_date: date, *, daily_cap: int) -> FakeQuotaSnapshot:
        self.calls.append((quota_date, daily_cap))
        return FakeQuotaSnapshot(17)


def _build_app() -> tuple[
    FastAPI,
    FakePermissions,
    FakeDatabaseRepository,
    FakeQuotaReader,
    dict[str, bool],
]:
    permissions = FakePermissions()
    repository = FakeDatabaseRepository()
    quota = FakeQuotaReader()
    runtime = {"is_running": False}
    settings = Settings.model_validate(
        {
            "auth_enabled": True,
            "mp_config": {"resubscribe_daily_cap": 25},
        }
    )
    service = DatabaseReadService(
        cast(DatabaseReadRepository, repository),
        quota=cast(SubscriptionQuotaReader, quota),
        settings=lambda: settings,
        runtime_status=lambda: runtime,
        clock=lambda: NOW,
    )
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.database_read_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository, quota, runtime


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_p8_local_review_stats_routes_and_permissions() -> None:
    app, permissions, _repository, _quota, _runtime = _build_app()
    paths = app.openapi()["paths"]
    expected = {
        ("get", "/api/database/stats/subscription"),
        ("post", "/api/actions/mark_item_processed/{item_id}"),
        ("post", "/api/actions/clear_review_items"),
    }
    assert expected <= {(method, path) for path, operations in paths.items() for method in operations}

    with TestClient(app) as client:
        assert client.get("/api/database/stats/subscription").status_code == 401
        assert client.post("/api/actions/clear_review_items").status_code == 403
        _set_user(client, "fixture-user")
        assert client.get("/api/database/stats/subscription").status_code == 200
        assert client.post("/api/actions/clear_review_items").status_code == 403
        permissions.admins.add("fixture-user")
        assert client.post("/api/actions/clear_review_items").status_code == 200


def test_subscription_stats_use_read_only_p7_quota_snapshot() -> None:
    app, _permissions, repository, quota, _runtime = _build_app()
    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        response = client.get("/api/database/stats/subscription")

    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "data": {
            "watchlist": {"watching": 2, "paused": 1, "completed": 3},
            "actors": {"subscriptions": 4, "tracked_in_library": 5},
            "resubscribe": {"pending": 6},
            "native_collections": {"total": 7, "count": 2, "missing_items": 8},
            "custom_collections": {"total": 9, "count": 3, "missing_items": 10},
            "quota": {"mp": {"available": 17, "consumed": 8}},
        },
    }
    assert repository.subscription_reads == 1
    assert quota.calls == [(date(2026, 8, 27), 25)]


def test_mark_and_clear_review_items_preserve_busy_missing_and_storage_errors() -> None:
    app, permissions, repository, _quota, runtime = _build_app()
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        runtime["is_running"] = True
        busy = client.post("/api/actions/mark_item_processed/item-a")
        runtime["is_running"] = False
        missing = client.post("/api/actions/mark_item_processed/missing")
        removed = client.post("/api/actions/mark_item_processed/item-a")
        cleared = client.post("/api/actions/clear_review_items")
        empty = client.post("/api/actions/clear_review_items")
        repository.raise_on_clear = True
        storage_error = client.post("/api/actions/clear_review_items")

    assert busy.status_code == 409
    assert missing.status_code == 404
    assert removed.status_code == 200
    assert removed.json() == {"message": "项目 item-a 已成功从待复核列表移除。"}
    assert cleared.json() == {"message": "操作成功！已从待复核列表移除 1 个项目。"}
    assert empty.json() == {"message": "操作完成，待复核列表本就是空的。"}
    assert storage_error.status_code == 500
    assert storage_error.json() == {"error": "服务器在处理时发生内部错误"}
