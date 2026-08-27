from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.identity.users_router import effects_router
from etk.domains.subscription.portal import (
    PortalSubmissionResult,
    PortalSubscriptionConflictError,
    PortalSubscriptionRepository,
    PortalSubscriptionService,
    PortalTmdbPort,
)
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-portal-subscription-fixture-key"
NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)


class FakePermissions:
    @staticmethod
    def is_admin(user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeRepository:
    def __init__(self) -> None:
        self.unrestricted = False
        self.mp_config: dict[str, Any] = {"moviepilot_url": "https://mp.fixture.invalid"}
        self.status: str | None = None
        self.submissions: list[dict[str, Any]] = []
        self.conflict = False

    def can_subscribe_without_review(self, user_id: str) -> bool:
        assert user_id in {"fixture-user", "fixture-admin"}
        return self.unrestricted

    def moviepilot_config(self) -> dict[str, Any]:
        return dict(self.mp_config)

    def current_status(self, tmdb_id: str, item_type: str) -> str | None:
        del tmdb_id, item_type
        return self.status

    def submit(self, **values: Any) -> Any:
        if self.conflict:
            raise PortalSubscriptionConflictError("fixture conflict")
        self.submissions.append(dict(values))
        return PortalSubmissionResult((), response=dict(values.get("response_payload") or {}))


class FakeTmdb:
    def __init__(self) -> None:
        self.fail = False

    def movie(self, tmdb_id: int) -> dict[str, Any]:
        assert tmdb_id == 101
        if self.fail:
            raise RuntimeError("provider failure with secret query")
        return {
            "id": tmdb_id,
            "title": "Fixture Movie",
            "release_date": "2026-01-01",
            "poster_path": "/movie.jpg",
        }

    def tv(self, tmdb_id: int) -> dict[str, Any]:
        assert tmdb_id == 202
        if self.fail:
            raise RuntimeError("provider failure with secret query")
        return {
            "id": tmdb_id,
            "name": "Fixture Series",
            "first_air_date": "2025-01-01",
            "poster_path": "/series.jpg",
            "seasons": [
                {"id": 2001, "season_number": 0},
                {"id": 2101, "season_number": 1, "air_date": "2025-01-01"},
                {"id": 2102, "season_number": 2, "air_date": "2026-01-01"},
            ],
        }


def _build_app() -> tuple[FastAPI, FakeRepository, FakeTmdb]:
    repository = FakeRepository()
    tmdb = FakeTmdb()
    service = PortalSubscriptionService(
        cast(PortalSubscriptionRepository, repository),
        cast(PortalTmdbPort, tmdb),
        clock=lambda: NOW,
    )
    settings = Settings.model_validate({"auth_enabled": True})
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.portal_subscription_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app, repository, tmdb


def _set_user(client: TestClient, *, admin: bool = False) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set(
        "session",
        codec.dumps(
            {
                "emby_user_id": "fixture-admin" if admin else "fixture-user",
                "emby_username": "Fixture User",
                "emby_is_admin": admin,
            }
        ),
    )


def test_regular_user_request_is_review_only_and_does_not_stage_provider_write() -> None:
    app, repository, _tmdb = _build_app()
    with TestClient(app) as client:
        denied = client.post(
            "/api/portal/subscribe",
            json={"tmdb_id": 101, "item_type": "Movie"},
        )
        _set_user(client)
        response = client.post(
            "/api/portal/subscribe",
            json={"tmdb_id": 101, "item_type": "Movie", "item_name": "Fallback"},
        )

    assert denied.status_code == 401
    assert response.status_code == 200
    assert response.json() == {
        "status": "pending",
        "message": "“想看”请求已提交，请等待管理员审核。",
    }
    submission = repository.submissions[0]
    assert submission["target_status"] == "REQUESTED"
    assert submission["provider_intents"] == ()
    assert submission["consume_quota"] is False


def test_admin_series_request_stages_one_p7_operation_per_positive_season() -> None:
    app, repository, _tmdb = _build_app()
    with TestClient(app) as client:
        _set_user(client, admin=True)
        response = client.post(
            "/api/portal/subscribe",
            json={"tmdb_id": 202, "item_type": "Series"},
            headers={"Idempotency-Key": "series-request"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "status": "approved",
        "message": "订阅请求已提交，系统将自动处理！",
    }
    submission = repository.submissions[0]
    assert submission["target_status"] == "WANTED"
    assert [record.item_type for record in submission["records"]] == ["Series", "Season", "Season"]
    assert [intent.season_number for intent in submission["provider_intents"]] == [1, 2]
    assert submission["consume_quota"] is True
    assert submission["idempotency_key"] == "series-request"
    assert submission["request_item_type"] == "Series"
    assert submission["request_tmdb_id"] == "202"
    assert submission["request_season_number"] is None


def test_season_request_rejects_a_client_id_that_disagrees_with_tmdb() -> None:
    app, repository, _tmdb = _build_app()
    with TestClient(app) as client:
        _set_user(client, admin=True)
        response = client.post(
            "/api/portal/subscribe",
            json={
                "tmdb_id": 202,
                "item_type": "Season",
                "season_number": 1,
                "season_tmdb_id": 9999,
            },
        )

    assert response.status_code == 400
    assert response.json() == {"status": "error", "message": "季信息不匹配"}
    assert repository.submissions == []


def test_moviepilot_missing_keeps_wanted_without_provider_operation() -> None:
    app, repository, _tmdb = _build_app()
    repository.mp_config = {}
    with TestClient(app) as client:
        _set_user(client)
        response = client.post(
            "/api/portal/subscribe",
            json={"tmdb_id": 101, "item_type": "Movie"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "WANTED"
    assert repository.submissions[0]["provider_intents"] == ()


def test_tmdb_failure_is_sanitized() -> None:
    app, _repository, tmdb = _build_app()
    tmdb.fail = True
    with TestClient(app) as client:
        _set_user(client, admin=True)
        response = client.post(
            "/api/portal/subscribe",
            json={"tmdb_id": 101, "item_type": "Movie"},
        )

    assert response.status_code == 500
    assert response.json() == {
        "status": "error",
        "message": "无法获取媒体详情，请稍后再试。",
    }


def test_portal_idempotency_or_indeterminate_conflict_maps_to_409() -> None:
    app, repository, _tmdb = _build_app()
    repository.conflict = True
    with TestClient(app) as client:
        _set_user(client, admin=True)
        response = client.post(
            "/api/portal/subscribe",
            json={"tmdb_id": 101, "item_type": "Movie"},
            headers={"Idempotency-Key": "conflicting-request"},
        )

    assert response.status_code == 409
    assert response.json() == {
        "status": "error",
        "message": "订阅请求与已有进行中或待确认操作冲突",
    }
