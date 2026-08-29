from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.identity.users import UsersMediaProjection, UsersRepository, UsersService
from etk.domains.identity.users_repository import (
    IdentityUserLifecycleBusyError,
    UserTemplateNameConflictError,
)
from etk.domains.identity.users_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p8-users-local.json").read_text(encoding="utf-8"))
SECRET_KEY = b"p8-users-local-fixture-key"
NOW = datetime(2026, 8, 27, tzinfo=UTC)


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeUsersRepository:
    def __init__(self) -> None:
        self.templates: dict[int, dict[str, Any]] = {
            1: {
                "id": 1,
                "name": "Fixture Template",
                "description": None,
                "default_expiration_days": 30,
                "source_emby_user_id": "template-source",
                "allow_unrestricted_subscriptions": False,
                "personal_play_account_only": False,
                "max_concurrent_streams": 0,
            }
        }
        self.invitations: dict[int, dict[str, Any]] = {
            1: {
                "id": 1,
                "token": "active-token",
                "template_id": 1,
                "status": "active",
                "expires_at": NOW + timedelta(days=1),
                "created_at": NOW,
            },
            2: {
                "id": 2,
                "token": "expired-token",
                "template_id": 1,
                "status": "active",
                "expires_at": NOW - timedelta(seconds=1),
                "created_at": NOW,
            },
        }
        self.expirations: list[tuple[str, str | None]] = []
        self.expiry_busy = False
        self.chat_ids: list[tuple[str, str | None]] = []

    def list_templates(self) -> list[dict[str, Any]]:
        return deepcopy(list(self.templates.values()))

    def update_template(
        self,
        template_id: int,
        *,
        name: str,
        description: str | None,
        default_expiration_days: int | None,
        allow_unrestricted_subscriptions: bool,
        personal_play_account_only: bool,
        max_concurrent_streams: int,
    ) -> bool:
        if name == "Conflict":
            raise UserTemplateNameConflictError(name)
        value = self.templates.get(template_id)
        if value is None:
            return False
        value.update(
            name=name,
            description=description,
            default_expiration_days=default_expiration_days,
            allow_unrestricted_subscriptions=allow_unrestricted_subscriptions,
            personal_play_account_only=personal_play_account_only,
            max_concurrent_streams=max_concurrent_streams,
        )
        return True

    def delete_template(self, template_id: int) -> bool:
        return self.templates.pop(template_id, None) is not None

    def create_invitations(
        self,
        *,
        template_id: int,
        expiration_days: int | None,
        link_expires_in_days: int,
        count: int,
    ) -> list[str]:
        del expiration_days, link_expires_in_days
        if template_id not in self.templates:
            raise LookupError("模板不存在")
        return [f"created-token-{index}" for index in range(count)]

    def list_invitations(self) -> list[dict[str, Any]]:
        return deepcopy(list(self.invitations.values()))

    def delete_invitation(self, invitation_id: int) -> bool:
        return self.invitations.pop(invitation_id, None) is not None

    def invitation_status(self, token: str) -> Mapping[str, Any] | None:
        return next(
            (deepcopy(value) for value in self.invitations.values() if value["token"] == token),
            None,
        )

    def set_expiration(self, user_id: str, expiration_date: str | None) -> None:
        if self.expiry_busy:
            raise IdentityUserLifecycleBusyError("fixture expiry provider write started")
        self.expirations.append((user_id, expiration_date))

    @staticmethod
    def account_info(user_id: str) -> dict[str, Any] | None:
        return {"id": user_id, "name": "Fixture User", "expiration_date": NOW}

    def set_telegram_chat_id(self, user_id: str, chat_id: str | None) -> None:
        self.chat_ids.append((user_id, chat_id))


class FakeUsersMediaProjection:
    @staticmethod
    def request_history(
        user_id: str,
        *,
        page: int,
        page_size: int,
        status: str,
    ) -> tuple[list[dict[str, Any]], int]:
        return ([{"tmdb_id": "101", "user_id": user_id, "status": status}], page * page_size)

    @staticmethod
    def request_stats(user_id: str) -> dict[str, int]:
        del user_id
        return {"total": 2, "completed": 1, "processing": 0, "pending": 1, "failed": 0}


def _build_app() -> tuple[FastAPI, FakePermissions, FakeUsersRepository]:
    permissions = FakePermissions()
    repository = FakeUsersRepository()
    settings = Settings.model_validate(
        {
            "auth_enabled": True,
            "telegram_channel_id": "fixture-channel",
            "app_base_url": "https://fixture.invalid",
        }
    )
    service = UsersService(
        repository=cast(UsersRepository, repository),
        media=cast(UsersMediaProjection, FakeUsersMediaProjection()),
        settings=lambda: settings,
        clock=lambda: NOW,
    )
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.users_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_fixture_freezes_twelve_local_users_routes_and_openapi_contract() -> None:
    app, *_ = _build_app()
    paths = app.openapi()["paths"]
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    actual = {(method, path) for path, operations in paths.items() for method in operations}

    assert len(expected) == 12
    assert actual == expected
    for route in FIXTURE["routes"]:
        operation = paths[route["path"]][route["method"].lower()]
        assert set(operation["responses"]) - {"422"} == {str(status) for status in route["response_statuses"]}
        assert ("requestBody" in operation) is route["request_body"]


def test_public_login_and_admin_boundaries_are_preserved() -> None:
    app, permissions, _repository = _build_app()
    with TestClient(app) as client:
        public = client.get("/api/register/invite/validate/active-token")
        portal_denied = client.get("/api/portal/account-info")
        admin_denied = client.get("/api/admin/user_templates")
        _set_user(client, "user")
        portal = client.get("/api/portal/account-info")
        still_not_admin = client.get("/api/admin/user_templates")
        permissions.admins.add("user")
        admin = client.get("/api/admin/user_templates")

    assert public.status_code == 200 and public.json() == {"valid": True}
    assert portal_denied.status_code == 401
    assert admin_denied.status_code == 403
    assert portal.status_code == 200
    assert portal.json()["telegram_channel_id"] == "fixture-channel"
    assert still_not_admin.status_code == 403
    assert admin.status_code == 200


def test_invitation_create_and_expiration_paths_keep_legacy_envelopes() -> None:
    app, permissions, repository = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        created = client.post(
            "/api/admin/invitations",
            json={"template_id": 1, "count": 2, "link_expires_in_days": 7},
        )
        expired = client.get("/api/register/invite/validate/expired-token")
        missing = client.get("/api/register/invite/validate/missing-token")
        expiration = client.post(
            "/api/admin/users/user-a/expiration",
            json={"expiration_date": "2026-09-01T00:00:00+00:00"},
        )

    assert created.status_code == 201
    assert created.json() == {
        "status": "ok",
        "invite_links": [
            "https://fixture.invalid/register/invite/created-token-0",
            "https://fixture.invalid/register/invite/created-token-1",
        ],
        "count": 2,
    }
    assert expired.status_code == 410
    assert missing.status_code == 404
    assert expiration.status_code == 200
    assert repository.expirations == [("user-a", "2026-09-01T00:00:00+00:00")]


def test_expiration_update_reports_conflict_after_expiry_provider_write_started() -> None:
    app, permissions, repository = _build_app()
    permissions.admins.add("admin-user")
    repository.expiry_busy = True

    with TestClient(app) as client:
        _set_user(client, "admin-user")
        response = client.post(
            "/api/admin/users/user-a/expiration",
            json={"expiration_date": "2026-09-01T00:00:00+00:00"},
        )

    assert response.status_code == 409
    assert response.json() == {"status": "error", "message": "用户到期状态正在确认，请稍后重试"}
    assert repository.expirations == []


def test_json_transport_and_template_conflict_never_return_runtime_422() -> None:
    app, permissions, _repository = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        malformed = client.put(
            "/api/admin/user_templates/1",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/admin/invitations",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
        conflict = client.put(
            "/api/admin/user_templates/1",
            json={"name": "Conflict"},
        )
        invalid_path = client.delete("/api/admin/user_templates/not-an-int")

    assert malformed.status_code == 400 and malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415 and unsupported.headers["content-type"].startswith("text/html")
    assert conflict.status_code == 409
    assert invalid_path.status_code == 404 and invalid_path.headers["content-type"].startswith("text/html")
    assert all(response.status_code != 422 for response in (malformed, unsupported, conflict, invalid_path))
