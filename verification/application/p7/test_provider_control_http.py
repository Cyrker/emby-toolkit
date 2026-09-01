from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.sharing.service import SharingHttpResult
from etk.domains.subscription.provider_control import PROVIDER_CONTROL_USE_CASE, ProviderControlService
from etk.domains.subscription.provider_control_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p7-provider-control.json").read_text(encoding="utf-8"))
SECRET_KEY = b"p11-provider-control-fixture-key"


class Permissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class Service(ProviderControlService):
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def cloud_resources(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("cloud_resources", kwargs))
        return SharingHttpResult(
            {
                "success": True,
                "data": [],
                "total": 0,
                "stats": {
                    "shared_pool_total": 0,
                    "hdhive_total": 0,
                    "hdhive_filtered": 0,
                    "channel_total": 0,
                    "shown": 0,
                    "limit": 50,
                    "warnings": [],
                },
            }
        )

    def cloud_download(self, payload: dict[str, Any], **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("cloud_download", (payload, kwargs)))
        return self._accepted("cloud-operation")

    def hdhive_configuration(self) -> SharingHttpResult:
        return SharingHttpResult({"success": True, "authorized": True})

    def save_hdhive_configuration(self, payload: dict[str, Any]) -> SharingHttpResult:
        self.calls.append(("save_hdhive_configuration", payload))
        return SharingHttpResult({"success": True, "message": "影巢配置保存成功！"})

    def hdhive_authorize_url(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("hdhive_authorize_url", kwargs))
        return SharingHttpResult({"success": True, "authorize_url": "https://hdhive.fixture/authorize"})

    def clear_hdhive_authorization(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("clear_hdhive_authorization", kwargs))
        return self._accepted("clear-operation")

    def hdhive_subscriptions(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("hdhive_subscriptions", kwargs))
        return SharingHttpResult({"success": True, "data": []})

    def create_hdhive_subscription(self, payload: dict[str, Any], **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("create_hdhive_subscription", (payload, kwargs)))
        return self._accepted("create-subscription")

    def hdhive_subscription_check(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("hdhive_subscription_check", kwargs))
        return SharingHttpResult({"success": True, "data": {"exists": False}})

    def delete_hdhive_subscription(self, subscription_id: int, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("delete_hdhive_subscription", (subscription_id, kwargs)))
        return self._accepted("delete-subscription")

    def hdhive_resources(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("hdhive_resources", kwargs))
        return SharingHttpResult({"success": True, "data": [], "total": 0, "filtered": 0})

    def hdhive_download(self, payload: dict[str, Any], **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("hdhive_download", (payload, kwargs)))
        return self._accepted("download-operation")

    def hdhive_checkin(self, payload: dict[str, Any], **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("hdhive_checkin", (payload, kwargs)))
        return self._accepted("checkin-operation")

    def telegram_configuration(self) -> SharingHttpResult:
        return SharingHttpResult({"success": True, "data": {"enabled": False, "password": ""}})

    def save_telegram_configuration(self, payload: dict[str, Any]) -> SharingHttpResult:
        self.calls.append(("save_telegram_configuration", payload))
        return SharingHttpResult({"success": True, "message": "频道订阅监听配置已保存生效"})

    def telegram_status(self, command_id: str = "") -> SharingHttpResult:
        if command_id:
            self.calls.append(("telegram_status", command_id))
            return SharingHttpResult(
                {
                    "success": False,
                    "accepted": True,
                    "status": "requesting",
                    "command_id": command_id,
                    "message": "Telegram 操作仍在处理中",
                }
            )
        return SharingHttpResult({"success": True, "data": {"status": "authorized"}})

    def telegram_send_code(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("telegram_send_code", kwargs))
        return SharingHttpResult({"success": True, "message": "验证码已发送到您的 Telegram 客户端"})

    def telegram_login(self, code: Any, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("telegram_login", (code, kwargs)))
        if not code:
            return SharingHttpResult({"success": False, "message": "请输入验证码"})
        return SharingHttpResult({"success": True, "message": "登录成功！监听服务已启动。"})

    def telegram_logout(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("telegram_logout", kwargs))
        return SharingHttpResult({"success": True, "message": "已注销并清除凭证"})

    def telegram_clear_session(self, **kwargs: Any) -> SharingHttpResult:
        self.calls.append(("telegram_clear_session", kwargs))
        return SharingHttpResult(
            {
                "success": True,
                "message": "未发现需要清理的 TG 登录会话文件",
                "data": {"deleted": [], "missing": ["tg_userbot.session"]},
            }
        )

    @staticmethod
    def _accepted(operation_id: str) -> SharingHttpResult:
        return SharingHttpResult(
            {
                "success": True,
                "accepted": True,
                "operation_id": operation_id,
                "status": "ready",
            },
            202,
        )


def _build_app(*, auth_enabled: bool = False) -> tuple[FastAPI, Permissions, Service]:
    permissions = Permissions()
    service = Service()
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={PROVIDER_CONTROL_USE_CASE: lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, service


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_provider_control_fixture_freezes_twenty_native_operations() -> None:
    app, _permissions, _service = _build_app()
    paths = app.openapi()["paths"]
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    actual = {(method, path) for path, operations in paths.items() for method in operations}

    assert actual == expected
    for route in FIXTURE["routes"]:
        operation = paths[route["path"]][route["method"].lower()]
        assert set(operation["responses"]) == {str(value) for value in route["response_statuses"]}
        assert ("requestBody" in operation) is route["request_body"]


def test_provider_control_routes_require_native_admin_permission() -> None:
    app, permissions, _service = _build_app(auth_enabled=True)
    with TestClient(app) as client:
        denied_get = client.get("/api/subscription/cloud/resources?title=Fixture")
        denied_post = client.post("/api/subscription/tg_userbot/logout")
        permissions.admins.add("admin")
        _set_user(client, "admin")
        allowed = client.get("/api/subscription/tg_userbot/status")

    assert denied_get.status_code == 403
    assert denied_get.json() == {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}
    assert denied_post.status_code == 403
    assert allowed.json() == {"success": True, "data": {"status": "authorized"}}


def test_provider_control_http_passes_idempotency_and_does_not_require_empty_control_bodies() -> None:
    app, _permissions, service = _build_app()
    headers = {"Idempotency-Key": "fixture-operation"}
    with TestClient(app) as client:
        checkin = client.post("/api/subscription/hdhive/checkin", json={}, headers=headers)
        send_code = client.post("/api/subscription/tg_userbot/send_code", headers=headers)
        logout = client.post("/api/subscription/tg_userbot/logout", headers=headers)
        clear = client.post("/api/subscription/tg_userbot/clear_session", headers=headers)

    assert checkin.status_code == 202
    assert send_code.status_code == 200
    assert logout.status_code == 200
    assert clear.status_code == 200
    assert service.calls == [
        (
            "hdhive_checkin",
            ({}, {"principal_id": "local", "idempotency_key": "fixture-operation"}),
        ),
        ("telegram_send_code", {"idempotency_key": "fixture-operation"}),
        ("telegram_logout", {"idempotency_key": "fixture-operation"}),
        ("telegram_clear_session", {"idempotency_key": "fixture-operation"}),
    ]


def test_provider_control_http_reads_durable_telegram_command_by_existing_status_route() -> None:
    app, _permissions, service = _build_app()
    with TestClient(app) as client:
        response = client.get("/api/subscription/tg_userbot/status?command_id=fixture-command")

    assert response.status_code == 200
    assert response.json() == {
        "success": False,
        "accepted": True,
        "status": "requesting",
        "command_id": "fixture-command",
        "message": "Telegram 操作仍在处理中",
    }
    assert service.calls == [("telegram_status", "fixture-command")]


def test_provider_control_body_transport_and_login_validation_are_legacy_shaped() -> None:
    app, _permissions, _service = _build_app()
    with TestClient(app) as client:
        malformed = client.post(
            "/api/subscription/hdhive/config",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/subscription/cloud/download",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
        scalar = client.post("/api/subscription/tg_userbot/login", json=[])
        missing_code = client.post("/api/subscription/tg_userbot/login", json={})

    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert scalar.status_code == 500
    assert scalar.headers["content-type"].startswith("text/html")
    assert missing_code.json() == {"success": False, "message": "请输入验证码"}
