from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115.playback import PlaybackDirectResponse, PlaybackUpstreamStream
from etk.domains.p115.playback_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-playback.json").read_text(encoding="utf-8"))
HTTP_CASES = tuple(FIXTURE["http_cases"])


class FakePermissions:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def is_admin(self, user_id: str) -> bool:
        self.calls.append(user_id)
        return user_id == "fixture-admin"


class FakePlaybackService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.direct_response = PlaybackDirectResponse("error", 404, "fixture direct link not found")
        self.temp_error: Exception | None = None

    def get_public_config(self) -> dict[str, Any]:
        self.calls.append(("get_public_config", None))
        return {"enabled": True, "usable_count": 1, "accounts": []}

    def save_pool_settings(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(("save_pool_settings", dict(payload)))
        return {"enabled": bool(payload.get("enabled")), "accounts": []}

    def add_account(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(("add_account", dict(payload)))
        return {"id": "fixture-account", "alias": payload.get("alias", "Fixture")}

    def update_account(self, account_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(("update_account", (account_id, dict(payload))))
        return {"id": account_id, **dict(payload)}

    def delete_account(self, account_id: str) -> bool:
        self.calls.append(("delete_account", account_id))
        return account_id == "fixture-account"

    def speedtest_account(self, account_id: str, intent_key: str = "") -> dict[str, Any]:
        self.calls.append(("speedtest_account", (account_id, intent_key)))
        return {"bps": 1024, "speed_text": "1.00 KB/s"}

    def cleanup(self) -> int:
        self.calls.append(("cleanup", None))
        return 2

    def get_temp_config(self) -> dict[str, Any]:
        self.calls.append(("get_temp_config", None))
        return {"name": "ETK临时目录", "cid": "fixture-cid", "cleanup_cron": "0 * * * *"}

    def save_temp_config(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(("save_temp_config", dict(payload)))
        if self.temp_error is not None:
            raise self.temp_error
        return {"name": "ETK临时目录", "cid": "fixture-cid", **dict(payload), "accounts": []}

    def direct_play(self, pick_code: str, user_agent: str, range_header: str) -> PlaybackDirectResponse:
        self.calls.append(("direct_play", (pick_code, user_agent, range_header)))
        return self.direct_response


class FakeQrService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.sessions: dict[tuple[str, str], str] = {}

    def create(self, owner_user_id: str, *, mode: str, app_type: str) -> dict[str, Any]:
        self.calls.append(("create", (owner_user_id, mode, app_type)))
        uid = f"uid-{owner_user_id}"
        self.sessions[(owner_user_id, uid)] = mode
        return {"success": True, "data": {"qrcode": f"https://qr.fixture.invalid/{owner_user_id}", "uid": uid}}

    def poll(self, owner_user_id: str, *, provider_uid: str = "") -> dict[str, Any]:
        self.calls.append(("poll", (owner_user_id, provider_uid)))
        if (owner_user_id, provider_uid) not in self.sessions:
            return {"success": False, "status": "expired", "message": "请先获取二维码"}
        return {"success": True, "status": "waiting", "message": "等待扫码"}


def _build_native(
    *, auth_enabled: bool = False
) -> tuple[TestClient, FakePlaybackService, FakeQrService, LegacySessionCodec]:
    permissions = FakePermissions()
    container = AppContainer(
        Settings.model_validate({"auth_enabled": auth_enabled}),
        repository_providers={"identity": lambda _: permissions},
    )
    playback = FakePlaybackService()
    qr = FakeQrService()
    codec = LegacySessionCodec.from_secret_key(b"p6-p115-playback-session")
    app = FastAPI()
    app.state.container = container
    app.state.p115_playback_service = playback
    app.state.p115_playback_qr_service = qr
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), playback, qr, codec


def _set_session(client: TestClient, codec: LegacySessionCodec, values: Mapping[str, Any]) -> None:
    client.cookies.set("session", codec.dumps(values))


def _request(client: TestClient, request: Mapping[str, Any]):
    kwargs: dict[str, Any] = {
        "headers": request.get("headers"),
        "follow_redirects": False,
    }
    if "body" in request:
        kwargs["json"] = request["body"]
    elif "raw_body" in request:
        kwargs["content"] = request["raw_body"]
    return client.request(str(request["method"]), str(request["path"]), **kwargs)


def _snapshot(response: Any, expected: Mapping[str, Any]) -> dict[str, Any]:
    value: dict[str, Any] = {"status": response.status_code}
    if "text" in expected:
        value["text"] = response.text
    if "body" in expected:
        value["body"] = response.json()
    if "headers" in expected:
        value["headers"] = {name: response.headers[name] for name in expected["headers"]}
    return value


@pytest.mark.parametrize("case", HTTP_CASES, ids=[case["case_id"] for case in HTTP_CASES])
def test_frozen_playback_http_cases(case: Mapping[str, Any]) -> None:
    client, playback, _qr, codec = _build_native(auth_enabled=bool(case.get("settings", {}).get("auth_enabled")))
    if case.get("session") is not None:
        _set_session(client, codec, case["session"])
    service = case.get("service", {})
    if service.get("direct_kind") == "error":
        playback.direct_response = PlaybackDirectResponse("error", int(service["status"]), str(service["body"]))
    elif service.get("direct_kind") == "redirect":
        playback.direct_response = PlaybackDirectResponse(
            "redirect",
            int(service["status"]),
            location=str(service["location"]),
        )
    if service.get("temp_error"):
        playback.temp_error = RuntimeError(str(service["temp_error"]))

    with client:
        response = _request(client, case["request"])

    assert _snapshot(response, case["response"]) == case["response"]
    if case["case_id"] == "admin-denial-precedes-json":
        assert playback.calls == []
    if case["case_id"] in {"empty-body-without-json-content-type", "empty-json-body"}:
        assert playback.calls == []
    if case["case_id"] == "public-direct-play-redirect":
        assert playback.calls == [("direct_play", ("fixture-pick", "FixturePlayer/1.0", "bytes=7-19"))]


def test_head_is_always_empty_200_without_service_calls() -> None:
    client, playback, _qr, _codec = _build_native()
    with client:
        plain = client.head("/api/p115/play/fixture-pick")
        named = client.head("/api/p115/play/fixture-pick/Nested/Movie.mkv")
    assert (plain.status_code, plain.content) == (200, b"")
    assert (named.status_code, named.content) == (200, b"")
    assert playback.calls == []


def test_stream_response_forwards_status_headers_chunks_and_closes() -> None:
    closed: list[bool] = []
    client, playback, _qr, _codec = _build_native()
    playback.direct_response = PlaybackDirectResponse(
        "stream",
        206,
        stream=PlaybackUpstreamStream(
            status_code=206,
            headers={"content-type": "video/mp4", "content-range": "bytes 0-6/7"},
            chunks=(b"fixture",),
            close=lambda: closed.append(True),
        ),
    )
    with client:
        response = client.get("/api/p115/play/fixture-pick", headers={"range": "bytes=0-6"})
    assert response.status_code == 206
    assert response.content == b"fixture"
    assert response.headers["content-range"] == "bytes 0-6/7"
    assert closed == [True]


def test_stream_response_closes_when_chunk_iteration_fails() -> None:
    closed: list[bool] = []

    def broken_chunks():
        yield b"fixture"
        raise RuntimeError("fixture stream failed")

    client, playback, _qr, _codec = _build_native()
    playback.direct_response = PlaybackDirectResponse(
        "stream",
        200,
        stream=PlaybackUpstreamStream(
            status_code=200,
            headers={"content-type": "video/mp4"},
            chunks=broken_chunks(),
            close=lambda: closed.append(True),
        ),
    )
    with client, pytest.raises((RuntimeError, ExceptionGroup), match="fixture stream failed"):
        client.get("/api/p115/play/fixture-pick")
    assert closed == [True]


def test_qrcode_sessions_are_scoped_to_the_logged_in_owner() -> None:
    client, _playback, qr, codec = _build_native()
    with client:
        _set_session(client, codec, {"emby_user_id": "fixture-user-a"})
        created = client.get("/api/p115/play_pool/cookie_qrcode?mode=openapi&app=alipaymini")
        uid = created.json()["data"]["uid"]

        _set_session(client, codec, {"emby_user_id": "fixture-user-b"})
        foreign = client.get("/api/p115/play_pool/cookie_qrcode/status", params={"uid": uid})

        _set_session(client, codec, {"emby_user_id": "fixture-user-a"})
        owned = client.get("/api/p115/play_pool/cookie_qrcode/status", params={"uid": uid})

    assert foreign.json() == {"success": False, "status": "expired", "message": "请先获取二维码"}
    assert owned.json() == {"success": True, "status": "waiting", "message": "等待扫码"}
    assert qr.calls == [
        ("create", ("fixture-user-a", "openapi", "alipaymini")),
        ("poll", ("fixture-user-b", "uid-fixture-user-a")),
        ("poll", ("fixture-user-a", "uid-fixture-user-a")),
    ]


def test_remaining_admin_routes_dispatch_without_contract_coercion() -> None:
    client, playback, _qr, codec = _build_native(auth_enabled=True)
    with client:
        _set_session(client, codec, {"emby_user_id": "fixture-admin", "emby_username": "Fixture Admin"})
        responses = [
            client.post("/api/p115/play_pool", json={"enabled": False}),
            client.post("/api/p115/play_pool/accounts", json={"cookie": "fixture-cookie", "alias": "Fixture"}),
            client.put("/api/p115/play_pool/accounts/fixture-account", json={"enabled": False}),
            client.delete("/api/p115/play_pool/accounts/fixture-account"),
            client.post(
                "/api/p115/play_pool/accounts/fixture-account/speedtest",
                headers={"idempotency-key": "fixture-intent"},
            ),
            client.post("/api/p115/play_pool/cleanup"),
            client.get("/api/p115/temp_dir_config"),
            client.post("/api/p115/temp_dir_config", json={"cleanup_cron": ""}),
        ]

    assert all(response.status_code == 200 for response in responses)
    assert ("speedtest_account", ("fixture-account", "fixture-intent")) in playback.calls


def test_openapi_declares_exact_15_bindings_without_automatic_422() -> None:
    client, _playback, _qr, _codec = _build_native()
    schema = cast(FastAPI, client.app).openapi()
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    expected_paths = {path for _method, path in expected}
    actual = {
        (method, path)
        for path, operations in schema["paths"].items()
        if path in expected_paths
        for method in operations
    }
    assert len(expected) == 15
    assert actual == expected
    for method, path in expected:
        assert "422" not in schema["paths"][path][method]["responses"], (method, path)
    play_responses = schema["paths"]["/api/p115/play/{pick_code}"]["get"]["responses"]
    assert {"206", "default"}.issubset(play_responses)
    assert schema["paths"]["/api/p115/play_pool"]["post"]["requestBody"]["required"] is True


@pytest.mark.parametrize("user_id", ["fixture-user", "fixture-admin"])
def test_user_contribution_and_reward_endpoints_are_removed(user_id: str) -> None:
    client, playback, qr, codec = _build_native(auth_enabled=True)
    with client:
        _set_session(client, codec, {"emby_user_id": user_id})
        for method, path in (
            ("GET", "/api/p115/play_pool/user-account"),
            ("POST", "/api/p115/play_pool/user-account"),
            ("DELETE", "/api/p115/play_pool/user-account"),
            ("GET", "/api/p115/play_pool/user-account/rewards"),
        ):
            assert client.request(method, path).status_code == 404
    assert playback.calls == []
    assert qr.calls == []


@pytest.mark.parametrize("path", ["cookie_qrcode", "cookie_qrcode/status"])
def test_pool_qrcode_is_only_available_to_administrators(path: str) -> None:
    client, _playback, qr, codec = _build_native(auth_enabled=True)
    with client:
        _set_session(client, codec, {"emby_user_id": "fixture-user"})
        assert client.get(f"/api/p115/play_pool/{path}").status_code == 403
        _set_session(client, codec, {"emby_user_id": "fixture-admin"})
        assert client.get(f"/api/p115/play_pool/{path}").status_code == 200
    assert len(qr.calls) == 1
