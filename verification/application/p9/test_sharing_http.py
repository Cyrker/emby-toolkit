from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.sharing import InMemoryContributionLedger, SharingService
from etk.domains.sharing.router import router
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p9-sharing-http-fixture-key"


class FakePermissions:
    @staticmethod
    def is_admin(user_id: str) -> bool:
        return user_id == "admin"

    @staticmethod
    def is_active(user_id: str) -> bool:
        return bool(user_id)


class FakeCenter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def list_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("list_sources", dict(query)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"items": [{"id": "source-1"}], "total": 1})

    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("register_device", (name, server_id_hash)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"device": {"id": "device-1"}})

    def device_status(self) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("device_status", {}))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"device": {"id": "device-1"}})

    def health(self) -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True})

    def register_source(self, source_kind: str, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("register_source", (source_kind, dict(payload))))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 201, {"source_id": "source-1"})

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("acquire_transfer_lease", dict(payload)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"lease": "lease-1"})

    def report_transfer(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("report_transfer", dict(payload)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"accepted": True})

    def quote_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("quote_share_request", dict(payload)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"cost": 1})

    def list_share_requests(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("list_share_requests", dict(query)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"items": [], "total": 0})

    def create_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("create_share_request", dict(payload)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 202, {"group_id": "group-1"})


def _build_app() -> tuple[FastAPI, FakeCenter]:
    center = FakeCenter()
    app = FastAPI()
    app.state.container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app.state.sharing_service = SharingService(InMemoryContributionLedger({"admin": 5}), center)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: LegacySessionCodec.from_secret_key(SECRET_KEY))
    app.include_router(router)
    return app, center


def _set_admin(client: TestClient) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": "admin"}))


def test_native_sharing_routes_require_admin_and_cover_core_contracts() -> None:
    app, center = _build_app()
    with TestClient(app) as client:
        assert client.get("/api/shared/resources/credit/balance").status_code == 403
        _set_admin(client)
        balance = client.get("/api/shared/resources/credit/balance")
        ledger = client.get("/api/shared/resources/credit/ledger?limit=1")
        sources = client.get("/api/shared/resources/center/sources?limit=2&offset=3")
        registered = client.post(
            "/api/shared/resources/shares/manual-create",
            json={"source_kind": "movie", "tmdb_id": "101"},
        )
        lease = client.post("/api/shared/resources/transfers/lease", json={"source_id": "source-1"})
        report = client.post("/api/shared/resources/transfers/report", json={"source_id": "source-1", "result": "ok"})
        quoted = client.post("/api/shared/resources/share-requests/quote", json={"tmdb_id": "101"})
        listed = client.get("/api/shared/resources/share-requests")
        created = client.post("/api/shared/resources/share-requests", json={"tmdb_id": "101"})

    assert balance.json()["credit"] == 5
    assert ledger.json()["count"] == 0
    assert sources.json()["items"] == [{"id": "source-1"}]
    assert registered.status_code == 201
    assert lease.status_code == 200
    assert report.status_code == 200
    assert quoted.json()["cost"] == 1
    assert listed.status_code == 200
    assert created.status_code == 202
    assert [name for name, _payload in center.calls] == [
        "list_sources",
        "register_source",
        "acquire_transfer_lease",
        "report_transfer",
        "quote_share_request",
        "list_share_requests",
        "create_share_request",
    ]
    assert center.calls[0][1]["limit"] == 2
    assert center.calls[0][1]["offset"] == 3


def test_native_sharing_routes_reject_malformed_json_and_secret_payloads() -> None:
    app, center = _build_app()
    with TestClient(app) as client:
        _set_admin(client)
        malformed = client.post(
            "/api/shared/resources/transfers/report",
            content="not-json",
            headers={"content-type": "application/json"},
        )
        secret = client.post(
            "/api/shared/resources/shares/manual-create",
            json={"source_kind": "movie", "api_key": "must-not-persist"},
        )

    assert malformed.status_code == 400
    assert secret.status_code == 400
    assert [name for name, _payload in center.calls] == []
