from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.sharing import (
    InMemoryContributionLedger,
    SharingConfigurationService,
    SharingServerIdentity,
    SharingService,
)
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

    def list_display_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("list_display_sources", dict(query)))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "items": [
                    {
                        "id": "source-1",
                        "source_kind": "logical_season",
                        "pool_complete": True,
                        "total_size": 12,
                        "media_signature_json": {"resolution": "2160p", "video_codec": "hevc"},
                        "children": [{"id": "episode-1"}],
                    }
                ],
                "total": 1,
            },
        )

    def list_display_tags(self) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("list_display_tags", {}))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"items": [{"label": "4K", "value": "4k"}]})

    def list_display_home(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("list_display_home", dict(query)))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"sections": [{"key": "latest", "items": [{"id": "home-1", "total_size": 5}]}]},
        )

    def list_display_children(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("list_display_children", dict(query)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"children": [{"id": "episode-1"}]})

    def display_detail(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("display_detail", dict(query)))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"resources": [{"id": "resource-1", "children": [{"id": "hidden"}]}]},
        )

    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("register_device", (name, server_id_hash)))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"device": {"id": "device-1"}})

    def device_status(self) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("device_status", {}))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "device": {
                    "id": "device-1",
                    "share_heartbeat_meta_json": {"forced_offline": True, "ban_reason": "fixture"},
                }
            },
        )

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


class DisplayParityCenter:
    @staticmethod
    def list_display_sources(**query: Any) -> ProviderResponse[dict[str, Any]]:
        del query
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "items": [
                    {
                        "id": "jsonish",
                        "media_signature_json": '{"resolution":"2160p","video_codec":"hevc"}',
                        "completed_certified_meta_json": '{"is_completed_certified":true}',
                        "children_count": "unknown",
                        "children": [{"id": "child-1"}],
                    },
                    {
                        "id": "ongoing",
                        "is_ongoing_hub": True,
                        "is_short_drama": False,
                        "children": [
                            {
                                "completed_certified_meta_json": '{"is_completed_certified":true}',
                                "short_drama_meta_json": '{"is_short_drama":true}',
                            }
                        ],
                    },
                ]
            },
        )

    @staticmethod
    def display_detail(**query: Any) -> ProviderResponse[dict[str, Any]]:
        del query
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "resources": [
                    {
                        "id": "season-pack",
                        "item_type": "Season",
                        "children": [{"id": "must-be-lazy"}],
                    }
                ]
            },
        )


class FakeAppSettings:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {
            "emby_server_id_cache": "fixture-server-id",
            "shared_resource_config": {"p115_shared_center_url": "https://center.fixture"},
        }

    def load(self, key: str) -> Any:
        return self.values.get(key)

    def save(self, key: str, value: Any) -> None:
        self.values[key] = value


def _build_app() -> tuple[FastAPI, FakeCenter, FakeAppSettings]:
    center = FakeCenter()
    app_settings = FakeAppSettings()
    app = FastAPI()
    app.state.container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app.state.sharing_service = SharingService(
        InMemoryContributionLedger({"admin": 5}),
        center,
        configuration=SharingConfigurationService(app_settings),
        identity=SharingServerIdentity({}, app_settings),
    )
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: LegacySessionCodec.from_secret_key(SECRET_KEY))
    app.include_router(router)
    return app, center, app_settings


def _set_admin(client: TestClient) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": "admin"}))


def test_native_sharing_routes_require_admin_and_cover_core_contracts() -> None:
    app, center, _app_settings = _build_app()
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
    source = sources.json()["items"][0]
    assert source["id"] == "source-1"
    assert source["version_summary"] == {"resolution": "2160p", "video_codec": "hevc", "codec": "hevc"}
    assert source["is_completed_certified"] is True
    assert source["has_children"] is True
    assert "children" not in source
    assert registered.status_code == 201
    assert lease.status_code == 200
    assert report.status_code == 200
    assert quoted.json()["cost"] == 1
    assert listed.status_code == 200
    assert created.status_code == 202
    assert [name for name, _payload in center.calls] == [
        "list_display_sources",
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
    app, center, _app_settings = _build_app()
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


def test_native_sharing_config_device_identity_and_display_browse_contracts() -> None:
    app, center, app_settings = _build_app()
    with TestClient(app) as client:
        _set_admin(client)
        initial = client.get("/api/shared/resources/config")
        saved = client.post(
            "/api/shared/resources/config",
            json={
                "p115_shared_resource_enabled": "yes",
                "p115_shared_center_url": "https://new-center.fixture/",
                "p115_shared_resource_mode": "legacy",
                "p115_shared_virtual_auto_promote_movie_percent": 500,
                "p115_shared_max_active_shares": 9,
                "p115_shared_center_home_sections": [{"key": "latest"}],
            },
        )
        registered = client.post("/api/shared/resources/center/device/register", json={})
        status = client.get("/api/shared/resources/center/device/status")
        tags = client.get("/api/shared/resources/center/sources/tags")
        home = client.get("/api/shared/resources/center/sources/home?limit_per_section=3")
        children = client.get(
            "/api/shared/resources/center/sources/children?source_id=logical-1&source_ids=logical-2,logical-1"
        )
        detail = client.get(
            "/api/shared/resources/center/sources/detail?source_kind=logical_season&source_id=logical-1"
        )

    assert initial.json()["data"]["p115_shared_resource_mode"] == "rapid"
    saved_config = saved.json()["data"]
    assert saved_config["p115_shared_resource_enabled"] is True
    assert saved_config["p115_shared_center_url"] == "https://new-center.fixture"
    assert saved_config["p115_shared_resource_mode"] == "rapid"
    assert saved_config["p115_shared_virtual_auto_promote_movie_percent"] == 100
    assert "p115_shared_max_active_shares" not in saved_config
    assert registered.status_code == 200
    assert registered.json()["data"]["p115_shared_resource_enabled"] is True
    assert center.calls[0] == (
        "register_device",
        ("ETK Device", "13bbd32d4a8d08f48cf9aa2169adc4163f008e9dd115a201f64f4e6a182acba1"),
    )
    assert status.json()["local_server_id_hash"] == center.calls[0][1][1]
    assert status.json()["device"]["forced_offline"] is True
    assert status.json()["device"]["forced_offline_reason"] == "fixture"
    assert tags.json()["items"] == [{"label": "4K", "value": "4k"}]
    assert home.json()["sections"][0]["items"][0]["size"] == 5
    assert home.json()["items"] == []
    assert children.json()["children"][0]["id"] == "episode-1"
    assert detail.json()["data"]["resources"][0]["id"] == "resource-1"
    assert detail.json()["children"] == []
    assert app_settings.values["shared_resource_config"]["p115_shared_resource_enabled"] is True

    calls = {name: payload for name, payload in center.calls[2:]}
    assert calls["list_display_home"]["limit_per_section"] == 3
    assert calls["list_display_children"]["source_ids"] == ["logical-2", "logical-1"]
    assert calls["display_detail"]["source_kind"] == "logical_season"


def test_display_browse_preserves_jsonish_flags_pack_slimming_and_safe_counts() -> None:
    service = SharingService(InMemoryContributionLedger(), DisplayParityCenter())

    listed = service.list_sources()
    detailed = service.display_detail()

    jsonish, ongoing = listed.payload["items"]
    assert jsonish["version_summary"] == {"resolution": "2160p", "video_codec": "hevc", "codec": "hevc"}
    assert jsonish["is_completed_certified"] is True
    assert jsonish["children_count"] == 1
    assert ongoing["is_completed_certified"] is False
    assert ongoing["is_short_drama"] is False
    assert ongoing["short_drama_meta_json"]["manual_override"] is True
    assert "children" not in detailed.payload["resources"][0]
