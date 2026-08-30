from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import SimpleNamespace
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
from etk.domains.sharing.operations import (
    SharingOperationConflictError,
    SharingOperationKind,
    SharingOperationScopeBusyError,
    SharingOperationStatus,
    sharing_operation_key,
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
                    "server_id_hash": "13bbd32d4a8d08f48cf9aa2169adc4163f008e9dd115a201f64f4e6a182acba1",
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


class FakeOperations:
    def __init__(self) -> None:
        self.intents: list[Any] = []
        self.by_key: dict[str, Any] = {}
        self.by_id: dict[Any, Any] = {}

    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]:
        existing = self.by_key.get(intent.operation_key)
        if existing is not None:
            if (
                existing.request_hash != intent.request_hash
                or existing.kind is not intent.kind
                or existing.scope_key != intent.scope_key
            ):
                raise SharingOperationConflictError("fixture operation identity changed")
            return existing, True
        if any(
            operation.scope_key == intent.scope_key and not operation.status.terminal
            for operation in self.by_id.values()
        ):
            raise SharingOperationScopeBusyError("fixture operation scope is busy")
        operation = SimpleNamespace(
            id=intent.id,
            operation_key=intent.operation_key,
            request_hash=intent.request_hash,
            kind=intent.kind,
            scope_key=intent.scope_key,
            request=intent.request,
            status=SharingOperationStatus.READY,
            attempts=0,
            state_revision=0,
            available_at=intent.available_at,
            step_started_at=None,
            cancel_requested_at=None,
            finished_at=None,
            result=None,
            error=None,
        )
        self.intents.append(intent)
        self.by_key[intent.operation_key] = operation
        self.by_id[intent.id] = operation
        return operation, False

    def reserve(self, intent: Any) -> Any:
        operation, _replayed = self.reserve_with_replay(intent)
        return operation

    def read(self, operation_id: Any) -> Any | None:
        return self.by_id.get(operation_id)

    def request_cancel(self, operation_id: Any) -> Any | None:
        operation = self.by_id.get(operation_id)
        if operation is None or operation.status.terminal:
            return operation
        operation.cancel_requested_at = datetime.now(UTC)
        operation.state_revision += 1
        if operation.step_started_at is None:
            operation.status = SharingOperationStatus.CANCELLED
            operation.finished_at = operation.cancel_requested_at
            operation.error = {
                "type": "cancelled_before_provider_request",
                "provider_may_have_been_called": False,
            }
        return operation


def _build_app() -> tuple[FastAPI, FakeCenter, FakeAppSettings, FakeOperations]:
    center = FakeCenter()
    app_settings = FakeAppSettings()
    operations = FakeOperations()
    app = FastAPI()
    app.state.container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app.state.sharing_service = SharingService(
        InMemoryContributionLedger({"admin": 5}),
        center,
        operations,
        configuration=SharingConfigurationService(app_settings),
        identity=SharingServerIdentity({}, app_settings),
    )
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: LegacySessionCodec.from_secret_key(SECRET_KEY))
    app.include_router(router)
    return app, center, app_settings, operations


def _set_admin(client: TestClient) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": "admin"}))


def test_native_sharing_routes_require_admin_and_cover_core_contracts() -> None:
    app, center, _app_settings, operations = _build_app()
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
        operation_status = client.get(f"/api/shared/resources/operations/{registered.json()['operation_id']}/status")

    assert balance.json()["credit"] == 5
    assert ledger.json()["count"] == 0
    source = sources.json()["items"][0]
    assert source["id"] == "source-1"
    assert source["version_summary"] == {"resolution": "2160p", "video_codec": "hevc", "codec": "hevc"}
    assert source["is_completed_certified"] is True
    assert source["has_children"] is True
    assert "children" not in source
    assert registered.status_code == 202
    assert lease.status_code == 202
    assert report.status_code == 202
    assert quoted.json()["cost"] == 1
    assert listed.status_code == 200
    assert created.status_code == 202
    assert operation_status.status_code == 200
    assert operation_status.json()["operation"]["status"] == "ready"
    assert [name for name, _payload in center.calls] == [
        "list_display_sources",
        "quote_share_request",
        "list_share_requests",
    ]
    assert [intent.request["action"] for intent in operations.intents] == [
        "register_source",
        "acquire_transfer_lease",
        "report_transfer",
        "create_share_request",
    ]
    assert center.calls[0][1]["limit"] == 2
    assert center.calls[0][1]["offset"] == 3


def test_native_sharing_routes_reject_malformed_json_and_secret_payloads() -> None:
    app, center, _app_settings, operations = _build_app()
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
        oversized_key = client.post(
            "/api/shared/resources/center/device/register",
            json={},
            headers={"Idempotency-Key": "x" * 257},
        )

    assert malformed.status_code == 400
    assert secret.status_code == 400
    assert oversized_key.status_code == 400
    assert [name for name, _payload in center.calls] == []
    assert operations.intents == []


def test_native_sharing_operation_cancellation_contract() -> None:
    app, _center, _app_settings, operations = _build_app()
    with TestClient(app) as client:
        denied = client.post(f"/api/shared/resources/operations/{'0' * 32}/cancel")
        _set_admin(client)
        missing = client.post(f"/api/shared/resources/operations/{'0' * 32}/cancel")
        staged = client.post(
            "/api/shared/resources/shares/manual-create",
            json={"source_kind": "movie", "root_fid": "root-cancel"},
        )
        operation_id = staged.json()["operation_id"]
        cancelled = client.post(f"/api/shared/resources/operations/{operation_id}/cancel")
        replay = client.post(f"/api/shared/resources/operations/{operation_id}/cancel")

        requesting = client.post(
            "/api/shared/resources/shares/manual-create",
            json={"source_kind": "movie", "root_fid": "root-requesting"},
        )
        requesting_operation = operations.by_id[
            next(key for key in operations.by_id if str(key) == requesting.json()["operation_id"])
        ]
        requesting_operation.status = SharingOperationStatus.REQUESTING
        requesting_operation.step_started_at = datetime.now(UTC)
        requested = client.post(f"/api/shared/resources/operations/{requesting.json()['operation_id']}/cancel")

        confirmed = client.post(
            "/api/shared/resources/shares/manual-create",
            json={"source_kind": "movie", "root_fid": "root-confirmed"},
        )
        confirmed_operation = operations.by_id[
            next(key for key in operations.by_id if str(key) == confirmed.json()["operation_id"])
        ]
        confirmed_operation.status = SharingOperationStatus.CONFIRMED
        confirmed_operation.finished_at = datetime.now(UTC)
        conflict = client.post(f"/api/shared/resources/operations/{confirmed.json()['operation_id']}/cancel")

    assert denied.status_code == 403
    assert missing.status_code == 404
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    assert replay.status_code == 200
    assert requested.status_code == 202
    assert requested.json()["status"] == "requesting"
    assert conflict.status_code == 409


def test_native_sharing_mutations_replay_and_reject_identity_or_scope_conflicts() -> None:
    app, center, _app_settings, operations = _build_app()
    headers = {"Idempotency-Key": "fixture-source-registration"}
    payload = {"source_kind": "movie", "root_fid": "root-1", "tmdb_id": "101", "title": "Fixture"}
    with TestClient(app) as client:
        _set_admin(client)
        created = client.post(
            "/api/shared/resources/shares/manual-create?account_id=attacker-controlled",
            json=payload,
            headers=headers,
        )
        replay = client.post("/api/shared/resources/shares/manual-create", json=payload, headers=headers)
        identity_conflict = client.post(
            "/api/shared/resources/shares/manual-create",
            json={**payload, "title": "Changed"},
            headers=headers,
        )
        scope_conflict = client.post(
            "/api/shared/resources/shares/manual-create",
            json={**payload, "title": "Changed"},
            headers={"Idempotency-Key": "fixture-source-registration-2"},
        )

    assert created.status_code == 202
    assert replay.status_code == 202
    assert replay.json()["operation_id"] == created.json()["operation_id"]
    assert replay.json()["replayed"] is True
    assert identity_conflict.status_code == 409
    assert scope_conflict.status_code == 409
    assert len(operations.intents) == 1
    assert operations.intents[0].operation_key == sharing_operation_key(
        kind=SharingOperationKind.REGISTER_SOURCE,
        principal_id="admin",
        idempotency_key="fixture-source-registration",
    )
    assert center.calls == []


def test_native_sharing_config_device_identity_and_display_browse_contracts() -> None:
    app, center, app_settings, operations = _build_app()
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
    assert registered.status_code == 202
    assert registered.json()["status"] == "ready"
    assert len(operations.intents) == 1
    device_request = operations.intents[0].request
    assert device_request["action"] == "register_device"
    assert device_request["payload"]["server_id_hash"] == (
        "13bbd32d4a8d08f48cf9aa2169adc4163f008e9dd115a201f64f4e6a182acba1"
    )
    assert status.json()["local_server_id_hash"] == device_request["payload"]["server_id_hash"]
    assert status.json()["device"]["forced_offline"] is True
    assert status.json()["device"]["forced_offline_reason"] == "fixture"
    assert tags.json()["items"] == [{"label": "4K", "value": "4k"}]
    assert home.json()["sections"][0]["items"][0]["size"] == 5
    assert home.json()["items"] == []
    assert children.json()["children"][0]["id"] == "episode-1"
    assert detail.json()["data"]["resources"][0]["id"] == "resource-1"
    assert detail.json()["children"] == []
    assert app_settings.values["shared_resource_config"]["p115_shared_resource_enabled"] is True
    assert "register_device" not in [name for name, _payload in center.calls]

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
