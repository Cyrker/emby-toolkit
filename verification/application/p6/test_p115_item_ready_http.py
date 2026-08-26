from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.bootstrap.app_factory import create_app
from etk.domains.ingestion import PostgresIngestionMonitorLedger
from etk.domains.media.models import MediaItemType
from etk.domains.p115.item_ready import P115ItemReadyService
from etk.domains.p115.media_info import P115MediaInfoNotFound
from etk.domains.p115.router import router
from etk.processes.p6_ingestion import PersistentPendingBindingGateway

SHA1 = "A" * 40
FIXTURE = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/p6-p115-ingestion-delete.json").read_text(
        encoding="utf-8"
    )
)


class FakeIdentities:
    def __init__(self) -> None:
        self.values = {"pc-fixture": SHA1}
        self.calls: list[str] = []

    def sha1_by_pick_code(self, pick_code: object) -> str:
        normalized = str(pick_code or "")
        self.calls.append(normalized)
        if normalized not in self.values:
            raise P115MediaInfoNotFound("missing")
        return self.values[normalized]


class FakeVerifier:
    def __init__(self, item: Mapping[str, Any] | None = None) -> None:
        self.item = item
        self.calls: list[tuple[str, str, str]] = []

    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        self.calls.append((sha1, item_id, expected_pick_code))
        return self.item


class FakePendingBindings:
    def __init__(
        self,
        outcome: Mapping[str, Any] | None = None,
        replay_outcome: Mapping[str, Any] | None = None,
    ) -> None:
        self.outcome = outcome
        self.replay_outcome = replay_outcome
        self.calls: list[dict[str, Any]] = []
        self.replay_calls: list[dict[str, str]] = []

    def replay_pending_binding(
        self,
        *,
        emby_item_id: str,
        sha1: str,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        self.replay_calls.append({"emby_item_id": emby_item_id, "sha1": sha1, "pick_code": expected_pick_code})
        return self.replay_outcome

    def accept_pending_binding(
        self,
        item: Mapping[str, Any],
        *,
        sha1: str,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        self.calls.append({"item": dict(item), "sha1": sha1, "pick_code": expected_pick_code})
        return self.outcome


class FakeMedia:
    def __init__(self, outcome: Mapping[str, Any] | None = None) -> None:
        self.outcome = outcome or {"updated": False, "tmdb_id": "100"}
        self.calls: list[dict[str, Any]] = []

    def bind_emby_item(self, **kwargs: Any) -> Mapping[str, Any]:
        self.calls.append(dict(kwargs))
        return self.outcome


def _item() -> dict[str, Any]:
    return {
        "Id": "101",
        "Type": "Movie",
        "Path": "/fixture/movie.mkv",
        "ProviderIds": {"Tmdb": "100"},
    }


def _service(
    *,
    item: Mapping[str, Any] | None = None,
    pending: Mapping[str, Any] | None = None,
    replay: Mapping[str, Any] | None = None,
    media: Mapping[str, Any] | None = None,
) -> tuple[P115ItemReadyService, FakeIdentities, FakeVerifier, FakePendingBindings, FakeMedia]:
    identities = FakeIdentities()
    verifier = FakeVerifier(_item() if item is None else item)
    bindings = FakePendingBindings(pending, replay)
    media_port = FakeMedia(media)
    return (
        P115ItemReadyService(
            identities=identities,
            emby=verifier,
            pending_bindings=bindings,
            media=media_port,
        ),
        identities,
        verifier,
        bindings,
        media_port,
    )


def _client(service: P115ItemReadyService) -> TestClient:
    app = FastAPI()
    app.state.p115_item_ready_service = service
    app.include_router(router)
    return TestClient(app)


def _case(case_id: str) -> Mapping[str, Any]:
    return next(case for case in FIXTURE["cases"] if case["case_id"] == case_id)


def test_fixture_defines_the_two_auth_none_item_ready_contracts() -> None:
    routes = [entry for entry in FIXTURE["routes"] if entry["path"].endswith("/item-ready")]
    assert routes == [
        {
            "method": "POST",
            "path": "/api/p115/mediainfo/<pick_code>/item-ready",
            "auth": "none",
            "request_fields": ["path.identity", "query.emby_item_id"],
            "response_statuses": [200, 404, 409, 503],
        },
        {
            "method": "POST",
            "path": "/api/p115/mediainfo/sha1/<sha1>/item-ready",
            "auth": "none",
            "request_fields": ["path.identity", "query.emby_item_id"],
            "response_statuses": [200, 404, 409, 503],
        },
    ]


@pytest.mark.parametrize(
    "case_id",
    ["item-ready-pickcode-accepted", "item-ready-sha1-accepted"],
)
def test_item_ready_pickcode_and_sha1_routes_match_the_accepted_fixture(case_id: str) -> None:
    case = _case(case_id)
    service, _identities, verifier, pending, media = _service(pending={"ok": True, "accepted": True, "item_id": "101"})
    with _client(service) as client:
        response = client.request(case["request"]["method"], case["request"]["path"])

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert len(verifier.calls) == 1
    assert len(pending.calls) == 1
    assert media.calls == []


def test_pickcode_cache_miss_matches_fixture_without_verification_or_side_effects() -> None:
    case = _case("item-ready-pickcode-cache-missing")
    service, _identities, verifier, pending, media = _service()
    with _client(service) as client:
        response = client.request(case["request"]["method"], case["request"]["path"])

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert verifier.calls == []
    assert pending.calls == []
    assert media.calls == []


def test_sha1_identity_mismatch_matches_fixture_without_binding_side_effects() -> None:
    case = _case("item-ready-sha1-identity-mismatch")
    service, _identities, verifier, pending, media = _service(item={})
    with _client(service) as client:
        response = client.request(case["request"]["method"], case["request"]["path"])

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert len(verifier.calls) == 1
    assert pending.calls == []
    assert media.calls == []


def test_durable_pending_replay_is_returned_without_native_rebind() -> None:
    service, _identities, _verifier, pending, media = _service(
        replay={"ok": True, "skipped": True, "reason": "already_completed"}
    )

    result = service.accept_by_sha1(SHA1, "101")

    assert result.status_code == 200
    assert result.payload == {"ok": True, "skipped": True, "reason": "already_completed"}
    assert len(pending.replay_calls) == 1
    assert pending.calls == []
    assert media.calls == []


def test_non_pending_callback_invokes_only_the_native_media_binding_port() -> None:
    service, _identities, _verifier, pending, media = _service(
        media={
            "updated": True,
            "parent_updates": 0,
            "tmdb_id": "100",
            "title": "Fixture",
            "identity_restored": False,
        }
    )

    result = service.accept_by_pick_code("pc-fixture", "101")

    assert result.status_code == 200
    assert result.payload == {
        "ok": True,
        "rebound": True,
        "updated": True,
        "parent_updates": 0,
        "tmdb_id": "100",
        "title": "Fixture",
        "identity_restored": False,
    }
    assert len(pending.calls) == 1
    assert media.calls == [
        {
            "item_id": "101",
            "item_type": MediaItemType.MOVIE,
            "item_path": "/fixture/movie.mkv",
            "sha1": SHA1,
            "pick_code": "pc-fixture",
            "tmdb_id": "100",
            "series_id": "",
            "season_id": "",
        }
    ]


def test_native_rebind_retryable_mapping_matches_fixture() -> None:
    case = _case("item-ready-rebind-retryable")
    service, _identities, _verifier, _pending, _media = _service(media={"updated": False, "reason": "asset_not_found"})
    with _client(service) as client:
        response = client.request(case["request"]["method"], case["request"]["path"])

    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]


def test_default_bootstrap_registers_item_ready_and_durable_pending_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from etk.bootstrap import app_factory

    database = type(
        "FixtureDatabase",
        (),
        {
            "unit_of_work": staticmethod(lambda: None),
            "raw_pool": object(),
            "engine": object(),
            "close": staticmethod(lambda: None),
        },
    )()
    monkeypatch.setattr(app_factory, "_create_database_runtime", lambda _container: database)
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=b"p6-item-ready-bootstrap",
    )
    container = app.state.container
    try:
        assert isinstance(container.repository("ingestion.monitor"), PostgresIngestionMonitorLedger)
        assert isinstance(container.client("ingestion.pending-bindings"), PersistentPendingBindingGateway)
        assert isinstance(container.use_case("p115.item-ready"), P115ItemReadyService)
    finally:
        container.close()


def test_invalid_verified_item_stops_before_pending_or_media_ports() -> None:
    service, _identities, verifier, pending, media = _service(item={"Id": "101", "Type": "Series"})

    result = service.accept_by_sha1(SHA1, "101")

    assert result.status_code == 409
    assert result.payload == {"ok": False, "skipped": True, "reason": "invalid_item"}
    assert len(verifier.calls) == 1
    assert pending.calls == []
    assert media.calls == []
