from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

from etk.domains.media.bridge_client import HttpxPluginUpdateGateway
from etk.domains.media.models import (
    ImagePolicyKey,
    ImagePolicyRecord,
    MediaIdentity,
    MediaItemType,
    MetadataAggregate,
    ServiceResult,
)
from etk.domains.media.router import router
from etk.domains.media.service import MediaService
from etk.platform import Settings

_CONTRACT = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/p5-media-bridge.json").read_text(
        encoding="utf-8"
    )
)
_VALIDATION_FIXTURE = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/bridge-validation-errors.json").read_text(
        encoding="utf-8"
    )
)
_BRIDGE_MANIFEST = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/contracts/bridge-contract-ownership.yaml").read_text(
        encoding="utf-8"
    )
)


class FakeRepository:
    def __init__(self) -> None:
        self.policy: ImagePolicyRecord | None = None
        self.replacements: list[tuple[ImagePolicyKey, Sequence[Mapping[str, Any]], Sequence[Mapping[str, Any]]]] = []
        self.event_reservations: dict[str, str] = {}

    def resolve_by_path(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> MediaIdentity | None:
        if path == "/missing":
            return None
        return MediaIdentity(
            "100" if requested_type is MediaItemType.MOVIE else "200",
            "movie" if requested_type is MediaItemType.MOVIE else "tv",
            requested_type,
            season_number,
            episode_number,
        )

    def resolve_by_fingerprint(
        self,
        requested_type: MediaItemType,
        *,
        sha1: str = "",
        pick_code: str = "",
    ) -> MediaIdentity | None:
        del sha1, pick_code
        return self.resolve_by_path("/fixture", requested_type)

    def resolve_by_emby_id(self, item_id: str) -> MediaIdentity | None:
        del item_id
        return MediaIdentity("100", "movie", MediaItemType.MOVIE)

    def load_metadata(
        self,
        identity: MediaIdentity,
        requested_type: MediaItemType,
    ) -> MetadataAggregate | None:
        return MetadataAggregate(
            identity,
            requested_type,
            {
                "tmdb_id": "100",
                "item_type": requested_type.value,
                "title": "Fixture",
                "original_title": "Fixture",
                "overview": "Fixture metadata",
                "metadata_ready": True,
                "actors_ready": False,
                "genres_json": [],
                "keywords_json": [],
                "production_companies_json": [],
                "networks_json": [],
                "official_rating_json": {},
                "emby_item_ids_json": ["emby-100"],
                "season_number": identity.season_number,
                "episode_number": identity.episode_number,
            },
        )

    def find_backfill_targets_by_emby_ids(self, item_ids: Sequence[str]) -> tuple[MediaIdentity, ...]:
        del item_ids
        return (MediaIdentity("100", "movie", MediaItemType.MOVIE),)

    def find_backfill_targets_by_library(self, library_id: str) -> tuple[MediaIdentity, ...]:
        del library_id
        return (MediaIdentity("200", "tv", MediaItemType.SERIES),)

    def load_setting(self, key: str) -> Any:
        del key
        return None

    def get_image_policy(self, key: ImagePolicyKey) -> ImagePolicyRecord | None:
        del key
        return self.policy

    def replace_image_policy(
        self,
        key: ImagePolicyKey,
        rules: Sequence[Mapping[str, Any]],
        images: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        self.replacements.append((key, rules, images))
        return ()

    def source_is_referenced(self, source_url: str) -> bool:
        del source_url
        return False

    def bind_emby_item(self, **kwargs: Any) -> Mapping[str, Any]:
        return {"updated": True, **kwargs}

    def reserve_emby_event(self, event_key: str, ttl_seconds: int) -> str | None:
        del ttl_seconds
        if event_key in self.event_reservations:
            return None
        token = f"token-{len(self.event_reservations) + 1}"
        self.event_reservations[event_key] = token
        return token

    def release_emby_event(self, event_key: str, token: str) -> bool:
        return self.event_reservations.pop(event_key, None) == token


class FakeJobs:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> Any:
        del parameters, options
        self.keys.append(task_key)
        return SimpleNamespace()

    def current(self) -> Any:
        return None


class FakeImages:
    def screenshot_token(self, content_hash: str) -> str:
        return f"etk-cache://{content_hash}"

    def archive_metadata(
        self,
        payload: Mapping[str, Any],
        base_url: str,
    ) -> Mapping[str, Any]:
        del base_url
        return payload

    def live_candidates(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        include_all_languages: bool,
    ) -> list[dict[str, Any]]:
        del requested_type, identity, include_all_languages
        return [{"type": "Primary", "source_url": "http://fixture.invalid/primary.jpg", "width": 1000}]

    def archive_policy_images(
        self,
        images: Sequence[Mapping[str, Any]],
        base_url: str,
    ) -> list[dict[str, Any]]:
        del base_url
        return [dict(value) for value in images]

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> None:
        del sources

    def refresh_images(self, requested_type: MediaItemType, identity: MediaIdentity) -> int:
        del requested_type, identity
        return 3

    def schedule_missing_collection_cleanup(self, tmdb_id: str) -> None:
        del tmdb_id


class FakeEventGateway:
    def __init__(self, owner: str) -> None:
        self.owner = owner
        self.payloads: list[dict[str, Any]] = []

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        del host_url, remote_addr
        event = str(payload["Event"])
        self.payloads.append(dict(payload))
        return ServiceResult({"status": self.owner, "event": event}, 202)


class FakeEvents:
    def __init__(self) -> None:
        self.p5 = FakeEventGateway("p5-media-compatibility")
        self.future = FakeEventGateway("future-domain-compatibility")


class FakeContainer:
    def __init__(self, service: MediaService, plugin_updates: HttpxPluginUpdateGateway) -> None:
        self._service = service
        self._plugin_updates = plugin_updates

    def use_case(self, name: str) -> Any:
        assert name == "media"
        return self._service

    def client(self, name: str) -> Any:
        assert name == "media-plugin-updates"
        return self._plugin_updates


@contextmanager
def _client() -> Any:
    settings = Settings()
    repository = FakeRepository()
    jobs = FakeJobs()
    events = FakeEvents()
    service = MediaService(
        repository=repository,
        settings=lambda: settings,
        jobs=jobs,
        images=FakeImages(),
        p5_events=events.p5,
        future_events=events.future,
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            content=b"fixture-dll",
            headers={"Content-Length": "11"},
            request=request,
        )
    )
    plugin_updates = HttpxPluginUpdateGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=transport),
    )
    app = FastAPI()
    app.state.container = FakeContainer(service, plugin_updates)
    app.include_router(router)
    with TestClient(app) as client:
        yield client, repository, jobs, events, app


class _LegacyPluginArtifact:
    headers = {"Content-Length": "11"}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int) -> Any:
        assert chunk_size == 128 * 1024
        yield b"fixture-dll"

    def close(self) -> None:
        return None


@contextmanager
def _legacy_client(monkeypatch: pytest.MonkeyPatch) -> Any:
    webhook = import_module("routes.webhook")
    extensions = import_module("extensions")
    metadata = import_module("database.metadata_provider_db")
    image_policy = import_module("handler.media_image_policy")

    monkeypatch.setattr(extensions, "media_processor_instance", object())
    monkeypatch.setitem(webhook.config_manager.APP_CONFIG, "tmdb_api_key", "fixture-key")
    monkeypatch.setattr(webhook.task_manager, "submit_task", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        webhook,
        "_resolve_missing_metadata_backfill_targets",
        lambda item_ids: [{"tmdb_id": "100", "media_type": "movie"}],
    )
    monkeypatch.setattr(
        webhook,
        "_resolve_library_missing_metadata_backfill_targets",
        lambda library_id: [{"tmdb_id": "200", "media_type": "tv"}],
    )
    monkeypatch.setattr(
        metadata,
        "resolve_metadata_identity_by_path",
        lambda path, requested_type, **numbers: {
            "tmdb_id": "100",
            "media_type": "movie",
            "season_number": numbers.get("season_number"),
            "episode_number": numbers.get("episode_number"),
        },
    )
    monkeypatch.setattr(
        image_policy,
        "get_live_image_candidates",
        lambda *args, **kwargs: [
            {
                "type": "Primary",
                "source_url": "http://fixture.invalid/primary.jpg",
                "width": 1000,
            }
        ],
    )
    monkeypatch.setattr(
        image_policy,
        "sync_image_policy",
        lambda *args, **kwargs: [
            {
                "type": "Primary",
                "source_url": "http://fixture.invalid/primary.jpg",
                "width": 1000,
                "index": 0,
            }
        ],
    )
    monkeypatch.setattr(webhook.requests, "get", lambda *args, **kwargs: _LegacyPluginArtifact())

    app = Flask("p5-legacy-bridge-golden")
    app.config.update(TESTING=True)
    app.register_blueprint(webhook.webhook_bp)
    with app.test_client() as client:
        yield client


def _response_snapshot(response: Any, expected: Mapping[str, Any]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {"status": response.status_code}
    expected_headers = expected.get("headers")
    if isinstance(expected_headers, Mapping):
        observed_headers = {str(key).lower(): value for key, value in response.headers.items()}
        snapshot["headers"] = {str(key): observed_headers[str(key)] for key in expected_headers}
    if "body_sha256" in expected:
        raw = response.get_data() if hasattr(response, "get_data") else response.content
        snapshot["body_sha256"] = hashlib.sha256(raw).hexdigest()
    else:
        body = response.get_json(silent=True) if hasattr(response, "get_json") else response.json()
        snapshot["body"] = body
    return snapshot


def _legacy_request(client: Any, request_contract: Mapping[str, Any]) -> Any:
    return client.open(
        str(request_contract["path"]),
        method=str(request_contract["method"]),
        query_string=request_contract.get("query"),
        json=request_contract.get("body"),
    )


def _native_request(client: TestClient, request_contract: Mapping[str, Any]) -> Any:
    return client.request(
        str(request_contract["method"]),
        str(request_contract["path"]),
        params=request_contract.get("query"),
        json=request_contract.get("body"),
    )


def _minimum_emby_event_payload(event_name: str, index: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": event_name,
        "EventId": f"fixture-event-{index}",
    }
    if event_name == "user.policyupdated":
        payload["User"] = {"Id": "user-1", "Name": "Fixture User"}
        return payload
    if event_name == "collection.items.removed":
        payload["Item"] = {"Id": "collection-1", "Type": "BoxSet", "Name": "Fixture Collection"}
        return payload
    if event_name in {"metadata.update", "image.update"}:
        payload["Item"] = {
            "Id": "movie-1",
            "Type": "Movie",
            "Name": "Fixture Movie",
            "Path": "/fixture/media/movie.mkv",
        }
        if event_name == "image.update":
            payload["Image"] = {
                "Type": "Primary",
                "Url": "https://image.tmdb.org/t/p/original/fixture.jpg",
            }
        return payload

    item_type = "Episode" if event_name == "playback.halfway" else "Movie"
    payload["User"] = {"Id": "user-1", "Name": "Fixture User"}
    payload["Item"] = {
        "Id": "item-1",
        "Type": item_type,
        "Name": "Fixture Item",
        "SeriesName": "Fixture Series" if item_type == "Episode" else None,
    }
    if event_name.startswith("item."):
        payload["UserData"] = {
            "IsFavorite": event_name == "item.markfavorite",
            "Played": event_name == "item.markplayed",
        }
    if event_name.startswith("playback."):
        payload["PlaybackInfo"] = {
            "PositionTicks": 60,
            "RunTimeTicks": 100,
            "PlayedToCompletion": event_name == "playback.stop",
        }
    return payload


def test_all_ten_inbound_bridge_operations_match_the_independent_legacy_golden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cases = _CONTRACT["golden_master"]["inbound_cases"]
    assert {(case["request"]["method"], case["request"]["path"]) for case in cases} == {
        (contract["method"], contract["path"]) for contract in _CONTRACT["inbound"]
    }

    with _legacy_client(monkeypatch) as legacy, _client() as (native, *_rest):
        for case in cases:
            expected = case["response"]
            legacy_response = _legacy_request(legacy, case["request"])
            native_response = _native_request(native, case["request"])
            assert _response_snapshot(legacy_response, expected) == expected, case["case_id"]
            assert _response_snapshot(native_response, expected) == expected, case["case_id"]


def test_emby_event_manifest_splits_transport_and_business_execution_ownership() -> None:
    event_contract = next(
        contract
        for contract in _BRIDGE_MANIFEST["contracts"]
        if contract["direction"] == "inbound" and contract["path"] == "/api/emby/events"
    )
    ownership = event_contract["event_ownership"]

    assert ownership == _CONTRACT["event_ownership"]
    assert {value["event"] for value in ownership} == set(_CONTRACT["emby_events"])
    assert [value["event"] for value in ownership if value["target_stage"] == "P5"] == [
        "metadata.update",
        "image.update",
    ]
    assert all(value["transport_owner"] == "etk.domains.media.router" for value in ownership)
    assert all(value["execution_owner"] == "routes.webhook.emby_webhook" for value in ownership)
    assert sum(value["execution_kind"] == "future-domain-legacy-compatibility" for value in ownership) == 11


def test_ten_bridge_inbound_routes_execute_the_native_contract() -> None:
    with _client() as (client, repository, jobs, _events, _app):
        invalid = client.get("/api/emby/metadata")
        metadata = client.get(
            "/api/emby/metadata",
            params={"path": "/movie.mkv", "item_type": "Movie"},
        )
        all_backfill = client.post("/api/emby/metadata/backfill")
        one_backfill = client.post("/api/emby/metadata/backfill/item", json={"emby_item_id": "one"})
        many_backfill = client.post(
            "/api/emby/metadata/backfill/items",
            json={"emby_item_ids": ["one", "one", "two"]},
        )
        library_backfill = client.post(
            "/api/emby/metadata/backfill/library",
            json={"emby_library_id": "7", "refresh_mode": "missing_metadata"},
        )
        search = client.get(
            "/api/emby/metadata/images/search",
            params={"path": "/movie.mkv", "item_type": "Movie", "include_all_languages": "true"},
        )
        refresh = client.post(
            "/api/emby/metadata/images/refresh",
            params={"path": "/movie.mkv", "item_type": "Movie"},
        )
        sync = client.post(
            "/api/emby/metadata/images/sync",
            params={"path": "/movie.mkv", "item_type": "Movie"},
            json={"rules": [{"Type": "Primary", "Limit": 1}]},
        )
        plugin = client.get("/api/emby/plugin-update")

    assert invalid.status_code == 400
    assert invalid.json() == {"error": "invalid metadata path request"}
    assert metadata.status_code == 200
    assert metadata.headers["Cache-Control"] == "no-store"
    assert metadata.json()["item_type"] == "Movie"
    assert all_backfill.status_code == 202
    assert one_backfill.status_code == 202
    assert many_backfill.status_code == 202
    assert library_backfill.status_code == 202
    assert jobs.keys == ["backfill-media-metadata"] * 4
    assert search.status_code == 200
    assert search.headers["Cache-Control"] == "no-store"
    assert refresh.json() == {"ok": True, "updated": 3}
    assert sync.status_code == 200
    assert repository.replacements
    assert plugin.status_code == 200
    assert plugin.content == b"fixture-dll"
    assert plugin.headers["Content-Disposition"] == 'attachment; filename="ETKMediaInfoBridge.dll"'
    assert plugin.headers["Cache-Control"] == "no-store"


def test_thirteen_emby_event_names_dispatch_to_two_p5_and_eleven_future_domain_events() -> None:
    names = list(_CONTRACT["emby_events"])
    payloads = [_minimum_emby_event_payload(name, index) for index, name in enumerate(names, start=1)]
    payload_by_event = {str(payload["Event"]): payload for payload in payloads}
    assert payload_by_event["user.policyupdated"]["User"] == {"Id": "user-1", "Name": "Fixture User"}
    assert payload_by_event["metadata.update"]["Item"]["Path"] == "/fixture/media/movie.mkv"
    assert payload_by_event["image.update"]["Image"]["Type"] == "Primary"
    assert payload_by_event["collection.items.removed"]["Item"]["Type"] == "BoxSet"
    for name in {
        "item.markfavorite",
        "item.unmarkfavorite",
        "item.markplayed",
        "item.markunplayed",
        "item.rate",
    }:
        assert payload_by_event[name]["User"]["Id"] == "user-1"
        assert payload_by_event[name]["Item"]["Id"] == "item-1"
        assert "UserData" in payload_by_event[name]
    for name in {"playback.start", "playback.pause", "playback.stop", "playback.halfway"}:
        assert payload_by_event[name]["User"]["Id"] == "user-1"
        assert payload_by_event[name]["Item"]["Id"] == "item-1"
        assert payload_by_event[name]["PlaybackInfo"]["RunTimeTicks"] == 100

    with _client() as (client, _repository, _jobs, events, _app):
        rejected = client.post(
            "/api/emby/events",
            json={"_etk_source": "wrong", "Event": "metadata.update"},
        )
        responses = [client.post("/api/emby/events", json=payload) for payload in payloads]

    assert rejected.status_code == 400
    assert rejected.json() == {"status": "invalid_plugin_event"}
    assert [response.status_code for response in responses] == [202] * len(names)
    assert [payload["Event"] for payload in events.p5.payloads] == ["metadata.update", "image.update"]
    assert [payload["Event"] for payload in events.future.payloads] == [
        "user.policyupdated",
        "item.markfavorite",
        "item.unmarkfavorite",
        "item.markplayed",
        "item.markunplayed",
        "item.rate",
        "playback.start",
        "playback.pause",
        "playback.stop",
        "playback.halfway",
        "collection.items.removed",
    ]
    assert events.p5.payloads == [
        payload for payload in payloads if payload["Event"] in {"metadata.update", "image.update"}
    ]
    assert events.future.payloads == [
        payload for payload in payloads if payload["Event"] not in {"metadata.update", "image.update"}
    ]
    assert [response.json()["status"] for response in responses] == [
        "p5-media-compatibility" if name in {"metadata.update", "image.update"} else "future-domain-compatibility"
        for name in names
    ]


def test_unknown_emby_event_keeps_the_legacy_ignored_response_outside_both_owners() -> None:
    with _client() as (client, _repository, _jobs, events, _app):
        response = client.post(
            "/api/emby/events",
            json={
                "_etk_source": "ETKMediaInfoBridge",
                "Event": "plugin.future-event",
                "EventId": "unknown-fixture-event",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "event_ignored_not_in_trigger_list"}
    assert events.p5.payloads == []
    assert events.future.payloads == []


def test_frozen_bridge_validation_errors_execute_through_the_native_router() -> None:
    cases = [case for case in _VALIDATION_FIXTURE["cases"] if str(case["request"]["path"]).startswith("/api/emby/")]
    with _client() as (client, _repository, _jobs, _events, _app):
        responses = [
            client.request(
                case["request"]["method"],
                case["request"]["path"],
                params=case["request"].get("query"),
                json=case["request"].get("body"),
            )
            for case in cases
        ]

    assert len(cases) == 3
    for case, response in zip(cases, responses, strict=True):
        assert response.status_code == case["response"]["status"]
        assert response.json() == case["response"]["body"]


def test_p5_openapi_declares_every_json_body_without_automatic_422() -> None:
    with _client() as (_client_value, _repository, _jobs, _events, app):
        schema = app.openapi()
    expected = {value["path"] for value in _CONTRACT["inbound"]}
    assert set(schema["paths"]) == expected
    for path in (
        "/api/emby/metadata/backfill/item",
        "/api/emby/metadata/backfill/items",
        "/api/emby/metadata/backfill/library",
        "/api/emby/metadata/images/sync",
        "/api/emby/events",
    ):
        operation = schema["paths"][path]["post"]
        assert operation["requestBody"]
        assert "422" not in operation["responses"]


def test_p5_openapi_declares_exact_bridge_response_status_matrix() -> None:
    with _client() as (_client_value, _repository, _jobs, _events, app):
        schema = app.openapi()
    contracts = {
        (str(contract["method"]).lower(), str(contract["path"])): {
            int(status) for status in contract["response_statuses"]
        }
        for contract in _BRIDGE_MANIFEST["contracts"]
        if contract["direction"] == "inbound" and str(contract["path"]).startswith("/api/emby/")
    }

    assert len(contracts) == 10
    for (method, path), expected_statuses in contracts.items():
        declared_statuses = {
            int(status) for status in schema["paths"][path][method]["responses"] if str(status).isdigit()
        }
        assert declared_statuses == expected_statuses, f"{method.upper()} {path}"
