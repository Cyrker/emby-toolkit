from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.domains.media.models import MediaIdentity, MediaItemType, ServiceResult
from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.p115.media_info import P115MediaInfoCacheRecord, P115MediaInfoCacheService
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings

SHA_A = "A" * 40
REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p6-p115-mediainfo.json"
FIXTURE: Mapping[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
FIXTURE_CASES: tuple[Mapping[str, Any], ...] = tuple(FIXTURE["cases"])


class FakeCredentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(account_id="115001")

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("HTTP query must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("HTTP query must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeRepository:
    def __init__(self, record: P115MediaInfoCacheRecord | None) -> None:
        self.record = record

    def find_by_sha1(self, sha1: str, *, record_hit: bool = False) -> P115MediaInfoCacheRecord | None:
        del record_hit
        return self.record if self.record is not None and self.record.sha1 == sha1 else None

    def resolve_sha1_by_pick_code(self, pick_code: str, *, account_id: str) -> str | None:
        if pick_code == "pc-a" and account_id == "115001" and self.record is not None:
            return self.record.sha1
        return None


class FakeMediaService:
    def __init__(self, backfill: ServiceResult | None = None) -> None:
        self.backfill = backfill or ServiceResult({"ok": True, "submitted": True}, 202)
        self.metadata_calls: list[tuple[MediaIdentity, MediaItemType, str]] = []
        self.backfill_calls = 0

    def provide_bridge_metadata_identity(
        self,
        identity: MediaIdentity,
        requested_type: MediaItemType,
        *,
        base_url: str,
    ) -> ServiceResult:
        self.metadata_calls.append((identity, requested_type, base_url))
        return ServiceResult(
            {
                "ProviderIds": {"Tmdb": identity.tmdb_id},
                "Type": requested_type.value,
                "IndexNumber": identity.episode_number,
            },
            headers={"Cache-Control": "no-store"},
        )

    def backfill_all(self) -> ServiceResult:
        self.backfill_calls += 1
        return self.backfill


def _record(
    *,
    formatted: object | None = None,
    raw: object | None = None,
) -> P115MediaInfoCacheRecord:
    return P115MediaInfoCacheRecord.create(
        sha1=SHA_A,
        formatted=(
            [
                {
                    "MediaSourceInfo": {"MediaStreams": [], "Container": "mkv"},
                    "Chapters": [],
                }
            ]
            if formatted is None
            else formatted
        ),
        raw=(
            {
                "_etk": {
                    "tmdb_id": "101",
                    "type": "episode",
                    "sha1": SHA_A,
                    "season_number": 1,
                    "episode_number": 2,
                }
            }
            if raw is None
            else raw
        ),
        cache_revision=0,
        raw_schema_version=1,
        formatter_revision="p6-v1",
        probe_revision="ffprobe-v1",
        hit_count=0,
    )


def _client(
    record: P115MediaInfoCacheRecord | None,
    *,
    media: FakeMediaService | None = None,
) -> tuple[TestClient, AppContainer, FakeMediaService]:
    container = AppContainer(Settings())
    media_service = media or FakeMediaService()
    app = FastAPI()
    app.state.container = container
    app.state.p115_media_info_service = P115MediaInfoCacheService(FakeRepository(record), FakeCredentials())
    app.state.media_service = media_service
    app.include_router(router)
    return TestClient(app), container, media_service


def test_fixture_defines_only_the_six_p6_6_native_mediainfo_operations() -> None:
    assert FIXTURE["schema_version"] == 1
    assert FIXTURE["captured_live"] is False
    assert {(route["method"], route["path"]) for route in FIXTURE["routes"]} == {
        ("GET", "/api/p115/mediainfo/<pick_code>"),
        ("GET", "/api/p115/mediainfo/sha1/<sha1>"),
        ("GET", "/api/p115/mediainfo/<pick_code>/metadata"),
        ("GET", "/api/p115/mediainfo/sha1/<sha1>/metadata"),
        ("POST", "/api/p115/mediainfo/<pick_code>/metadata/backfill"),
        ("POST", "/api/p115/mediainfo/sha1/<sha1>/metadata/backfill"),
    }
    notes = " ".join(str(note) for note in FIXTURE["compatibility_notes"])
    assert "are now separately owned by P6.7 and P6.10" in notes


@pytest.mark.parametrize("case", FIXTURE_CASES, ids=[str(case["case_id"]) for case in FIXTURE_CASES])
def test_frozen_p6_6_mediainfo_http_cases(case: Mapping[str, Any]) -> None:
    record_mode = str(case["record"])
    if record_mode == "valid":
        record = _record()
    elif record_mode == "invalid_formatted":
        record = _record(formatted={"unexpected": True})
    else:
        assert record_mode == "missing"
        record = None
    client, container, _media = _client(record)
    request = case["request"]
    expected = case["expected"]
    assert isinstance(request, Mapping)
    assert isinstance(expected, Mapping)
    try:
        with client:
            response = client.request(
                str(request["method"]),
                str(request["path"]),
                params=request.get("query"),
            )
    finally:
        container.close()

    assert response.status_code != 422 or case["case_id"] == "invalid-formatted-cache-is-422"
    assert response.status_code == expected["status"]
    assert response.json() == expected["body"]
    for name, value in (expected.get("headers") or {}).items():
        assert response.headers[str(name)] == value


def test_formatted_cache_endpoints_preserve_status_shape_and_no_store() -> None:
    client, container, _media = _client(_record())
    try:
        with client:
            by_sha1 = client.get(f"/api/p115/mediainfo/sha1/{SHA_A.lower()}")
            by_pick_code = client.get("/api/p115/mediainfo/pc-a")
    finally:
        container.close()

    for response in (by_sha1, by_pick_code):
        assert response.status_code == 200
        assert response.headers["Cache-Control"] == "no-store"
        assert response.json() == {
            "MediaSourceInfo": {"MediaStreams": [], "Container": "mkv", "Chapters": []},
            "Chapters": [],
        }


@pytest.mark.parametrize(
    ("record", "path", "status", "payload"),
    [
        (_record(), "/api/p115/mediainfo/sha1/not-sha1", 400, {"error": "invalid sha1"}),
        (None, f"/api/p115/mediainfo/sha1/{SHA_A}", 404, {"error": "media info cache not found"}),
        (
            _record(formatted={"unexpected": True}),
            f"/api/p115/mediainfo/sha1/{SHA_A}",
            422,
            {"error": "invalid media info cache"},
        ),
        (_record(), "/api/p115/mediainfo/missing-pc", 404, {"error": "media info cache not found"}),
    ],
)
def test_formatted_cache_errors_are_not_fastapi_validation_drift(
    record: P115MediaInfoCacheRecord | None,
    path: str,
    status: int,
    payload: Mapping[str, str],
) -> None:
    client, container, _media = _client(record)
    try:
        with client:
            response = client.get(path)
    finally:
        container.close()
    assert response.status_code == status
    assert response.json() == payload


def test_metadata_endpoints_delegate_verified_identity_to_p5_service() -> None:
    client, container, media = _client(_record())
    try:
        with client:
            by_sha1 = client.get(
                f"/api/p115/mediainfo/sha1/{SHA_A}/metadata",
                params={"item_type": "Episode", "episode_number": "7"},
            )
            by_pick_code = client.get("/api/p115/mediainfo/pc-a/metadata")
    finally:
        container.close()

    assert by_sha1.status_code == 200
    assert by_sha1.headers["Cache-Control"] == "no-store"
    assert by_sha1.json()["IndexNumber"] == 7
    assert by_pick_code.status_code == 200
    assert by_pick_code.json()["Type"] == "Series"
    assert [call[1] for call in media.metadata_calls] == [MediaItemType.EPISODE, MediaItemType.SERIES]
    assert media.metadata_calls[0][0].tmdb_id == "101"
    assert media.metadata_calls[0][0].season_number == 1


def test_metadata_endpoint_rejects_invalid_item_type_and_missing_raw_identity() -> None:
    client, container, _media = _client(_record())
    try:
        with client:
            invalid = client.get(
                f"/api/p115/mediainfo/sha1/{SHA_A}/metadata",
                params={"item_type": "Audio"},
            )
    finally:
        container.close()
    assert invalid.status_code == 400
    assert invalid.json() == {"error": "invalid item_type"}

    missing_client, missing_container, _media = _client(_record(raw={}))
    try:
        with missing_client:
            missing = missing_client.get(f"/api/p115/mediainfo/sha1/{SHA_A}/metadata")
    finally:
        missing_container.close()
    assert missing.status_code == 404
    assert missing.json() == {"error": "metadata identity not found"}


@pytest.mark.parametrize(
    ("result", "status", "payload"),
    [
        (ServiceResult({"ok": True, "submitted": True}, 202), 202, {"ok": True, "submitted": True}),
        (
            ServiceResult({"ok": True, "submitted": False, "reason": "task_already_running"}),
            200,
            {"ok": True, "submitted": False, "reason": "task_already_running"},
        ),
        (
            ServiceResult({"ok": False, "submitted": False, "reason": "etk_busy"}, 409),
            409,
            {"ok": False, "submitted": False, "reason": "etk_busy"},
        ),
    ],
)
def test_backfill_endpoints_preserve_global_task_submission_contract(
    result: ServiceResult,
    status: int,
    payload: Mapping[str, Any],
) -> None:
    media = FakeMediaService(result)
    client, container, _media = _client(_record(), media=media)
    try:
        with client:
            response = client.post(f"/api/p115/mediainfo/sha1/{SHA_A}/metadata/backfill")
    finally:
        container.close()
    assert response.status_code == status
    assert response.json() == payload
    assert media.backfill_calls == 1


def test_backfill_identity_miss_is_404_and_does_not_submit() -> None:
    media = FakeMediaService()
    client, container, _media = _client(None, media=media)
    try:
        with client:
            by_sha1 = client.post(f"/api/p115/mediainfo/sha1/{SHA_A}/metadata/backfill")
            by_pick_code = client.post("/api/p115/mediainfo/missing/metadata/backfill")
    finally:
        container.close()
    assert by_sha1.status_code == 404
    assert by_pick_code.status_code == 404
    assert media.backfill_calls == 0
