from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.domains.p115.intro_sync import (
    P115IntroSyncConflict,
    P115IntroSyncService,
    extract_intro_chapters,
    merge_intro_chapters,
)
from etk.domains.p115.media_info import P115MediaInfoCacheRecord
from etk.domains.p115.router import router

SHA_A = "A" * 40
REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p6-p115-intro-sync.json"
FIXTURE: Mapping[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
FIXTURE_CASES: tuple[Mapping[str, Any], ...] = tuple(FIXTURE["cases"])

INTRO_CHAPTERS = [
    {
        "StartPositionTicks": 10,
        "Name": "片头",
        "MarkerType": "IntroStart",
        "ChapterIndex": 1,
    },
    {
        "StartPositionTicks": 20,
        "Name": "片头结束",
        "MarkerType": "IntroEnd",
        "ChapterIndex": 2,
    },
]
OLD_INTRO_CHAPTERS = [
    {
        "StartPositionTicks": 1,
        "Name": "旧片头",
        "MarkerType": "IntroStart",
        "ChapterIndex": 1,
    },
    {
        "StartPositionTicks": 2,
        "Name": "旧片头结束",
        "MarkerType": "IntroEnd",
        "ChapterIndex": 2,
    },
]


class FakeFeature:
    def __init__(self, enabled: bool) -> None:
        self.value = enabled
        self.calls = 0

    def enabled(self) -> bool:
        self.calls += 1
        return self.value


class FakeVerifier:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.calls: list[tuple[str, str, str]] = []

    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        self.calls.append((sha1, item_id, expected_pick_code))
        if self.mode == "mismatch":
            return None
        if self.mode == "matched_without_intro":
            return {"Name": "Fixture.mkv", "Chapters": [{"Name": "普通章节"}]}
        assert self.mode == "matched_intro"
        return {"Name": "Fixture.mkv", "Chapters": deepcopy(INTRO_CHAPTERS)}


class FakeRepository:
    def __init__(self, record: P115MediaInfoCacheRecord | None, *, cas_failures: int = 0) -> None:
        self.record = record
        self.cas_failures = cas_failures
        self.find_calls = 0
        self.cas_calls = 0
        self.resolve_calls: list[tuple[str, str]] = []

    def find_by_sha1(
        self,
        sha1: str,
        *,
        record_hit: bool = False,
    ) -> P115MediaInfoCacheRecord | None:
        assert record_hit is False
        self.find_calls += 1
        return self.record if self.record is not None and self.record.sha1 == sha1 else None

    def resolve_sha1_by_pick_code(self, pick_code: str, *, account_id: str) -> str | None:
        self.resolve_calls.append((pick_code, account_id))
        return SHA_A if pick_code == "pc-a" and account_id == "115001" else None

    def replace_formatted_if_revision(
        self,
        sha1: str,
        *,
        expected_cache_revision: int,
        formatted: object,
    ) -> P115MediaInfoCacheRecord | None:
        self.cas_calls += 1
        if self.record is None or self.record.sha1 != sha1:
            return None
        if self.cas_failures > 0:
            self.cas_failures -= 1
            concurrent = deepcopy(self.record.formatted)
            if isinstance(concurrent, list) and concurrent and isinstance(concurrent[0], dict):
                concurrent[0]["ConcurrentRevision"] = self.record.cache_revision + 1
            self.record = replace(
                self.record,
                formatted=concurrent,
                cache_revision=self.record.cache_revision + 1,
            )
            return None
        if self.record.cache_revision != expected_cache_revision:
            return None
        self.record = replace(
            self.record,
            formatted=deepcopy(formatted),
            cache_revision=self.record.cache_revision + 1,
        )
        return self.record


class FakeExporter:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    def publish_if_changed(
        self,
        *,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None:
        self.calls.append(
            {
                "sha1": sha1,
                "chapters": [dict(chapter) for chapter in chapters],
                "file_name": file_name,
                "reason": reason,
                "cache_revision": cache_revision,
            }
        )
        if self.fail:
            raise RuntimeError("fixture export failure")


def _formatted(chapters: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "MediaSourceInfo": {"MediaStreams": [], "Container": "mkv"},
            "Chapters": [
                {"StartPositionTicks": 0, "Name": "开场", "ChapterIndex": 0},
                *[dict(chapter) for chapter in chapters],
                {
                    "StartPositionTicks": 100,
                    "Name": "片尾",
                    "MarkerType": "CreditsStart",
                    "ChapterIndex": 3,
                },
            ],
        }
    ]


def _record(mode: str) -> P115MediaInfoCacheRecord | None:
    if mode == "missing":
        return None
    assert mode in {"old_intro", "current_intro"}
    return P115MediaInfoCacheRecord.create(
        sha1=SHA_A,
        formatted=_formatted(INTRO_CHAPTERS if mode == "current_intro" else OLD_INTRO_CHAPTERS),
        raw={"fixture": True},
        cache_revision=4,
        raw_schema_version=1,
        formatter_revision="p6-v1",
        probe_revision="ffprobe-v1",
        hit_count=0,
    )


def _service(
    *,
    feature_enabled: bool = True,
    record: P115MediaInfoCacheRecord | None = None,
    verifier_mode: str = "matched_intro",
    cas_failures: int = 0,
    export_failure: bool = False,
) -> tuple[P115IntroSyncService, FakeRepository, FakeFeature, FakeVerifier, FakeExporter]:
    repository = FakeRepository(record, cas_failures=cas_failures)
    feature = FakeFeature(feature_enabled)
    verifier = FakeVerifier(verifier_mode)
    exporter = FakeExporter(fail=export_failure)
    service = P115IntroSyncService(
        repository,
        feature,
        verifier,
        exporter,
        lambda: "115001",
    )
    return service, repository, feature, verifier, exporter


def _client(service: P115IntroSyncService) -> TestClient:
    app = FastAPI()
    app.state.p115_intro_sync_service = service
    app.include_router(router)
    return TestClient(app)


def test_fixture_freezes_two_auth_none_bodyless_intro_sync_routes() -> None:
    assert FIXTURE["schema_version"] == 1
    assert FIXTURE["captured_live"] is False
    assert {(route["method"], route["path"]) for route in FIXTURE["routes"]} == {
        ("POST", "/api/p115/mediainfo/<pick_code>/intro-sync"),
        ("POST", "/api/p115/mediainfo/sha1/<sha1>/intro-sync"),
    }
    for route in FIXTURE["routes"]:
        assert route["auth"] == "none"
        assert route["request_body"] is False
        assert route["query"] == [{"name": "emby_item_id", "required": False}]


@pytest.mark.parametrize("case", FIXTURE_CASES, ids=[str(case["case_id"]) for case in FIXTURE_CASES])
def test_frozen_intro_sync_http_cases(case: Mapping[str, Any]) -> None:
    service, repository, feature, verifier, exporter = _service(
        feature_enabled=str(case["feature"]) == "enabled",
        record=_record(str(case["record"])),
        verifier_mode=str(case["verifier"]),
    )
    request = case["request"]
    expected = case["expected"]
    assert isinstance(request, Mapping)
    assert isinstance(expected, Mapping)
    with _client(service) as client:
        response = client.request(
            str(request["method"]),
            str(request["path"]),
            params=request.get("query"),
        )

    assert response.status_code != 422
    assert response.status_code == expected["status"]
    assert response.json() == expected["body"]
    if case["case_id"] == "pick-code-updated":
        assert repository.cas_calls == 1
        assert exporter.calls[0]["cache_revision"] == 5
        assert verifier.calls == [(SHA_A, "1001", "pc-a")]
    elif case["case_id"] == "sha1-unchanged":
        assert repository.cas_calls == 0
        assert exporter.calls == []
    elif case["case_id"] == "disabled-before-sha1-and-item-validation":
        assert feature.calls == 1
        assert verifier.calls == []
        assert repository.find_calls == 0


def test_openapi_declares_optional_query_without_422_body_or_auth_drift() -> None:
    app = FastAPI()
    app.include_router(router)
    paths = app.openapi()["paths"]
    expected_statuses = {
        "/api/p115/mediainfo/{pick_code}/intro-sync": {"200", "404"},
        "/api/p115/mediainfo/sha1/{sha1}/intro-sync": {"200"},
    }
    for path, statuses in expected_statuses.items():
        operation = paths[path]["post"]
        parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
        path_name = "pick_code" if "pick_code" in path else "sha1"
        assert parameters[path_name]["in"] == "path"
        assert parameters[path_name]["required"] is True
        assert parameters["emby_item_id"]["in"] == "query"
        assert parameters["emby_item_id"]["required"] is False
        assert set(operation["responses"]) == statuses
        assert "requestBody" not in operation
        assert "security" not in operation


def test_route_ignores_body_content_type_and_needs_no_auth_container() -> None:
    service, _repository, _feature, _verifier, _exporter = _service(record=_record("old_intro"))
    with _client(service) as client:
        response = client.post(
            f"/api/p115/mediainfo/sha1/{SHA_A}/intro-sync?emby_item_id=1001",
            content=b"not-json",
            headers={"Content-Type": "text/plain"},
        )
    assert response.status_code == 200
    assert response.json() == {"ok": True, "updated": True, "chapter_count": 2}


def test_extraction_preserves_duplicate_and_reversed_legacy_marker_semantics() -> None:
    reversed_pair = {
        "Chapters": [
            {"StartPositionTicks": "10", "MarkerType": "IntroEnd"},
            {"StartPositionTicks": "20.9", "MarkerType": "IntroStart"},
            {"StartPositionTicks": "21", "MarkerType": "IntroStart", "Name": "重复片头"},
            {"StartPositionTicks": -1, "MarkerType": "IntroStart"},
            {"StartPositionTicks": "invalid", "MarkerType": "IntroEnd"},
        ]
    }
    chapters = extract_intro_chapters([reversed_pair])
    assert [(chapter["StartPositionTicks"], chapter["MarkerType"]) for chapter in chapters] == [
        (10, "IntroEnd"),
        (20, "IntroStart"),
        (21, "IntroStart"),
    ]
    assert chapters[0]["Name"] == "片头结束"
    assert chapters[1]["ChapterIndex"] == 1


def test_extraction_requires_pair_and_rejects_sa_snapshot() -> None:
    assert extract_intro_chapters({"Chapters": [INTRO_CHAPTERS[0]]}) == []
    snapshot = deepcopy(INTRO_CHAPTERS)
    snapshot[1]["Name"] = "片头结束 #SA"
    assert extract_intro_chapters({"Chapters": snapshot}) == []


def test_merge_is_non_mutating_and_preserves_normal_and_credits_chapters() -> None:
    original = _formatted(OLD_INTRO_CHAPTERS)
    before = deepcopy(original)
    merged = merge_intro_chapters(original, INTRO_CHAPTERS)

    assert original == before
    assert isinstance(merged, list)
    merged_chapters = merged[0]["Chapters"]
    assert [chapter.get("MarkerType") for chapter in merged_chapters] == [
        None,
        "CreditsStart",
        "IntroStart",
        "IntroEnd",
    ]
    assert extract_intro_chapters(merged) == INTRO_CHAPTERS


def test_cas_reloads_and_recomputes_without_overwriting_concurrent_cache_changes() -> None:
    service, repository, _feature, _verifier, exporter = _service(
        record=_record("old_intro"),
        cas_failures=2,
    )
    result = service.sync_by_sha1(SHA_A, "1001")

    assert result == {"ok": True, "updated": True, "chapter_count": 2}
    assert repository.find_calls == 3
    assert repository.cas_calls == 3
    assert repository.record is not None and repository.record.cache_revision == 7
    assert isinstance(repository.record.formatted, list)
    assert repository.record.formatted[0]["ConcurrentRevision"] == 6
    assert exporter.calls[0]["cache_revision"] == 7


def test_three_cas_conflicts_fail_closed_without_export() -> None:
    service, repository, _feature, _verifier, exporter = _service(
        record=_record("old_intro"),
        cas_failures=3,
    )
    with pytest.raises(P115IntroSyncConflict, match="changed during intro synchronization"):
        service.sync_by_sha1(SHA_A, "1001")
    assert repository.cas_calls == 3
    assert exporter.calls == []


def test_export_failure_does_not_change_successful_http_result() -> None:
    service, repository, _feature, _verifier, exporter = _service(
        record=_record("old_intro"),
        export_failure=True,
    )
    with _client(service) as client:
        response = client.post(f"/api/p115/mediainfo/sha1/{SHA_A}/intro-sync?emby_item_id=1001")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "updated": True, "chapter_count": 2}
    assert repository.record is not None and repository.record.cache_revision == 5
    assert exporter.calls[0]["reason"] == "emby_item_updated"
