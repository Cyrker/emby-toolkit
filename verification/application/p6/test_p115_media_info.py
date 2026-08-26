from __future__ import annotations

from typing import Any

import pytest

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.p115.media_info import (
    P115MediaInfoCacheRecord,
    P115MediaInfoCacheService,
    P115MediaInfoInvalidCache,
    P115MediaInfoInvalidIdentity,
    P115MediaInfoNotFound,
)

SHA_A = "A" * 40


class FakeCredentials:
    def __init__(self, account_id: str = "115001") -> None:
        self.account_id = account_id

    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(account_id=self.account_id)

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("cache query must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("cache query must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeRepository:
    def __init__(self, record: P115MediaInfoCacheRecord | None) -> None:
        self.record = record
        self.lookups: list[tuple[str, bool]] = []
        self.pick_codes: list[tuple[str, str]] = []

    def find_by_sha1(self, sha1: str, *, record_hit: bool = False) -> P115MediaInfoCacheRecord | None:
        self.lookups.append((sha1, record_hit))
        return self.record if self.record is not None and self.record.sha1 == sha1 else None

    def resolve_sha1_by_pick_code(self, pick_code: str, *, account_id: str) -> str | None:
        self.pick_codes.append((pick_code, account_id))
        return self.record.sha1 if self.record is not None and pick_code == "pc-a" else None


def _formatted() -> list[dict[str, Any]]:
    return [
        {
            "Chapters": [],
            "MediaSourceInfo": {
                "Size": 123,
                "Type": "Default",
                "Bitrate": 0,
                "Formats": [],
                "Chapters": [],
                "IsRemote": True,
                "Protocol": "File",
                "Container": "mkv",
                "MediaStreams": [],
                "RunTimeTicks": 0,
            },
        }
    ]


def _record(*, formatted: object | None = None, raw: object | None = None) -> P115MediaInfoCacheRecord:
    return P115MediaInfoCacheRecord.create(
        sha1=SHA_A,
        formatted=_formatted() if formatted is None else formatted,
        raw={"_etk": {"tmdb_id": "101", "type": "movie", "sha1": SHA_A}} if raw is None else raw,
        cache_revision=3,
        raw_schema_version=1,
        formatter_revision="p6-v1",
        probe_revision="ffprobe-v1",
        hit_count=2,
    )


def test_cache_service_returns_canonical_bridge_shape_and_records_hit() -> None:
    repository = FakeRepository(_record())
    service = P115MediaInfoCacheService(repository, FakeCredentials())

    payload = service.formatted_by_sha1(SHA_A.lower())

    assert payload == _formatted()[0]
    assert repository.lookups == [(SHA_A, True)]


def test_cache_service_resolves_pick_code_only_with_current_account() -> None:
    repository = FakeRepository(_record())
    service = P115MediaInfoCacheService(repository, FakeCredentials("115009"))

    payload = service.formatted_by_pick_code("pc-a")

    assert payload["MediaSourceInfo"]["Container"] == "mkv"
    assert repository.pick_codes == [("pc-a", "115009")]


@pytest.mark.parametrize("value", ["", "A" * 39, "G" * 40])
def test_cache_service_rejects_invalid_sha1(value: str) -> None:
    service = P115MediaInfoCacheService(FakeRepository(_record()), FakeCredentials())

    with pytest.raises(P115MediaInfoInvalidIdentity):
        service.formatted_by_sha1(value)


def test_cache_service_distinguishes_missing_and_invalid_formatted_cache() -> None:
    with pytest.raises(P115MediaInfoNotFound):
        P115MediaInfoCacheService(FakeRepository(None), FakeCredentials()).formatted_by_sha1(SHA_A)

    invalid = _record(formatted={"unexpected": True})
    with pytest.raises(P115MediaInfoInvalidCache):
        P115MediaInfoCacheService(FakeRepository(invalid), FakeCredentials()).formatted_by_sha1(SHA_A)


def test_cache_service_extracts_stable_metadata_identity_without_writing() -> None:
    service = P115MediaInfoCacheService(FakeRepository(_record()), FakeCredentials())

    identity = service.metadata_identity_by_sha1(SHA_A)

    assert identity.tmdb_id == "101"
    assert identity.media_type == "movie"
    assert identity.sha1 == SHA_A
    assert identity.season_number is None
    assert identity.episode_number is None


def test_cache_service_rejects_raw_identity_without_supported_media_type() -> None:
    record = _record(raw={"_etk": {"tmdb_id": "101", "type": "music", "sha1": SHA_A}})

    with pytest.raises(P115MediaInfoNotFound):
        P115MediaInfoCacheService(FakeRepository(record), FakeCredentials()).metadata_identity_by_sha1(SHA_A)


def test_cache_service_pick_code_miss_does_not_fall_back_to_cross_account_rows() -> None:
    repository = FakeRepository(_record())
    service = P115MediaInfoCacheService(repository, FakeCredentials(""))

    with pytest.raises(P115MediaInfoNotFound):
        service.formatted_by_pick_code("pc-a")

    assert repository.pick_codes == []
