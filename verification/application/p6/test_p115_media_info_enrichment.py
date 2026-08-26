from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.p115.media_info import (
    P115MediaInfoCacheRecord,
    P115MediaProbeItemStatus,
    P115MediaProbeReservation,
)
from etk.domains.p115.media_info_enrichment import P115StrmMediaInfoEnrichmentService
from etk.domains.p115.media_info_formatter import P115MediaInfoPolicySnapshot
from etk.domains.p115.media_probe import (
    P115MediaProbeRequest,
    P115MediaProbeResult,
    P115MediaProbeStatus,
    media_info_formatter_revision,
)

SHA_A = "A" * 40


class FakeCredentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(account_id="115001")

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("enrichment must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("enrichment must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakePolicies:
    def __init__(
        self,
        *,
        language_revision: str = "fixture-language-v1",
        stream_revision: str = "fixture-stream-v1",
        error: Exception | None = None,
    ) -> None:
        self.language_revision = language_revision
        self.stream_revision = stream_revision
        self.error = error
        self.calls = 0

    def snapshot(self) -> P115MediaInfoPolicySnapshot:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return P115MediaInfoPolicySnapshot.create(
            language_mapping=({"label": "English", "value": "eng", "aliases": ["en"]},),
            default_stream_config={
                "audio_lang_priority": ["eng"],
                "subtitle_lang_priority": ["eng"],
            },
            language_mapping_revision=self.language_revision,
            default_stream_config_revision=self.stream_revision,
        )


class FakeProgress:
    def checkpoint(self) -> None:
        return None

    def control_state(self) -> dict[str, bool]:
        return {"cancel_requested": False, "timed_out": False}


class FakeRepository:
    def __init__(
        self,
        cache: P115MediaInfoCacheRecord | None = None,
        reservation: P115MediaProbeReservation | None = None,
    ) -> None:
        self.cache = cache
        self.reservation = reservation
        self.reserves: list[dict[str, Any]] = []
        self.confirmed: list[dict[str, Any]] = []
        self.finished: list[dict[str, Any]] = []

    def find_by_sha1(self, sha1: str, *, record_hit: bool = False) -> P115MediaInfoCacheRecord | None:
        assert sha1 == SHA_A
        assert record_hit is False
        return self.cache

    def reserve_probe(self, **values: Any) -> P115MediaProbeReservation:
        self.reserves.append(values)
        if self.reservation is not None:
            return self.reservation
        return P115MediaProbeReservation(
            id=str(uuid4()),
            status=P115MediaProbeItemStatus.PROBING,
            attempts=1,
            claim_token=str(uuid4()),
            cache_revision=self.cache.cache_revision if self.cache is not None else None,
        )

    def confirm_probe(self, reservation: P115MediaProbeReservation, **values: Any) -> int:
        assert reservation.claimed
        self.confirmed.append(values)
        return (reservation.cache_revision or 0) + int(values.get("formatted") is not None)

    def finish_probe(self, reservation: P115MediaProbeReservation, **values: Any) -> None:
        assert reservation.claimed
        self.finished.append(values)


class FakeProbe:
    def __init__(self, result: P115MediaProbeResult) -> None:
        self.result = result
        self.calls: list[tuple[P115MediaProbeRequest, P115MediaInfoPolicySnapshot | None]] = []

    def probe(
        self,
        request: P115MediaProbeRequest,
        *,
        cancel: Any = None,
        policy: P115MediaInfoPolicySnapshot | None = None,
    ) -> P115MediaProbeResult:
        assert cancel is None or cancel() is False
        self.calls.append((request, policy))
        return self.result


def _request() -> P115MediaProbeRequest:
    return P115MediaProbeRequest(
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-a",
        file_name="Fixture.mkv",
        file_size=1024,
    )


def _raw() -> dict[str, Any]:
    return {
        "format": {"format_name": "matroska", "duration": "60", "size": "1024"},
        "streams": [
            {
                "index": 0,
                "codec_type": "video",
                "codec_name": "hevc",
                "width": 1920,
                "height": 1080,
                "disposition": {"default": 1},
                "tags": {},
            }
        ],
        "chapters": [],
        "_etk": {"sha1": SHA_A},
    }


def _formatted() -> list[dict[str, Any]]:
    return [{"MediaSourceInfo": {"MediaStreams": [], "Container": "mkv"}, "Chapters": []}]


def _cache(
    *,
    formatted: object | None,
    raw: Mapping[str, Any] | None,
    formatter_revision: str | None = None,
) -> P115MediaInfoCacheRecord:
    return P115MediaInfoCacheRecord.create(
        sha1=SHA_A,
        formatted=formatted,
        raw=raw,
        cache_revision=4,
        raw_schema_version=1,
        formatter_revision=formatter_revision or media_info_formatter_revision(FakePolicies().snapshot()),
        probe_revision="fixture-probe-v1",
        hit_count=0,
    )


def _probe_result(status: P115MediaProbeStatus = P115MediaProbeStatus.CONFIRMED) -> P115MediaProbeResult:
    return P115MediaProbeResult(
        status,
        "fixture probe result",
        formatted=_formatted() if status is P115MediaProbeStatus.CONFIRMED else None,
        raw=_raw() if status is P115MediaProbeStatus.CONFIRMED else None,
        provider_backend="openapi",
        probe_method="ffprobe",
    )


def _service(
    repository: FakeRepository,
    probe: FakeProbe,
    *,
    policies: FakePolicies | None = None,
) -> P115StrmMediaInfoEnrichmentService:
    return P115StrmMediaInfoEnrichmentService(
        credentials=FakeCredentials(),
        repository=repository,
        probe=probe,
        policies=policies or FakePolicies(),
    )


def test_formatted_cache_reuse_is_persistently_confirmed_without_probe() -> None:
    repository = FakeRepository(_cache(formatted=_formatted(), raw=None))
    probe = FakeProbe(_probe_result())

    result = _service(repository, probe).enrich(_request(), operation_key="strm-op", progress=FakeProgress())

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.cache_reused is True
    assert probe.calls == []
    assert repository.confirmed[0]["provider_backend"] == "cache"
    assert "formatted" not in repository.confirmed[0]
    assert repository.finished == []


def test_raw_cache_is_reformatted_and_atomically_confirmed_without_online_probe() -> None:
    repository = FakeRepository(_cache(formatted=None, raw=_raw()))
    probe = FakeProbe(_probe_result())

    result = _service(repository, probe).enrich(_request(), operation_key="strm-op", progress=FakeProgress())

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.provider_backend == "raw"
    assert probe.calls == []
    confirmed = repository.confirmed[0]
    assert confirmed["provider_backend"] == "raw"
    assert confirmed["formatted"][0]["MediaSourceInfo"]["MediaStreams"][0]["Width"] == 1920
    assert confirmed["raw"]["_etk"]["sha1"] == SHA_A


def test_formatted_cache_from_an_old_policy_is_reformatted_from_raw() -> None:
    repository = FakeRepository(
        _cache(
            formatted=_formatted(),
            raw=_raw(),
            formatter_revision="fixture-old-v1",
        )
    )
    probe = FakeProbe(_probe_result())

    result = _service(repository, probe).enrich(_request(), operation_key="strm-op", progress=FakeProgress())

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.provider_backend == "raw"
    assert probe.calls == []
    assert repository.confirmed[0]["provider_backend"] == "raw"
    assert "formatted" in repository.confirmed[0]


def test_online_probe_success_persists_formatted_raw_and_terminal_worklist() -> None:
    repository = FakeRepository()
    probe = FakeProbe(_probe_result())

    result = _service(repository, probe).enrich(_request(), operation_key="strm-op", progress=FakeProgress())

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert len(probe.calls) == 1
    confirmed = repository.confirmed[0]
    assert confirmed["provider_backend"] == "openapi"
    assert confirmed["formatted"] == _formatted()
    assert confirmed["raw"] == _raw()
    assert confirmed["probe_revision"] == "p6-ffprobe-v1"


def test_online_probe_failure_is_persistent_and_does_not_claim_success() -> None:
    repository = FakeRepository()
    probe = FakeProbe(_probe_result(P115MediaProbeStatus.RETRYABLE))

    result = _service(repository, probe).enrich(_request(), operation_key="strm-op", progress=FakeProgress())

    assert result.status is P115MediaProbeStatus.RETRYABLE
    assert repository.confirmed == []
    assert repository.finished[0]["status"] is P115MediaProbeItemStatus.RETRYABLE
    assert repository.finished[0]["error"]["message"] == "fixture probe result"


def test_terminal_worklist_replay_does_not_repeat_probe_or_persistence() -> None:
    reservation = P115MediaProbeReservation(
        id=str(uuid4()),
        status=P115MediaProbeItemStatus.CONFIRMED,
        attempts=1,
        cache_revision=3,
        result={
            "cache_reused": True,
            "diagnostic": "fixture replay",
            "provider_backend": "cache",
        },
    )
    repository = FakeRepository(reservation=reservation)
    probe = FakeProbe(_probe_result())

    result = _service(repository, probe).enrich(_request(), operation_key="strm-op", progress=FakeProgress())

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.cache_reused is True
    assert result.diagnostic == "fixture replay"
    assert probe.calls == []
    assert repository.confirmed == []
    assert repository.finished == []


def test_probe_work_generation_is_bound_to_policy_provenance() -> None:
    first_policies = FakePolicies(language_revision="fixture-language-v1")
    second_policies = FakePolicies(language_revision="fixture-language-v2")
    terminal = P115MediaProbeReservation(
        id=str(uuid4()),
        status=P115MediaProbeItemStatus.REJECTED,
        attempts=1,
    )
    first_repository = FakeRepository(reservation=terminal)
    second_repository = FakeRepository(reservation=terminal)

    _service(first_repository, FakeProbe(_probe_result()), policies=first_policies).enrich(
        _request(), operation_key="strm-op", progress=FakeProgress()
    )
    _service(second_repository, FakeProbe(_probe_result()), policies=second_policies).enrich(
        _request(), operation_key="strm-op", progress=FakeProgress()
    )

    first_key = first_repository.reserves[0]["provenance_key"]
    second_key = second_repository.reserves[0]["provenance_key"]
    assert first_policies.calls == 1
    assert second_policies.calls == 1
    assert len(first_key) == 64 and first_key.isalnum()
    assert len(second_key) == 64 and second_key.isalnum()
    assert first_key != second_key


def test_invalid_policy_uses_a_separate_terminal_work_generation() -> None:
    invalid_policies = FakePolicies(error=ValueError("invalid fixture policy"))
    invalid_repository = FakeRepository()
    valid_repository = FakeRepository(
        reservation=P115MediaProbeReservation(
            id=str(uuid4()),
            status=P115MediaProbeItemStatus.REJECTED,
            attempts=1,
        )
    )

    invalid_result = _service(
        invalid_repository,
        FakeProbe(_probe_result()),
        policies=invalid_policies,
    ).enrich(_request(), operation_key="strm-op", progress=FakeProgress())
    _service(valid_repository, FakeProbe(_probe_result())).enrich(
        _request(), operation_key="strm-op", progress=FakeProgress()
    )

    assert invalid_result.status is P115MediaProbeStatus.REJECTED
    assert invalid_repository.finished[0]["status"] is P115MediaProbeItemStatus.REJECTED
    assert invalid_repository.reserves[0]["provenance_key"] != valid_repository.reserves[0]["provenance_key"]
