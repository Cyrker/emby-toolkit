from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID, uuid4

import httpx
from pydantic import SecretStr

from etk.domains.media.editing import MediaCommandOutcome, MediaCommandStatus
from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.p115.media_info import (
    P115MediaInfoCacheRecord,
    P115MediaInfoRestoreTarget,
    P115MediaProbeItemStatus,
    P115MediaProbeReservation,
)
from etk.domains.p115.media_info_formatter import P115MediaInfoPolicySnapshot
from etk.domains.p115.media_info_restore import (
    EmbyMediaInfoInspection,
    HttpxEmbyMediaInfoQueryGateway,
    P115MediaInfoRestoreService,
)
from etk.domains.p115.media_probe import (
    P115MediaProbeResult,
    P115MediaProbeStatus,
)
from etk.platform import Settings
from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus

SHA_A = "A" * 40
SHA_B = "B" * 40
SHA_C = "C" * 40


class FakeCredentials:
    def __init__(self, account_ids: Sequence[str] = ("115001",)) -> None:
        self.account_ids = tuple(account_ids)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self.account_ids) - 1)
        self.calls += 1
        return P115CredentialSnapshot(account_id=self.account_ids[index])

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("restore must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("restore must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.effects: list[tuple[str, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        return None

    def record_started_effect(self, label: str) -> None:
        self.effects.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.effects.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.effects.append(("abandoned", label))


class FakeFingerprints:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("fingerprints ready", {"failed_assets": 0})
        self.calls = 0

    def execute(self, progress: Any) -> TaskOutcome:
        self.calls += 1
        progress.update(100, "fingerprints checked")
        return self.outcome


class FakeEmbyQuery:
    def __init__(self, inspection: EmbyMediaInfoInspection) -> None:
        self.inspection = inspection
        self.calls: list[tuple[str, ...]] = []

    def inspect(self, item_ids: Sequence[str]) -> EmbyMediaInfoInspection:
        self.calls.append(tuple(item_ids))
        return self.inspection


class FakeRepository:
    def __init__(
        self,
        targets: Sequence[P115MediaInfoRestoreTarget],
        caches: Mapping[str, P115MediaInfoCacheRecord | None] | None = None,
        reservations: Sequence[P115MediaProbeReservation] = (),
    ) -> None:
        self.targets = tuple(targets)
        self.caches = dict(caches or {})
        self.reservations = list(reservations)
        self.reserve_calls: list[dict[str, Any]] = []
        self.finished: list[tuple[P115MediaProbeReservation, dict[str, Any]]] = []

    def list_restore_targets(self, *, account_id: str) -> tuple[P115MediaInfoRestoreTarget, ...]:
        assert account_id == "115001"
        return self.targets

    def find_by_sha1(self, sha1: str, *, record_hit: bool = False) -> P115MediaInfoCacheRecord | None:
        assert record_hit is False
        return self.caches.get(sha1)

    def reserve_probe(self, **values: Any) -> P115MediaProbeReservation:
        self.reserve_calls.append(values)
        if self.reservations:
            return self.reservations.pop(0)
        cache = self.caches.get(values["target"].sha1)
        return P115MediaProbeReservation(
            id=str(uuid4()),
            status=P115MediaProbeItemStatus.PROBING,
            attempts=1,
            claim_token=str(uuid4()),
            cache_revision=cache.cache_revision if cache is not None else None,
        )

    def finish_probe(self, reservation: P115MediaProbeReservation, **values: Any) -> None:
        self.finished.append((reservation, values))


class FakeProbe:
    def __init__(self, result: P115MediaProbeResult) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def probe(self, request: Any, *, cancel: Any = None, policy: Any = None) -> P115MediaProbeResult:
        self.calls.append((request, policy))
        assert cancel is not None
        return self.result


class FakePolicies:
    def __init__(self) -> None:
        self.value = _policy()
        self.calls = 0

    def snapshot(self) -> P115MediaInfoPolicySnapshot:
        self.calls += 1
        return self.value


class FakeCommands:
    def __init__(self, outcome: MediaCommandOutcome | None = None) -> None:
        self.outcome = outcome or MediaCommandOutcome(MediaCommandStatus.CONFIRMED, str(uuid4()))
        self.cached_calls: list[dict[str, Any]] = []
        self.generated_calls: list[dict[str, Any]] = []
        self.execute_calls: list[UUID] = []

    def apply_mediainfo(self, context: Any, mediainfo: Any, **values: Any) -> MediaCommandOutcome:
        self.cached_calls.append({"context": context, "mediainfo": mediainfo, **values})
        return self.outcome

    def apply_generated_mediainfo(self, context: Any, mediainfo: Any, raw: Any, **values: Any) -> MediaCommandOutcome:
        self.generated_calls.append({"context": context, "mediainfo": mediainfo, "raw": raw, **values})
        return self.outcome

    def execute(self, effect_id: UUID) -> MediaCommandOutcome:
        self.execute_calls.append(effect_id)
        return self.outcome


def _target(sha1: str, item_id: str) -> P115MediaInfoRestoreTarget:
    return P115MediaInfoRestoreTarget(
        tmdb_id="101",
        item_type="Movie",
        title="Fixture Movie",
        path=f"/library/{item_id}.strm",
        file_name=f"{item_id}.mkv",
        emby_item_id=item_id,
        sha1=sha1,
        pick_code=f"pc-{item_id}",
        file_size=4096,
        original_language="en",
    )


def _formatted(name: str = "Fixture.mkv") -> list[dict[str, Any]]:
    return [
        {
            "MediaSourceInfo": {
                "Name": name,
                "MediaStreams": [{"Type": "Video", "Width": 1920, "Height": 1080}],
            },
            "Chapters": [],
        }
    ]


def _raw(*, subtitle_language: str = "eng") -> dict[str, Any]:
    streams: list[dict[str, Any]] = [
        {
            "index": 0,
            "codec_type": "video",
            "codec_name": "hevc",
            "width": 1920,
            "height": 1080,
            "r_frame_rate": "24/1",
        }
    ]
    if subtitle_language:
        streams.append(
            {
                "index": 1,
                "codec_type": "subtitle",
                "codec_name": "subrip",
                "tags": {"language": subtitle_language},
            }
        )
    return {"streams": streams, "format": {"duration": "10.0"}}


def _cache(
    sha1: str,
    *,
    formatted: object | None,
    raw: Mapping[str, Any] | None,
    revision: int = 4,
) -> P115MediaInfoCacheRecord:
    return P115MediaInfoCacheRecord.create(
        sha1=sha1,
        formatted=formatted,
        raw=raw,
        cache_revision=revision,
        raw_schema_version=1,
        formatter_revision="fixture-v1",
        probe_revision="fixture-v1",
        hit_count=0,
    )


def _policy() -> P115MediaInfoPolicySnapshot:
    return P115MediaInfoPolicySnapshot.create(
        language_mapping=[{"label": "英语", "value": "eng", "aliases": ["en"]}],
        default_stream_config={},
        language_mapping_revision="fixture-language-v1",
        default_stream_config_revision="fixture-stream-v1",
    )


def _confirmed_probe() -> P115MediaProbeResult:
    return P115MediaProbeResult(
        P115MediaProbeStatus.CONFIRMED,
        "probe complete",
        formatted=_formatted(),
        raw=_raw(),
        provider_backend="openapi",
        probe_method="ffprobe",
        preid="D" * 40,
    )


def _service(
    repository: FakeRepository,
    *,
    inspection: EmbyMediaInfoInspection | None = None,
    fingerprint_outcome: TaskOutcome | None = None,
    probe_result: P115MediaProbeResult | None = None,
    commands: FakeCommands | None = None,
) -> tuple[P115MediaInfoRestoreService, FakeProbe, FakeCommands]:
    ids = tuple(target.emby_item_id for target in repository.targets)
    query = inspection or EmbyMediaInfoInspection(ids, frozenset(ids), frozenset(ids), frozenset())
    probe = FakeProbe(probe_result or _confirmed_probe())
    command_gateway = commands or FakeCommands()
    return (
        P115MediaInfoRestoreService(
            credentials=FakeCredentials(),
            repository=repository,
            fingerprints=FakeFingerprints(fingerprint_outcome),
            emby=FakeEmbyQuery(query),
            probe=probe,
            policies=FakePolicies(),
            commands=command_gateway,
        ),
        probe,
        command_gateway,
    )


def test_emby_query_keeps_unreturned_and_failed_items_out_of_missing_set() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["X-Emby-Token"] == "fixture-token"
        return httpx.Response(
            200,
            json={
                "Items": [
                    {"Id": "missing", "MediaStreams": [{"Type": "Video", "Width": 0, "Height": 0}]},
                    {
                        "Id": "complete",
                        "MediaSources": [{"MediaStreams": [{"Type": "Video", "Width": 3840}]}],
                    },
                ]
            },
        )

    def settings() -> Settings:
        return Settings(
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
        )

    gateway = HttpxEmbyMediaInfoQueryGateway(
        settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = gateway.inspect(("missing", "complete", "unreturned"))

    assert result.returned_ids == frozenset({"missing", "complete"})
    assert result.missing_video_ids == frozenset({"missing"})
    assert result.unreturned_ids == frozenset({"unreturned"})
    assert result.failed_ids == frozenset()
    assert requests[0].url.params["Ids"] == "missing,complete,unreturned"
    assert requests[0].url.params["Fields"] == "MediaSources,MediaStreams"


def test_incremental_restore_applies_only_returned_item_missing_dimensions() -> None:
    missing = _target(SHA_A, "missing")
    complete = _target(SHA_B, "complete")
    unreturned = _target(SHA_C, "unreturned")
    repository = FakeRepository(
        (missing, complete, unreturned),
        {SHA_A: _cache(SHA_A, formatted=_formatted(), raw=_raw())},
    )
    inspection = EmbyMediaInfoInspection(
        ("missing", "complete", "unreturned"),
        frozenset({"missing", "complete"}),
        frozenset({"missing"}),
        frozenset(),
    )
    service, probe, commands = _service(repository, inspection=inspection)

    outcome = service.execute(FakeProgress(), operation_key="fixture-job", force_full_update=False)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["selected"] == 1
    assert outcome.details["emby_unreturned"] == 1
    assert outcome.details["emby_already_complete"] == 1
    assert len(commands.cached_calls) == 1
    assert commands.cached_calls[0]["context"].item_id == "missing"
    assert commands.cached_calls[0]["expected_cache_revision"] == 4
    assert probe.calls == []


def test_full_restore_reformats_raw_before_online_probe_and_replaces_atomically() -> None:
    target = _target(SHA_A, "raw-item")
    repository = FakeRepository((target,), {SHA_A: _cache(SHA_A, formatted=_formatted("old"), raw=_raw())})
    service, probe, commands = _service(repository)

    outcome = service.execute(FakeProgress(), operation_key="fixture-full", force_full_update=True)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["raw_reformatted"] == 1
    assert outcome.details["online_probe_attempts"] == 0
    assert len(repository.reserve_calls) == 1
    assert len(repository.reserve_calls[0]["provenance_key"]) == 64
    assert len(commands.generated_calls) == 1
    assert commands.generated_calls[0]["provider_backend"] == "raw"
    assert commands.generated_calls[0]["probe_result"] == {"probe_method": "raw_reformat"}
    assert probe.calls == []


def test_full_restore_rejects_stale_raw_when_reservation_captures_a_newer_cache_revision() -> None:
    target = _target(SHA_A, "raw-race-item")
    cache = _cache(SHA_A, formatted=_formatted("old"), raw=_raw(), revision=4)
    repository = FakeRepository(
        (target,),
        {SHA_A: cache},
        reservations=(
            P115MediaProbeReservation(
                id=str(uuid4()),
                status=P115MediaProbeItemStatus.PROBING,
                attempts=1,
                claim_token=str(uuid4()),
                cache_revision=5,
            ),
        ),
    )
    service, probe, commands = _service(repository)

    outcome = service.execute(FakeProgress(), operation_key="fixture-raw-race", force_full_update=True)

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["failed"] == 1
    assert outcome.details["retryable_probes"] == 1
    assert outcome.details["raw_reformatted"] == 0
    assert commands.generated_calls == []
    assert probe.calls == []
    assert repository.finished[0][1]["status"] is P115MediaProbeItemStatus.RETRYABLE
    assert repository.finished[0][1]["error"] == {"code": "cache_revision_changed_before_restore_decision"}


def test_full_restore_rejects_cache_creation_after_empty_read_and_reclaim_uses_latest_raw() -> None:
    target = _target(SHA_A, "empty-race-item")
    first_reservation = P115MediaProbeReservation(
        id=str(uuid4()),
        status=P115MediaProbeItemStatus.PROBING,
        attempts=1,
        claim_token=str(uuid4()),
        cache_revision=5,
    )
    reclaimed_reservation = P115MediaProbeReservation(
        id=first_reservation.id,
        status=P115MediaProbeItemStatus.PROBING,
        attempts=2,
        claim_token=str(uuid4()),
        cache_revision=5,
    )
    repository = FakeRepository(
        (target,),
        {SHA_A: None},
        reservations=(first_reservation, reclaimed_reservation),
    )
    service, probe, commands = _service(repository)

    first = service.execute(FakeProgress(), operation_key="fixture-empty-race", force_full_update=True)

    assert first.status is TaskOutcomeStatus.FAILED
    assert first.details["failed"] == 1
    assert first.details["retryable_probes"] == 1
    assert probe.calls == []
    assert commands.cached_calls == []
    assert commands.generated_calls == []
    assert repository.finished[0][0] == first_reservation
    assert repository.finished[0][1]["error"] == {"code": "cache_revision_changed_before_restore_decision"}

    repository.caches[SHA_A] = _cache(SHA_A, formatted=None, raw=_raw(), revision=5)
    reclaimed = service.execute(FakeProgress(), operation_key="fixture-empty-race", force_full_update=True)

    assert reclaimed.status is TaskOutcomeStatus.SUCCEEDED
    assert reclaimed.details["raw_reformatted"] == 1
    assert reclaimed.details["online_probe_attempts"] == 0
    assert probe.calls == []
    assert len(commands.generated_calls) == 1
    assert commands.generated_calls[0]["probe_item_id"] == reclaimed_reservation.id
    assert commands.generated_calls[0]["probe_claim_token"] == reclaimed_reservation.claim_token


def test_incremental_restore_rejects_invalid_cache_decision_when_revision_drifts_before_reserve() -> None:
    target = _target(SHA_A, "invalid-race-item")
    cache = _cache(
        SHA_A,
        formatted={"invalid": "formatted-cache"},
        raw=_raw(subtitle_language="und"),
        revision=4,
    )
    repository = FakeRepository(
        (target,),
        {SHA_A: cache},
        reservations=(
            P115MediaProbeReservation(
                id=str(uuid4()),
                status=P115MediaProbeItemStatus.PROBING,
                attempts=2,
                claim_token=str(uuid4()),
                cache_revision=5,
            ),
        ),
    )
    service, probe, commands = _service(repository)

    outcome = service.execute(FakeProgress(), operation_key="fixture-invalid-race", force_full_update=False)

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["invalid_formatted_cache"] == 1
    assert outcome.details["stale_raw_subtitle"] == 1
    assert outcome.details["retryable_probes"] == 1
    assert probe.calls == []
    assert commands.cached_calls == []
    assert commands.generated_calls == []
    assert repository.finished[0][1]["status"] is P115MediaProbeItemStatus.RETRYABLE
    assert repository.finished[0][1]["error"] == {"code": "cache_revision_changed_before_restore_decision"}


def test_full_restore_probe_failure_does_not_clear_existing_formatted_cache() -> None:
    target = _target(SHA_A, "failed-item")
    old_formatted = _formatted("old-cache")
    stale_raw = _raw(subtitle_language="und")
    repository = FakeRepository((target,), {SHA_A: _cache(SHA_A, formatted=old_formatted, raw=stale_raw)})
    service, probe, commands = _service(
        repository,
        probe_result=P115MediaProbeResult(
            P115MediaProbeStatus.RETRYABLE,
            "fixture timeout",
            provider_backend="openapi",
            probe_method="ffprobe",
        ),
    )

    outcome = service.execute(FakeProgress(), operation_key="fixture-failed", force_full_update=True)

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["stale_raw_subtitle"] == 1
    assert outcome.details["retryable_probes"] == 1
    assert len(probe.calls) == 1
    assert commands.cached_calls == []
    assert commands.generated_calls == []
    persisted = repository.caches[SHA_A]
    assert persisted is not None
    assert persisted.formatted == old_formatted
    assert repository.finished[0][1]["status"] is P115MediaProbeItemStatus.RETRYABLE


def test_online_probe_success_stages_generated_cache_and_effect() -> None:
    target = _target(SHA_A, "probe-item")
    repository = FakeRepository((target,), {SHA_A: None})
    service, probe, commands = _service(repository)

    outcome = service.execute(FakeProgress(), operation_key="fixture-probe", force_full_update=True)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["online_probe_attempts"] == 1
    assert outcome.details["online_probed"] == 1
    assert len(probe.calls) == 1
    assert probe.calls[0][0].pick_code == "pc-probe-item"
    assert len(commands.generated_calls) == 1
    call = commands.generated_calls[0]
    assert call["provider_backend"] == "openapi"
    assert call["probe_result"]["preid"] == "D" * 40


def test_confirmed_worklist_replay_resumes_effect_without_reprobing() -> None:
    target = _target(SHA_A, "resume-item")
    effect_id = uuid4()
    repository = FakeRepository(
        (target,),
        {SHA_A: None},
        reservations=(
            P115MediaProbeReservation(
                id=str(uuid4()),
                status=P115MediaProbeItemStatus.CONFIRMED,
                attempts=1,
                result={"effect_id": str(effect_id), "cache_revision": 1},
            ),
        ),
    )
    service, probe, commands = _service(repository)

    outcome = service.execute(FakeProgress(), operation_key="fixture-resume", force_full_update=True)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["resumed_effects"] == 1
    assert commands.execute_calls == [effect_id]
    assert probe.calls == []
    assert commands.generated_calls == []


def test_fingerprint_partial_is_reported_even_when_resolvable_target_succeeds() -> None:
    target = _target(SHA_A, "partial-item")
    repository = FakeRepository((target,), {SHA_A: _cache(SHA_A, formatted=_formatted(), raw=_raw())})
    service, _probe, _commands = _service(
        repository,
        fingerprint_outcome=TaskOutcome.partial("one unresolved", {"failed_assets": 1}),
    )

    outcome = service.execute(FakeProgress(), operation_key="fixture-partial", force_full_update=False)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["restored"] == 1
    assert outcome.details["unresolved_after_fingerprint_repair"] == 1
