from __future__ import annotations

import struct
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from etk.domains.p115 import intro_detection_adapters
from etk.domains.p115.intro_detection import (
    CREDITS_SAMPLE_STEPS,
    INTRO_DETECTION_ALGORITHM_VERSION,
    INTRO_SAMPLE_STEPS,
    MAX_WORK_EPISODES_PER_BATCH,
    CancelCheck,
    FingerprintRef,
    IntroBatchDisposition,
    IntroBatchResult,
    IntroDetectionCancelled,
    IntroDetectionExecutionService,
    IntroDetectionIntent,
    IntroDetectionMatchTimeout,
    IntroDetectionOperation,
    IntroDetectionOperationStatus,
    IntroDetectionPhase,
    IntroDetectionPolicySnapshot,
    IntroEpisodeSnapshot,
    IntroJobTrigger,
    IntroTriggerMode,
    MatchControl,
    detect_common_intro,
    normalize_credits_start,
    normalize_intro_range,
    parse_raw_chromaprint,
)
from etk.domains.p115.intro_detection_adapters import (
    NativeIntroDetectionBatchEngine,
    _rotating_batch,
)


def _episode(number: int, *, runtime_seconds: float = 1500) -> IntroEpisodeSnapshot:
    return IntroEpisodeSnapshot.create(
        item_id=f"item-{number}",
        series_tmdb_id="series-42",
        season_number=1,
        episode_number=number,
        sha1=f"{number:040X}",
        pick_code=f"pick-{number}",
        source_library_id="library-a",
        runtime_seconds=runtime_seconds,
        cache_revision=number,
    )


def _intent(*, mode: IntroTriggerMode = IntroTriggerMode.IMPORT) -> IntroDetectionIntent:
    return IntroDetectionIntent.create(
        account_id="115001",
        trigger=IntroJobTrigger.IMPORT,
        policy=IntroDetectionPolicySnapshot.create(
            trigger_mode=mode,
            credits_enabled=True,
            library_ids=("library-a",),
        ),
        episodes=(_episode(2), _episode(1)),
        available_at=datetime.now(UTC),
    )


def _operation(*, episode_count: int = 4) -> IntroDetectionOperation:
    intent = IntroDetectionIntent.create(
        account_id="115001",
        trigger=IntroJobTrigger.IMPORT,
        policy=IntroDetectionPolicySnapshot.create(
            trigger_mode=IntroTriggerMode.IMPORT,
            credits_enabled=True,
        ),
        episodes=tuple(_episode(index) for index in range(1, episode_count + 1)),
        available_at=datetime.now(UTC),
    )
    now = datetime.now(UTC)
    return IntroDetectionOperation(
        id=uuid4(),
        sequence=1,
        operation_key=intent.operation_key,
        request_hash=intent.request_hash,
        source_set_revision=intent.source_set_revision,
        policy_revision=intent.policy_revision,
        series_tmdb_id=intent.series_tmdb_id,
        season_number=intent.season_number,
        algorithm_version=intent.algorithm_version,
        account_id=intent.account_id,
        trigger_mode=intent.trigger_mode,
        request=intent.request,
        status=IntroDetectionOperationStatus.CLAIMED,
        phase=IntroDetectionPhase.RESTORE,
        state_revision=1,
        progress={},
        attempts=1,
        available_at=now,
        claimed_by="fixture-worker",
        claim_token=uuid4(),
        claimed_at=now,
        lease_expires_at=now + timedelta(minutes=20),
        cancel_requested_at=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


def test_intent_is_deterministic_and_contains_only_immutable_snapshot_data() -> None:
    first = _intent()
    second = _intent()

    assert first.operation_key == second.operation_key
    assert first.request_hash == second.request_hash
    assert first.source_set_revision == second.source_set_revision
    assert first.policy_revision == second.policy_revision
    assert first.algorithm_version == INTRO_DETECTION_ALGORITHM_VERSION
    assert first.provider_backend == "openapi"
    assert first.request["provider_backend"] == "openapi"
    assert [episode["episode_number"] for episode in first.request["episodes"]] == [1, 2]
    serialized = repr(first.request).lower()
    assert "http://" not in serialized
    assert "https://" not in serialized
    assert "direct_url" not in serialized

    cookie = IntroDetectionIntent.create(
        account_id="115001",
        provider_backend="cookie",
        trigger=IntroJobTrigger.IMPORT,
        policy=IntroDetectionPolicySnapshot.create(
            trigger_mode=IntroTriggerMode.IMPORT,
            credits_enabled=True,
            library_ids=("library-a",),
        ),
        episodes=(_episode(2), _episode(1)),
    )
    assert cookie.operation_key != first.operation_key
    assert cookie.request["provider_backend"] == "cookie"


def test_trigger_policy_preserves_legacy_mode_contract() -> None:
    imported = IntroDetectionPolicySnapshot.create(
        trigger_mode=IntroTriggerMode.IMPORT,
        credits_enabled=False,
    )
    favorite = IntroDetectionPolicySnapshot.create(
        trigger_mode=IntroTriggerMode.FAVORITE,
        credits_enabled=False,
    )
    playback = IntroDetectionPolicySnapshot.create(
        trigger_mode=IntroTriggerMode.PLAYBACK,
        credits_enabled=False,
    )

    assert imported.accepts(IntroJobTrigger.IMPORT)
    assert imported.accepts(IntroJobTrigger.BACKFILL)
    assert not imported.accepts(IntroJobTrigger.FAVORITE)
    assert favorite.accepts(IntroJobTrigger.IMPORT)
    assert favorite.accepts(IntroJobTrigger.FAVORITE)
    assert favorite.accepts(IntroJobTrigger.BACKFILL)
    assert playback.accepts(IntroJobTrigger.PLAYBACK)
    assert not playback.accepts(IntroJobTrigger.BACKFILL)


def test_frozen_windows_and_marker_normalization_match_v6() -> None:
    assert INTRO_SAMPLE_STEPS == (180, 300, 600)
    assert CREDITS_SAMPLE_STEPS == (180, 300)
    assert MAX_WORK_EPISODES_PER_BATCH == 3
    assert normalize_intro_range(2.9, 30) == (0, 300_000_000)
    assert normalize_intro_range(5, 20) is None
    assert normalize_intro_range(10, 400) == (100_000_000, 2_500_000_000)
    assert normalize_credits_start(1000, 980) == 9_800_000_000
    assert normalize_credits_start(1000, 995) is None
    assert normalize_credits_start(1000, 600) is None


def test_raw_chromaprint_requires_legacy_minimum_and_parses_little_endian_int32() -> None:
    values = tuple(range(-20, 20))
    raw = struct.pack(f"<{len(values)}i", *values)

    assert parse_raw_chromaprint(raw) == values
    assert parse_raw_chromaprint(raw[:76]) == ()
    assert parse_raw_chromaprint(raw + b"x") == values


def test_common_intro_uses_longest_base_and_returns_episode_ranges() -> None:
    first = _episode(1)
    second = _episode(2)
    base_values = tuple(index * 7919 for index in range(1600))
    short_values = base_values[:1500]
    match = detect_common_intro(
        (
            FingerprintRef(first, base_values, 180),
            FingerprintRef(second, short_values, 180),
        )
    )

    assert match is not None
    assert match.base.episode == first
    assert set(match.ranges_by_sha1) == {first.sha1, second.sha1}
    assert all(end > start >= 0 for start, end in match.ranges_by_sha1.values())


def test_match_control_checks_cooperative_cancellation() -> None:
    control = MatchControl(60, cancel=lambda: True)

    with pytest.raises(IntroDetectionCancelled):
        control.checkpoint()


class _AllowPolicy:
    def permits(self, operation: IntroDetectionOperation) -> bool:
        assert operation.id
        return True


class _NoProgressBatch:
    def execute_batch(
        self,
        operation: IntroDetectionOperation,
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult:
        assert operation.id
        assert not cancel()
        return IntroBatchResult(
            IntroBatchDisposition.CONTINUE,
            IntroDetectionPhase.INTRO_EXTRACT,
            False,
        )


def test_execution_service_turns_no_progress_into_bounded_backoff() -> None:
    service = IntroDetectionExecutionService(_NoProgressBatch(), _AllowPolicy())

    result = service.execute(_operation(), cancel=lambda: False)

    assert result.disposition is IntroBatchDisposition.RETRYABLE
    assert result.retry_after_seconds == 3
    assert result.progress["no_progress_batches"] == 1


def test_execution_service_rejects_unpinned_legacy_operation_before_policy_or_provider_work() -> None:
    class _ForbiddenPolicy:
        def permits(self, operation: IntroDetectionOperation) -> bool:
            del operation
            raise AssertionError("legacy operation must fail before policy evaluation")

    class _ForbiddenBatch:
        def execute_batch(
            self,
            operation: IntroDetectionOperation,
            *,
            cancel: CancelCheck,
        ) -> IntroBatchResult:
            del operation, cancel
            raise AssertionError("legacy operation must fail before provider work")

    operation = _operation()
    legacy_request = {key: value for key, value in operation.request.items() if key != "provider_backend"}

    result = IntroDetectionExecutionService(_ForbiddenBatch(), _ForbiddenPolicy()).execute(
        replace(operation, request=legacy_request),
        cancel=lambda: False,
    )

    assert result.disposition is IntroBatchDisposition.REJECTED
    assert result.error == {"type": "provider_backend_unpinned"}


def test_no_progress_rotates_later_episodes_and_rejects_after_two_full_sweeps() -> None:
    operation = _operation(episode_count=5)
    pending = operation.episodes

    assert [episode.episode_number for episode in _rotating_batch(pending, 0)] == [1, 2, 3]
    assert [episode.episode_number for episode in _rotating_batch(pending, 1)] == [4, 5, 1]

    service = IntroDetectionExecutionService(_NoProgressBatch(), _AllowPolicy())
    exhausted = service.execute(
        replace(operation, progress={"no_progress_batches": 3}),
        cancel=lambda: False,
    )

    assert exhausted.disposition is IntroBatchDisposition.REJECTED
    assert exhausted.progress["no_progress_batches"] == 4
    assert exhausted.error == {
        "type": "no_progress_exhausted",
        "batches": 4,
        "last_reason": "",
    }


class _RestoreRepository:
    def __init__(self) -> None:
        self.staged: tuple[Any, ...] = ()

    def chapter_markers(self, _episode: IntroEpisodeSnapshot) -> tuple[int, int, None]:
        return 10, 20, None

    def stage_chapters(self, *_args: Any, projections: Any = None, **_kwargs: Any) -> tuple[UUID, ...]:
        if projections is None and len(_args) >= 3:
            projections = _args[2]
        assert projections is not None
        self.staged = tuple(projections)
        return tuple(uuid4() for _ in self.staged)

    def pending_episodes(self, *_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        return ()

    def has_detection_failure(self, *_args: Any, **_kwargs: Any) -> bool:
        return False


class _Authorization:
    def authorization_revision(self) -> str:
        return "a" * 64


class _FingerprintLifetime:
    def __init__(self) -> None:
        self.clear_calls = 0

    def clear(self) -> None:
        self.clear_calls += 1


def test_intro_common_match_timeout_uses_bounded_no_progress_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    operation = _operation(episode_count=3)
    repository = _RestoreRepository()
    engine = NativeIntroDetectionBatchEngine(
        cast(Any, repository),
        cast(Any, _FingerprintLifetime()),
        _Authorization(),
    )
    fingerprints = tuple(FingerprintRef(episode, tuple(range(40)), 180) for episode in operation.episodes)
    monkeypatch.setattr(
        engine,
        "_fingerprint_set",
        lambda *_args, **_kwargs: fingerprints,
    )

    def _timeout(*_args: Any, **_kwargs: Any) -> None:
        raise IntroDetectionMatchTimeout("fixture common-match timeout")

    monkeypatch.setattr(intro_detection_adapters, "detect_common_intro", _timeout)
    raw_result = engine._execute_intro(
        operation,
        cast(UUID, operation.claim_token),
        operation.episodes,
        cancel=lambda: False,
    )

    assert raw_result.disposition is IntroBatchDisposition.CONTINUE
    assert raw_result.progressed is False
    assert raw_result.progress == {"last_no_progress_reason": "intro_common_match_timeout"}

    class _TimedOutBatch:
        def execute_batch(self, *_args: Any, **_kwargs: Any) -> IntroBatchResult:
            return raw_result

    exhausted = IntroDetectionExecutionService(_TimedOutBatch(), _AllowPolicy()).execute(
        replace(operation, progress={"no_progress_batches": 1}),
        cancel=lambda: False,
    )

    assert exhausted.disposition is IntroBatchDisposition.REJECTED
    assert exhausted.error == {
        "type": "no_progress_exhausted",
        "batches": 2,
        "last_reason": "intro_common_match_timeout",
    }


def test_native_batch_restore_stages_at_most_three_durable_chapter_effects() -> None:
    repository = _RestoreRepository()
    fingerprints = _FingerprintLifetime()
    engine = NativeIntroDetectionBatchEngine(
        cast(Any, repository),
        cast(Any, fingerprints),
        _Authorization(),
    )

    result = engine.execute_batch(_operation(episode_count=5), cancel=lambda: False)

    assert result.disposition is IntroBatchDisposition.CONTINUE
    assert result.progressed is True
    assert len(repository.staged) == 3
    assert fingerprints.clear_calls == 1


def test_native_batch_clears_operation_local_url_cache_on_cancellation() -> None:
    fingerprints = _FingerprintLifetime()
    engine = NativeIntroDetectionBatchEngine(
        cast(Any, _RestoreRepository()),
        cast(Any, fingerprints),
        _Authorization(),
    )

    with pytest.raises(IntroDetectionCancelled):
        engine.execute_batch(_operation(), cancel=lambda: True)

    assert fingerprints.clear_calls == 1


class _ExplodingRestoreRepository(_RestoreRepository):
    def chapter_markers(self, _episode: IntroEpisodeSnapshot) -> tuple[int, int, None]:
        raise RuntimeError("fixture repository failure")


def test_native_batch_clears_operation_local_url_cache_on_exception() -> None:
    fingerprints = _FingerprintLifetime()
    engine = NativeIntroDetectionBatchEngine(
        cast(Any, _ExplodingRestoreRepository()),
        cast(Any, fingerprints),
        _Authorization(),
    )

    with pytest.raises(RuntimeError, match="fixture repository failure"):
        engine.execute_batch(_operation(), cancel=lambda: False)

    assert fingerprints.clear_calls == 1
