from __future__ import annotations

import hashlib
import json
import struct
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from statistics import median
from typing import Any, Protocol
from uuid import UUID, uuid5

INTRO_DETECTION_ALGORITHM_VERSION = 6
INTRO_DETECTION_AGGREGATE_TYPE = "p115_intro_detection_operation"
INTRO_DETECTION_TOPIC = "p115.intro-detection.requested"
INTRO_DETECTION_TOPICS = (INTRO_DETECTION_TOPIC,)
INTRO_DETECTION_NAMESPACE = UUID("bf8d3022-c21c-4d96-90f2-2207051f3cdd")

INTRO_SAMPLE_STEPS = (180, 300, 600)
CREDITS_SAMPLE_STEPS = (180, 300)
MIN_COMPARABLE_EPISODES = 2
MAX_WORK_EPISODES_PER_BATCH = 3
BATCH_CONTINUATION_DELAY_SECONDS = 3
MIN_INTRO_SECONDS = 18
MAX_INTRO_SECONDS = 240
MAX_INTRO_START_SECONDS = 360
MIN_CREDITS_SECONDS = 18
MAX_CREDITS_SECONDS = 300
CREDITS_BOUNDARY_MARGIN_SECONDS = 8
COMMON_MATCH_TIMEOUT_SECONDS = 60
EPISODE_MATCH_TIMEOUT_SECONDS = 20
INTRO_TICKS = 10_000_000
FFMPEG_TIMEOUT_SECONDS = 900
FINGERPRINT_HAMMING_DISTANCE = 6
FINGERPRINT_MAX_CONSECUTIVE_MISSES = 40
FINGERPRINT_MIN_MATCH_RATIO = 0.85
FINGERPRINT_ANCHOR_WIDTH = 3
FINGERPRINT_CACHE_TOLERANCE_SECONDS = 5
FINGERPRINT_MIN_VALUES = 40
CPU_YIELD_INTERVAL = 32
CPU_YIELD_SECONDS = 0.001
NO_PROGRESS_BACKOFF_CAP_SECONDS = 300
NO_PROGRESS_SWEEP_LIMIT = 2
FINGERPRINT_ANCHOR_MASKS = (
    0x0F0F0F0F,
    0xF0F0F0F0,
    0x33333333,
    0xCCCCCCCC,
)

CancelCheck = Callable[[], bool]


class IntroTriggerMode(StrEnum):
    OFF = "off"
    IMPORT = "import"
    FAVORITE = "favorite"
    PLAYBACK = "playback"


class IntroJobTrigger(StrEnum):
    IMPORT = "import"
    FAVORITE = "favorite"
    PLAYBACK = "playback"
    BACKFILL = "backfill"


class IntroFingerprintKind(StrEnum):
    INTRO = "intro"
    CREDITS = "credits"


class IntroDetectionOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"

    @property
    def terminal(self) -> bool:
        return self in {
            IntroDetectionOperationStatus.CONFIRMED,
            IntroDetectionOperationStatus.REJECTED,
            IntroDetectionOperationStatus.CANCELLED,
            IntroDetectionOperationStatus.SUPERSEDED,
        }


class IntroDetectionPhase(StrEnum):
    RESTORE = "restore"
    INTRO_EXTRACT = "intro_extract"
    INTRO_MATCH = "intro_match"
    CREDITS_EXTRACT = "credits_extract"
    CREDITS_MATCH = "credits_match"
    PROJECT = "project"


class IntroBatchDisposition(StrEnum):
    CONTINUE = "continue"
    CONFIRMED = "confirmed"
    RETRYABLE = "retryable"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class IntroDetectionError(RuntimeError):
    pass


class IntroDetectionConflictError(IntroDetectionError):
    pass


class IntroDetectionStateError(IntroDetectionError):
    pass


class IntroDetectionClaimLostError(IntroDetectionStateError):
    pass


class IntroDetectionNotFoundError(IntroDetectionError):
    pass


class IntroDetectionCancelled(IntroDetectionError):
    pass


class IntroDetectionMatchTimeout(IntroDetectionError):
    pass


@dataclass(frozen=True, slots=True)
class IntroEpisodeSnapshot:
    item_id: str
    series_tmdb_id: str
    season_number: int
    episode_number: int
    sha1: str
    pick_code: str
    source_library_id: str = ""
    runtime_seconds: float = 0
    cache_revision: int = 0
    title: str = ""
    series_title: str = ""

    @classmethod
    def create(
        cls,
        *,
        item_id: object,
        series_tmdb_id: object,
        season_number: object,
        episode_number: object,
        sha1: object,
        pick_code: object,
        source_library_id: object = "",
        runtime_seconds: object = 0,
        cache_revision: object = 0,
        title: object = "",
        series_title: object = "",
    ) -> IntroEpisodeSnapshot:
        normalized_sha1 = _canonical_sha1(sha1)
        season = _nonnegative_int(season_number, "season_number")
        episode = _nonnegative_int(episode_number, "episode_number")
        revision = _nonnegative_int(cache_revision, "cache_revision")
        runtime = float(str(runtime_seconds or 0))
        if runtime < 0:
            raise ValueError("runtime_seconds must not be negative")
        return cls(
            item_id=_required_text(item_id, "item_id", maximum=255),
            series_tmdb_id=_required_text(series_tmdb_id, "series_tmdb_id", maximum=255),
            season_number=season,
            episode_number=episode,
            sha1=normalized_sha1,
            pick_code=_required_text(pick_code, "pick_code", maximum=255),
            source_library_id=_optional_text(source_library_id, maximum=255),
            runtime_seconds=runtime,
            cache_revision=revision,
            title=_optional_text(title, maximum=1024),
            series_title=_optional_text(series_title, maximum=1024),
        )

    def to_request(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "series_tmdb_id": self.series_tmdb_id,
            "season_number": self.season_number,
            "episode_number": self.episode_number,
            "sha1": self.sha1,
            "pick_code": self.pick_code,
            "source_library_id": self.source_library_id,
            "runtime_seconds": self.runtime_seconds,
            "cache_revision": self.cache_revision,
            "title": self.title,
            "series_title": self.series_title,
        }


@dataclass(frozen=True, slots=True)
class IntroDetectionPolicySnapshot:
    trigger_mode: IntroTriggerMode
    credits_enabled: bool
    library_ids: tuple[str, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        trigger_mode: IntroTriggerMode | str,
        credits_enabled: bool,
        library_ids: Sequence[object] = (),
    ) -> IntroDetectionPolicySnapshot:
        mode = IntroTriggerMode(str(trigger_mode))
        if not isinstance(credits_enabled, bool):
            raise TypeError("credits_enabled must be boolean")
        normalized_libraries = tuple(sorted({_optional_text(value, maximum=255) for value in library_ids} - {""}))
        return cls(mode, credits_enabled, normalized_libraries)

    def accepts(self, trigger: IntroJobTrigger | str) -> bool:
        normalized = IntroJobTrigger(str(trigger))
        if self.trigger_mode is IntroTriggerMode.OFF:
            return False
        if self.trigger_mode is IntroTriggerMode.IMPORT:
            return normalized in {IntroJobTrigger.IMPORT, IntroJobTrigger.BACKFILL}
        if self.trigger_mode is IntroTriggerMode.FAVORITE:
            return normalized in {
                IntroJobTrigger.IMPORT,
                IntroJobTrigger.FAVORITE,
                IntroJobTrigger.BACKFILL,
            }
        return normalized is IntroJobTrigger.PLAYBACK

    def permits_library(self, library_id: str) -> bool:
        return not self.library_ids or str(library_id or "").strip() in self.library_ids

    def to_request(self) -> dict[str, Any]:
        return {
            "trigger_mode": self.trigger_mode.value,
            "credits_enabled": self.credits_enabled,
            "library_ids": list(self.library_ids),
        }


@dataclass(frozen=True, slots=True)
class IntroDetectionIntent:
    operation_key: str
    request_hash: bytes
    source_set_revision: bytes
    policy_revision: bytes
    series_tmdb_id: str
    season_number: int
    algorithm_version: int
    account_id: str
    provider_backend: str
    trigger_mode: IntroTriggerMode
    request: Mapping[str, Any]
    available_at: datetime

    @classmethod
    def create(
        cls,
        *,
        account_id: object,
        trigger: IntroJobTrigger | str,
        policy: IntroDetectionPolicySnapshot,
        episodes: Sequence[IntroEpisodeSnapshot],
        available_at: datetime | None = None,
        algorithm_version: int = INTRO_DETECTION_ALGORITHM_VERSION,
        provider_backend: str = "openapi",
    ) -> IntroDetectionIntent:
        if len(episodes) < MIN_COMPARABLE_EPISODES:
            raise ValueError("intro detection requires at least two comparable episodes")
        ordered = tuple(
            sorted(
                episodes,
                key=lambda episode: (episode.episode_number, episode.sha1, episode.item_id),
            )
        )
        series_ids = {episode.series_tmdb_id for episode in ordered}
        seasons = {episode.season_number for episode in ordered}
        if len(series_ids) != 1 or len(seasons) != 1:
            raise ValueError("intro detection episodes must belong to one season")
        if len({episode.sha1 for episode in ordered}) != len(ordered):
            raise ValueError("intro detection episode SHA1 values must be unique")
        normalized_trigger = IntroJobTrigger(str(trigger))
        if not policy.accepts(normalized_trigger):
            raise ValueError("intro detection policy does not accept this trigger")
        filtered = tuple(episode for episode in ordered if policy.permits_library(episode.source_library_id))
        if len(filtered) < MIN_COMPARABLE_EPISODES:
            raise ValueError("intro detection library policy leaves fewer than two episodes")
        version = _positive_int(algorithm_version, "algorithm_version")
        normalized_account = _required_text(account_id, "account_id", maximum=255)
        if not normalized_account.isdigit():
            raise ValueError("account_id must be numeric")
        normalized_backend = _provider_backend(provider_backend)
        episode_payload = [episode.to_request() for episode in filtered]
        source_revision = _sha256_bytes(episode_payload)
        policy_payload = policy.to_request()
        policy_revision = _sha256_bytes(policy_payload)
        request = {
            "account_id": normalized_account,
            "provider_backend": normalized_backend,
            "trigger": normalized_trigger.value,
            "algorithm_version": version,
            "series_tmdb_id": filtered[0].series_tmdb_id,
            "season_number": filtered[0].season_number,
            "source_set_revision": source_revision.hex(),
            "policy_revision": policy_revision.hex(),
            "policy": policy_payload,
            "episodes": episode_payload,
        }
        request_hash = _sha256_bytes(request)
        semantic = {
            "account_id": normalized_account,
            "provider_backend": normalized_backend,
            "algorithm_version": version,
            "series_tmdb_id": filtered[0].series_tmdb_id,
            "season_number": filtered[0].season_number,
            "source_set_revision": source_revision.hex(),
            "policy_revision": policy_revision.hex(),
        }
        return cls(
            operation_key=_sha256_bytes(semantic).hex(),
            request_hash=request_hash,
            source_set_revision=source_revision,
            policy_revision=policy_revision,
            series_tmdb_id=filtered[0].series_tmdb_id,
            season_number=filtered[0].season_number,
            algorithm_version=version,
            account_id=normalized_account,
            provider_backend=normalized_backend,
            trigger_mode=policy.trigger_mode,
            request=request,
            available_at=_aware(available_at or datetime.now(UTC), "available_at"),
        )


@dataclass(frozen=True, slots=True)
class IntroDetectionOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    source_set_revision: bytes
    policy_revision: bytes
    series_tmdb_id: str
    season_number: int
    algorithm_version: int
    account_id: str
    trigger_mode: IntroTriggerMode
    request: Mapping[str, Any]
    status: IntroDetectionOperationStatus
    phase: IntroDetectionPhase
    state_revision: int
    progress: Mapping[str, Any]
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    cancel_requested_at: datetime | None
    result: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def episodes(self) -> tuple[IntroEpisodeSnapshot, ...]:
        raw = self.request.get("episodes")
        if not isinstance(raw, list):
            raise IntroDetectionStateError("intro detection request has no episode snapshot")
        return tuple(_episode_from_request(value) for value in raw)

    @property
    def provider_backend(self) -> str:
        return _provider_backend(self.request.get("provider_backend"))


@dataclass(frozen=True, slots=True)
class IntroDetectionReservation:
    operation: IntroDetectionOperation
    created: bool


@dataclass(frozen=True, slots=True)
class IntroDetectionTransition:
    operation: IntroDetectionOperation
    changed: bool


@dataclass(frozen=True, slots=True)
class FingerprintRef:
    episode: IntroEpisodeSnapshot
    values: tuple[int, ...]
    sample_seconds: int
    window_start_seconds: float = 0
    kind: IntroFingerprintKind = IntroFingerprintKind.INTRO

    @property
    def seconds_per_value(self) -> float:
        return float(self.sample_seconds) / max(1, len(self.values))


@dataclass(frozen=True, slots=True)
class CommonIntroMatch:
    base: FingerprintRef
    base_start_seconds: float
    base_end_seconds: float
    ranges_by_sha1: Mapping[str, tuple[int, int]]
    matched_count: int


@dataclass(frozen=True, slots=True)
class CommonCreditsMatch:
    base: FingerprintRef
    base_start_seconds: float
    base_end_seconds: float
    starts_by_sha1: Mapping[str, int]
    matched_count: int


@dataclass(frozen=True, slots=True)
class IntroChapterProjection:
    episode: IntroEpisodeSnapshot
    intro_start_ticks: int | None = None
    intro_end_ticks: int | None = None
    credits_start_ticks: int | None = None

    def __post_init__(self) -> None:
        if (self.intro_start_ticks is None) != (self.intro_end_ticks is None):
            raise ValueError("intro chapter projection requires both start and end")
        if self.intro_start_ticks is not None:
            if (
                self.intro_start_ticks < 0
                or self.intro_end_ticks is None
                or self.intro_end_ticks <= self.intro_start_ticks
            ):
                raise ValueError("intro chapter projection range is invalid")
        if self.credits_start_ticks is not None and self.credits_start_ticks <= 0:
            raise ValueError("credits chapter projection start is invalid")
        if self.intro_start_ticks is None and self.credits_start_ticks is None:
            raise ValueError("chapter projection must contain intro or credits")


@dataclass(frozen=True, slots=True)
class IntroBatchResult:
    disposition: IntroBatchDisposition
    phase: IntroDetectionPhase
    progressed: bool
    progress: Mapping[str, Any] = field(default_factory=dict)
    result: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None
    retry_after_seconds: float = 0


class IntroDetectionOperationRepositoryPort(Protocol):
    def reserve(self, intent: IntroDetectionIntent) -> IntroDetectionReservation: ...

    def read(self, operation_id: UUID) -> IntroDetectionOperation | None: ...


class IntroDetectionPolicyPort(Protocol):
    """P7-owned favorite/watchlist checks remain behind this narrow read-only port."""

    def permits(self, operation: IntroDetectionOperation) -> bool: ...


class IntroDetectionBatchPort(Protocol):
    def execute_batch(
        self,
        operation: IntroDetectionOperation,
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult: ...


class IntroDetectionSubmissionService:
    def __init__(self, repository: IntroDetectionOperationRepositoryPort) -> None:
        self._repository = repository

    def submit(self, intent: IntroDetectionIntent) -> IntroDetectionReservation:
        return self._repository.reserve(intent)


class IntroDetectionExecutionService:
    def __init__(self, batch: IntroDetectionBatchPort, policy: IntroDetectionPolicyPort) -> None:
        self._batch = batch
        self._policy = policy

    def execute(self, operation: IntroDetectionOperation, *, cancel: CancelCheck) -> IntroBatchResult:
        if cancel() or operation.cancel_requested_at is not None:
            return IntroBatchResult(
                IntroBatchDisposition.CANCELLED,
                operation.phase,
                False,
                error={"type": "cancel_requested"},
            )
        try:
            _ = operation.provider_backend
        except ValueError:
            return IntroBatchResult(
                IntroBatchDisposition.REJECTED,
                operation.phase,
                False,
                error={"type": "provider_backend_unpinned"},
            )
        if not self._policy.permits(operation):
            return IntroBatchResult(
                IntroBatchDisposition.CONFIRMED,
                operation.phase,
                False,
                result={"skipped": True, "reason": "policy_not_permitted"},
            )
        result = self._batch.execute_batch(operation, cancel=cancel)
        if result.disposition is not IntroBatchDisposition.CONTINUE or result.progressed:
            return result
        no_progress = int(operation.progress.get("no_progress_batches") or 0) + 1
        progress = {**dict(result.progress), "no_progress_batches": no_progress}
        batch_count = max(1, (len(operation.episodes) + MAX_WORK_EPISODES_PER_BATCH - 1) // MAX_WORK_EPISODES_PER_BATCH)
        no_progress_budget = batch_count * NO_PROGRESS_SWEEP_LIMIT
        if no_progress >= no_progress_budget:
            return IntroBatchResult(
                IntroBatchDisposition.REJECTED,
                result.phase,
                False,
                progress=progress,
                error={
                    "type": "no_progress_exhausted",
                    "batches": no_progress,
                    "last_reason": str(result.progress.get("last_no_progress_reason") or ""),
                },
            )
        delay = min(
            BATCH_CONTINUATION_DELAY_SECONDS * (2 ** min(no_progress - 1, 7)),
            NO_PROGRESS_BACKOFF_CAP_SECONDS,
        )
        return IntroBatchResult(
            IntroBatchDisposition.RETRYABLE,
            result.phase,
            False,
            progress=progress,
            error={"type": "no_progress", "batches": no_progress},
            retry_after_seconds=delay,
        )


@dataclass(slots=True)
class MatchControl:
    timeout_seconds: float
    cancel: CancelCheck = lambda: False
    clock: Callable[[], float] = time.monotonic
    sleeper: Callable[[float], None] = time.sleep
    _deadline: float = field(init=False)

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("match timeout must be positive")
        self._deadline = self.clock() + self.timeout_seconds

    def checkpoint(self, *, yield_cpu: bool = False) -> None:
        if self.cancel():
            raise IntroDetectionCancelled("intro fingerprint matching was cancelled")
        if yield_cpu:
            self.sleeper(CPU_YIELD_SECONDS)
        if self.clock() > self._deadline:
            raise IntroDetectionMatchTimeout("intro fingerprint matching exceeded its budget")


def parse_raw_chromaprint(output: bytes) -> tuple[int, ...]:
    if not output or len(output) < 80:
        return ()
    usable = len(output) - (len(output) % 4)
    if usable <= 0:
        return ()
    try:
        return tuple(struct.unpack(f"<{usable // 4}i", output[:usable]))
    except struct.error:
        return ()


def normalize_intro_range(start_seconds: float, end_seconds: float) -> tuple[int, int] | None:
    start = float(start_seconds)
    end = float(end_seconds)
    if start < 3:
        start = 0
    if end - start < MIN_INTRO_SECONDS:
        return None
    if end - start > MAX_INTRO_SECONDS:
        end = start + MAX_INTRO_SECONDS
    return int(round(start * INTRO_TICKS)), int(round(end * INTRO_TICKS))


def normalize_credits_start(runtime_seconds: float, start_seconds: float) -> int | None:
    runtime = float(runtime_seconds)
    start = float(start_seconds)
    if start <= 0:
        return None
    if runtime > 0:
        tail = runtime - start
        if tail < MIN_CREDITS_SECONDS or tail > MAX_CREDITS_SECONDS + CREDITS_BOUNDARY_MARGIN_SECONDS:
            return None
    return int(round(start * INTRO_TICKS))


def credits_tail_duration_ticks(runtime_seconds: float, start_ticks: int) -> float | None:
    runtime = float(runtime_seconds)
    if runtime <= 0:
        return None
    tail = runtime * INTRO_TICKS - int(start_ticks)
    minimum = MIN_CREDITS_SECONDS * INTRO_TICKS
    maximum = (MAX_CREDITS_SECONDS + CREDITS_BOUNDARY_MARGIN_SECONDS) * INTRO_TICKS
    return float(tail) if minimum <= tail <= maximum else None


def infer_credits_start_from_tail(runtime_seconds: float, tail_ticks: float) -> int | None:
    runtime = float(runtime_seconds)
    if runtime <= 0:
        return None
    return normalize_credits_start(runtime, runtime - float(tail_ticks) / INTRO_TICKS)


def detect_common_intro(
    fingerprints: Sequence[FingerprintRef],
    *,
    max_start_seconds: float = MAX_INTRO_START_SECONDS,
    control: MatchControl | None = None,
) -> CommonIntroMatch | None:
    if not fingerprints:
        return None
    active = control or MatchControl(COMMON_MATCH_TIMEOUT_SECONDS)
    base = max(fingerprints, key=lambda value: len(value.values))
    segments: list[tuple[FingerprintRef, float, float, float, float]] = []
    for index, fingerprint in enumerate(fingerprints):
        active.checkpoint(yield_cpu=bool(index and index % CPU_YIELD_INTERVAL == 0))
        if fingerprint is base:
            continue
        segment = find_best_common_segment(
            base,
            fingerprint,
            max_start_seconds=max_start_seconds,
            control=active,
        )
        if segment is not None:
            segments.append((fingerprint, *segment))
    if not segments:
        return None
    base_start = float(median(segment[1] for segment in segments))
    base_end = float(median(segment[2] for segment in segments))
    consensus = [
        segment for segment in segments if abs(segment[1] - base_start) <= 20 and abs(segment[2] - base_end) <= 30
    ]
    if not consensus:
        return None
    ranges: dict[str, list[tuple[float, float]]] = {base.episode.sha1: []}
    for fingerprint, own_base_start, own_base_end, own_start, own_end in consensus:
        ranges[base.episode.sha1].append((own_base_start, own_base_end))
        ranges.setdefault(fingerprint.episode.sha1, []).append((own_start, own_end))
    normalized: dict[str, tuple[int, int]] = {}
    for sha1, values in ranges.items():
        own_range = normalize_intro_range(
            float(median(value[0] for value in values)),
            float(median(value[1] for value in values)),
        )
        if own_range is not None:
            normalized[sha1] = own_range
    base_range = normalize_intro_range(base_start, base_end)
    if base_range is None or not normalized:
        return None
    return CommonIntroMatch(
        base,
        base_range[0] / INTRO_TICKS,
        base_range[1] / INTRO_TICKS,
        normalized,
        len(consensus),
    )


def detect_common_credits(
    fingerprints: Sequence[FingerprintRef],
    *,
    max_start_seconds: float,
    control: MatchControl | None = None,
) -> CommonCreditsMatch | None:
    if not fingerprints:
        return None
    active = control or MatchControl(COMMON_MATCH_TIMEOUT_SECONDS)
    base = max(fingerprints, key=lambda value: len(value.values))
    segments: list[tuple[FingerprintRef, float, float, float, float]] = []
    for index, fingerprint in enumerate(fingerprints):
        active.checkpoint(yield_cpu=bool(index and index % CPU_YIELD_INTERVAL == 0))
        if fingerprint is base:
            continue
        segment = find_best_common_segment(
            base,
            fingerprint,
            max_start_seconds=max_start_seconds,
            control=active,
        )
        if segment is None:
            continue
        base_start, base_end, own_start, own_end = segment
        duration = min(base_end - base_start, own_end - own_start)
        if MIN_CREDITS_SECONDS <= duration <= MAX_CREDITS_SECONDS:
            segments.append((fingerprint, base_start, base_end, own_start, own_end))
    if not segments:
        return None
    base_start = float(median(segment[1] for segment in segments))
    base_end = float(median(segment[2] for segment in segments))
    consensus = [
        segment for segment in segments if abs(segment[1] - base_start) <= 30 and abs(segment[2] - base_end) <= 45
    ]
    if not consensus:
        return None
    starts: dict[str, list[float]] = {base.episode.sha1: []}
    for fingerprint, own_base_start, _base_end, own_start, _own_end in consensus:
        starts[base.episode.sha1].append(base.window_start_seconds + own_base_start)
        starts.setdefault(fingerprint.episode.sha1, []).append(fingerprint.window_start_seconds + own_start)
    runtime_by_sha1 = {fingerprint.episode.sha1: fingerprint.episode.runtime_seconds for fingerprint in fingerprints}
    normalized = {
        sha1: start
        for sha1, values in starts.items()
        if (
            start := normalize_credits_start(
                runtime_by_sha1.get(sha1, 0),
                float(median(values)),
            )
        )
        is not None
    }
    if not normalized:
        return None
    return CommonCreditsMatch(base, base_start, base_end, normalized, len(consensus))


def credits_match_touches_window_start(
    match: CommonCreditsMatch,
    fingerprints: Sequence[FingerprintRef],
) -> bool:
    matched = touching = 0
    for fingerprint in fingerprints:
        start = match.starts_by_sha1.get(fingerprint.episode.sha1)
        if start is None:
            continue
        matched += 1
        if start / INTRO_TICKS - fingerprint.window_start_seconds <= CREDITS_BOUNDARY_MARGIN_SECONDS:
            touching += 1
    return matched > 0 and touching * 2 >= matched


def match_intro_against_template(
    base: FingerprintRef,
    other: FingerprintRef,
    base_start_seconds: float,
    base_end_seconds: float,
    *,
    max_start_seconds: float = MAX_INTRO_START_SECONDS,
    control: MatchControl | None = None,
) -> tuple[int, int] | None:
    matched = find_template_matched_range(
        base,
        other,
        base_start_seconds,
        base_end_seconds,
        max_start_seconds=max_start_seconds,
        min_seconds=MIN_INTRO_SECONDS,
        control=control,
    )
    return normalize_intro_range(*matched) if matched is not None else None


def match_credits_against_template(
    base: FingerprintRef,
    other: FingerprintRef,
    base_start_seconds: float,
    base_end_seconds: float,
    *,
    max_start_seconds: float,
    control: MatchControl | None = None,
) -> int | None:
    matched = find_template_matched_range(
        base,
        other,
        base_start_seconds,
        base_end_seconds,
        max_start_seconds=max_start_seconds,
        min_seconds=MIN_CREDITS_SECONDS,
        control=control,
    )
    if matched is None:
        return None
    own_start, own_end = matched
    duration = min(base_end_seconds - base_start_seconds, own_end - own_start)
    if duration < MIN_CREDITS_SECONDS or duration > MAX_CREDITS_SECONDS:
        return None
    return normalize_credits_start(
        other.episode.runtime_seconds,
        other.window_start_seconds + own_start,
    )


def find_template_matched_range(
    base: FingerprintRef,
    other: FingerprintRef,
    base_start_seconds: float,
    base_end_seconds: float,
    *,
    max_start_seconds: float,
    min_seconds: float,
    control: MatchControl | None = None,
) -> tuple[float, float] | None:
    active = control or MatchControl(EPISODE_MATCH_TIMEOUT_SECONDS)
    active.checkpoint()
    a, b = base.values, other.values
    if not a or not b or base.seconds_per_value <= 0 or other.seconds_per_value <= 0:
        return None
    min_len = max(8, int(float(min_seconds) / max(base.seconds_per_value, other.seconds_per_value)))
    template_start = max(0, int(round(float(base_start_seconds) / base.seconds_per_value)))
    template_end = min(len(a), int(round(float(base_end_seconds) / base.seconds_per_value)))
    template_len = template_end - template_start
    if template_len < min_len:
        return None
    width = min(FINGERPRINT_ANCHOR_WIDTH, min_len)
    max_other_start = min(max(len(b) - width, 0), int(float(max_start_seconds) / other.seconds_per_value))
    index: dict[tuple[int, tuple[int, ...]], list[int]] = {}
    for offset in range(max_other_start + 1):
        active.checkpoint(yield_cpu=bool(offset and offset % CPU_YIELD_INTERVAL == 0))
        if offset + width > len(b):
            break
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = mask_index, tuple(value & mask for value in b[offset : offset + width])
            index.setdefault(key, []).append(offset)
    min_overlap = max(min_len, int(template_len * 0.75))
    tolerance = max(2, int(8 / base.seconds_per_value))
    best: tuple[int, int, int] | None = None
    best_score = (0, 0, 0)
    for base_offset in range(template_start, max(template_start, template_end - width) + 1):
        active.checkpoint(yield_cpu=bool(base_offset and base_offset % CPU_YIELD_INTERVAL == 0))
        if base_offset + width > len(a):
            break
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = mask_index, tuple(value & mask for value in a[base_offset : base_offset + width])
            for other_offset in index.get(key, ())[:8]:
                right_len, right_matches = _extend_fuzzy_match(
                    a,
                    b,
                    base_offset,
                    other_offset,
                    1,
                    active,
                )
                left_len, left_matches = _extend_fuzzy_match(
                    a,
                    b,
                    base_offset - 1,
                    other_offset - 1,
                    -1,
                    active,
                )
                total = left_len + right_len
                matches = left_matches + right_matches
                if total < min_len or matches / total < FINGERPRINT_MIN_MATCH_RATIO:
                    continue
                base_match_start = base_offset - left_len
                other_match_start = other_offset - left_len
                base_match_end = base_match_start + total
                overlap = min(base_match_end, template_end) - max(base_match_start, template_start)
                if (
                    overlap < min_overlap
                    or base_match_start > template_start + tolerance
                    or base_match_end < template_end - tolerance
                ):
                    continue
                score = overlap, matches, total
                if score > best_score:
                    best_score = score
                    best = base_match_start, other_match_start, base_match_end
    if best is None:
        return None
    base_match_start, other_match_start, _ = best
    own_start = other_match_start + template_start - base_match_start
    own_end = other_match_start + template_end - base_match_start
    if own_start < 0 or own_end <= own_start or own_end > len(b):
        return None
    return own_start * other.seconds_per_value, own_end * other.seconds_per_value


def find_best_common_segment(
    base: FingerprintRef,
    other: FingerprintRef,
    *,
    max_start_seconds: float = MAX_INTRO_START_SECONDS,
    control: MatchControl | None = None,
) -> tuple[float, float, float, float] | None:
    active = control or MatchControl(COMMON_MATCH_TIMEOUT_SECONDS)
    active.checkpoint()
    a, b = base.values, other.values
    if not a or not b:
        return None
    seconds_per_value = max(base.seconds_per_value, other.seconds_per_value)
    min_len = max(8, int(MIN_INTRO_SECONDS / seconds_per_value))
    max_start = max(min(len(a), len(b)) - min_len, 0)
    max_start_index = min(max_start, int(max_start_seconds / seconds_per_value))
    if max_start_index <= 0:
        return None
    width = min(FINGERPRINT_ANCHOR_WIDTH, min_len)
    index: dict[tuple[int, tuple[int, ...]], list[int]] = {}
    upper_b = min(len(b) - width + 1, max_start_index + 1)
    for other_offset in range(max(0, upper_b)):
        active.checkpoint(yield_cpu=bool(other_offset and other_offset % CPU_YIELD_INTERVAL == 0))
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = mask_index, tuple(value & mask for value in b[other_offset : other_offset + width])
            index.setdefault(key, []).append(other_offset)
    best_base = best_other = best_length = 0
    upper_a = min(len(a) - width + 1, max_start_index + 1)
    for base_offset in range(max(0, upper_a)):
        active.checkpoint(yield_cpu=bool(base_offset and base_offset % CPU_YIELD_INTERVAL == 0))
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = mask_index, tuple(value & mask for value in a[base_offset : base_offset + width])
            for other_offset in index.get(key, ())[:8]:
                right_len, right_matches = _extend_fuzzy_match(
                    a,
                    b,
                    base_offset,
                    other_offset,
                    1,
                    active,
                )
                left_len, left_matches = _extend_fuzzy_match(
                    a,
                    b,
                    base_offset - 1,
                    other_offset - 1,
                    -1,
                    active,
                )
                total = left_len + right_len
                matches = left_matches + right_matches
                if total < min_len or matches / total < FINGERPRINT_MIN_MATCH_RATIO:
                    continue
                if total > best_length:
                    best_base = base_offset - left_len
                    best_other = other_offset - left_len
                    best_length = total
    if best_length < min_len:
        return None
    start = best_base * base.seconds_per_value
    if start > max_start_seconds:
        return None
    return (
        start,
        (best_base + best_length) * base.seconds_per_value,
        best_other * other.seconds_per_value,
        (best_other + best_length) * other.seconds_per_value,
    )


def operation_id(operation_key: str) -> UUID:
    normalized = _digest(operation_key, "operation_key")
    return uuid5(INTRO_DETECTION_NAMESPACE, normalized)


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _extend_fuzzy_match(
    left: Sequence[int],
    right: Sequence[int],
    left_index: int,
    right_index: int,
    direction: int,
    control: MatchControl,
) -> tuple[int, int]:
    total = matches = misses = 0
    last_good_total = last_good_matches = 0
    while 0 <= left_index < len(left) and 0 <= right_index < len(right):
        total += 1
        distance = ((int(left[left_index]) ^ int(right[right_index])) & 0xFFFFFFFF).bit_count()
        if distance <= FINGERPRINT_HAMMING_DISTANCE:
            matches += 1
            misses = 0
            last_good_total = total
            last_good_matches = matches
        else:
            misses += 1
            if misses > FINGERPRINT_MAX_CONSECUTIVE_MISSES:
                break
        if total % (CPU_YIELD_INTERVAL * 16) == 0:
            control.checkpoint(yield_cpu=True)
        left_index += direction
        right_index += direction
    return last_good_total, last_good_matches


def _episode_from_request(value: object) -> IntroEpisodeSnapshot:
    if not isinstance(value, Mapping):
        raise IntroDetectionStateError("intro detection episode snapshot is invalid")
    try:
        return IntroEpisodeSnapshot.create(**dict(value))
    except (TypeError, ValueError) as exc:
        raise IntroDetectionStateError("intro detection episode snapshot is invalid") from exc


def _sha256_bytes(value: object) -> bytes:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).digest()


def _canonical_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 40:
        raise ValueError("sha1 must be 40 hexadecimal characters")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("sha1 must be 40 hexadecimal characters") from exc
    return normalized


def _provider_backend(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in {"openapi", "cookie"}:
        raise ValueError("provider_backend must be openapi or cookie")
    return normalized


def _digest(value: object, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64:
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a SHA-256 digest") from exc
    return normalized


def _required_text(value: object, field_name: str, *, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field_name} must be 1-{maximum} characters")
    return normalized


def _optional_text(value: object, *, maximum: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > maximum:
        raise ValueError(f"text value must not exceed {maximum} characters")
    return normalized


def _nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a non-negative integer")
    parsed = int(str(value))
    if parsed < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return parsed


def _positive_int(value: object, field_name: str) -> int:
    parsed = _nonnegative_int(value, field_name)
    if parsed < 1:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


__all__ = [
    "BATCH_CONTINUATION_DELAY_SECONDS",
    "COMMON_MATCH_TIMEOUT_SECONDS",
    "CREDITS_BOUNDARY_MARGIN_SECONDS",
    "CREDITS_SAMPLE_STEPS",
    "CommonCreditsMatch",
    "CommonIntroMatch",
    "EPISODE_MATCH_TIMEOUT_SECONDS",
    "FFMPEG_TIMEOUT_SECONDS",
    "FingerprintRef",
    "INTRO_DETECTION_AGGREGATE_TYPE",
    "INTRO_DETECTION_ALGORITHM_VERSION",
    "INTRO_DETECTION_TOPIC",
    "INTRO_DETECTION_TOPICS",
    "INTRO_SAMPLE_STEPS",
    "INTRO_TICKS",
    "IntroBatchDisposition",
    "IntroBatchResult",
    "IntroChapterProjection",
    "IntroDetectionBatchPort",
    "IntroDetectionCancelled",
    "IntroDetectionClaimLostError",
    "IntroDetectionConflictError",
    "IntroDetectionExecutionService",
    "IntroDetectionIntent",
    "IntroDetectionMatchTimeout",
    "IntroDetectionNotFoundError",
    "IntroDetectionOperation",
    "IntroDetectionOperationRepositoryPort",
    "IntroDetectionOperationStatus",
    "IntroDetectionPhase",
    "IntroDetectionPolicyPort",
    "IntroDetectionPolicySnapshot",
    "IntroDetectionReservation",
    "IntroDetectionStateError",
    "IntroDetectionSubmissionService",
    "IntroDetectionTransition",
    "IntroEpisodeSnapshot",
    "IntroFingerprintKind",
    "IntroJobTrigger",
    "IntroTriggerMode",
    "MAX_CREDITS_SECONDS",
    "MAX_INTRO_SECONDS",
    "MAX_INTRO_START_SECONDS",
    "MAX_WORK_EPISODES_PER_BATCH",
    "MIN_COMPARABLE_EPISODES",
    "MIN_CREDITS_SECONDS",
    "MIN_INTRO_SECONDS",
    "MatchControl",
    "canonical_json",
    "credits_match_touches_window_start",
    "credits_tail_duration_ticks",
    "detect_common_credits",
    "detect_common_intro",
    "find_best_common_segment",
    "find_template_matched_range",
    "infer_credits_start_from_tail",
    "match_credits_against_template",
    "match_intro_against_template",
    "normalize_credits_start",
    "normalize_intro_range",
    "operation_id",
    "parse_raw_chromaprint",
]
