from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from statistics import median
from typing import Any, Protocol
from uuid import UUID

from .intro_detection import (
    CREDITS_SAMPLE_STEPS,
    EPISODE_MATCH_TIMEOUT_SECONDS,
    FFMPEG_TIMEOUT_SECONDS,
    FINGERPRINT_CACHE_TOLERANCE_SECONDS,
    FINGERPRINT_MIN_VALUES,
    INTRO_SAMPLE_STEPS,
    INTRO_TICKS,
    MAX_INTRO_START_SECONDS,
    MAX_WORK_EPISODES_PER_BATCH,
    MIN_COMPARABLE_EPISODES,
    CancelCheck,
    FingerprintRef,
    IntroBatchDisposition,
    IntroBatchResult,
    IntroChapterProjection,
    IntroDetectionCancelled,
    IntroDetectionMatchTimeout,
    IntroDetectionOperation,
    IntroDetectionPhase,
    IntroDetectionPolicyPort,
    IntroDetectionPolicySnapshot,
    IntroEpisodeSnapshot,
    IntroFingerprintKind,
    IntroJobTrigger,
    MatchControl,
    credits_match_touches_window_start,
    credits_tail_duration_ticks,
    detect_common_credits,
    detect_common_intro,
    infer_credits_start_from_tail,
    match_credits_against_template,
    match_intro_against_template,
    parse_raw_chromaprint,
)
from .media_info_adapters import (
    ControlledProcessProfile,
    ControlledProcessRunner,
    ControlledProcessStatus,
    P115DirectUrl,
)

INTRO_FINGERPRINT_USER_AGENT = "Mozilla/5.0"
MAX_CHROMAPRINT_STDOUT_BYTES = 4 * 1024 * 1024
MAX_CHROMAPRINT_STDERR_BYTES = 256 * 1024


class IntroFingerprintExtractionStatus(StrEnum):
    SUCCEEDED = "succeeded"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    OUTPUT_LIMIT = "output_limit"
    INVALID_OUTPUT = "invalid_output"


@dataclass(frozen=True, slots=True)
class IntroFingerprintExtraction:
    status: IntroFingerprintExtractionStatus
    sample_seconds: int
    window_start_seconds: float
    values: tuple[int, ...] = field(default=(), repr=False)
    reason: str = ""
    backend: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status is IntroFingerprintExtractionStatus.SUCCEEDED

    def as_fingerprint(
        self,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
    ) -> FingerprintRef | None:
        if not self.succeeded:
            return None
        return FingerprintRef(
            episode,
            self.values,
            self.sample_seconds,
            self.window_start_seconds,
            kind,
        )


class LegacyIntroPolicySource(Protocol):
    """Read-only P7 compatibility seam; it deliberately exposes no mutation."""

    def snapshot(self) -> IntroDetectionPolicySnapshot: ...

    def season_is_favorite(self, series_tmdb_id: str, season_number: int) -> bool: ...


class LegacyIntroCandidateSource(Protocol):
    def list_library_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]: ...

    def list_favorite_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]: ...


class LegacyP7IntroReadAdapter(LegacyIntroPolicySource, LegacyIntroCandidateSource):
    """Concrete callable-backed adapter for P7 reads without importing its rules into P6."""

    def __init__(
        self,
        *,
        policy_loader: Callable[[], IntroDetectionPolicySnapshot],
        favorite_reader: Callable[[str, int], bool],
        library_candidates: Callable[[int, Sequence[str]], Sequence[Sequence[IntroEpisodeSnapshot]]],
        favorite_candidates: Callable[[int, Sequence[str]], Sequence[Sequence[IntroEpisodeSnapshot]]],
    ) -> None:
        self._policy_loader = policy_loader
        self._favorite_reader = favorite_reader
        self._library_candidates = library_candidates
        self._favorite_candidates = favorite_candidates

    def snapshot(self) -> IntroDetectionPolicySnapshot:
        snapshot = self._policy_loader()
        if not isinstance(snapshot, IntroDetectionPolicySnapshot):
            raise TypeError("legacy intro policy loader returned an invalid snapshot")
        return snapshot

    def season_is_favorite(self, series_tmdb_id: str, season_number: int) -> bool:
        return bool(self._favorite_reader(str(series_tmdb_id), int(season_number)))

    def list_library_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]:
        return self._library_candidates(_positive_limit(limit), tuple(library_ids))

    def list_favorite_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]:
        return self._favorite_candidates(_positive_limit(limit), tuple(library_ids))


class NativeIntroPolicyAdapter(IntroDetectionPolicyPort):
    def __init__(self, source: LegacyIntroPolicySource) -> None:
        self._source = source

    def permits(self, operation: IntroDetectionOperation) -> bool:
        current = self._source.snapshot()
        trigger = IntroJobTrigger(str(operation.request.get("trigger") or ""))
        if not current.accepts(trigger):
            return False
        if any(not current.permits_library(episode.source_library_id) for episode in operation.episodes):
            return False
        if current.trigger_mode.value == "favorite":
            return self._source.season_is_favorite(operation.series_tmdb_id, operation.season_number)
        return True


class LegacyP7IntroPolicyAdapter(NativeIntroPolicyAdapter):
    """Compatibility name retained for P6 fixtures and old imports."""


class IntroAccountIdentityPort(Protocol):
    def account_id(self) -> str: ...


class IntroDirectUrlResolverPort(Protocol):
    def resolve_download_url(
        self,
        pick_code: str,
        *,
        user_agent: str,
    ) -> P115DirectUrl | None: ...


class AccountBoundIntroDirectUrlPort(Protocol):
    def assert_account(self, account_id: str) -> None: ...

    def resolve(
        self,
        account_id: str,
        pick_code: str,
        *,
        provider_backend: str = "",
    ) -> P115DirectUrl | None: ...


class IntroDetectionAccountChanged(RuntimeError):
    pass


class AccountBoundIntroDirectUrlGateway:
    def __init__(
        self,
        identity: IntroAccountIdentityPort,
        resolver: IntroDirectUrlResolverPort,
    ) -> None:
        self._identity = identity
        self._resolver = resolver

    def assert_account(self, account_id: str) -> None:
        expected = _account_id(account_id)
        if _account_id(self._identity.account_id()) != expected:
            raise IntroDetectionAccountChanged("115 account changed during intro fingerprint extraction")

    def resolve(
        self,
        account_id: str,
        pick_code: str,
        *,
        provider_backend: str = "",
    ) -> P115DirectUrl | None:
        expected = _account_id(account_id)
        self.assert_account(expected)
        resolved = self._resolver.resolve_download_url(
            _required_text(pick_code, "pick_code"),
            user_agent=INTRO_FINGERPRINT_USER_AGENT,
        )
        self.assert_account(expected)
        backend = str(provider_backend or "").strip().lower()
        if backend and (backend not in {"openapi", "cookie"} or resolved is None or resolved.backend != backend):
            return None
        return resolved


class PinnedIntroDirectUrlResolverPort(Protocol):
    def resolve_backend(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        expected_backend: str,
        user_agent: str,
    ) -> P115DirectUrl | None: ...


class NativeP115IntroDirectUrlGateway:
    """Resolve one URL through the backend frozen in the durable operation."""

    def __init__(self, credential_store: Any, direct_urls: PinnedIntroDirectUrlResolverPort) -> None:
        self._credential_store = credential_store
        self._direct_urls = direct_urls

    def assert_account(self, account_id: str) -> None:
        expected = _account_id(account_id)
        snapshot = self._credential_store.load()
        if _account_id(getattr(snapshot, "account_id", "")) != expected:
            raise IntroDetectionAccountChanged("115 account changed during intro fingerprint extraction")

    def resolve(
        self,
        account_id: str,
        pick_code: str,
        *,
        provider_backend: str = "",
    ) -> P115DirectUrl | None:
        expected = _account_id(account_id)
        backend = _provider_backend(provider_backend)
        self.assert_account(expected)
        resolved = self._direct_urls.resolve_backend(
            _required_text(pick_code, "pick_code"),
            expected_account_id=expected,
            expected_backend=backend,
            user_agent=INTRO_FINGERPRINT_USER_AGENT,
        )
        self.assert_account(expected)
        if resolved is not None and resolved.backend != backend:
            raise RuntimeError("115 direct URL resolver changed the frozen provider backend")
        return resolved


class LegacyIntroAccountIdentityAdapter:
    """Read the account identity from an existing LegacyP115CredentialStore."""

    def __init__(self, credential_store: Any) -> None:
        self._credential_store = credential_store

    def account_id(self) -> str:
        snapshot = self._credential_store.load()
        return str(getattr(snapshot, "account_id", "") or "").strip()


class LegacyIntroDirectUrlResolverAdapter:
    """Adapt LegacyP115DirectUrlGateway.resolve to the generic read-only resolver port."""

    def __init__(self, credential_store: Any, direct_urls: Any) -> None:
        self._credential_store = credential_store
        self._direct_urls = direct_urls

    def resolve_download_url(
        self,
        pick_code: str,
        *,
        user_agent: str,
    ) -> P115DirectUrl | None:
        snapshot = self._credential_store.load()
        account_id = str(getattr(snapshot, "account_id", "") or "").strip()
        return self._direct_urls.resolve(
            pick_code,
            expected_account_id=account_id,
            user_agent=user_agent,
        )


def legacy_account_bound_direct_urls(
    credential_store: Any | None = None,
    direct_urls: Any | None = None,
) -> AccountBoundIntroDirectUrlGateway:
    if credential_store is None:
        from .adapters import LegacyP115CredentialStore

        credential_store = LegacyP115CredentialStore()
    if direct_urls is None:
        from .media_info_adapters import LegacyP115DirectUrlGateway

        direct_urls = LegacyP115DirectUrlGateway()
    return AccountBoundIntroDirectUrlGateway(
        LegacyIntroAccountIdentityAdapter(credential_store),
        LegacyIntroDirectUrlResolverAdapter(credential_store, direct_urls),
    )


class ChromaprintProcessAdapter:
    def __init__(
        self,
        runner: ControlledProcessRunner,
        *,
        timeout_seconds: float = FFMPEG_TIMEOUT_SECONDS,
        max_stdout_bytes: int = MAX_CHROMAPRINT_STDOUT_BYTES,
        max_stderr_bytes: int = MAX_CHROMAPRINT_STDERR_BYTES,
    ) -> None:
        if timeout_seconds <= 0 or max_stdout_bytes <= 0 or max_stderr_bytes <= 0:
            raise ValueError("Chromaprint process limits must be positive")
        self._runner = runner
        self._timeout_seconds = float(timeout_seconds)
        self._max_stdout_bytes = int(max_stdout_bytes)
        self._max_stderr_bytes = int(max_stderr_bytes)

    def extract(
        self,
        direct_url: P115DirectUrl,
        *,
        window_start_seconds: float,
        sample_seconds: int,
        audio_stream_index: int | None,
        cancel: CancelCheck,
    ) -> IntroFingerprintExtraction:
        sample = _sample_seconds(sample_seconds)
        arguments = [
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "5",
            "-headers",
            f"User-Agent: {direct_url.user_agent}\r\n",
        ]
        if window_start_seconds > 0:
            arguments.extend(("-ss", f"{window_start_seconds:.3f}"))
        arguments.extend(
            (
                "-i",
                direct_url.url,
                "-map",
                f"0:{audio_stream_index}" if audio_stream_index is not None else "0:a:0",
                "-t",
                str(sample),
                "-ac",
                "1",
                "-acodec",
                "pcm_s16le",
                "-ar",
                "16000",
                "-vn",
                "-sn",
                "-f",
                "chromaprint",
                "-fp_format",
                "raw",
                "-",
            )
        )
        outcome = self._runner.run(
            ControlledProcessProfile.FFMPEG,
            arguments,
            max_stdin_bytes=0,
            max_stdout_bytes=self._max_stdout_bytes,
            max_stderr_bytes=self._max_stderr_bytes,
            timeout_seconds=self._timeout_seconds,
            cancel=cancel,
        )
        values = parse_raw_chromaprint(outcome.stdout) if outcome.succeeded else ()
        if outcome.succeeded and values:
            return IntroFingerprintExtraction(
                IntroFingerprintExtractionStatus.SUCCEEDED,
                sample,
                float(window_start_seconds),
                values,
                backend=direct_url.backend,
            )
        status = {
            ControlledProcessStatus.TIMED_OUT: IntroFingerprintExtractionStatus.TIMED_OUT,
            ControlledProcessStatus.CANCELLED: IntroFingerprintExtractionStatus.CANCELLED,
            ControlledProcessStatus.OUTPUT_LIMIT: IntroFingerprintExtractionStatus.OUTPUT_LIMIT,
            ControlledProcessStatus.INVALID_OUTPUT: IntroFingerprintExtractionStatus.INVALID_OUTPUT,
        }.get(outcome.status, IntroFingerprintExtractionStatus.FAILED)
        if outcome.succeeded:
            status = IntroFingerprintExtractionStatus.INVALID_OUTPUT
        return IntroFingerprintExtraction(
            status,
            sample,
            float(window_start_seconds),
            reason=outcome.diagnostic,
            backend=direct_url.backend,
        )


class P115IntroFingerprintAdapter:
    """Operation-local URL reuse; signed URLs never leave this object."""

    def __init__(
        self,
        direct_urls: AccountBoundIntroDirectUrlPort,
        process: ChromaprintProcessAdapter,
        *,
        success_delay_seconds: float = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if success_delay_seconds < 0:
            raise ValueError("fingerprint success delay must not be negative")
        self._direct_urls = direct_urls
        self._process = process
        self._success_delay_seconds = float(success_delay_seconds)
        self._sleeper = sleeper
        self._cache: dict[tuple[str, str, str], P115DirectUrl] = {}

    def extract(
        self,
        account_id: str,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        *,
        provider_backend: str = "openapi",
        sample_seconds: int,
        audio_stream_index: int | None,
        cancel: CancelCheck,
    ) -> IntroFingerprintExtraction:
        expected_account = _account_id(account_id)
        backend = _provider_backend(provider_backend)
        window_start, sample = fingerprint_window(episode, kind, sample_seconds)
        if sample <= 0:
            return IntroFingerprintExtraction(
                IntroFingerprintExtractionStatus.UNAVAILABLE,
                sample,
                window_start,
                reason="runtime_unavailable",
            )
        cache_key = (expected_account, backend, episode.pick_code)
        cached = self._cache.get(cache_key)
        reused = cached is not None
        direct_url = cached or self._direct_urls.resolve(
            expected_account,
            episode.pick_code,
            provider_backend=backend,
        )
        if direct_url is None:
            return IntroFingerprintExtraction(
                IntroFingerprintExtractionStatus.UNAVAILABLE,
                sample,
                window_start,
                reason="direct_url_unavailable",
            )
        self._cache[cache_key] = direct_url
        outcome = self._extract_with_account_fence(
            expected_account,
            direct_url,
            window_start_seconds=window_start,
            sample_seconds=sample,
            audio_stream_index=audio_stream_index,
            cancel=cancel,
        )
        if not outcome.succeeded and reused and outcome.status is not IntroFingerprintExtractionStatus.CANCELLED:
            self._cache.pop(cache_key, None)
            refreshed = self._direct_urls.resolve(
                expected_account,
                episode.pick_code,
                provider_backend=backend,
            )
            if refreshed is not None:
                self._cache[cache_key] = refreshed
                outcome = self._extract_with_account_fence(
                    expected_account,
                    refreshed,
                    window_start_seconds=window_start,
                    sample_seconds=sample,
                    audio_stream_index=audio_stream_index,
                    cancel=cancel,
                )
        if outcome.succeeded:
            _interruptible_delay(
                self._success_delay_seconds,
                cancel=cancel,
                sleeper=self._sleeper,
            )
        return outcome

    def _extract_with_account_fence(
        self,
        account_id: str,
        direct_url: P115DirectUrl,
        *,
        window_start_seconds: float,
        sample_seconds: int,
        audio_stream_index: int | None,
        cancel: CancelCheck,
    ) -> IntroFingerprintExtraction:
        self._direct_urls.assert_account(account_id)
        try:
            return self._process.extract(
                direct_url,
                window_start_seconds=window_start_seconds,
                sample_seconds=sample_seconds,
                audio_stream_index=audio_stream_index,
                cancel=cancel,
            )
        finally:
            self._direct_urls.assert_account(account_id)

    def clear(self) -> None:
        self._cache.clear()


class IntroChaptersChangedHook(Protocol):
    """P9 compatibility notification only; P6 never owns shared delivery."""

    def chapters_staged(
        self,
        operation_id: str,
        episode_sha1s: Sequence[str],
        effect_ids: Sequence[str],
    ) -> None: ...


class NoopIntroChaptersChangedHook:
    def chapters_staged(
        self,
        operation_id: str,
        episode_sha1s: Sequence[str],
        effect_ids: Sequence[str],
    ) -> None:
        del operation_id, episode_sha1s, effect_ids


class IntroDetectionWorkRepository(Protocol):
    def pending_episodes(
        self,
        operation: IntroDetectionOperation,
        kind: IntroFingerprintKind,
    ) -> tuple[IntroEpisodeSnapshot, ...]: ...

    def load_fingerprint(
        self,
        sha1: str,
        kind: IntroFingerprintKind,
        *,
        minimum_sample_seconds: int = 0,
    ) -> tuple[tuple[int, ...], int] | None: ...

    def save_fingerprint(
        self,
        sha1: str,
        kind: IntroFingerprintKind,
        values: Sequence[int],
        *,
        sample_seconds: int,
    ) -> bool: ...

    def has_detection_failure(
        self,
        failure_key: str,
        kind: IntroFingerprintKind,
        *,
        algorithm_version: int,
    ) -> bool: ...

    def mark_detection_failure(
        self,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        *,
        scope: str,
        reason: str,
        algorithm_version: int,
        sample_count: int = 0,
        episode_count: int = 0,
    ) -> None: ...

    def record_fingerprint_failure(
        self,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        *,
        sample_seconds: int,
        reason: str,
        algorithm_version: int,
    ) -> int: ...

    def clear_fingerprint_failure(
        self,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        *,
        algorithm_version: int,
    ) -> None: ...

    def chapter_markers(
        self,
        episode: IntroEpisodeSnapshot,
    ) -> tuple[int | None, int | None, int | None]: ...

    def fingerprint_audio_stream_index(self, episode: IntroEpisodeSnapshot) -> int | None: ...

    def stage_chapters(
        self,
        operation_id: UUID,
        claim_token: UUID,
        projections: Sequence[IntroChapterProjection],
        *,
        authorization_revision: str,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
    ) -> tuple[UUID, ...]: ...


class IntroChapterAuthorizationPort(Protocol):
    def authorization_revision(self) -> str: ...


class EmbyAuthorizationRevisionAdapter:
    """Concrete adapter for HttpxEmbyBridgeClient or another revision source."""

    def __init__(self, bridge: Any) -> None:
        self._bridge = bridge

    def authorization_revision(self) -> str:
        revision = str(self._bridge.authorization_revision() or "").strip().lower()
        if len(revision) != 64:
            raise ValueError("Emby authorization revision is invalid")
        try:
            bytes.fromhex(revision)
        except ValueError as exc:
            raise ValueError("Emby authorization revision is invalid") from exc
        return revision


class NativeIntroDetectionBatchEngine:
    def __init__(
        self,
        repository: IntroDetectionWorkRepository,
        fingerprints: P115IntroFingerprintAdapter,
        authorization: IntroChapterAuthorizationPort,
        *,
        chapters_changed: IntroChaptersChangedHook | None = None,
    ) -> None:
        self._repository = repository
        self._fingerprints = fingerprints
        self._authorization = authorization
        self._chapters_changed = chapters_changed or NoopIntroChaptersChangedHook()

    def execute_batch(
        self,
        operation: IntroDetectionOperation,
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult:
        try:
            return self._execute_batch(operation, cancel=cancel)
        finally:
            self._fingerprints.clear()

    def _execute_batch(
        self,
        operation: IntroDetectionOperation,
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult:
        token = operation.claim_token
        if token is None:
            raise RuntimeError("intro detection batch requires an active claim")
        if cancel():
            raise IntroDetectionCancelled("intro detection batch was cancelled")
        restored = self._restore_cached(operation, token, cancel=cancel)
        if restored is not None:
            return restored
        intro_pending = self._repository.pending_episodes(operation, IntroFingerprintKind.INTRO)
        if intro_pending:
            return self._execute_intro(operation, token, intro_pending, cancel=cancel)
        policy = operation.request.get("policy")
        credits_enabled = isinstance(policy, dict) and policy.get("credits_enabled") is True
        if credits_enabled:
            credits_pending = self._repository.pending_episodes(operation, IntroFingerprintKind.CREDITS)
            if credits_pending:
                return self._execute_credits(operation, token, credits_pending, cancel=cancel)
        return IntroBatchResult(
            IntroBatchDisposition.CONFIRMED,
            IntroDetectionPhase.PROJECT,
            False,
            result={
                "episodes": len(operation.episodes),
                "algorithm_version": operation.algorithm_version,
                "durable_effects_staged": True,
            },
        )

    def _restore_cached(
        self,
        operation: IntroDetectionOperation,
        token: UUID,
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult | None:
        restored = {str(value) for value in operation.progress.get("restored_sha1s", [])}
        projections: list[IntroChapterProjection] = []
        for episode in operation.episodes:
            if episode.sha1 in restored:
                continue
            intro_start, intro_end, credits_start = self._repository.chapter_markers(episode)
            if intro_start is None and credits_start is None:
                restored.add(episode.sha1)
                continue
            projections.append(
                IntroChapterProjection(
                    episode,
                    intro_start_ticks=intro_start,
                    intro_end_ticks=intro_end,
                    credits_start_ticks=credits_start,
                )
            )
            if len(projections) >= MAX_WORK_EPISODES_PER_BATCH:
                break
        if not projections:
            return None
        self._checkpoint(cancel)
        progress = {
            **dict(operation.progress),
            "restored_sha1s": sorted(restored | {projection.episode.sha1 for projection in projections}),
        }
        effects = self._repository.stage_chapters(
            operation.id,
            token,
            projections,
            authorization_revision=self._authorization.authorization_revision(),
            phase=IntroDetectionPhase.RESTORE,
            progress=progress,
        )
        self._notify(operation.id, projections, effects)
        return IntroBatchResult(
            IntroBatchDisposition.CONTINUE,
            IntroDetectionPhase.RESTORE,
            True,
            progress=progress,
        )

    def _execute_intro(
        self,
        operation: IntroDetectionOperation,
        token: UUID,
        pending: Sequence[IntroEpisodeSnapshot],
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult:
        batch = _rotating_batch(pending, operation.progress.get("no_progress_batches"))
        templates = self._templates(operation, batch, IntroFingerprintKind.INTRO)
        if len(templates) < MIN_COMPARABLE_EPISODES:
            return self._no_progress(IntroDetectionPhase.INTRO_EXTRACT, "insufficient_intro_templates")
        match = None
        fingerprints: tuple[FingerprintRef, ...] = ()
        selected_sample = 0
        try:
            for sample_seconds in INTRO_SAMPLE_STEPS:
                fingerprints = self._fingerprint_set(
                    operation,
                    templates,
                    IntroFingerprintKind.INTRO,
                    sample_seconds,
                    cancel=cancel,
                )
                if len(fingerprints) < MIN_COMPARABLE_EPISODES:
                    continue
                match = detect_common_intro(
                    fingerprints,
                    max_start_seconds=min(MAX_INTRO_START_SECONDS, sample_seconds),
                    control=MatchControl(60, cancel=cancel),
                )
                if match is not None:
                    selected_sample = sample_seconds
                    break
        except IntroDetectionMatchTimeout:
            return self._no_progress(IntroDetectionPhase.INTRO_MATCH, "intro_common_match_timeout")
        if match is None:
            return self._no_progress(IntroDetectionPhase.INTRO_MATCH, "intro_no_common_match")
        projections: list[IntroChapterProjection] = []
        for episode in batch:
            self._checkpoint(cancel)
            own_range = match.ranges_by_sha1.get(episode.sha1)
            if own_range is None:
                try:
                    own_range = self._match_intro_progressively(
                        operation,
                        episode,
                        match.base,
                        match.base_start_seconds,
                        match.base_end_seconds,
                        selected_sample,
                        cancel=cancel,
                    )
                except IntroDetectionMatchTimeout:
                    self._repository.mark_detection_failure(
                        episode,
                        IntroFingerprintKind.INTRO,
                        scope="episode",
                        reason="intro_episode_match_timeout",
                        algorithm_version=operation.algorithm_version,
                        sample_count=len(fingerprints),
                        episode_count=len(operation.episodes),
                    )
                    continue
            if own_range is not None:
                projections.append(
                    IntroChapterProjection(
                        episode,
                        intro_start_ticks=own_range[0],
                        intro_end_ticks=own_range[1],
                    )
                )
        if not projections:
            return self._no_progress(IntroDetectionPhase.INTRO_MATCH, "intro_template_unmatched")
        return self._stage(
            operation,
            token,
            projections,
            phase=IntroDetectionPhase.PROJECT,
            kind="intro",
            cancel=cancel,
        )

    def _execute_credits(
        self,
        operation: IntroDetectionOperation,
        token: UUID,
        pending: Sequence[IntroEpisodeSnapshot],
        *,
        cancel: CancelCheck,
    ) -> IntroBatchResult:
        batch = _rotating_batch(pending, operation.progress.get("no_progress_batches"))
        inferred = self._infer_credits(operation, batch)
        if inferred:
            return self._stage(
                operation,
                token,
                inferred,
                phase=IntroDetectionPhase.PROJECT,
                kind="credits_inferred",
                cancel=cancel,
            )
        templates = self._templates(operation, batch, IntroFingerprintKind.CREDITS)
        if len(templates) < MIN_COMPARABLE_EPISODES:
            return self._no_progress(IntroDetectionPhase.CREDITS_EXTRACT, "insufficient_credits_templates")
        match = None
        fingerprints: tuple[FingerprintRef, ...] = ()
        selected_sample = 0
        try:
            for sample_seconds in CREDITS_SAMPLE_STEPS:
                selected_sample = sample_seconds
                fingerprints = self._fingerprint_set(
                    operation,
                    templates,
                    IntroFingerprintKind.CREDITS,
                    sample_seconds,
                    cancel=cancel,
                )
                if len(fingerprints) < MIN_COMPARABLE_EPISODES:
                    continue
                match = detect_common_credits(
                    fingerprints,
                    max_start_seconds=sample_seconds,
                    control=MatchControl(60, cancel=cancel),
                )
                if match is None:
                    continue
                if not credits_match_touches_window_start(match, fingerprints):
                    break
                if sample_seconds >= CREDITS_SAMPLE_STEPS[-1]:
                    match = None
                    break
        except IntroDetectionMatchTimeout:
            self._repository.mark_detection_failure(
                batch[0],
                IntroFingerprintKind.CREDITS,
                scope="season",
                reason="credits_common_match_timeout",
                algorithm_version=operation.algorithm_version,
                sample_count=len(fingerprints),
                episode_count=len(operation.episodes),
            )
            return IntroBatchResult(
                IntroBatchDisposition.CONTINUE,
                IntroDetectionPhase.CREDITS_MATCH,
                True,
                progress={"credits_terminal_reason": "credits_common_match_timeout"},
            )
        if match is None:
            if len(fingerprints) >= MIN_COMPARABLE_EPISODES and selected_sample >= CREDITS_SAMPLE_STEPS[-1]:
                self._repository.mark_detection_failure(
                    batch[0],
                    IntroFingerprintKind.CREDITS,
                    scope="season",
                    reason="credits_no_safe_common_match",
                    algorithm_version=operation.algorithm_version,
                    sample_count=len(fingerprints),
                    episode_count=len(operation.episodes),
                )
                return IntroBatchResult(
                    IntroBatchDisposition.CONTINUE,
                    IntroDetectionPhase.CREDITS_MATCH,
                    True,
                    progress={"credits_terminal_reason": "credits_no_safe_common_match"},
                )
            return self._no_progress(IntroDetectionPhase.CREDITS_MATCH, "credits_no_common_match")
        projections: list[IntroChapterProjection] = []
        for episode in batch:
            self._checkpoint(cancel)
            own_start = match.starts_by_sha1.get(episode.sha1)
            had_fingerprint = own_start is not None
            if own_start is None:
                try:
                    own_start, had_fingerprint = self._match_credits_progressively(
                        operation,
                        episode,
                        match.base,
                        match.base_start_seconds,
                        match.base_end_seconds,
                        selected_sample,
                        cancel=cancel,
                    )
                except IntroDetectionMatchTimeout:
                    self._repository.mark_detection_failure(
                        episode,
                        IntroFingerprintKind.CREDITS,
                        scope="episode",
                        reason="credits_episode_match_timeout",
                        algorithm_version=operation.algorithm_version,
                        sample_count=len(fingerprints),
                        episode_count=len(operation.episodes),
                    )
                    continue
            if own_start is None and had_fingerprint:
                self._repository.mark_detection_failure(
                    episode,
                    IntroFingerprintKind.CREDITS,
                    scope="episode",
                    reason="credits_template_no_match",
                    algorithm_version=operation.algorithm_version,
                    sample_count=len(fingerprints),
                    episode_count=len(operation.episodes),
                )
                continue
            if own_start is not None:
                projections.append(IntroChapterProjection(episode, credits_start_ticks=own_start))
        if not projections:
            return self._no_progress(IntroDetectionPhase.CREDITS_MATCH, "credits_template_unmatched")
        return self._stage(
            operation,
            token,
            projections,
            phase=IntroDetectionPhase.PROJECT,
            kind="credits",
            cancel=cancel,
        )

    def _templates(
        self,
        operation: IntroDetectionOperation,
        priority: Sequence[IntroEpisodeSnapshot],
        kind: IntroFingerprintKind,
    ) -> tuple[IntroEpisodeSnapshot, ...]:
        selected: list[IntroEpisodeSnapshot] = []
        seen: set[str] = set()
        for episode in (*priority, *operation.episodes):
            if episode.sha1 in seen:
                continue
            if self._repository.has_detection_failure(
                f"episode:{episode.sha1}",
                kind,
                algorithm_version=operation.algorithm_version,
            ):
                continue
            selected.append(episode)
            seen.add(episode.sha1)
            if len(selected) >= MAX_WORK_EPISODES_PER_BATCH:
                break
        return tuple(selected)

    def _fingerprint_set(
        self,
        operation: IntroDetectionOperation,
        episodes: Sequence[IntroEpisodeSnapshot],
        kind: IntroFingerprintKind,
        sample_seconds: int,
        *,
        cancel: CancelCheck,
    ) -> tuple[FingerprintRef, ...]:
        fingerprints = [
            fingerprint
            for episode in episodes
            if (
                fingerprint := self._load_or_extract(
                    operation,
                    episode,
                    kind,
                    sample_seconds,
                    cancel=cancel,
                )
            )
            is not None
            and len(fingerprint.values) >= FINGERPRINT_MIN_VALUES
        ]
        return tuple(fingerprints)

    def _load_or_extract(
        self,
        operation: IntroDetectionOperation,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        sample_seconds: int,
        *,
        cancel: CancelCheck,
    ) -> FingerprintRef | None:
        cached = self._repository.load_fingerprint(
            episode.sha1,
            kind,
            minimum_sample_seconds=sample_seconds,
        )
        if cached is not None:
            values, coverage = cached
            sliced = _slice_cached_fingerprint(values, coverage, sample_seconds, kind)
            window_start, bounded = fingerprint_window(episode, kind, sample_seconds)
            return FingerprintRef(episode, sliced, bounded, window_start, kind)
        outcome = self._fingerprints.extract(
            operation.account_id,
            episode,
            kind,
            provider_backend=operation.provider_backend,
            sample_seconds=sample_seconds,
            audio_stream_index=self._repository.fingerprint_audio_stream_index(episode),
            cancel=cancel,
        )
        if outcome.status is IntroFingerprintExtractionStatus.CANCELLED:
            raise IntroDetectionCancelled("Chromaprint extraction was cancelled")
        if not outcome.succeeded:
            self._repository.record_fingerprint_failure(
                episode,
                kind,
                sample_seconds=sample_seconds,
                reason=outcome.status.value,
                algorithm_version=operation.algorithm_version,
            )
            return None
        self._repository.save_fingerprint(
            episode.sha1,
            kind,
            outcome.values,
            sample_seconds=outcome.sample_seconds,
        )
        self._repository.clear_fingerprint_failure(
            episode,
            kind,
            algorithm_version=operation.algorithm_version,
        )
        return outcome.as_fingerprint(episode, kind)

    def _match_intro_progressively(
        self,
        operation: IntroDetectionOperation,
        episode: IntroEpisodeSnapshot,
        base: FingerprintRef,
        base_start: float,
        base_end: float,
        initial_sample: int,
        *,
        cancel: CancelCheck,
    ) -> tuple[int, int] | None:
        for sample_seconds in INTRO_SAMPLE_STEPS:
            if sample_seconds < initial_sample:
                continue
            fingerprint = self._load_or_extract(
                operation,
                episode,
                IntroFingerprintKind.INTRO,
                sample_seconds,
                cancel=cancel,
            )
            if fingerprint is None or len(fingerprint.values) < FINGERPRINT_MIN_VALUES:
                continue
            matched = match_intro_against_template(
                base,
                fingerprint,
                base_start,
                base_end,
                max_start_seconds=min(MAX_INTRO_START_SECONDS, sample_seconds),
                control=MatchControl(EPISODE_MATCH_TIMEOUT_SECONDS, cancel=cancel),
            )
            if matched is not None:
                return matched
        return None

    def _match_credits_progressively(
        self,
        operation: IntroDetectionOperation,
        episode: IntroEpisodeSnapshot,
        base: FingerprintRef,
        base_start: float,
        base_end: float,
        initial_sample: int,
        *,
        cancel: CancelCheck,
    ) -> tuple[int | None, bool]:
        had_fingerprint = False
        for sample_seconds in CREDITS_SAMPLE_STEPS:
            if sample_seconds < initial_sample:
                continue
            fingerprint = self._load_or_extract(
                operation,
                episode,
                IntroFingerprintKind.CREDITS,
                sample_seconds,
                cancel=cancel,
            )
            if fingerprint is None or len(fingerprint.values) < FINGERPRINT_MIN_VALUES:
                continue
            had_fingerprint = True
            matched = match_credits_against_template(
                base,
                fingerprint,
                base_start,
                base_end,
                max_start_seconds=sample_seconds,
                control=MatchControl(EPISODE_MATCH_TIMEOUT_SECONDS, cancel=cancel),
            )
            if matched is None:
                continue
            relative = matched / INTRO_TICKS - fingerprint.window_start_seconds
            if relative > 8:
                return matched, True
            if sample_seconds >= CREDITS_SAMPLE_STEPS[-1]:
                return None, True
        return None, had_fingerprint

    def _infer_credits(
        self,
        operation: IntroDetectionOperation,
        pending: Sequence[IntroEpisodeSnapshot],
    ) -> tuple[IntroChapterProjection, ...]:
        tails = [
            tail
            for episode in operation.episodes
            if (
                tail := credits_tail_duration_ticks(
                    episode.runtime_seconds,
                    self._repository.chapter_markers(episode)[2] or 0,
                )
            )
            is not None
        ]
        if len(tails) < 2:
            return ()
        median_tail = float(median(tails))
        projections: list[IntroChapterProjection] = []
        intro_pending = {
            episode.sha1 for episode in self._repository.pending_episodes(operation, IntroFingerprintKind.INTRO)
        }
        for episode in pending:
            intro_start, intro_end, _ = self._repository.chapter_markers(episode)
            if intro_start is None or intro_end is None:
                if episode.sha1 not in intro_pending:
                    self._repository.mark_detection_failure(
                        episode,
                        IntroFingerprintKind.CREDITS,
                        scope="episode",
                        reason="credits_inference_no_intro",
                        algorithm_version=operation.algorithm_version,
                        sample_count=len(tails),
                        episode_count=len(operation.episodes),
                    )
                continue
            start = infer_credits_start_from_tail(episode.runtime_seconds, median_tail)
            if start is not None:
                projections.append(IntroChapterProjection(episode, credits_start_ticks=start))
        return tuple(projections)

    def _stage(
        self,
        operation: IntroDetectionOperation,
        token: UUID,
        projections: Sequence[IntroChapterProjection],
        *,
        phase: IntroDetectionPhase,
        kind: str,
        cancel: CancelCheck,
    ) -> IntroBatchResult:
        self._checkpoint(cancel)
        progress = {
            **dict(operation.progress),
            "no_progress_batches": 0,
            f"{kind}_projected": int(operation.progress.get(f"{kind}_projected") or 0) + len(projections),
        }
        effects = self._repository.stage_chapters(
            operation.id,
            token,
            tuple(projections[:MAX_WORK_EPISODES_PER_BATCH]),
            authorization_revision=self._authorization.authorization_revision(),
            phase=phase,
            progress=progress,
        )
        self._notify(operation.id, projections, effects)
        return IntroBatchResult(
            IntroBatchDisposition.CONTINUE,
            phase,
            True,
            progress=progress,
        )

    def _notify(
        self,
        operation_id: UUID,
        projections: Sequence[IntroChapterProjection],
        effects: Sequence[UUID],
    ) -> None:
        try:
            self._chapters_changed.chapters_staged(
                str(operation_id),
                [projection.episode.sha1 for projection in projections],
                [str(effect) for effect in effects],
            )
        except Exception:
            # P9 compatibility notification cannot determine P6 operation success.
            return

    @staticmethod
    def _checkpoint(cancel: CancelCheck) -> None:
        if cancel():
            raise IntroDetectionCancelled("intro detection batch was cancelled")

    @staticmethod
    def _no_progress(phase: IntroDetectionPhase, reason: str) -> IntroBatchResult:
        return IntroBatchResult(
            IntroBatchDisposition.CONTINUE,
            phase,
            False,
            progress={"last_no_progress_reason": reason},
        )


def fingerprint_window(
    episode: IntroEpisodeSnapshot,
    kind: IntroFingerprintKind,
    requested_sample_seconds: int,
) -> tuple[float, int]:
    sample = _sample_seconds(requested_sample_seconds)
    if kind is IntroFingerprintKind.INTRO:
        return 0, sample
    if episode.runtime_seconds <= 0:
        return 0, 0
    bounded = min(sample, max(1, int(episode.runtime_seconds)))
    return max(0, episode.runtime_seconds - bounded), bounded


def _rotating_batch(
    pending: Sequence[IntroEpisodeSnapshot],
    no_progress_batches: object,
) -> tuple[IntroEpisodeSnapshot, ...]:
    episodes = tuple(pending)
    if not episodes:
        return ()
    try:
        completed_batches = max(0, int(str(no_progress_batches or 0)))
    except ValueError:
        completed_batches = 0
    start = (completed_batches * MAX_WORK_EPISODES_PER_BATCH) % len(episodes)
    count = min(MAX_WORK_EPISODES_PER_BATCH, len(episodes))
    return tuple(episodes[(start + offset) % len(episodes)] for offset in range(count))


def _slice_cached_fingerprint(
    values: Sequence[int],
    cached_coverage_seconds: int,
    requested_seconds: int,
    kind: IntroFingerprintKind,
) -> tuple[int, ...]:
    normalized = tuple(int(value) for value in values)
    coverage = max(1, int(cached_coverage_seconds))
    requested = min(max(1, int(requested_seconds)), coverage)
    if requested + FINGERPRINT_CACHE_TOLERANCE_SECONDS >= coverage:
        return normalized
    count = max(FINGERPRINT_MIN_VALUES, int(round(len(normalized) * requested / coverage)))
    count = min(count, len(normalized))
    if kind is IntroFingerprintKind.CREDITS:
        return normalized[-count:]
    return normalized[:count]


def _interruptible_delay(
    seconds: float,
    *,
    cancel: CancelCheck,
    sleeper: Callable[[float], None],
) -> None:
    remaining = float(seconds)
    while remaining > 0 and not cancel():
        interval = min(0.1, remaining)
        sleeper(interval)
        remaining -= interval


def _sample_seconds(value: int) -> int:
    if isinstance(value, bool):
        raise ValueError("sample_seconds must be positive")
    sample = int(value)
    if sample <= 0 or sample > INTRO_SAMPLE_STEPS[-1]:
        raise ValueError("sample_seconds is outside the frozen intro detection windows")
    return sample


def _account_id(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized.isdigit():
        raise ValueError("115 account ID must be numeric")
    return normalized


def _provider_backend(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in {"openapi", "cookie"}:
        raise ValueError("115 provider backend must be openapi or cookie")
    return normalized


def _required_text(value: object, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_limit(value: int) -> int:
    if isinstance(value, bool) or int(value) < 1:
        raise ValueError("legacy intro candidate limit must be positive")
    return int(value)


__all__ = [
    "AccountBoundIntroDirectUrlGateway",
    "AccountBoundIntroDirectUrlPort",
    "ChromaprintProcessAdapter",
    "INTRO_FINGERPRINT_USER_AGENT",
    "IntroAccountIdentityPort",
    "IntroChaptersChangedHook",
    "IntroChapterAuthorizationPort",
    "EmbyAuthorizationRevisionAdapter",
    "IntroDetectionAccountChanged",
    "IntroDirectUrlResolverPort",
    "IntroFingerprintExtraction",
    "IntroFingerprintExtractionStatus",
    "LegacyIntroCandidateSource",
    "LegacyIntroAccountIdentityAdapter",
    "LegacyIntroDirectUrlResolverAdapter",
    "LegacyIntroPolicySource",
    "LegacyP7IntroReadAdapter",
    "LegacyP7IntroPolicyAdapter",
    "NoopIntroChaptersChangedHook",
    "NativeIntroDetectionBatchEngine",
    "NativeIntroPolicyAdapter",
    "NativeP115IntroDirectUrlGateway",
    "P115IntroFingerprintAdapter",
    "PinnedIntroDirectUrlResolverPort",
    "fingerprint_window",
    "legacy_account_bound_direct_urls",
]
