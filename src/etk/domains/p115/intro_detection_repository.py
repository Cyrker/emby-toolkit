from __future__ import annotations

import hashlib
import json
import zlib
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from etk.domains.media.effects import SqlAlchemyMediaEffectRepository
from etk.platform.db import SqlAlchemyUnitOfWork

from .intro_detection import (
    BATCH_CONTINUATION_DELAY_SECONDS,
    CREDITS_SAMPLE_STEPS,
    FINGERPRINT_CACHE_TOLERANCE_SECONDS,
    INTRO_DETECTION_AGGREGATE_TYPE,
    INTRO_DETECTION_TOPIC,
    INTRO_SAMPLE_STEPS,
    IntroChapterProjection,
    IntroDetectionCancelled,
    IntroDetectionClaimLostError,
    IntroDetectionConflictError,
    IntroDetectionIntent,
    IntroDetectionNotFoundError,
    IntroDetectionOperation,
    IntroDetectionOperationStatus,
    IntroDetectionPhase,
    IntroDetectionReservation,
    IntroDetectionStateError,
    IntroDetectionTransition,
    IntroEpisodeSnapshot,
    IntroFingerprintKind,
    IntroTriggerMode,
    canonical_json,
    operation_id,
)

_ACTIVE_STATUSES = ("ready", "claimed", "retryable")
_TERMINAL_STATUSES = ("confirmed", "rejected", "cancelled", "superseded")


class PostgresP115IntroDetectionRepository:
    """One-UoW owner for season operations, legacy caches, and chapter intents."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reserve(self, intent: IntroDetectionIntent) -> IntroDetectionReservation:
        _validate_intent(intent)
        _assert_no_external_url(intent.request, "intro detection request")
        with self._unit_of_work() as uow:
            _advisory_lock(uow, f"p115-intro-season:{intent.series_tmdb_id}:{intent.season_number}")
            existing = _operation_by_key(uow, intent.operation_key, for_update=True)
            if existing is not None:
                operation = _operation_projection(existing)
                _assert_reservation_matches(operation, intent)
                return IntroDetectionReservation(operation, created=False)

            active_rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_intro_detection_operations
                        WHERE series_tmdb_id = :series_tmdb_id
                          AND season_number = :season_number
                          AND status IN ('ready','claimed','retryable')
                        ORDER BY event_sequence
                        FOR UPDATE
                        """
                    ),
                    {
                        "series_tmdb_id": intent.series_tmdb_id,
                        "season_number": intent.season_number,
                    },
                )
                .mappings()
                .all()
            )
            for row in active_rows:
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_intro_detection_operations
                        SET status = 'superseded', claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            error_json = CAST(:error_json AS jsonb), finished_at = NOW(),
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                          AND status IN ('ready','claimed','retryable')
                        """
                    ),
                    {
                        "operation_id": row["id"],
                        "error_json": canonical_json(
                            {
                                "type": "source_or_policy_superseded",
                                "replacement_operation_key": intent.operation_key,
                            }
                        ),
                    },
                )
                _complete_outbox(uow, UUID(str(row["id"])))

            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_intro_detection_operations (
                            id, operation_key, request_hash, source_set_revision,
                            policy_revision, series_tmdb_id, season_number,
                            algorithm_version, account_id, trigger_mode, request_json,
                            status, phase, available_at
                        ) VALUES (
                            :id, :operation_key, :request_hash, :source_set_revision,
                            :policy_revision, :series_tmdb_id, :season_number,
                            :algorithm_version, :account_id, :trigger_mode,
                            CAST(:request_json AS jsonb), 'ready', 'restore', :available_at
                        )
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id(intent.operation_key),
                        "operation_key": intent.operation_key,
                        "request_hash": intent.request_hash,
                        "source_set_revision": intent.source_set_revision,
                        "policy_revision": intent.policy_revision,
                        "series_tmdb_id": intent.series_tmdb_id,
                        "season_number": intent.season_number,
                        "algorithm_version": intent.algorithm_version,
                        "account_id": intent.account_id,
                        "trigger_mode": intent.trigger_mode.value,
                        "request_json": canonical_json(intent.request),
                        "available_at": intent.available_at,
                    },
                )
                .mappings()
                .one()
            )
            operation = _operation_projection(row)
            _stage_outbox(uow, operation)
            uow.commit()
            return IntroDetectionReservation(operation, created=True)
        raise AssertionError("unreachable")

    def read(self, operation_id: UUID) -> IntroDetectionOperation | None:
        with self._unit_of_work() as uow:
            row = _operation_row(uow, operation_id)
            return _operation_projection(row) if row is not None else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> IntroDetectionOperation | None:
        worker = _required_text(worker_id, "worker_id", maximum=255)
        _positive_seconds(lease_seconds, "lease_seconds")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_intro_detection_operations
                        SET status = 'claimed', claimed_by = :worker_id,
                            claim_token = :claim_token, claimed_at = NOW(),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            attempts = attempts + 1, state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :operation_id
                          AND status IN ('ready','retryable')
                          AND available_at <= NOW()
                          AND cancel_requested_at IS NULL
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "worker_id": worker,
                        "claim_token": uuid4(),
                        "lease_seconds": lease_seconds,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            _complete_outbox(uow, operation_id)
            uow.commit()
            return _operation_projection(row)

    def heartbeat(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        lease_seconds: float,
    ) -> bool:
        _positive_seconds(lease_seconds, "lease_seconds")
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_intro_detection_operations
                    SET lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                        updated_at = NOW()
                    WHERE id = :operation_id AND status = 'claimed'
                      AND claim_token = :claim_token AND lease_expires_at > NOW()
                      AND cancel_requested_at IS NULL
                    """
                ),
                {
                    "operation_id": operation_id,
                    "claim_token": claim_token,
                    "lease_seconds": lease_seconds,
                },
            )
            changed = int(getattr(updated, "rowcount", 0) or 0) == 1
            if changed:
                uow.commit()
            return changed
        raise AssertionError("unreachable")

    def is_cancel_requested(self, operation_id: UUID, claim_token: UUID) -> bool:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT cancel_requested_at, lease_expires_at > NOW() AS lease_active
                        FROM p115_intro_detection_operations
                        WHERE id = :operation_id AND status = 'claimed'
                          AND claim_token = :claim_token
                        """
                    ),
                    {"operation_id": operation_id, "claim_token": claim_token},
                )
                .mappings()
                .one_or_none()
            )
        return row is None or row["cancel_requested_at"] is not None or not bool(row["lease_active"])

    def request_cancel(self, operation_id: UUID) -> IntroDetectionTransition:
        with self._unit_of_work() as uow:
            row = _operation_row(uow, operation_id, for_update=True)
            if row is None:
                raise IntroDetectionNotFoundError(f"intro detection operation {operation_id} was not found")
            operation = _operation_projection(row)
            if operation.status.terminal:
                return IntroDetectionTransition(operation, changed=False)
            if operation.status is IntroDetectionOperationStatus.CLAIMED:
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_intro_detection_operations
                            SET cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
                                state_revision = state_revision + 1, updated_at = NOW()
                            WHERE id = :operation_id AND status = 'claimed'
                            RETURNING *
                            """
                        ),
                        {"operation_id": operation_id},
                    )
                    .mappings()
                    .one()
                )
            else:
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_intro_detection_operations
                            SET status = 'cancelled', cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
                                error_json = CAST(:error_json AS jsonb), finished_at = NOW(),
                                state_revision = state_revision + 1, updated_at = NOW()
                            WHERE id = :operation_id AND status IN ('ready','retryable')
                            RETURNING *
                            """
                        ),
                        {
                            "operation_id": operation_id,
                            "error_json": canonical_json({"type": "cancel_requested"}),
                        },
                    )
                    .mappings()
                    .one()
                )
                _complete_outbox(uow, operation_id)
            uow.commit()
            return IntroDetectionTransition(_operation_projection(updated), changed=True)
        raise AssertionError("unreachable")

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        error: Mapping[str, Any] | None,
        retry_after_seconds: float,
    ) -> IntroDetectionTransition:
        _nonnegative_seconds(retry_after_seconds, "retry_after_seconds")
        return self._claimed_transition(
            operation_id,
            claim_token,
            IntroDetectionOperationStatus.RETRYABLE,
            phase=phase,
            progress=progress,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )

    def schedule_continuation(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        delay_seconds: float = BATCH_CONTINUATION_DELAY_SECONDS,
    ) -> IntroDetectionTransition:
        return self.mark_retryable(
            operation_id,
            claim_token,
            phase=phase,
            progress=progress,
            error=None,
            retry_after_seconds=delay_seconds,
        )

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> IntroDetectionTransition:
        return self._claimed_transition(
            operation_id,
            claim_token,
            IntroDetectionOperationStatus.CONFIRMED,
            phase=phase,
            progress=progress,
            result=result,
        )

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> IntroDetectionTransition:
        return self._claimed_transition(
            operation_id,
            claim_token,
            IntroDetectionOperationStatus.REJECTED,
            phase=phase,
            progress=progress,
            error=error,
        )

    def mark_cancelled(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
    ) -> IntroDetectionTransition:
        return self._claimed_transition(
            operation_id,
            claim_token,
            IntroDetectionOperationStatus.CANCELLED,
            phase=phase,
            progress=progress,
            error={"type": "cancelled"},
        )

    def _claimed_transition(
        self,
        operation_id: UUID,
        claim_token: UUID,
        target: IntroDetectionOperationStatus,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> IntroDetectionTransition:
        if target not in {
            IntroDetectionOperationStatus.RETRYABLE,
            IntroDetectionOperationStatus.CONFIRMED,
            IntroDetectionOperationStatus.REJECTED,
            IntroDetectionOperationStatus.CANCELLED,
        }:
            raise ValueError("unsupported claimed transition")
        normalized_progress = _json_object(progress, "intro detection progress")
        normalized_result = _json_object(result, "intro detection result") if result is not None else None
        normalized_error = _json_object(error, "intro detection error") if error is not None else None
        _assert_no_external_url(normalized_progress, "intro detection progress")
        _assert_no_external_url(normalized_result, "intro detection result")
        _assert_no_external_url(normalized_error, "intro detection error")
        terminal = target in {
            IntroDetectionOperationStatus.CONFIRMED,
            IntroDetectionOperationStatus.REJECTED,
            IntroDetectionOperationStatus.CANCELLED,
        }
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_intro_detection_operations
                        SET status = :status, phase = :phase,
                            progress_json = CAST(:progress_json AS jsonb),
                            available_at = CASE WHEN :status = 'retryable'
                                THEN NOW() + make_interval(secs => :retry_after_seconds)
                                ELSE available_at END,
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            result_json = CAST(:result_json AS jsonb),
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = CASE WHEN :terminal THEN NOW() ELSE NULL END,
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id AND status = 'claimed'
                          AND claim_token = :claim_token AND lease_expires_at > NOW()
                          AND cancel_requested_at IS NULL
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "status": target.value,
                        "phase": phase.value,
                        "progress_json": canonical_json(normalized_progress),
                        "retry_after_seconds": retry_after_seconds,
                        "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                        "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                        "terminal": terminal,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                cancelled = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_intro_detection_operations
                            SET status = 'cancelled', claimed_by = NULL,
                                claim_token = NULL, claimed_at = NULL,
                                lease_expires_at = NULL,
                                result_json = NULL,
                                error_json = CAST(:error_json AS jsonb),
                                finished_at = NOW(), state_revision = state_revision + 1,
                                updated_at = NOW()
                            WHERE id = :operation_id AND status = 'claimed'
                              AND claim_token = :claim_token AND lease_expires_at > NOW()
                              AND cancel_requested_at IS NOT NULL
                            RETURNING *
                            """
                        ),
                        {
                            "operation_id": operation_id,
                            "claim_token": claim_token,
                            "error_json": canonical_json({"type": "cancelled"}),
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if cancelled is not None:
                    operation = _operation_projection(cancelled)
                    _complete_outbox(uow, operation.id)
                    uow.commit()
                    return IntroDetectionTransition(operation, changed=True)
                current = _operation_row(uow, operation_id)
                if current is None:
                    raise IntroDetectionNotFoundError(f"intro detection operation {operation_id} was not found")
                operation = _operation_projection(current)
                if operation.status is target and target.terminal:
                    return IntroDetectionTransition(operation, changed=False)
                if operation.status is IntroDetectionOperationStatus.CANCELLED:
                    return IntroDetectionTransition(operation, changed=False)
                raise IntroDetectionClaimLostError("intro detection operation lost its active claim")
            operation = _operation_projection(row)
            if target is IntroDetectionOperationStatus.RETRYABLE:
                _stage_outbox(uow, operation)
            else:
                _complete_outbox(uow, operation_id)
            uow.commit()
            return IntroDetectionTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def expire_stale_claims(self, *, limit: int = 100) -> int:
        _positive_int(limit, "limit")
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, claim_token, cancel_requested_at, phase, progress_json
                        FROM p115_intro_detection_operations
                        WHERE status = 'claimed' AND lease_expires_at <= NOW()
                        ORDER BY lease_expires_at, event_sequence
                        LIMIT :limit FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
            for stale in rows:
                cancelled = stale["cancel_requested_at"] is not None
                target = "cancelled" if cancelled else "retryable"
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_intro_detection_operations
                            SET status = :status, available_at = NOW(),
                                claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                                lease_expires_at = NULL, error_json = CAST(:error_json AS jsonb),
                                finished_at = CASE WHEN :status = 'cancelled' THEN NOW() ELSE NULL END,
                                state_revision = state_revision + 1, updated_at = NOW()
                            WHERE id = :operation_id AND status = 'claimed'
                              AND claim_token = :claim_token AND lease_expires_at <= NOW()
                            RETURNING *
                            """
                        ),
                        {
                            "operation_id": stale["id"],
                            "claim_token": stale["claim_token"],
                            "status": target,
                            "error_json": canonical_json({"type": "cancelled" if cancelled else "claim_lease_expired"}),
                        },
                    )
                    .mappings()
                    .one()
                )
                operation = _operation_projection(updated)
                if cancelled:
                    _complete_outbox(uow, operation.id)
                else:
                    _stage_outbox(uow, operation)
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        _positive_int(limit, "limit")
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT operation.id
                        FROM p115_intro_detection_operations AS operation
                        JOIN outbox_events AS event
                          ON event.event_key = :topic || ':' || operation.id::text
                         AND event.topic = :topic
                        WHERE event.status = 'dead'
                          AND operation.status IN ('ready','retryable')
                          AND operation.claim_token IS NULL
                        ORDER BY operation.event_sequence
                        LIMIT :limit FOR UPDATE OF operation SKIP LOCKED
                        """
                    ),
                    {"topic": INTRO_DETECTION_TOPIC, "limit": limit},
                )
                .mappings()
                .all()
            )
            for row in rows:
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_intro_detection_operations
                        SET status = 'rejected', error_json = CAST(:error_json AS jsonb),
                            finished_at = NOW(), state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :operation_id AND status IN ('ready','retryable')
                          AND claim_token IS NULL
                        """
                    ),
                    {
                        "operation_id": row["id"],
                        "error_json": canonical_json({"type": "outbox_dispatch_dead"}),
                    },
                )
                _complete_outbox(uow, UUID(str(row["id"])))
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def load_fingerprint(
        self,
        sha1: str,
        kind: IntroFingerprintKind,
        *,
        minimum_sample_seconds: int = 0,
    ) -> tuple[tuple[int, ...], int] | None:
        cache_key = fingerprint_cache_key(sha1, kind)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT fingerprint_zlib, fingerprint_count, sample_seconds
                        FROM p115_intro_fingerprint_cache
                        WHERE sha1 = :cache_key
                        """
                    ),
                    {"cache_key": cache_key},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        try:
            values = json.loads(zlib.decompress(bytes(row["fingerprint_zlib"])).decode("utf-8"))
            normalized = tuple(int(value) for value in values)
            sample_seconds = int(row["sample_seconds"])
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, zlib.error):
            return None
        if not normalized or len(normalized) != int(row["fingerprint_count"]):
            return None
        coverage = normalize_fingerprint_coverage(sample_seconds, kind)
        if coverage + FINGERPRINT_CACHE_TOLERANCE_SECONDS < int(minimum_sample_seconds or 0):
            return None
        return normalized, coverage

    def save_fingerprint(
        self,
        sha1: str,
        kind: IntroFingerprintKind,
        values: Sequence[int],
        *,
        sample_seconds: int,
    ) -> bool:
        normalized_values = tuple(int(value) for value in values)
        if not normalized_values:
            raise ValueError("fingerprint values must not be empty")
        sample = normalize_fingerprint_coverage(sample_seconds, kind)
        maximum_sample = CREDITS_SAMPLE_STEPS[-1] if kind is IntroFingerprintKind.CREDITS else INTRO_SAMPLE_STEPS[-1]
        blob = zlib.compress(canonical_json(list(normalized_values)).encode("utf-8"))
        with self._unit_of_work() as uow:
            row = uow.session.execute(
                text(
                    """
                        INSERT INTO p115_intro_fingerprint_cache (
                            sha1, fingerprint_zlib, fingerprint_count,
                            sample_seconds, updated_at
                        ) VALUES (
                            :cache_key, :fingerprint_zlib, :fingerprint_count,
                            :sample_seconds, NOW()
                        )
                        ON CONFLICT (sha1) DO UPDATE SET
                            fingerprint_zlib = EXCLUDED.fingerprint_zlib,
                            fingerprint_count = EXCLUDED.fingerprint_count,
                            sample_seconds = EXCLUDED.sample_seconds,
                            updated_at = NOW()
                        WHERE EXCLUDED.sample_seconds >= LEAST(
                            p115_intro_fingerprint_cache.sample_seconds,
                            :maximum_sample_seconds
                        )
                        RETURNING sha1
                        """
                ),
                {
                    "cache_key": fingerprint_cache_key(sha1, kind),
                    "fingerprint_zlib": blob,
                    "fingerprint_count": len(normalized_values),
                    "sample_seconds": sample,
                    "maximum_sample_seconds": maximum_sample,
                },
            ).one_or_none()
            if row is None:
                return False
            uow.commit()
            return True
        raise AssertionError("unreachable")

    def has_detection_failure(
        self,
        failure_key: str,
        kind: IntroFingerprintKind,
        *,
        algorithm_version: int,
    ) -> bool:
        with self._unit_of_work() as uow:
            found = uow.session.scalar(
                text(
                    """
                    SELECT 1 FROM p115_intro_detection_failures
                    WHERE failure_key = :failure_key AND kind = :kind
                      AND algorithm_version = :algorithm_version
                    LIMIT 1
                    """
                ),
                {
                    "failure_key": _required_text(failure_key, "failure_key", maximum=512),
                    "kind": kind.value,
                    "algorithm_version": _positive_int(algorithm_version, "algorithm_version"),
                },
            )
            return bool(found)
        raise AssertionError("unreachable")

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
    ) -> None:
        normalized_scope = "episode" if scope == "episode" else "season"
        failure_key = (
            episode_failure_key(episode.sha1)
            if normalized_scope == "episode"
            else season_failure_key(episode.series_tmdb_id, episode.season_number)
        )
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO p115_intro_detection_failures (
                        failure_key, kind, algorithm_version, scope,
                        series_tmdb_id, season_number, sha1, reason,
                        sample_count, episode_count, updated_at
                    ) VALUES (
                        :failure_key, :kind, :algorithm_version, :scope,
                        :series_tmdb_id, :season_number, :sha1, :reason,
                        :sample_count, :episode_count, NOW()
                    )
                    ON CONFLICT (failure_key, kind, algorithm_version) DO UPDATE SET
                        reason = EXCLUDED.reason, sample_count = EXCLUDED.sample_count,
                        episode_count = EXCLUDED.episode_count, updated_at = NOW()
                    """
                ),
                {
                    "failure_key": failure_key,
                    "kind": kind.value,
                    "algorithm_version": _positive_int(algorithm_version, "algorithm_version"),
                    "scope": normalized_scope,
                    "series_tmdb_id": episode.series_tmdb_id,
                    "season_number": episode.season_number,
                    "sha1": episode.sha1 if normalized_scope == "episode" else "",
                    "reason": str(reason or "")[:200],
                    "sample_count": _nonnegative_int(sample_count, "sample_count"),
                    "episode_count": _nonnegative_int(episode_count, "episode_count"),
                },
            )
            uow.commit()

    def record_fingerprint_failure(
        self,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        *,
        sample_seconds: int,
        reason: str,
        algorithm_version: int,
    ) -> int:
        steps = CREDITS_SAMPLE_STEPS if kind is IntroFingerprintKind.CREDITS else INTRO_SAMPLE_STEPS
        sample = _positive_int(sample_seconds, "sample_seconds")
        if abs(sample - steps[0]) > FINGERPRINT_CACHE_TOLERANCE_SECONDS:
            return 0
        with self._unit_of_work() as uow:
            attempts = int(
                uow.session.scalar(
                    text(
                        """
                        INSERT INTO p115_intro_fingerprint_failures (
                            failure_key, kind, algorithm_version, attempts,
                            last_sample_seconds, last_reason, series_tmdb_id,
                            season_number, sha1, updated_at
                        ) VALUES (
                            :failure_key, :kind, :algorithm_version, 1,
                            :sample_seconds, :reason, :series_tmdb_id,
                            :season_number, :sha1, NOW()
                        )
                        ON CONFLICT (failure_key, kind, algorithm_version) DO UPDATE SET
                            attempts = p115_intro_fingerprint_failures.attempts + 1,
                            last_sample_seconds = EXCLUDED.last_sample_seconds,
                            last_reason = EXCLUDED.last_reason,
                            series_tmdb_id = EXCLUDED.series_tmdb_id,
                            season_number = EXCLUDED.season_number,
                            sha1 = EXCLUDED.sha1, updated_at = NOW()
                        RETURNING attempts
                        """
                    ),
                    {
                        "failure_key": episode_failure_key(episode.sha1),
                        "kind": kind.value,
                        "algorithm_version": _positive_int(algorithm_version, "algorithm_version"),
                        "sample_seconds": sample,
                        "reason": str(reason or "")[:200],
                        "series_tmdb_id": episode.series_tmdb_id,
                        "season_number": episode.season_number,
                        "sha1": episode.sha1,
                    },
                )
                or 0
            )
            uow.commit()
            return attempts
        raise AssertionError("unreachable")

    def clear_fingerprint_failure(
        self,
        episode: IntroEpisodeSnapshot,
        kind: IntroFingerprintKind,
        *,
        algorithm_version: int,
    ) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    DELETE FROM p115_intro_fingerprint_failures
                    WHERE failure_key = :failure_key AND kind = :kind
                      AND algorithm_version = :algorithm_version
                    """
                ),
                {
                    "failure_key": episode_failure_key(episode.sha1),
                    "kind": kind.value,
                    "algorithm_version": _positive_int(algorithm_version, "algorithm_version"),
                },
            )
            uow.commit()

    def pending_episodes(
        self,
        operation: IntroDetectionOperation,
        kind: IntroFingerprintKind,
    ) -> tuple[IntroEpisodeSnapshot, ...]:
        season_key = season_failure_key(operation.series_tmdb_id, operation.season_number)
        if self.has_detection_failure(
            season_key,
            kind,
            algorithm_version=operation.algorithm_version,
        ):
            return ()
        pending: list[IntroEpisodeSnapshot] = []
        for episode in operation.episodes:
            if self.has_detection_failure(
                episode_failure_key(episode.sha1),
                kind,
                algorithm_version=operation.algorithm_version,
            ):
                continue
            if not self._cache_has_kind(episode.sha1, kind):
                pending.append(episode)
        return tuple(pending)

    def _cache_has_kind(self, sha1: str, kind: IntroFingerprintKind) -> bool:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": _canonical_sha1(sha1)},
            )
        return _mediainfo_has_marker(value, kind)

    def chapter_markers(
        self,
        episode: IntroEpisodeSnapshot,
    ) -> tuple[int | None, int | None, int | None]:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": episode.sha1},
            )
        root = _mediainfo_root(value)
        if root is None or not isinstance(root.get("Chapters"), list):
            return None, None, None
        return _marker_ticks(root["Chapters"])

    def fingerprint_audio_stream_index(self, episode: IntroEpisodeSnapshot) -> int | None:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": episode.sha1},
            )
        root = _mediainfo_root(value)
        if root is None:
            return None
        source = root.get("MediaSourceInfo")
        if not isinstance(source, Mapping):
            source = root
        streams = source.get("MediaStreams")
        if not isinstance(streams, list):
            return None
        valid = [stream for stream in streams if _valid_audio_stream(stream)]
        if not valid:
            return None
        selected = next((stream for stream in valid if stream.get("IsDefault") is True), valid[0])
        return int(selected["Index"])

    def list_season_snapshots(
        self,
        series_tmdb_id: str,
        season_number: int,
    ) -> tuple[IntroEpisodeSnapshot, ...]:
        series = _required_text(series_tmdb_id, "series_tmdb_id", maximum=255)
        season = _nonnegative_int(season_number, "season_number")
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT e.title, e.parent_series_tmdb_id, e.season_number,
                               e.episode_number, e.emby_item_ids_json,
                               e.file_sha1_json, e.file_pickcode_json,
                               e.asset_details_json, p.title AS series_title
                        FROM media_metadata AS e
                        LEFT JOIN media_metadata AS p
                          ON p.tmdb_id = e.parent_series_tmdb_id
                         AND p.item_type = 'Series'
                        WHERE e.item_type = 'Episode'
                          AND e.parent_series_tmdb_id = :series_tmdb_id
                          AND e.season_number = :season_number
                          AND e.in_library = TRUE
                        ORDER BY e.episode_number, e.tmdb_id
                        """
                    ),
                    {"series_tmdb_id": series, "season_number": season},
                )
                .mappings()
                .all()
            )
            snapshots: list[IntroEpisodeSnapshot] = []
            seen_sha1: set[str] = set()
            for row in rows:
                identity = _first_aligned_identity(dict(row))
                if identity is None:
                    continue
                item_id, sha1, pick_code, library_id = identity
                if sha1 in seen_sha1:
                    continue
                cache = (
                    uow.session.execute(
                        text(
                            """
                            SELECT mediainfo_json, cache_revision
                            FROM p115_mediainfo_cache WHERE sha1 = :sha1
                            """
                        ),
                        {"sha1": sha1},
                    )
                    .mappings()
                    .one_or_none()
                )
                snapshots.append(
                    IntroEpisodeSnapshot.create(
                        item_id=item_id,
                        series_tmdb_id=series,
                        season_number=season,
                        episode_number=row["episode_number"],
                        sha1=sha1,
                        pick_code=pick_code,
                        source_library_id=library_id,
                        runtime_seconds=_runtime_seconds(cache["mediainfo_json"] if cache else None),
                        cache_revision=int(cache["cache_revision"] if cache else 0),
                        title=row.get("title") or "",
                        series_title=row.get("series_title") or series,
                    )
                )
                seen_sha1.add(sha1)
            return tuple(snapshots)
        raise AssertionError("unreachable")

    def stage_chapters(
        self,
        operation_id: UUID,
        claim_token: UUID,
        projections: Sequence[IntroChapterProjection],
        *,
        authorization_revision: str,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
    ) -> tuple[UUID, ...]:
        if not projections or len(projections) > 3:
            raise ValueError("chapter staging requires one to three episode projections")
        authorization = _digest(authorization_revision, "Emby authorization revision")
        normalized_progress = _json_object(progress, "intro detection progress")
        _assert_no_external_url(normalized_progress, "intro detection progress")
        with self._unit_of_work() as uow:
            operation_row = _require_live_claim(uow, operation_id, claim_token)
            operation = _operation_projection(operation_row)
            episode_by_sha1 = {episode.sha1: episode for episode in operation.episodes}
            effect_ids: list[UUID] = []
            for projection in projections:
                expected_episode = episode_by_sha1.get(projection.episode.sha1)
                if expected_episode != projection.episode:
                    raise IntroDetectionStateError("chapter projection is outside the immutable season snapshot")
                _lock_media_binding(uow, projection.episode)
                cache = (
                    uow.session.execute(
                        text(
                            """
                            SELECT mediainfo_json, cache_revision
                            FROM p115_mediainfo_cache
                            WHERE sha1 = :sha1 FOR UPDATE
                            """
                        ),
                        {"sha1": projection.episode.sha1},
                    )
                    .mappings()
                    .one_or_none()
                )
                if cache is None or cache["mediainfo_json"] is None:
                    raise IntroDetectionStateError("chapter projection has no MediaInfo cache")
                merged, marker_values, changed = _merge_projection(cache["mediainfo_json"], projection)
                if changed:
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_mediainfo_cache
                            SET mediainfo_json = CAST(:mediainfo_json AS jsonb), updated_at = NOW()
                            WHERE sha1 = :sha1 AND cache_revision = :cache_revision
                            """
                        ),
                        {
                            "sha1": projection.episode.sha1,
                            "cache_revision": int(cache["cache_revision"]),
                            "mediainfo_json": canonical_json(merged),
                        },
                    )
                payload = {
                    "item_id": projection.episode.item_id,
                    "sha1": projection.episode.sha1,
                    "emby_authorization_revision": authorization,
                    "mediainfo": merged,
                    "intro_start_ticks": marker_values[0],
                    "intro_end_ticks": marker_values[1],
                    "credits_start_ticks": marker_values[2],
                    "clear_intro": False,
                    "clear_credits": False,
                    "intro_detection_operation_id": str(operation.id),
                }
                effect_key = _chapter_effect_key(operation, projection.episode, payload)
                reservation = SqlAlchemyMediaEffectRepository(uow.session).reserve(
                    effect_key=effect_key,
                    effect_type="emby.chapters.update",
                    aggregate_type="media_asset",
                    aggregate_id=f"{projection.episode.item_id}:{projection.episode.sha1}",
                    payload=payload,
                )
                effect_ids.append(reservation.effect.id)
            persisted_progress = {
                **normalized_progress,
                "effect_ids": sorted({*map(str, effect_ids), *map(str, normalized_progress.get("effect_ids", []))}),
            }
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_intro_detection_operations
                    SET phase = :phase, progress_json = CAST(:progress_json AS jsonb),
                        state_revision = state_revision + 1, updated_at = NOW()
                    WHERE id = :operation_id AND status = 'claimed'
                      AND claim_token = :claim_token AND lease_expires_at > NOW()
                      AND cancel_requested_at IS NULL
                    """
                ),
                {
                    "operation_id": operation_id,
                    "claim_token": claim_token,
                    "phase": phase.value,
                    "progress_json": canonical_json(persisted_progress),
                },
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                raise IntroDetectionClaimLostError("chapter projection lost the active operation claim")
            uow.commit()
            return tuple(effect_ids)
        raise AssertionError("unreachable")


def fingerprint_cache_key(sha1: str, kind: IntroFingerprintKind) -> str:
    normalized = _canonical_sha1(sha1)
    return f"{normalized}:credits" if kind is IntroFingerprintKind.CREDITS else normalized


def normalize_fingerprint_coverage(sample_seconds: int, kind: IntroFingerprintKind) -> int:
    sample = _positive_int(sample_seconds, "sample_seconds")
    maximum = CREDITS_SAMPLE_STEPS[-1] if kind is IntroFingerprintKind.CREDITS else INTRO_SAMPLE_STEPS[-1]
    return min(sample, maximum)


def season_failure_key(series_tmdb_id: str, season_number: int) -> str:
    series = _required_text(series_tmdb_id, "series_tmdb_id", maximum=255)
    season = _nonnegative_int(season_number, "season_number")
    return f"season:{series}:{season}"


def episode_failure_key(sha1: str) -> str:
    return f"episode:{_canonical_sha1(sha1)}"


def _validate_intent(intent: IntroDetectionIntent) -> None:
    if hashlib.sha256(canonical_json(intent.request).encode("utf-8")).digest() != intent.request_hash:
        raise ValueError("request_hash does not match the canonical intro detection request")
    if str(intent.request.get("source_set_revision") or "") != intent.source_set_revision.hex():
        raise ValueError("source_set_revision does not match the intro detection request")
    if str(intent.request.get("policy_revision") or "") != intent.policy_revision.hex():
        raise ValueError("policy_revision does not match the intro detection request")
    _digest(intent.operation_key, "operation_key")


def _assert_reservation_matches(operation: IntroDetectionOperation, intent: IntroDetectionIntent) -> None:
    if operation.request_hash != intent.request_hash:
        raise IntroDetectionConflictError("intro operation_key was reused with another request")
    if (
        operation.source_set_revision != intent.source_set_revision
        or operation.policy_revision != intent.policy_revision
        or operation.series_tmdb_id != intent.series_tmdb_id
        or operation.season_number != intent.season_number
        or operation.algorithm_version != intent.algorithm_version
        or operation.account_id != intent.account_id
        or operation.trigger_mode is not intent.trigger_mode
    ):
        raise IntroDetectionConflictError("intro operation identity changed")


def _operation_projection(row: RowMapping) -> IntroDetectionOperation:
    return IntroDetectionOperation(
        id=UUID(str(row["id"])),
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        source_set_revision=bytes(row["source_set_revision"]),
        policy_revision=bytes(row["policy_revision"]),
        series_tmdb_id=str(row["series_tmdb_id"]),
        season_number=int(row["season_number"]),
        algorithm_version=int(row["algorithm_version"]),
        account_id=str(row["account_id"]),
        trigger_mode=IntroTriggerMode(str(row["trigger_mode"])),
        request=dict(row["request_json"] or {}),
        status=IntroDetectionOperationStatus(str(row["status"])),
        phase=IntroDetectionPhase(str(row["phase"])),
        state_revision=int(row["state_revision"]),
        progress=dict(row["progress_json"] or {}),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=UUID(str(row["claim_token"])) if row["claim_token"] is not None else None,
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        cancel_requested_at=row["cancel_requested_at"],
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error=dict(row["error_json"]) if row["error_json"] is not None else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _operation_row(
    uow: SqlAlchemyUnitOfWork,
    operation_id_value: UUID,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text(f"SELECT * FROM p115_intro_detection_operations WHERE id = :operation_id{suffix}"),
            {"operation_id": operation_id_value},
        )
        .mappings()
        .one_or_none()
    )


def _operation_by_key(
    uow: SqlAlchemyUnitOfWork,
    operation_key: str,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text(f"SELECT * FROM p115_intro_detection_operations WHERE operation_key = :operation_key{suffix}"),
            {"operation_key": operation_key},
        )
        .mappings()
        .one_or_none()
    )


def _require_live_claim(
    uow: SqlAlchemyUnitOfWork,
    operation_id_value: UUID,
    claim_token: UUID,
) -> RowMapping:
    row = (
        uow.session.execute(
            text(
                """
                SELECT *, lease_expires_at > NOW() AS lease_active
                FROM p115_intro_detection_operations
                WHERE id = :operation_id
                FOR UPDATE
                """
            ),
            {"operation_id": operation_id_value},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise IntroDetectionClaimLostError("intro detection operation lost its active claim")
    if row["cancel_requested_at"] is not None:
        raise IntroDetectionCancelled("intro detection operation cancellation was requested")
    if (
        str(row["status"]) != IntroDetectionOperationStatus.CLAIMED.value
        or row["claim_token"] != claim_token
        or not bool(row["lease_active"])
    ):
        raise IntroDetectionClaimLostError("intro detection operation lost its active claim")
    return row


def _stage_outbox(uow: SqlAlchemyUnitOfWork, operation: IntroDetectionOperation) -> None:
    payload = {
        "operation_id": str(operation.id),
        "sequence": operation.sequence,
        "operation_key": operation.operation_key,
        "request_hash": operation.request_hash.hex(),
    }
    uow.session.execute(
        text(
            """
            INSERT INTO outbox_events (
                id, event_key, topic, aggregate_type, aggregate_id,
                payload_version, payload_json, status, available_at
            ) VALUES (
                :id, :event_key, :topic, :aggregate_type, :aggregate_id,
                1, CAST(:payload_json AS jsonb), 'pending', :available_at
            )
            ON CONFLICT (event_key) DO UPDATE SET
                status = 'pending', available_at = EXCLUDED.available_at,
                published_at = NULL, processed_at = NULL,
                claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                last_error = NULL, updated_at = NOW()
            WHERE outbox_events.status <> 'dead'
            """
        ),
        {
            "id": uuid4(),
            "event_key": _outbox_key(operation.id),
            "topic": INTRO_DETECTION_TOPIC,
            "aggregate_type": INTRO_DETECTION_AGGREGATE_TYPE,
            "aggregate_id": str(operation.id),
            "payload_json": canonical_json(payload),
            "available_at": operation.available_at,
        },
    )


def _complete_outbox(uow: SqlAlchemyUnitOfWork, operation_id_value: UUID) -> None:
    uow.session.execute(
        text(
            """
            UPDATE outbox_events
            SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                updated_at = NOW()
            WHERE event_key = :event_key AND topic = :topic
              AND status IN ('pending','publishing','published','dead')
            """
        ),
        {"event_key": _outbox_key(operation_id_value), "topic": INTRO_DETECTION_TOPIC},
    )


def _outbox_key(operation_id_value: UUID) -> str:
    return f"{INTRO_DETECTION_TOPIC}:{operation_id_value}"


def _advisory_lock(uow: SqlAlchemyUnitOfWork, key: str) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": key},
    )


def _lock_media_binding(uow: SqlAlchemyUnitOfWork, episode: IntroEpisodeSnapshot) -> None:
    rows = (
        uow.session.execute(
            text(
                """
                SELECT emby_item_ids_json, file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE item_type = 'Episode'
                  AND emby_item_ids_json @> CAST(:item_ids AS jsonb)
                FOR UPDATE
                """
            ),
            {"item_ids": canonical_json([episode.item_id])},
        )
        .mappings()
        .all()
    )
    if len(rows) != 1:
        raise IntroDetectionStateError("Emby episode binding changed before chapter staging")
    row = rows[0]
    item_ids = _json_list(row.get("emby_item_ids_json"))
    sha1s = [_optional_canonical_sha1(value) for value in _json_list(row.get("file_sha1_json"))]
    pick_codes = [str(value or "").strip() for value in _json_list(row.get("file_pickcode_json"))]
    if len({len(item_ids), len(sha1s), len(pick_codes)}) != 1:
        raise IntroDetectionStateError("episode asset identity arrays are not aligned")
    matches = [
        index
        for index, item_id in enumerate(item_ids)
        if str(item_id or "").strip() == episode.item_id
        and sha1s[index] == episode.sha1
        and pick_codes[index] == episode.pick_code
    ]
    if len(matches) != 1:
        raise IntroDetectionStateError("episode identity changed before chapter staging")


def _merge_projection(
    mediainfo: object,
    projection: IntroChapterProjection,
) -> tuple[object, tuple[int | None, int | None, int | None], bool]:
    cloned = deepcopy(mediainfo)
    root = _mediainfo_root(cloned)
    if root is None:
        raise IntroDetectionStateError("MediaInfo cache has an invalid root")
    existing = root.get("Chapters")
    chapters = list(existing) if isinstance(existing, list) else []
    intro_start, intro_end, credits_start = _marker_ticks(chapters)
    if projection.intro_start_ticks is not None:
        intro_start = projection.intro_start_ticks
        intro_end = projection.intro_end_ticks
    if projection.credits_start_ticks is not None:
        credits_start = projection.credits_start_ticks
    marker_types = {"IntroStart", "IntroEnd", "CreditsStart"}
    kept = [
        deepcopy(chapter)
        for chapter in chapters
        if not (isinstance(chapter, Mapping) and str(chapter.get("MarkerType") or "") in marker_types)
    ]
    additions: list[dict[str, Any]] = []
    if intro_start is not None and intro_end is not None:
        additions.extend(
            [
                {
                    "StartPositionTicks": intro_start,
                    "Name": "片头",
                    "MarkerType": "IntroStart",
                    "ChapterIndex": len(kept),
                },
                {
                    "StartPositionTicks": intro_end,
                    "Name": "片头结束",
                    "MarkerType": "IntroEnd",
                    "ChapterIndex": len(kept) + 1,
                },
            ]
        )
    if credits_start is not None:
        additions.append(
            {
                "StartPositionTicks": credits_start,
                "Name": "片尾",
                "MarkerType": "CreditsStart",
                "ChapterIndex": len(kept) + len(additions),
            }
        )
    merged_chapters = kept + additions
    changed = merged_chapters != chapters
    root["Chapters"] = merged_chapters
    return cloned, (intro_start, intro_end, credits_start), changed


def _marker_ticks(chapters: Sequence[object]) -> tuple[int | None, int | None, int | None]:
    intro_start = intro_end = credits_start = None
    for chapter in chapters:
        if not isinstance(chapter, Mapping):
            continue
        try:
            ticks = int(str(chapter.get("StartPositionTicks")))
        except (TypeError, ValueError):
            continue
        marker = str(chapter.get("MarkerType") or "")
        if marker == "IntroStart":
            intro_start = ticks
        elif marker == "IntroEnd":
            intro_end = ticks
        elif marker == "CreditsStart":
            credits_start = ticks
    if intro_start is None or intro_end is None or intro_end <= intro_start:
        intro_start = intro_end = None
    return intro_start, intro_end, credits_start


def _chapter_effect_key(
    operation: IntroDetectionOperation,
    episode: IntroEpisodeSnapshot,
    payload: Mapping[str, Any],
) -> str:
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"p6:intro:emby.chapters.update:{operation.id}:{episode.item_id}:{episode.sha1}:{digest}"


def _first_aligned_identity(row: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    item_ids = [str(value or "").strip() for value in _json_list(row.get("emby_item_ids_json"))]
    sha1s = [_optional_canonical_sha1(value) for value in _json_list(row.get("file_sha1_json"))]
    pick_codes = [str(value or "").strip() for value in _json_list(row.get("file_pickcode_json"))]
    assets = _json_list(row.get("asset_details_json"))
    for index in range(min(len(item_ids), len(sha1s), len(pick_codes))):
        if not item_ids[index] or not sha1s[index] or not pick_codes[index]:
            continue
        asset = assets[index] if index < len(assets) and isinstance(assets[index], Mapping) else {}
        library_id = str(asset.get("source_library_id") or "").strip()
        return item_ids[index], str(sha1s[index]), pick_codes[index], library_id
    return None


def _runtime_seconds(mediainfo: object) -> float:
    root = _mediainfo_root(mediainfo)
    if root is None:
        return 0
    source = root.get("MediaSourceInfo")
    if not isinstance(source, Mapping):
        source = root
    try:
        ticks = int(source.get("RunTimeTicks") or root.get("RunTimeTicks") or 0)
    except (TypeError, ValueError):
        return 0
    return ticks / 10_000_000 if ticks > 0 else 0


def _mediainfo_has_marker(mediainfo: object, kind: IntroFingerprintKind) -> bool:
    root = _mediainfo_root(mediainfo)
    if root is None:
        return False
    chapters = root.get("Chapters")
    if not isinstance(chapters, list):
        return False
    intro_start, intro_end, credits_start = _marker_ticks(chapters)
    if kind is IntroFingerprintKind.CREDITS:
        return credits_start is not None
    return intro_start is not None and intro_end is not None


def _valid_audio_stream(stream: object) -> bool:
    if not isinstance(stream, Mapping):
        return False
    if str(stream.get("Type") or "").strip().lower() != "audio":
        return False
    codec = str(stream.get("Codec") or "").strip().lower()
    if not codec or codec in {"unknown", "none", "null", "data"} or codec.isdigit():
        return False
    try:
        return int(str(stream.get("Index"))) >= 0
    except (TypeError, ValueError):
        return False


def _mediainfo_root(mediainfo: object) -> dict[str, Any] | None:
    if isinstance(mediainfo, dict):
        return mediainfo
    if isinstance(mediainfo, list) and mediainfo and isinstance(mediainfo[0], dict):
        return mediainfo[0]
    return None


def _json_list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _json_object(value: Mapping[str, Any] | None, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    normalized = json.loads(canonical_json(dict(value)))
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must be an object")
    return normalized


def _assert_no_external_url(value: object, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, str):
        lowered = value.lower()
        if "http://" in lowered or "https://" in lowered:
            raise ValueError(f"{field_name} must not persist an external URL")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).strip().lower() in {"direct_url", "signed_url", "download_url"}:
                raise ValueError(f"{field_name} must not persist a direct URL")
            _assert_no_external_url(item, field_name)
        return
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        for item in value:
            _assert_no_external_url(item, field_name)


def _canonical_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 40:
        raise ValueError("sha1 must be 40 hexadecimal characters")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("sha1 must be 40 hexadecimal characters") from exc
    return normalized


def _optional_canonical_sha1(value: object) -> str | None:
    try:
        return _canonical_sha1(value)
    except ValueError:
        return None


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


def _positive_seconds(value: float, field_name: str) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _nonnegative_seconds(value: float, field_name: str) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")


__all__ = [
    "PostgresP115IntroDetectionRepository",
    "episode_failure_key",
    "fingerprint_cache_key",
    "normalize_fingerprint_coverage",
    "season_failure_key",
]
