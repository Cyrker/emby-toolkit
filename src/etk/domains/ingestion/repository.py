from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from etk.platform.jobs.json_payload import canonical_json, normalize_json_object
from etk.platform.jobs.types import UnitOfWorkFactory

from .monitoring import (
    INGESTION_MONITOR_TOPIC,
    BindingReservationResult,
    BindingStatus,
    BindingTransitionResult,
    FileObservation,
    IngestionMonitorConflictError,
    IngestionMonitorNotFoundError,
    IngestionMonitorStateError,
    MonitorClassification,
    MonitorEventType,
    MonitorIntent,
    MonitorItemProjection,
    MonitorItemStatus,
    MonitorSource,
    PendingBindingProjection,
    ReservationResult,
    StabilityOutcome,
    TransitionResult,
    evaluate_stability,
    normalize_media_path,
    normalize_observed_path,
)

_COALESCIBLE_STATUSES = (
    MonitorItemStatus.PENDING.value,
    MonitorItemStatus.STABILITY_WAIT.value,
    MonitorItemStatus.READY.value,
    MonitorItemStatus.RETRYABLE.value,
    MonitorItemStatus.SUSPENDED.value,
)
_MONITOR_TERMINAL_STATUSES = frozenset(
    {
        MonitorItemStatus.CONFIRMED,
        MonitorItemStatus.REJECTED,
        MonitorItemStatus.INDETERMINATE,
        MonitorItemStatus.SUPERSEDED,
    }
)
_BINDING_TERMINAL_STATUSES = frozenset({BindingStatus.CONFIRMED, BindingStatus.REJECTED, BindingStatus.INDETERMINATE})

T = TypeVar("T")


class SqlAlchemyIngestionRepository:
    """Session-bound ingestion primitives for atomic monitor, binding, and Outbox updates."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def reserve_intents(self, intents: Sequence[MonitorIntent]) -> tuple[ReservationResult, ...]:
        lock_keys = sorted(
            {f"producer:{intent.producer_key}" for intent in intents}
            | {f"path:{intent.path_key}" for intent in intents}
        )
        for lock_key in lock_keys:
            self.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
                {"lock_key": lock_key},
            )
        results = tuple(self._reserve_intent(intent) for intent in intents)
        return tuple(
            ReservationResult(
                item=self.item(result.item.id) or result.item,
                created=result.created,
                superseded_ids=result.superseded_ids,
            )
            for result in results
        )

    def _reserve_intent(self, intent: MonitorIntent) -> ReservationResult:
        existing = self._item_by_producer_key(intent.producer_key, for_update=True)
        if existing is not None:
            projection = _item_projection(existing)
            if projection.request_hash != intent.request_hash:
                raise IngestionMonitorConflictError("producer_key was already used with a different monitor intent")
            return ReservationResult(projection, created=False)
        rows = (
            self.session.execute(
                text(
                    """
                    SELECT *
                    FROM ingestion_monitor_items
                    WHERE path_key = :path_key
                      AND status = ANY(CAST(:statuses AS text[]))
                    ORDER BY event_sequence, id
                    FOR UPDATE
                    """
                ),
                {"path_key": intent.path_key, "statuses": list(_COALESCIBLE_STATUSES)},
            )
            .mappings()
            .all()
        )
        superseded_ids = tuple(UUID(str(row["id"])) for row in rows)
        if superseded_ids:
            self.session.execute(
                text(
                    """
                    UPDATE ingestion_monitor_items
                    SET status = 'superseded',
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        result_json = NULL,
                        error_json = '{"type":"coalesced_by_newer_event"}'::jsonb,
                        finished_at = NOW(), updated_at = NOW()
                    WHERE id = ANY(CAST(:ids AS uuid[]))
                    """
                ),
                {"ids": list(superseded_ids)},
            )
            for item_id in superseded_ids:
                self._cancel_outbox(item_id)

        item_id = uuid4()
        row = (
            self.session.execute(
                text(
                    """
                    INSERT INTO ingestion_monitor_items (
                        id, producer_key, request_hash, source, event_type,
                        path, path_key, related_path, correlation_id, monitor_root,
                        config_revision, refresh_delay_seconds, classification, status, available_at,
                        stability_deadline
                    ) VALUES (
                        :id, :producer_key, :request_hash, :source, :event_type,
                        :path, :path_key, :related_path, :correlation_id, :monitor_root,
                        :config_revision, :refresh_delay_seconds, :classification, :status, :available_at,
                        :stability_deadline
                    )
                    RETURNING *
                    """
                ),
                {
                    "id": item_id,
                    "producer_key": intent.producer_key,
                    "request_hash": intent.request_hash,
                    "source": intent.source.value,
                    "event_type": intent.event_type.value,
                    "path": intent.path,
                    "path_key": intent.path_key,
                    "related_path": intent.related_path,
                    "correlation_id": intent.correlation_id,
                    "monitor_root": intent.monitor_root,
                    "config_revision": intent.config_revision,
                    "refresh_delay_seconds": intent.refresh_delay_seconds,
                    "classification": intent.classification.value,
                    "status": (
                        MonitorItemStatus.SUSPENDED.value if intent.suspended else MonitorItemStatus.PENDING.value
                    ),
                    "available_at": intent.available_at,
                    "stability_deadline": intent.stability_deadline,
                },
            )
            .mappings()
            .one()
        )
        return ReservationResult(_item_projection(row), created=True, superseded_ids=superseded_ids)

    def item(self, item_id: UUID, *, for_update: bool = False) -> MonitorItemProjection | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = (
            self.session.execute(
                text(f"SELECT * FROM ingestion_monitor_items WHERE id = :item_id{suffix}"),
                {"item_id": item_id},
            )
            .mappings()
            .one_or_none()
        )
        return _item_projection(row) if row is not None else None

    def due_for_stability(self, *, limit: int = 100) -> tuple[MonitorItemProjection, ...]:
        _positive_integer(limit, "limit")
        rows = (
            self.session.execute(
                text(
                    """
                    SELECT item.*
                    FROM ingestion_monitor_items AS item
                    WHERE item.status IN ('pending', 'stability_wait')
                      AND item.available_at <= NOW()
                      AND NOT EXISTS (
                          SELECT 1
                          FROM ingestion_monitor_items AS claimed
                          WHERE claimed.path_key = item.path_key
                            AND claimed.id <> item.id
                            AND claimed.status = 'claimed'
                      )
                    ORDER BY item.available_at, item.event_sequence, item.id
                    LIMIT :limit
                    FOR UPDATE OF item SKIP LOCKED
                    """
                ),
                {"limit": limit},
            )
            .mappings()
            .all()
        )
        return tuple(_item_projection(row) for row in rows)

    def record_stability_sample(
        self,
        item_id: UUID,
        observation: FileObservation,
        *,
        observed_at: datetime | None = None,
    ) -> tuple[MonitorItemProjection, StabilityOutcome]:
        now = _aware(observed_at or datetime.now(UTC), "observed_at")
        current = self.item(item_id, for_update=True)
        if current is None:
            raise IngestionMonitorNotFoundError(f"monitor item {item_id} was not found")
        if current.status is MonitorItemStatus.READY:
            return current, StabilityOutcome.READY
        if current.status not in {MonitorItemStatus.PENDING, MonitorItemStatus.STABILITY_WAIT}:
            raise IngestionMonitorStateError(
                f"monitor item {item_id} is {current.status.value}, not waiting for stability"
            )
        if now < current.available_at:
            return current, StabilityOutcome.WAITING

        decision = evaluate_stability(current, observation, observed_at=now)
        if decision.outcome is StabilityOutcome.READY:
            row = self._update_item(
                item_id,
                """
                status = 'ready', available_at = :available_at,
                last_size = :last_size, last_mtime_ns = :last_mtime_ns,
                stable_matches = :stable_matches, error_json = NULL,
                updated_at = NOW()
                """,
                {
                    "available_at": now
                    + timedelta(
                        seconds=(
                            current.refresh_delay_seconds
                            if current.classification is MonitorClassification.REFRESH_ONLY
                            else 0
                        )
                    ),
                    "last_size": decision.last_size,
                    "last_mtime_ns": decision.last_mtime_ns,
                    "stable_matches": decision.stable_matches,
                },
            )
            projection = _item_projection(row)
            self._stage_outbox(projection)
            return projection, decision.outcome

        if decision.outcome is StabilityOutcome.TIMED_OUT:
            row = self._update_item(
                item_id,
                """
                status = 'rejected', last_size = :last_size,
                last_mtime_ns = :last_mtime_ns, stable_matches = :stable_matches,
                error_json = CAST(:error_json AS jsonb), finished_at = NOW(), updated_at = NOW()
                """,
                {
                    "last_size": decision.last_size,
                    "last_mtime_ns": decision.last_mtime_ns,
                    "stable_matches": decision.stable_matches,
                    "error_json": canonical_json(
                        {"type": "stability_timeout", "message": "file did not stabilize before its deadline"}
                    ),
                },
            )
            self._complete_outbox(item_id)
            return _item_projection(row), decision.outcome

        error_type = "file_missing" if decision.outcome is StabilityOutcome.MISSING else None
        row = self._update_item(
            item_id,
            """
            status = 'stability_wait', available_at = :available_at,
            last_size = :last_size, last_mtime_ns = :last_mtime_ns,
            stable_matches = :stable_matches,
            error_json = CAST(:error_json AS jsonb), updated_at = NOW()
            """,
            {
                "available_at": now + timedelta(seconds=1),
                "last_size": decision.last_size,
                "last_mtime_ns": decision.last_mtime_ns,
                "stable_matches": decision.stable_matches,
                "error_json": canonical_json({"type": error_type}) if error_type else None,
            },
        )
        return _item_projection(row), decision.outcome

    def mark_ready(self, item_id: UUID, *, available_at: datetime | None = None) -> TransitionResult:
        normalized_available_at = _aware(available_at, "available_at") if available_at is not None else None
        current = self.item(item_id, for_update=True)
        if current is None:
            raise IngestionMonitorNotFoundError(f"monitor item {item_id} was not found")
        if current.status is MonitorItemStatus.READY:
            return TransitionResult(current, changed=False)
        if current.status not in {
            MonitorItemStatus.PENDING,
            MonitorItemStatus.STABILITY_WAIT,
            MonitorItemStatus.RETRYABLE,
        }:
            raise IngestionMonitorStateError(f"monitor item {item_id} cannot become ready from {current.status.value}")
        row = self._update_item(
            item_id,
            "status = 'ready', available_at = COALESCE(:available_at, NOW()), error_json = NULL, updated_at = NOW()",
            {"available_at": normalized_available_at},
        )
        projection = _item_projection(row)
        self._stage_outbox(projection)
        return TransitionResult(projection, changed=True)

    def claim_by_id(
        self,
        item_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> MonitorItemProjection | None:
        worker = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        claim_token = uuid4()
        row = (
            self.session.execute(
                text(
                    """
                    UPDATE ingestion_monitor_items AS item
                    SET status = 'claimed', claimed_by = :worker_id,
                        claim_token = :claim_token, claimed_at = NOW(),
                        lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                        attempts = attempts + 1, updated_at = NOW()
                    WHERE item.id = :item_id
                      AND item.status IN ('ready', 'retryable')
                      AND item.available_at <= NOW()
                      AND NOT EXISTS (
                          SELECT 1
                          FROM ingestion_monitor_items AS claimed
                          WHERE claimed.path_key = item.path_key
                            AND claimed.id <> item.id
                            AND claimed.status = 'claimed'
                      )
                    RETURNING item.*
                    """
                ),
                {
                    "item_id": item_id,
                    "worker_id": worker,
                    "claim_token": claim_token,
                    "lease_seconds": lease_seconds,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        self._complete_outbox(item_id)
        return _item_projection(row)

    def claim(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
        limit: int = 1,
    ) -> tuple[MonitorItemProjection, ...]:
        worker = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        _positive_integer(limit, "limit")
        claim_token = uuid4()
        rows = (
            self.session.execute(
                text(
                    """
                    WITH candidates AS (
                        SELECT item.id
                        FROM ingestion_monitor_items AS item
                        WHERE item.status IN ('ready', 'retryable')
                          AND item.available_at <= NOW()
                          AND NOT EXISTS (
                              SELECT 1
                              FROM ingestion_monitor_items AS claimed
                              WHERE claimed.path_key = item.path_key
                                AND claimed.id <> item.id
                                AND claimed.status = 'claimed'
                          )
                        ORDER BY item.available_at, item.event_sequence, item.id
                        LIMIT :limit
                        FOR UPDATE OF item SKIP LOCKED
                    )
                    UPDATE ingestion_monitor_items AS item
                    SET status = 'claimed', claimed_by = :worker_id,
                        claim_token = :claim_token, claimed_at = NOW(),
                        lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                        attempts = attempts + 1, updated_at = NOW()
                    FROM candidates
                    WHERE item.id = candidates.id
                    RETURNING item.*
                    """
                ),
                {
                    "worker_id": worker,
                    "claim_token": claim_token,
                    "lease_seconds": lease_seconds,
                    "limit": limit,
                },
            )
            .mappings()
            .all()
        )
        projections = tuple(sorted((_item_projection(row) for row in rows), key=lambda item: item.event_sequence))
        for projection in projections:
            self._complete_outbox(projection.id)
        return projections

    def mark_confirmed(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        return self._transition_item(item_id, claim_token, MonitorItemStatus.CONFIRMED, result=result)

    def mark_rejected(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        return self._transition_item(item_id, claim_token, MonitorItemStatus.REJECTED, error=error)

    def mark_indeterminate(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        return self._transition_item(item_id, claim_token, MonitorItemStatus.INDETERMINATE, error=error)

    def mark_retryable(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> TransitionResult:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must not be negative")
        return self._transition_item(
            item_id,
            claim_token,
            MonitorItemStatus.RETRYABLE,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )

    def _transition_item(
        self,
        item_id: UUID,
        claim_token: UUID,
        status: MonitorItemStatus,
        *,
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> TransitionResult:
        normalized_result = _json(result, "monitor result")
        normalized_error = _json(error, "monitor error")
        terminal = status in _MONITOR_TERMINAL_STATUSES
        if status is MonitorItemStatus.RETRYABLE:
            current_claim = self.item(item_id, for_update=True)
            if (
                current_claim is not None
                and current_claim.status is MonitorItemStatus.CLAIMED
                and current_claim.claim_token == claim_token
                and self._has_unclaimed_successor(current_claim)
            ):
                row = self._update_item(
                    item_id,
                    """
                    status = 'superseded',
                    claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_expires_at = NULL,
                    result_json = NULL,
                    error_json = '{"type":"retry_superseded_by_newer_event"}'::jsonb,
                    finished_at = NOW(), updated_at = NOW()
                    """,
                    {},
                )
                self._complete_outbox(item_id)
                return TransitionResult(_item_projection(row), changed=True)
        row = (
            self.session.execute(
                text(
                    """
                    UPDATE ingestion_monitor_items
                    SET status = :status,
                        available_at = CASE WHEN :status = 'retryable'
                            THEN NOW() + make_interval(secs => :retry_after_seconds)
                            ELSE available_at END,
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        result_json = CAST(:result_json AS jsonb),
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = CASE WHEN :terminal THEN NOW() ELSE NULL END,
                        updated_at = NOW()
                    WHERE id = :item_id AND status = 'claimed' AND claim_token = :claim_token
                    RETURNING *
                    """
                ),
                {
                    "item_id": item_id,
                    "claim_token": claim_token,
                    "status": status.value,
                    "retry_after_seconds": retry_after_seconds,
                    "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                    "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                    "terminal": terminal,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            projection = _item_projection(row)
            if status is MonitorItemStatus.RETRYABLE:
                self._stage_outbox(projection)
            else:
                self._complete_outbox(item_id)
            return TransitionResult(projection, changed=True)

        current = self.item(item_id)
        if current is None:
            raise IngestionMonitorNotFoundError(f"monitor item {item_id} was not found")
        if not terminal or current.status is not status:
            raise IngestionMonitorStateError(
                f"monitor item {item_id} is {current.status.value}, not a claim owned by this token"
            )
        if current.result != normalized_result or current.error != normalized_error:
            raise IngestionMonitorConflictError("duplicate monitor terminal transition changed its payload")
        return TransitionResult(current, changed=False)

    def expire_stale_claims(self, *, limit: int = 100, retry_delay_seconds: float = 0) -> int:
        _positive_integer(limit, "limit")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        rows = (
            self.session.execute(
                text(
                    """
                    SELECT *
                    FROM ingestion_monitor_items
                    WHERE status = 'claimed' AND lease_expires_at <= NOW()
                    ORDER BY lease_expires_at, event_sequence, id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                    """
                ),
                {"limit": limit},
            )
            .mappings()
            .all()
        )
        for row in rows:
            projection = _item_projection(row)
            assert projection.claim_token is not None
            self._transition_item(
                projection.id,
                projection.claim_token,
                MonitorItemStatus.RETRYABLE,
                error={"type": "claim_lease_expired"},
                retry_after_seconds=retry_delay_seconds,
            )
        return len(rows)

    def suspend_active(self) -> int:
        rows = self.session.execute(
            text(
                """
                UPDATE ingestion_monitor_items
                SET status = 'suspended', updated_at = NOW()
                WHERE status IN ('pending', 'stability_wait', 'ready', 'retryable')
                RETURNING id
                """
            )
        ).all()
        for row in rows:
            self._cancel_outbox(UUID(str(row[0])))
        return len(rows)

    def resume_suspended(
        self,
        *,
        quiet_window_seconds: float = 3,
        stability_timeout_seconds: float = 60,
    ) -> int:
        if quiet_window_seconds < 0 or stability_timeout_seconds <= 0:
            raise ValueError("resume timing values are invalid")
        rows = self.session.execute(
            text(
                """
                    UPDATE ingestion_monitor_items
                    SET status = 'pending',
                        available_at = NOW() + make_interval(secs => :quiet_seconds),
                        stability_deadline = NOW() + make_interval(secs => :quiet_seconds + :timeout_seconds),
                        last_size = NULL, last_mtime_ns = NULL, stable_matches = 0,
                        result_json = NULL, error_json = NULL, finished_at = NULL,
                        updated_at = NOW()
                    WHERE status = 'suspended'
                    RETURNING id
                    """
            ),
            {"quiet_seconds": quiet_window_seconds, "timeout_seconds": stability_timeout_seconds},
        ).all()
        return len(rows)

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        _positive_integer(limit, "limit")
        rows = self.session.execute(
            text(
                """
                WITH candidates AS (
                    SELECT item.id
                    FROM ingestion_monitor_items AS item
                    JOIN outbox_events AS event
                      ON event.aggregate_id = item.id::text
                    WHERE event.topic = :topic
                      AND event.aggregate_type = 'ingestion_monitor_item'
                      AND event.status = 'dead'
                      AND item.status IN ('ready', 'retryable')
                    ORDER BY event.updated_at, item.event_sequence, item.id
                    LIMIT :limit
                    FOR UPDATE OF item SKIP LOCKED
                )
                UPDATE ingestion_monitor_items AS item
                SET status = 'rejected',
                    error_json = '{"type":"monitor_dispatch_exhausted"}'::jsonb,
                    finished_at = NOW(), updated_at = NOW()
                FROM candidates
                WHERE item.id = candidates.id
                RETURNING item.id
                """
            ),
            {"topic": INGESTION_MONITOR_TOPIC, "limit": limit},
        ).all()
        return len(rows)

    def known_asset_paths(self) -> frozenset[str]:
        rows = self.session.execute(
            text(
                """
                SELECT asset.value->>'path' AS path
                FROM media_metadata AS metadata
                CROSS JOIN LATERAL jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(metadata.asset_details_json) = 'array'
                        THEN metadata.asset_details_json
                        ELSE '[]'::jsonb
                    END
                ) AS asset(value)
                WHERE NULLIF(BTRIM(asset.value->>'path'), '') IS NOT NULL
                ORDER BY asset.value->>'path'
                """
            )
        ).scalars()
        return frozenset(normalize_media_path(path) for path in rows if str(path or "").strip())

    def reserve_binding(
        self,
        *,
        binding_key: str,
        monitor_item_id: UUID,
        path: object,
        sha1: str | None = None,
        pick_code: str | None = None,
        media_info: Mapping[str, Any] | None = None,
    ) -> BindingReservationResult:
        key = _nonempty(binding_key, "binding_key", maximum=512)
        self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
            {"lock_key": f"binding:{key}"},
        )
        normalized_path = normalize_observed_path(path)
        normalized_sha1 = _sha1(sha1)
        normalized_pick_code = _pick_code(pick_code)
        normalized_media_info = _json(media_info, "binding media_info")
        request = {
            "monitor_item_id": str(monitor_item_id),
            "path": normalized_path,
            "path_key": normalize_media_path(normalized_path),
            "sha1": normalized_sha1,
            "pick_code": normalized_pick_code,
            "media_info": normalized_media_info,
        }
        request_hash = hashlib.sha256(canonical_json(request, field="pending binding").encode()).digest()
        existing = (
            self.session.execute(
                text("SELECT * FROM ingestion_pending_bindings WHERE binding_key = :binding_key FOR UPDATE"),
                {"binding_key": key},
            )
            .mappings()
            .one_or_none()
        )
        if existing is not None:
            projection = _binding_projection(existing)
            if projection.request_hash != request_hash:
                raise IngestionMonitorConflictError("binding_key was already used with a different request")
            return BindingReservationResult(projection, created=False)
        if self.item(monitor_item_id) is None:
            raise IngestionMonitorNotFoundError(f"monitor item {monitor_item_id} was not found")
        binding_id = uuid4()
        row = (
            self.session.execute(
                text(
                    """
                    INSERT INTO ingestion_pending_bindings (
                        id, binding_key, request_hash, monitor_item_id,
                        path, path_key, sha1, pick_code, media_info_json
                    ) VALUES (
                        :id, :binding_key, :request_hash, :monitor_item_id,
                        :path, :path_key, :sha1, :pick_code, CAST(:media_info_json AS jsonb)
                    )
                    RETURNING *
                    """
                ),
                {
                    "id": binding_id,
                    "binding_key": key,
                    "request_hash": request_hash,
                    "monitor_item_id": monitor_item_id,
                    "path": normalized_path,
                    "path_key": normalize_media_path(normalized_path),
                    "sha1": normalized_sha1,
                    "pick_code": normalized_pick_code,
                    "media_info_json": (
                        canonical_json(normalized_media_info) if normalized_media_info is not None else None
                    ),
                },
            )
            .mappings()
            .one()
        )
        return BindingReservationResult(_binding_projection(row), created=True)

    def binding(self, binding_id: UUID, *, for_update: bool = False) -> PendingBindingProjection | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = (
            self.session.execute(
                text(f"SELECT * FROM ingestion_pending_bindings WHERE id = :binding_id{suffix}"),
                {"binding_id": binding_id},
            )
            .mappings()
            .one_or_none()
        )
        return _binding_projection(row) if row is not None else None

    def binding_for_monitor_item(
        self,
        monitor_item_id: UUID,
        *,
        for_update: bool = False,
    ) -> PendingBindingProjection | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = (
            self.session.execute(
                text(
                    f"""
                    SELECT *
                    FROM ingestion_pending_bindings
                    WHERE monitor_item_id = :monitor_item_id
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1{suffix}
                    """
                ),
                {"monitor_item_id": monitor_item_id},
            )
            .mappings()
            .one_or_none()
        )
        return _binding_projection(row) if row is not None else None

    def lookup_for_item_ready(
        self,
        *,
        path: object | None = None,
        sha1: str | None = None,
        pick_code: str | None = None,
        emby_item_id: str | None = None,
    ) -> PendingBindingProjection | None:
        path_key = (
            normalize_media_path(normalize_observed_path(path)) if path is not None and str(path).strip() else None
        )
        normalized_sha1 = _sha1(sha1)
        normalized_pick_code = _pick_code(pick_code)
        normalized_emby_item_id = str(emby_item_id or "").strip() or None
        if path_key is None and normalized_sha1 is None and normalized_pick_code is None:
            raise ValueError("item-ready lookup requires a path, SHA1, or PickCode")
        row = (
            self.session.execute(
                text(
                    """
                    SELECT *
                    FROM ingestion_pending_bindings
                    WHERE (CAST(:path_key AS text) IS NOT NULL AND path_key = CAST(:path_key AS text))
                       OR (CAST(:sha1 AS text) IS NOT NULL AND sha1 = CAST(:sha1 AS text))
                       OR (CAST(:pick_code AS text) IS NOT NULL AND pick_code = CAST(:pick_code AS text))
                    ORDER BY
                        CASE WHEN status IN ('pending', 'retryable', 'claimed') THEN 0
                             WHEN CAST(:emby_item_id AS text) IS NOT NULL
                                  AND status = 'confirmed'
                                  AND emby_item_id = CAST(:emby_item_id AS text) THEN 1
                             ELSE 2 END,
                        CASE WHEN CAST(:path_key AS text) IS NOT NULL
                                  AND path_key = CAST(:path_key AS text) THEN 0
                             WHEN CAST(:pick_code AS text) IS NOT NULL
                                  AND pick_code = CAST(:pick_code AS text) THEN 1
                             ELSE 2 END,
                        created_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                {
                    "path_key": path_key,
                    "sha1": normalized_sha1,
                    "pick_code": normalized_pick_code,
                    "emby_item_id": normalized_emby_item_id,
                },
            )
            .mappings()
            .one_or_none()
        )
        return _binding_projection(row) if row is not None else None

    def claim_binding_by_id(
        self,
        binding_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> PendingBindingProjection | None:
        worker = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        row = (
            self.session.execute(
                text(
                    """
                    UPDATE ingestion_pending_bindings AS binding
                    SET status = 'claimed', claimed_by = :worker_id,
                        claim_token = :claim_token, claimed_at = NOW(),
                        lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                        attempts = attempts + 1, updated_at = NOW()
                    WHERE binding.id = :binding_id
                      AND binding.status IN ('pending', 'retryable')
                      AND binding.available_at <= NOW()
                      AND EXISTS (
                          SELECT 1
                          FROM ingestion_monitor_items AS item
                          JOIN media_effects AS effect
                            ON effect.aggregate_type = 'ingestion_monitor'
                           AND effect.aggregate_id = item.id::text
                           AND effect.effect_type = 'emby.library.paths_changed'
                           AND effect.status = 'confirmed'
                          WHERE item.id = binding.monitor_item_id
                            AND item.status = 'claimed'
                            AND item.lease_expires_at > NOW()
                      )
                    RETURNING binding.*
                    """
                ),
                {
                    "binding_id": binding_id,
                    "worker_id": worker,
                    "claim_token": uuid4(),
                    "lease_seconds": lease_seconds,
                },
            )
            .mappings()
            .one_or_none()
        )
        return _binding_projection(row) if row is not None else None

    def terminalize_binding_for_monitor_claim(
        self,
        binding_id: UUID,
        *,
        monitor_item_id: UUID,
        monitor_claim_token: UUID,
        status: BindingStatus,
        error: Mapping[str, Any],
    ) -> BindingTransitionResult:
        if status not in {BindingStatus.REJECTED, BindingStatus.INDETERMINATE}:
            raise ValueError("pre-delivery binding terminal status must be rejected or indeterminate")
        normalized_error = _json(error, "binding error")
        assert normalized_error is not None
        row = (
            self.session.execute(
                text(
                    """
                    UPDATE ingestion_pending_bindings AS binding
                    SET status = :status,
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        emby_item_id = NULL, result_json = NULL,
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = NOW(), updated_at = NOW()
                    FROM ingestion_monitor_items AS item
                    WHERE binding.id = :binding_id
                      AND binding.monitor_item_id = :monitor_item_id
                      AND binding.status IN ('pending', 'retryable')
                      AND item.id = binding.monitor_item_id
                      AND item.status = 'claimed'
                      AND item.claim_token = :monitor_claim_token
                    RETURNING binding.*
                    """
                ),
                {
                    "binding_id": binding_id,
                    "monitor_item_id": monitor_item_id,
                    "monitor_claim_token": monitor_claim_token,
                    "status": status.value,
                    "error_json": canonical_json(normalized_error),
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            return BindingTransitionResult(_binding_projection(row), changed=True)
        current = self.binding(binding_id)
        if current is None:
            raise IngestionMonitorNotFoundError(f"pending binding {binding_id} was not found")
        if current.status is not status or current.error != normalized_error:
            raise IngestionMonitorStateError(
                f"pending binding {binding_id} cannot be terminalized from {current.status.value}"
            )
        return BindingTransitionResult(current, changed=False)

    def mark_binding_confirmed(
        self,
        binding_id: UUID,
        claim_token: UUID,
        *,
        emby_item_id: str,
        result: Mapping[str, Any] | None = None,
    ) -> BindingTransitionResult:
        return self._transition_binding(
            binding_id,
            claim_token,
            BindingStatus.CONFIRMED,
            emby_item_id=_nonempty(emby_item_id, "emby_item_id", maximum=255),
            result=result,
        )

    def mark_binding_rejected(
        self,
        binding_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> BindingTransitionResult:
        return self._transition_binding(binding_id, claim_token, BindingStatus.REJECTED, error=error)

    def mark_binding_indeterminate(
        self,
        binding_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> BindingTransitionResult:
        return self._transition_binding(binding_id, claim_token, BindingStatus.INDETERMINATE, error=error)

    def mark_binding_retryable(
        self,
        binding_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> BindingTransitionResult:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must not be negative")
        return self._transition_binding(
            binding_id,
            claim_token,
            BindingStatus.RETRYABLE,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )

    def _transition_binding(
        self,
        binding_id: UUID,
        claim_token: UUID,
        status: BindingStatus,
        *,
        emby_item_id: str | None = None,
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> BindingTransitionResult:
        normalized_result = _json(result, "binding result")
        normalized_error = _json(error, "binding error")
        terminal = status in _BINDING_TERMINAL_STATUSES
        row = (
            self.session.execute(
                text(
                    """
                    UPDATE ingestion_pending_bindings
                    SET status = :status,
                        available_at = CASE WHEN :status = 'retryable'
                            THEN NOW() + make_interval(secs => :retry_after_seconds)
                            ELSE available_at END,
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        emby_item_id = :emby_item_id,
                        result_json = CAST(:result_json AS jsonb),
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = CASE WHEN :terminal THEN NOW() ELSE NULL END,
                        updated_at = NOW()
                    WHERE id = :binding_id AND status = 'claimed' AND claim_token = :claim_token
                    RETURNING *
                    """
                ),
                {
                    "binding_id": binding_id,
                    "claim_token": claim_token,
                    "status": status.value,
                    "retry_after_seconds": retry_after_seconds,
                    "emby_item_id": emby_item_id,
                    "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                    "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                    "terminal": terminal,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            return BindingTransitionResult(_binding_projection(row), changed=True)
        current = self.binding(binding_id)
        if current is None:
            raise IngestionMonitorNotFoundError(f"pending binding {binding_id} was not found")
        if not terminal or current.status is not status:
            raise IngestionMonitorStateError(
                f"pending binding {binding_id} is {current.status.value}, not a claim owned by this token"
            )
        if (
            current.emby_item_id != emby_item_id
            or current.result != normalized_result
            or current.error != normalized_error
        ):
            raise IngestionMonitorConflictError("duplicate binding terminal transition changed its payload")
        return BindingTransitionResult(current, changed=False)

    def expire_stale_bindings(self, *, limit: int = 100, retry_delay_seconds: float = 0) -> int:
        _positive_integer(limit, "limit")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        rows = self.session.execute(
            text(
                """
                WITH candidates AS (
                    SELECT id
                    FROM ingestion_pending_bindings
                    WHERE status = 'claimed' AND lease_expires_at <= NOW()
                    ORDER BY lease_expires_at, created_at, id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ingestion_pending_bindings AS binding
                SET status = 'retryable',
                    available_at = NOW() + make_interval(secs => :retry_delay_seconds),
                    claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_expires_at = NULL,
                    emby_item_id = NULL, result_json = NULL,
                    error_json = '{"type":"claim_lease_expired"}'::jsonb,
                    finished_at = NULL, updated_at = NOW()
                FROM candidates
                WHERE binding.id = candidates.id
                RETURNING binding.id
                """
            ),
            {"limit": limit, "retry_delay_seconds": retry_delay_seconds},
        ).all()
        return len(rows)

    def _item_by_producer_key(self, producer_key: str, *, for_update: bool = False) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            self.session.execute(
                text(f"SELECT * FROM ingestion_monitor_items WHERE producer_key = :producer_key{suffix}"),
                {"producer_key": producer_key},
            )
            .mappings()
            .one_or_none()
        )

    def _has_unclaimed_successor(self, item: MonitorItemProjection) -> bool:
        return bool(
            self.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM ingestion_monitor_items
                        WHERE path_key = :path_key
                          AND id <> :item_id
                          AND status = ANY(CAST(:statuses AS text[]))
                    )
                    """
                ),
                {
                    "path_key": item.path_key,
                    "item_id": item.id,
                    "statuses": list(_COALESCIBLE_STATUSES),
                },
            )
        )

    def _update_item(self, item_id: UUID, assignments: str, values: Mapping[str, Any]) -> RowMapping:
        row = (
            self.session.execute(
                text(f"UPDATE ingestion_monitor_items SET {assignments} WHERE id = :item_id RETURNING *"),
                {"item_id": item_id, **dict(values)},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise IngestionMonitorNotFoundError(f"monitor item {item_id} was not found")
        return row

    def _stage_outbox(self, item: MonitorItemProjection) -> None:
        payload = {
            "item_id": str(item.id),
            "event_sequence": item.event_sequence,
            "producer_key": item.producer_key,
            "request_hash": item.request_hash.hex(),
        }
        self.session.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id,
                    payload_version, payload_json, status, available_at
                ) VALUES (
                    :id, :event_key, :topic, 'ingestion_monitor_item', :aggregate_id,
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
                "event_key": monitor_outbox_event_key(item.id),
                "topic": INGESTION_MONITOR_TOPIC,
                "aggregate_id": str(item.id),
                "payload_json": canonical_json(payload, field="ingestion monitor outbox payload"),
                "available_at": item.available_at,
            },
        )

    def _complete_outbox(self, item_id: UUID) -> None:
        self.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE event_key = :event_key
                  AND topic = :topic
                  AND status IN ('pending', 'publishing', 'published')
                """
            ),
            {"event_key": monitor_outbox_event_key(item_id), "topic": INGESTION_MONITOR_TOPIC},
        )

    def _cancel_outbox(self, item_id: UUID) -> None:
        self.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'cancelled',
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE event_key = :event_key
                  AND topic = :topic
                  AND status IN ('pending', 'publishing', 'published')
                """
            ),
            {"event_key": monitor_outbox_event_key(item_id), "topic": INGESTION_MONITOR_TOPIC},
        )


class PostgresIngestionMonitorLedger:
    """UoW wrapper used by the Coordinator, Worker, scan task, and item-ready transport."""

    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    def record(self, intents: Sequence[MonitorIntent]) -> tuple[ReservationResult, ...]:
        with self._uow_factory() as uow:
            result = SqlAlchemyIngestionRepository(uow.session).reserve_intents(intents)
            uow.commit()
            return result

    def read(self, item_id: UUID) -> MonitorItemProjection | None:
        with self._uow_factory() as uow:
            return SqlAlchemyIngestionRepository(uow.session).item(item_id)

    def due_for_stability(self, *, limit: int = 100) -> tuple[MonitorItemProjection, ...]:
        with self._uow_factory() as uow:
            result = SqlAlchemyIngestionRepository(uow.session).due_for_stability(limit=limit)
            uow.commit()
            return result

    def record_stability_sample(
        self,
        item_id: UUID,
        observation: FileObservation,
        *,
        observed_at: datetime | None = None,
    ) -> tuple[MonitorItemProjection, StabilityOutcome]:
        with self._uow_factory() as uow:
            result = SqlAlchemyIngestionRepository(uow.session).record_stability_sample(
                item_id,
                observation,
                observed_at=observed_at,
            )
            uow.commit()
            return result

    def mark_ready(self, item_id: UUID, *, available_at: datetime | None = None) -> TransitionResult:
        return self._write("mark_ready", item_id, available_at=available_at)

    def claim_by_id(
        self,
        item_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> MonitorItemProjection | None:
        return self._write("claim_by_id", item_id, worker_id=worker_id, lease_seconds=lease_seconds)

    def claim(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
        limit: int = 1,
    ) -> tuple[MonitorItemProjection, ...]:
        return self._write("claim", worker_id=worker_id, lease_seconds=lease_seconds, limit=limit)

    def mark_confirmed(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        return self._write("mark_confirmed", item_id, claim_token, result=result)

    def mark_rejected(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        return self._write("mark_rejected", item_id, claim_token, error=error)

    def mark_indeterminate(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        return self._write("mark_indeterminate", item_id, claim_token, error=error)

    def mark_retryable(
        self,
        item_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> TransitionResult:
        return self._write(
            "mark_retryable",
            item_id,
            claim_token,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )

    def expire_stale_claims(self, *, limit: int = 100, retry_delay_seconds: float = 0) -> int:
        return self._write("expire_stale_claims", limit=limit, retry_delay_seconds=retry_delay_seconds)

    def suspend_active(self) -> int:
        return self._write("suspend_active")

    def resume_suspended(
        self,
        *,
        quiet_window_seconds: float = 3,
        stability_timeout_seconds: float = 60,
    ) -> int:
        return self._write(
            "resume_suspended",
            quiet_window_seconds=quiet_window_seconds,
            stability_timeout_seconds=stability_timeout_seconds,
        )

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        return self._write("reject_dead_dispatches", limit=limit)

    def known_asset_paths(self) -> frozenset[str]:
        with self._uow_factory() as uow:
            return SqlAlchemyIngestionRepository(uow.session).known_asset_paths()

    def reserve_binding(self, **values: Any) -> BindingReservationResult:
        return self._write("reserve_binding", **values)

    def read_binding(self, binding_id: UUID) -> PendingBindingProjection | None:
        with self._uow_factory() as uow:
            return SqlAlchemyIngestionRepository(uow.session).binding(binding_id)

    def binding_for_monitor_item(self, monitor_item_id: UUID) -> PendingBindingProjection | None:
        with self._uow_factory() as uow:
            return SqlAlchemyIngestionRepository(uow.session).binding_for_monitor_item(monitor_item_id)

    def lookup_for_item_ready(self, **values: Any) -> PendingBindingProjection | None:
        with self._uow_factory() as uow:
            return SqlAlchemyIngestionRepository(uow.session).lookup_for_item_ready(**values)

    def claim_binding_by_id(self, binding_id: UUID, **values: Any) -> PendingBindingProjection | None:
        return self._write("claim_binding_by_id", binding_id, **values)

    def terminalize_binding_for_monitor_claim(
        self,
        binding_id: UUID,
        *,
        monitor_item_id: UUID,
        monitor_claim_token: UUID,
        status: BindingStatus,
        error: Mapping[str, Any],
    ) -> BindingTransitionResult:
        return self._write(
            "terminalize_binding_for_monitor_claim",
            binding_id,
            monitor_item_id=monitor_item_id,
            monitor_claim_token=monitor_claim_token,
            status=status,
            error=error,
        )

    def mark_binding_confirmed(self, binding_id: UUID, claim_token: UUID, **values: Any) -> BindingTransitionResult:
        return self._write("mark_binding_confirmed", binding_id, claim_token, **values)

    def mark_binding_rejected(self, binding_id: UUID, claim_token: UUID, **values: Any) -> BindingTransitionResult:
        return self._write("mark_binding_rejected", binding_id, claim_token, **values)

    def mark_binding_indeterminate(
        self,
        binding_id: UUID,
        claim_token: UUID,
        **values: Any,
    ) -> BindingTransitionResult:
        return self._write("mark_binding_indeterminate", binding_id, claim_token, **values)

    def mark_binding_retryable(self, binding_id: UUID, claim_token: UUID, **values: Any) -> BindingTransitionResult:
        return self._write("mark_binding_retryable", binding_id, claim_token, **values)

    def expire_stale_bindings(self, *, limit: int = 100, retry_delay_seconds: float = 0) -> int:
        return self._write("expire_stale_bindings", limit=limit, retry_delay_seconds=retry_delay_seconds)

    def _write(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        with self._uow_factory() as uow:
            method = getattr(SqlAlchemyIngestionRepository(uow.session), operation)
            result = method(*args, **kwargs)
            uow.commit()
            return result


def monitor_outbox_event_key(item_id: UUID) -> str:
    return f"{INGESTION_MONITOR_TOPIC}:{item_id}"


def _item_projection(row: RowMapping) -> MonitorItemProjection:
    return MonitorItemProjection(
        id=UUID(str(row["id"])),
        event_sequence=int(row["event_sequence"]),
        producer_key=str(row["producer_key"]),
        request_hash=bytes(row["request_hash"]),
        source=MonitorSource(str(row["source"])),
        event_type=MonitorEventType(str(row["event_type"])),
        path=str(row["path"]),
        path_key=str(row["path_key"]),
        related_path=str(row["related_path"]) if row["related_path"] is not None else None,
        correlation_id=UUID(str(row["correlation_id"])) if row["correlation_id"] is not None else None,
        monitor_root=str(row["monitor_root"]),
        config_revision=str(row["config_revision"]),
        refresh_delay_seconds=int(row["refresh_delay_seconds"]),
        classification=_classification(str(row["classification"])),
        status=MonitorItemStatus(str(row["status"])),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        stability_deadline=row["stability_deadline"],
        last_size=int(row["last_size"]) if row["last_size"] is not None else None,
        last_mtime_ns=int(row["last_mtime_ns"]) if row["last_mtime_ns"] is not None else None,
        stable_matches=int(row["stable_matches"]),
        claimed_by=str(row["claimed_by"]) if row["claimed_by"] is not None else None,
        claim_token=UUID(str(row["claim_token"])) if row["claim_token"] is not None else None,
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        result=dict(row["result_json"]) if isinstance(row["result_json"], Mapping) else None,
        error=dict(row["error_json"]) if isinstance(row["error_json"], Mapping) else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _binding_projection(row: RowMapping) -> PendingBindingProjection:
    return PendingBindingProjection(
        id=UUID(str(row["id"])),
        binding_key=str(row["binding_key"]),
        request_hash=bytes(row["request_hash"]),
        monitor_item_id=UUID(str(row["monitor_item_id"])),
        path=str(row["path"]),
        path_key=str(row["path_key"]),
        sha1=str(row["sha1"]) if row["sha1"] is not None else None,
        pick_code=str(row["pick_code"]) if row["pick_code"] is not None else None,
        media_info=dict(row["media_info_json"]) if isinstance(row["media_info_json"], Mapping) else None,
        status=BindingStatus(str(row["status"])),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=str(row["claimed_by"]) if row["claimed_by"] is not None else None,
        claim_token=UUID(str(row["claim_token"])) if row["claim_token"] is not None else None,
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        emby_item_id=str(row["emby_item_id"]) if row["emby_item_id"] is not None else None,
        result=dict(row["result_json"]) if isinstance(row["result_json"], Mapping) else None,
        error=dict(row["error_json"]) if isinstance(row["error_json"], Mapping) else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _classification(value: str):
    from .monitoring import MonitorClassification

    return MonitorClassification(value)


def _json(value: Mapping[str, Any] | None, field: str) -> dict[str, Any] | None:
    return normalize_json_object(dict(value), field=field) if value is not None else None


def _sha1(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip().upper()
    if len(normalized) != 40:
        raise ValueError("sha1 must contain 40 hexadecimal characters")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("sha1 must contain 40 hexadecimal characters") from exc
    return normalized


def _optional_nonempty(value: object, field: str, *, maximum: int | None = None) -> str | None:
    if value is None or not str(value).strip():
        return None
    return _nonempty(value, field, maximum=maximum)


def _pick_code(value: object) -> str | None:
    normalized = _optional_nonempty(value, "pick_code", maximum=255)
    return normalized.casefold() if normalized is not None else None


def _nonempty(value: object, field: str, *, maximum: int | None = None) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if maximum is not None and len(normalized) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return normalized


def _positive(value: float, field: str) -> None:
    if value <= 0:
        raise ValueError(f"{field} must be positive")


def _positive_integer(value: int, field: str) -> None:
    if value < 1:
        raise ValueError(f"{field} must be positive")


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


__all__ = [
    "PostgresIngestionMonitorLedger",
    "SqlAlchemyIngestionRepository",
    "monitor_outbox_event_key",
]
