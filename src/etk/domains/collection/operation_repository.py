from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object
from etk.platform.jobs.types import UnitOfWork, UnitOfWorkFactory

from .operations import (
    COLLECTION_OPERATION_AGGREGATE_TYPE,
    COLLECTION_OPERATION_TOPIC,
    CollectionOperation,
    CollectionOperationConflictError,
    CollectionOperationIndeterminateBlockerError,
    CollectionOperationIntent,
    CollectionOperationKind,
    CollectionOperationReservation,
    CollectionOperationScopeBusyError,
    CollectionOperationStateError,
    CollectionOperationStatus,
    CollectionOperationStep,
    collection_image_digest,
    collection_operation_key,
)


class PostgresCollectionOperationRepository:
    def __init__(self, unit_of_work: UnitOfWorkFactory) -> None:
        self._unit_of_work = unit_of_work

    def reserve(self, intent: CollectionOperationIntent) -> CollectionOperationReservation:
        with self._unit_of_work() as uow:
            reservation = self._reserve_in_uow(uow, intent)
            uow.commit()
            return reservation

    def reserve_in_uow(
        self,
        uow: UnitOfWork,
        intent: CollectionOperationIntent,
    ) -> CollectionOperationReservation:
        """Reserve inside an existing transaction owned by a cross-domain ingress."""
        return self._reserve_in_uow(uow, intent)

    def reserve_custom_delete(
        self,
        *,
        collection_id: int,
        principal_id: str,
        idempotency_key: str | None,
        now: datetime | None = None,
    ) -> CollectionOperationReservation | None:
        normalized_collection_id = int(collection_id)
        timestamp = _utc(now)
        explicit = str(idempotency_key or "").strip() or None
        with self._unit_of_work() as uow:
            if explicit is not None:
                operation_key = collection_operation_key(
                    kind=CollectionOperationKind.CUSTOM_DELETE,
                    principal_id=principal_id,
                    idempotency_key=explicit,
                )
                existing = self._read_by_key_in_uow(uow, operation_key, lock=True)
                if existing is not None:
                    if int(existing.request.get("collection_id") or 0) != normalized_collection_id:
                        raise CollectionOperationConflictError(
                            "idempotency key was already used for another custom collection delete"
                        )
                    uow.commit()
                    return CollectionOperationReservation(existing, created=False)

            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, name, emby_collection_id, generated_media_info_json
                        FROM custom_collections
                        WHERE id = :collection_id
                        FOR UPDATE
                        """
                    ),
                    {"collection_id": normalized_collection_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            emby_collection_id = str(row.get("emby_collection_id") or "").strip() or None
            placeholder_tmdb_ids = sorted(
                {
                    str(item.get("tmdb_id") or "").strip()
                    for item in (row.get("generated_media_info_json") or [])
                    if isinstance(item, Mapping) and str(item.get("tmdb_id") or "").strip()
                }
            )
            intent = CollectionOperationIntent.create(
                kind=CollectionOperationKind.CUSTOM_DELETE,
                scope_key=(
                    f"emby-collection:{emby_collection_id}"
                    if emby_collection_id is not None
                    else f"custom-collection:{normalized_collection_id}"
                ),
                principal_id=principal_id,
                request={
                    "collection_id": normalized_collection_id,
                    "collection_name": str(row.get("name") or ""),
                    "emby_collection_id": emby_collection_id,
                    "placeholder_tmdb_ids": placeholder_tmdb_ids,
                },
                idempotency_key=explicit,
                available_at=timestamp,
            )
            reservation = self._reserve_in_uow(uow, intent)
            if reservation.created:
                deleted = uow.session.execute(
                    text("DELETE FROM custom_collections WHERE id = :collection_id"),
                    {"collection_id": normalized_collection_id},
                )
                if int(getattr(deleted, "rowcount", 0) or 0) != 1:
                    raise CollectionOperationStateError("custom collection delete lost its locked row")
            uow.commit()
            return reservation

    def reserve_missing_delete_children(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        tmdb_collection_id: str,
        emby_collection_ids: Sequence[str],
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        normalized_tmdb_id = _required_text(tmdb_collection_id, "tmdb_collection_id")
        normalized_ids = tuple(sorted({_required_text(value, "emby_collection_id") for value in emby_collection_ids}))
        normalized_state = normalize_durable_json_object(dict(state), field="collection operation state")
        child_ids = [str(value) for value in normalized_state.get("child_operation_ids", [])]
        warnings = [dict(value) for value in normalized_state.get("warnings", []) if isinstance(value, Mapping)]
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            for emby_collection_id in normalized_ids:
                intent = CollectionOperationIntent.create(
                    kind=CollectionOperationKind.NATIVE_DELETE,
                    scope_key=f"emby-collection:{emby_collection_id}",
                    principal_id="collection-system:missing-cleanup",
                    request={
                        "emby_collection_id": emby_collection_id,
                        "expected_tmdb_collection_id": normalized_tmdb_id,
                        "reason": "explicit_tmdb_404_cleanup",
                        "parent_operation_id": str(operation_id),
                    },
                    idempotency_key=f"{operation_id}:{emby_collection_id}",
                    available_at=timestamp,
                )
                try:
                    reservation = self._reserve_in_uow(uow, intent)
                    child_id = str(reservation.operation.id)
                except CollectionOperationScopeBusyError as exc:
                    existing = (
                        uow.session.execute(
                            text(
                                """
                                SELECT id, kind, request_json, status
                                FROM collection_operations
                                WHERE scope_key = :scope_key
                                  AND status IN (
                                    'ready','claimed','requesting','awaiting_readback',
                                    'retryable','indeterminate'
                                  )
                                ORDER BY event_sequence DESC
                                LIMIT 1
                                """
                            ),
                            {"scope_key": intent.scope_key},
                        )
                        .mappings()
                        .one_or_none()
                    )
                    existing_request = existing.get("request_json") if existing is not None else None
                    existing_id = UUID(str(existing["id"])) if existing is not None else None
                    if existing is not None and str(existing["status"]) == "indeterminate":
                        if existing_id is None:
                            raise CollectionOperationStateError(
                                "indeterminate blocker is missing its identity"
                            ) from exc
                        raise CollectionOperationIndeterminateBlockerError(existing_id) from exc
                    if (
                        existing is None
                        or str(existing["kind"]) != CollectionOperationKind.NATIVE_DELETE.value
                        or not isinstance(existing_request, Mapping)
                        or str(existing_request.get("emby_collection_id") or "") != emby_collection_id
                        or str(existing_request.get("expected_tmdb_collection_id") or "") != normalized_tmdb_id
                    ):
                        raise CollectionOperationScopeBusyError(
                            f"collection scope for {emby_collection_id!r} is owned by a non-delete operation"
                        ) from exc
                    child_id = str(existing_id)
                    warnings.append(
                        {
                            "type": "remote_collection_scope_busy",
                            "emby_collection_id": emby_collection_id,
                            "operation_id": child_id or None,
                        }
                    )
                if child_id and child_id not in child_ids:
                    child_ids.append(child_id)
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'claimed', current_step = 'discover_missing_targets', "
                "step_started_at = NULL, state_json = CAST(:state_json AS jsonb), error_json = NULL",
                {
                    "state_json": canonical_json(
                        {
                            **dict(normalized_state),
                            "child_operation_ids": child_ids,
                            "warnings": warnings,
                        }
                    )
                },
            )
            uow.commit()
            return _operation(row)

    def native_scope_for_tmdb(self, tmdb_collection_id: str) -> str:
        normalized_tmdb_id = _required_text(tmdb_collection_id, "tmdb_collection_id")
        with self._unit_of_work() as uow:
            emby_id = uow.session.scalar(
                text(
                    """
                    SELECT emby_collection_id
                    FROM collections_info
                    WHERE tmdb_collection_id = :tmdb_collection_id
                    """
                ),
                {"tmdb_collection_id": normalized_tmdb_id},
            )
        normalized_emby_id = str(emby_id or "").strip()
        return (
            f"emby-collection:{normalized_emby_id}"
            if normalized_emby_id
            else f"tmdb-collection:{normalized_tmdb_id}:missing"
        )

    def read(self, operation_id: UUID) -> CollectionOperation | None:
        with self._unit_of_work() as uow:
            return self._read_in_uow(uow, operation_id)

    def current_image_metadata_digest(self, tmdb_collection_id: str) -> str | None:
        normalized_tmdb_id = _required_text(tmdb_collection_id, "tmdb_collection_id")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT poster_path, backdrop_path
                        FROM collections_info
                        WHERE tmdb_collection_id = :tmdb_collection_id
                        """
                    ),
                    {"tmdb_collection_id": normalized_tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        primary = _tmdb_image_url(row.get("poster_path"))
        backdrop = _tmdb_image_url(row.get("backdrop_path"))
        images = {
            key: value
            for key, value in {
                "primary": primary,
                "backdrop": backdrop,
                "thumb": backdrop,
            }.items()
            if value is not None
        }
        return collection_image_digest(images)

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> CollectionOperation | None:
        normalized_worker = _required_text(worker_id, "worker_id")
        if float(lease_seconds) <= 0:
            raise ValueError("collection operation lease must be positive")
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE collection_operations
                        SET status = 'claimed', attempts = attempts + 1,
                            claimed_by = :worker_id, claim_token = :claim_token,
                            claimed_at = :now, lease_expires_at = :lease_expires_at,
                            state_revision = state_revision + 1, updated_at = :now
                        WHERE id = :id
                          AND status IN ('ready','retryable','awaiting_readback')
                          AND available_at <= :now
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "worker_id": normalized_worker,
                        "claim_token": uuid4(),
                        "now": timestamp,
                        "lease_expires_at": timestamp + timedelta(seconds=float(lease_seconds)),
                    },
                )
                .mappings()
                .one_or_none()
            )
            uow.commit()
            return _operation(row) if row is not None else None

    def begin_provider_step(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        step: CollectionOperationStep | str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        normalized_step = step if isinstance(step, CollectionOperationStep) else CollectionOperationStep(step)
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="collection operation state")
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(uow, operation_id, claim_token, timestamp)
            if operation.claimed_at is None or operation.lease_expires_at is None:
                raise CollectionOperationStateError("collection operation claim timestamps are incomplete")
            lease_seconds = max(1.0, (operation.lease_expires_at - operation.claimed_at).total_seconds())
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'requesting', current_step = :step, step_started_at = :now, "
                "state_json = CAST(:state_json AS jsonb), error_json = NULL, "
                "claimed_at = :now, lease_expires_at = :lease_expires_at",
                {
                    "step": normalized_step.value,
                    "now": timestamp,
                    "lease_expires_at": timestamp + timedelta(seconds=lease_seconds),
                    "state_json": canonical_json(normalized_state),
                },
            )
            uow.commit()
            return _operation(row)

    def continue_claim(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        next_step: CollectionOperationStep | str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        normalized_step = (
            next_step if isinstance(next_step, CollectionOperationStep) else CollectionOperationStep(next_step)
        )
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="collection operation state")
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'claimed', current_step = :step, step_started_at = NULL, "
                "state_json = CAST(:state_json AS jsonb), error_json = NULL",
                {"step": normalized_step.value, "state_json": canonical_json(normalized_state)},
            )
            uow.commit()
            return _operation(row)

    def defer_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> CollectionOperation:
        return self._defer(
            operation_id,
            claim_token,
            status=CollectionOperationStatus.AWAITING_READBACK,
            state=state,
            error=error,
            retry_after_seconds=retry_after_seconds,
            preserve_step_started=True,
            now=now,
        )

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> CollectionOperation:
        return self._defer(
            operation_id,
            claim_token,
            status=CollectionOperationStatus.RETRYABLE,
            state=state,
            error=error,
            retry_after_seconds=retry_after_seconds,
            preserve_step_started=False,
            now=now,
        )

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=CollectionOperationStatus.CONFIRMED,
            state=state,
            result=result,
            error=None,
            now=now,
        )

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=CollectionOperationStatus.REJECTED,
            state=state,
            result=None,
            error=error,
            now=now,
        )

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=CollectionOperationStatus.INDETERMINATE,
            state=state,
            result=None,
            error=error,
            now=now,
        )

    def reconcile_indeterminate(
        self,
        operation_id: UUID,
        *,
        expected_state_revision: int,
        provider_present: bool,
        resolved_by: str,
        observation: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        normalized_resolver = _required_text(resolved_by, "resolved_by")
        normalized_observation = normalize_durable_json_object(
            dict(observation),
            field="collection operation reconciliation observation",
        )
        with self._unit_of_work() as uow:
            operation = self._read_in_uow(uow, operation_id, lock=True)
            if operation is None:
                raise CollectionOperationStateError("collection operation was not found")
            if operation.status is not CollectionOperationStatus.INDETERMINATE:
                raise CollectionOperationStateError("collection operation is not indeterminate")
            if operation.state_revision != int(expected_state_revision):
                raise CollectionOperationStateError("collection operation state revision changed")
            if operation.kind not in {
                CollectionOperationKind.NATIVE_DELETE,
                CollectionOperationKind.REMOVAL_SURVIVAL_CHECK,
            }:
                raise CollectionOperationStateError(
                    "collection operation kind does not support provider reconciliation"
                )
            emby_collection_id = _required_text(
                operation.request.get("emby_collection_id"),
                "emby_collection_id",
            )
            if not provider_present:
                uow.session.execute(
                    text(
                        """
                        UPDATE collections_info
                        SET emby_collection_id = NULL, is_active = FALSE, last_checked_at = :now
                        WHERE emby_collection_id = :emby_collection_id
                        """
                    ),
                    {"emby_collection_id": emby_collection_id, "now": timestamp},
                )
            confirmed = operation.kind is CollectionOperationKind.REMOVAL_SURVIVAL_CHECK or not provider_present
            status = CollectionOperationStatus.CONFIRMED if confirmed else CollectionOperationStatus.REJECTED
            reconciliation = {
                "provider_present": bool(provider_present),
                "resolved_by": normalized_resolver,
                "resolved_at": timestamp.isoformat(),
                "observation": normalized_observation,
            }
            result = {
                "operation": operation.kind.value,
                "emby_collection_id": emby_collection_id,
                "reconciled": True,
                **reconciliation,
            }
            error = (
                None
                if confirmed
                else {
                    "type": "provider_write_not_observed",
                    **reconciliation,
                }
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE collection_operations
                        SET status = :status,
                            state_json = CAST(:state_json AS jsonb),
                            result_json = CAST(:result_json AS jsonb),
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = :now,
                            state_revision = state_revision + 1,
                            updated_at = :now
                        WHERE id = :id
                          AND status = 'indeterminate'
                          AND state_revision = :expected_state_revision
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "status": status.value,
                        "state_json": canonical_json({**dict(operation.state), "reconciliation": reconciliation}),
                        "result_json": canonical_json(result),
                        "error_json": canonical_json(error) if error is not None else None,
                        "now": timestamp,
                        "expected_state_revision": int(expected_state_revision),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise CollectionOperationStateError("collection operation reconciliation lost its revision fence")
            uow.commit()
            return _operation(row)

    def complete_native_delete(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        emby_collection_id: str,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            uow.session.execute(
                text(
                    """
                    UPDATE collections_info
                    SET emby_collection_id = NULL, is_active = FALSE, last_checked_at = :now
                    WHERE emby_collection_id = :emby_collection_id
                    """
                ),
                {"emby_collection_id": emby_collection_id, "now": timestamp},
            )
            row = self._terminal_update(
                uow,
                operation_id,
                claim_token,
                status=CollectionOperationStatus.CONFIRMED,
                state=state,
                result=result,
                error=None,
                now=timestamp,
            )
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    def checkpoint_missing_cleanup(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        tmdb_collection_id: str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        normalized_tmdb_id = _required_text(tmdb_collection_id, "tmdb_collection_id")
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            native = (
                uow.session.execute(
                    text(
                        """
                        DELETE FROM collections_info
                        WHERE tmdb_collection_id = :tmdb_collection_id
                        RETURNING poster_path, backdrop_path
                        """
                    ),
                    {"tmdb_collection_id": normalized_tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            policy = (
                uow.session.execute(
                    text(
                        """
                        DELETE FROM media_image_policy_cache
                        WHERE item_type = 'BoxSet' AND tmdb_id = :tmdb_collection_id
                          AND season_number = -1 AND episode_number = -1
                        RETURNING images_json
                        """
                    ),
                    {"tmdb_collection_id": normalized_tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            sources = _deleted_sources(native, policy)
            normalized_state = normalize_durable_json_object(
                {
                    **dict(state),
                    "local_cleanup_applied": True,
                    "source_urls": list(sources),
                },
                field="collection operation state",
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'claimed', current_step = 'cleanup_local', step_started_at = NULL, "
                "state_json = CAST(:state_json AS jsonb), error_json = NULL",
                {"state_json": canonical_json(normalized_state)},
            )
            uow.commit()
            return _operation(row)

    def complete_survival_check(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        emby_collection_id: str,
        survived: bool,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            if not survived:
                uow.session.execute(
                    text(
                        """
                        UPDATE collections_info
                        SET emby_collection_id = NULL, is_active = FALSE, last_checked_at = :now
                        WHERE emby_collection_id = :emby_collection_id
                        """
                    ),
                    {"emby_collection_id": emby_collection_id, "now": timestamp},
                )
            row = self._terminal_update(
                uow,
                operation_id,
                claim_token,
                status=CollectionOperationStatus.CONFIRMED,
                state=state,
                result=result,
                error=None,
                now=timestamp,
            )
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    def expire_stale_claims(
        self,
        *,
        limit: int = 100,
        retry_delay_seconds: float = 1,
        now: datetime | None = None,
    ) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    WITH stale AS (
                        SELECT id, step_started_at
                        FROM collection_operations
                        WHERE status IN ('claimed','requesting')
                          AND lease_expires_at <= :now
                        ORDER BY lease_expires_at, event_sequence
                        FOR UPDATE SKIP LOCKED
                        LIMIT :limit
                    )
                    UPDATE collection_operations AS operation
                    SET status = CASE
                            WHEN stale.step_started_at IS NULL THEN 'retryable'
                            ELSE 'awaiting_readback'
                        END,
                        available_at = :available_at,
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        state_revision = operation.state_revision + 1,
                        updated_at = :now
                    FROM stale
                    WHERE operation.id = stale.id
                    RETURNING operation.id
                    """
                ),
                {
                    "now": timestamp,
                    "available_at": timestamp + timedelta(seconds=max(0.0, float(retry_delay_seconds))),
                    "limit": max(1, int(limit)),
                },
            ).all()
            for row in rows:
                self._requeue_outbox(
                    uow,
                    UUID(str(row[0])),
                    timestamp + timedelta(seconds=max(0.0, float(retry_delay_seconds))),
                )
            uow.commit()
            return len(rows)

    def reject_dead_dispatches(self, *, limit: int = 100, now: datetime | None = None) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    WITH candidates AS (
                        SELECT operation.id, operation.step_started_at
                        FROM collection_operations AS operation
                        JOIN outbox_events AS outbox
                          ON outbox.event_key = :topic || ':' || operation.id::text
                        WHERE outbox.status = 'dead'
                          AND operation.status NOT IN ('confirmed','rejected','indeterminate')
                        ORDER BY operation.event_sequence
                        FOR UPDATE OF operation SKIP LOCKED
                        LIMIT :limit
                    )
                    UPDATE collection_operations AS operation
                    SET status = CASE
                            WHEN candidates.step_started_at IS NULL THEN 'rejected'
                            ELSE 'indeterminate'
                        END,
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = :now,
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        state_revision = operation.state_revision + 1,
                        updated_at = :now
                    FROM candidates
                    WHERE operation.id = candidates.id
                    RETURNING operation.id
                    """
                ),
                {
                    "topic": COLLECTION_OPERATION_TOPIC,
                    "limit": max(1, int(limit)),
                    "error_json": canonical_json({"type": "collection_operation_dispatch_exhausted"}),
                    "now": timestamp,
                },
            ).all()
            uow.commit()
            return len(rows)

    def _defer(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        status: CollectionOperationStatus,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        preserve_step_started: bool,
        now: datetime | None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="collection operation state")
        normalized_error = normalize_durable_json_object(dict(error), field="collection operation error")
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = :status, state_json = CAST(:state_json AS jsonb), "
                "error_json = CAST(:error_json AS jsonb), available_at = :available_at, "
                + ("" if preserve_step_started else "step_started_at = NULL, ")
                + "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
                {
                    "status": status.value,
                    "state_json": canonical_json(normalized_state),
                    "error_json": canonical_json(normalized_error),
                    "available_at": timestamp + timedelta(seconds=max(0.0, float(retry_after_seconds))),
                },
            )
            self._requeue_outbox(
                uow,
                operation_id,
                timestamp + timedelta(seconds=max(0.0, float(retry_after_seconds))),
            )
            uow.commit()
            return _operation(row)

    def _mark_terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        status: CollectionOperationStatus,
        state: Mapping[str, Any],
        result: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
        now: datetime | None,
    ) -> CollectionOperation:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            row = self._terminal_update(
                uow,
                operation_id,
                claim_token,
                status=status,
                state=state,
                result=result,
                error=error,
                now=timestamp,
            )
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    def _terminal_update(
        self,
        uow: UnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        *,
        status: CollectionOperationStatus,
        state: Mapping[str, Any],
        result: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
        now: datetime,
    ) -> RowMapping:
        normalized_state = normalize_durable_json_object(dict(state), field="collection operation state")
        normalized_result = (
            normalize_durable_json_object(dict(result), field="collection operation result")
            if result is not None
            else None
        )
        normalized_error = (
            normalize_durable_json_object(dict(error), field="collection operation error")
            if error is not None
            else None
        )
        return self._claimed_update(
            uow,
            operation_id,
            claim_token,
            "status = :status, step_started_at = NULL, "
            "state_json = CAST(:state_json AS jsonb), result_json = CAST(:result_json AS jsonb), "
            "error_json = CAST(:error_json AS jsonb), finished_at = :now, "
            "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
            {
                "status": status.value,
                "state_json": canonical_json(normalized_state),
                "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                "now": now,
            },
        )

    def _reserve_in_uow(
        self,
        uow: UnitOfWork,
        intent: CollectionOperationIntent,
    ) -> CollectionOperationReservation:
        uow.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope_key, 0))"),
            {"scope_key": intent.scope_key},
        )
        active_key = uow.session.scalar(
            text(
                """
                SELECT operation_key
                FROM collection_operations
                WHERE scope_key = :scope_key
                  AND status IN ('ready','claimed','requesting','awaiting_readback','retryable','indeterminate')
                LIMIT 1
                """
            ),
            {"scope_key": intent.scope_key},
        )
        if active_key is not None and str(active_key) != intent.operation_key:
            raise CollectionOperationScopeBusyError(
                f"collection operation scope {intent.scope_key!r} already has an active or indeterminate operation"
            )
        row = (
            uow.session.execute(
                text(
                    """
                    INSERT INTO collection_operations (
                        id, operation_key, request_hash, kind, scope_key,
                        request_json, status, current_step, available_at
                    ) VALUES (
                        :id, :operation_key, :request_hash, :kind, :scope_key,
                        CAST(:request_json AS jsonb), 'ready', 'reserved', :available_at
                    )
                    ON CONFLICT (operation_key) DO NOTHING
                    RETURNING *
                    """
                ),
                {
                    "id": intent.id,
                    "operation_key": intent.operation_key,
                    "request_hash": intent.request_hash,
                    "kind": intent.kind.value,
                    "scope_key": intent.scope_key,
                    "request_json": canonical_json(dict(intent.request), field="collection operation request"),
                    "available_at": intent.available_at,
                },
            )
            .mappings()
            .one_or_none()
        )
        created = row is not None
        if row is None:
            row = (
                uow.session.execute(
                    text("SELECT * FROM collection_operations WHERE operation_key = :operation_key FOR UPDATE"),
                    {"operation_key": intent.operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise CollectionOperationStateError("operation conflict was not visible")
            if (
                bytes(row["request_hash"]) != intent.request_hash
                or str(row["kind"]) != intent.kind.value
                or str(row["scope_key"]) != intent.scope_key
            ):
                raise CollectionOperationConflictError("idempotency key was already used for another request")
        operation = _operation(row)
        if created:
            self._insert_outbox(uow, operation)
        return CollectionOperationReservation(operation, created=created)

    def _read_in_uow(
        self,
        uow: UnitOfWork,
        operation_id: UUID,
        *,
        lock: bool = False,
    ) -> CollectionOperation | None:
        suffix = " FOR UPDATE" if lock else ""
        row = (
            uow.session.execute(
                text(f"SELECT * FROM collection_operations WHERE id = :id{suffix}"),
                {"id": operation_id},
            )
            .mappings()
            .one_or_none()
        )
        return _operation(row) if row is not None else None

    def _read_by_key_in_uow(
        self,
        uow: UnitOfWork,
        operation_key: str,
        *,
        lock: bool,
    ) -> CollectionOperation | None:
        suffix = " FOR UPDATE" if lock else ""
        row = (
            uow.session.execute(
                text(f"SELECT * FROM collection_operations WHERE operation_key = :key{suffix}"),
                {"key": operation_key},
            )
            .mappings()
            .one_or_none()
        )
        return _operation(row) if row is not None else None

    def _require_live_claim(
        self,
        uow: UnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        now: datetime,
    ) -> CollectionOperation:
        operation = self._read_in_uow(uow, operation_id, lock=True)
        if operation is None:
            raise CollectionOperationStateError("collection operation was not found")
        if (
            operation.status not in {CollectionOperationStatus.CLAIMED, CollectionOperationStatus.REQUESTING}
            or operation.claim_token != claim_token
        ):
            raise CollectionOperationStateError("collection operation lost its active claim")
        if operation.lease_expires_at is None or operation.lease_expires_at <= now:
            raise CollectionOperationStateError("collection operation claim lease expired")
        return operation

    @staticmethod
    def _claimed_update(
        uow: UnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        assignments: str,
        values: Mapping[str, Any],
    ) -> RowMapping:
        row = (
            uow.session.execute(
                text(
                    f"""
                    UPDATE collection_operations
                    SET {assignments}, state_revision = state_revision + 1, updated_at = NOW()
                    WHERE id = :id AND status IN ('claimed','requesting')
                      AND claim_token = :claim_token
                    RETURNING *
                    """
                ),
                {"id": operation_id, "claim_token": claim_token, **dict(values)},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CollectionOperationStateError("collection operation transition lost its claim")
        return row

    @staticmethod
    def _insert_outbox(uow: UnitOfWork, operation: CollectionOperation) -> None:
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
                """
            ),
            {
                "id": uuid4(),
                "event_key": f"{COLLECTION_OPERATION_TOPIC}:{operation.id}",
                "topic": COLLECTION_OPERATION_TOPIC,
                "aggregate_type": COLLECTION_OPERATION_AGGREGATE_TYPE,
                "aggregate_id": str(operation.id),
                "payload_json": canonical_json(payload),
                "available_at": operation.available_at,
            },
        )

    @staticmethod
    def _complete_outbox(uow: UnitOfWork, operation_id: UUID) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE event_key = :event_key AND status <> 'dead'
                """
            ),
            {"event_key": f"{COLLECTION_OPERATION_TOPIC}:{operation_id}"},
        )

    @staticmethod
    def _requeue_outbox(
        uow: UnitOfWork,
        operation_id: UUID,
        available_at: datetime,
    ) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'pending', available_at = :available_at,
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    published_at = NULL, processed_at = NULL, updated_at = NOW()
                WHERE event_key = :event_key AND status <> 'dead'
                """
            ),
            {
                "event_key": f"{COLLECTION_OPERATION_TOPIC}:{operation_id}",
                "available_at": available_at,
            },
        )


def _operation(row: RowMapping) -> CollectionOperation:
    return CollectionOperation(
        id=UUID(str(row["id"])),
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        kind=CollectionOperationKind(row["kind"]),
        scope_key=str(row["scope_key"]),
        request=dict(row["request_json"] or {}),
        status=CollectionOperationStatus(row["status"]),
        current_step=CollectionOperationStep(row["current_step"]),
        state_revision=int(row["state_revision"]),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=UUID(str(row["claim_token"])) if row["claim_token"] is not None else None,
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        step_started_at=row["step_started_at"],
        state=dict(row["state_json"] or {}),
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error=dict(row["error_json"]) if row["error_json"] is not None else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _deleted_sources(native: RowMapping | None, policy: RowMapping | None) -> tuple[str, ...]:
    sources: list[str] = []
    if native is not None:
        sources.extend(
            normalized
            for value in (native.get("poster_path"), native.get("backdrop_path"))
            if (normalized := _tmdb_image_url(value)) is not None
        )
    images = policy.get("images_json") if policy is not None else None
    if isinstance(images, list):
        sources.extend(
            str(item.get("source_url") or "").strip()
            for item in images
            if isinstance(item, Mapping) and str(item.get("source_url") or "").strip()
        )
    return tuple(dict.fromkeys(sources))


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"collection operation {field} must not be empty")
    return normalized


def _tmdb_image_url(value: object) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if normalized.startswith(("http://", "https://")):
        return normalized
    return f"https://image.tmdb.org/t/p/original/{normalized.lstrip('/')}"


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        raise ValueError("collection operation timestamp must be timezone-aware")
    return value.astimezone(UTC)


__all__ = ["PostgresCollectionOperationRepository"]
