from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json

from .models import P115AuthorizationSession
from .playback import (
    PLAYBACK_AGGREGATE_TYPE,
    PLAYBACK_TOPIC,
    P115PlaybackConflictError,
    P115PlaybackNotFoundError,
    P115PlaybackStateError,
    PlaybackArtifact,
    PlaybackArtifactIntent,
    PlaybackArtifactStatus,
    PlaybackLease,
    PlaybackLeaseIntent,
    PlaybackLeaseReservation,
    PlaybackLeaseStatus,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationIntent,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackQrSession,
    PlaybackQrStatus,
    PlaybackReservation,
    PlaybackTransition,
    safe_playback_json,
    safe_receipt,
    sha256_json,
)
from .playback import operation_id as playback_operation_id

_CLAIMABLE = ("ready", "retryable", "awaiting_readback", "remote_confirmed")
_TERMINAL = ("confirmed", "rejected", "indeterminate")
_PROVIDER_SESSION_STABLE_FIELDS = (
    "artifact_key",
    "provider_request_id",
    "remote_fid",
    "request_id",
    "target_cid",
)


class PostgresP115PlaybackRepository:
    """PostgreSQL UoW owner for durable P115 playback state and dispatch."""

    def __init__(self, uow_factory: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    def reserve(self, intent: PlaybackOperationIntent) -> PlaybackReservation:
        request = safe_playback_json(intent.request, field="playback operation request")
        _operation_intent(intent)
        if bytes(intent.request_hash) != bytes.fromhex(sha256_json(request)):
            raise ValueError("request_hash does not match the canonical playback request")
        with self._uow_factory() as uow:
            _advisory_lock(uow, f"p115-playback-operation:{intent.operation_key}")
            current = _operation_by_key(uow, intent.operation_key, for_update=True)
            if current is not None:
                operation = _operation_projection(current)
                if operation.request_hash != bytes(intent.request_hash):
                    raise P115PlaybackConflictError("playback operation_key was reused with a different request hash")
                if (
                    operation.account_id != intent.account_id
                    or operation.provider_backend != intent.provider_backend
                    or operation.kind is not intent.kind
                    or operation.mode is not intent.mode
                    or operation.artifact_key != intent.artifact_key
                    or operation.artifact_generation != intent.artifact_generation
                ):
                    raise P115PlaybackConflictError("playback operation identity changed")
                return PlaybackReservation(operation, created=False)

            if intent.kind is PlaybackOperationKind.RECLAIM:
                artifact = _lock_reclaim_artifact(uow, intent)
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_artifacts
                        SET status = 'reclaim_pending', state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :artifact_id
                        """
                    ),
                    {"artifact_id": artifact["id"]},
                )

            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_playback_operations (
                            id, operation_key, request_hash, kind, mode, account_id,
                            provider_backend, request_json, artifact_key,
                            artifact_generation, status, available_at
                        ) VALUES (
                            :id, :operation_key, :request_hash, :kind, :mode, :account_id,
                            :provider_backend, CAST(:request_json AS jsonb), :artifact_key,
                            :artifact_generation, 'ready', :available_at
                        )
                        RETURNING *
                        """
                    ),
                    {
                        "id": playback_operation_id(intent.operation_key),
                        "operation_key": intent.operation_key,
                        "request_hash": bytes(intent.request_hash),
                        "kind": intent.kind.value,
                        "mode": intent.mode.value,
                        "account_id": intent.account_id,
                        "provider_backend": intent.provider_backend,
                        "request_json": canonical_json(request, field="playback operation request"),
                        "artifact_key": intent.artifact_key,
                        "artifact_generation": intent.artifact_generation,
                        "available_at": intent.available_at,
                    },
                )
                .mappings()
                .one()
            )
            operation = _operation_projection(row)
            _stage_outbox(uow, operation)
            uow.commit()
            return PlaybackReservation(operation, created=True)
        raise AssertionError("unreachable")

    def read(self, operation_id: UUID) -> PlaybackOperation | None:
        with self._uow_factory() as uow:
            row = _operation_row(uow, operation_id)
            return _operation_projection(row) if row is not None else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> PlaybackOperation | None:
        worker = _required(worker_id, "worker_id", 255)
        _positive_seconds(lease_seconds, "lease_seconds")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_operations
                        SET status = CASE WHEN status IN ('ready','retryable') THEN 'claimed' ELSE status END,
                            claimed_by = :worker_id, claim_token = :claim_token,
                            claimed_at = NOW(),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            attempts = attempts + 1, state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :operation_id
                          AND status = ANY(CAST(:statuses AS text[]))
                          AND claim_token IS NULL AND available_at <= NOW()
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "worker_id": worker,
                        "claim_token": uuid4(),
                        "lease_seconds": lease_seconds,
                        "statuses": list(_CLAIMABLE),
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

    def checkpoint_request(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        session: Mapping[str, Any],
        lease_seconds: float,
    ) -> PlaybackTransition:
        payload = safe_playback_json(session, field="playback provider session")
        _positive_seconds(lease_seconds, "lease_seconds")
        with self._uow_factory() as uow:
            current = _require_claim(uow, operation_id, claim_token)
            if current.status not in {
                PlaybackOperationStatus.CLAIMED,
                PlaybackOperationStatus.REQUESTING,
            }:
                raise P115PlaybackStateError(
                    f"playback operation {operation_id} cannot checkpoint from {current.status.value}"
                )
            merged = _merge_provider_session(current, payload)
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_operations
                        SET status = 'requesting',
                            provider_session_json = CAST(:session_json AS jsonb),
                            provider_request_started_at = COALESCE(provider_request_started_at, NOW()),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            error_json = NULL, state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :operation_id AND claim_token = :claim_token
                          AND lease_expires_at > NOW()
                          AND status IN ('claimed','requesting')
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "session_json": canonical_json(merged, field="playback provider session"),
                        "lease_seconds": lease_seconds,
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _claim_transition(uow, row, operation_id, claim_token, "checkpoint request")
            uow.commit()
            return PlaybackTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def mark_remote_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        receipt: Mapping[str, Any],
        *,
        readback: Mapping[str, Any] | None = None,
    ) -> PlaybackTransition:
        normalized_receipt = safe_receipt(receipt, field="playback remote receipt")
        normalized_readback = safe_receipt(readback, field="playback readback") if readback is not None else None
        return self._claimed_update(
            operation_id,
            claim_token,
            """
            status = 'remote_confirmed',
            provider_request_started_at = COALESCE(provider_request_started_at, NOW()),
            remote_receipt_json = CAST(:receipt_json AS jsonb),
            readback_json = CAST(:readback_json AS jsonb), error_json = NULL
            """,
            allowed=(
                PlaybackOperationStatus.CLAIMED,
                PlaybackOperationStatus.REQUESTING,
                PlaybackOperationStatus.AWAITING_READBACK,
                PlaybackOperationStatus.REMOTE_CONFIRMED,
            ),
            params={
                "receipt_json": canonical_json(normalized_receipt, field="playback remote receipt"),
                "readback_json": (
                    canonical_json(normalized_readback, field="playback readback")
                    if normalized_readback is not None
                    else None
                ),
            },
            action="mark remote confirmed",
        )

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
    ) -> PlaybackTransition:
        normalized = safe_playback_json(error, field="playback retry error")
        _nonnegative_seconds(retry_after_seconds, "retry_after_seconds")
        transition = self._claimed_update(
            operation_id,
            claim_token,
            """
            status = 'retryable', available_at = NOW() + make_interval(secs => :retry_after_seconds),
            error_json = CAST(:error_json AS jsonb), claimed_by = NULL,
            claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL
            """,
            allowed=(
                PlaybackOperationStatus.CLAIMED,
                PlaybackOperationStatus.AWAITING_READBACK,
                PlaybackOperationStatus.REMOTE_CONFIRMED,
            ),
            params={
                "retry_after_seconds": retry_after_seconds,
                "error_json": canonical_json(normalized, field="playback retry error"),
            },
            action="mark retryable",
            stage=True,
        )
        return transition

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        session: Mapping[str, Any] | None,
        error: Mapping[str, Any],
        retry_after_seconds: float,
    ) -> PlaybackTransition:
        normalized_session = (
            safe_playback_json(session, field="playback provider session") if session is not None else None
        )
        normalized_error = safe_playback_json(error, field="playback readback error")
        _nonnegative_seconds(retry_after_seconds, "retry_after_seconds")
        with self._uow_factory() as uow:
            current = _require_claim(uow, operation_id, claim_token)
            if current.status not in {
                PlaybackOperationStatus.CLAIMED,
                PlaybackOperationStatus.REQUESTING,
                PlaybackOperationStatus.AWAITING_READBACK,
            }:
                raise P115PlaybackStateError(
                    f"playback operation {operation_id} cannot await readback from {current.status.value}"
                )
            merged = (
                _merge_provider_session(current, normalized_session)
                if normalized_session is not None
                else current.provider_session
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_operations
                        SET status = 'awaiting_readback',
                            provider_request_started_at = COALESCE(provider_request_started_at, NOW()),
                            provider_session_json = CAST(:session_json AS jsonb),
                            error_json = CAST(:error_json AS jsonb),
                            available_at = NOW() + make_interval(secs => :retry_after_seconds),
                            claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                            lease_expires_at = NULL, state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :operation_id AND claim_token = :claim_token
                          AND lease_expires_at > NOW()
                          AND status IN ('claimed','requesting','awaiting_readback')
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "session_json": (
                            canonical_json(merged, field="playback provider session") if merged is not None else None
                        ),
                        "error_json": canonical_json(normalized_error, field="playback readback error"),
                        "retry_after_seconds": retry_after_seconds,
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _claim_transition(uow, row, operation_id, claim_token, "mark awaiting readback")
            _stage_outbox(uow, operation)
            uow.commit()
            return PlaybackTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        artifact: PlaybackArtifactIntent | None = None,
    ) -> PlaybackTransition:
        normalized_result = safe_playback_json(result, field="playback result")
        with self._uow_factory() as uow:
            current = _require_claim(uow, operation_id, claim_token)
            if current.status is PlaybackOperationStatus.CONFIRMED:
                return PlaybackTransition(current, changed=False)
            if current.status not in {
                PlaybackOperationStatus.CLAIMED,
                PlaybackOperationStatus.REMOTE_CONFIRMED,
            }:
                raise P115PlaybackStateError(
                    f"playback operation {operation_id} cannot be confirmed from {current.status.value}"
                )
            if artifact is not None:
                _validate_artifact_intent(artifact, current)
                _insert_artifact(uow, artifact)
            elif current.kind is PlaybackOperationKind.RECLAIM:
                _mark_reclaim_artifact_confirmed(uow, current)
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_operations
                        SET status = 'confirmed', result_json = CAST(:result_json AS jsonb),
                            error_json = NULL, artifact_key = COALESCE(:artifact_key, artifact_key),
                            artifact_generation = COALESCE(:artifact_generation, artifact_generation),
                            claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                            lease_expires_at = NULL, finished_at = NOW(),
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id AND claim_token = :claim_token
                          AND lease_expires_at > NOW()
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "result_json": canonical_json(normalized_result, field="playback result"),
                        "artifact_key": artifact.artifact_key if artifact is not None else None,
                        "artifact_generation": artifact.generation if artifact is not None else None,
                    },
                )
                .mappings()
                .one()
            )
            _complete_outbox(uow, operation_id)
            uow.commit()
            return PlaybackTransition(_operation_projection(row), changed=True)
        raise AssertionError("unreachable")

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
    ) -> PlaybackTransition:
        return self._terminal(operation_id, claim_token, PlaybackOperationStatus.REJECTED, error)

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
    ) -> PlaybackTransition:
        return self._terminal(operation_id, claim_token, PlaybackOperationStatus.INDETERMINATE, error)

    def artifact_by_key(self, artifact_key: str) -> PlaybackArtifact | None:
        key = _digest(artifact_key, "artifact_key")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM p115_playback_artifacts
                        WHERE artifact_key = :artifact_key
                        ORDER BY generation DESC LIMIT 1
                        """
                    ),
                    {"artifact_key": key},
                )
                .mappings()
                .one_or_none()
            )
            return _artifact_projection(row) if row is not None else None

    def artifact(self, artifact_id: UUID) -> PlaybackArtifact | None:
        with self._uow_factory() as uow:
            row = _artifact_row(uow, artifact_id)
            return _artifact_projection(row) if row is not None else None

    def latest_artifact_generation(self, artifact_key: str) -> int:
        key = _digest(artifact_key, "artifact_key")
        with self._uow_factory() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT GREATEST(
                        COALESCE((
                            SELECT MAX(generation) FROM p115_playback_artifacts
                            WHERE artifact_key = :artifact_key
                        ), 0),
                        COALESCE((
                            SELECT MAX(artifact_generation) FROM p115_playback_operations
                            WHERE artifact_key = :artifact_key AND kind = 'prepare'
                        ), 0)
                    )
                    """
                ),
                {"artifact_key": key},
            )
            return int(value or 0)
        raise AssertionError("unreachable")

    def reserve_lease(self, intent: PlaybackLeaseIntent) -> PlaybackLeaseReservation:
        _lease_intent(intent)
        with self._uow_factory() as uow:
            _advisory_lock(uow, f"p115-playback-lease:{intent.lease_key}")
            existing = _lease_by_key(uow, intent.lease_key, for_update=True)
            if existing is not None:
                lease = _lease_projection(existing)
                if (
                    lease.mode is not intent.mode
                    or lease.owner_user_id != intent.owner_user_id
                    or lease.item_id != intent.item_id
                    or lease.play_session_id != intent.play_session_id
                    or lease.client_key != intent.client_key
                    or lease.user_agent_hash != intent.user_agent_hash
                ):
                    raise P115PlaybackConflictError("playback lease identity changed")
                artifact = _artifact_row(uow, intent.artifact_id, for_update=True)
                if artifact is None or str(artifact["status"]) != PlaybackArtifactStatus.AVAILABLE.value:
                    raise P115PlaybackStateError("playback lease requires an available artifact")
                if PlaybackMode(str(artifact["mode"])) is not intent.mode:
                    raise P115PlaybackConflictError("playback lease artifact mode changed")
                if lease.status is PlaybackLeaseStatus.ACTIVE:
                    if lease.artifact_id != intent.artifact_id:
                        raise P115PlaybackConflictError("active playback lease artifact changed")
                    if intent.expires_at > lease.expires_at:
                        existing = (
                            uow.session.execute(
                                text(
                                    """
                                    UPDATE p115_playback_leases
                                    SET expires_at = :expires_at, cleanup_not_before = :expires_at,
                                        state_revision = state_revision + 1, updated_at = NOW()
                                    WHERE id = :lease_id AND status = 'active' RETURNING *
                                    """
                                ),
                                {"lease_id": lease.id, "expires_at": intent.expires_at},
                            )
                            .mappings()
                            .one()
                        )
                        lease = _lease_projection(existing)
                        uow.commit()
                    return PlaybackLeaseReservation(lease, created=False)

                if lease.artifact_id != intent.artifact_id:
                    previous_artifact = _artifact_row(uow, lease.artifact_id)
                    if (
                        previous_artifact is None
                        or str(previous_artifact["artifact_key"]) != str(artifact["artifact_key"])
                        or str(previous_artifact["status"]) != PlaybackArtifactStatus.RECLAIMED.value
                        or int(artifact["generation"]) <= int(previous_artifact["generation"])
                    ):
                        raise P115PlaybackConflictError("terminal playback lease artifact generation changed")
                existing = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_playback_leases
                            SET artifact_id = :artifact_id, status = 'active', source = :source,
                                expires_at = :expires_at, cleanup_not_before = :expires_at,
                                released_at = NULL, state_revision = state_revision + 1,
                                updated_at = NOW()
                            WHERE id = :lease_id AND status IN ('released','expired')
                            RETURNING *
                            """
                        ),
                        {
                            "lease_id": lease.id,
                            "artifact_id": intent.artifact_id,
                            "source": intent.source,
                            "expires_at": intent.expires_at,
                        },
                    )
                    .mappings()
                    .one()
                )
                lease = _lease_projection(existing)
                uow.commit()
                return PlaybackLeaseReservation(lease, created=False)

            artifact = _artifact_row(uow, intent.artifact_id, for_update=True)
            if artifact is None or str(artifact["status"]) != PlaybackArtifactStatus.AVAILABLE.value:
                raise P115PlaybackStateError("playback lease requires an available artifact")
            if PlaybackMode(str(artifact["mode"])) is not intent.mode:
                raise P115PlaybackConflictError("playback lease artifact mode changed")
            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_playback_leases (
                            id, lease_key, artifact_id, mode, owner_user_id, item_id,
                            play_session_id, client_key, user_agent_hash, source,
                            status, expires_at, cleanup_not_before
                        ) VALUES (
                            :id, :lease_key, :artifact_id, :mode, :owner_user_id, :item_id,
                            :play_session_id, :client_key, :user_agent_hash, :source,
                            'active', :expires_at, :expires_at
                        ) RETURNING *
                        """
                    ),
                    {
                        "id": uuid4(),
                        "lease_key": intent.lease_key,
                        "artifact_id": intent.artifact_id,
                        "mode": intent.mode.value,
                        "owner_user_id": intent.owner_user_id,
                        "item_id": intent.item_id,
                        "play_session_id": intent.play_session_id,
                        "client_key": intent.client_key,
                        "user_agent_hash": intent.user_agent_hash,
                        "source": intent.source,
                        "expires_at": intent.expires_at,
                    },
                )
                .mappings()
                .one()
            )
            uow.commit()
            return PlaybackLeaseReservation(_lease_projection(row), created=True)
        raise AssertionError("unreachable")

    def lease(self, lease_id: UUID) -> PlaybackLease | None:
        with self._uow_factory() as uow:
            row = _lease_row(uow, lease_id)
            return _lease_projection(row) if row is not None else None

    def active_counts(self, account_ids: Sequence[str]) -> Mapping[str, int]:
        ids = sorted({_required(value, "account_id", 255) for value in account_ids})
        if not ids:
            return {}
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT artifact.account_id, COUNT(*) AS total
                        FROM p115_playback_leases AS lease
                        JOIN p115_playback_artifacts AS artifact ON artifact.id = lease.artifact_id
                        WHERE lease.status = 'active' AND lease.expires_at > NOW()
                          AND artifact.account_id = ANY(CAST(:account_ids AS text[]))
                        GROUP BY artifact.account_id
                        """
                    ),
                    {"account_ids": ids},
                )
                .mappings()
                .all()
            )
            counts = {value: 0 for value in ids}
            counts.update({str(row["account_id"]): int(row["total"]) for row in rows})
            return counts
        raise AssertionError("unreachable")

    def release_matching(
        self,
        *,
        play_session_id: str,
        item_id: str,
        owner_user_id: str,
        client_key: str,
        cleanup_delays: Mapping[PlaybackMode, float],
    ) -> tuple[PlaybackLease, ...]:
        delays = _cleanup_delays(cleanup_delays)
        session_id = str(play_session_id or "").strip()
        item = str(item_id or "").strip()
        owner = str(owner_user_id or "").strip()
        client = str(client_key or "").strip()
        if not session_id and not item:
            return ()
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_leases
                        SET status = 'released', released_at = NOW(),
                            cleanup_not_before = NOW() + make_interval(secs => CASE mode
                                WHEN 'pool' THEN :pool_delay WHEN 'copy' THEN :copy_delay
                                WHEN 'virtual' THEN :virtual_delay ELSE :maintenance_delay END),
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE status = 'active'
                          AND (
                            (:play_session_id <> '' AND play_session_id = :play_session_id)
                            OR ((:play_session_id = '' OR NOT EXISTS (
                                    SELECT 1 FROM p115_playback_leases AS session_match
                                    WHERE session_match.status = 'active'
                                      AND session_match.play_session_id = :play_session_id
                                ))
                                AND :item_id <> '' AND item_id = :item_id
                                AND (:owner_user_id = '' OR owner_user_id = :owner_user_id)
                                AND (:client_key = '' OR client_key = :client_key))
                          )
                        RETURNING *
                        """
                    ),
                    {
                        "play_session_id": session_id,
                        "item_id": item,
                        "owner_user_id": owner,
                        "client_key": client,
                        **delays,
                    },
                )
                .mappings()
                .all()
            )
            uow.commit()
            return tuple(_lease_projection(row) for row in rows)
        raise AssertionError("unreachable")

    def release_lease(
        self,
        lease_id: UUID,
        *,
        cleanup_delay_seconds: float,
    ) -> PlaybackLease:
        delay = _nonnegative_seconds(cleanup_delay_seconds, "cleanup_delay_seconds")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_leases
                        SET status = 'released', released_at = NOW(),
                            cleanup_not_before = NOW() + make_interval(secs => :delay),
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :lease_id AND status = 'active'
                        RETURNING *
                        """
                    ),
                    {"lease_id": lease_id, "delay": delay},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                current = _lease_row(uow, lease_id)
                if current is None:
                    raise P115PlaybackNotFoundError(f"playback lease {lease_id} was not found")
                return _lease_projection(current)
            uow.commit()
            return _lease_projection(row)
        raise AssertionError("unreachable")

    def release_account_leases(
        self,
        account_id: str,
        *,
        cleanup_delays: Mapping[PlaybackMode, float],
    ) -> tuple[PlaybackLease, ...]:
        account = _required(account_id, "account_id", 255)
        delays = _cleanup_delays(cleanup_delays)
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_leases AS lease
                        SET status = 'released', released_at = NOW(),
                            cleanup_not_before = NOW() + make_interval(secs => CASE lease.mode
                                WHEN 'pool' THEN :pool_delay WHEN 'copy' THEN :copy_delay
                                WHEN 'virtual' THEN :virtual_delay ELSE :maintenance_delay END),
                            state_revision = lease.state_revision + 1, updated_at = NOW()
                        FROM p115_playback_artifacts AS artifact
                        WHERE lease.artifact_id = artifact.id AND artifact.account_id = :account_id
                          AND lease.status = 'active'
                        RETURNING lease.*
                        """
                    ),
                    {"account_id": account, **delays},
                )
                .mappings()
                .all()
            )
            uow.commit()
            return tuple(_lease_projection(row) for row in rows)
        raise AssertionError("unreachable")

    def expire_leases(self, *, limit: int = 100) -> tuple[PlaybackLease, ...]:
        _positive_int(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH due AS (
                            SELECT id FROM p115_playback_leases
                            WHERE status = 'active' AND expires_at <= NOW()
                            ORDER BY expires_at, id LIMIT :limit FOR UPDATE SKIP LOCKED
                        )
                        UPDATE p115_playback_leases AS lease
                        SET status = 'expired', released_at = NOW(),
                            cleanup_not_before = GREATEST(cleanup_not_before, NOW()),
                            state_revision = state_revision + 1, updated_at = NOW()
                        FROM due WHERE lease.id = due.id RETURNING lease.*
                        """
                    ),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
            uow.commit()
            return tuple(_lease_projection(row) for row in rows)
        raise AssertionError("unreachable")

    def reclaimable_artifacts(self, *, limit: int = 100) -> tuple[PlaybackArtifact, ...]:
        _positive_int(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT artifact.* FROM p115_playback_artifacts AS artifact
                        WHERE artifact.status = 'available'
                          AND artifact.reclaim_not_before <= NOW()
                          AND NOT EXISTS (
                              SELECT 1 FROM p115_playback_leases AS lease
                              WHERE lease.artifact_id = artifact.id AND lease.status = 'active'
                                AND lease.expires_at > NOW()
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM p115_playback_leases AS lease
                              WHERE lease.artifact_id = artifact.id
                                AND lease.cleanup_not_before > NOW()
                          )
                        ORDER BY artifact.reclaim_not_before, artifact.id LIMIT :limit
                        """
                    ),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
            return tuple(_artifact_projection(row) for row in rows)
        raise AssertionError("unreachable")

    def expire_stale_claims(self, *, limit: int = 100) -> int:
        _positive_int(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM p115_playback_operations
                        WHERE claim_token IS NOT NULL AND lease_expires_at <= NOW()
                        ORDER BY lease_expires_at, event_sequence, id
                        LIMIT :limit FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
            for stale in rows:
                previous = PlaybackOperationStatus(str(stale["status"]))
                if previous in {PlaybackOperationStatus.CLAIMED, PlaybackOperationStatus.READY}:
                    target = PlaybackOperationStatus.RETRYABLE
                elif previous in {
                    PlaybackOperationStatus.REQUESTING,
                    PlaybackOperationStatus.AWAITING_READBACK,
                }:
                    target = PlaybackOperationStatus.AWAITING_READBACK
                else:
                    target = previous
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_playback_operations
                            SET status = :status, available_at = NOW(),
                                claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                                lease_expires_at = NULL, error_json = CAST(:error_json AS jsonb),
                                state_revision = state_revision + 1, updated_at = NOW()
                            WHERE id = :operation_id AND claim_token = :claim_token RETURNING *
                            """
                        ),
                        {
                            "operation_id": stale["id"],
                            "claim_token": stale["claim_token"],
                            "status": target.value,
                            "error_json": canonical_json(
                                {"type": "claim_lease_expired", "recovered_from": previous.value}
                            ),
                        },
                    )
                    .mappings()
                    .one()
                )
                _stage_outbox(uow, _operation_projection(updated))
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        _positive_int(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT operation.id, operation.status
                        FROM p115_playback_operations AS operation
                        JOIN outbox_events AS event
                          ON event.event_key = :topic || ':' || operation.id::text
                         AND event.topic = :topic
                        WHERE event.status = 'dead' AND operation.claim_token IS NULL
                          AND operation.status NOT IN ('confirmed','rejected','indeterminate')
                        ORDER BY operation.event_sequence, operation.id
                        LIMIT :limit FOR UPDATE OF operation SKIP LOCKED
                        """
                    ),
                    {"topic": PLAYBACK_TOPIC, "limit": limit},
                )
                .mappings()
                .all()
            )
            for row in rows:
                previous = PlaybackOperationStatus(str(row["status"]))
                terminal = (
                    PlaybackOperationStatus.REJECTED
                    if previous in {PlaybackOperationStatus.READY, PlaybackOperationStatus.RETRYABLE}
                    else PlaybackOperationStatus.INDETERMINATE
                )
                _terminal_without_claim(
                    uow,
                    UUID(str(row["id"])),
                    terminal,
                    {"type": "outbox_dispatch_dead", "recovered_from": previous.value},
                )
                operation = _require_operation(_operation_row(uow, UUID(str(row["id"]))), UUID(str(row["id"])))
                if operation.kind is PlaybackOperationKind.RECLAIM:
                    if terminal is PlaybackOperationStatus.REJECTED:
                        _restore_reclaim_artifact(uow, operation)
                    else:
                        _mark_reclaim_artifact_indeterminate(uow, operation)
                _complete_outbox(uow, UUID(str(row["id"])))
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def expire_qr_sessions(self, *, limit: int = 100) -> int:
        _positive_int(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH due AS (
                            SELECT id FROM p115_playback_qr_sessions
                            WHERE status <> 'expired' AND expires_at <= NOW()
                            ORDER BY expires_at, id
                            LIMIT :limit FOR UPDATE SKIP LOCKED
                        )
                        UPDATE p115_playback_qr_sessions AS session
                        SET status = 'expired',
                            error_message = CASE
                                WHEN session.error_message = '' THEN 'authorization session expired'
                                ELSE session.error_message
                            END,
                            credentials_json = NULL,
                            provider_session_json = jsonb_build_object(
                                'uid', session.provider_uid,
                                'created_at', 0,
                                'time_value', '',
                                'sign', '',
                                'app_type', session.app_type,
                                'code_verifier', ''
                            ),
                            polled_at = NOW(), updated_at = NOW()
                        FROM due WHERE session.id = due.id
                        RETURNING session.id
                        """
                    ),
                    {"limit": limit},
                )
                .scalars()
                .all()
            )
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def create_qr_session(
        self,
        *,
        owner_user_id: str,
        mode: str,
        app_type: str,
        provider_session: P115AuthorizationSession,
        ttl_seconds: float,
    ) -> PlaybackQrSession:
        owner = _required(owner_user_id, "owner_user_id", 255)
        normalized_mode = _required(mode, "mode", 32)
        normalized_app = _required(app_type, "app_type", 64)
        ttl = _positive_seconds(ttl_seconds, "ttl_seconds")
        payload = _provider_session_json(provider_session)
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_playback_qr_sessions (
                            id, owner_user_id, provider_uid, mode, app_type, status,
                            provider_session_json, expires_at
                        ) VALUES (
                            :id, :owner_user_id, :provider_uid, :mode, :app_type, 'waiting',
                            CAST(:provider_session_json AS jsonb),
                            NOW() + make_interval(secs => :ttl_seconds)
                        ) RETURNING *
                        """
                    ),
                    {
                        "id": uuid4(),
                        "owner_user_id": owner,
                        "provider_uid": provider_session.uid,
                        "mode": normalized_mode,
                        "app_type": normalized_app,
                        "provider_session_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                        "ttl_seconds": ttl,
                    },
                )
                .mappings()
                .one()
            )
            uow.commit()
            return _qr_projection(row)
        raise AssertionError("unreachable")

    def qr_session(self, *, owner_user_id: str, provider_uid: str = "") -> PlaybackQrSession | None:
        owner = _required(owner_user_id, "owner_user_id", 255)
        uid = str(provider_uid or "").strip()
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM p115_playback_qr_sessions
                        WHERE owner_user_id = :owner_user_id
                          AND (:provider_uid = '' OR provider_uid = :provider_uid)
                        ORDER BY created_at DESC LIMIT 1
                        """
                    ),
                    {"owner_user_id": owner, "provider_uid": uid},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            if str(row["status"]) != "expired" and row["expires_at"] <= datetime.now(UTC):
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_playback_qr_sessions
                            SET status = 'expired', error_message = 'authorization session expired',
                                credentials_json = NULL,
                                provider_session_json = jsonb_build_object(
                                    'uid', provider_uid,
                                    'created_at', 0,
                                    'time_value', '',
                                    'sign', '',
                                    'app_type', app_type,
                                    'code_verifier', ''
                                ),
                                polled_at = NOW(), updated_at = NOW()
                            WHERE id = :session_id AND status <> 'expired' AND expires_at <= NOW()
                            RETURNING *
                            """
                        ),
                        {"session_id": row["id"]},
                    )
                    .mappings()
                    .one_or_none()
                )
                if updated is None:
                    row = _qr_row(uow, UUID(str(row["id"])))
                    if row is None:
                        return None
                else:
                    row = updated
                    uow.commit()
            return _qr_projection(row)

    def update_qr_session(
        self,
        session_id: UUID,
        *,
        status: PlaybackQrStatus,
        credentials: Mapping[str, Any] | None = None,
        error_message: str = "",
    ) -> PlaybackQrSession:
        credential_payload = _qr_credentials(credentials) if credentials is not None else None
        if status is PlaybackQrStatus.SUCCEEDED and credential_payload is None:
            raise ValueError("successful QR session requires credentials")
        if status is not PlaybackQrStatus.SUCCEEDED and credential_payload is not None:
            raise ValueError("QR credentials may only be stored for a successful session")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_qr_sessions
                        SET status = :status, credentials_json = CAST(:credentials_json AS jsonb),
                            provider_session_json = CASE WHEN :scrub_provider THEN jsonb_build_object(
                                'uid', provider_uid,
                                'created_at', 0,
                                'time_value', '',
                                'sign', '',
                                'app_type', app_type,
                                'code_verifier', ''
                            ) ELSE provider_session_json END,
                            error_message = :error_message, polled_at = NOW(), updated_at = NOW()
                        WHERE id = :session_id AND status = 'waiting' AND expires_at > NOW()
                        RETURNING *
                        """
                    ),
                    {
                        "session_id": session_id,
                        "status": status.value,
                        "scrub_provider": status is not PlaybackQrStatus.WAITING,
                        "credentials_json": (
                            json.dumps(credential_payload, ensure_ascii=False, separators=(",", ":"))
                            if credential_payload is not None
                            else None
                        ),
                        "error_message": str(error_message or "")[:2000],
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                current = _qr_row(uow, session_id)
                if current is None:
                    raise P115PlaybackNotFoundError(f"playback QR session {session_id} was not found")
                if str(current["status"]) == status.value:
                    return _qr_projection(current)
                raise P115PlaybackStateError("playback QR session is no longer waiting or has expired")
            uow.commit()
            return _qr_projection(row)
        raise AssertionError("unreachable")

    def _claimed_update(
        self,
        operation_id: UUID,
        claim_token: UUID,
        assignments: str,
        *,
        allowed: Sequence[PlaybackOperationStatus],
        params: Mapping[str, Any],
        action: str,
        stage: bool = False,
    ) -> PlaybackTransition:
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        f"""
                        UPDATE p115_playback_operations
                        SET {assignments}, state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id AND claim_token = :claim_token
                          AND lease_expires_at > NOW()
                          AND status = ANY(CAST(:statuses AS text[]))
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "statuses": [value.value for value in allowed],
                        **dict(params),
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _claim_transition(uow, row, operation_id, claim_token, action)
            if stage:
                _stage_outbox(uow, operation)
            uow.commit()
            return PlaybackTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def _terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        status: PlaybackOperationStatus,
        error: Mapping[str, Any],
    ) -> PlaybackTransition:
        normalized = safe_playback_json(error, field="playback terminal error")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_playback_operations
                        SET status = :status, result_json = NULL,
                            error_json = CAST(:error_json AS jsonb), finished_at = NOW(),
                            claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                            lease_expires_at = NULL, state_revision = state_revision + 1,
                            updated_at = NOW()
                        WHERE id = :operation_id AND claim_token = :claim_token
                          AND lease_expires_at > NOW()
                          AND status NOT IN ('confirmed','rejected','indeterminate')
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "status": status.value,
                        "error_json": canonical_json(normalized, field="playback terminal error"),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                current = _require_operation(_operation_row(uow, operation_id), operation_id)
                if current.status is status and current.error == normalized:
                    return PlaybackTransition(current, changed=False)
                raise P115PlaybackStateError(f"playback operation {operation_id} rejected {status.value}")
            operation = _operation_projection(row)
            if operation.kind is PlaybackOperationKind.RECLAIM:
                if status is PlaybackOperationStatus.INDETERMINATE:
                    _mark_reclaim_artifact_indeterminate(uow, operation)
                elif status is PlaybackOperationStatus.REJECTED:
                    _restore_reclaim_artifact(uow, operation)
            _complete_outbox(uow, operation_id)
            uow.commit()
            return PlaybackTransition(operation, changed=True)
        raise AssertionError("unreachable")


def _operation_intent(intent: PlaybackOperationIntent) -> None:
    _digest(intent.operation_key, "operation_key")
    if len(bytes(intent.request_hash)) != 32:
        raise ValueError("request_hash must contain 32 bytes")
    _required(intent.account_id, "account_id", 255)
    if intent.provider_backend not in {"openapi", "cookie"}:
        raise ValueError("provider_backend must be openapi or cookie")
    if intent.artifact_key is not None:
        _digest(intent.artifact_key, "artifact_key")
    if (intent.artifact_key is None) != (intent.artifact_generation is None):
        raise ValueError("artifact_key and artifact_generation must be supplied together")


def _merge_provider_session(
    operation: PlaybackOperation,
    incoming: Mapping[str, Any],
) -> dict[str, Any]:
    payload = safe_playback_json(incoming, field="playback provider session")
    pinned = {
        "account_id": operation.account_id,
        "backend": operation.provider_backend,
        "kind": operation.kind.value,
    }
    current = dict(operation.provider_session or {})
    for field, expected in pinned.items():
        for candidate in (current.get(field), payload.get(field)):
            if candidate is not None and str(candidate) != expected:
                raise P115PlaybackConflictError(f"playback provider session {field} changed")
    request = dict(operation.request)
    authoritative = {
        "artifact_key": operation.artifact_key or request.get("artifact_key"),
        "target_cid": request.get("target_cid"),
    }
    for field in _PROVIDER_SESSION_STABLE_FIELDS:
        previous = current.get(field)
        expected = authoritative.get(field)
        if expected not in (None, "") and previous is not None and str(previous).strip() != str(expected).strip():
            raise P115PlaybackConflictError(f"playback provider session {field} changed")
        if field not in payload:
            continue
        candidate = payload[field]
        if candidate is None or not str(candidate).strip():
            raise P115PlaybackConflictError(f"playback provider session {field} cannot be cleared")
        if previous not in (None, "") and str(candidate).strip() != str(previous).strip():
            raise P115PlaybackConflictError(f"playback provider session {field} changed")
        if expected not in (None, "") and str(candidate).strip() != str(expected).strip():
            raise P115PlaybackConflictError(f"playback provider session {field} changed")
    current.update(payload)
    current.update(pinned)
    return safe_playback_json(current, field="playback provider session")


def _lease_intent(intent: PlaybackLeaseIntent) -> None:
    _digest(intent.lease_key, "lease_key")
    _digest(intent.user_agent_hash, "user_agent_hash")
    if intent.expires_at.tzinfo is None:
        raise ValueError("expires_at must be timezone-aware")
    if intent.expires_at <= datetime.now(UTC):
        raise ValueError("expires_at must be in the future")


def _validate_artifact_intent(intent: PlaybackArtifactIntent, operation: PlaybackOperation) -> None:
    _digest(intent.artifact_key, "artifact_key")
    if intent.generation < 1:
        raise ValueError("artifact generation must be positive")
    if operation.kind is not PlaybackOperationKind.PREPARE:
        raise P115PlaybackConflictError("only prepare operations may commit playback artifacts")
    if intent.operation_id != operation.id:
        raise P115PlaybackConflictError("artifact operation_id does not match playback operation")
    if intent.artifact_key != operation.artifact_key or intent.generation != operation.artifact_generation:
        raise P115PlaybackConflictError("artifact generation does not match playback operation")
    if (
        intent.mode is not operation.mode
        or intent.account_id != operation.account_id
        or intent.provider_backend != operation.provider_backend
    ):
        raise P115PlaybackConflictError("artifact provider identity does not match playback operation")
    safe_receipt(intent.receipt, field="playback artifact receipt")


def _insert_artifact(uow: SqlAlchemyUnitOfWork, intent: PlaybackArtifactIntent) -> None:
    receipt = safe_receipt(intent.receipt, field="playback artifact receipt")
    remote_fid = _required(receipt.get("remote_fid") or receipt.get("fid"), "remote_fid", 255)
    remote_pick_code = _required(receipt.get("remote_pick_code") or receipt.get("pick_code"), "remote_pick_code", 255)
    values = {
        "id": uuid4(),
        "artifact_key": intent.artifact_key,
        "generation": intent.generation,
        "operation_id": intent.operation_id,
        "mode": intent.mode.value,
        "account_id": intent.account_id,
        "provider_backend": intent.provider_backend,
        "target_cid": _required(intent.target_cid, "target_cid", 255),
        "source_sha1": intent.source_sha1,
        "source_size": intent.source_size,
        "source_pick_code": str(intent.source_pick_code or ""),
        "source_fid": str(intent.source_fid or ""),
        "file_name": _required(intent.file_name, "file_name", 4096),
        "remote_fid": remote_fid,
        "remote_pick_code": remote_pick_code,
        "receipt_json": canonical_json(receipt, field="playback artifact receipt"),
        "reclaim_not_before": intent.reclaim_not_before,
    }
    uow.session.execute(
        text(
            """
            INSERT INTO p115_playback_artifacts (
                id, artifact_key, generation, operation_id, mode, account_id,
                provider_backend, target_cid, source_sha1, source_size,
                source_pick_code, source_fid, file_name, remote_fid,
                remote_pick_code, status, remote_receipt_json, reclaim_not_before
            ) VALUES (
                :id, :artifact_key, :generation, :operation_id, :mode, :account_id,
                :provider_backend, :target_cid, :source_sha1, :source_size,
                :source_pick_code, :source_fid, :file_name, :remote_fid,
                :remote_pick_code, 'available', CAST(:receipt_json AS jsonb), :reclaim_not_before
            )
            ON CONFLICT (artifact_key, generation) DO NOTHING
            """
        ),
        values,
    )
    row = (
        uow.session.execute(
            text(
                """
                SELECT * FROM p115_playback_artifacts
                WHERE artifact_key = :artifact_key AND generation = :generation
                FOR UPDATE
                """
            ),
            {"artifact_key": intent.artifact_key, "generation": intent.generation},
        )
        .mappings()
        .one()
    )
    if (
        UUID(str(row["operation_id"])) != intent.operation_id
        or str(row["mode"]) != intent.mode.value
        or str(row["account_id"]) != intent.account_id
        or str(row["provider_backend"]) != intent.provider_backend
        or str(row["target_cid"]) != values["target_cid"]
        or str(row["source_sha1"]) != str(intent.source_sha1)
        or int(row["source_size"]) != int(intent.source_size)
        or str(row["source_pick_code"]) != values["source_pick_code"]
        or str(row["source_fid"]) != values["source_fid"]
        or str(row["file_name"]) != values["file_name"]
        or str(row["remote_fid"]) != remote_fid
        or str(row["remote_pick_code"]) != remote_pick_code
        or str(row["status"]) != PlaybackArtifactStatus.AVAILABLE.value
        or _mapping(row["remote_receipt_json"]) != receipt
    ):
        raise P115PlaybackConflictError("playback artifact generation conflicts with another identity")


def _lock_reclaim_artifact(uow: SqlAlchemyUnitOfWork, intent: PlaybackOperationIntent) -> RowMapping:
    request = safe_playback_json(intent.request, field="playback reclaim request")
    artifact_id_text = _required(request.get("artifact_id"), "reclaim artifact_id", 64)
    try:
        artifact_id = UUID(artifact_id_text)
    except ValueError as exc:
        raise ValueError("reclaim artifact_id must be a UUID") from exc
    artifact_key = _digest(request.get("artifact_key") or intent.artifact_key, "reclaim artifact_key")
    generation = _positive_int(
        request.get("artifact_generation") or intent.artifact_generation,
        "reclaim artifact_generation",
    )
    if intent.artifact_key != artifact_key or intent.artifact_generation != generation:
        raise P115PlaybackConflictError("reclaim operation artifact identity changed")
    row = _artifact_row(uow, artifact_id, for_update=True)
    if row is None:
        raise P115PlaybackNotFoundError(f"playback artifact {artifact_id} was not found")
    if str(row["artifact_key"]) != artifact_key or int(row["generation"]) != generation:
        raise P115PlaybackConflictError("reclaim request does not identify the locked artifact")
    if str(row["status"]) != PlaybackArtifactStatus.AVAILABLE.value:
        raise P115PlaybackStateError("playback artifact is not available for reclamation")
    if row["reclaim_not_before"] > datetime.now(UTC) or _artifact_has_blocking_lease(uow, artifact_id):
        raise P115PlaybackStateError("playback artifact still has an active cleanup lease")
    return row


def _mark_reclaim_artifact_confirmed(uow: SqlAlchemyUnitOfWork, operation: PlaybackOperation) -> None:
    artifact = _reclaim_artifact_for_operation(uow, operation, for_update=True)
    if str(artifact["status"]) == PlaybackArtifactStatus.RECLAIMED.value:
        return
    if str(artifact["status"]) != PlaybackArtifactStatus.RECLAIM_PENDING.value:
        raise P115PlaybackStateError("reclaim artifact is not pending")
    if _artifact_has_blocking_lease(uow, UUID(str(artifact["id"]))):
        raise P115PlaybackStateError("reclaim artifact acquired a new cleanup lease")
    uow.session.execute(
        text(
            """
            UPDATE p115_playback_artifacts
            SET status = 'reclaimed', reclaimed_at = NOW(),
                state_revision = state_revision + 1, updated_at = NOW()
            WHERE id = :artifact_id AND status = 'reclaim_pending'
            """
        ),
        {"artifact_id": artifact["id"]},
    )


def _mark_reclaim_artifact_indeterminate(uow: SqlAlchemyUnitOfWork, operation: PlaybackOperation) -> None:
    artifact = _reclaim_artifact_for_operation(uow, operation, for_update=True)
    if str(artifact["status"]) in {
        PlaybackArtifactStatus.RECLAIMED.value,
        PlaybackArtifactStatus.INDETERMINATE.value,
    }:
        return
    uow.session.execute(
        text(
            """
            UPDATE p115_playback_artifacts
            SET status = 'indeterminate', state_revision = state_revision + 1, updated_at = NOW()
            WHERE id = :artifact_id AND status = 'reclaim_pending'
            """
        ),
        {"artifact_id": artifact["id"]},
    )


def _restore_reclaim_artifact(uow: SqlAlchemyUnitOfWork, operation: PlaybackOperation) -> None:
    artifact = _reclaim_artifact_for_operation(uow, operation, for_update=True)
    uow.session.execute(
        text(
            """
            UPDATE p115_playback_artifacts
            SET status = 'available', state_revision = state_revision + 1, updated_at = NOW()
            WHERE id = :artifact_id AND status = 'reclaim_pending'
            """
        ),
        {"artifact_id": artifact["id"]},
    )


def _reclaim_artifact_for_operation(
    uow: SqlAlchemyUnitOfWork,
    operation: PlaybackOperation,
    *,
    for_update: bool,
) -> RowMapping:
    request = safe_playback_json(operation.request, field="playback reclaim request")
    try:
        artifact_id = UUID(str(request.get("artifact_id") or ""))
    except ValueError as exc:
        raise P115PlaybackStateError("reclaim operation has an invalid artifact_id") from exc
    row = _artifact_row(uow, artifact_id, for_update=for_update)
    if row is None:
        raise P115PlaybackNotFoundError(f"playback artifact {artifact_id} was not found")
    if str(row["artifact_key"]) != str(operation.artifact_key or "") or int(row["generation"]) != int(
        operation.artifact_generation or 0
    ):
        raise P115PlaybackConflictError("reclaim operation no longer matches its artifact")
    return row


def _artifact_has_blocking_lease(uow: SqlAlchemyUnitOfWork, artifact_id: UUID) -> bool:
    return bool(
        uow.session.scalar(
            text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM p115_playback_leases
                    WHERE artifact_id = :artifact_id
                      AND ((status = 'active' AND expires_at > NOW()) OR cleanup_not_before > NOW())
                )
                """
            ),
            {"artifact_id": artifact_id},
        )
    )


def _operation_projection(row: Mapping[Any, Any]) -> PlaybackOperation:
    return PlaybackOperation(
        id=UUID(str(row["id"])),
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        kind=PlaybackOperationKind(str(row["kind"])),
        mode=PlaybackMode(str(row["mode"])),
        account_id=str(row["account_id"]),
        provider_backend=str(row["provider_backend"]),
        request=_mapping(row.get("request_json")),
        status=PlaybackOperationStatus(str(row["status"])),
        state_revision=int(row["state_revision"]),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row.get("claimed_by"),
        claim_token=UUID(str(row["claim_token"])) if row.get("claim_token") else None,
        claimed_at=row.get("claimed_at"),
        lease_expires_at=row.get("lease_expires_at"),
        provider_request_started_at=row.get("provider_request_started_at"),
        provider_session=_optional_mapping(row.get("provider_session_json")),
        remote_receipt=_optional_mapping(row.get("remote_receipt_json")),
        readback=_optional_mapping(row.get("readback_json")),
        result=_optional_mapping(row.get("result_json")),
        error=_optional_mapping(row.get("error_json")),
        finished_at=row.get("finished_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        artifact_key=row.get("artifact_key"),
        artifact_generation=int(row["artifact_generation"]) if row.get("artifact_generation") else None,
    )


def _artifact_projection(row: Mapping[Any, Any]) -> PlaybackArtifact:
    return PlaybackArtifact(
        id=UUID(str(row["id"])),
        artifact_key=str(row["artifact_key"]),
        generation=int(row["generation"]),
        operation_id=UUID(str(row["operation_id"])),
        mode=PlaybackMode(str(row["mode"])),
        account_id=str(row["account_id"]),
        provider_backend=str(row["provider_backend"]),
        target_cid=str(row["target_cid"]),
        source_sha1=str(row["source_sha1"]),
        source_size=int(row["source_size"]),
        source_pick_code=str(row["source_pick_code"]),
        source_fid=str(row["source_fid"]),
        file_name=str(row["file_name"]),
        remote_fid=str(row["remote_fid"]),
        remote_pick_code=str(row["remote_pick_code"]),
        status=PlaybackArtifactStatus(str(row["status"])),
        state_revision=int(row["state_revision"]),
        remote_receipt=_mapping(row["remote_receipt_json"]),
        reclaim_not_before=row["reclaim_not_before"],
        last_verified_at=row.get("last_verified_at"),
        reclaimed_at=row.get("reclaimed_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _lease_projection(row: Mapping[Any, Any]) -> PlaybackLease:
    return PlaybackLease(
        id=UUID(str(row["id"])),
        lease_key=str(row["lease_key"]),
        artifact_id=UUID(str(row["artifact_id"])),
        mode=PlaybackMode(str(row["mode"])),
        owner_user_id=str(row["owner_user_id"]),
        item_id=str(row["item_id"]),
        play_session_id=str(row["play_session_id"]),
        client_key=str(row["client_key"]),
        user_agent_hash=str(row["user_agent_hash"]),
        source=str(row["source"]),
        status=PlaybackLeaseStatus(str(row["status"])),
        state_revision=int(row["state_revision"]),
        expires_at=row["expires_at"],
        cleanup_not_before=row["cleanup_not_before"],
        released_at=row.get("released_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _qr_projection(row: Mapping[Any, Any]) -> PlaybackQrSession:
    provider = _mapping(row["provider_session_json"])
    return PlaybackQrSession(
        id=UUID(str(row["id"])),
        owner_user_id=str(row["owner_user_id"]),
        provider_uid=str(row["provider_uid"]),
        mode=str(row["mode"]),
        app_type=str(row["app_type"]),
        status=PlaybackQrStatus(str(row["status"])),
        provider_session=P115AuthorizationSession(
            uid=str(provider.get("uid") or ""),
            created_at=float(provider.get("created_at") or 0),
            time_value=str(provider.get("time_value") or ""),
            sign=str(provider.get("sign") or ""),
            app_type=str(provider.get("app_type") or "web"),
            code_verifier=str(provider.get("code_verifier") or ""),
        ),
        credentials=_optional_mapping(row.get("credentials_json")),
        error_message=str(row.get("error_message") or ""),
        expires_at=row["expires_at"],
        polled_at=row.get("polled_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _operation_row(uow: SqlAlchemyUnitOfWork, operation_id: UUID, *, for_update: bool = False) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text("SELECT * FROM p115_playback_operations WHERE id = :operation_id" + suffix),
            {"operation_id": operation_id},
        )
        .mappings()
        .one_or_none()
    )


def _operation_by_key(uow: SqlAlchemyUnitOfWork, operation_key: str, *, for_update: bool = False) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text("SELECT * FROM p115_playback_operations WHERE operation_key = :key" + suffix),
            {"key": operation_key},
        )
        .mappings()
        .one_or_none()
    )


def _artifact_row(uow: SqlAlchemyUnitOfWork, artifact_id: UUID, *, for_update: bool = False) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text("SELECT * FROM p115_playback_artifacts WHERE id = :artifact_id" + suffix),
            {"artifact_id": artifact_id},
        )
        .mappings()
        .one_or_none()
    )


def _lease_row(uow: SqlAlchemyUnitOfWork, lease_id: UUID) -> RowMapping | None:
    return (
        uow.session.execute(
            text("SELECT * FROM p115_playback_leases WHERE id = :lease_id"),
            {"lease_id": lease_id},
        )
        .mappings()
        .one_or_none()
    )


def _lease_by_key(uow: SqlAlchemyUnitOfWork, lease_key: str, *, for_update: bool = False) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text("SELECT * FROM p115_playback_leases WHERE lease_key = :key" + suffix),
            {"key": lease_key},
        )
        .mappings()
        .one_or_none()
    )


def _qr_row(uow: SqlAlchemyUnitOfWork, session_id: UUID) -> RowMapping | None:
    return (
        uow.session.execute(
            text("SELECT * FROM p115_playback_qr_sessions WHERE id = :session_id"),
            {"session_id": session_id},
        )
        .mappings()
        .one_or_none()
    )


def _require_operation(row: RowMapping | None, operation_id: UUID) -> PlaybackOperation:
    if row is None:
        raise P115PlaybackNotFoundError(f"playback operation {operation_id} was not found")
    return _operation_projection(row)


def _require_claim(uow: SqlAlchemyUnitOfWork, operation_id: UUID, claim_token: UUID) -> PlaybackOperation:
    operation = _require_operation(_operation_row(uow, operation_id, for_update=True), operation_id)
    if operation.claim_token != claim_token:
        raise P115PlaybackStateError(f"playback operation {operation_id} is owned by another claim")
    if operation.lease_expires_at is None or operation.lease_expires_at <= datetime.now(UTC):
        raise P115PlaybackStateError(f"playback operation {operation_id} claim lease expired")
    return operation


def _claim_transition(
    uow: SqlAlchemyUnitOfWork,
    row: RowMapping | None,
    operation_id: UUID,
    claim_token: UUID,
    action: str,
) -> PlaybackOperation:
    if row is not None:
        return _operation_projection(row)
    current = _require_operation(_operation_row(uow, operation_id), operation_id)
    if current.claim_token != claim_token:
        raise P115PlaybackStateError(f"{action} lost the playback claim")
    raise P115PlaybackStateError(f"{action} is invalid from {current.status.value}")


def _terminal_without_claim(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    status: PlaybackOperationStatus,
    error: Mapping[str, Any],
) -> None:
    normalized = safe_playback_json(error, field="playback terminal error")
    uow.session.execute(
        text(
            """
            UPDATE p115_playback_operations
            SET status = :status, result_json = NULL, error_json = CAST(:error_json AS jsonb),
                finished_at = NOW(), state_revision = state_revision + 1, updated_at = NOW()
            WHERE id = :operation_id AND claim_token IS NULL
              AND status NOT IN ('confirmed','rejected','indeterminate')
            """
        ),
        {
            "operation_id": operation_id,
            "status": status.value,
            "error_json": canonical_json(normalized, field="playback terminal error"),
        },
    )


def _stage_outbox(uow: SqlAlchemyUnitOfWork, operation: PlaybackOperation) -> None:
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
                published_at = NULL, processed_at = NULL, claimed_by = NULL,
                claim_token = NULL, claimed_at = NULL, last_error = NULL, updated_at = NOW()
            WHERE outbox_events.status <> 'dead'
            """
        ),
        {
            "id": uuid4(),
            "event_key": _outbox_key(operation.id),
            "topic": PLAYBACK_TOPIC,
            "aggregate_type": PLAYBACK_AGGREGATE_TYPE,
            "aggregate_id": str(operation.id),
            "payload_json": canonical_json(payload, field="playback outbox payload"),
            "available_at": operation.available_at,
        },
    )


def _complete_outbox(uow: SqlAlchemyUnitOfWork, operation_id: UUID) -> None:
    uow.session.execute(
        text(
            """
            UPDATE outbox_events
            SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                claimed_by = NULL, claim_token = NULL, claimed_at = NULL, updated_at = NOW()
            WHERE event_key = :event_key AND topic = :topic
              AND status IN ('pending','publishing','published','dead')
            """
        ),
        {"event_key": _outbox_key(operation_id), "topic": PLAYBACK_TOPIC},
    )


def _outbox_key(operation_id: UUID) -> str:
    return f"{PLAYBACK_TOPIC}:{operation_id}"


def _advisory_lock(uow: SqlAlchemyUnitOfWork, key: str) -> None:
    uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": key})


def _provider_session_json(session: P115AuthorizationSession) -> dict[str, Any]:
    uid = _required(session.uid, "provider uid", 512)
    return {
        "uid": uid,
        "created_at": float(session.created_at),
        "time_value": str(session.time_value or ""),
        "sign": str(session.sign or ""),
        "app_type": str(session.app_type or "web"),
        "code_verifier": str(session.code_verifier or ""),
    }


def _qr_credentials(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(dict(value), ensure_ascii=False, allow_nan=False))
    if not isinstance(payload, dict) or not payload:
        raise ValueError("QR credentials must be a non-empty JSON object")
    allowed = {"access_token", "refresh_token", "cookie", "app_type", "account_id"}
    if not set(payload).issubset(allowed):
        raise ValueError("QR credentials contain unsupported fields")
    if not any(str(payload.get(key) or "").strip() for key in ("access_token", "cookie")):
        raise ValueError("QR credentials require access_token or cookie")
    return payload


def _cleanup_delays(values: Mapping[PlaybackMode, float]) -> dict[str, float]:
    result: dict[str, float] = {}
    for mode in PlaybackMode:
        raw = values.get(mode, values.get(mode.value, 0))  # type: ignore[arg-type]
        result[f"{mode.value}_delay"] = _nonnegative_seconds(raw, f"{mode.value} cleanup delay")
    return result


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_mapping(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _required(value: Any, field: str, maximum: int) -> str:
    text_value = str(value or "").strip()
    if not text_value or len(text_value) > maximum:
        raise ValueError(f"{field} must contain between 1 and {maximum} characters")
    return text_value


def _digest(value: Any, field: str) -> str:
    text_value = str(value or "").strip().lower()
    if len(text_value) != 64:
        raise ValueError(f"{field} must be a SHA-256 digest")
    try:
        bytes.fromhex(text_value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a SHA-256 digest") from exc
    return text_value


def _positive_seconds(value: Any, field: str) -> float:
    number = float(value)
    if number <= 0:
        raise ValueError(f"{field} must be positive")
    return number


def _nonnegative_seconds(value: Any, field: str) -> float:
    number = float(value)
    if number < 0:
        raise ValueError(f"{field} must not be negative")
    return number


def _positive_int(value: Any, field: str) -> int:
    number = int(value)
    if number < 1:
        raise ValueError(f"{field} must be positive")
    return number


__all__ = ["PostgresP115PlaybackRepository"]
