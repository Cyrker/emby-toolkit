from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_json_object

from .upload_monitor import (
    UPLOAD_MONITOR_CONFIG_KEY,
    UPLOAD_MONITOR_TOPIC,
    UploadedSource,
    UploadEventType,
    UploadFileFingerprint,
    UploadMonitorConfigRevisionChanged,
    UploadMonitorControl,
    UploadMonitorIntent,
    UploadOperation,
    UploadOperationStatus,
    UploadReservation,
    UploadSource,
    UploadTransition,
    upload_monitor_config_revision,
)

_TERMINAL_STATUSES = frozenset(
    {
        UploadOperationStatus.CONFIRMED,
        UploadOperationStatus.REJECTED,
        UploadOperationStatus.INDETERMINATE,
        UploadOperationStatus.SUPERSEDED,
    }
)
_CLAIMABLE_STATUSES = (
    UploadOperationStatus.READY.value,
    UploadOperationStatus.RETRYABLE.value,
    UploadOperationStatus.AWAITING_READBACK.value,
    UploadOperationStatus.REMOTE_CONFIRMED.value,
    UploadOperationStatus.PROJECTING_LOCAL.value,
    UploadOperationStatus.INGESTION_PENDING.value,
)
_ZERO_REVISION = "0" * 64


class P115UploadRepositoryError(RuntimeError):
    pass


class P115UploadConflictError(P115UploadRepositoryError):
    pass


class P115UploadNotFoundError(P115UploadRepositoryError):
    pass


class P115UploadStateError(P115UploadRepositoryError):
    pass


class PostgresP115UploadMonitorRepository:
    """PostgreSQL UoW boundary for upload configuration, operations, Outbox, and completed sources."""

    def __init__(self, uow_factory: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    def load_config(self) -> Mapping[str, Any] | None:
        with self._uow_factory() as uow:
            value = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :setting_key"),
                {"setting_key": UPLOAD_MONITOR_CONFIG_KEY},
            )
            return dict(value) if isinstance(value, Mapping) else None

    def ensure_config(self, config: Mapping[str, Any], revision: str) -> UploadMonitorControl:
        normalized = normalize_json_object(dict(config), field="p115 upload monitor config")
        expected_revision = upload_monitor_config_revision(normalized)
        requested_revision = _revision(revision)
        if requested_revision != expected_revision:
            raise ValueError("upload monitor config revision does not match the canonical config")
        with self._uow_factory() as uow:
            _advisory_lock(uow, "p115-upload-monitor-config")
            control = _control_row(uow, for_update=True)
            if control is None:
                raise P115UploadStateError("upload monitor singleton control is missing")
            current_revision = str(control["config_revision"])
            current_config = _config_row(uow, for_update=True)
            if current_revision != "0" * 64:
                if current_config is None or upload_monitor_config_revision(current_config) != current_revision:
                    raise P115UploadStateError("upload monitor config and control revisions diverged")
                return _control_projection(control)

            uow.session.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "setting_key": UPLOAD_MONITOR_CONFIG_KEY,
                    "value_json": canonical_json(normalized, field="p115 upload monitor config"),
                },
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_monitor_controls
                        SET generation = generation + 1,
                            config_revision = :revision,
                            requested_at = NOW(),
                            updated_at = NOW()
                        WHERE singleton_key = 1 AND config_revision = repeat('0', 64)
                        RETURNING *
                        """
                    ),
                    {"revision": requested_revision},
                )
                .mappings()
                .one()
            )
            uow.commit()
            return _control_projection(row)
        raise AssertionError("unreachable")

    def save_config(self, config: Mapping[str, Any], revision: str) -> UploadMonitorControl:
        normalized = normalize_json_object(dict(config), field="p115 upload monitor config")
        expected_revision = upload_monitor_config_revision(normalized)
        requested_revision = _revision(revision)
        if requested_revision != expected_revision:
            raise ValueError("upload monitor config revision does not match the canonical config")
        with self._uow_factory() as uow:
            _advisory_lock(uow, "p115-upload-monitor-config")
            _control_row(uow, for_update=True)
            uow.session.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "setting_key": UPLOAD_MONITOR_CONFIG_KEY,
                    "value_json": canonical_json(normalized, field="p115 upload monitor config"),
                },
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_monitor_controls
                        SET generation = generation + CASE WHEN config_revision <> :revision THEN 1 ELSE 0 END,
                            config_revision = :revision,
                            requested_at = CASE WHEN config_revision <> :revision THEN NOW() ELSE requested_at END,
                            updated_at = NOW()
                        WHERE singleton_key = 1
                        RETURNING *
                        """
                    ),
                    {"revision": requested_revision},
                )
                .mappings()
                .one()
            )
            uow.commit()
            return _control_projection(row)
        raise AssertionError("unreachable")

    def control(self) -> UploadMonitorControl | None:
        with self._uow_factory() as uow:
            row = _control_row(uow)
            return _control_projection(row) if row is not None else None

    def apply_config(self, revision: str) -> UploadMonitorControl:
        requested_revision = _revision(revision)
        with self._uow_factory() as uow:
            row = _apply_config(uow, requested_revision)
            uow.commit()
            return _control_projection(row)
        raise AssertionError("unreachable")

    def reserve(self, intent: UploadMonitorIntent) -> UploadReservation:
        _validate_intent(intent)
        mapping = normalize_json_object(dict(intent.mapping), field="p115 upload mapping snapshot")
        completed_source = _json(intent.completed_source, "completed upload source snapshot")
        target_cid = _nonempty(mapping.get("target_cid"), "mapping.target_cid", maximum=255)
        with self._uow_factory() as uow:
            _advisory_lock(uow, f"p115-upload-operation:{intent.operation_key}")
            existing = _operation_by_key(uow, intent.operation_key, for_update=True)
            if existing is not None:
                operation = _operation_projection(existing)
                if operation.request_hash != bytes(intent.request_hash):
                    raise P115UploadConflictError("upload operation_key was already used with a different request hash")
                return UploadReservation(operation, created=False)

            control = _control_row(uow, for_update=True)
            if control is None:
                raise P115UploadStateError("upload monitor singleton control is missing")
            if str(control["config_revision"]) != intent.config_revision:
                raise UploadMonitorConfigRevisionChanged("upload intent config revision is stale")
            config = _config_row(uow, for_update=True)
            if config is None:
                raise P115UploadStateError("upload monitor config is missing")
            if upload_monitor_config_revision(config) != intent.config_revision:
                raise P115UploadConflictError("upload monitor config and control revisions diverged")

            fingerprint = intent.fingerprint.to_dict() if intent.fingerprint is not None else None
            identity = (
                {"device": intent.fingerprint.device, "inode": intent.fingerprint.inode}
                if intent.fingerprint is not None
                else None
            )
            status = "observed" if bool(config.get("enabled")) else "suspended"
            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_upload_operations (
                            id, operation_key, request_hash, source, event_type, correlation_id,
                            mapping_id, mapping_snapshot_json, config_revision, config_generation,
                            config_snapshot_json, path, path_key, relative_path, target_cid,
                            fingerprint_json, file_identity_json, completed_source_json,
                            status, available_at, stability_deadline
                        ) VALUES (
                            :id, :operation_key, :request_hash, :source, :event_type, :correlation_id,
                            :mapping_id, CAST(:mapping_json AS jsonb), :config_revision, :config_generation,
                            CAST(:config_json AS jsonb), :path, :path_key, :relative_path, :target_cid,
                            CAST(:fingerprint_json AS jsonb), CAST(:identity_json AS jsonb),
                            CAST(:completed_source_json AS jsonb),
                            :status, :available_at, :stability_deadline
                        )
                        RETURNING *
                        """
                    ),
                    {
                        "id": intent.id,
                        "operation_key": intent.operation_key,
                        "request_hash": bytes(intent.request_hash),
                        "source": intent.source.value,
                        "event_type": intent.event_type.value,
                        "correlation_id": intent.correlation_id,
                        "mapping_id": intent.mapping_id,
                        "mapping_json": canonical_json(mapping, field="p115 upload mapping snapshot"),
                        "config_revision": intent.config_revision,
                        "config_generation": int(control["generation"]),
                        "config_json": canonical_json(config, field="p115 upload config snapshot"),
                        "path": intent.path,
                        "path_key": intent.path_key,
                        "relative_path": intent.relative_path,
                        "target_cid": target_cid,
                        "fingerprint_json": canonical_json(fingerprint) if fingerprint is not None else None,
                        "identity_json": canonical_json(identity) if identity is not None else None,
                        "completed_source_json": (
                            canonical_json(completed_source, field="completed upload source snapshot")
                            if completed_source is not None
                            else None
                        ),
                        "status": status,
                        "available_at": intent.available_at,
                        "stability_deadline": intent.stability_deadline,
                    },
                )
                .mappings()
                .one()
            )
            uow.commit()
            return UploadReservation(_operation_projection(row), created=True)
        raise AssertionError("unreachable")

    def read(self, operation_id: UUID) -> UploadOperation | None:
        with self._uow_factory() as uow:
            row = _operation_row(uow, operation_id)
            return _operation_projection(row) if row is not None else None

    def due_for_stability(self, *, limit: int = 100) -> tuple[UploadOperation, ...]:
        _positive_integer(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_upload_operations
                        WHERE status IN ('observed', 'stability_wait')
                          AND available_at <= NOW()
                        ORDER BY available_at, event_sequence, id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
            uow.commit()
            return tuple(_operation_projection(row) for row in rows)
        raise AssertionError("unreachable")

    def record_stability_sample(
        self,
        operation_id: UUID,
        observation: UploadFileFingerprint | None,
    ) -> UploadTransition:
        with self._uow_factory() as uow:
            current_row = _operation_row(uow, operation_id, for_update=True)
            current = _require_operation(current_row, operation_id)
            if current.status not in {
                UploadOperationStatus.OBSERVED,
                UploadOperationStatus.STABILITY_WAIT,
            }:
                return UploadTransition(current, changed=False)

            now = datetime.now(UTC)
            if current.event_type is UploadEventType.DELETED:
                row = _set_ready(uow, operation_id)
                operation = _operation_projection(row)
                _stage_outbox(uow, operation)
                uow.commit()
                return UploadTransition(operation, changed=True)

            if observation is None:
                if now >= current.stability_deadline:
                    row = _terminal_without_claim(
                        uow,
                        operation_id,
                        UploadOperationStatus.REJECTED,
                        error={"type": "stability_timeout", "reason": "source_missing"},
                    )
                    _cancel_outbox(uow, operation_id)
                else:
                    row = (
                        uow.session.execute(
                            text(
                                """
                                UPDATE p115_upload_operations
                                SET status = 'stability_wait', stable_matches = 0,
                                    available_at = NOW() + INTERVAL '1 second',
                                    state_revision = state_revision + 1, updated_at = NOW()
                                WHERE id = :operation_id
                                RETURNING *
                                """
                            ),
                            {"operation_id": operation_id},
                        )
                        .mappings()
                        .one()
                    )
                uow.commit()
                return UploadTransition(_operation_projection(row), changed=True)

            previous_fingerprint = current.fingerprint
            unchanged = (
                current.last_size == observation.size
                and current.last_mtime_ns == observation.mtime_ns
                and previous_fingerprint is not None
                and previous_fingerprint.device == observation.device
                and previous_fingerprint.inode == observation.inode
            )
            matches = current.stable_matches + 1 if unchanged else 1
            ready = matches >= 3
            timed_out = now >= current.stability_deadline and not ready
            status = "ready" if ready else "rejected" if timed_out else "stability_wait"
            error = {"type": "stability_timeout", "reason": "source_changed"} if timed_out else None
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_operations
                        SET status = :status,
                            fingerprint_json = CAST(:fingerprint_json AS jsonb),
                            file_identity_json = CAST(:identity_json AS jsonb),
                            last_size = :last_size, last_mtime_ns = :last_mtime_ns,
                            stable_matches = :stable_matches,
                            available_at = CASE WHEN :status = 'stability_wait'
                                THEN NOW() + INTERVAL '1 second' ELSE NOW() END,
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = CASE WHEN :status = 'rejected' THEN NOW() ELSE NULL END,
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "status": status,
                        "fingerprint_json": canonical_json(observation.to_dict()),
                        "identity_json": canonical_json({"device": observation.device, "inode": observation.inode}),
                        "last_size": observation.size,
                        "last_mtime_ns": observation.mtime_ns,
                        "stable_matches": matches,
                        "error_json": canonical_json(error) if error is not None else None,
                    },
                )
                .mappings()
                .one()
            )
            operation = _operation_projection(row)
            if ready:
                _stage_outbox(uow, operation)
            elif timed_out:
                _cancel_outbox(uow, operation_id)
            uow.commit()
            return UploadTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def mark_ready(self, operation_id: UUID) -> UploadTransition:
        with self._uow_factory() as uow:
            current = _require_operation(_operation_row(uow, operation_id, for_update=True), operation_id)
            if current.status is UploadOperationStatus.READY:
                return UploadTransition(current, changed=False)
            if current.status not in {
                UploadOperationStatus.OBSERVED,
                UploadOperationStatus.STABILITY_WAIT,
                UploadOperationStatus.RETRYABLE,
            }:
                raise P115UploadStateError(
                    f"upload operation {operation_id} cannot become ready from {current.status.value}"
                )
            row = _set_ready(uow, operation_id)
            operation = _operation_projection(row)
            _stage_outbox(uow, operation)
            uow.commit()
            return UploadTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> UploadOperation | None:
        worker = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_operations AS operation
                        SET status = CASE WHEN status IN ('ready', 'retryable') THEN 'claimed' ELSE status END,
                            claimed_by = :worker_id, claim_token = :claim_token,
                            claimed_at = NOW(),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            attempts = attempts + 1,
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE operation.id = :operation_id
                          AND operation.status = ANY(CAST(:statuses AS text[]))
                          AND operation.claim_token IS NULL
                          AND operation.available_at <= NOW()
                          AND NOT EXISTS (
                              SELECT 1
                              FROM p115_upload_operations AS claimed
                              WHERE claimed.path_key = operation.path_key
                                AND claimed.id <> operation.id
                                AND claimed.claim_token IS NOT NULL
                          )
                        RETURNING operation.*
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "worker_id": worker,
                        "claim_token": uuid4(),
                        "lease_seconds": lease_seconds,
                        "statuses": list(_CLAIMABLE_STATUSES),
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
        backend: str,
        session: Mapping[str, Any] | None,
        lease_seconds: float = 3600,
    ) -> UploadTransition:
        normalized_backend = _nonempty(backend, "backend", maximum=255)
        if normalized_backend not in {"openapi", "cookie"}:
            raise ValueError("upload backend must be openapi or cookie")
        normalized_session = _json(session, "upload session")
        account_id = _nonempty(
            normalized_session.get("account_id") if normalized_session is not None else None,
            "upload session account_id",
            maximum=255,
        )
        if not account_id.isdigit():
            raise ValueError("upload session account_id must be numeric")
        _positive(lease_seconds, "lease_seconds")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_operations
                        SET status = 'requesting',
                            provider_backend = COALESCE(provider_backend, :backend),
                            upload_session_json = COALESCE(
                                CAST(:session_json AS jsonb), upload_session_json
                            ),
                            provider_request_started_at = COALESCE(provider_request_started_at, NOW()),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            error_json = NULL,
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                          AND claim_token = :claim_token
                          AND status IN ('claimed', 'requesting')
                          AND (provider_backend IS NULL OR provider_backend = :backend)
                          AND (
                              upload_session_json IS NULL
                              OR NOT upload_session_json ? 'account_id'
                              OR upload_session_json ->> 'account_id' = :account_id
                          )
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "backend": normalized_backend,
                        "account_id": account_id,
                        "lease_seconds": lease_seconds,
                        "session_json": (
                            canonical_json(normalized_session, field="upload session")
                            if normalized_session is not None
                            else None
                        ),
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _require_claim_transition(uow, row, operation_id, claim_token, "checkpoint request")
            uow.commit()
            return UploadTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def mark_requesting(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        backend: str,
        session: Mapping[str, Any] | None = None,
        lease_seconds: float = 3600,
    ) -> UploadTransition:
        return self.checkpoint_request(
            operation_id,
            claim_token,
            backend=backend,
            session=session,
            lease_seconds=lease_seconds,
        )

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        session: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> UploadTransition:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must not be negative")
        normalized_session = _json(session, "upload session")
        normalized_error = _json(error, "upload readback error")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_operations
                        SET status = 'awaiting_readback',
                            upload_session_json = COALESCE(
                                CAST(:session_json AS jsonb), upload_session_json
                            ),
                            error_json = COALESCE(CAST(:error_json AS jsonb), error_json),
                            available_at = NOW() + make_interval(secs => :retry_after_seconds),
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                          AND claim_token = :claim_token
                          AND status IN ('requesting', 'awaiting_readback')
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "retry_after_seconds": retry_after_seconds,
                        "session_json": (
                            canonical_json(normalized_session, field="upload session")
                            if normalized_session is not None
                            else None
                        ),
                        "error_json": (
                            canonical_json(normalized_error, field="upload readback error")
                            if normalized_error is not None
                            else None
                        ),
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _require_claim_transition(
                uow,
                row,
                operation_id,
                claim_token,
                UploadOperationStatus.AWAITING_READBACK.value,
            )
            _stage_outbox(uow, operation)
            uow.commit()
            return UploadTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def mark_remote_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        receipt: Mapping[str, Any],
    ) -> UploadTransition:
        normalized = _validated_remote_receipt(receipt)
        return self._claimed_phase_transition(
            operation_id,
            claim_token,
            UploadOperationStatus.REMOTE_CONFIRMED,
            allowed=(
                UploadOperationStatus.REQUESTING,
                UploadOperationStatus.AWAITING_READBACK,
                UploadOperationStatus.REMOTE_CONFIRMED,
            ),
            assignments="""
                remote_receipt_json = CAST(:receipt_json AS jsonb),
                remote_confirmed_at = COALESCE(remote_confirmed_at, NOW()),
                error_json = NULL
            """,
            values={"receipt_json": canonical_json(normalized, field="upload remote receipt")},
        )

    def mark_projecting_local(
        self,
        operation_id: UUID,
        claim_token: UUID,
        projection: Mapping[str, Any] | None = None,
    ) -> UploadTransition:
        normalized = _json(projection, "upload local projection")
        return self._claimed_phase_transition(
            operation_id,
            claim_token,
            UploadOperationStatus.PROJECTING_LOCAL,
            allowed=(
                UploadOperationStatus.CLAIMED,
                UploadOperationStatus.REMOTE_CONFIRMED,
                UploadOperationStatus.PROJECTING_LOCAL,
            ),
            assignments="""
                local_projection_state = 'projecting',
                local_projection_json = COALESCE(
                    CAST(:projection_json AS jsonb), local_projection_json
                ),
                error_json = NULL
            """,
            values={
                "projection_json": (
                    canonical_json(normalized, field="upload local projection") if normalized is not None else None
                )
            },
        )

    def mark_ingestion_pending(
        self,
        operation_id: UUID,
        claim_token: UUID,
        projection: Mapping[str, Any],
    ) -> UploadTransition:
        normalized = normalize_json_object(dict(projection), field="upload local projection")
        return self._claimed_phase_transition(
            operation_id,
            claim_token,
            UploadOperationStatus.INGESTION_PENDING,
            allowed=(UploadOperationStatus.PROJECTING_LOCAL, UploadOperationStatus.INGESTION_PENDING),
            assignments="""
                local_projection_state = 'ingestion_pending',
                local_projection_json = CAST(:projection_json AS jsonb),
                local_projected_at = COALESCE(local_projected_at, NOW()),
                ingestion_requested_at = COALESCE(ingestion_requested_at, NOW()),
                error_json = NULL
            """,
            values={"projection_json": canonical_json(normalized, field="upload local projection")},
        )

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        completed: UploadedSource | None,
    ) -> UploadTransition:
        normalized_result = normalize_json_object(dict(result), field="upload result")
        if completed is not None:
            _validate_completed_source(operation_id, completed)
        with self._uow_factory() as uow:
            if completed is not None:
                existing_row = _source_by_record_key(uow, completed.record_key, for_update=True)
                if existing_row is not None:
                    existing = _source_projection(existing_row)
                    if existing.operation_sequence > completed.operation_sequence:
                        transition = _terminal_transition(
                            uow,
                            operation_id,
                            claim_token,
                            UploadOperationStatus.SUPERSEDED,
                            allowed=(
                                UploadOperationStatus.REMOTE_CONFIRMED,
                                UploadOperationStatus.PROJECTING_LOCAL,
                                UploadOperationStatus.INGESTION_PENDING,
                            ),
                            error={
                                "type": "newer_completed_source_exists",
                                "existing_operation_id": str(existing.operation_id),
                                "existing_sequence": existing.operation_sequence,
                                "operation_sequence": completed.operation_sequence,
                            },
                        )
                        _cancel_outbox(uow, operation_id)
                        uow.commit()
                        return transition
            transition = _terminal_transition(
                uow,
                operation_id,
                claim_token,
                UploadOperationStatus.CONFIRMED,
                allowed=(
                    UploadOperationStatus.CLAIMED,
                    UploadOperationStatus.REMOTE_CONFIRMED,
                    UploadOperationStatus.PROJECTING_LOCAL,
                    UploadOperationStatus.INGESTION_PENDING,
                ),
                result=normalized_result,
            )
            operation = transition.operation
            if completed is not None:
                if operation.mapping_id != completed.mapping_id or operation.path_key != completed.path_key:
                    raise P115UploadConflictError("completed source does not match the upload operation")
                if transition.changed:
                    _upsert_completed_source(uow, completed)
                else:
                    existing_row = _source_by_record_key(uow, completed.record_key)
                    if existing_row is None or _source_projection(existing_row).payload() != completed.payload():
                        raise P115UploadConflictError("duplicate confirmed transition changed its completed source")
            elif operation.event_type is UploadEventType.DELETED:
                _delete_completed_source_for_operation(uow, operation)
            _cancel_outbox(uow, operation_id)
            uow.commit()
            return transition
        raise AssertionError("unreachable")

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
    ) -> UploadTransition:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must not be negative")
        normalized_error = normalize_json_object(dict(error), field="upload retry error")
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_upload_operations
                        SET status = CASE WHEN status = 'claimed' THEN 'retryable' ELSE status END,
                            available_at = NOW() + make_interval(secs => :retry_after_seconds),
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            result_json = NULL, error_json = CAST(:error_json AS jsonb),
                            finished_at = NULL,
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                          AND claim_token = :claim_token
                          AND (
                              (status = 'claimed' AND provider_request_started_at IS NULL)
                              OR status IN ('remote_confirmed', 'projecting_local', 'ingestion_pending')
                          )
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "retry_after_seconds": retry_after_seconds,
                        "error_json": canonical_json(normalized_error, field="upload retry error"),
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _require_claim_transition(uow, row, operation_id, claim_token, "mark retryable")
            _stage_outbox(uow, operation)
            uow.commit()
            return UploadTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
    ) -> UploadTransition:
        return self._terminal(
            operation_id,
            claim_token,
            UploadOperationStatus.REJECTED,
            error=error,
        )

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
    ) -> UploadTransition:
        return self._terminal(
            operation_id,
            claim_token,
            UploadOperationStatus.INDETERMINATE,
            error=error,
        )

    def mark_superseded(
        self,
        operation_id: UUID,
        claim_token: UUID | None,
        *,
        error: Mapping[str, Any],
    ) -> UploadTransition:
        normalized_error = normalize_json_object(dict(error), field="upload superseded error")
        with self._uow_factory() as uow:
            token_predicate = "claim_token IS NULL" if claim_token is None else "claim_token = :claim_token"
            row = (
                uow.session.execute(
                    text(
                        f"""
                        UPDATE p115_upload_operations
                        SET status = 'superseded',
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            result_json = NULL, error_json = CAST(:error_json AS jsonb),
                            finished_at = NOW(), state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                          AND status NOT IN ('confirmed', 'rejected', 'indeterminate', 'superseded')
                          AND {token_predicate}
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "error_json": canonical_json(normalized_error, field="upload superseded error"),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                transition = UploadTransition(_operation_projection(row), changed=True)
            else:
                current = _require_operation(_operation_row(uow, operation_id), operation_id)
                if current.status is not UploadOperationStatus.SUPERSEDED or current.error != normalized_error:
                    raise P115UploadStateError(
                        f"upload operation {operation_id} is not supersedable by this claim token"
                    )
                transition = UploadTransition(current, changed=False)
            _cancel_outbox(uow, operation_id)
            uow.commit()
            return transition
        raise AssertionError("unreachable")

    def expire_stale_claims(self, *, limit: int = 100, retry_delay_seconds: float = 0) -> int:
        _positive_integer(limit, "limit")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_upload_operations
                        WHERE claim_token IS NOT NULL AND lease_expires_at <= NOW()
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
            for stale in rows:
                status = UploadOperationStatus(str(stale["status"]))
                if status is UploadOperationStatus.CLAIMED:
                    target = UploadOperationStatus.RETRYABLE
                elif status in {UploadOperationStatus.REQUESTING, UploadOperationStatus.AWAITING_READBACK}:
                    target = UploadOperationStatus.AWAITING_READBACK
                else:
                    target = status
                row = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_upload_operations
                            SET status = :status,
                                available_at = NOW() + make_interval(secs => :retry_delay_seconds),
                                claimed_by = NULL, claim_token = NULL,
                                claimed_at = NULL, lease_expires_at = NULL,
                                error_json = CAST(:error_json AS jsonb),
                                state_revision = state_revision + 1, updated_at = NOW()
                            WHERE id = :operation_id AND claim_token = :claim_token
                            RETURNING *
                            """
                        ),
                        {
                            "operation_id": stale["id"],
                            "claim_token": stale["claim_token"],
                            "status": target.value,
                            "retry_delay_seconds": retry_delay_seconds,
                            "error_json": canonical_json(
                                {
                                    "type": "claim_lease_expired",
                                    "recovered_from": status.value,
                                    "remote_request_may_have_started": status.request_may_have_left_process,
                                }
                            ),
                        },
                    )
                    .mappings()
                    .one()
                )
                _stage_outbox(uow, _operation_projection(row))
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def synchronize_config(self, revision: str) -> tuple[int, int]:
        requested_revision = _revision(revision)
        with self._uow_factory() as uow:
            _advisory_lock(uow, "p115-upload-monitor-config")
            control = _control_row(uow, for_update=True)
            if control is None or str(control["config_revision"]) != requested_revision:
                raise P115UploadConflictError("cannot synchronize a stale upload monitor config revision")
            config = _config_row(uow, for_update=True)
            if config is None or upload_monitor_config_revision(config) != requested_revision:
                raise P115UploadConflictError("upload monitor config snapshot does not match its control revision")

            suspended_ids: tuple[UUID, ...] = ()
            resumed_ids: tuple[UUID, ...] = ()
            if not bool(config.get("enabled")):
                suspended_ids = _updated_operation_ids(
                    uow,
                    """
                    UPDATE p115_upload_operations
                    SET status = 'suspended', state_revision = state_revision + 1, updated_at = NOW()
                    WHERE claim_token IS NULL
                      AND status IN ('observed', 'stability_wait', 'ready', 'retryable')
                    RETURNING id
                    """,
                )
                for operation_id in suspended_ids:
                    _cancel_outbox(uow, operation_id)
            else:
                resumed_ids = _updated_operation_ids(
                    uow,
                    """
                    UPDATE p115_upload_operations
                    SET status = 'observed', available_at = NOW(),
                        stability_deadline = GREATEST(stability_deadline, NOW() + INTERVAL '60 seconds'),
                        stable_matches = 0, last_size = NULL, last_mtime_ns = NULL,
                        error_json = NULL, state_revision = state_revision + 1, updated_at = NOW()
                    WHERE claim_token IS NULL AND status = 'suspended'
                    RETURNING id
                    """,
                )
            _apply_config(uow, requested_revision, for_update=False)
            uow.commit()
            return len(suspended_ids), len(resumed_ids)
        raise AssertionError("unreachable")

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        _positive_integer(limit, "limit")
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT operation.id, operation.status
                        FROM p115_upload_operations AS operation
                        JOIN outbox_events AS event
                          ON event.event_key = :topic || ':' || operation.id::text
                         AND event.topic = :topic
                        WHERE event.status = 'dead'
                          AND operation.claim_token IS NULL
                          AND operation.status IN (
                              'ready', 'retryable', 'awaiting_readback',
                              'remote_confirmed', 'projecting_local', 'ingestion_pending'
                          )
                        ORDER BY operation.event_sequence, operation.id
                        LIMIT :limit
                        FOR UPDATE OF operation SKIP LOCKED
                        """
                    ),
                    {"topic": UPLOAD_MONITOR_TOPIC, "limit": limit},
                )
                .mappings()
                .all()
            )
            for row in rows:
                previous = UploadOperationStatus(str(row["status"]))
                terminal = (
                    UploadOperationStatus.REJECTED
                    if previous in {UploadOperationStatus.READY, UploadOperationStatus.RETRYABLE}
                    else UploadOperationStatus.INDETERMINATE
                )
                _terminal_without_claim(
                    uow,
                    UUID(str(row["id"])),
                    terminal,
                    error={"type": "outbox_dispatch_dead", "recovered_from": previous.value},
                )
            uow.commit()
            return len(rows)
        raise AssertionError("unreachable")

    def status(self) -> Mapping[str, Any]:
        with self._uow_factory() as uow:
            control_row = _control_row(uow)
            control = _control_projection(control_row) if control_row is not None else None
            config = _config_row(uow)
            counts = {
                str(row["status"]): int(row["total"])
                for row in uow.session.execute(
                    text("SELECT status, COUNT(*) AS total FROM p115_upload_operations GROUP BY status")
                )
                .mappings()
                .all()
            }
            completed = int(
                uow.session.scalar(text("SELECT COUNT(*) FROM p115_upload_records WHERE record_type = 'completed'"))
                or 0
            )
            pending = sum(counts.get(value, 0) for value in ("observed", "stability_wait", "ready", "claimed"))
            uploading = sum(
                counts.get(value, 0)
                for value in (
                    "requesting",
                    "awaiting_readback",
                    "remote_confirmed",
                    "projecting_local",
                    "ingestion_pending",
                )
            )
            failed = sum(counts.get(value, 0) for value in ("retryable", "rejected", "indeterminate"))
            return {
                "enabled": bool(config and config.get("enabled")),
                "config_revision": control.config_revision if control is not None else None,
                "applied_revision": control.applied_revision if control is not None else None,
                "generation": control.generation if control is not None else 0,
                "applied": control.applied if control is not None else False,
                "pending": pending,
                "uploading": uploading,
                "failed": failed,
                "completed": completed,
                "operations": counts,
            }
        raise AssertionError("unreachable")

    def source_by_path(self, mapping_id: str, path_key: str) -> UploadedSource | None:
        mapping = _nonempty(mapping_id, "mapping_id", maximum=512)
        normalized_path = _nonempty(path_key, "path_key", maximum=4096)
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_upload_records
                        WHERE record_type = 'completed'
                          AND mapping_id = :mapping_id
                          AND (
                              payload_json ->> 'path_key' = :path_key
                              OR (NOT payload_json ? 'path_key' AND local_path = :path_key)
                          )
                        ORDER BY updated_at DESC NULLS LAST, record_key
                        LIMIT 1
                        """
                    ),
                    {"mapping_id": mapping, "path_key": normalized_path},
                )
                .mappings()
                .one_or_none()
            )
            return _source_projection(row) if row is not None else None

    def sources_by_pick_codes(self, values: Sequence[str]) -> tuple[UploadedSource, ...]:
        pick_codes = sorted({_nonempty(value, "pick_code", maximum=255) for value in values})
        if not pick_codes:
            return ()
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_upload_records
                        WHERE record_type = 'completed' AND pick_code = ANY(CAST(:pick_codes AS text[]))
                        ORDER BY record_key
                        """
                    ),
                    {"pick_codes": pick_codes},
                )
                .mappings()
                .all()
            )
            return tuple(_source_projection(row) for row in rows)
        raise AssertionError("unreachable")

    def remove_sources(self, record_keys: Sequence[str]) -> int:
        keys = sorted({_nonempty(value, "record_key", maximum=4096) for value in record_keys})
        if not keys:
            return 0
        with self._uow_factory() as uow:
            rows = uow.session.execute(
                text(
                    """
                    DELETE FROM p115_upload_records
                    WHERE record_type = 'completed' AND record_key = ANY(CAST(:record_keys AS text[]))
                    RETURNING record_key
                    """
                ),
                {"record_keys": keys},
            ).all()
            count = len(rows)
            uow.commit()
            return count
        raise AssertionError("unreachable")

    def remove_sources_if_current(self, sources: Sequence[UploadedSource]) -> int:
        unique = {source.record_key: source for source in sources}
        if not unique:
            return 0
        removed = 0
        with self._uow_factory() as uow:
            for record_key in sorted(unique):
                expected = unique[record_key]
                row = _source_by_record_key(uow, record_key, for_update=True)
                if row is None or _source_projection(row).payload() != expected.payload():
                    continue
                deleted = (
                    uow.session.execute(
                        text(
                            """
                            DELETE FROM p115_upload_records
                            WHERE record_key = :record_key AND record_type = 'completed'
                            RETURNING record_key
                            """
                        ),
                        {"record_key": record_key},
                    )
                    .mappings()
                    .one_or_none()
                )
                removed += int(deleted is not None)
            uow.commit()
            return removed
        raise AssertionError("unreachable")

    def _claimed_phase_transition(
        self,
        operation_id: UUID,
        claim_token: UUID,
        status: UploadOperationStatus,
        *,
        allowed: Sequence[UploadOperationStatus],
        assignments: str,
        values: Mapping[str, Any],
    ) -> UploadTransition:
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        f"""
                        UPDATE p115_upload_operations
                        SET status = :status,
                            {assignments},
                            state_revision = state_revision + 1, updated_at = NOW()
                        WHERE id = :operation_id
                          AND claim_token = :claim_token
                          AND status = ANY(CAST(:allowed AS text[]))
                        RETURNING *
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "claim_token": claim_token,
                        "status": status.value,
                        "allowed": [value.value for value in allowed],
                        **dict(values),
                    },
                )
                .mappings()
                .one_or_none()
            )
            operation = _require_claim_transition(uow, row, operation_id, claim_token, status.value)
            uow.commit()
            return UploadTransition(operation, changed=True)
        raise AssertionError("unreachable")

    def _terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        status: UploadOperationStatus,
        *,
        error: Mapping[str, Any],
    ) -> UploadTransition:
        normalized_error = normalize_json_object(dict(error), field=f"upload {status.value} error")
        with self._uow_factory() as uow:
            transition = _terminal_transition(
                uow,
                operation_id,
                claim_token,
                status,
                allowed=(
                    UploadOperationStatus.CLAIMED,
                    UploadOperationStatus.REQUESTING,
                    UploadOperationStatus.AWAITING_READBACK,
                    UploadOperationStatus.REMOTE_CONFIRMED,
                    UploadOperationStatus.PROJECTING_LOCAL,
                    UploadOperationStatus.INGESTION_PENDING,
                ),
                error=normalized_error,
            )
            _cancel_outbox(uow, operation_id)
            uow.commit()
            return transition
        raise AssertionError("unreachable")


def upload_outbox_event_key(operation_id: UUID) -> str:
    return f"{UPLOAD_MONITOR_TOPIC}:{operation_id}"


def _control_row(uow: SqlAlchemyUnitOfWork, *, for_update: bool = False) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(text(f"SELECT * FROM p115_upload_monitor_controls WHERE singleton_key = 1{suffix}"))
        .mappings()
        .one_or_none()
    )


def _control_projection(row: RowMapping) -> UploadMonitorControl:
    return UploadMonitorControl(
        config_revision=str(row["config_revision"]),
        applied_revision=str(row["applied_revision"]) if row["applied_revision"] is not None else None,
        generation=int(row["generation"]),
        updated_at=row["updated_at"],
        applied_at=row["applied_at"],
    )


def _config_row(uow: SqlAlchemyUnitOfWork, *, for_update: bool = False) -> dict[str, Any] | None:
    suffix = " FOR UPDATE" if for_update else ""
    value = uow.session.scalar(
        text(f"SELECT value_json FROM app_settings WHERE setting_key = :setting_key{suffix}"),
        {"setting_key": UPLOAD_MONITOR_CONFIG_KEY},
    )
    return dict(value) if isinstance(value, Mapping) else None


def _apply_config(
    uow: SqlAlchemyUnitOfWork,
    revision: str,
    *,
    for_update: bool = True,
) -> RowMapping:
    control = _control_row(uow, for_update=for_update)
    if control is None:
        raise P115UploadStateError("upload monitor singleton control is missing")
    if str(control["config_revision"]) != revision:
        raise P115UploadConflictError("cannot apply a stale upload monitor config revision")
    return (
        uow.session.execute(
            text(
                """
                UPDATE p115_upload_monitor_controls
                SET applied_revision = :revision, applied_at = NOW(), updated_at = NOW()
                WHERE singleton_key = 1
                RETURNING *
                """
            ),
            {"revision": revision},
        )
        .mappings()
        .one()
    )


def _operation_row(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text(f"SELECT * FROM p115_upload_operations WHERE id = :operation_id{suffix}"),
            {"operation_id": operation_id},
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
            text(f"SELECT * FROM p115_upload_operations WHERE operation_key = :operation_key{suffix}"),
            {"operation_key": operation_key},
        )
        .mappings()
        .one_or_none()
    )


def _operation_projection(row: RowMapping) -> UploadOperation:
    fingerprint = row["fingerprint_json"]
    return UploadOperation(
        id=UUID(str(row["id"])),
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        source=UploadSource(str(row["source"])),
        event_type=UploadEventType(str(row["event_type"])),
        correlation_id=UUID(str(row["correlation_id"])) if row["correlation_id"] is not None else None,
        mapping_id=str(row["mapping_id"]),
        config_revision=str(row["config_revision"]),
        mapping=dict(row["mapping_snapshot_json"]),
        path=str(row["path"]),
        path_key=str(row["path_key"]),
        relative_path=str(row["relative_path"]),
        fingerprint=(UploadFileFingerprint.from_mapping(fingerprint) if isinstance(fingerprint, Mapping) else None),
        status=UploadOperationStatus(str(row["status"])),
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
        upload_backend=str(row["provider_backend"]) if row["provider_backend"] is not None else None,
        upload_session=_mapping_or_none(row["upload_session_json"]),
        remote_receipt=_mapping_or_none(row["remote_receipt_json"]),
        projection=_mapping_or_none(row["local_projection_json"]),
        result=_mapping_or_none(row["result_json"]),
        error=_mapping_or_none(row["error_json"]),
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_source=_mapping_or_none(row["completed_source_json"]),
    )


def _require_operation(row: RowMapping | None, operation_id: UUID) -> UploadOperation:
    if row is None:
        raise P115UploadNotFoundError(f"upload operation {operation_id} was not found")
    return _operation_projection(row)


def _require_claim_transition(
    uow: SqlAlchemyUnitOfWork,
    row: RowMapping | None,
    operation_id: UUID,
    claim_token: UUID,
    action: str,
) -> UploadOperation:
    if row is not None:
        return _operation_projection(row)
    current = _require_operation(_operation_row(uow, operation_id), operation_id)
    raise P115UploadStateError(
        f"cannot {action} for upload operation {operation_id}; "
        f"current status is {current.status.value} and the claim token is not current"
    )


def _set_ready(uow: SqlAlchemyUnitOfWork, operation_id: UUID) -> RowMapping:
    return (
        uow.session.execute(
            text(
                """
                UPDATE p115_upload_operations
                SET status = 'ready', available_at = NOW(), error_json = NULL,
                    state_revision = state_revision + 1, updated_at = NOW()
                WHERE id = :operation_id
                RETURNING *
                """
            ),
            {"operation_id": operation_id},
        )
        .mappings()
        .one()
    )


def _terminal_transition(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    claim_token: UUID,
    status: UploadOperationStatus,
    *,
    allowed: Sequence[UploadOperationStatus],
    result: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
) -> UploadTransition:
    if status not in _TERMINAL_STATUSES:
        raise ValueError("terminal upload transition requires a terminal status")
    normalized_result = _json(result, "upload terminal result")
    normalized_error = _json(error, "upload terminal error")
    row = (
        uow.session.execute(
            text(
                """
                UPDATE p115_upload_operations
                SET status = :status,
                    claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_expires_at = NULL,
                    result_json = CAST(:result_json AS jsonb),
                    error_json = CAST(:error_json AS jsonb),
                    finished_at = NOW(), state_revision = state_revision + 1, updated_at = NOW()
                WHERE id = :operation_id
                  AND claim_token = :claim_token
                  AND status = ANY(CAST(:allowed AS text[]))
                RETURNING *
                """
            ),
            {
                "operation_id": operation_id,
                "claim_token": claim_token,
                "status": status.value,
                "allowed": [value.value for value in allowed],
                "result_json": (
                    canonical_json(normalized_result, field="upload terminal result")
                    if normalized_result is not None
                    else None
                ),
                "error_json": (
                    canonical_json(normalized_error, field="upload terminal error")
                    if normalized_error is not None
                    else None
                ),
            },
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        return UploadTransition(_operation_projection(row), changed=True)
    current = _require_operation(_operation_row(uow, operation_id), operation_id)
    if current.status is not status:
        raise P115UploadStateError(
            f"upload operation {operation_id} is {current.status.value}, not a claim owned by this token"
        )
    if current.result != normalized_result or current.error != normalized_error:
        raise P115UploadConflictError("duplicate upload terminal transition changed its payload")
    return UploadTransition(current, changed=False)


def _terminal_without_claim(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    status: UploadOperationStatus,
    *,
    error: Mapping[str, Any],
) -> RowMapping:
    if status not in _TERMINAL_STATUSES:
        raise ValueError("terminal upload transition requires a terminal status")
    normalized_error = normalize_json_object(dict(error), field="upload terminal error")
    return (
        uow.session.execute(
            text(
                """
                UPDATE p115_upload_operations
                SET status = :status,
                    claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_expires_at = NULL,
                    result_json = NULL, error_json = CAST(:error_json AS jsonb),
                    finished_at = NOW(), state_revision = state_revision + 1, updated_at = NOW()
                WHERE id = :operation_id AND claim_token IS NULL
                RETURNING *
                """
            ),
            {
                "operation_id": operation_id,
                "status": status.value,
                "error_json": canonical_json(normalized_error, field="upload terminal error"),
            },
        )
        .mappings()
        .one()
    )


def _stage_outbox(uow: SqlAlchemyUnitOfWork, operation: UploadOperation) -> None:
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
                :id, :event_key, :topic, 'p115_upload_operation', :aggregate_id,
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
            "event_key": upload_outbox_event_key(operation.id),
            "topic": UPLOAD_MONITOR_TOPIC,
            "aggregate_id": str(operation.id),
            "payload_json": canonical_json(payload, field="p115 upload outbox payload"),
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
              AND status IN ('pending', 'publishing', 'published')
            """
        ),
        {"event_key": upload_outbox_event_key(operation_id), "topic": UPLOAD_MONITOR_TOPIC},
    )


def _cancel_outbox(uow: SqlAlchemyUnitOfWork, operation_id: UUID) -> None:
    uow.session.execute(
        text(
            """
            UPDATE outbox_events
            SET status = 'cancelled', claimed_by = NULL, claim_token = NULL,
                claimed_at = NULL, updated_at = NOW()
            WHERE event_key = :event_key AND topic = :topic
              AND status IN ('pending', 'publishing', 'published')
            """
        ),
        {"event_key": upload_outbox_event_key(operation_id), "topic": UPLOAD_MONITOR_TOPIC},
    )


def _upsert_completed_source(uow: SqlAlchemyUnitOfWork, source: UploadedSource) -> None:
    payload = source.payload()
    uow.session.execute(
        text(
            """
            INSERT INTO p115_upload_records (
                record_key, record_type, mapping_id, local_path, relative_path,
                target_cid, pick_code, status, payload_json, created_at, updated_at
            ) VALUES (
                :record_key, 'completed', :mapping_id, :local_path, :relative_path,
                :target_cid, :pick_code, 'completed', CAST(:payload_json AS jsonb), NOW(), NOW()
            )
            ON CONFLICT (record_key) DO UPDATE SET
                record_type = 'completed', mapping_id = EXCLUDED.mapping_id,
                local_path = EXCLUDED.local_path, relative_path = EXCLUDED.relative_path,
                target_cid = EXCLUDED.target_cid, pick_code = EXCLUDED.pick_code,
                status = 'completed', payload_json = EXCLUDED.payload_json, updated_at = NOW()
            """
        ),
        {
            "record_key": source.record_key,
            "mapping_id": source.mapping_id,
            "local_path": source.local_path,
            "relative_path": source.relative_path,
            "target_cid": source.target_cid,
            "pick_code": source.pick_code or None,
            "payload_json": canonical_json(payload, field="completed upload source"),
        },
    )


def _source_by_record_key(
    uow: SqlAlchemyUnitOfWork,
    record_key: str,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    row = (
        uow.session.execute(
            text(
                f"""
                SELECT * FROM p115_upload_records
                WHERE record_type = 'completed' AND record_key = :record_key
                {suffix}
                """
            ),
            {"record_key": record_key},
        )
        .mappings()
        .one_or_none()
    )
    return row


def _delete_completed_source_for_operation(
    uow: SqlAlchemyUnitOfWork,
    operation: UploadOperation,
) -> None:
    if operation.completed_source is None:
        raise P115UploadStateError("delete upload operation has no completed source snapshot")
    source = UploadedSource.from_snapshot(operation.completed_source)
    if source.mapping_id != operation.mapping_id or source.path_key != operation.path_key:
        raise P115UploadConflictError("delete upload operation completed source identity changed")
    current_row = _source_by_record_key(uow, source.record_key, for_update=True)
    if current_row is None or _source_projection(current_row).payload() != source.payload():
        return
    uow.session.execute(
        text(
            """
            DELETE FROM p115_upload_records
            WHERE record_key = :record_key
              AND record_type = 'completed'
              AND mapping_id = :mapping_id
              AND (
                  payload_json ->> 'path_key' = :path_key
                  OR (NOT payload_json ? 'path_key' AND local_path = :path_key)
              )
            """
        ),
        {
            "record_key": source.record_key,
            "mapping_id": source.mapping_id,
            "path_key": source.path_key,
        },
    )


def _source_projection(row: RowMapping) -> UploadedSource:
    payload = dict(row["payload_json"]) if isinstance(row["payload_json"], Mapping) else {}
    record_key = str(row["record_key"])
    local_path = str(payload.get("local_path") or row["local_path"] or "")
    path_key = str(payload.get("path_key") or _path_key(local_path))
    fingerprint_value = payload.get("fingerprint")
    fingerprint = (
        UploadFileFingerprint.from_mapping(fingerprint_value)
        if isinstance(fingerprint_value, Mapping)
        else UploadFileFingerprint(size=0, mtime_ns=0, device=0, inode=0)
    )
    receipt_value = payload.get("remote_receipt")
    receipt = dict(receipt_value) if isinstance(receipt_value, Mapping) else {}
    if not receipt and (payload.get("pick_code") or row["pick_code"]):
        receipt["pick_code"] = str(payload.get("pick_code") or row["pick_code"])
    completed_at = _completed_at(payload.get("completed_at"), row["updated_at"] or row["created_at"])
    operation_value = payload.get("operation_id")
    try:
        operation_id = UUID(str(operation_value))
    except (TypeError, ValueError, AttributeError):
        operation_id = uuid5(NAMESPACE_URL, f"etk:p115-upload-source:{record_key}")
    derived = payload.get("derived_paths")
    derived_ownership_value = payload.get("derived_ownership")
    derived_ownership = (
        {str(path): str(digest) for path, digest in derived_ownership_value.items()}
        if isinstance(derived_ownership_value, Mapping)
        else {}
    )
    return UploadedSource(
        record_key=record_key,
        operation_id=operation_id,
        mapping_id=str(payload.get("mapping_id") or row["mapping_id"] or ""),
        config_revision=str(payload.get("config_revision") or _ZERO_REVISION),
        local_path=local_path,
        path_key=path_key,
        relative_path=str(payload.get("relative_path") or row["relative_path"] or ""),
        target_cid=str(payload.get("target_cid") or row["target_cid"] or ""),
        mode=str(payload.get("mode") or "keep"),
        fingerprint=fingerprint,
        remote_receipt=receipt,
        derived_paths=tuple(str(value) for value in derived if str(value or "")) if isinstance(derived, list) else (),
        derived_ownership=derived_ownership,
        completed_at=completed_at,
        operation_sequence=int(payload.get("operation_sequence") or 0),
    )


def _validate_intent(intent: UploadMonitorIntent) -> None:
    _nonempty(intent.operation_key, "operation_key", maximum=512)
    if len(intent.request_hash) != 32:
        raise ValueError("upload request_hash must contain 32 bytes")
    _nonempty(intent.mapping_id, "mapping_id", maximum=512)
    _revision(intent.config_revision)
    _nonempty(intent.path, "path", maximum=4096)
    _nonempty(intent.path_key, "path_key", maximum=4096)
    if len(intent.relative_path) > 4096:
        raise ValueError("relative_path exceeds 4096 characters")
    _aware(intent.available_at, "available_at")
    _aware(intent.stability_deadline, "stability_deadline")
    if intent.stability_deadline < intent.available_at:
        raise ValueError("stability_deadline must not precede available_at")
    if (intent.event_type is UploadEventType.DELETED) != (intent.completed_source is not None):
        raise ValueError("delete upload intent requires exactly one completed source snapshot")
    if intent.completed_source is not None:
        source = UploadedSource.from_snapshot(intent.completed_source)
        if (
            source.mapping_id != intent.mapping_id
            or source.path_key != intent.path_key
            or source.mode != "keep"
            or str(intent.mapping.get("mode") or "") != "keep"
        ):
            raise ValueError("completed source snapshot does not match the delete upload intent")


def _validate_completed_source(operation_id: UUID, source: UploadedSource) -> None:
    if source.operation_id != operation_id:
        raise P115UploadConflictError("completed source operation_id does not match")
    _nonempty(source.record_key, "completed.record_key", maximum=4096)
    _nonempty(source.mapping_id, "completed.mapping_id", maximum=512)
    _revision(source.config_revision)
    _nonempty(source.local_path, "completed.local_path", maximum=4096)
    _nonempty(source.path_key, "completed.path_key", maximum=4096)
    _nonempty(source.target_cid, "completed.target_cid", maximum=255)
    _aware(source.completed_at, "completed.completed_at")
    _validated_remote_receipt(source.remote_receipt)
    normalize_json_object(source.payload(), field="completed upload source")


def _validated_remote_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    receipt = normalize_json_object(dict(value), field="upload remote receipt")
    _nonempty(receipt.get("fid"), "upload receipt fid", maximum=255)
    _nonempty(receipt.get("pick_code"), "upload receipt pick_code", maximum=255)
    _nonempty(receipt.get("parent_id"), "upload receipt parent_id", maximum=255)
    backend = _nonempty(receipt.get("backend"), "upload receipt backend", maximum=255)
    if backend not in {"openapi", "cookie"}:
        raise ValueError("upload receipt backend must be openapi or cookie")
    account_id = _nonempty(receipt.get("account_id"), "upload receipt account_id", maximum=255)
    if not account_id.isdigit():
        raise ValueError("upload receipt account_id must be numeric")
    return receipt


def _updated_operation_ids(uow: SqlAlchemyUnitOfWork, statement: str) -> tuple[UUID, ...]:
    return tuple(UUID(str(row["id"])) for row in uow.session.execute(text(statement)).mappings().all())


def _advisory_lock(uow: SqlAlchemyUnitOfWork, key: str) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": key},
    )


def _mapping_or_none(value: object) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _json(value: Mapping[str, Any] | None, field: str) -> dict[str, Any] | None:
    return normalize_json_object(dict(value), field=field) if value is not None else None


def _revision(value: object) -> str:
    revision = str(value or "").strip()
    if len(revision) != 64 or any(character not in "0123456789abcdef" for character in revision):
        raise ValueError("upload config revision must be a lowercase SHA-256 hex digest")
    return revision


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
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _path_key(value: object) -> str:
    return os.path.normcase(os.path.abspath(os.path.normpath(str(value or "").strip())))


def _completed_at(value: object, fallback: datetime) -> datetime:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return _aware(parsed, "completed_at")
        except ValueError:
            pass
    return fallback.astimezone(UTC)


__all__ = [
    "P115UploadConflictError",
    "P115UploadNotFoundError",
    "P115UploadRepositoryError",
    "P115UploadStateError",
    "PostgresP115UploadMonitorRepository",
    "upload_outbox_event_key",
]
