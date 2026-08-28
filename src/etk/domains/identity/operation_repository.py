from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

from .operations import (
    IDENTITY_OPERATION_AGGREGATE_TYPE,
    IDENTITY_OPERATION_TOPIC,
    IdentityAvatarArtifact,
    IdentityOperation,
    IdentityOperationIntent,
    IdentityOperationKind,
    IdentityOperationReservation,
    IdentityOperationStatus,
    IdentityOperationStep,
    identity_operation_key,
)


class IdentityOperationStateError(RuntimeError):
    pass


class IdentityOperationClaimLostError(IdentityOperationStateError):
    pass


class IdentityOperationScopeBusyError(IdentityOperationStateError):
    pass


class PostgresIdentityOperationRepository:
    """Identity effect ledger with short transactions, claim fencing, and shared Outbox."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reserve_registration(
        self,
        *,
        invitation_token: str,
        username: str,
        reservation_ttl_seconds: float = 900,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        token = str(invitation_token or "").strip()
        normalized_username = str(username or "").strip()
        timestamp = _utc(now)
        if not token or not normalized_username:
            raise ValueError("registration invitation and username must not be empty")
        ttl = float(reservation_ttl_seconds)
        if ttl <= 0:
            raise ValueError("registration reservation TTL must be positive")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT invitations.id AS invitation_id,
                               invitations.status AS invitation_status,
                               invitations.expires_at,
                               invitations.expiration_days,
                               invitations.reservation_id,
                               templates.id AS template_id,
                               templates.name AS template_name,
                               templates.description AS template_description,
                               templates.emby_policy_json,
                               templates.emby_configuration_json
                        FROM invitations
                        JOIN user_templates AS templates ON templates.id = invitations.template_id
                        WHERE invitations.token = :token
                        FOR UPDATE OF invitations
                        """
                    ),
                    {"token": token},
                )
                .mappings()
                .first()
            )
            if row is None:
                raise LookupError("邀请链接无效或已被使用")
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:username_scope, 0))"),
                {"username_scope": f"identity:registration:username:{normalized_username.casefold()}"},
            )
            invitation_status = str(row["invitation_status"])
            expires_at = row["expires_at"]
            if expires_at is not None and expires_at < timestamp:
                raise LookupError("邀请链接无效或已被使用")
            if invitation_status in {"reserved", "used"} and row["reservation_id"] is not None:
                existing = self._read_in_uow(uow, UUID(str(row["reservation_id"])), lock=True)
                if existing is None or existing.kind is not IdentityOperationKind.REGISTER:
                    raise IdentityOperationStateError("invitation reservation is inconsistent")
                if str(existing.request.get("username") or "").casefold() != normalized_username.casefold():
                    raise LookupError("邀请链接无效或已被使用")
                uow.commit()
                return IdentityOperationReservation(existing, created=False)
            if invitation_status != "active":
                raise LookupError("邀请链接无效或已被使用")

            active_username_operation = uow.session.scalar(
                text(
                    """
                    SELECT id
                    FROM identity_operations
                    WHERE kind = 'register'
                      AND lower(request_json ->> 'username') = :normalized_username
                      AND status IN (
                          'ready','claimed','requesting','awaiting_readback','retryable',
                          'compensating','remote_confirmed','indeterminate'
                      )
                    LIMIT 1
                    """
                ),
                {"normalized_username": normalized_username.casefold()},
            )
            if active_username_operation is not None:
                raise IdentityOperationScopeBusyError(
                    f"identity registration username {normalized_username!r} already has an active "
                    "or indeterminate operation"
                )

            expiration_days = int(row["expiration_days"] or 0)
            expiration_date = timestamp + timedelta(days=expiration_days) if expiration_days > 0 else None
            reservation_generation = (
                int(
                    uow.session.scalar(
                        text(
                            """
                        SELECT COUNT(*)
                        FROM identity_operations
                        WHERE kind = 'register' AND scope_key = :scope_key
                        """
                        ),
                        {"scope_key": f"invitation:{int(row['invitation_id'])}"},
                    )
                    or 0
                )
                + 1
            )
            request: dict[str, Any] = {
                "invitation_id": int(row["invitation_id"]),
                "reservation_generation": reservation_generation,
                "username": normalized_username,
                "template_id": int(row["template_id"]),
                "template_name": str(row["template_name"]),
                "template_description": row["template_description"],
                "template_policy": dict(row["emby_policy_json"] or {}),
                "template_configuration": (
                    dict(row["emby_configuration_json"])
                    if isinstance(row["emby_configuration_json"], Mapping)
                    else None
                ),
                "expiration_date": expiration_date.isoformat() if expiration_date is not None else None,
            }
            intent = IdentityOperationIntent.create(
                kind=IdentityOperationKind.REGISTER,
                scope_key=f"invitation:{int(row['invitation_id'])}",
                request=request,
                available_at=timestamp,
                idempotency_key=(
                    f"invitation:{int(row['invitation_id'])}:{reservation_generation}:{normalized_username.casefold()}"
                ),
            )
            reservation = self._reserve_in_uow(
                uow,
                intent,
                outbox_available_at=timestamp + timedelta(seconds=ttl),
            )
            if not reservation.created:
                raise IdentityOperationStateError("active invitation replayed an existing operation unexpectedly")
            uow.session.execute(
                text(
                    """
                    UPDATE invitations
                    SET status = 'reserved', reservation_id = :operation_id,
                        reserved_at = :now, reservation_expires_at = :reservation_expires_at
                    WHERE id = :invitation_id AND status = 'active'
                    """
                ),
                {
                    "operation_id": intent.id,
                    "now": timestamp,
                    "reservation_expires_at": timestamp + timedelta(seconds=ttl),
                    "invitation_id": int(row["invitation_id"]),
                },
            )
            uow.commit()
            return reservation

    def reserve(
        self,
        intent: IdentityOperationIntent,
        *,
        outbox_delay_seconds: float = 0,
    ) -> IdentityOperationReservation:
        with self._unit_of_work() as uow:
            reservation = self._reserve_in_uow(
                uow,
                intent,
                outbox_available_at=(
                    intent.available_at + timedelta(seconds=max(0, float(outbox_delay_seconds)))
                    if outbox_delay_seconds
                    else None
                ),
            )
            uow.commit()
            return reservation

    def reserve_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: IdentityOperationIntent,
        *,
        outbox_available_at: datetime | None = None,
    ) -> IdentityOperationReservation:
        """Reserve inside a transaction that also freezes a template propagation snapshot."""
        return self._reserve_in_uow(
            uow,
            intent,
            outbox_available_at=outbox_available_at,
        )

    def finalize_request_snapshot(
        self,
        operation_id: UUID,
        request: Mapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_request = normalize_durable_json_object(
            dict(request),
            field="identity operation request",
        )
        request_bytes = canonical_json(normalized_request).encode()
        request_hash = hashlib.sha256(request_bytes).digest()
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM identity_operations WHERE id = :id FOR UPDATE"),
                    {"id": operation_id},
                )
                .mappings()
                .first()
            )
            if row is None:
                raise IdentityOperationStateError("identity operation was not found")
            operation = _operation(row)
            if (
                operation.status is not IdentityOperationStatus.READY
                or operation.attempts != 0
                or operation.current_step is not IdentityOperationStep.RESERVED
                or operation.request.get("snapshot_pending") is not True
            ):
                raise IdentityOperationStateError("identity operation snapshot is no longer reservable")
            if not _same_snapshot_caller_intent(operation.kind, operation.request, normalized_request):
                raise IdentityOperationStateError("identity operation snapshot changed caller intent")
            updated = (
                uow.session.execute(
                    text(
                        """
                        UPDATE identity_operations
                        SET request_hash = :request_hash,
                            request_json = CAST(:request_json AS jsonb),
                            state_revision = state_revision + 1,
                            updated_at = :now
                        WHERE id = :id AND status = 'ready' AND attempts = 0
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "request_hash": request_hash,
                        "request_json": canonical_json(normalized_request),
                        "now": timestamp,
                    },
                )
                .mappings()
                .one()
            )
            uow.session.execute(
                text(
                    """
                    UPDATE outbox_events
                    SET payload_json = jsonb_set(
                            payload_json,
                            '{request_hash}',
                            to_jsonb(CAST(:request_hash_hex AS text)),
                            TRUE
                        ),
                        updated_at = :now
                    WHERE event_key = :event_key AND status = 'pending'
                    """
                ),
                {
                    "event_key": f"{IDENTITY_OPERATION_TOPIC}:{operation_id}",
                    "request_hash_hex": request_hash.hex(),
                    "now": timestamp,
                },
            )
            uow.commit()
            return _operation(updated)

    def reject_ready_before_write(
        self,
        operation_id: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_error = normalize_durable_json_object(dict(error), field="identity operation error")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE identity_operations
                        SET status = 'rejected',
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = :now,
                            state_revision = state_revision + 1,
                            updated_at = :now
                        WHERE id = :id AND status = 'ready' AND attempts = 0
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "error_json": canonical_json(normalized_error),
                        "now": timestamp,
                    },
                )
                .mappings()
                .first()
            )
            if row is None:
                raise IdentityOperationStateError("identity operation cannot reject a started snapshot")
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    def reserve_avatar_upload(
        self,
        *,
        user_id: str,
        content: bytes,
        content_type: str,
        previous_tag: str | None,
        idempotency_key: str | None = None,
        outbox_delay_seconds: float = 0,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        normalized_user_id = str(user_id or "").strip()
        normalized_content_type = str(content_type or "").partition(";")[0].strip().casefold()
        payload = bytes(content)
        if not normalized_user_id:
            raise ValueError("avatar owner must not be empty")
        if normalized_content_type not in {"image/jpeg", "image/png", "image/webp"}:
            raise ValueError("avatar content type is unsupported")
        if not payload or len(payload) > 5 * 1024 * 1024:
            raise ValueError("avatar size is invalid")
        timestamp = _utc(now)
        digest = hashlib.sha256(payload).digest()
        normalized_previous_tag = str(previous_tag or "").strip() or None
        normalized_idempotency = str(idempotency_key or "").strip() or None
        request_id = (
            hashlib.sha256(normalized_idempotency.encode("utf-8")).hexdigest()
            if normalized_idempotency is not None
            else str(uuid4())
        )
        artifact_id = uuid5(
            NAMESPACE_URL,
            f"etk:identity-avatar:{normalized_user_id}:{digest.hex()}:{request_id}",
        )
        intent = IdentityOperationIntent.create(
            kind=IdentityOperationKind.UPLOAD_AVATAR,
            scope_key=f"user:{normalized_user_id}",
            request={
                "user_id": normalized_user_id,
                "request_id": request_id,
                "artifact_id": str(artifact_id),
                "sha256": digest.hex(),
                "size_bytes": len(payload),
                "content_type": normalized_content_type,
                "previous_tag": normalized_previous_tag,
            },
            available_at=timestamp,
            idempotency_key=normalized_idempotency,
        )
        with self._unit_of_work() as uow:
            reservation = self._reserve_in_uow(
                uow,
                intent,
                outbox_available_at=(
                    timestamp + timedelta(seconds=max(0, float(outbox_delay_seconds))) if outbox_delay_seconds else None
                ),
            )
            if reservation.created:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO identity_avatar_artifacts (
                            id, operation_id, owner_user_id, sha256, size_bytes, content_type,
                            content_bytes, status, expires_at, created_at
                        ) VALUES (
                            :id, :operation_id, :owner_user_id, :sha256, :size_bytes, :content_type,
                            :content_bytes, 'staged', :expires_at, :created_at
                        )
                        """
                    ),
                    {
                        "id": artifact_id,
                        "operation_id": reservation.operation.id,
                        "owner_user_id": normalized_user_id,
                        "sha256": digest,
                        "size_bytes": len(payload),
                        "content_type": normalized_content_type,
                        "content_bytes": payload,
                        "expires_at": timestamp + timedelta(hours=24),
                        "created_at": timestamp,
                    },
                )
            uow.commit()
            return reservation

    def load_avatar_artifact(
        self,
        artifact_id: UUID,
        *,
        operation_id: UUID,
        owner_user_id: str,
        now: datetime | None = None,
    ) -> IdentityAvatarArtifact | None:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, operation_id, owner_user_id, sha256, size_bytes,
                               content_type, content_bytes
                        FROM identity_avatar_artifacts
                        WHERE id = :id AND operation_id = :operation_id
                          AND owner_user_id = :owner_user_id
                          AND status IN ('staged','claimed') AND expires_at > :now
                        """
                    ),
                    {
                        "id": artifact_id,
                        "operation_id": operation_id,
                        "owner_user_id": owner_user_id,
                        "now": timestamp,
                    },
                )
                .mappings()
                .first()
            )
        if row is None or row["content_bytes"] is None:
            return None
        content = bytes(row["content_bytes"])
        digest = bytes(row["sha256"])
        return IdentityAvatarArtifact(
            id=UUID(str(row["id"])),
            operation_id=UUID(str(row["operation_id"])),
            owner_user_id=str(row["owner_user_id"]),
            sha256=digest,
            size_bytes=int(row["size_bytes"]),
            content_type=str(row["content_type"]),
            content=content,
        )

    def read(self, operation_id: UUID) -> IdentityOperation | None:
        with self._unit_of_work() as uow:
            return self._read_in_uow(uow, operation_id)

    def registration_token_matches(self, operation_id: UUID, invitation_token: str) -> bool:
        token = str(invitation_token or "").strip()
        if not token:
            return False
        with self._unit_of_work() as uow:
            return bool(
                uow.session.scalar(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM identity_operations AS operation
                            JOIN invitations AS invitation
                              ON invitation.id = CAST(
                                  operation.request_json ->> 'invitation_id' AS integer
                              )
                            WHERE operation.id = :operation_id
                              AND operation.kind = 'register'
                              AND invitation.token = :token
                        )
                        """
                    ),
                    {"operation_id": operation_id, "token": token},
                )
            )

    def read_active_scope(self, scope_key: str) -> IdentityOperation | None:
        normalized_scope = str(scope_key or "").strip()
        if not normalized_scope:
            return None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM identity_operations
                        WHERE scope_key = :scope_key
                          AND kind <> 'register'
                          AND status IN (
                              'ready','claimed','requesting','awaiting_readback','retryable',
                              'compensating','remote_confirmed','indeterminate'
                          )
                        ORDER BY event_sequence DESC
                        LIMIT 1
                        """
                    ),
                    {"scope_key": normalized_scope},
                )
                .mappings()
                .first()
            )
            return _operation(row) if row is not None else None

    def read_idempotent(
        self,
        *,
        kind: IdentityOperationKind,
        scope_key: str,
        idempotency_key: str,
    ) -> IdentityOperation | None:
        operation_key = identity_operation_key(
            kind=kind,
            scope_key=scope_key,
            idempotency_key=idempotency_key,
        )
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM identity_operations WHERE operation_key = :operation_key"),
                    {"operation_key": operation_key},
                )
                .mappings()
                .first()
            )
            return _operation(row) if row is not None else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation | None:
        normalized_worker = str(worker_id or "").strip()
        timestamp = _utc(now)
        if not normalized_worker or float(lease_seconds) <= 0:
            raise ValueError("identity operation claim requires a worker and positive lease")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE identity_operations
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
                .first()
            )
            uow.commit()
            return _operation(row) if row is not None else None

    def mark_step_started(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        step: IdentityOperationStep | str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        normalized_step = step if isinstance(step, IdentityOperationStep) else IdentityOperationStep(step)
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(uow, operation_id, claim_token, now=timestamp)
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'requesting', current_step = :step, step_started_at = :now, "
                "state_json = CAST(:state_json AS jsonb), error_json = NULL",
                {"step": normalized_step.value, "now": timestamp, "state_json": canonical_json(normalized_state)},
                renew_lease_at=timestamp,
                renew_lease_until=_renewed_claim_expiry(operation, timestamp),
            )
            if normalized_step is not IdentityOperationStep.PREFLIGHT_USERNAME:
                self._arm_outbox(uow, operation_id, available_at=timestamp)
            uow.commit()
            return _operation(row)

    def continue_claim(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        next_step: IdentityOperationStep | str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        normalized_step = (
            next_step if isinstance(next_step, IdentityOperationStep) else IdentityOperationStep(next_step)
        )
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={
                    IdentityOperationStatus.CLAIMED,
                    IdentityOperationStatus.REQUESTING,
                    IdentityOperationStatus.COMPENSATING,
                    IdentityOperationStatus.REMOTE_CONFIRMED,
                },
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'claimed', current_step = :step, step_started_at = NULL, "
                "state_json = CAST(:state_json AS jsonb), error_json = NULL",
                {"step": normalized_step.value, "state_json": canonical_json(normalized_state)},
                renew_lease_at=timestamp,
                renew_lease_until=_renewed_claim_expiry(operation, timestamp),
            )
            uow.commit()
            return _operation(row)

    def begin_compensation(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        normalized_error = normalize_durable_json_object(dict(error), field="identity operation error")
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={
                    IdentityOperationStatus.CLAIMED,
                    IdentityOperationStatus.REQUESTING,
                    IdentityOperationStatus.REMOTE_CONFIRMED,
                },
                allow_cancel_requested=True,
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'compensating', current_step = 'delete_remote_user', "
                "step_started_at = :now, state_json = CAST(:state_json AS jsonb), "
                "error_json = CAST(:error_json AS jsonb)",
                {
                    "now": timestamp,
                    "state_json": canonical_json(normalized_state),
                    "error_json": canonical_json(normalized_error),
                },
                renew_lease_at=timestamp,
                renew_lease_until=_renewed_claim_expiry(operation, timestamp),
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
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        normalized_error = normalize_durable_json_object(dict(error), field="identity operation error")
        with self._unit_of_work() as uow:
            self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={
                    IdentityOperationStatus.CLAIMED,
                    IdentityOperationStatus.REQUESTING,
                    IdentityOperationStatus.COMPENSATING,
                    IdentityOperationStatus.REMOTE_CONFIRMED,
                },
                allow_cancel_requested=True,
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'awaiting_readback', state_json = CAST(:state_json AS jsonb), "
                "error_json = CAST(:error_json AS jsonb), available_at = :available_at, "
                "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
                {
                    "state_json": canonical_json(normalized_state),
                    "error_json": canonical_json(normalized_error),
                    "available_at": timestamp + timedelta(seconds=max(0, float(retry_after_seconds))),
                },
            )
            uow.commit()
            return _operation(row)

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        normalized_error = normalize_durable_json_object(dict(error), field="identity operation error")
        with self._unit_of_work() as uow:
            self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={IdentityOperationStatus.CLAIMED, IdentityOperationStatus.REQUESTING},
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'retryable', step_started_at = NULL, state_json = CAST(:state_json AS jsonb), "
                "error_json = CAST(:error_json AS jsonb), available_at = :available_at, "
                "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
                {
                    "state_json": canonical_json(normalized_state),
                    "error_json": canonical_json(normalized_error),
                    "available_at": timestamp + timedelta(seconds=max(0, float(retry_after_seconds))),
                },
            )
            uow.commit()
            return _operation(row)

    def defer_local_commit(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        normalized_error = normalize_durable_json_object(dict(error), field="identity operation error")
        with self._unit_of_work() as uow:
            self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={
                    IdentityOperationStatus.CLAIMED,
                    IdentityOperationStatus.REQUESTING,
                    IdentityOperationStatus.REMOTE_CONFIRMED,
                },
                allow_cancel_requested=True,
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'awaiting_readback', current_step = 'local_commit', "
                "step_started_at = :now, state_json = CAST(:state_json AS jsonb), "
                "error_json = CAST(:error_json AS jsonb), available_at = :available_at, "
                "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
                {
                    "now": timestamp,
                    "state_json": canonical_json(normalized_state),
                    "error_json": canonical_json(normalized_error),
                    "available_at": timestamp + timedelta(seconds=max(0, float(retry_after_seconds))),
                },
            )
            uow.commit()
            return _operation(row)

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        release_invitation: bool,
        now: datetime | None = None,
    ) -> IdentityOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=IdentityOperationStatus.REJECTED,
            state=state,
            result=None,
            error=error,
            release_invitation=release_invitation,
            now=_utc(now),
        )

    def mark_compensated(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=IdentityOperationStatus.COMPENSATED,
            state=state,
            result=result,
            error=None,
            release_invitation=True,
            now=_utc(now),
        )

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=IdentityOperationStatus.INDETERMINATE,
            state=state,
            result=None,
            error=error,
            release_invitation=False,
            now=_utc(now),
        )

    def complete_registration(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        normalized_result = normalize_durable_json_object(dict(result), field="identity operation result")
        remote_user_id = str(normalized_state.get("remote_user_id") or "").strip()
        if not remote_user_id:
            raise IdentityOperationStateError("registration cannot commit without a remote user receipt")
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={IdentityOperationStatus.CLAIMED, IdentityOperationStatus.REMOTE_CONFIRMED},
                allow_cancel_requested=True,
            )
            if operation.kind is not IdentityOperationKind.REGISTER:
                raise IdentityOperationStateError("identity operation is not a registration")
            invitation_id = int(operation.request["invitation_id"])
            username = str(operation.request["username"])
            template_id = int(operation.request["template_id"])
            expiration_date = operation.request.get("expiration_date")
            template_policy = operation.request.get("template_policy")
            if not isinstance(template_policy, Mapping):
                raise IdentityOperationStateError("registration template policy is unavailable at local commit")
            invitation = (
                uow.session.execute(
                    text(
                        """
                        SELECT id FROM invitations
                        WHERE id = :invitation_id AND status = 'reserved'
                          AND reservation_id = :operation_id
                        FOR UPDATE
                        """
                    ),
                    {"invitation_id": invitation_id, "operation_id": operation_id},
                )
                .mappings()
                .first()
            )
            if invitation is None:
                raise IdentityOperationStateError("registration invitation reservation was lost")
            uow.session.execute(
                text(
                    """
                    UPDATE identity_projection_clock
                    SET revision = revision + 1, updated_at = :now
                    WHERE singleton_id = 1
                    """
                ),
                {"now": timestamp},
            )
            uow.session.execute(
                text("DELETE FROM identity_user_tombstones WHERE user_id = :user_id"),
                {"user_id": remote_user_id},
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users (id, name, is_administrator, policy_json)
                    VALUES (
                        :user_id, :username, :is_administrator,
                        CAST(:policy_json AS jsonb)
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        is_administrator = EXCLUDED.is_administrator,
                        policy_json = EXCLUDED.policy_json
                    """
                ),
                {
                    "user_id": remote_user_id,
                    "username": username,
                    "is_administrator": bool(template_policy.get("IsAdministrator", False)),
                    "policy_json": canonical_json(dict(template_policy)),
                },
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (
                        emby_user_id, status, expiration_date, created_by, template_id
                    ) VALUES (
                        :user_id, :status, :expiration_date, 'self-registered', :template_id
                    )
                    ON CONFLICT (emby_user_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        expiration_date = EXCLUDED.expiration_date,
                        created_by = EXCLUDED.created_by,
                        template_id = EXCLUDED.template_id
                    """
                ),
                {
                    "user_id": remote_user_id,
                    "status": "disabled" if bool(template_policy.get("IsDisabled", False)) else "active",
                    "expiration_date": expiration_date,
                    "template_id": template_id,
                },
            )
            uow.session.execute(
                text(
                    """
                    UPDATE invitations
                    SET status = 'used', used_by_user_id = :user_id,
                        reservation_expires_at = GREATEST(reservation_expires_at, :now)
                    WHERE id = :invitation_id AND reservation_id = :operation_id
                    """
                ),
                {
                    "user_id": remote_user_id,
                    "now": timestamp,
                    "invitation_id": invitation_id,
                    "operation_id": operation_id,
                },
            )
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'confirmed', current_step = 'local_commit', step_started_at = NULL, "
                "state_json = CAST(:state_json AS jsonb), result_json = CAST(:result_json AS jsonb), "
                "error_json = NULL, finished_at = :now, "
                "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
                {
                    "state_json": canonical_json(normalized_state),
                    "result_json": canonical_json(normalized_result),
                    "now": timestamp,
                },
            )
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    def complete_user_operation(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        normalized_result = normalize_durable_json_object(dict(result), field="identity operation result")
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=timestamp,
                allowed={
                    IdentityOperationStatus.CLAIMED,
                    IdentityOperationStatus.REQUESTING,
                    IdentityOperationStatus.REMOTE_CONFIRMED,
                },
                allow_cancel_requested=True,
            )
            user_id = str(operation.request.get("user_id") or "").strip()
            if not user_id:
                raise IdentityOperationStateError("identity user operation is missing its user id")
            uow.session.execute(
                text(
                    """
                    UPDATE identity_projection_clock
                    SET revision = revision + 1, updated_at = :now
                    WHERE singleton_id = 1
                    """
                ),
                {"now": timestamp},
            )
            if operation.kind is IdentityOperationKind.ASSIGN_TEMPLATE:
                template_id = int(operation.request["template_id"])
                template_policy = operation.request.get("template_policy")
                if not isinstance(template_policy, Mapping):
                    raise IdentityOperationStateError("assigned template policy is unavailable at local commit")
                updated_user = uow.session.execute(
                    text(
                        """
                        UPDATE emby_users
                        SET is_administrator = :is_administrator,
                            policy_json = CAST(:policy_json AS jsonb),
                            last_updated_at = :now
                        WHERE id = :user_id
                        """
                    ),
                    {
                        "user_id": user_id,
                        "is_administrator": bool(template_policy.get("IsAdministrator", False)),
                        "policy_json": canonical_json(dict(template_policy)),
                        "now": timestamp,
                    },
                )
                if int(getattr(updated_user, "rowcount", 0) or 0) != 1:
                    raise IdentityOperationStateError("assigned template user is missing from local identity store")
                uow.session.execute(
                    text(
                        """
                        INSERT INTO emby_users_extended (emby_user_id, status, template_id)
                        VALUES (:user_id, :status, :template_id)
                        ON CONFLICT (emby_user_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            template_id = EXCLUDED.template_id
                        """
                    ),
                    {
                        "user_id": user_id,
                        "status": ("disabled" if bool(template_policy.get("IsDisabled", False)) else "active"),
                        "template_id": template_id,
                    },
                )
            elif operation.kind is IdentityOperationKind.SET_STATUS:
                status = "disabled" if bool(operation.request.get("disable")) else "active"
                desired_policy = operation.request.get("desired_policy")
                if not isinstance(desired_policy, Mapping):
                    raise IdentityOperationStateError("status policy is unavailable at local commit")
                updated_user = uow.session.execute(
                    text(
                        """
                        UPDATE emby_users
                        SET is_administrator = :is_administrator,
                            policy_json = CAST(:policy_json AS jsonb),
                            last_updated_at = :now
                        WHERE id = :user_id
                        """
                    ),
                    {
                        "user_id": user_id,
                        "is_administrator": bool(desired_policy.get("IsAdministrator", False)),
                        "policy_json": canonical_json(dict(desired_policy)),
                        "now": timestamp,
                    },
                )
                if int(getattr(updated_user, "rowcount", 0) or 0) != 1:
                    raise IdentityOperationStateError("status user is missing from local identity store")
                uow.session.execute(
                    text(
                        """
                        INSERT INTO emby_users_extended (emby_user_id, status)
                        VALUES (:user_id, :status)
                        ON CONFLICT (emby_user_id) DO UPDATE SET status = EXCLUDED.status
                        """
                    ),
                    {"user_id": user_id, "status": status},
                )
            elif operation.kind is IdentityOperationKind.DELETE_USER:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO identity_user_tombstones (
                            user_id, generation, last_operation_id, created_at, updated_at
                        ) VALUES (
                            :user_id, 1, :operation_id, :now, :now
                        )
                        ON CONFLICT (user_id) DO UPDATE SET
                            generation = identity_user_tombstones.generation + 1,
                            last_operation_id = EXCLUDED.last_operation_id,
                            updated_at = EXCLUDED.updated_at
                        """
                    ),
                    {"user_id": user_id, "operation_id": operation_id, "now": timestamp},
                )
                uow.session.execute(
                    text("DELETE FROM emby_favorite_items WHERE user_id = :user_id"),
                    {"user_id": user_id},
                )
                uow.session.execute(
                    text("DELETE FROM user_media_data WHERE user_id = :user_id"),
                    {"user_id": user_id},
                )
                uow.session.execute(
                    text("DELETE FROM emby_users WHERE id = :user_id"),
                    {"user_id": user_id},
                )
            elif operation.kind is IdentityOperationKind.UPLOAD_AVATAR:
                new_tag = str(normalized_result.get("new_tag") or "").strip() or None
                artifact_id = UUID(str(operation.request["artifact_id"]))
                updated = uow.session.execute(
                    text(
                        """
                        UPDATE emby_users
                        SET profile_image_tag = :new_tag, last_updated_at = :now
                        WHERE id = :user_id
                        """
                    ),
                    {"user_id": user_id, "new_tag": new_tag, "now": timestamp},
                )
                if int(getattr(updated, "rowcount", 0) or 0) != 1:
                    raise IdentityOperationStateError("avatar owner is missing from the local identity store")
                artifact = uow.session.execute(
                    text(
                        """
                        UPDATE identity_avatar_artifacts
                        SET status = 'consumed', consumed_at = :now, content_bytes = NULL
                        WHERE id = :artifact_id AND operation_id = :operation_id
                          AND owner_user_id = :user_id
                          AND status IN ('staged','claimed','purged')
                        """
                    ),
                    {
                        "artifact_id": artifact_id,
                        "operation_id": operation_id,
                        "user_id": user_id,
                        "now": timestamp,
                    },
                )
                if int(getattr(artifact, "rowcount", 0) or 0) != 1:
                    raise IdentityOperationStateError("avatar artifact is unavailable at local commit")
            else:
                raise IdentityOperationStateError("identity operation is not a supported user mutation")

            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'confirmed', current_step = 'local_commit', step_started_at = NULL, "
                "state_json = CAST(:state_json AS jsonb), result_json = CAST(:result_json AS jsonb), "
                "error_json = NULL, finished_at = :now, "
                "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
                {
                    "state_json": canonical_json(normalized_state),
                    "result_json": canonical_json(normalized_result),
                    "now": timestamp,
                },
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
                        SELECT id, status, current_step, step_started_at
                        FROM identity_operations
                        WHERE status IN ('claimed','requesting','compensating','remote_confirmed')
                          AND lease_expires_at <= :now
                        ORDER BY lease_expires_at, event_sequence
                        FOR UPDATE SKIP LOCKED
                        LIMIT :limit
                    )
                    UPDATE identity_operations AS operation
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
                    "available_at": timestamp + timedelta(seconds=max(0, float(retry_delay_seconds))),
                    "limit": max(1, int(limit)),
                },
            ).all()
            uow.commit()
            return len(rows)

    def purge_expired_avatar_artifacts(
        self,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    WITH expired AS (
                        SELECT id
                        FROM identity_avatar_artifacts
                        WHERE status IN ('staged','claimed') AND expires_at <= :now
                        ORDER BY expires_at, created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT :limit
                    )
                    UPDATE identity_avatar_artifacts AS artifact
                    SET status = 'purged', consumed_at = :now, content_bytes = NULL
                    FROM expired
                    WHERE artifact.id = expired.id
                    RETURNING artifact.id
                    """
                ),
                {"now": timestamp, "limit": max(1, int(limit))},
            ).all()
            uow.commit()
            return len(rows)

    def reject_dead_dispatches(
        self,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            ids = [
                UUID(str(row[0]))
                for row in uow.session.execute(
                    text(
                        """
                        SELECT operation.id
                        FROM identity_operations AS operation
                        JOIN outbox_events AS outbox
                          ON outbox.event_key = 'identity.operation.requested:' || operation.id::text
                        WHERE outbox.status = 'dead'
                          AND operation.status NOT IN (
                              'confirmed','rejected','compensated','indeterminate','cancelled'
                          )
                        ORDER BY operation.event_sequence
                        FOR UPDATE OF operation SKIP LOCKED
                        LIMIT :limit
                        """
                    ),
                    {"limit": max(1, int(limit))},
                )
            ]
            for operation_id in ids:
                operation = self._read_in_uow(uow, operation_id, lock=True)
                if operation is None or operation.terminal:
                    continue
                provider_may_have_started = _has_provider_receipt(operation.state) or (
                    operation.step_started_at is not None
                    and not (
                        operation.kind is IdentityOperationKind.REGISTER
                        and operation.current_step is IdentityOperationStep.PREFLIGHT_USERNAME
                    )
                )
                if not provider_may_have_started and operation.kind is IdentityOperationKind.REGISTER:
                    self._release_invitation(uow, operation, now=timestamp)
                uow.session.execute(
                    text(
                        """
                        UPDATE identity_operations
                        SET status = :status,
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = :now,
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            state_revision = state_revision + 1,
                            updated_at = :now
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": operation.id,
                        "status": "indeterminate" if provider_may_have_started else "rejected",
                        "error_json": canonical_json({"type": "identity_operation_dispatch_exhausted"}),
                        "now": timestamp,
                    },
                )
            uow.commit()
            return len(ids)

    def _reserve_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: IdentityOperationIntent,
        *,
        outbox_available_at: datetime | None = None,
    ) -> IdentityOperationReservation:
        if intent.kind is not IdentityOperationKind.REGISTER:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope_key, 0))"),
                {"scope_key": intent.scope_key},
            )
            user_id = intent.scope_key.removeprefix("user:")
            registration_in_progress = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM identity_operations AS registration
                        WHERE registration.kind = 'register'
                          AND registration.status IN (
                              'ready','claimed','requesting','awaiting_readback','retryable',
                              'compensating','remote_confirmed','indeterminate'
                          )
                          AND (
                              registration.state_json ->> 'remote_user_id' = :user_id
                              OR lower(registration.request_json ->> 'username') = (
                                  SELECT lower(name) FROM emby_users WHERE id = :user_id
                              )
                          )
                    )
                    """
                ),
                {"user_id": user_id},
            )
            if registration_in_progress:
                raise IdentityOperationScopeBusyError(
                    f"identity user {user_id!r} is owned by an active or indeterminate registration"
                )
            if intent.kind is IdentityOperationKind.ASSIGN_TEMPLATE:
                template_id = int(intent.request["template_id"])
                template_exists = uow.session.scalar(
                    text("SELECT id FROM user_templates WHERE id = :template_id FOR KEY SHARE"),
                    {"template_id": template_id},
                )
                if template_exists is None:
                    raise LookupError("identity template does not exist")
            active_key = uow.session.scalar(
                text(
                    """
                    SELECT operation_key
                    FROM identity_operations
                    WHERE scope_key = :scope_key
                      AND kind <> 'register'
                      AND status IN (
                          'ready','claimed','requesting','awaiting_readback','retryable',
                          'compensating','remote_confirmed','indeterminate'
                      )
                    LIMIT 1
                    """
                ),
                {"scope_key": intent.scope_key},
            )
            if active_key is not None and str(active_key) != intent.operation_key:
                raise IdentityOperationScopeBusyError(
                    f"identity operation scope {intent.scope_key!r} already has an active or indeterminate mutation"
                )
        row = (
            uow.session.execute(
                text(
                    """
                    INSERT INTO identity_operations (
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
                    "request_json": canonical_json(dict(intent.request), field="identity operation request"),
                    "available_at": intent.available_at,
                },
            )
            .mappings()
            .first()
        )
        created = row is not None
        if row is None:
            row = (
                uow.session.execute(
                    text("SELECT * FROM identity_operations WHERE operation_key = :key FOR UPDATE"),
                    {"key": intent.operation_key},
                )
                .mappings()
                .one()
            )
        operation = _operation(row)
        if (
            operation.id != intent.id
            or operation.request_hash != intent.request_hash
            or operation.kind is not intent.kind
            or operation.scope_key != intent.scope_key
        ):
            raise IdentityOperationStateError("identity operation idempotency conflict")
        if created:
            self._stage_outbox(uow, operation, available_at=outbox_available_at)
        return IdentityOperationReservation(operation, created)

    @staticmethod
    def _read_in_uow(
        uow: SqlAlchemyUnitOfWork,
        operation_id: UUID,
        *,
        lock: bool = False,
    ) -> IdentityOperation | None:
        suffix = " FOR UPDATE" if lock else ""
        row = (
            uow.session.execute(
                text(f"SELECT * FROM identity_operations WHERE id = :id{suffix}"),
                {"id": operation_id},
            )
            .mappings()
            .first()
        )
        return _operation(row) if row is not None else None

    @staticmethod
    def _require_live_claim(
        uow: SqlAlchemyUnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime,
        allowed: set[IdentityOperationStatus] | None = None,
        allow_cancel_requested: bool = False,
    ) -> IdentityOperation:
        statuses = allowed or {IdentityOperationStatus.CLAIMED}
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT *, lease_expires_at > :now AS lease_active
                    FROM identity_operations WHERE id = :id FOR UPDATE
                    """
                ),
                {"id": operation_id, "now": now},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise IdentityOperationClaimLostError("identity operation was not found")
        operation = _operation(row)
        if operation.status not in statuses or operation.claim_token != claim_token or not bool(row["lease_active"]):
            raise IdentityOperationClaimLostError("identity operation lost its active claim")
        if operation.cancel_requested_at is not None and not allow_cancel_requested:
            raise IdentityOperationClaimLostError("identity operation cancellation was requested")
        return operation

    @staticmethod
    def _claimed_update(
        uow: SqlAlchemyUnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        assignments: str,
        values: Mapping[str, Any],
        *,
        renew_lease_at: datetime | None = None,
        renew_lease_until: datetime | None = None,
    ) -> Mapping[Any, Any]:
        if (renew_lease_at is None) != (renew_lease_until is None):
            raise ValueError("identity claim lease renewal requires both timestamps")
        lease_assignment = (
            ", claimed_at = :renew_lease_at, lease_expires_at = :renew_lease_until"
            if renew_lease_until is not None
            else ""
        )
        return (
            uow.session.execute(
                text(
                    f"""
                    UPDATE identity_operations
                    SET {assignments}{lease_assignment},
                        state_revision = state_revision + 1, updated_at = NOW()
                    WHERE id = :id AND claim_token = :claim_token
                    RETURNING *
                    """
                ),
                {
                    "id": operation_id,
                    "claim_token": claim_token,
                    "renew_lease_at": renew_lease_at,
                    "renew_lease_until": renew_lease_until,
                    **dict(values),
                },
            )
            .mappings()
            .one()
        )

    def _mark_terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        status: IdentityOperationStatus,
        state: Mapping[str, Any],
        result: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
        release_invitation: bool,
        now: datetime,
    ) -> IdentityOperation:
        normalized_state = normalize_durable_json_object(dict(state), field="identity operation state")
        result_json = (
            canonical_json(normalize_durable_json_object(dict(result), field="identity operation result"))
            if result is not None
            else None
        )
        error_json = (
            canonical_json(normalize_durable_json_object(dict(error), field="identity operation error"))
            if error is not None
            else None
        )
        with self._unit_of_work() as uow:
            operation = self._require_live_claim(
                uow,
                operation_id,
                claim_token,
                now=now,
                allowed={
                    IdentityOperationStatus.CLAIMED,
                    IdentityOperationStatus.REQUESTING,
                    IdentityOperationStatus.COMPENSATING,
                    IdentityOperationStatus.REMOTE_CONFIRMED,
                },
                allow_cancel_requested=True,
            )
            if release_invitation and operation.kind is IdentityOperationKind.REGISTER:
                self._release_invitation(uow, operation, now=now)
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = :status, state_json = CAST(:state_json AS jsonb), "
                "result_json = CAST(:result_json AS jsonb), error_json = CAST(:error_json AS jsonb), "
                "finished_at = :now, claimed_by = NULL, claim_token = NULL, "
                "claimed_at = NULL, lease_expires_at = NULL",
                {
                    "status": status.value,
                    "state_json": canonical_json(normalized_state),
                    "result_json": result_json,
                    "error_json": error_json,
                    "now": now,
                },
            )
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    @staticmethod
    def _release_invitation(
        uow: SqlAlchemyUnitOfWork,
        operation: IdentityOperation,
        *,
        now: datetime,
    ) -> None:
        invitation_id = int(operation.request["invitation_id"])
        uow.session.execute(
            text(
                """
                UPDATE invitations
                SET status = CASE WHEN expires_at IS NULL OR expires_at >= :now THEN 'active' ELSE 'expired' END,
                    reservation_id = NULL, reserved_at = NULL, reservation_expires_at = NULL
                WHERE id = :invitation_id AND reservation_id = :operation_id AND status = 'reserved'
                """
            ),
            {"now": now, "invitation_id": invitation_id, "operation_id": operation.id},
        )

    @staticmethod
    def _stage_outbox(
        uow: SqlAlchemyUnitOfWork,
        operation: IdentityOperation,
        *,
        available_at: datetime | None = None,
    ) -> None:
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
                    payload_version, payload_json, status, available_at, created_at, updated_at
                ) VALUES (
                    :id, :event_key, :topic, :aggregate_type, :aggregate_id,
                    1, CAST(:payload_json AS jsonb), 'pending', :available_at, :created_at, :created_at
                )
                """
            ),
            {
                "id": uuid4(),
                "event_key": f"{IDENTITY_OPERATION_TOPIC}:{operation.id}",
                "topic": IDENTITY_OPERATION_TOPIC,
                "aggregate_type": IDENTITY_OPERATION_AGGREGATE_TYPE,
                "aggregate_id": str(operation.id),
                "payload_json": canonical_json(payload),
                "available_at": available_at or operation.available_at,
                "created_at": operation.available_at,
            },
        )

    @staticmethod
    def _complete_outbox(uow: SqlAlchemyUnitOfWork, operation_id: UUID) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL, updated_at = NOW()
                WHERE event_key = :event_key AND status <> 'dead'
                """
            ),
            {"event_key": f"{IDENTITY_OPERATION_TOPIC}:{operation_id}"},
        )

    @staticmethod
    def _arm_outbox(
        uow: SqlAlchemyUnitOfWork,
        operation_id: UUID,
        *,
        available_at: datetime,
    ) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET available_at = LEAST(available_at, :available_at), updated_at = NOW()
                WHERE event_key = :event_key AND status = 'pending'
                """
            ),
            {
                "event_key": f"{IDENTITY_OPERATION_TOPIC}:{operation_id}",
                "available_at": available_at,
            },
        )


def _renewed_claim_expiry(operation: IdentityOperation, now: datetime) -> datetime:
    if operation.claimed_at is None or operation.lease_expires_at is None:
        raise IdentityOperationStateError("identity operation claim is missing its lease interval")
    return now + (operation.lease_expires_at - operation.claimed_at)


def _same_snapshot_caller_intent(
    kind: IdentityOperationKind,
    reserved: Mapping[str, Any],
    finalized: Mapping[str, Any],
) -> bool:
    fields = {
        IdentityOperationKind.ASSIGN_TEMPLATE: ("user_id", "request_id", "template_id"),
        IdentityOperationKind.SET_STATUS: ("user_id", "request_id", "disable"),
    }.get(kind)
    return fields is not None and all(reserved.get(field) == finalized.get(field) for field in fields)


def _operation(row: Mapping[Any, Any]) -> IdentityOperation:
    return IdentityOperation(
        id=UUID(str(row["id"])),
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        kind=IdentityOperationKind(str(row["kind"])),
        scope_key=str(row["scope_key"]),
        request=dict(row["request_json"]),
        status=IdentityOperationStatus(str(row["status"])),
        current_step=IdentityOperationStep(str(row["current_step"])),
        state_revision=int(row["state_revision"]),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=UUID(str(row["claim_token"])) if row["claim_token"] is not None else None,
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        cancel_requested_at=row["cancel_requested_at"],
        step_started_at=row["step_started_at"],
        state=dict(row["state_json"] or {}),
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error=dict(row["error_json"]) if row["error_json"] is not None else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _has_provider_receipt(state: Mapping[str, Any]) -> bool:
    if str(state.get("remote_user_id") or "").strip():
        return True
    receipts = state.get("receipts")
    return bool(
        isinstance(receipts, Mapping)
        and any(isinstance(receipt, Mapping) and receipt.get("confirmed") is True for receipt in receipts.values())
    )


def _utc(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("identity operation timestamp must be timezone-aware")
    return timestamp.astimezone(UTC)


__all__ = [
    "IdentityOperationClaimLostError",
    "IdentityOperationStateError",
    "IdentityOperationScopeBusyError",
    "PostgresIdentityOperationRepository",
]
