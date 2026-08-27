from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from .effect_ports import (
    IdentityEmbyMutationPort,
    IdentityEmbyRegistrationPort,
    ProviderWriteOutcome,
    ProviderWriteResult,
    RegistrationSecretLease,
)
from .operations import (
    IdentityAvatarArtifact,
    IdentityOperation,
    IdentityOperationIntent,
    IdentityOperationKind,
    IdentityOperationReservation,
    IdentityOperationStatus,
    IdentityOperationStep,
)

_UNAVAILABLE = object()


class IdentityMutationIdempotencyConflictError(ValueError):
    pass


class IdentityOperationRepositoryPort(Protocol):
    def reserve_registration(
        self,
        *,
        invitation_token: str,
        username: str,
        reservation_ttl_seconds: float = 900,
        now: datetime | None = None,
    ) -> IdentityOperationReservation: ...

    def read(self, operation_id: UUID) -> IdentityOperation | None: ...

    def registration_token_matches(self, operation_id: UUID, invitation_token: str) -> bool: ...

    def read_active_scope(self, scope_key: str) -> IdentityOperation | None: ...

    def read_idempotent(
        self,
        *,
        kind: IdentityOperationKind,
        scope_key: str,
        idempotency_key: str,
    ) -> IdentityOperation | None: ...

    def reserve(
        self,
        intent: IdentityOperationIntent,
        *,
        outbox_delay_seconds: float = 0,
    ) -> IdentityOperationReservation: ...

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
    ) -> IdentityOperationReservation: ...

    def load_avatar_artifact(
        self,
        artifact_id: UUID,
        *,
        operation_id: UUID,
        owner_user_id: str,
        now: datetime | None = None,
    ) -> IdentityAvatarArtifact | None: ...

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation | None: ...

    def finalize_request_snapshot(
        self,
        operation_id: UUID,
        request: Mapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def reject_ready_before_write(
        self,
        operation_id: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def mark_step_started(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        step: IdentityOperationStep | str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def continue_claim(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        next_step: IdentityOperationStep | str,
        state: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def begin_compensation(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def defer_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def defer_local_commit(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        release_invitation: bool,
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def mark_compensated(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def complete_registration(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...

    def complete_user_operation(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> IdentityOperation: ...


@dataclass(frozen=True, slots=True)
class IdentityOperationExecution:
    operation: IdentityOperation
    claimed: bool


class RegistrationSagaCoordinator:
    """Runs registration without holding a DB lock across an Emby request."""

    def __init__(
        self,
        repository: IdentityOperationRepositoryPort,
        emby: IdentityEmbyRegistrationPort,
        *,
        max_attempts: int = 4,
        readback_delay_seconds: float = 5,
    ) -> None:
        if int(max_attempts) < 2:
            raise ValueError("identity operation max_attempts must be at least two")
        if float(readback_delay_seconds) < 0:
            raise ValueError("identity operation readback delay must be non-negative")
        self._repository = repository
        self._emby = emby
        self._max_attempts = int(max_attempts)
        self._readback_delay_seconds = float(readback_delay_seconds)

    def reserve(
        self,
        *,
        invitation_token: str,
        username: str,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        return self._repository.reserve_registration(
            invitation_token=invitation_token,
            username=username,
            now=now,
        )

    def read(self, operation_id: UUID) -> IdentityOperation | None:
        return self._repository.read(operation_id)

    def registration_token_matches(self, operation_id: UUID, invitation_token: str) -> bool:
        return self._repository.registration_token_matches(operation_id, invitation_token)

    def execute_with_secret(
        self,
        operation_id: UUID,
        secret: RegistrationSecretLease,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution | None:
        return self._execute(
            operation_id,
            secret=secret,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            now=now,
        )

    def execute_recovery(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution | None:
        return self._execute(
            operation_id,
            secret=None,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            now=now,
        )

    def _execute(
        self,
        operation_id: UUID,
        *,
        secret: RegistrationSecretLease | None,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None,
    ) -> IdentityOperationExecution | None:
        operation = self._repository.claim_by_id(
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            now=now,
        )
        if operation is None:
            current = self._repository.read(operation_id)
            return IdentityOperationExecution(current, claimed=False) if current is not None else None
        if operation.claim_token is None:
            raise AssertionError("claimed identity operation is missing its claim token")
        if operation.kind is not IdentityOperationKind.REGISTER:
            rejected = self._repository.mark_rejected(
                operation.id,
                operation.claim_token,
                state=operation.state,
                error={"type": "unsupported_identity_operation"},
                release_invitation=False,
                now=now,
            )
            return IdentityOperationExecution(rejected, claimed=True)
        completed = self._registration(operation, secret=secret, now=now)
        return IdentityOperationExecution(completed, claimed=True)

    def _registration(
        self,
        operation: IdentityOperation,
        *,
        secret: RegistrationSecretLease | None,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        state = dict(operation.state)
        username = str(operation.request["username"])

        if not bool(state.get("preflight_absent")):
            if (
                operation.current_step is not IdentityOperationStep.PREFLIGHT_USERNAME
                or operation.step_started_at is None
            ):
                operation = self._repository.mark_step_started(
                    operation.id,
                    claim_token,
                    step=IdentityOperationStep.PREFLIGHT_USERNAME,
                    state=state,
                    now=now,
                )
            try:
                existing = self._emby.find_user_by_name(username)
            except Exception:
                if operation.attempts >= self._max_attempts:
                    return self._repository.mark_rejected(
                        operation.id,
                        claim_token,
                        state=state,
                        error={"type": "username_preflight_unavailable"},
                        release_invitation=True,
                        now=now,
                    )
                return self._repository.mark_retryable(
                    operation.id,
                    claim_token,
                    state=state,
                    error={"type": "username_preflight_unavailable"},
                    retry_after_seconds=self._readback_delay_seconds,
                    now=now,
                )
            if existing is not None:
                return self._repository.mark_rejected(
                    operation.id,
                    claim_token,
                    state=state,
                    error={"type": "username_conflict"},
                    release_invitation=True,
                    now=now,
                )
            state["preflight_absent"] = True
            operation = self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=IdentityOperationStep.CREATE,
                state=state,
                now=now,
            )

        if not str(state.get("remote_user_id") or "").strip():
            create_checkpointed = (
                operation.current_step is IdentityOperationStep.CREATE and operation.step_started_at is not None
            )
            if secret is None and not create_checkpointed:
                return self._repository.mark_rejected(
                    operation.id,
                    claim_token,
                    state=state,
                    error={"type": "registration_secret_unavailable_before_create"},
                    release_invitation=True,
                    now=now,
                )
            operation = self._create_or_recover(operation, state=state, username=username, now=now)
            if operation.status is not IdentityOperationStatus.CLAIMED:
                return operation
            state = dict(operation.state)

        if not _step_confirmed(state, "credential_set"):
            operation = self._password_or_compensate(
                operation,
                state=state,
                username=username,
                secret=secret,
                now=now,
            )
            if operation.status is not IdentityOperationStatus.CLAIMED:
                return operation
            state = dict(operation.state)

        if not _step_confirmed(state, "policy"):
            operation = self._policy_or_recover(operation, state=state, now=now)
            if operation.status is not IdentityOperationStatus.CLAIMED:
                return operation
            state = dict(operation.state)

        configuration = operation.request.get("template_configuration")
        if isinstance(configuration, Mapping) and configuration and not _step_confirmed(state, "configuration"):
            operation = self._configuration_or_recover(operation, state=state, now=now)
            if operation.status is not IdentityOperationStatus.CLAIMED:
                return operation
            state = dict(operation.state)

        operation = self._repository.continue_claim(
            operation.id,
            claim_token,
            next_step=IdentityOperationStep.LOCAL_COMMIT,
            state=state,
            now=now,
        )
        result = {
            "username": username,
            "remote_user_id": str(state["remote_user_id"]),
            "expiration_info": _expiration_info(operation.request.get("expiration_date")),
            "template_description": str(
                operation.request.get("template_description") or operation.request.get("template_name") or ""
            ),
        }
        try:
            return self._repository.complete_registration(
                operation.id,
                claim_token,
                state=state,
                result=result,
                now=now,
            )
        except Exception:
            failure_state = dict(state)
            local_attempts = int(failure_state.get("registration_local_commit_attempts") or 0) + 1
            failure_state["registration_local_commit_attempts"] = local_attempts
            if local_attempts >= self._max_attempts:
                return self._repository.mark_indeterminate(
                    operation.id,
                    claim_token,
                    state=failure_state,
                    error={"type": "registration_local_commit_failed"},
                    now=now,
                )
            return self._repository.defer_local_commit(
                operation.id,
                claim_token,
                state=failure_state,
                error={"type": "registration_local_commit_failed"},
                retry_after_seconds=self._readback_delay_seconds,
                now=now,
            )

    def _create_or_recover(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        username: str,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        result: ProviderWriteResult | None = None
        user_id: str | None = None
        already_started = (
            operation.current_step is IdentityOperationStep.CREATE and operation.step_started_at is not None
        )
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=IdentityOperationStep.CREATE,
                state=state,
                now=now,
            )
            try:
                result, user_id = self._emby.create_user(username)
            except Exception:
                return self._repository.mark_indeterminate(
                    operation.id,
                    claim_token,
                    state=state,
                    error={"type": "remote_user_create_ownership_unverifiable"},
                    now=now,
                )
        else:
            return self._repository.mark_indeterminate(
                operation.id,
                claim_token,
                state=state,
                error={"type": "remote_user_create_ownership_unverifiable"},
                now=now,
            )
        if result is not None and result.outcome is ProviderWriteOutcome.REJECTED:
            return self._repository.mark_rejected(
                operation.id,
                claim_token,
                state=state,
                error={"type": "remote_user_create_rejected", "status_code": result.status_code},
                release_invitation=True,
                now=now,
            )
        if result is None or result.outcome is not ProviderWriteOutcome.CONFIRMED or not str(user_id or "").strip():
            return self._repository.mark_indeterminate(
                operation.id,
                claim_token,
                state=state,
                error={"type": "remote_user_create_ownership_unverifiable"},
                now=now,
            )
        user_id = str(user_id).strip()
        state["remote_user_id"] = user_id
        _confirm_step(state, "create", status_code=result.status_code if result is not None else None)
        return self._repository.continue_claim(
            operation.id,
            claim_token,
            next_step=IdentityOperationStep.PASSWORD,
            state=state,
            now=now,
        )

    def _password_or_compensate(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        username: str,
        secret: RegistrationSecretLease | None,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        if secret is None:
            return self._compensate(
                operation,
                state=state,
                error_type="registration_secret_unavailable_after_recovery",
                now=now,
            )
        password = secret.reveal()
        already_started = (
            operation.current_step is IdentityOperationStep.PASSWORD and operation.step_started_at is not None
        )
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=IdentityOperationStep.PASSWORD,
                state=state,
                now=now,
            )
            write_result = self._emby.set_password(str(state["remote_user_id"]), password)
        verified = self._emby.verify_password(username, password)
        if verified is True:
            _confirm_step(
                state,
                "credential_set",
                status_code=write_result.status_code if write_result else None,
            )
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=IdentityOperationStep.POLICY,
                state=state,
                now=now,
            )
        return self._compensate(
            operation,
            state=state,
            error_type="registration_password_not_confirmed",
            now=now,
        )

    def _policy_or_recover(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        policy = operation.request.get("template_policy")
        if not isinstance(policy, Mapping):
            return self._compensate(operation, state=state, error_type="invalid_template_policy", now=now)
        already_started = (
            operation.current_step is IdentityOperationStep.POLICY and operation.step_started_at is not None
        )
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=IdentityOperationStep.POLICY,
                state=state,
                now=now,
            )
            write_result = self._emby.set_policy(str(state["remote_user_id"]), policy)
        observed = self._read_user(str(state["remote_user_id"]))
        if isinstance(observed, Mapping) and _mapping_contains(observed.get("Policy"), policy):
            _confirm_step(state, "policy", status_code=write_result.status_code if write_result else None)
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=IdentityOperationStep.CONFIGURATION,
                state=state,
                now=now,
            )
        if write_result is not None and write_result.outcome is ProviderWriteOutcome.REJECTED:
            return self._compensate(operation, state=state, error_type="registration_policy_rejected", now=now)
        return self._defer_or_compensate(
            operation,
            state=state,
            error_type="registration_policy_not_observed",
            now=now,
        )

    def _configuration_or_recover(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        configuration = operation.request.get("template_configuration")
        if not isinstance(configuration, Mapping):
            return self._compensate(operation, state=state, error_type="invalid_template_configuration", now=now)
        already_started = (
            operation.current_step is IdentityOperationStep.CONFIGURATION and operation.step_started_at is not None
        )
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=IdentityOperationStep.CONFIGURATION,
                state=state,
                now=now,
            )
            write_result = self._emby.set_configuration(str(state["remote_user_id"]), configuration)
        observed = self._read_user(str(state["remote_user_id"]))
        if isinstance(observed, Mapping) and _mapping_contains(observed.get("Configuration"), configuration):
            _confirm_step(state, "configuration", status_code=write_result.status_code if write_result else None)
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=IdentityOperationStep.LOCAL_COMMIT,
                state=state,
                now=now,
            )
        if write_result is not None and write_result.outcome is ProviderWriteOutcome.REJECTED:
            return self._compensate(
                operation,
                state=state,
                error_type="registration_configuration_rejected",
                now=now,
            )
        return self._defer_or_compensate(
            operation,
            state=state,
            error_type="registration_configuration_not_observed",
            now=now,
        )

    def _defer_or_compensate(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        error_type: str,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        if operation.attempts >= self._max_attempts:
            return self._compensate(operation, state=state, error_type=error_type, now=now)
        return self._repository.defer_readback(
            operation.id,
            claim_token,
            state=state,
            error={"type": error_type},
            retry_after_seconds=self._readback_delay_seconds,
            now=now,
        )

    def _compensate(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        error_type: str,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        remote_user_id = str(state.get("remote_user_id") or "").strip()
        if not remote_user_id:
            return self._repository.mark_rejected(
                operation.id,
                claim_token,
                state=state,
                error={"type": error_type},
                release_invitation=True,
                now=now,
            )
        compensation_attempt = int(state.get("registration_compensation_attempts") or 0)
        if state.get("registration_compensation_claim_attempt") != operation.attempts:
            compensation_attempt += 1
            state["registration_compensation_attempts"] = compensation_attempt
            state["registration_compensation_claim_attempt"] = operation.attempts
        already_started = (
            operation.current_step is IdentityOperationStep.DELETE_REMOTE_USER and operation.step_started_at is not None
        )
        if not already_started:
            operation = self._repository.begin_compensation(
                operation.id,
                claim_token,
                state=state,
                error={"type": error_type},
                now=now,
            )
            delete_result = self._emby.delete_user(remote_user_id)
        else:
            delete_result = None
        observed = self._read_user(remote_user_id)
        if observed is None:
            _confirm_step(state, "delete_remote_user", status_code=delete_result.status_code if delete_result else None)
            return self._repository.mark_compensated(
                operation.id,
                claim_token,
                state=state,
                result={"remote_user_absent": True},
                now=now,
            )
        if compensation_attempt >= self._max_attempts or (
            delete_result is not None and delete_result.outcome is ProviderWriteOutcome.REJECTED
        ):
            return self._repository.mark_indeterminate(
                operation.id,
                claim_token,
                state=state,
                error={"type": "remote_user_compensation_not_confirmed"},
                now=now,
            )
        return self._repository.defer_readback(
            operation.id,
            claim_token,
            state=state,
            error={"type": "remote_user_compensation_outcome_unknown"},
            retry_after_seconds=self._readback_delay_seconds,
            now=now,
        )

    def _read_user(self, user_id: str) -> Mapping[str, object] | None | object:
        try:
            return self._emby.get_user(user_id)
        except Exception:
            return _UNAVAILABLE


class IdentityUserMutationCoordinator:
    """Durable Emby user mutations with authoritative readback before local commit."""

    _SUPPORTED_KINDS = frozenset(
        {
            IdentityOperationKind.ASSIGN_TEMPLATE,
            IdentityOperationKind.SET_STATUS,
            IdentityOperationKind.DELETE_USER,
            IdentityOperationKind.UPLOAD_AVATAR,
        }
    )

    def __init__(
        self,
        repository: IdentityOperationRepositoryPort,
        emby: IdentityEmbyMutationPort,
        *,
        max_attempts: int = 4,
        readback_delay_seconds: float = 5,
    ) -> None:
        if int(max_attempts) < 2:
            raise ValueError("identity operation max_attempts must be at least two")
        if float(readback_delay_seconds) < 0:
            raise ValueError("identity operation readback delay must be non-negative")
        self._repository = repository
        self._emby = emby
        self._max_attempts = int(max_attempts)
        self._readback_delay_seconds = float(readback_delay_seconds)

    def reserve(
        self,
        *,
        kind: IdentityOperationKind,
        user_id: str,
        request: Mapping[str, Any],
        idempotency_key: str | None = None,
        outbox_delay_seconds: float = 90,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        if kind not in self._SUPPORTED_KINDS or kind is IdentityOperationKind.UPLOAD_AVATAR:
            raise ValueError("identity mutation kind must use its supported reservation path")
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            raise ValueError("identity mutation user id must not be empty")
        payload = {"user_id": normalized_user_id, **dict(request)}
        normalized_idempotency = str(idempotency_key or "").strip()
        operation_identity = normalized_idempotency or (
            str(payload.get("request_id") or "").strip() if payload.get("snapshot_pending") is True else None
        )
        intent = IdentityOperationIntent.create(
            kind=kind,
            scope_key=f"user:{normalized_user_id}",
            request=payload,
            available_at=now,
            idempotency_key=operation_identity,
        )
        try:
            return self._repository.reserve(intent, outbox_delay_seconds=outbox_delay_seconds)
        except Exception:
            normalized_key = str(idempotency_key or "").strip()
            if not normalized_key:
                existing = self._repository.read_active_scope(f"user:{normalized_user_id}")
                if (
                    existing is not None
                    and existing.kind is kind
                    and _same_caller_intent(kind, existing.request, payload)
                ):
                    return IdentityOperationReservation(existing, created=False)
                raise
            existing = self._repository.read_idempotent(
                kind=kind,
                scope_key=f"user:{normalized_user_id}",
                idempotency_key=normalized_key,
            )
            if existing is None:
                raise
            if not _same_caller_intent(kind, existing.request, payload):
                raise IdentityMutationIdempotencyConflictError(
                    "identity idempotency key was already used for a different caller intent"
                ) from None
            return IdentityOperationReservation(existing, created=False)

    def read(self, operation_id: UUID) -> IdentityOperation | None:
        return self._repository.read(operation_id)

    def finalize_request(
        self,
        operation_id: UUID,
        request: Mapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> IdentityOperation:
        return self._repository.finalize_request_snapshot(operation_id, request, now=now)

    def reject_ready(
        self,
        operation_id: UUID,
        *,
        error_type: str,
        now: datetime | None = None,
    ) -> IdentityOperation:
        return self._repository.reject_ready_before_write(
            operation_id,
            error={"type": str(error_type)},
            now=now,
        )

    def find_idempotent(
        self,
        *,
        kind: IdentityOperationKind,
        user_id: str,
        idempotency_key: str,
    ) -> IdentityOperation | None:
        normalized_user_id = str(user_id or "").strip()
        normalized_key = str(idempotency_key or "").strip()
        if kind not in self._SUPPORTED_KINDS or kind is IdentityOperationKind.UPLOAD_AVATAR:
            raise ValueError("identity mutation kind does not support HTTP idempotency lookup")
        if not normalized_user_id or not normalized_key:
            raise ValueError("identity idempotency lookup requires a user and key")
        return self._repository.read_idempotent(
            kind=kind,
            scope_key=f"user:{normalized_user_id}",
            idempotency_key=normalized_key,
        )

    def reserve_avatar(
        self,
        *,
        user_id: str,
        content: bytes,
        content_type: str,
        previous_tag: str | None,
        idempotency_key: str | None = None,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        normalized_key = str(idempotency_key or "").strip() or None
        normalized_user_id = str(user_id or "").strip()
        normalized_content_type = str(content_type or "").partition(";")[0].strip().casefold()
        digest = hashlib.sha256(bytes(content)).hexdigest()

        def existing_reservation() -> IdentityOperationReservation | None:
            existing = (
                self._repository.read_idempotent(
                    kind=IdentityOperationKind.UPLOAD_AVATAR,
                    scope_key=f"user:{normalized_user_id}",
                    idempotency_key=normalized_key,
                )
                if normalized_key is not None
                else self._repository.read_active_scope(f"user:{normalized_user_id}")
            )
            if existing is None:
                return None
            if existing.kind is not IdentityOperationKind.UPLOAD_AVATAR:
                return None
            if (
                existing.request.get("sha256") != digest
                or existing.request.get("size_bytes") != len(content)
                or existing.request.get("content_type") != normalized_content_type
            ):
                raise IdentityMutationIdempotencyConflictError(
                    "avatar idempotency key was already used for different content"
                )
            return IdentityOperationReservation(existing, created=False)

        replay = existing_reservation()
        if replay is not None and normalized_key is not None:
            return replay
        try:
            return self._repository.reserve_avatar_upload(
                user_id=normalized_user_id,
                content=content,
                content_type=normalized_content_type,
                previous_tag=previous_tag,
                idempotency_key=normalized_key,
                outbox_delay_seconds=90,
                now=now,
            )
        except Exception:
            replay = existing_reservation()
            if replay is not None:
                return replay
            raise

    def execute(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution | None:
        operation = self._repository.claim_by_id(
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            now=now,
        )
        if operation is None:
            current = self._repository.read(operation_id)
            return IdentityOperationExecution(current, claimed=False) if current is not None else None
        if operation.claim_token is None:
            raise AssertionError("claimed identity operation is missing its claim token")
        if operation.request.get("snapshot_pending") is True:
            completed = self._repository.mark_rejected(
                operation.id,
                operation.claim_token,
                state=operation.state,
                error={"type": "provider_snapshot_unavailable_before_write"},
                release_invitation=False,
                now=now,
            )
            return IdentityOperationExecution(completed, claimed=True)
        if operation.kind is IdentityOperationKind.ASSIGN_TEMPLATE:
            completed = self._assign_template(operation, now=now)
        elif operation.kind is IdentityOperationKind.SET_STATUS:
            completed = self._set_status(operation, now=now)
        elif operation.kind is IdentityOperationKind.DELETE_USER:
            completed = self._delete_user(operation, now=now)
        elif operation.kind is IdentityOperationKind.UPLOAD_AVATAR:
            completed = self._upload_avatar(operation, now=now)
        else:
            completed = self._repository.mark_rejected(
                operation.id,
                operation.claim_token,
                state=operation.state,
                error={"type": "unsupported_identity_operation"},
                release_invitation=False,
                now=now,
            )
        return IdentityOperationExecution(completed, claimed=True)

    def _assign_template(
        self,
        operation: IdentityOperation,
        *,
        now: datetime | None,
    ) -> IdentityOperation:
        if bool(operation.state.get("assign_template_compensation")):
            return self._restore_assign_template(operation, now=now)
        policy = operation.request.get("template_policy")
        if not isinstance(policy, Mapping):
            return self._reject_before_write(operation, "invalid_template_policy", now=now)
        operation = self._apply_mapping(
            operation,
            step=IdentityOperationStep.POLICY,
            receipt="policy",
            desired=policy,
            observed_key="Policy",
            writer=self._emby.set_policy,
            next_step=IdentityOperationStep.CONFIGURATION,
            now=now,
        )
        if operation.status is not IdentityOperationStatus.CLAIMED:
            return operation
        configuration = operation.request.get("template_configuration")
        if isinstance(configuration, Mapping) and configuration:
            operation = self._apply_mapping(
                operation,
                step=IdentityOperationStep.CONFIGURATION,
                receipt="configuration",
                desired=configuration,
                observed_key="Configuration",
                writer=self._emby.set_configuration,
                next_step=IdentityOperationStep.LOCAL_COMMIT,
                now=now,
            )
            if operation.status is not IdentityOperationStatus.CLAIMED:
                return operation
        return self._complete_user_projection(
            operation,
            state=operation.state,
            result={
                "user_id": str(operation.request["user_id"]),
                "template_id": int(operation.request["template_id"]),
            },
            now=now,
        )

    def _set_status(
        self,
        operation: IdentityOperation,
        *,
        now: datetime | None,
    ) -> IdentityOperation:
        policy = operation.request.get("desired_policy")
        if not isinstance(policy, Mapping):
            return self._reject_before_write(operation, "invalid_status_policy", now=now)
        operation = self._apply_mapping(
            operation,
            step=IdentityOperationStep.POLICY,
            receipt="status_policy",
            desired=policy,
            observed_key="Policy",
            writer=self._emby.set_policy,
            next_step=IdentityOperationStep.LOCAL_COMMIT,
            now=now,
        )
        if operation.status is not IdentityOperationStatus.CLAIMED:
            return operation
        return self._complete_user_projection(
            operation,
            state=operation.state,
            result={
                "user_id": str(operation.request["user_id"]),
                "disabled": bool(operation.request.get("disable")),
            },
            now=now,
        )

    def _delete_user(
        self,
        operation: IdentityOperation,
        *,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        state = dict(operation.state)
        user_id = str(operation.request["user_id"])
        if _step_confirmed(state, "delete_remote_user"):
            return self._complete_user_projection(
                operation,
                state=state,
                result={"user_id": user_id, "remote_user_absent": True},
                now=now,
            )
        already_started = (
            operation.current_step is IdentityOperationStep.DELETE_REMOTE_USER and operation.step_started_at is not None
        )
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=IdentityOperationStep.DELETE_REMOTE_USER,
                state=state,
                now=now,
            )
            write_result = self._emby.delete_user(user_id)
        observed = self._read_user(user_id)
        if observed is None:
            _confirm_step(
                state,
                "delete_remote_user",
                status_code=write_result.status_code if write_result else None,
            )
            return self._complete_user_projection(
                operation,
                state=state,
                result={"user_id": user_id, "remote_user_absent": True},
                now=now,
            )
        if write_result is not None and write_result.outcome is ProviderWriteOutcome.REJECTED:
            return self._repository.mark_rejected(
                operation.id,
                claim_token,
                state=state,
                error={"type": "remote_user_delete_rejected", "status_code": write_result.status_code},
                release_invitation=False,
                now=now,
            )
        return self._defer_or_indeterminate(
            operation,
            state=state,
            error_type="remote_user_delete_not_observed",
            now=now,
        )

    def _upload_avatar(
        self,
        operation: IdentityOperation,
        *,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        state = dict(operation.state)
        user_id = str(operation.request["user_id"])
        if _step_confirmed(state, "upload_avatar"):
            return self._complete_user_projection(
                operation,
                state=state,
                result={"user_id": user_id, "new_tag": state.get("avatar_new_tag")},
                now=now,
            )
        already_started = (
            operation.current_step is IdentityOperationStep.UPLOAD_AVATAR and operation.step_started_at is not None
        )
        expected_digest = bytes.fromhex(str(operation.request["sha256"]))
        if already_started:
            if self._read_avatar_digest(user_id) == expected_digest:
                return self._complete_avatar(
                    operation,
                    state=state,
                    status_code=None,
                    now=now,
                )
            return self._defer_or_indeterminate(
                operation,
                state=state,
                error_type="avatar_upload_not_observed",
                now=now,
            )
        artifact = self._repository.load_avatar_artifact(
            UUID(str(operation.request["artifact_id"])),
            operation_id=operation.id,
            owner_user_id=user_id,
            now=now,
        )
        if artifact is None:
            return self._reject_before_write(operation, "avatar_artifact_unavailable", now=now)
        if (
            artifact.operation_id != operation.id
            or artifact.sha256 != expected_digest
            or hashlib.sha256(artifact.content).digest() != expected_digest
            or artifact.size_bytes != int(operation.request["size_bytes"])
            or len(artifact.content) != artifact.size_bytes
        ):
            return self._reject_before_write(operation, "avatar_artifact_integrity_failure", now=now)
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=IdentityOperationStep.UPLOAD_AVATAR,
                state=state,
                now=now,
            )
            write_result = self._emby.upload_avatar(
                user_id,
                artifact.content,
                artifact.content_type,
            )
        observed_digest = self._read_avatar_digest(user_id)
        if observed_digest == expected_digest:
            return self._complete_avatar(
                operation,
                state=state,
                status_code=write_result.status_code if write_result else None,
                now=now,
            )
        if write_result is not None and write_result.outcome is ProviderWriteOutcome.REJECTED:
            return self._repository.mark_rejected(
                operation.id,
                claim_token,
                state=state,
                error={"type": "avatar_upload_rejected", "status_code": write_result.status_code},
                release_invitation=False,
                now=now,
            )
        return self._defer_or_indeterminate(
            operation,
            state=state,
            error_type="avatar_upload_not_observed",
            now=now,
        )

    def _complete_avatar(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        status_code: int | None,
        now: datetime | None,
    ) -> IdentityOperation:
        user_id = str(operation.request["user_id"])
        observed = self._read_user(user_id)
        new_tag = _primary_image_tag(observed)
        if new_tag is None:
            return self._defer_or_indeterminate(
                operation,
                state=state,
                error_type="avatar_tag_not_observed",
                now=now,
            )
        state["avatar_new_tag"] = new_tag
        _confirm_step(state, "upload_avatar", status_code=status_code)
        return self._complete_user_projection(
            operation,
            state=state,
            result={"user_id": user_id, "new_tag": new_tag},
            now=now,
        )

    def _complete_user_projection(
        self,
        operation: IdentityOperation,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None,
    ) -> IdentityOperation:
        try:
            return self._repository.complete_user_operation(
                operation.id,
                _claim_token(operation),
                state=state,
                result=result,
                now=now,
            )
        except Exception:
            claim_token = _claim_token(operation)
            failure_state = dict(state)
            local_attempts = int(failure_state.get("local_commit_attempts") or 0) + 1
            failure_state["local_commit_attempts"] = local_attempts
            if local_attempts >= self._max_attempts:
                return self._repository.mark_indeterminate(
                    operation.id,
                    claim_token,
                    state=failure_state,
                    error={"type": "identity_local_commit_failed"},
                    now=now,
                )
            return self._repository.defer_local_commit(
                operation.id,
                claim_token,
                state=failure_state,
                error={"type": "identity_local_commit_failed"},
                retry_after_seconds=self._readback_delay_seconds,
                now=now,
            )

    def _apply_mapping(
        self,
        operation: IdentityOperation,
        *,
        step: IdentityOperationStep,
        receipt: str,
        desired: Mapping[str, object],
        observed_key: str,
        writer: Any,
        next_step: IdentityOperationStep,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        state = dict(operation.state)
        if _step_confirmed(state, receipt):
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=next_step,
                state=state,
                now=now,
            )
        user_id = str(operation.request["user_id"])
        already_started = operation.current_step is step and operation.step_started_at is not None
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=step,
                state=state,
                now=now,
            )
            write_result = writer(user_id, desired)
        observed = self._read_user(user_id)
        if isinstance(observed, Mapping) and _mapping_contains(observed.get(observed_key), desired):
            _confirm_step(state, receipt, status_code=write_result.status_code if write_result else None)
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=next_step,
                state=state,
                now=now,
            )
        if write_result is not None and write_result.outcome is ProviderWriteOutcome.REJECTED:
            if _has_confirmed_receipt(state):
                return self._start_assign_template_compensation(
                    operation,
                    state=state,
                    error_type=f"{receipt}_rejected_after_prior_effect",
                    now=now,
                )
            return self._repository.mark_rejected(
                operation.id,
                claim_token,
                state=state,
                error={"type": f"{receipt}_rejected", "status_code": write_result.status_code},
                release_invitation=False,
                now=now,
            )
        return self._defer_or_indeterminate(
            operation,
            state=state,
            error_type=f"{receipt}_not_observed",
            now=now,
        )

    def _reject_before_write(
        self,
        operation: IdentityOperation,
        error_type: str,
        *,
        now: datetime | None,
    ) -> IdentityOperation:
        return self._repository.mark_rejected(
            operation.id,
            _claim_token(operation),
            state=operation.state,
            error={"type": error_type},
            release_invitation=False,
            now=now,
        )

    def _defer_or_indeterminate(
        self,
        operation: IdentityOperation,
        *,
        state: Mapping[str, Any],
        error_type: str,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        if operation.attempts >= self._max_attempts:
            if operation.kind is IdentityOperationKind.ASSIGN_TEMPLATE and _has_confirmed_receipt(state):
                return self._start_assign_template_compensation(
                    operation,
                    state=dict(state),
                    error_type=error_type,
                    now=now,
                )
            return self._repository.mark_indeterminate(
                operation.id,
                claim_token,
                state=state,
                error={"type": error_type},
                now=now,
            )
        return self._repository.defer_readback(
            operation.id,
            claim_token,
            state=state,
            error={"type": error_type},
            retry_after_seconds=self._readback_delay_seconds,
            now=now,
        )

    def _start_assign_template_compensation(
        self,
        operation: IdentityOperation,
        *,
        state: dict[str, Any],
        error_type: str,
        now: datetime | None,
    ) -> IdentityOperation:
        state["assign_template_compensation"] = True
        state["compensation_reason"] = error_type
        operation = self._repository.continue_claim(
            operation.id,
            _claim_token(operation),
            next_step=IdentityOperationStep.CONFIGURATION,
            state=state,
            now=now,
        )
        return self._restore_assign_template(operation, now=now)

    def _restore_assign_template(
        self,
        operation: IdentityOperation,
        *,
        now: datetime | None,
    ) -> IdentityOperation:
        previous_configuration = operation.request.get("previous_configuration")
        if isinstance(previous_configuration, Mapping):
            operation = self._restore_mapping(
                operation,
                step=IdentityOperationStep.CONFIGURATION,
                receipt="restore_configuration",
                desired=previous_configuration,
                observed_key="Configuration",
                writer=self._emby.set_configuration,
                next_step=IdentityOperationStep.POLICY,
                now=now,
            )
            if operation.status is not IdentityOperationStatus.CLAIMED:
                return operation
        previous_policy = operation.request.get("previous_policy")
        if not isinstance(previous_policy, Mapping):
            return self._repository.mark_indeterminate(
                operation.id,
                _claim_token(operation),
                state=operation.state,
                error={"type": "assign_template_prior_policy_missing"},
                now=now,
            )
        operation = self._restore_mapping(
            operation,
            step=IdentityOperationStep.POLICY,
            receipt="restore_policy",
            desired=previous_policy,
            observed_key="Policy",
            writer=self._emby.set_policy,
            next_step=IdentityOperationStep.LOCAL_COMMIT,
            now=now,
        )
        if operation.status is not IdentityOperationStatus.CLAIMED:
            return operation
        return self._repository.mark_rejected(
            operation.id,
            _claim_token(operation),
            state=operation.state,
            error={
                "type": "assign_template_compensated",
                "cause": str(operation.state.get("compensation_reason") or "unknown"),
            },
            release_invitation=False,
            now=now,
        )

    def _restore_mapping(
        self,
        operation: IdentityOperation,
        *,
        step: IdentityOperationStep,
        receipt: str,
        desired: Mapping[str, object],
        observed_key: str,
        writer: Any,
        next_step: IdentityOperationStep,
        now: datetime | None,
    ) -> IdentityOperation:
        claim_token = _claim_token(operation)
        state = dict(operation.state)
        if _step_confirmed(state, receipt):
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=next_step,
                state=state,
                now=now,
            )
        attempts = dict(state.get("compensation_attempts") or {})
        attempt_claims = dict(state.get("compensation_attempt_claims") or {})
        if attempt_claims.get(receipt) != operation.attempts:
            attempt_claims[receipt] = operation.attempts
            attempts[receipt] = int(attempts.get(receipt) or 0) + 1
            state["compensation_attempt_claims"] = attempt_claims
            state["compensation_attempts"] = attempts
        user_id = str(operation.request["user_id"])
        already_started = operation.current_step is step and operation.step_started_at is not None
        write_result: ProviderWriteResult | None = None
        if not already_started:
            operation = self._repository.mark_step_started(
                operation.id,
                claim_token,
                step=step,
                state=state,
                now=now,
            )
            write_result = writer(user_id, desired)
        observed = self._read_user(user_id)
        if isinstance(observed, Mapping) and _mapping_contains(observed.get(observed_key), desired):
            _confirm_step(state, receipt, status_code=write_result.status_code if write_result else None)
            return self._repository.continue_claim(
                operation.id,
                claim_token,
                next_step=next_step,
                state=state,
                now=now,
            )
        if int(attempts.get(receipt) or 0) >= self._max_attempts or (
            write_result is not None and write_result.outcome is ProviderWriteOutcome.REJECTED
        ):
            return self._repository.mark_indeterminate(
                operation.id,
                claim_token,
                state=state,
                error={"type": f"{receipt}_not_confirmed"},
                now=now,
            )
        return self._repository.defer_readback(
            operation.id,
            claim_token,
            state=state,
            error={"type": f"{receipt}_not_observed"},
            retry_after_seconds=self._readback_delay_seconds,
            now=now,
        )

    def _read_user(self, user_id: str) -> Mapping[str, object] | None | object:
        try:
            return self._emby.get_user(user_id)
        except Exception:
            return _UNAVAILABLE

    def _read_avatar_digest(self, user_id: str) -> bytes | None | object:
        try:
            return self._emby.avatar_sha256(user_id)
        except Exception:
            return _UNAVAILABLE


class IdentityOperationExecutor:
    """Worker-facing dispatcher. Registration recovery intentionally has no secret lease."""

    def __init__(
        self,
        registration: RegistrationSagaCoordinator,
        user_mutations: IdentityUserMutationCoordinator,
        repository: IdentityOperationRepositoryPort,
    ) -> None:
        self._registration = registration
        self._user_mutations = user_mutations
        self._repository = repository

    def execute(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution | None:
        operation = self._repository.read(operation_id)
        if operation is None:
            return None
        if operation.kind is IdentityOperationKind.REGISTER:
            return self._registration.execute_recovery(
                operation_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                now=now,
            )
        return self._user_mutations.execute(
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            now=now,
        )


def _confirm_step(state: dict[str, Any], step: str, *, status_code: int | None) -> None:
    receipts = dict(state.get("receipts") or {})
    receipt: dict[str, Any] = {"confirmed": True}
    if status_code is not None:
        receipt["status_code"] = int(status_code)
    receipts[step] = receipt
    state["receipts"] = receipts


def _claim_token(operation: IdentityOperation) -> UUID:
    if operation.claim_token is None:
        raise AssertionError("claimed identity operation is missing its claim token")
    return operation.claim_token


def _step_confirmed(state: Mapping[str, Any], step: str) -> bool:
    receipts = state.get("receipts")
    return bool(
        isinstance(receipts, Mapping)
        and isinstance(receipts.get(step), Mapping)
        and receipts[step].get("confirmed") is True
    )


def _has_confirmed_receipt(state: Mapping[str, Any]) -> bool:
    receipts = state.get("receipts")
    return bool(
        isinstance(receipts, Mapping)
        and any(isinstance(value, Mapping) and value.get("confirmed") is True for value in receipts.values())
    )


def _same_caller_intent(
    kind: IdentityOperationKind,
    existing: Mapping[str, Any],
    requested: Mapping[str, Any],
) -> bool:
    fields = {
        IdentityOperationKind.ASSIGN_TEMPLATE: ("user_id", "template_id"),
        IdentityOperationKind.SET_STATUS: ("user_id", "disable"),
        IdentityOperationKind.DELETE_USER: ("user_id",),
    }.get(kind, ("user_id",))
    return all(existing.get(field) == requested.get(field) for field in fields)


def _primary_image_tag(observed: object) -> str | None:
    if not isinstance(observed, Mapping):
        return None
    direct = str(observed.get("PrimaryImageTag") or "").strip()
    if direct:
        return direct
    image_tags = observed.get("ImageTags")
    if isinstance(image_tags, Mapping):
        value = str(image_tags.get("Primary") or "").strip()
        return value or None
    return None


def _mapping_contains(observed: object, expected: Mapping[str, object]) -> bool:
    if not isinstance(observed, Mapping):
        return False
    if not expected:
        return not observed
    for key, value in expected.items():
        current = observed.get(key)
        if isinstance(value, Mapping):
            if not _mapping_contains(current, value):
                return False
        elif current != value:
            return False
    return True


def _expiration_info(value: object) -> str:
    if value is None:
        return "永久有效"
    normalized = str(value)
    return f"至 {normalized[:10]}"


__all__ = [
    "IdentityOperationExecution",
    "IdentityOperationExecutor",
    "IdentityOperationRepositoryPort",
    "IdentityMutationIdempotencyConflictError",
    "IdentityUserMutationCoordinator",
    "RegistrationSagaCoordinator",
]
