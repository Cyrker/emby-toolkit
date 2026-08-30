from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from etk.platform.jobs.json_payload import normalize_durable_json_object

from .operations import (
    SharingOperation,
    SharingOperationPreconditionError,
    SharingOperationStateError,
    SharingOperationStatus,
)
from .repository import DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS, PostgresSharingOperationRepository


class SharingDeliveryDisposition(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    RETRYABLE = "retryable"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class SharingDeliveryOutcome:
    disposition: SharingDeliveryDisposition
    result: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, SharingDeliveryDisposition):
            raise TypeError("sharing delivery disposition is invalid")
        for field in ("result", "error"):
            value = getattr(self, field)
            if value is not None:
                if not isinstance(value, Mapping):
                    raise TypeError(f"sharing delivery {field} must be an object")
                object.__setattr__(
                    self,
                    field,
                    normalize_durable_json_object(dict(value), field=f"sharing delivery {field}"),
                )
        if self.retry_after_seconds is not None:
            if isinstance(self.retry_after_seconds, bool):
                raise TypeError("sharing delivery retry delay must be numeric")
            delay = float(self.retry_after_seconds)
            if not math.isfinite(delay) or delay < 0:
                raise ValueError("sharing delivery retry delay must be non-negative and finite")
            object.__setattr__(self, "retry_after_seconds", delay)

    @classmethod
    def confirmed(cls, result: Mapping[str, Any] | None = None) -> SharingDeliveryOutcome:
        return cls(SharingDeliveryDisposition.CONFIRMED, result=dict(result or {}))

    @classmethod
    def rejected(cls, error: Mapping[str, Any] | None = None) -> SharingDeliveryOutcome:
        return cls(SharingDeliveryDisposition.REJECTED, error=dict(error or {}))

    @classmethod
    def retryable(
        cls,
        error: Mapping[str, Any] | None = None,
        *,
        retry_after_seconds: float | None = None,
    ) -> SharingDeliveryOutcome:
        return cls(
            SharingDeliveryDisposition.RETRYABLE,
            error=dict(error or {}),
            retry_after_seconds=retry_after_seconds,
        )

    @classmethod
    def indeterminate(cls, error: Mapping[str, Any] | None = None) -> SharingDeliveryOutcome:
        return cls(SharingDeliveryDisposition.INDETERMINATE, error=dict(error or {}))


class SharingOperationDeliveryPort(Protocol):
    """Provider-specific sharing delivery with an explicit write checkpoint.

    ``request`` must invoke ``checkpoint`` immediately before the first
    external mutation. Validation and read-only discovery belong before that
    callback. Recovery never receives a write callback and therefore cannot
    resend the original mutation.
    """

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool: ...

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome: ...

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome: ...


@dataclass(frozen=True, slots=True)
class SharingOperationExecution:
    operation: SharingOperation
    claimed: bool


class SharingOperationExecutor:
    """Execute one durable sharing effect without replaying a checkpointed write."""

    def __init__(
        self,
        repository: PostgresSharingOperationRepository,
        delivery: SharingOperationDeliveryPort,
        *,
        max_attempts: int = DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS,
        retry_delay_seconds: float = 5,
    ) -> None:
        if isinstance(max_attempts, bool) or int(max_attempts) < 2:
            raise ValueError("sharing operation max_attempts must be at least two")
        delay = float(retry_delay_seconds)
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("sharing operation retry delay must be non-negative and finite")
        self._repository = repository
        self._delivery = delivery
        self._max_attempts = int(max_attempts)
        self._retry_delay_seconds = delay

    def execute(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> SharingOperationExecution | None:
        claimed = self._repository.claim_by_id(
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )
        if claimed is None:
            current = self._repository.read(operation_id)
            return SharingOperationExecution(current, claimed=False) if current is not None else None
        token = claimed.claim_token
        if token is None:
            raise AssertionError("claimed sharing operation is missing its claim token")

        if claimed.step_started_at is not None:
            recovered = self._recover(claimed, token)
            return SharingOperationExecution(recovered, claimed=True)

        try:
            self._repository.validate_prewrite(claimed)
        except SharingOperationStateError:
            completed = self._repository.mark_rejected(
                claimed.id,
                token,
                error={"type": "sharing_local_precondition_changed"},
            )
            return SharingOperationExecution(completed, claimed=True)
        except Exception as exc:
            completed = self._retry_before_write(
                claimed,
                token,
                error={"type": "sharing_local_precondition_error", "error_class": type(exc).__name__},
            )
            return SharingOperationExecution(completed, claimed=True)

        current = claimed
        checkpointed = False
        lost_claim: SharingOperation | None = None

        def checkpoint() -> None:
            nonlocal checkpointed, current, lost_claim
            if checkpointed:
                raise RuntimeError("sharing delivery invoked the provider checkpoint more than once")
            checkpointed = True
            try:
                current = self._repository.mark_requesting(claimed.id, token)
            except BaseException:
                try:
                    durable = self._repository.read(claimed.id)
                except BaseException:
                    raise
                if durable is not None:
                    current = durable
                    if durable.claim_token != token:
                        lost_claim = durable
                    elif durable.step_started_at is None:
                        checkpointed = False
                raise

        try:
            outcome = _validated_outcome(self._delivery.request(claimed, checkpoint=checkpoint))
        except SharingOperationPreconditionError:
            if lost_claim is not None:
                return SharingOperationExecution(lost_claim, claimed=True)
            completed = (
                self._post_checkpoint_failure(
                    current,
                    token,
                    error={"type": "sharing_local_precondition_changed"},
                )
                if checkpointed
                else self._repository.mark_rejected(
                    claimed.id,
                    token,
                    error={"type": "sharing_local_precondition_changed"},
                )
            )
            return SharingOperationExecution(completed, claimed=True)
        except (TypeError, ValueError) as exc:
            if lost_claim is not None:
                return SharingOperationExecution(lost_claim, claimed=True)
            if checkpointed:
                completed = self._post_checkpoint_failure(
                    current,
                    token,
                    error={"type": "invalid_checkpointed_delivery", "error_class": type(exc).__name__},
                )
            else:
                completed = self._repository.mark_rejected(
                    claimed.id,
                    token,
                    error={"type": "invalid_operation_request", "error_class": type(exc).__name__},
                )
            return SharingOperationExecution(completed, claimed=True)
        except Exception as exc:
            if lost_claim is not None:
                return SharingOperationExecution(lost_claim, claimed=True)
            error = {"type": "sharing_delivery_error", "error_class": type(exc).__name__}
            completed = (
                self._post_checkpoint_failure(current, token, error=error)
                if checkpointed
                else self._retry_before_write(claimed, token, error=error)
            )
            return SharingOperationExecution(completed, claimed=True)

        if not checkpointed and outcome.disposition is SharingDeliveryDisposition.CONFIRMED:
            completed = self._repository.mark_indeterminate(
                claimed.id,
                token,
                error={
                    "type": "provider_checkpoint_missing",
                    "provider_may_have_been_called": True,
                },
            )
            return SharingOperationExecution(completed, claimed=True)

        try:
            completed = self._apply_outcome(current, token, outcome, checkpointed=checkpointed)
        except SharingOperationStateError:
            durable = self._repository.read(claimed.id)
            if durable is not None and durable.status is SharingOperationStatus.CANCELLED:
                completed = durable
            else:
                raise
        return SharingOperationExecution(completed, claimed=True)

    def _recover(self, operation: SharingOperation, token: UUID) -> SharingOperation:
        if not self._supports_readback(operation):
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                error={
                    "type": "authoritative_readback_unavailable",
                    "provider_may_have_been_called": True,
                },
            )
        try:
            outcome = _validated_outcome(self._delivery.readback(operation))
        except Exception as exc:
            return self._defer_or_exhaust(
                operation,
                token,
                error={"type": "sharing_readback_error", "error_class": type(exc).__name__},
            )
        if outcome.disposition is SharingDeliveryDisposition.CONFIRMED:
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result=dict(outcome.result or {"authoritative_readback": True}),
            )
        if outcome.disposition is SharingDeliveryDisposition.REJECTED:
            return self._repository.mark_rejected(
                operation.id,
                token,
                error=dict(outcome.error or {"type": "provider_rejected_by_readback"}),
            )
        return self._defer_or_exhaust(
            operation,
            token,
            error=dict(outcome.error or {"type": "provider_state_not_confirmed"}),
            retry_after_seconds=outcome.retry_after_seconds,
        )

    def _apply_outcome(
        self,
        operation: SharingOperation,
        token: UUID,
        outcome: SharingDeliveryOutcome,
        *,
        checkpointed: bool,
    ) -> SharingOperation:
        if outcome.disposition is SharingDeliveryDisposition.CONFIRMED:
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result=dict(outcome.result or {"provider_response_confirmed": True}),
            )
        if outcome.disposition is SharingDeliveryDisposition.REJECTED:
            return self._repository.mark_rejected(
                operation.id,
                token,
                error=dict(outcome.error or {"type": "provider_rejected"}),
            )
        error = dict(outcome.error or {"type": f"provider_{outcome.disposition.value}"})
        if not checkpointed:
            if outcome.disposition is SharingDeliveryDisposition.RETRYABLE:
                return self._retry_before_write(
                    operation,
                    token,
                    error=error,
                    retry_after_seconds=outcome.retry_after_seconds,
                )
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                error={**error, "provider_may_have_been_called": True},
            )
        return self._post_checkpoint_failure(
            operation,
            token,
            error=error,
            retry_after_seconds=outcome.retry_after_seconds,
        )

    def _retry_before_write(
        self,
        operation: SharingOperation,
        token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float | None = None,
    ) -> SharingOperation:
        if operation.attempts >= self._max_attempts:
            return self._repository.mark_rejected(
                operation.id,
                token,
                error={**dict(error), "type": "prewrite_recovery_exhausted", "attempts": operation.attempts},
            )
        return self._repository.mark_retryable(
            operation.id,
            token,
            error=dict(error),
            retry_after_seconds=self._delay(retry_after_seconds),
        )

    def _post_checkpoint_failure(
        self,
        operation: SharingOperation,
        token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float | None = None,
    ) -> SharingOperation:
        if not self._supports_readback(operation):
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                error={
                    **dict(error),
                    "authoritative_readback": False,
                    "provider_may_have_been_called": True,
                },
            )
        return self._defer_or_exhaust(
            operation,
            token,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )

    def _defer_or_exhaust(
        self,
        operation: SharingOperation,
        token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float | None = None,
    ) -> SharingOperation:
        if operation.attempts >= self._max_attempts:
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                error={
                    **dict(error),
                    "type": "authoritative_readback_exhausted",
                    "attempts": operation.attempts,
                    "provider_may_have_been_called": True,
                },
            )
        return self._repository.mark_awaiting_readback(
            operation.id,
            token,
            error=dict(error),
            retry_after_seconds=self._delay(retry_after_seconds),
        )

    def _supports_readback(self, operation: SharingOperation) -> bool:
        try:
            return bool(self._delivery.supports_authoritative_readback(operation))
        except Exception:
            return False

    def _delay(self, value: float | None) -> float:
        if value is None:
            return self._retry_delay_seconds
        try:
            delay = float(value)
        except (TypeError, ValueError, OverflowError):
            return self._retry_delay_seconds
        if not math.isfinite(delay) or delay < 0:
            return self._retry_delay_seconds
        return delay


def _validated_outcome(value: object) -> SharingDeliveryOutcome:
    if not isinstance(value, SharingDeliveryOutcome):
        raise TypeError("sharing delivery returned an invalid outcome")
    return SharingDeliveryOutcome(
        value.disposition,
        result=value.result,
        error=value.error,
        retry_after_seconds=value.retry_after_seconds,
    )


__all__ = [
    "SharingDeliveryDisposition",
    "SharingDeliveryOutcome",
    "SharingOperationDeliveryPort",
    "SharingOperationExecution",
    "SharingOperationExecutor",
]
