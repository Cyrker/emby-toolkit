from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from .moviepilot import (
    MoviePilotProviderError,
    MoviePilotProviderErrorKind,
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteOutcome,
    MoviePilotWriteResult,
)
from .operations import SubscriptionOperation, SubscriptionOperationStatus


class SubscriptionProviderAction(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    UPDATE_STATUS = "update_status"
    DELETE = "delete"
    DELETE_BY_ID = "delete_by_id"
    SEARCH = "search"
    DELETE_DOWNLOADS = "delete_downloads"
    DELETE_TRANSFER_HISTORY = "delete_transfer_history"
    REDO_TRANSFER_HISTORY = "redo_transfer_history"


@dataclass(frozen=True, slots=True)
class SubscriptionOperationExecution:
    operation: SubscriptionOperation
    claimed: bool


@dataclass(frozen=True, slots=True)
class _WritePlan:
    action: SubscriptionProviderAction
    tmdb_id: str | None
    season: int | None
    subscription_id: str | None
    payload: Mapping[str, Any]
    expected: Mapping[str, Any]

    @property
    def supports_authoritative_readback(self) -> bool:
        return self.action in {
            SubscriptionProviderAction.CREATE,
            SubscriptionProviderAction.UPDATE,
            SubscriptionProviderAction.UPDATE_STATUS,
            SubscriptionProviderAction.DELETE,
        }


class MoviePilotOperationGateway(Protocol):
    def query_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotQueryResult: ...

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult: ...

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult: ...

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult: ...

    def delete_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotWriteResult: ...

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult: ...

    def search_subscription(self, subscription_id: int | str) -> MoviePilotWriteResult: ...

    def delete_download_tasks(self, hashes: Sequence[str]) -> MoviePilotWriteResult: ...

    def delete_transfer_history(self, record: Mapping[str, Any]) -> MoviePilotWriteResult: ...

    def redo_transfer_history(self, history_ids: Sequence[int]) -> MoviePilotWriteResult: ...


class SubscriptionOperationRepositoryPort(Protocol):
    def read(self, operation_id: UUID) -> SubscriptionOperation | None: ...

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SubscriptionOperation | None: ...

    def mark_requesting(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_remote_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        receipt: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        readback: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_confirmed_without_write(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        readback: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        readback: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def mark_cancelled_before_write(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        evidence: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation: ...

    def is_cancel_requested(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> bool: ...


class SubscriptionOperationExecutor:
    """Executes one durable MoviePilot write and recovers through readback only."""

    def __init__(
        self,
        repository: SubscriptionOperationRepositoryPort,
        gateway: MoviePilotOperationGateway,
        *,
        max_attempts: int = 4,
        readback_delay_seconds: float = 5,
    ) -> None:
        if isinstance(max_attempts, bool) or int(max_attempts) < 2:
            raise ValueError("subscription operation max_attempts must be at least two")
        delay = float(readback_delay_seconds)
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("subscription operation readback delay must be non-negative and finite")
        self._repository = repository
        self._gateway = gateway
        self._max_attempts = int(max_attempts)
        self._readback_delay_seconds = delay

    def execute(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SubscriptionOperationExecution | None:
        operation = self._repository.claim_by_id(
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            now=now,
        )
        if operation is None:
            current = self._repository.read(operation_id)
            return SubscriptionOperationExecution(current, claimed=False) if current is not None else None
        if operation.claim_token is None:
            raise AssertionError("claimed subscription operation is missing its claim token")
        claim_token = operation.claim_token

        try:
            plan = _write_plan(operation)
        except (TypeError, ValueError) as exc:
            rejected = self._repository.mark_rejected(
                operation.id,
                claim_token,
                error={"type": "invalid_operation_request", "error_class": type(exc).__name__},
                now=now,
            )
            return SubscriptionOperationExecution(rejected, claimed=True)

        if operation.provider_request_started_at is not None:
            recovered = (
                self._readback(operation, claim_token, plan, now=now)
                if plan.supports_authoritative_readback
                else self._recover_without_readback(operation, claim_token, plan, now=now)
            )
            return SubscriptionOperationExecution(recovered, claimed=True)

        if self._repository.is_cancel_requested(operation.id, claim_token, now=now):
            current = self._repository.read(operation.id)
            return SubscriptionOperationExecution(current, claimed=True) if current is not None else None

        if operation.request.get("preflight_readback") is True and plan.supports_authoritative_readback:
            preflight = self._preflight(operation, claim_token, plan, now=now)
            if preflight is not None:
                return SubscriptionOperationExecution(preflight, claimed=True)

        requesting = self._repository.mark_requesting(operation.id, claim_token, now=now)
        if requesting.status is not SubscriptionOperationStatus.REQUESTING:
            return SubscriptionOperationExecution(requesting, claimed=True)

        try:
            write_result = self._write(plan)
        except MoviePilotProviderError as exc:
            error = _provider_error(exc)
            if exc.outcome is MoviePilotWriteOutcome.REJECTED and exc.kind is not MoviePilotProviderErrorKind.CONFLICT:
                rejected = self._repository.mark_rejected(operation.id, claim_token, error=error, now=now)
                return SubscriptionOperationExecution(rejected, claimed=True)
            recovered = (
                self._readback(requesting, claim_token, plan, prior_error=error, now=now)
                if plan.supports_authoritative_readback
                else self._repository.mark_indeterminate(
                    operation.id,
                    claim_token,
                    error={**error, "action": plan.action.value, "authoritative_readback": False},
                    now=now,
                )
            )
            return SubscriptionOperationExecution(recovered, claimed=True)
        except (TypeError, ValueError) as exc:
            rejected = self._repository.mark_rejected(
                operation.id,
                claim_token,
                error={
                    "type": "provider_request_rejected_before_transport",
                    "error_class": type(exc).__name__,
                },
                now=now,
            )
            return SubscriptionOperationExecution(rejected, claimed=True)
        except Exception as exc:
            error = {"type": "provider_unexpected_error", "error_class": type(exc).__name__}
            recovered = (
                self._readback(requesting, claim_token, plan, prior_error=error, now=now)
                if plan.supports_authoritative_readback
                else self._repository.mark_indeterminate(
                    operation.id,
                    claim_token,
                    error={**error, "action": plan.action.value, "authoritative_readback": False},
                    now=now,
                )
            )
            return SubscriptionOperationExecution(recovered, claimed=True)

        remote_confirmed = self._repository.mark_remote_confirmed(
            operation.id,
            claim_token,
            receipt={
                "operation": write_result.operation,
                "status_code": write_result.status_code,
                "idempotent": write_result.idempotent,
            },
            now=now,
        )
        reconciled = (
            self._readback(remote_confirmed, claim_token, plan, now=now)
            if plan.supports_authoritative_readback
            else self._repository.mark_confirmed(
                operation.id,
                claim_token,
                result={"action": plan.action.value, "provider_response_confirmed": True},
                readback={"disposition": "provider_accepted", "authoritative_readback": False},
                now=now,
            )
        )
        return SubscriptionOperationExecution(reconciled, claimed=True)

    def _write(self, plan: _WritePlan) -> MoviePilotWriteResult:
        if plan.action is SubscriptionProviderAction.CREATE:
            return self._gateway.create_subscription(plan.payload)
        if plan.action is SubscriptionProviderAction.UPDATE:
            return self._gateway.update_subscription(plan.payload)
        if plan.action is SubscriptionProviderAction.UPDATE_STATUS:
            status = str(plan.payload["status"])
            total_episodes = _optional_nonnegative_int(plan.payload.get("total_episodes"), "total_episodes")
            return self._gateway.update_subscription_status(
                _required_plan_tmdb_id(plan),
                status,
                season=plan.season,
                total_episodes=total_episodes,
            )
        if plan.action is SubscriptionProviderAction.DELETE:
            return self._gateway.delete_subscription(_required_plan_tmdb_id(plan), season=plan.season)
        if plan.action is SubscriptionProviderAction.DELETE_BY_ID:
            return self._gateway.delete_subscription_by_id(_required_subscription_id(plan))
        if plan.action is SubscriptionProviderAction.SEARCH:
            return self._gateway.search_subscription(_required_subscription_id(plan))
        if plan.action is SubscriptionProviderAction.DELETE_DOWNLOADS:
            return self._gateway.delete_download_tasks(_string_sequence(plan.payload.get("hashes"), "hashes"))
        if plan.action is SubscriptionProviderAction.DELETE_TRANSFER_HISTORY:
            return self._gateway.delete_transfer_history(
                _mapping(plan.payload.get("record"), "record"),
            )
        if plan.action is SubscriptionProviderAction.REDO_TRANSFER_HISTORY:
            return self._gateway.redo_transfer_history(
                _positive_int_sequence(plan.payload.get("history_ids"), "history_ids")
            )
        raise AssertionError(f"unsupported subscription provider action: {plan.action.value}")

    def _preflight(
        self,
        operation: SubscriptionOperation,
        claim_token: UUID,
        plan: _WritePlan,
        *,
        now: datetime | None,
    ) -> SubscriptionOperation | None:
        try:
            readback = self._gateway.query_subscription(_required_plan_tmdb_id(plan), season=plan.season)
        except MoviePilotProviderError as exc:
            error = {"type": "preflight_readback_failed", **_provider_error(exc)}
            if operation.attempts >= self._max_attempts:
                return self._repository.mark_rejected(
                    operation.id,
                    claim_token,
                    error={**error, "attempts": operation.attempts},
                    now=now,
                )
            return self._repository.mark_retryable(
                operation.id,
                claim_token,
                error=error,
                retry_after_seconds=self._readback_delay_seconds,
                now=now,
            )
        except Exception as exc:
            error = {"type": "preflight_readback_failed", "error_class": type(exc).__name__}
            if operation.attempts >= self._max_attempts:
                return self._repository.mark_rejected(
                    operation.id,
                    claim_token,
                    error={**error, "attempts": operation.attempts},
                    now=now,
                )
            return self._repository.mark_retryable(
                operation.id,
                claim_token,
                error=error,
                retry_after_seconds=self._readback_delay_seconds,
                now=now,
            )
        if not _desired_state_observed(plan, readback):
            return None
        return self._repository.mark_confirmed_without_write(
            operation.id,
            claim_token,
            result={"action": plan.action.value, "provider_write_invoked": False},
            readback=_readback_projection(readback),
            now=now,
        )

    def _recover_without_readback(
        self,
        operation: SubscriptionOperation,
        claim_token: UUID,
        plan: _WritePlan,
        *,
        now: datetime | None,
    ) -> SubscriptionOperation:
        if operation.provider_receipt is not None:
            return self._repository.mark_confirmed(
                operation.id,
                claim_token,
                result={"action": plan.action.value, "provider_response_confirmed": True},
                readback={"disposition": "provider_accepted", "authoritative_readback": False},
                now=now,
            )
        return self._repository.mark_indeterminate(
            operation.id,
            claim_token,
            error={
                "type": "provider_outcome_unknown_without_readback",
                "action": plan.action.value,
                "authoritative_readback": False,
            },
            now=now,
        )

    def _readback(
        self,
        operation: SubscriptionOperation,
        claim_token: UUID,
        plan: _WritePlan,
        *,
        prior_error: Mapping[str, Any] | None = None,
        now: datetime | None,
    ) -> SubscriptionOperation:
        try:
            readback = self._gateway.query_subscription(_required_plan_tmdb_id(plan), season=plan.season)
        except MoviePilotProviderError as exc:
            return self._defer_or_terminalize(
                operation,
                claim_token,
                plan,
                error={"type": "readback_failed", **_provider_error(exc)},
                readback=None,
                now=now,
            )
        except Exception as exc:
            return self._defer_or_terminalize(
                operation,
                claim_token,
                plan,
                error={"type": "readback_failed", "error_class": type(exc).__name__},
                readback=None,
                now=now,
            )

        projection = _readback_projection(readback)
        interrupted = self._repository.is_cancel_requested(operation.id, claim_token, now=now)
        current = self._repository.read(operation.id) if interrupted else operation
        if current is None or (interrupted and not _claim_is_active(current, claim_token, now=now)):
            return current or operation
        if _desired_state_observed(plan, readback):
            return self._repository.mark_confirmed(
                operation.id,
                claim_token,
                result={"action": plan.action.value, "readback_confirmed": True},
                readback=projection,
                now=now,
            )
        return self._defer_or_terminalize(
            current,
            claim_token,
            plan,
            error=dict(prior_error or {"type": "desired_state_not_observed"}),
            readback=projection,
            now=now,
        )

    def _defer_or_terminalize(
        self,
        operation: SubscriptionOperation,
        claim_token: UUID,
        plan: _WritePlan,
        *,
        error: Mapping[str, Any],
        readback: Mapping[str, Any] | None,
        now: datetime | None,
    ) -> SubscriptionOperation:
        if operation.attempts >= self._max_attempts:
            return self._repository.mark_indeterminate(
                operation.id,
                claim_token,
                error={**dict(error), "action": plan.action.value, "attempts": operation.attempts},
                readback=readback,
                now=now,
            )
        return self._repository.mark_awaiting_readback(
            operation.id,
            claim_token,
            error={**dict(error), "action": plan.action.value, "attempts": operation.attempts},
            retry_after_seconds=self._readback_delay_seconds,
            now=now,
        )


def _write_plan(operation: SubscriptionOperation) -> _WritePlan:
    request = operation.request
    try:
        action = SubscriptionProviderAction(str(request.get("action") or operation.kind.value))
    except ValueError as exc:
        raise ValueError("unsupported subscription provider action") from exc
    tmdb_id = str(request.get("tmdb_id") or request.get("tmdbid") or "").strip()
    subscription_id = str(request.get("subscription_id") or "").strip()
    if (
        action
        in {
            SubscriptionProviderAction.CREATE,
            SubscriptionProviderAction.UPDATE,
            SubscriptionProviderAction.UPDATE_STATUS,
            SubscriptionProviderAction.DELETE,
        }
        and not tmdb_id
    ):
        raise ValueError("subscription provider operation requires tmdb_id")
    if action in {SubscriptionProviderAction.DELETE_BY_ID, SubscriptionProviderAction.SEARCH} and not subscription_id:
        raise ValueError("subscription provider operation requires subscription_id")
    season = _optional_nonnegative_int(request.get("season"), "season")
    payload = request.get("payload", {})
    expected = request.get("expected", {})
    if not isinstance(payload, Mapping) or not isinstance(expected, Mapping):
        raise TypeError("subscription provider payload and expected state must be objects")
    normalized_payload = dict(payload)
    if action in {SubscriptionProviderAction.CREATE, SubscriptionProviderAction.UPDATE} and not normalized_payload:
        raise ValueError("subscription provider create/update operation requires a payload")
    if action is SubscriptionProviderAction.UPDATE_STATUS:
        status = str(normalized_payload.get("status") or "").strip().upper()
        if status not in {"P", "R", "S"}:
            raise ValueError("subscription provider status operation requires P, R, or S")
        normalized_payload["status"] = status
    if action is SubscriptionProviderAction.DELETE_DOWNLOADS:
        _string_sequence(normalized_payload.get("hashes"), "hashes")
    if action is SubscriptionProviderAction.DELETE_TRANSFER_HISTORY:
        _mapping(normalized_payload.get("record"), "record")
    if action is SubscriptionProviderAction.REDO_TRANSFER_HISTORY:
        _positive_int_sequence(normalized_payload.get("history_ids"), "history_ids")
    return _WritePlan(action, tmdb_id or None, season, subscription_id or None, normalized_payload, dict(expected))


def _desired_state_observed(plan: _WritePlan, readback: MoviePilotQueryResult) -> bool:
    if plan.action is SubscriptionProviderAction.DELETE:
        return readback.disposition is MoviePilotQueryDisposition.EMPTY
    if not readback.found or readback.subscription is None:
        return False
    for key, expected in plan.expected.items():
        if readback.subscription.get(key) != expected:
            return False
    return True


def _required_plan_tmdb_id(plan: _WritePlan) -> str:
    if plan.tmdb_id is None:
        raise ValueError(f"subscription provider action {plan.action.value!r} requires tmdb_id")
    return plan.tmdb_id


def _required_subscription_id(plan: _WritePlan) -> str:
    if plan.subscription_id is None:
        raise ValueError(f"subscription provider action {plan.action.value!r} requires subscription_id")
    return plan.subscription_id


def _string_sequence(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise TypeError(f"subscription provider {field} must be an array")
    normalized = tuple(dict.fromkeys(str(item or "").strip() for item in value if str(item or "").strip()))
    if not normalized:
        raise ValueError(f"subscription provider {field} must not be empty")
    return normalized


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise TypeError(f"subscription provider {field} must be a non-empty object")
    return dict(value)


def _positive_int_sequence(value: object, field: str) -> tuple[int, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise TypeError(f"subscription provider {field} must be an array")
    normalized: list[int] = []
    for item in value:
        if isinstance(item, bool):
            raise ValueError(f"subscription provider {field} values must be positive integers")
        try:
            number = int(item)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"subscription provider {field} values must be positive integers") from exc
        if number < 1:
            raise ValueError(f"subscription provider {field} values must be positive integers")
        if number not in normalized:
            normalized.append(number)
    if not normalized:
        raise ValueError(f"subscription provider {field} must not be empty")
    return tuple(normalized)


def _readback_projection(readback: MoviePilotQueryResult) -> dict[str, Any]:
    projection: dict[str, Any] = {"disposition": readback.disposition.value}
    if readback.subscription is None:
        return projection
    allowed = {
        "id",
        "tmdbid",
        "tmdb_id",
        "season",
        "state",
        "status",
        "total_episode",
        "type",
        "name",
        "title",
    }
    projection["subscription"] = {key: value for key, value in readback.subscription.items() if str(key) in allowed}
    return projection


def _provider_error(error: MoviePilotProviderError) -> dict[str, Any]:
    return {
        "type": "moviepilot_provider_error",
        "kind": error.kind.value,
        "operation": error.operation,
        "status_code": error.status_code,
        "outcome": error.outcome.value if error.outcome is not None else None,
    }


def _optional_nonnegative_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _claim_is_active(
    operation: SubscriptionOperation,
    claim_token: UUID,
    *,
    now: datetime | None,
) -> bool:
    timestamp = now or datetime.now(UTC)
    return (
        operation.claim_token == claim_token
        and operation.lease_expires_at is not None
        and operation.lease_expires_at > timestamp
    )


__all__ = [
    "MoviePilotOperationGateway",
    "SubscriptionOperationExecution",
    "SubscriptionOperationExecutor",
    "SubscriptionOperationRepositoryPort",
    "SubscriptionProviderAction",
]
