from __future__ import annotations

import hashlib
import json
import socket
import uuid
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from etk.platform.jobs.json_payload import normalize_json_object
from etk.platform.observability.redaction import redact_sensitive

from .directory_sync import P115TaskProgress
from .organize_plan import P115OrganizePlan, P115OrganizePlanRepository, P115PlanAction

P115_ORGANIZE_EXECUTION_SCHEMA_VERSION = 1
P115_ORGANIZE_EFFECT_LEASE_SECONDS = 900
P115_ORGANIZE_TASK_KEY = "scan-organize-115"
_ALLOWED_ACTION_KINDS = frozenset({"ENSURE_DIRECTORY", "MOVE", "RENAME"})


class P115ExecutionStatus(StrEnum):
    PREPARED = "prepared"
    RUNNING = "running"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    MANUAL_REVIEW = "manual_review"

    @property
    def terminal(self) -> bool:
        return self in {
            P115ExecutionStatus.CONFIRMED,
            P115ExecutionStatus.REJECTED,
            P115ExecutionStatus.INDETERMINATE,
            P115ExecutionStatus.MANUAL_REVIEW,
        }


class P115ExecutionActionStatus(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    MANUAL_REVIEW = "manual_review"


class P115ExecutionEffectStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"

    @property
    def terminal(self) -> bool:
        return self in {
            P115ExecutionEffectStatus.CONFIRMED,
            P115ExecutionEffectStatus.REJECTED,
            P115ExecutionEffectStatus.INDETERMINATE,
        }


class P115ExecutionError(RuntimeError):
    pass


class P115ExecutionConflict(P115ExecutionError):
    pass


class P115ExecutionBusy(P115ExecutionError):
    pass


class P115ExecutionNotFound(P115ExecutionError):
    pass


class P115ExecutionPreflightError(P115ExecutionError):
    pass


class P115ExecutionStateError(P115ExecutionError):
    pass


class P115ProviderNoEffectError(P115ExecutionError):
    """The transport proved that no provider request left the process."""


@dataclass(frozen=True, slots=True)
class P115RemoteEntry:
    id: str
    parent_id: str
    name: str
    entry_kind: str
    sha1: str = ""
    pick_code: str = ""
    size: int = 0
    updated_at: str = ""
    raw_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "parent_id": self.parent_id,
            "name": self.name,
            "entry_kind": self.entry_kind,
            "sha1": self.sha1,
            "pick_code": self.pick_code,
            "size": self.size,
            "updated_at": self.updated_at,
            "raw_hash": self.raw_hash,
        }


class P115OrganizeMutationClient(Protocol):
    @property
    def backend(self) -> str: ...

    def list_children(self, parent_id: str) -> Sequence[P115RemoteEntry]: ...

    def ensure_directory(self, parent_id: str, name: str) -> Mapping[str, Any]: ...

    def move(self, file_id: str, parent_id: str) -> Mapping[str, Any]: ...

    def rename(self, file_id: str, name: str) -> Mapping[str, Any]: ...


class P115OrganizeMutationGateway(Protocol):
    def open(
        self,
        *,
        expected_account_id: str,
        expected_backend: str | None = None,
    ) -> P115OrganizeMutationClient | None: ...


@dataclass(frozen=True, slots=True)
class P115ExecutionEffect:
    effect_id: UUID
    effect_key: str
    request_hash: str
    request: dict[str, Any]
    kind: str
    status: P115ExecutionEffectStatus
    attempts: int
    provider_backend: str
    available_at: datetime
    last_attempt_at: datetime | None = None
    sponsor_execution_id: UUID | None = None
    sponsor_action_index: int | None = None
    sponsor_account_id: str | None = None
    claimed_by: str | None = None
    claim_token: UUID | None = None
    claimed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    provider_call_started_at: datetime | None = None
    receipt: dict[str, Any] | None = None
    receipt_persisted_at: datetime | None = None
    readback: dict[str, Any] | None = None
    readback_at: datetime | None = None
    resolved_object_id: str | None = None
    resolved_parent_cid: str | None = None
    completion_mode: str | None = None
    resolution_status: str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    finished_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "effect_id": str(self.effect_id),
            "effect_key": self.effect_key,
            "request_hash": self.request_hash,
            "request": deepcopy(self.request),
            "kind": self.kind,
            "status": self.status.value,
            "attempts": self.attempts,
            "provider_backend": self.provider_backend,
            "available_at": _iso(self.available_at),
            "last_attempt_at": _iso(self.last_attempt_at),
            "sponsor": {
                "execution_id": str(self.sponsor_execution_id) if self.sponsor_execution_id else None,
                "action_index": self.sponsor_action_index,
                "account_id": self.sponsor_account_id,
            },
            "claim": {
                "claimed_by": self.claimed_by,
                "claim_token": str(self.claim_token) if self.claim_token else None,
                "claimed_at": _iso(self.claimed_at),
                "lease_expires_at": _iso(self.lease_expires_at),
            },
            "provider_call_started_at": _iso(self.provider_call_started_at),
            "receipt": deepcopy(self.receipt),
            "receipt_persisted_at": _iso(self.receipt_persisted_at),
            "readback": deepcopy(self.readback),
            "readback_at": _iso(self.readback_at),
            "resolved_object_id": self.resolved_object_id,
            "resolved_parent_cid": self.resolved_parent_cid,
            "completion_mode": self.completion_mode,
            "resolution_status": self.resolution_status,
            "result": deepcopy(self.result),
            "error": deepcopy(self.error),
            "finished_at": _iso(self.finished_at),
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
        }


@dataclass(frozen=True, slots=True)
class P115ExecutionAction:
    plan_id: str
    action_index: int
    action_id: str
    kind: str
    effect_key: str
    binding: dict[str, Any]
    disposition: str
    status: P115ExecutionActionStatus
    verification_status: str
    verification: dict[str, Any]
    effect: P115ExecutionEffect

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "action_index": self.action_index,
            "action_id": self.action_id,
            "kind": self.kind,
            "effect_key": self.effect_key,
            "binding": deepcopy(self.binding),
            "disposition": self.disposition,
            "status": self.status.value,
            "verification_status": self.verification_status,
            "verification": deepcopy(self.verification),
            "effect": self.effect.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class P115OrganizeExecution:
    execution_id: UUID
    plan_id: str
    approved_intent_id: str
    request_hash: str
    status: P115ExecutionStatus
    account_id: str
    provider_backend: str
    preflight_hash: str
    preflight: dict[str, Any]
    actions: tuple[P115ExecutionAction, ...]
    job_id: UUID
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    schema_version: int = P115_ORGANIZE_EXECUTION_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": str(self.execution_id),
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "approved_execution_intent_id": self.approved_intent_id,
            "request_hash": self.request_hash,
            "status": self.status.value,
            "account_id": self.account_id,
            "provider_backend": self.provider_backend,
            "preflight_hash": self.preflight_hash,
            "preflight": deepcopy(self.preflight),
            "actions": [action.to_dict() for action in self.actions],
            "job_id": str(self.job_id),
            "result": deepcopy(self.result),
            "error": deepcopy(self.error),
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
        }


@dataclass(frozen=True, slots=True)
class P115PreparedAction:
    action_id: str
    state: str
    binding: dict[str, Any]
    readback: dict[str, Any]


@dataclass(frozen=True, slots=True)
class P115ExecutionJob:
    job_id: UUID
    attempt_id: UUID
    outbox_event_id: UUID
    task_key: str
    payload: dict[str, Any]
    requested_by_user_id: str | None
    trace_id: str | None


@dataclass(frozen=True, slots=True)
class P115JobAttemptOwner:
    job_id: UUID
    attempt_id: UUID
    worker_id: str


@dataclass(frozen=True, slots=True)
class P115ExecutionReservation:
    execution: P115OrganizeExecution
    created: bool


@dataclass(frozen=True, slots=True)
class P115ExecutionStart:
    execution: P115OrganizeExecution
    p4_terminalized: bool = False


@dataclass(frozen=True, slots=True)
class P115ExecutionRunResult:
    status: P115ExecutionStatus
    message: str
    details: dict[str, Any]
    p4_terminalized: bool


@dataclass(frozen=True, slots=True)
class P115Reconciliation:
    reconciliation_id: UUID
    approved_intent_id: str
    request_hash: str
    execution_id: UUID
    action_index: int
    effect_key: str
    status: str
    outcome: str | None
    actor: str | None
    evidence: dict[str, Any] | None
    created_at: datetime
    finished_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "reconciliation_id": str(self.reconciliation_id),
            "approved_reconciliation_intent_id": self.approved_intent_id,
            "request_hash": self.request_hash,
            "execution_id": str(self.execution_id),
            "action_index": self.action_index,
            "effect_key": self.effect_key,
            "status": self.status,
            "outcome": self.outcome,
            "actor": self.actor,
            "evidence": deepcopy(self.evidence),
            "created_at": _iso(self.created_at),
            "finished_at": _iso(self.finished_at),
        }


@dataclass(frozen=True, slots=True)
class P115ReconciliationReservation:
    reconciliation: P115Reconciliation
    execution: P115OrganizeExecution
    action: P115ExecutionAction
    created: bool


@dataclass(frozen=True, slots=True)
class P115ReconciliationResult:
    reconciliation: P115Reconciliation
    execution: P115OrganizeExecution

    def to_dict(self) -> dict[str, Any]:
        return {
            "reconciliation": self.reconciliation.to_dict(),
            "execution": self.execution.to_dict(),
            "external_mutations_executed": 0,
        }


@dataclass(frozen=True, slots=True)
class _ActionInspection:
    state: str
    binding: dict[str, Any]
    readback: dict[str, Any]


class P115OrganizeExecutionRepository(Protocol):
    def find_by_intent(self, approved_intent_id: str) -> P115OrganizeExecution | None: ...

    def reserve(
        self,
        plan: P115OrganizePlan,
        *,
        execution_id: UUID,
        approved_intent_id: str,
        request_hash: str,
        provider_backend: str,
        preflight: Mapping[str, Any],
        prepared_actions: Sequence[P115PreparedAction],
        job: P115ExecutionJob,
    ) -> P115ExecutionReservation: ...

    def get(self, execution_id: UUID) -> P115OrganizeExecution | None: ...

    def recover_orphaned(self, execution_id: UUID) -> P115OrganizeExecution | None: ...

    def start(self, execution_id: UUID, owner: P115JobAttemptOwner) -> P115ExecutionStart: ...

    def observe_confirmed(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def stop_for_postcondition_drift(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        binding: Mapping[str, Any],
        verification: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def claim_effect(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        lease_seconds: int,
    ) -> P115ExecutionAction | None: ...

    def confirm_without_request(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def mark_requesting(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        binding: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def persist_receipt(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        receipt: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def complete_effect(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        status: P115ExecutionEffectStatus,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
        result: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
    ) -> P115OrganizeExecution: ...

    def stop_before_request(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        error: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def finish(
        self,
        execution_id: UUID,
        owner: P115JobAttemptOwner,
        *,
        records: Sequence[Mapping[str, Any]],
        result: Mapping[str, Any],
    ) -> P115OrganizeExecution: ...

    def begin_reconciliation(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        request_hash: str,
        *,
        actor: str | None,
    ) -> P115ReconciliationReservation: ...

    def complete_reconciliation(
        self,
        reconciliation_id: UUID,
        *,
        outcome: str,
        evidence: Mapping[str, Any],
        records: Sequence[Mapping[str, Any]],
    ) -> P115ReconciliationResult: ...


class P115OrganizeExecutionService:
    def __init__(
        self,
        *,
        plans: P115OrganizePlanRepository,
        repository: P115OrganizeExecutionRepository,
        mutations: P115OrganizeMutationGateway,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._plans = plans
        self._repository = repository
        self._mutations = mutations
        self._clock = clock or (lambda: datetime.now(UTC))

    def prepare(
        self,
        plan_id: str,
        approved_intent_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> P115ExecutionReservation:
        normalized_plan_id = _required_text(plan_id, "plan_id", maximum=128)
        normalized_intent = _required_text(
            approved_intent_id,
            "approved_execution_intent_id",
            maximum=255,
        )
        request_hash = _hash_json(
            {
                "plan_id": normalized_plan_id,
                "approved_execution_intent_id": normalized_intent,
            }
        )
        existing = self._repository.find_by_intent(normalized_intent)
        if existing is not None:
            if existing.plan_id != normalized_plan_id or existing.request_hash != request_hash:
                raise P115ExecutionConflict("approved_execution_intent_id 已用于不同的执行请求")
            return P115ExecutionReservation(existing, False)

        plan = self._require_ready_plan(normalized_plan_id)
        self._assert_plan_current(plan)
        client = self._open_client(plan.account_id)
        checked_at = self._clock().astimezone(UTC)
        preflight, prepared = _preflight_plan(plan, client, checked_at=checked_at)
        self._assert_plan_current(plan)

        execution_id = execution_id_for(plan.plan_id, normalized_intent)
        job = execution_job_for(
            execution_id,
            requested_by_user_id=_optional_text(requested_by_user_id),
            trace_id=_optional_text(trace_id),
        )
        return self._repository.reserve(
            plan,
            execution_id=execution_id,
            approved_intent_id=normalized_intent,
            request_hash=request_hash,
            provider_backend=client.backend,
            preflight=preflight,
            prepared_actions=prepared,
            job=job,
        )

    def get(self, execution_id: UUID) -> P115OrganizeExecution | None:
        return self._repository.recover_orphaned(execution_id)

    def execute(
        self,
        execution_id: UUID,
        progress: P115TaskProgress,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str | None = None,
    ) -> P115ExecutionRunResult:
        owner = P115JobAttemptOwner(
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=_required_text(
                worker_id or f"p115-organize:{socket.gethostname()}",
                "worker_id",
                maximum=255,
            ),
        )
        started = self._repository.start(execution_id, owner)
        execution = started.execution
        if started.p4_terminalized:
            return _run_result(execution, _terminal_message(execution), p4_terminalized=True)

        plan = self._require_ready_plan(execution.plan_id, allow_expired_after_reservation=True)
        client = self._open_client(plan.account_id, expected_backend=execution.provider_backend)
        resolved_directories = _resolved_directories(execution)
        total = max(len(execution.actions), 1)
        progress.update(0, f"=== 开始执行 115 整理计划 {plan.plan_id} ===")

        for plan_action in plan.actions:
            progress.checkpoint()
            execution_action = _execution_action(execution, plan_action.action_id)
            try:
                self._assert_plan_current(plan)
                inspection = _inspect_action(plan_action, client, resolved_directories)
            except Exception as exc:
                stopped = self._repository.stop_before_request(
                    execution_id,
                    plan_action.action_id,
                    owner,
                    error=_safe_error("preflight_drift", exc),
                )
                return _run_result(
                    stopped,
                    "整理执行在外部请求前被前置条件阻断",
                    p4_terminalized=True,
                )

            if execution_action.status is P115ExecutionActionStatus.CONFIRMED:
                if inspection.state != "confirmed":
                    drifted = self._repository.stop_for_postcondition_drift(
                        execution_id,
                        plan_action.action_id,
                        owner,
                        binding=inspection.binding,
                        verification=inspection.readback,
                        error={
                            "type": "confirmed_postcondition_drift",
                            "message": "历史已确认动作的远端后置条件已漂移，禁止自动重发",
                        },
                    )
                    return _run_result(
                        drifted,
                        "历史整理结果已漂移，必须人工复核",
                        p4_terminalized=True,
                    )
                execution = self._repository.observe_confirmed(
                    execution_id,
                    plan_action.action_id,
                    owner,
                    binding=inspection.binding,
                    readback=inspection.readback,
                )
                _remember_directory(
                    plan_action,
                    _execution_action(execution, plan_action.action_id),
                    resolved_directories,
                )
                continue

            if execution_action.status is not P115ExecutionActionStatus.PENDING:
                raise P115ExecutionStateError(
                    f"动作 {plan_action.action_id} 不是可执行状态: {execution_action.status.value}"
                )

            claimed_action = self._repository.claim_effect(
                execution_id,
                plan_action.action_id,
                owner,
                lease_seconds=P115_ORGANIZE_EFFECT_LEASE_SECONDS,
            )
            if claimed_action is None:
                current = self._repository.recover_orphaned(execution_id)
                if current is None:
                    raise P115ExecutionNotFound("整理执行不存在")
                return _run_result(current, "整理动作正在由另一个执行持有", p4_terminalized=False)
            effect_claim_token = claimed_action.effect.claim_token
            if effect_claim_token is None:
                raise P115ExecutionStateError("整理动作 claim 缺少 fencing token")

            if inspection.state == "confirmed":
                execution = self._repository.confirm_without_request(
                    execution_id,
                    plan_action.action_id,
                    owner,
                    effect_claim_token,
                    binding=inspection.binding,
                    readback=inspection.readback,
                )
                _remember_directory(
                    plan_action,
                    _execution_action(execution, plan_action.action_id),
                    resolved_directories,
                )
                continue

            self._repository.mark_requesting(
                execution_id,
                plan_action.action_id,
                owner,
                effect_claim_token,
                binding=inspection.binding,
            )
            _record_progress_effect(progress, "record_started_effect", plan_action.effect_key)
            provider_response: Mapping[str, Any] | None = None
            provider_error: Exception | None = None
            no_effect_proved = False
            try:
                provider_response = _mutate(plan_action, client, inspection.binding)
            except P115ProviderNoEffectError as exc:
                provider_error = exc
                no_effect_proved = True
            except Exception as exc:
                provider_error = exc

            receipt = _provider_receipt(provider_response, provider_error, no_effect_proved=no_effect_proved)
            self._repository.persist_receipt(
                execution_id,
                plan_action.action_id,
                owner,
                effect_claim_token,
                receipt=receipt,
            )
            readback = _readback_action(plan_action, client, inspection.binding)
            effect_status, result, error = _classify_provider_outcome(
                provider_response,
                provider_error,
                no_effect_proved=no_effect_proved,
                readback=readback,
            )
            execution = self._repository.complete_effect(
                execution_id,
                plan_action.action_id,
                owner,
                effect_claim_token,
                status=effect_status,
                binding=readback.binding,
                readback=readback.readback,
                result=result,
                error=error,
            )
            if effect_status is not P115ExecutionEffectStatus.CONFIRMED:
                _record_progress_effect(progress, "record_abandoned_effect", plan_action.effect_key)
                if effect_status is P115ExecutionEffectStatus.RETRYABLE:
                    message = "115 明确未接收整理请求，可在重新生成 Plan 后重试"
                elif effect_status is P115ExecutionEffectStatus.REJECTED:
                    message = "115 提供方拒绝整理请求"
                else:
                    message = "115 整理结果不确定，禁止自动重放"
                return _run_result(execution, message, p4_terminalized=True)

            _record_progress_effect(progress, "record_committed_effect", plan_action.effect_key)
            execution_action = _execution_action(execution, plan_action.action_id)
            _remember_directory(plan_action, execution_action, resolved_directories)
            completed = plan_action.action_index + 1
            progress.update(min(int((completed / total) * 95), 95), f"已确认整理动作 {completed}/{total}")

        records = records_from_plan(plan)
        execution = self._repository.finish(
            execution_id,
            owner,
            records=records,
            result={
                "confirmed_actions": len(plan.actions),
                "record_count": len(records),
                "external_result": "authoritative_readback_confirmed",
            },
        )
        progress.update(100, f"115 整理计划执行完成：{len(plan.actions)} 个动作已回读确认")
        return _run_result(execution, "115 整理计划执行完成", p4_terminalized=True)

    def reconcile(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        *,
        actor: str | None = None,
    ) -> P115ReconciliationResult:
        normalized_intent = _required_text(
            approved_intent_id,
            "approved_reconciliation_intent_id",
            maximum=255,
        )
        request_hash = _hash_json(
            {
                "execution_id": str(execution_id),
                "approved_reconciliation_intent_id": normalized_intent,
            }
        )
        reservation = self._repository.begin_reconciliation(
            execution_id,
            normalized_intent,
            request_hash,
            actor=_optional_text(actor),
        )
        if reservation.reconciliation.status == "completed":
            return P115ReconciliationResult(reservation.reconciliation, reservation.execution)

        execution = reservation.execution
        plan = self._require_ready_plan(execution.plan_id, allow_expired_after_reservation=True)
        plan_action = next(
            (value for value in plan.actions if value.action_id == reservation.action.action_id),
            None,
        )
        if plan_action is None:
            raise P115ExecutionStateError("对账动作不属于不可变整理计划")
        client = self._open_client(plan.account_id, expected_backend=execution.provider_backend)
        readback = _readback_action(plan_action, client, reservation.action.binding)
        outcome = {
            "confirmed": "applied",
            "not_applied": "absent",
            "conflict": "conflict",
            "unknown": "unknown",
        }.get(readback.state, "unknown")
        evidence = {
            "provider_backend": client.backend,
            "readback": readback.readback,
            "external_mutations_executed": 0,
        }
        return self._repository.complete_reconciliation(
            reservation.reconciliation.reconciliation_id,
            outcome=outcome,
            evidence=evidence,
            records=records_from_plan(plan),
        )

    def _require_ready_plan(
        self,
        plan_id: str,
        *,
        allow_expired_after_reservation: bool = False,
    ) -> P115OrganizePlan:
        plan = self._plans.get(plan_id)
        if plan is None:
            raise P115ExecutionNotFound("整理计划不存在")
        if plan.status != "ready" or plan.errors or not plan.actions:
            raise P115ExecutionPreflightError("只有 ready 且包含动作的整理计划可以执行")
        if not allow_expired_after_reservation and plan.expires_at <= self._clock().astimezone(UTC):
            raise P115ExecutionPreflightError("整理计划已过期，请重新生成 Dry Run")
        _validate_action_graph(plan)
        return plan

    def _assert_plan_current(self, plan: P115OrganizePlan) -> None:
        self._plans.assert_current(plan)

    def _open_client(
        self,
        account_id: str,
        *,
        expected_backend: str | None = None,
    ) -> P115OrganizeMutationClient:
        client = self._mutations.open(
            expected_account_id=account_id,
            expected_backend=expected_backend,
        )
        if client is None:
            raise P115ExecutionPreflightError("无法初始化账号绑定的 115 整理执行客户端")
        backend = str(client.backend or "").strip().lower()
        if backend not in {"openapi", "cookie"}:
            raise P115ExecutionPreflightError("115 整理客户端缺少固定后端身份")
        if expected_backend is not None and backend != expected_backend:
            raise P115ExecutionPreflightError("115 整理执行后端在任务期间发生变化")
        return client


def _preflight_plan(
    plan: P115OrganizePlan,
    client: P115OrganizeMutationClient,
    *,
    checked_at: datetime,
) -> tuple[dict[str, Any], tuple[P115PreparedAction, ...]]:
    _validate_action_graph(plan)
    resolved_directories: dict[str, str | None] = {}
    listings: dict[str, tuple[P115RemoteEntry, ...]] = {}
    prepared: list[P115PreparedAction] = []

    def children(parent_id: str) -> tuple[P115RemoteEntry, ...]:
        if parent_id not in listings:
            listings[parent_id] = tuple(client.list_children(parent_id))
        return listings[parent_id]

    for action in plan.actions:
        parent_id = _resolved_parent(action, resolved_directories)
        if action.kind == "ENSURE_DIRECTORY":
            existing: P115RemoteEntry | None = None
            if parent_id is not None:
                matches = _name_matches(children(parent_id), _target_name(action))
                if len(matches) > 1:
                    raise P115ExecutionPreflightError("目标父目录存在多个同名条目")
                if matches:
                    existing = matches[0]
                    if existing.entry_kind != "directory":
                        raise P115ExecutionPreflightError("计划目录路径已被同名文件占用")
            binding = _binding(action, parent_id, existing)
            state = "confirmed" if existing is not None else "needs_request"
            readback = _readback("confirmed", entry=existing) if existing else _readback("not_applied")
            resolved_directories[action.action_id] = existing.id if existing is not None else None
        else:
            target_name = _target_name(action)
            target_entries = children(parent_id) if parent_id is not None else ()
            target_matches = _name_matches(target_entries, target_name)
            source_id = _source_id(action)
            exact_target = next((entry for entry in target_matches if entry.id == source_id), None)
            foreign = tuple(entry for entry in target_matches if entry.id != source_id)
            if foreign:
                raise P115ExecutionPreflightError("目标文件名已被其他对象占用")
            if exact_target is not None:
                _assert_stable_file_identity(exact_target, action)
                binding = _binding(action, parent_id, exact_target)
                state = "confirmed"
                readback = _readback("confirmed", entry=exact_target)
            else:
                source = _source_for_preflight(action, children)
                binding = _binding(action, parent_id, source)
                state = "needs_request"
                readback = _readback("not_applied", entry=source)
        prepared.append(P115PreparedAction(action.action_id, state, binding, readback))

    listing_evidence = {
        parent_id: {
            "entry_count": len(entries),
            "snapshot_hash": _hash_json([entry.to_dict() for entry in entries]),
        }
        for parent_id, entries in sorted(listings.items())
    }
    preflight = {
        "checked_at": checked_at.astimezone(UTC).isoformat(),
        "account_id": plan.account_id,
        "provider_backend": client.backend,
        "plan_input_fingerprint": plan.input_fingerprint,
        "action_count": len(plan.actions),
        "external_mutations_executed": 0,
        "listing_evidence": listing_evidence,
    }
    return preflight, tuple(prepared)


def _inspect_action(
    action: P115PlanAction,
    client: P115OrganizeMutationClient,
    resolved_directories: Mapping[str, str | None],
) -> _ActionInspection:
    parent_id = _resolved_parent(action, resolved_directories)
    if parent_id is None:
        raise P115ExecutionPreflightError("依赖目录尚未取得回读确认的 CID")
    target_children = tuple(client.list_children(parent_id))
    target_name = _target_name(action)
    target_matches = _name_matches(target_children, target_name)
    if action.kind == "ENSURE_DIRECTORY":
        if len(target_matches) > 1:
            raise P115ExecutionPreflightError("目标父目录存在多个同名条目")
        if target_matches:
            entry = target_matches[0]
            if entry.entry_kind != "directory":
                raise P115ExecutionPreflightError("计划目录路径已被同名文件占用")
            binding = _binding(action, parent_id, entry)
            return _ActionInspection("confirmed", binding, _readback("confirmed", entry=entry))
        return _ActionInspection("needs_request", _binding(action, parent_id), _readback("not_applied"))

    source_id = _source_id(action)
    exact_target = next((entry for entry in target_matches if entry.id == source_id), None)
    foreign = tuple(entry for entry in target_matches if entry.id != source_id)
    if foreign:
        raise P115ExecutionPreflightError("目标文件名已被其他对象占用")
    if exact_target is not None:
        _assert_stable_file_identity(exact_target, action)
        if action.kind == "MOVE":
            old_parent = str(action.preconditions.get("source_parent_id") or "")
            old_children = tuple(client.list_children(old_parent)) if old_parent and old_parent != parent_id else ()
            if any(entry.id == source_id for entry in old_children):
                raise P115ExecutionPreflightError("移动后源目录仍可见同一文件，远端状态尚未收敛")
        binding = _binding(action, parent_id, exact_target)
        return _ActionInspection("confirmed", binding, _readback("confirmed", entry=exact_target))

    source_parent = str(action.preconditions.get("source_parent_id") or "") if action.kind == "MOVE" else parent_id
    source_children = tuple(client.list_children(source_parent))
    source = next((entry for entry in source_children if entry.id == source_id), None)
    if source is None:
        raise P115ExecutionPreflightError("源文件不在冻结的动作父目录中")
    _assert_action_source_identity(source, action)
    return _ActionInspection(
        "needs_request",
        _binding(action, parent_id, source),
        _readback("not_applied", entry=source),
    )


def _readback_action(
    action: P115PlanAction,
    client: P115OrganizeMutationClient,
    binding: Mapping[str, Any],
) -> _ActionInspection:
    parent_id = str(binding.get("parent_id") or "")
    if not parent_id:
        return _ActionInspection("unknown", dict(binding), _readback("unknown", message="缺少目标父目录 CID"))
    try:
        target_children = tuple(client.list_children(parent_id))
    except Exception as exc:
        return _ActionInspection(
            "unknown",
            dict(binding),
            _readback("unknown", error=_safe_error("readback_failed", exc)),
        )
    target_name = _target_name(action)
    target_matches = _name_matches(target_children, target_name)
    if action.kind == "ENSURE_DIRECTORY":
        directories = tuple(entry for entry in target_matches if entry.entry_kind == "directory")
        if len(directories) == 1 and len(target_matches) == 1:
            updated = _binding(action, parent_id, directories[0])
            return _ActionInspection("confirmed", updated, _readback("confirmed", entry=directories[0]))
        if not target_matches:
            return _ActionInspection("not_applied", dict(binding), _readback("not_applied"))
        return _ActionInspection("conflict", dict(binding), _readback("conflict", entries=target_matches))

    source_id = _source_id(action)
    expected = tuple(entry for entry in target_matches if entry.id == source_id)
    foreign = tuple(entry for entry in target_matches if entry.id != source_id)
    if foreign or len(expected) > 1:
        return _ActionInspection("conflict", dict(binding), _readback("conflict", entries=target_matches))
    if len(expected) == 1:
        try:
            _assert_stable_file_identity(expected[0], action)
        except Exception as exc:
            return _ActionInspection(
                "conflict",
                dict(binding),
                _readback("conflict", entry=expected[0], error=_safe_error("identity_mismatch", exc)),
            )
        if action.kind == "MOVE":
            old_parent_id = str(action.preconditions.get("source_parent_id") or "")
            try:
                old_children = tuple(client.list_children(old_parent_id)) if old_parent_id != parent_id else ()
            except Exception as exc:
                return _ActionInspection(
                    "unknown",
                    dict(binding),
                    _readback("unknown", error=_safe_error("readback_failed", exc)),
                )
            if any(entry.id == source_id for entry in old_children):
                return _ActionInspection(
                    "unknown",
                    dict(binding),
                    _readback("unknown", entries=(expected[0], *old_children)),
                )
        updated = _binding(action, parent_id, expected[0])
        return _ActionInspection("confirmed", updated, _readback("confirmed", entry=expected[0]))

    old_parent_id = str(action.preconditions.get("source_parent_id") or "")
    if action.kind == "RENAME":
        old_parent_id = parent_id
    try:
        old_children = tuple(client.list_children(old_parent_id)) if old_parent_id else ()
    except Exception as exc:
        return _ActionInspection(
            "unknown",
            dict(binding),
            _readback("unknown", error=_safe_error("readback_failed", exc)),
        )
    old_name = str(action.preconditions.get("source_name") or action.subject.get("current_name") or "")
    original = next(
        (entry for entry in old_children if entry.id == source_id and entry.name == old_name),
        None,
    )
    if original is not None and not target_matches:
        try:
            _assert_action_source_identity(original, action)
        except Exception as exc:
            return _ActionInspection(
                "conflict",
                dict(binding),
                _readback("conflict", entry=original, error=_safe_error("identity_mismatch", exc)),
            )
        return _ActionInspection("not_applied", dict(binding), _readback("not_applied", entry=original))
    return _ActionInspection(
        "unknown",
        dict(binding),
        _readback("unknown", entries=tuple(target_matches) + tuple(old_children)),
    )


def _mutate(
    action: P115PlanAction,
    client: P115OrganizeMutationClient,
    binding: Mapping[str, Any],
) -> Mapping[str, Any]:
    parent_id = str(binding.get("parent_id") or "")
    if action.kind == "ENSURE_DIRECTORY":
        return client.ensure_directory(parent_id, _target_name(action))
    source_id = _source_id(action)
    if action.kind == "MOVE":
        return client.move(source_id, parent_id)
    if action.kind == "RENAME":
        return client.rename(source_id, _target_name(action))
    raise P115ExecutionStateError(f"不支持的整理动作: {action.kind}")


def _classify_provider_outcome(
    response: Mapping[str, Any] | None,
    provider_error: Exception | None,
    *,
    no_effect_proved: bool,
    readback: _ActionInspection,
) -> tuple[P115ExecutionEffectStatus, dict[str, Any] | None, dict[str, Any] | None]:
    if readback.state == "confirmed":
        return (
            P115ExecutionEffectStatus.CONFIRMED,
            {"external_result": "authoritative_readback_confirmed"},
            None,
        )
    if no_effect_proved and readback.state == "not_applied":
        return (
            P115ExecutionEffectStatus.RETRYABLE,
            None,
            _safe_error("provider_no_effect", provider_error or RuntimeError("provider request was not sent")),
        )
    if provider_error is None and not _provider_success(response) and readback.state == "not_applied":
        return (
            P115ExecutionEffectStatus.REJECTED,
            None,
            {
                "type": "provider_rejected",
                "message": "115 提供方拒绝请求，权威回读确认副作用未发生",
            },
        )
    error = (
        _safe_error("provider_outcome_unknown", provider_error)
        if provider_error is not None
        else {
            "type": "provider_readback_unconfirmed",
            "message": "115 请求可能已发出但权威回读无法确认结果，禁止自动重放",
        }
    )
    return P115ExecutionEffectStatus.INDETERMINATE, None, error


def _source_for_preflight(
    action: P115PlanAction,
    children: Callable[[str], Sequence[P115RemoteEntry]],
) -> P115RemoteEntry:
    source_id = _source_id(action)
    source_parent_id = str(action.preconditions.get("source_parent_id") or "")
    if action.kind == "RENAME":
        source_parent_id = str(action.subject.get("origin_parent_id") or "")
    if not source_id or not source_parent_id:
        raise P115ExecutionPreflightError("动作缺少冻结的源文件身份")
    source = next((entry for entry in children(source_parent_id) if entry.id == source_id), None)
    if source is None:
        raise P115ExecutionPreflightError("冻结的源文件已不存在或已移动")
    _assert_original_source_identity(source, action)
    return source


def _assert_original_source_identity(source: P115RemoteEntry, action: P115PlanAction) -> None:
    expected_name = str(action.preconditions.get("source_name") or "")
    expected_parent = str(action.preconditions.get("source_parent_id") or "")
    if action.kind == "RENAME":
        expected_name = str(action.subject.get("origin_name") or expected_name)
        expected_parent = str(action.subject.get("origin_parent_id") or expected_parent)
    _assert_file_identity(
        source,
        action,
        expected_name=expected_name,
        expected_parent=expected_parent,
        check_raw_hash=True,
    )


def _assert_action_source_identity(source: P115RemoteEntry, action: P115PlanAction) -> None:
    _assert_file_identity(
        source,
        action,
        expected_name=str(action.preconditions.get("source_name") or action.subject.get("current_name") or ""),
        expected_parent=(
            str(action.preconditions.get("source_parent_id") or "") if action.kind == "MOVE" else source.parent_id
        ),
        check_raw_hash=action.kind == "MOVE",
    )


def _assert_stable_file_identity(source: P115RemoteEntry, action: P115PlanAction) -> None:
    _assert_file_identity(
        source,
        action,
        expected_name=source.name,
        expected_parent=source.parent_id,
        check_raw_hash=False,
    )


def _assert_file_identity(
    source: P115RemoteEntry,
    action: P115PlanAction,
    *,
    expected_name: str,
    expected_parent: str,
    check_raw_hash: bool,
) -> None:
    comparisons = {
        "id": (source.id, _source_id(action)),
        "name": (source.name, expected_name),
        "parent_id": (source.parent_id, expected_parent),
        "sha1": (source.sha1.upper(), str(action.preconditions.get("source_sha1") or "").upper()),
        "size": (source.size, int(action.preconditions.get("source_size") or 0)),
    }
    mismatches = [field for field, (actual, expected) in comparisons.items() if actual != expected]
    expected_raw_hash = str(action.preconditions.get("source_raw_hash") or "")
    if check_raw_hash and expected_raw_hash and source.raw_hash != expected_raw_hash:
        mismatches.append("raw_hash")
    if source.entry_kind != "file":
        mismatches.append("entry_kind")
    if mismatches:
        raise P115ExecutionPreflightError(f"源文件身份已漂移: {sorted(set(mismatches))}")


def _resolved_parent(
    action: P115PlanAction,
    resolved_directories: Mapping[str, str | None],
) -> str | None:
    parent_action_id = _parent_action_id(action)
    if parent_action_id:
        return resolved_directories.get(parent_action_id)
    parent_id = str(action.subject.get("parent_cid") or action.destination.get("parent_cid") or "").strip()
    if not parent_id:
        raise P115ExecutionPreflightError("动作缺少目标父目录身份")
    return parent_id


def _parent_action_id(action: P115PlanAction) -> str:
    return str(
        action.destination.get("parent_action_id")
        or action.subject.get("parent_action_id")
        or action.preconditions.get("source_parent_action_id")
        or ""
    ).strip()


def _source_id(action: P115PlanAction) -> str:
    value = str(action.preconditions.get("source_id") or action.subject.get("file_id") or "").strip()
    if not value and action.kind != "ENSURE_DIRECTORY":
        raise P115ExecutionPreflightError("动作缺少源文件 ID")
    return value


def _target_name(action: P115PlanAction) -> str:
    value = action.destination.get("intermediate_name") if action.kind == "MOVE" else action.destination.get("name")
    name = str(value or "").strip()
    if not name:
        raise P115ExecutionPreflightError("动作缺少目标名称")
    return name


def _binding(
    action: P115PlanAction,
    parent_id: str | None,
    entry: P115RemoteEntry | None = None,
) -> dict[str, Any]:
    return {
        "parent_id": parent_id,
        "parent_action_id": _parent_action_id(action),
        "target_root_cid": str(action.destination.get("target_root_cid") or ""),
        "target_path": str(action.destination.get("path") or ""),
        "target_name": _target_name(action),
        "resolved_entry_id": entry.id if entry is not None else None,
        "source": entry.to_dict() if entry is not None and action.kind != "ENSURE_DIRECTORY" else None,
        "provider_request_required": entry is None,
    }


def _name_matches(entries: Sequence[P115RemoteEntry], name: str) -> tuple[P115RemoteEntry, ...]:
    expected = str(name or "").casefold()
    return tuple(entry for entry in entries if entry.name.casefold() == expected)


def _validate_action_graph(plan: P115OrganizePlan) -> None:
    seen: set[str] = set()
    for expected_index, action in enumerate(plan.actions):
        if action.action_index != expected_index:
            raise P115ExecutionPreflightError("整理计划动作索引不连续")
        if action.kind not in _ALLOWED_ACTION_KINDS:
            raise P115ExecutionPreflightError(f"整理计划包含不支持的动作: {action.kind}")
        if action.action_id in seen:
            raise P115ExecutionPreflightError("整理计划包含重复 action_id")
        if any(dependency not in seen for dependency in action.dependencies):
            raise P115ExecutionPreflightError("整理计划动作依赖不是已冻结的前序动作")
        if action.destructive:
            raise P115ExecutionPreflightError("P6.4 不执行 destructive 动作")
        if action.kind in {"MOVE", "RENAME"} and action.preconditions.get("destination_must_be_absent") is not True:
            raise P115ExecutionPreflightError("MOVE/RENAME 必须冻结 destination_must_be_absent=true")
        request = effect_request_document(action)
        if _hash_json(request) != action.effect_key:
            raise P115ExecutionPreflightError("整理动作 effect_key 与冻结语义不一致")
        seen.add(action.action_id)


def effect_request_document(action: P115PlanAction) -> dict[str, Any]:
    return {
        "kind": action.kind,
        "account_id": str(action.preconditions.get("account_id") or ""),
        "subject": {
            str(key): deepcopy(value) for key, value in action.subject.items() if not str(key).endswith("_action_id")
        },
        "destination": {
            str(key): deepcopy(value)
            for key, value in action.destination.items()
            if not str(key).endswith("_action_id")
        },
    }


def effect_request_hash(action: P115PlanAction) -> str:
    return _hash_json(
        {
            "schema_version": P115_ORGANIZE_EXECUTION_SCHEMA_VERSION,
            "semantic_request": effect_request_document(action),
        }
    )


def _resolved_directories(execution: P115OrganizeExecution) -> dict[str, str | None]:
    resolved: dict[str, str | None] = {}
    for action in execution.actions:
        if action.kind != "ENSURE_DIRECTORY" or action.status is not P115ExecutionActionStatus.CONFIRMED:
            continue
        entry_id = action.binding.get("resolved_entry_id")
        if not entry_id:
            entry = action.verification.get("entry")
            if isinstance(entry, Mapping):
                entry_id = entry.get("id")
        resolved[action.action_id] = str(entry_id) if entry_id else None
    return resolved


def _remember_directory(
    plan_action: P115PlanAction,
    execution_action: P115ExecutionAction,
    resolved: dict[str, str | None],
) -> None:
    if plan_action.kind != "ENSURE_DIRECTORY":
        return
    entry_id = execution_action.binding.get("resolved_entry_id")
    if not entry_id:
        entry = execution_action.verification.get("entry")
        if isinstance(entry, Mapping):
            entry_id = entry.get("id")
    if not entry_id:
        raise P115ExecutionStateError("已确认目录动作缺少权威回读 CID")
    resolved[plan_action.action_id] = str(entry_id)


def records_from_plan(plan: P115OrganizePlan) -> list[dict[str, Any]]:
    sources = {str(item.get("id") or ""): item for item in plan.source_snapshot}
    rename_by_source: dict[str, str] = {}
    moves: dict[str, P115PlanAction] = {}
    for action in plan.actions:
        source_id = str(action.preconditions.get("source_id") or action.subject.get("file_id") or "")
        if action.kind == "MOVE":
            moves[source_id] = action
        elif action.kind == "RENAME":
            rename_by_source[source_id] = str(action.destination.get("name") or "")
    rules = {
        str(rule.get("id") or ""): rule
        for rule in plan.config_snapshot.get("sorting_rules", [])
        if isinstance(rule, Mapping)
    }
    records: list[dict[str, Any]] = []
    for source_id, move in sorted(moves.items()):
        source = sources.get(source_id, {})
        recognition = move.decision_basis.get("recognition")
        recognition = dict(recognition) if isinstance(recognition, Mapping) else {}
        rule_id = str(move.decision_basis.get("sorting_rule_id") or "")
        rule = rules.get(rule_id, {})
        records.append(
            {
                "file_id": source_id,
                "pick_code": str(source.get("pick_code") or "") or None,
                "original_name": str(source.get("name") or ""),
                "renamed_name": rename_by_source.get(source_id) or str(source.get("name") or ""),
                "status": "success",
                "fail_reason": None,
                "tmdb_id": str(recognition.get("tmdb_id") or "") or None,
                "media_type": str(recognition.get("media_type") or "") or None,
                "target_cid": str(move.destination.get("target_root_cid") or "") or None,
                "category_name": str(rule.get("dir_name") or rule.get("name") or "") or None,
                "season_number": recognition.get("season_number"),
            }
        )
    return records


def _execution_action(execution: P115OrganizeExecution, action_id: str) -> P115ExecutionAction:
    value = next((action for action in execution.actions if action.action_id == action_id), None)
    if value is None:
        raise P115ExecutionStateError(f"执行账本缺少 action {action_id}")
    return value


def _provider_receipt(
    response: Mapping[str, Any] | None,
    error: Exception | None,
    *,
    no_effect_proved: bool,
) -> dict[str, Any]:
    return {
        "received_response": response is not None,
        "response": _safe_mapping(response),
        "error": _safe_error("provider_call_failed", error) if error is not None else None,
        "no_effect_proved": no_effect_proved,
    }


def _provider_success(response: Mapping[str, Any] | None) -> bool:
    return isinstance(response, Mapping) and (response.get("state") is True or response.get("success") is True)


def _safe_mapping(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    safe = redact_sensitive(dict(value))
    if not isinstance(safe, Mapping):
        return {}
    return normalize_json_object(dict(safe), field="p115 provider response")


def _safe_error(error_type: str, exc: BaseException) -> dict[str, Any]:
    message = redact_sensitive(str(exc))
    return normalize_json_object(
        {
            "type": error_type,
            "message": str(message),
            "exception": type(exc).__name__,
        },
        field="p115 execution error",
    )


def _readback(
    state: str,
    *,
    entry: P115RemoteEntry | None = None,
    entries: Sequence[P115RemoteEntry] = (),
    message: str = "",
    error: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "state": state,
        "entry": entry.to_dict() if entry is not None else None,
        "entries": [value.to_dict() for value in entries],
        "message": message,
        "error": deepcopy(dict(error)) if error is not None else None,
    }


def _run_result(
    execution: P115OrganizeExecution,
    message: str,
    *,
    p4_terminalized: bool,
) -> P115ExecutionRunResult:
    return P115ExecutionRunResult(
        execution.status,
        message,
        execution.to_dict(),
        p4_terminalized,
    )


def _terminal_message(execution: P115OrganizeExecution) -> str:
    return {
        P115ExecutionStatus.CONFIRMED: "115 整理计划已完成",
        P115ExecutionStatus.REJECTED: "115 整理计划已被拒绝",
        P115ExecutionStatus.INDETERMINATE: "115 整理计划结果不确定，禁止自动重放",
        P115ExecutionStatus.MANUAL_REVIEW: "115 整理计划需要人工复核",
    }.get(execution.status, "115 整理计划正在执行")


def _record_progress_effect(progress: P115TaskProgress, method_name: str, label: str) -> None:
    method = getattr(progress, method_name, None)
    if callable(method):
        method(label)


def _required_text(value: object, field: str, *, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field} 必须是 1-{maximum} 个字符")
    return normalized


def _optional_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def execution_id_for(plan_id: str, approved_intent_id: str) -> UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"etk:p115-organize-execution:{plan_id}:{approved_intent_id}")


def execution_job_for(
    execution_id: UUID,
    *,
    requested_by_user_id: str | None,
    trace_id: str | None,
) -> P115ExecutionJob:
    job_id = uuid.uuid5(uuid.NAMESPACE_URL, f"etk:p115-organize-job:{execution_id}")
    attempt_id = uuid.uuid5(uuid.NAMESPACE_URL, f"etk:p115-organize-attempt:{execution_id}:1")
    event_id = uuid.uuid5(uuid.NAMESPACE_URL, f"etk:p115-organize-dispatch:{execution_id}:1")
    return P115ExecutionJob(
        job_id=job_id,
        attempt_id=attempt_id,
        outbox_event_id=event_id,
        task_key=P115_ORGANIZE_TASK_KEY,
        payload={
            "payload_version": 1,
            "parameters": {"execution_id": str(execution_id)},
            "processor_type": "media",
            "display_name": "网盘文件整理",
            "silent": False,
        },
        requested_by_user_id=requested_by_user_id,
        trace_id=trace_id,
    )


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


__all__ = [
    "P115ExecutionAction",
    "P115ExecutionActionStatus",
    "P115ExecutionBusy",
    "P115ExecutionConflict",
    "P115ExecutionEffect",
    "P115ExecutionEffectStatus",
    "P115ExecutionError",
    "P115ExecutionJob",
    "P115ExecutionNotFound",
    "P115ExecutionPreflightError",
    "P115ExecutionReservation",
    "P115ExecutionRunResult",
    "P115ExecutionStart",
    "P115ExecutionStateError",
    "P115ExecutionStatus",
    "P115JobAttemptOwner",
    "P115OrganizeExecution",
    "P115OrganizeExecutionRepository",
    "P115OrganizeExecutionService",
    "P115OrganizeMutationClient",
    "P115OrganizeMutationGateway",
    "P115PreparedAction",
    "P115ProviderNoEffectError",
    "P115Reconciliation",
    "P115ReconciliationReservation",
    "P115ReconciliationResult",
    "P115RemoteEntry",
    "P115_ORGANIZE_EFFECT_LEASE_SECONDS",
    "P115_ORGANIZE_EXECUTION_SCHEMA_VERSION",
    "P115_ORGANIZE_TASK_KEY",
    "effect_request_document",
    "effect_request_hash",
    "execution_id_for",
    "execution_job_for",
    "records_from_plan",
]
