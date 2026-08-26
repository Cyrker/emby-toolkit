from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import AttemptStatus, JobStatus, SqlAlchemyJobRepository
from etk.platform.jobs.json_payload import canonical_json, normalize_json_object

from .organize_execution import (
    P115ExecutionAction,
    P115ExecutionActionStatus,
    P115ExecutionBusy,
    P115ExecutionConflict,
    P115ExecutionEffect,
    P115ExecutionEffectStatus,
    P115ExecutionJob,
    P115ExecutionNotFound,
    P115ExecutionReservation,
    P115ExecutionStart,
    P115ExecutionStateError,
    P115ExecutionStatus,
    P115JobAttemptOwner,
    P115OrganizeExecution,
    P115PreparedAction,
    P115Reconciliation,
    P115ReconciliationReservation,
    P115ReconciliationResult,
    effect_request_document,
    effect_request_hash,
)
from .organize_plan import P115OrganizePlan
from .repository import assert_organize_plan_current


class SqlAlchemyP115OrganizeExecutionRepository:
    """Own P6 execution/effect state and its P4 orchestration transaction boundary."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def find_by_intent(self, approved_intent_id: str) -> P115OrganizeExecution | None:
        with self._unit_of_work() as uow:
            execution_id = uow.session.scalar(
                text(
                    """
                    SELECT execution_id
                    FROM p115_organize_plan_executions
                    WHERE approved_intent_id = :intent
                    """
                ),
                {"intent": approved_intent_id},
            )
            return _load_execution(uow, UUID(str(execution_id))) if execution_id is not None else None

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
    ) -> P115ExecutionReservation:
        prepared_by_id = {value.action_id: value for value in prepared_actions}
        if tuple(prepared_by_id) != tuple(action.action_id for action in plan.actions):
            raise P115ExecutionStateError("whole-plan preflight did not cover the immutable action order")
        normalized_preflight = normalize_json_object(dict(preflight), field="p115 execution preflight")
        preflight_hash = _json_hash(normalized_preflight)
        try:
            with self._unit_of_work() as uow:
                _advisory_lock(uow, f"p115-organize-plan:{plan.plan_id}")
                _advisory_lock(uow, f"p115-organize-intent:{approved_intent_id}")
                existing = _execution_by_intent(uow, approved_intent_id)
                if existing is not None:
                    loaded = _load_execution(uow, UUID(str(existing["execution_id"])))
                    if loaded is None:
                        raise P115ExecutionStateError("approved intent points to a missing execution")
                    if loaded.plan_id != plan.plan_id or loaded.request_hash != request_hash:
                        raise P115ExecutionConflict("approved execution intent was used for another request")
                    return P115ExecutionReservation(loaded, False)
                consumed = uow.session.scalar(
                    text(
                        """
                        SELECT execution_id
                        FROM p115_organize_plan_executions
                        WHERE plan_id = :plan_id
                        FOR UPDATE
                        """
                    ),
                    {"plan_id": plan.plan_id},
                )
                if consumed is not None:
                    raise P115ExecutionConflict("immutable organize Plan has already been consumed")

                assert_organize_plan_current(uow, plan, require_unexpired=True)
                reservations = self._classify_effects(
                    uow,
                    plan,
                    prepared_by_id,
                    provider_backend=provider_backend,
                )
                self._insert_job(uow, job)
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_organize_plan_executions (
                            execution_id, plan_id, schema_version, approved_intent_id,
                            request_hash, account_id, provider_backend, status,
                            preflight_hash, preflight_json, job_id
                        ) VALUES (
                            :execution_id, :plan_id, 1, :approved_intent_id,
                            :request_hash, :account_id, :provider_backend, 'prepared',
                            :preflight_hash, CAST(:preflight_json AS jsonb), :job_id
                        )
                        """
                    ),
                    {
                        "execution_id": execution_id,
                        "plan_id": plan.plan_id,
                        "approved_intent_id": approved_intent_id,
                        "request_hash": request_hash,
                        "account_id": plan.account_id,
                        "provider_backend": provider_backend,
                        "preflight_hash": preflight_hash,
                        "preflight_json": canonical_json(normalized_preflight),
                        "job_id": job.job_id,
                    },
                )
                for action, prepared, reservation in reservations:
                    if reservation["create_effect"]:
                        self._insert_effect(
                            uow,
                            plan,
                            action,
                            prepared,
                            provider_backend=provider_backend,
                        )
                    uow.session.execute(
                        text(
                            """
                            INSERT INTO p115_organize_execution_actions (
                                execution_id, plan_id, action_index, action_id,
                                effect_key, binding_json, disposition, status,
                                verification_status, verification_json
                            ) VALUES (
                                :execution_id, :plan_id, :action_index, :action_id,
                                :effect_key, CAST(:binding_json AS jsonb), :disposition, :status,
                                :verification_status, CAST(:verification_json AS jsonb)
                            )
                            """
                        ),
                        {
                            "execution_id": execution_id,
                            "plan_id": plan.plan_id,
                            "action_index": action.action_index,
                            "action_id": action.action_id,
                            "effect_key": action.effect_key,
                            "binding_json": canonical_json(prepared.binding),
                            "disposition": reservation["disposition"],
                            "status": reservation["mapping_status"],
                            "verification_status": reservation["verification_status"],
                            "verification_json": canonical_json(prepared.readback),
                        },
                    )
                    _append_event(
                        uow,
                        effect_key=action.effect_key,
                        execution_id=execution_id,
                        event_type=str(reservation["event_type"]),
                        previous_status=reservation["previous_status"],
                        new_status=reservation["effect_status"],
                        evidence={
                            "action_id": action.action_id,
                            "action_index": action.action_index,
                            "disposition": reservation["disposition"],
                            "preflight": prepared.readback,
                            "external_mutations_executed": 0,
                        },
                    )
                loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
                uow.commit()
                return P115ExecutionReservation(loaded, True)
        except IntegrityError as exc:
            if _constraint_name(exc) == "uq_job_runs_single_active_root_serial":
                raise P115ExecutionBusy("another serial Job is already active") from exc
            raise
        raise AssertionError("unreachable")

    def _classify_effects(
        self,
        uow: SqlAlchemyUnitOfWork,
        plan: P115OrganizePlan,
        prepared_by_id: Mapping[str, P115PreparedAction],
        *,
        provider_backend: str,
    ) -> list[tuple[Any, P115PreparedAction, dict[str, Any]]]:
        reservations: list[tuple[Any, P115PreparedAction, dict[str, Any]]] = []
        for action in plan.actions:
            prepared = prepared_by_id[action.action_id]
            effect = _effect_row(uow, action.effect_key, for_update=True)
            request_hash = effect_request_hash(action)
            request = effect_request_document(action)
            if effect is None:
                observed = prepared.state == "confirmed"
                reservations.append(
                    (
                        action,
                        prepared,
                        {
                            "create_effect": True,
                            "disposition": "observed_preexisting" if observed else "pending",
                            "mapping_status": "confirmed" if observed else "pending",
                            "verification_status": "verified" if observed else "pending",
                            "event_type": "effect_observed_preexisting" if observed else "effect_reserved",
                            "previous_status": None,
                            "effect_status": "confirmed" if observed else "pending",
                        },
                    )
                )
                continue
            if (
                str(effect["request_hash"]) != request_hash
                or dict(effect["request_json"] or {}) != request
                or str(effect["kind"]) != action.kind
            ):
                raise P115ExecutionConflict("effect_key was already used for a different semantic request")
            status = P115ExecutionEffectStatus(str(effect["status"]))
            if status is P115ExecutionEffectStatus.CONFIRMED and prepared.state == "confirmed":
                reservations.append(
                    (
                        action,
                        prepared,
                        {
                            "create_effect": False,
                            "disposition": "reused_confirmed",
                            "mapping_status": "confirmed",
                            "verification_status": "verified",
                            "event_type": "confirmed_effect_freshly_verified",
                            "previous_status": status.value,
                            "effect_status": status.value,
                        },
                    )
                )
                continue
            if status is P115ExecutionEffectStatus.RETRYABLE and prepared.state != "confirmed":
                reservations.append(
                    (
                        action,
                        prepared,
                        {
                            "create_effect": False,
                            "disposition": "reopened",
                            "mapping_status": "pending",
                            "verification_status": "pending",
                            "event_type": "retryable_effect_reopened",
                            "previous_status": status.value,
                            "effect_status": status.value,
                        },
                    )
                )
                continue
            raise P115ExecutionConflict(
                f"effect {action.effect_key} is {status.value}; fresh provider state does not permit replay"
            )
        return reservations

    @staticmethod
    def _insert_job(uow: SqlAlchemyUnitOfWork, job: P115ExecutionJob) -> None:
        repository = SqlAlchemyJobRepository(uow.session)
        repository.insert_job(
            {
                "id": job.job_id,
                "task_key": job.task_key,
                "trigger_type": "manual",
                "queue_name": "legacy.serial",
                "payload_version": 1,
                "request_payload_json": canonical_json(job.payload),
                "requested_by_user_id": job.requested_by_user_id,
                "parent_job_id": None,
                "workflow_step": None,
                "max_attempts": 1,
                "timeout_seconds": None,
                "scheduled_for": None,
                "trace_id": job.trace_id,
            }
        )
        repository.insert_attempt(attempt_id=job.attempt_id, job_id=job.job_id, attempt_no=1)
        repository.insert_dispatch_event(
            event_id=job.outbox_event_id,
            job_id=job.job_id,
            attempt_id=job.attempt_id,
        )

    @staticmethod
    def _insert_effect(
        uow: SqlAlchemyUnitOfWork,
        plan: P115OrganizePlan,
        action: Any,
        prepared: P115PreparedAction,
        *,
        provider_backend: str,
    ) -> None:
        observed = prepared.state == "confirmed"
        resolved_object_id, resolved_parent_cid = _resolved_identity(prepared.binding, prepared.readback)
        uow.session.execute(
            text(
                """
                INSERT INTO p115_organize_effects (
                    effect_id, effect_key, request_hash, request_json, kind,
                    origin_plan_id, origin_action_id, status, provider_backend,
                    readback_json, readback_at, resolved_object_id, resolved_parent_cid,
                    completion_mode, result_json, finished_at
                ) VALUES (
                    :effect_id, :effect_key, :request_hash, CAST(:request_json AS jsonb), :kind,
                    :origin_plan_id, :origin_action_id, :status, :provider_backend,
                    CAST(:readback_json AS jsonb), :readback_at, :resolved_object_id, :resolved_parent_cid,
                    :completion_mode, CAST(:result_json AS jsonb), :finished_at
                )
                """
            ),
            {
                "effect_id": uuid5(NAMESPACE_URL, f"etk:p115-organize-effect:{action.effect_key}"),
                "effect_key": action.effect_key,
                "request_hash": effect_request_hash(action),
                "request_json": canonical_json(effect_request_document(action)),
                "kind": action.kind,
                "origin_plan_id": plan.plan_id,
                "origin_action_id": action.action_id,
                "status": "confirmed" if observed else "pending",
                "provider_backend": provider_backend,
                "readback_json": canonical_json(prepared.readback) if observed else None,
                "readback_at": datetime.now(UTC) if observed else None,
                "resolved_object_id": resolved_object_id,
                "resolved_parent_cid": resolved_parent_cid,
                "completion_mode": "observed_preexisting" if observed else None,
                "result_json": canonical_json({"external_mutations_executed": 0}) if observed else None,
                "finished_at": datetime.now(UTC) if observed else None,
            },
        )

    def get(self, execution_id: UUID) -> P115OrganizeExecution | None:
        with self._unit_of_work() as uow:
            return _load_execution(uow, execution_id)

    def recover_orphaned(self, execution_id: UUID) -> P115OrganizeExecution | None:
        with self._unit_of_work() as uow:
            execution = _execution_row(uow, execution_id, for_update=True)
            if execution is None:
                return None
            status = P115ExecutionStatus(str(execution["status"]))
            if status not in {P115ExecutionStatus.PREPARED, P115ExecutionStatus.RUNNING}:
                return _load_execution(uow, execution_id)
            job = SqlAlchemyJobRepository(uow.session).get_job_row(UUID(str(execution["job_id"])), for_update=True)
            if job is None:
                raise P115ExecutionStateError("P6 execution points to a missing P4 Job")
            job_status = JobStatus(str(job["status"]))
            if job_status in {
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                JobStatus.RETRY_WAIT,
                JobStatus.CANCEL_REQUESTED,
            }:
                return _load_execution(uow, execution_id)

            error = {
                "type": "orchestration_attempt_terminated",
                "message": "P4 Job 已终止但 P6 execution 未提交终态",
                "job_status": job_status.value,
            }
            uncertain_effects = self._mark_active_effects_indeterminate(uow, execution_id, error)
            self._reject_unstarted_actions(uow, execution_id, error)
            nonconfirmed = int(
                uow.session.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM p115_organize_execution_actions
                        WHERE execution_id = :execution_id AND status <> 'confirmed'
                        """
                    ),
                    {"execution_id": execution_id},
                )
                or 0
            )
            if uncertain_effects:
                recovery_status = P115ExecutionStatus.INDETERMINATE
                error["message"] = "P4 Job 在 provider effect 活跃期间终止，已转入只读对账"
            elif nonconfirmed:
                recovery_status = P115ExecutionStatus.REJECTED
                error["message"] = "P4 Job 在 provider 请求前终止；未发送动作已释放供新 Plan 使用"
            else:
                recovery_status = P115ExecutionStatus.MANUAL_REVIEW
                error["message"] = "所有 provider effect 已确认，但 P4 Job 在最终记录提交前终止"
                self._mark_finalization_gap_manual_review(uow, execution_id, error)
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_plan_executions
                    SET status = :status, error_json = CAST(:error_json AS jsonb),
                        finished_at = NOW(), updated_at = NOW()
                    WHERE execution_id = :execution_id
                      AND status IN ('prepared', 'running')
                    """
                ),
                {
                    "execution_id": execution_id,
                    "status": recovery_status.value,
                    "error_json": canonical_json(error),
                },
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded

    def start(self, execution_id: UUID, owner: P115JobAttemptOwner) -> P115ExecutionStart:
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            status = P115ExecutionStatus(str(execution["status"]))
            if status.terminal:
                loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
                _terminalize_p4(uow, owner, loaded)
                uow.commit()
                return P115ExecutionStart(loaded, True)
            if status is P115ExecutionStatus.PREPARED:
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_organize_plan_executions
                        SET status = 'running', started_at = COALESCE(started_at, NOW()),
                            updated_at = NOW()
                        WHERE execution_id = :execution_id AND status = 'prepared'
                        """
                    ),
                    {"execution_id": execution_id},
                )
            elif status is not P115ExecutionStatus.RUNNING:
                raise P115ExecutionStateError(f"execution is not startable from {status.value}")
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return P115ExecutionStart(loaded, False)
        raise AssertionError("unreachable")

    def observe_confirmed(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        normalized_binding = normalize_json_object(dict(binding), field="p115 action binding")
        normalized_readback = normalize_json_object(dict(readback), field="p115 action verification")
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            action = _require_action_row(_action_row(uow, execution_id, action_id, for_update=True), action_id)
            if str(action["status"]) != "confirmed":
                raise P115ExecutionStateError("only an effective-confirmed mapping can be freshly observed")
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_execution_actions
                    SET binding_json = CAST(:binding_json AS jsonb),
                        verification_status = 'verified',
                        verification_json = CAST(:verification_json AS jsonb)
                    WHERE execution_id = :execution_id AND action_id = :action_id
                    """
                ),
                {
                    "execution_id": execution_id,
                    "action_id": action_id,
                    "binding_json": canonical_json(normalized_binding),
                    "verification_json": canonical_json(normalized_readback),
                },
            )
            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type="confirmed_effect_execution_readback",
                previous_status=str(action["effect_status"]),
                new_status=str(action["effect_status"]),
                evidence={"readback": normalized_readback, "external_mutations_executed": 0},
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded
        raise AssertionError("unreachable")

    def stop_for_postcondition_drift(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        binding: Mapping[str, Any],
        verification: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        return self._stop_execution(
            execution_id,
            action_id,
            owner,
            execution_status=P115ExecutionStatus.MANUAL_REVIEW,
            action_status=P115ExecutionActionStatus.MANUAL_REVIEW,
            verification_status="postcondition_drift",
            binding=binding,
            verification=verification,
            error=error,
            event_type="confirmed_postcondition_drift",
        )

    def claim_effect(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        lease_seconds: int,
    ) -> P115ExecutionAction | None:
        if lease_seconds < 1:
            raise ValueError("effect lease_seconds must be positive")
        claim_token = uuid4()
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_organize_effects AS effect
                        SET status = 'claimed', attempts = attempts + 1,
                            available_at = NOW(), last_attempt_at = NOW(),
                            provider_backend = execution.provider_backend,
                            sponsor_execution_id = mapping.execution_id,
                            sponsor_action_index = mapping.action_index,
                            sponsor_account_id = execution.account_id,
                            claimed_by = :worker_id, claim_token = :claim_token,
                            claimed_at = NOW(),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            provider_call_started_at = NULL,
                            receipt_json = NULL, receipt_persisted_at = NULL,
                            readback_json = NULL, readback_at = NULL,
                            resolved_object_id = NULL, resolved_parent_cid = NULL,
                            completion_mode = NULL, resolution_status = NULL,
                            result_json = NULL, error_json = NULL,
                            finished_at = NULL, updated_at = NOW()
                        FROM p115_organize_execution_actions AS mapping,
                             p115_organize_plan_executions AS execution,
                             p115_organize_plan_actions AS plan_action
                        WHERE mapping.execution_id = :execution_id
                          AND mapping.action_id = :action_id
                          AND mapping.status = 'pending'
                          AND execution.execution_id = mapping.execution_id
                          AND execution.status = 'running'
                          AND execution.job_id = :job_id
                          AND plan_action.plan_id = mapping.plan_id
                          AND plan_action.action_index = mapping.action_index
                          AND plan_action.effect_key = mapping.effect_key
                          AND effect.effect_key = mapping.effect_key
                          AND effect.status IN ('pending', 'retryable')
                          AND effect.available_at <= NOW()
                          AND NOT EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements_text(plan_action.dependencies_json) AS dependency(action_id)
                              LEFT JOIN p115_organize_execution_actions AS dependency_mapping
                                ON dependency_mapping.execution_id = mapping.execution_id
                               AND dependency_mapping.action_id = dependency.action_id
                              WHERE dependency_mapping.status IS DISTINCT FROM 'confirmed'
                          )
                        RETURNING mapping.action_id
                        """
                    ),
                    {
                        "execution_id": execution_id,
                        "action_id": action_id,
                        "job_id": owner.job_id,
                        "worker_id": owner.worker_id,
                        "claim_token": claim_token,
                        "lease_seconds": lease_seconds,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                uow.commit()
                return None
            action = _require_action_row(_action_row(uow, execution_id, action_id), action_id)
            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type="effect_claimed",
                previous_status="pending_or_retryable",
                new_status="claimed",
                attempt_no=int(action["attempts"]),
                claim_token=claim_token,
                evidence={
                    "sponsor_execution_id": str(execution_id),
                    "sponsor_action_index": int(action["action_index"]),
                    "provider_backend": str(execution["provider_backend"]),
                    "account_id": str(execution["account_id"]),
                },
            )
            loaded = _load_action_projection(uow, execution_id, action_id)
            uow.commit()
            return loaded

    def confirm_without_request(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        normalized_binding = normalize_json_object(dict(binding), field="p115 action binding")
        normalized_readback = normalize_json_object(dict(readback), field="p115 action readback")
        resolved_object_id, resolved_parent_cid = _resolved_identity(normalized_binding, normalized_readback)
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            action = _require_action_row(_action_row(uow, execution_id, action_id, for_update=True), action_id)
            changed = uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_effects
                    SET status = 'confirmed',
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        readback_json = CAST(:readback_json AS jsonb), readback_at = NOW(),
                        resolved_object_id = :resolved_object_id,
                        resolved_parent_cid = :resolved_parent_cid,
                        completion_mode = 'observed_preexisting',
                        result_json = CAST(:result_json AS jsonb), error_json = NULL,
                        finished_at = NOW(), updated_at = NOW()
                    WHERE effect_key = :effect_key
                      AND status = 'claimed'
                      AND claim_token = :claim_token
                    RETURNING effect_key
                    """
                ),
                {
                    "effect_key": action["effect_key"],
                    "claim_token": effect_claim_token,
                    "readback_json": canonical_json(normalized_readback),
                    "resolved_object_id": resolved_object_id,
                    "resolved_parent_cid": resolved_parent_cid,
                    "result_json": canonical_json({"external_mutations_executed": 0}),
                },
            ).scalar_one_or_none()
            if changed is None:
                raise P115ExecutionStateError("stale effect claim cannot confirm an observed result")
            self._confirm_mapping(
                uow,
                execution_id,
                action_id,
                binding=normalized_binding,
                readback=normalized_readback,
            )
            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type="effect_confirmed_without_request",
                previous_status="claimed",
                new_status="confirmed",
                attempt_no=int(action["attempts"]),
                claim_token=effect_claim_token,
                evidence={"readback": normalized_readback, "external_mutations_executed": 0},
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded
        raise AssertionError("unreachable")

    def mark_requesting(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        binding: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        normalized_binding = normalize_json_object(dict(binding), field="p115 action binding")
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            action = _require_action_row(_action_row(uow, execution_id, action_id, for_update=True), action_id)
            changed = uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_effects
                    SET status = 'requesting', provider_call_started_at = NOW(), updated_at = NOW()
                    WHERE effect_key = :effect_key
                      AND status = 'claimed'
                      AND claim_token = :claim_token
                    RETURNING effect_key
                    """
                ),
                {"effect_key": action["effect_key"], "claim_token": effect_claim_token},
            ).scalar_one_or_none()
            if changed is None:
                raise P115ExecutionStateError("stale effect claim cannot enter requesting")
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_execution_actions
                    SET binding_json = CAST(:binding_json AS jsonb)
                    WHERE execution_id = :execution_id AND action_id = :action_id
                    """
                ),
                {
                    "execution_id": execution_id,
                    "action_id": action_id,
                    "binding_json": canonical_json(normalized_binding),
                },
            )
            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type="provider_requesting_committed",
                previous_status="claimed",
                new_status="requesting",
                attempt_no=int(action["attempts"]),
                claim_token=effect_claim_token,
                evidence={
                    "binding": normalized_binding,
                    "provider_backend": str(execution["provider_backend"]),
                },
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded
        raise AssertionError("unreachable")

    def persist_receipt(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        receipt: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        normalized_receipt = normalize_json_object(dict(receipt), field="p115 provider receipt")
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            action = _require_action_row(_action_row(uow, execution_id, action_id, for_update=True), action_id)
            changed = uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_effects
                    SET status = 'awaiting_readback',
                        receipt_json = CAST(:receipt_json AS jsonb),
                        receipt_persisted_at = NOW(), updated_at = NOW()
                    WHERE effect_key = :effect_key
                      AND status = 'requesting'
                      AND claim_token = :claim_token
                    RETURNING effect_key
                    """
                ),
                {
                    "effect_key": action["effect_key"],
                    "claim_token": effect_claim_token,
                    "receipt_json": canonical_json(normalized_receipt),
                },
            ).scalar_one_or_none()
            if changed is None:
                raise P115ExecutionStateError("stale effect claim cannot persist provider receipt")
            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type="provider_receipt_persisted",
                previous_status="requesting",
                new_status="awaiting_readback",
                attempt_no=int(action["attempts"]),
                claim_token=effect_claim_token,
                evidence={"receipt": normalized_receipt},
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded
        raise AssertionError("unreachable")

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
    ) -> P115OrganizeExecution:
        if status not in {
            P115ExecutionEffectStatus.CONFIRMED,
            P115ExecutionEffectStatus.RETRYABLE,
            P115ExecutionEffectStatus.REJECTED,
            P115ExecutionEffectStatus.INDETERMINATE,
        }:
            raise ValueError("effect completion status is not supported")
        normalized_binding = normalize_json_object(dict(binding), field="p115 action binding")
        normalized_readback = normalize_json_object(dict(readback), field="p115 action readback")
        normalized_result = (
            normalize_json_object(dict(result), field="p115 effect result") if result is not None else None
        )
        normalized_error = normalize_json_object(dict(error), field="p115 effect error") if error is not None else None
        if status is not P115ExecutionEffectStatus.CONFIRMED and normalized_error is None:
            raise ValueError("non-confirmed effect completion requires an error")
        resolved_object_id, resolved_parent_cid = _resolved_identity(normalized_binding, normalized_readback)
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            action = _require_action_row(_action_row(uow, execution_id, action_id, for_update=True), action_id)
            terminal = status.terminal
            changed = uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_effects
                    SET status = :status, available_at = NOW(),
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        readback_json = CAST(:readback_json AS jsonb), readback_at = NOW(),
                        resolved_object_id = :resolved_object_id,
                        resolved_parent_cid = :resolved_parent_cid,
                        completion_mode = :completion_mode,
                        result_json = CAST(:result_json AS jsonb),
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = CASE WHEN :terminal THEN NOW() ELSE NULL END,
                        updated_at = NOW()
                    WHERE effect_key = :effect_key
                      AND status = 'awaiting_readback'
                      AND claim_token = :claim_token
                    RETURNING effect_key
                    """
                ),
                {
                    "effect_key": action["effect_key"],
                    "claim_token": effect_claim_token,
                    "status": status.value,
                    "readback_json": canonical_json(normalized_readback),
                    "resolved_object_id": resolved_object_id,
                    "resolved_parent_cid": resolved_parent_cid,
                    "completion_mode": "provider_applied" if status is P115ExecutionEffectStatus.CONFIRMED else None,
                    "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                    "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                    "terminal": terminal,
                },
            ).scalar_one_or_none()
            if changed is None:
                raise P115ExecutionStateError("stale effect claim cannot complete provider readback")

            if status is P115ExecutionEffectStatus.CONFIRMED:
                self._confirm_mapping(
                    uow,
                    execution_id,
                    action_id,
                    binding=normalized_binding,
                    readback=normalized_readback,
                )
            else:
                mapping_status = (
                    P115ExecutionActionStatus.INDETERMINATE
                    if status is P115ExecutionEffectStatus.INDETERMINATE
                    else P115ExecutionActionStatus.REJECTED
                )
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_organize_execution_actions
                        SET binding_json = CAST(:binding_json AS jsonb),
                            status = :status,
                            verification_status = :verification_status,
                            verification_json = CAST(:verification_json AS jsonb)
                        WHERE execution_id = :execution_id AND action_id = :action_id
                        """
                    ),
                    {
                        "execution_id": execution_id,
                        "action_id": action_id,
                        "binding_json": canonical_json(normalized_binding),
                        "status": mapping_status.value,
                        "verification_status": (
                            "unknown" if status is P115ExecutionEffectStatus.INDETERMINATE else "rejected"
                        ),
                        "verification_json": canonical_json(normalized_readback),
                    },
                )
                self._reject_unstarted_actions(uow, execution_id, normalized_error or {})
                execution_status = (
                    P115ExecutionStatus.INDETERMINATE
                    if status is P115ExecutionEffectStatus.INDETERMINATE
                    else P115ExecutionStatus.REJECTED
                )
                self._terminalize_execution(
                    uow,
                    execution_id,
                    owner,
                    status=execution_status,
                    error=normalized_error,
                )

            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type=f"effect_{status.value}",
                previous_status="awaiting_readback",
                new_status=status.value,
                attempt_no=int(action["attempts"]),
                claim_token=effect_claim_token,
                evidence={
                    "readback": normalized_readback,
                    "result": normalized_result,
                    "error": normalized_error,
                },
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded

        raise AssertionError("unreachable")

    def stop_before_request(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        error: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        return self._stop_execution(
            execution_id,
            action_id,
            owner,
            execution_status=P115ExecutionStatus.REJECTED,
            action_status=P115ExecutionActionStatus.REJECTED,
            verification_status="rejected",
            binding={},
            verification={},
            error=error,
            event_type="execution_precondition_rejected",
        )

    def _stop_execution(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        execution_status: P115ExecutionStatus,
        action_status: P115ExecutionActionStatus,
        verification_status: str,
        binding: Mapping[str, Any],
        verification: Mapping[str, Any],
        error: Mapping[str, Any],
        event_type: str,
    ) -> P115OrganizeExecution:
        normalized_error = normalize_json_object(dict(error), field="p115 execution stop error")
        normalized_binding = normalize_json_object(dict(binding), field="p115 action binding")
        normalized_verification = normalize_json_object(dict(verification), field="p115 action verification")
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            action = _require_action_row(_action_row(uow, execution_id, action_id, for_update=True), action_id)
            current_binding = dict(action["binding_json"] or {})
            current_verification = dict(action["verification_json"] or {})
            previous_effect_status = str(action["effect_status"])
            self._reject_unstarted_actions(uow, execution_id, normalized_error)
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_execution_actions
                    SET binding_json = CAST(:binding_json AS jsonb),
                        status = :status, verification_status = :verification_status,
                        verification_json = CAST(:verification_json AS jsonb),
                        disposition = 'blocked'
                    WHERE execution_id = :execution_id AND action_id = :action_id
                    """
                ),
                {
                    "execution_id": execution_id,
                    "action_id": action_id,
                    "binding_json": canonical_json(normalized_binding or current_binding),
                    "status": action_status.value,
                    "verification_status": verification_status,
                    "verification_json": canonical_json(normalized_verification or current_verification),
                },
            )
            self._terminalize_execution(
                uow,
                execution_id,
                owner,
                status=execution_status,
                error=normalized_error,
            )
            _append_event(
                uow,
                effect_key=str(action["effect_key"]),
                execution_id=execution_id,
                event_type=event_type,
                previous_status=previous_effect_status,
                new_status=str(
                    _require_action_row(
                        _action_row(uow, execution_id, action_id),
                        action_id,
                    )["effect_status"]
                ),
                evidence={
                    "binding": normalized_binding or current_binding,
                    "verification": normalized_verification or current_verification,
                    "error": normalized_error,
                    "external_mutations_executed": 0,
                },
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            uow.commit()
            return loaded

        raise AssertionError("unreachable")

    def finish(
        self,
        execution_id: UUID,
        owner: P115JobAttemptOwner,
        *,
        records: Sequence[Mapping[str, Any]],
        result: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        normalized_result = normalize_json_object(dict(result), field="p115 execution result")
        normalized_records = tuple(
            normalize_json_object(dict(record), field="p115 organize record") for record in records
        )
        with self._unit_of_work() as uow:
            execution = _require_execution_row(_execution_row(uow, execution_id, for_update=True), execution_id)
            _lock_owned_job_attempt(uow, execution, owner)
            incomplete = int(
                uow.session.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM p115_organize_execution_actions
                        WHERE execution_id = :execution_id AND status <> 'confirmed'
                        """
                    ),
                    {"execution_id": execution_id},
                )
                or 0
            )
            if incomplete:
                raise P115ExecutionStateError("execution cannot finish before every action is effective-confirmed")
            _upsert_organize_records(uow, normalized_records)
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_plan_executions
                    SET status = 'confirmed', result_json = CAST(:result_json AS jsonb),
                        error_json = NULL, finished_at = NOW(), updated_at = NOW()
                    WHERE execution_id = :execution_id AND status = 'running'
                    """
                ),
                {"execution_id": execution_id, "result_json": canonical_json(normalized_result)},
            )
            loaded = _require_execution(_load_execution(uow, execution_id), execution_id)
            _terminalize_p4(uow, owner, loaded)
            uow.commit()
            return loaded

        raise AssertionError("unreachable")

    def begin_reconciliation(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        request_hash: str,
        *,
        actor: str | None,
    ) -> P115ReconciliationReservation:
        try:
            with self._unit_of_work() as uow:
                _advisory_lock(uow, f"p115-reconciliation-intent:{approved_intent_id}")
                existing = _reconciliation_by_intent(uow, approved_intent_id, for_update=True)
                if existing is not None:
                    reconciliation = _reconciliation_projection(existing)
                    if reconciliation.execution_id != execution_id or reconciliation.request_hash != request_hash:
                        raise P115ExecutionConflict("approved reconciliation intent was used for another request")
                    execution = _require_execution(_load_execution(uow, execution_id), execution_id)
                    action = _require_action_projection(
                        _load_action_projection(uow, execution_id, reconciliation_action_id(uow, existing)),
                        reconciliation.effect_key,
                    )
                    return P115ReconciliationReservation(reconciliation, execution, action, False)

                execution_row = _require_execution_row(
                    _execution_row(uow, execution_id, for_update=True),
                    execution_id,
                )
                execution_status = P115ExecutionStatus(str(execution_row["status"]))
                if execution_status not in {
                    P115ExecutionStatus.INDETERMINATE,
                    P115ExecutionStatus.MANUAL_REVIEW,
                }:
                    raise P115ExecutionStateError("only indeterminate/manual-review execution can be reconciled")
                action = (
                    uow.session.execute(
                        text(
                            """
                            SELECT mapping.*, plan_action.kind, effect.status AS effect_status
                            FROM p115_organize_execution_actions AS mapping
                            JOIN p115_organize_plan_actions AS plan_action
                              ON plan_action.plan_id = mapping.plan_id
                             AND plan_action.action_index = mapping.action_index
                            JOIN p115_organize_effects AS effect
                              ON effect.effect_key = mapping.effect_key
                            WHERE mapping.execution_id = :execution_id
                              AND mapping.status IN ('indeterminate', 'manual_review')
                            ORDER BY mapping.action_index
                            LIMIT 1
                            FOR UPDATE OF mapping, effect
                            """
                        ),
                        {"execution_id": execution_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if action is None:
                    raise P115ExecutionStateError("execution has no action eligible for read-only reconciliation")
                reconciliation_id = uuid5(
                    NAMESPACE_URL,
                    f"etk:p115-organize-reconciliation:{execution_id}:{approved_intent_id}",
                )
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_organize_reconciliation_intents (
                            reconciliation_id, approved_intent_id, request_hash,
                            execution_id, action_index, effect_key, status, actor
                        ) VALUES (
                            :reconciliation_id, :approved_intent_id, :request_hash,
                            :execution_id, :action_index, :effect_key, 'running', :actor
                        )
                        """
                    ),
                    {
                        "reconciliation_id": reconciliation_id,
                        "approved_intent_id": approved_intent_id,
                        "request_hash": request_hash,
                        "execution_id": execution_id,
                        "action_index": int(action["action_index"]),
                        "effect_key": str(action["effect_key"]),
                        "actor": actor,
                    },
                )
                _append_event(
                    uow,
                    effect_key=str(action["effect_key"]),
                    execution_id=execution_id,
                    reconciliation_id=reconciliation_id,
                    event_type="reconciliation_started",
                    previous_status=str(action["effect_status"]),
                    new_status=str(action["effect_status"]),
                    actor=actor,
                    evidence={"external_mutations_executed": 0},
                )
                row = _require_reconciliation_row(
                    _reconciliation_row(uow, reconciliation_id),
                    reconciliation_id,
                )
                execution = _require_execution(_load_execution(uow, execution_id), execution_id)
                projected_action = _require_action_projection(
                    _load_action_projection(uow, execution_id, str(action["action_id"])),
                    str(action["effect_key"]),
                )
                reconciliation = _reconciliation_projection(row)
                uow.commit()
                return P115ReconciliationReservation(reconciliation, execution, projected_action, True)
        except IntegrityError as exc:
            if _constraint_name(exc) == "uq_p115_organize_reconciliation_active_effect":
                raise P115ExecutionBusy("another reconciliation is active for this effect") from exc
            raise
        raise AssertionError("unreachable")

    def complete_reconciliation(
        self,
        reconciliation_id: UUID,
        *,
        outcome: str,
        evidence: Mapping[str, Any],
        records: Sequence[Mapping[str, Any]],
    ) -> P115ReconciliationResult:
        if outcome not in {"applied", "absent", "conflict", "unknown"}:
            raise ValueError("unsupported reconciliation outcome")
        normalized_evidence = normalize_json_object(dict(evidence), field="p115 reconciliation evidence")
        normalized_records = tuple(
            normalize_json_object(dict(record), field="p115 organize record") for record in records
        )
        with self._unit_of_work() as uow:
            row = _require_reconciliation_row(
                _reconciliation_row(uow, reconciliation_id, for_update=True),
                reconciliation_id,
            )
            if str(row["status"]) == "completed":
                reconciliation = _reconciliation_projection(row)
                execution = _require_execution(
                    _load_execution(uow, reconciliation.execution_id),
                    reconciliation.execution_id,
                )
                return P115ReconciliationResult(reconciliation, execution)
            execution_id = UUID(str(row["execution_id"]))
            action_index = int(row["action_index"])
            mapping_status = "confirmed" if outcome == "applied" else "manual_review"
            verification_status = f"reconciled_{outcome}" if outcome in {"applied", "absent"} else outcome
            disposition = "reused_reconciled" if outcome == "applied" else "blocked"
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_execution_actions
                    SET status = :status, disposition = :disposition,
                        verification_status = :verification_status,
                        verification_json = CAST(:verification_json AS jsonb)
                    WHERE execution_id = :execution_id AND action_index = :action_index
                    """
                ),
                {
                    "execution_id": execution_id,
                    "action_index": action_index,
                    "status": mapping_status,
                    "disposition": disposition,
                    "verification_status": verification_status,
                    "verification_json": canonical_json(normalized_evidence),
                },
            )
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_reconciliation_intents
                    SET status = 'completed', outcome = :outcome,
                        evidence_json = CAST(:evidence_json AS jsonb), finished_at = NOW()
                    WHERE reconciliation_id = :reconciliation_id AND status = 'running'
                    """
                ),
                {
                    "reconciliation_id": reconciliation_id,
                    "outcome": outcome,
                    "evidence_json": canonical_json(normalized_evidence),
                },
            )
            remaining = int(
                uow.session.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM p115_organize_execution_actions
                        WHERE execution_id = :execution_id AND status <> 'confirmed'
                        """
                    ),
                    {"execution_id": execution_id},
                )
                or 0
            )
            if remaining == 0:
                _upsert_organize_records(uow, normalized_records)
                execution_status = "confirmed"
                result_json = canonical_json(
                    {
                        "external_result": "reconciled_readback_confirmed",
                        "external_mutations_executed": 0,
                        "record_count": len(normalized_records),
                    }
                )
                error_json = None
            else:
                execution_status = "manual_review"
                result_json = None
                error_json = canonical_json(
                    {
                        "type": "reconciliation_requires_manual_review",
                        "message": "只读对账未使整个 Plan 达到可确认终态",
                        "outcome": outcome,
                    }
                )
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_plan_executions
                    SET status = :status, result_json = CAST(:result_json AS jsonb),
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = COALESCE(finished_at, NOW()), updated_at = NOW()
                    WHERE execution_id = :execution_id
                    """
                ),
                {
                    "execution_id": execution_id,
                    "status": execution_status,
                    "result_json": result_json,
                    "error_json": error_json,
                },
            )
            _append_event(
                uow,
                effect_key=str(row["effect_key"]),
                execution_id=execution_id,
                reconciliation_id=reconciliation_id,
                event_type=f"reconciliation_{outcome}",
                previous_status=None,
                new_status=None,
                actor=row["actor"],
                evidence=normalized_evidence,
            )
            completed = _require_reconciliation_row(
                _reconciliation_row(uow, reconciliation_id),
                reconciliation_id,
            )
            execution = _require_execution(_load_execution(uow, execution_id), execution_id)
            reconciliation = _reconciliation_projection(completed)
            uow.commit()
            return P115ReconciliationResult(reconciliation, execution)

        raise AssertionError("unreachable")

    @staticmethod
    def _confirm_mapping(
        uow: SqlAlchemyUnitOfWork,
        execution_id: UUID,
        action_id: str,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
    ) -> None:
        uow.session.execute(
            text(
                """
                UPDATE p115_organize_execution_actions
                SET binding_json = CAST(:binding_json AS jsonb),
                    status = 'confirmed', verification_status = 'verified',
                    verification_json = CAST(:verification_json AS jsonb)
                WHERE execution_id = :execution_id AND action_id = :action_id
                """
            ),
            {
                "execution_id": execution_id,
                "action_id": action_id,
                "binding_json": canonical_json(binding),
                "verification_json": canonical_json(readback),
            },
        )

    @staticmethod
    def _mark_active_effects_indeterminate(
        uow: SqlAlchemyUnitOfWork,
        execution_id: UUID,
        error: Mapping[str, Any],
    ) -> int:
        rows = (
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_effects AS effect
                    SET status = 'indeterminate',
                        claimed_by = NULL, claim_token = NULL,
                        claimed_at = NULL, lease_expires_at = NULL,
                        error_json = CAST(:error_json AS jsonb),
                        finished_at = NOW(), updated_at = NOW()
                    FROM p115_organize_execution_actions AS mapping
                    WHERE mapping.execution_id = :execution_id
                      AND mapping.effect_key = effect.effect_key
                      AND effect.status IN ('claimed', 'requesting', 'awaiting_readback')
                    RETURNING effect.effect_key, effect.attempts
                    """
                ),
                {"execution_id": execution_id, "error_json": canonical_json(error)},
            )
            .mappings()
            .all()
        )
        for row in rows:
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_execution_actions
                    SET status = 'indeterminate', verification_status = 'unknown',
                        verification_json = CAST(:verification_json AS jsonb)
                    WHERE execution_id = :execution_id AND effect_key = :effect_key
                    """
                ),
                {
                    "execution_id": execution_id,
                    "effect_key": row["effect_key"],
                    "verification_json": canonical_json({"error": dict(error)}),
                },
            )
            _append_event(
                uow,
                effect_key=str(row["effect_key"]),
                execution_id=execution_id,
                event_type="orphaned_active_effect_indeterminate",
                previous_status="active",
                new_status="indeterminate",
                attempt_no=int(row["attempts"]),
                evidence={"error": dict(error)},
            )
        return len(rows)

    @staticmethod
    def _mark_finalization_gap_manual_review(
        uow: SqlAlchemyUnitOfWork,
        execution_id: UUID,
        error: Mapping[str, Any],
    ) -> None:
        action = (
            uow.session.execute(
                text(
                    """
                    SELECT mapping.action_index, mapping.effect_key, effect.status, effect.attempts
                    FROM p115_organize_execution_actions AS mapping
                    JOIN p115_organize_effects AS effect
                      ON effect.effect_key = mapping.effect_key
                    WHERE mapping.execution_id = :execution_id
                      AND mapping.status = 'confirmed'
                    ORDER BY mapping.action_index DESC
                    LIMIT 1
                    FOR UPDATE OF mapping, effect
                    """
                ),
                {"execution_id": execution_id},
            )
            .mappings()
            .one_or_none()
        )
        if action is None:
            raise P115ExecutionStateError("finalization recovery requires one confirmed execution action")
        uow.session.execute(
            text(
                """
                UPDATE p115_organize_execution_actions
                SET status = 'manual_review', verification_status = 'unknown',
                    disposition = 'blocked',
                    verification_json = CAST(:verification_json AS jsonb)
                WHERE execution_id = :execution_id AND action_index = :action_index
                """
            ),
            {
                "execution_id": execution_id,
                "action_index": int(action["action_index"]),
                "verification_json": canonical_json({"error": dict(error)}),
            },
        )
        _append_event(
            uow,
            effect_key=str(action["effect_key"]),
            execution_id=execution_id,
            event_type="orphaned_finalization_requires_reconciliation",
            previous_status=str(action["status"]),
            new_status=str(action["status"]),
            attempt_no=int(action["attempts"]) or None,
            evidence={"error": dict(error), "external_mutations_executed": 0},
        )

    @staticmethod
    def _reject_unstarted_actions(
        uow: SqlAlchemyUnitOfWork,
        execution_id: UUID,
        error: Mapping[str, Any],
    ) -> None:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT mapping.effect_key, effect.status, effect.attempts
                    FROM p115_organize_execution_actions AS mapping
                    JOIN p115_organize_effects AS effect
                      ON effect.effect_key = mapping.effect_key
                    WHERE mapping.execution_id = :execution_id
                      AND mapping.status = 'pending'
                    ORDER BY mapping.action_index
                    FOR UPDATE OF mapping, effect
                    """
                ),
                {"execution_id": execution_id},
            )
            .mappings()
            .all()
        )
        for row in rows:
            effect_status = P115ExecutionEffectStatus(str(row["status"]))
            if effect_status is P115ExecutionEffectStatus.PENDING:
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_organize_effects
                        SET status = 'retryable', available_at = NOW(),
                            error_json = CAST(:error_json AS jsonb), updated_at = NOW()
                        WHERE effect_key = :effect_key AND status = 'pending'
                        """
                    ),
                    {"effect_key": row["effect_key"], "error_json": canonical_json(error)},
                )
            uow.session.execute(
                text(
                    """
                    UPDATE p115_organize_execution_actions
                    SET status = 'rejected', verification_status = 'rejected',
                        verification_json = CAST(:verification_json AS jsonb)
                    WHERE execution_id = :execution_id AND effect_key = :effect_key
                      AND status = 'pending'
                    """
                ),
                {
                    "execution_id": execution_id,
                    "effect_key": row["effect_key"],
                    "verification_json": canonical_json({"error": dict(error)}),
                },
            )
            _append_event(
                uow,
                effect_key=str(row["effect_key"]),
                execution_id=execution_id,
                event_type="unstarted_effect_released",
                previous_status=effect_status.value,
                new_status=(
                    P115ExecutionEffectStatus.RETRYABLE.value
                    if effect_status is P115ExecutionEffectStatus.PENDING
                    else effect_status.value
                ),
                attempt_no=int(row["attempts"]) or None,
                evidence={"error": dict(error), "external_mutations_executed": 0},
            )

    @staticmethod
    def _terminalize_execution(
        uow: SqlAlchemyUnitOfWork,
        execution_id: UUID,
        owner: P115JobAttemptOwner,
        *,
        status: P115ExecutionStatus,
        error: Mapping[str, Any] | None,
    ) -> None:
        if not status.terminal or status is P115ExecutionStatus.CONFIRMED:
            raise ValueError("failure terminalization requires a non-confirmed terminal status")
        normalized_error = dict(error or {})
        uow.session.execute(
            text(
                """
                UPDATE p115_organize_plan_executions
                SET status = :status, result_json = NULL,
                    error_json = CAST(:error_json AS jsonb),
                    finished_at = NOW(), updated_at = NOW()
                WHERE execution_id = :execution_id AND status = 'running'
                """
            ),
            {
                "execution_id": execution_id,
                "status": status.value,
                "error_json": canonical_json(normalized_error),
            },
        )
        execution = _require_execution(_load_execution(uow, execution_id), execution_id)
        _terminalize_p4(uow, owner, execution)


def _execution_by_intent(uow: SqlAlchemyUnitOfWork, intent: str) -> RowMapping | None:
    return (
        uow.session.execute(
            text(
                """
                SELECT execution_id, plan_id, request_hash
                FROM p115_organize_plan_executions
                WHERE approved_intent_id = :intent
                FOR UPDATE
                """
            ),
            {"intent": intent},
        )
        .mappings()
        .one_or_none()
    )


def _execution_row(
    uow: SqlAlchemyUnitOfWork,
    execution_id: UUID,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text("SELECT * FROM p115_organize_plan_executions WHERE execution_id = :execution_id" + suffix),
            {"execution_id": execution_id},
        )
        .mappings()
        .one_or_none()
    )


def _action_row(
    uow: SqlAlchemyUnitOfWork,
    execution_id: UUID,
    action_id: str,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE OF mapping, effect" if for_update else ""
    return (
        uow.session.execute(
            text(
                """
                SELECT mapping.*, plan_action.kind,
                       effect.status AS effect_status, effect.attempts,
                       effect.claim_token AS effect_claim_token
                FROM p115_organize_execution_actions AS mapping
                JOIN p115_organize_plan_actions AS plan_action
                  ON plan_action.plan_id = mapping.plan_id
                 AND plan_action.action_index = mapping.action_index
                JOIN p115_organize_effects AS effect
                  ON effect.effect_key = mapping.effect_key
                WHERE mapping.execution_id = :execution_id
                  AND mapping.action_id = :action_id
                """
                + suffix
            ),
            {"execution_id": execution_id, "action_id": action_id},
        )
        .mappings()
        .one_or_none()
    )


def _effect_row(
    uow: SqlAlchemyUnitOfWork,
    effect_key: str,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text("SELECT * FROM p115_organize_effects WHERE effect_key = :effect_key" + suffix),
            {"effect_key": effect_key},
        )
        .mappings()
        .one_or_none()
    )


def _load_execution(uow: SqlAlchemyUnitOfWork, execution_id: UUID) -> P115OrganizeExecution | None:
    row = _execution_row(uow, execution_id)
    if row is None:
        return None
    action_rows = (
        uow.session.execute(
            text(
                """
                SELECT mapping.*, plan_action.kind,
                       effect.effect_id, effect.request_hash AS effect_request_hash,
                       effect.request_json, effect.status AS effect_status,
                       effect.attempts, effect.provider_backend AS effect_provider_backend,
                       effect.available_at, effect.last_attempt_at,
                       effect.sponsor_execution_id, effect.sponsor_action_index,
                       effect.sponsor_account_id, effect.claimed_by, effect.claim_token,
                       effect.claimed_at, effect.lease_expires_at,
                       effect.provider_call_started_at, effect.receipt_json,
                       effect.receipt_persisted_at, effect.readback_json,
                       effect.readback_at, effect.resolved_object_id,
                       effect.resolved_parent_cid, effect.completion_mode,
                       effect.resolution_status, effect.result_json AS effect_result_json,
                       effect.error_json AS effect_error_json, effect.finished_at AS effect_finished_at,
                       effect.created_at AS effect_created_at, effect.updated_at AS effect_updated_at
                FROM p115_organize_execution_actions AS mapping
                JOIN p115_organize_plan_actions AS plan_action
                  ON plan_action.plan_id = mapping.plan_id
                 AND plan_action.action_index = mapping.action_index
                JOIN p115_organize_effects AS effect
                  ON effect.effect_key = mapping.effect_key
                WHERE mapping.execution_id = :execution_id
                ORDER BY mapping.action_index
                """
            ),
            {"execution_id": execution_id},
        )
        .mappings()
        .all()
    )
    return P115OrganizeExecution(
        execution_id=UUID(str(row["execution_id"])),
        plan_id=str(row["plan_id"]),
        approved_intent_id=str(row["approved_intent_id"]),
        request_hash=str(row["request_hash"]),
        status=P115ExecutionStatus(str(row["status"])),
        account_id=str(row["account_id"]),
        provider_backend=str(row["provider_backend"]),
        preflight_hash=str(row["preflight_hash"]),
        preflight=dict(row["preflight_json"] or {}),
        actions=tuple(_action_projection(action) for action in action_rows),
        job_id=UUID(str(row["job_id"])),
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error=dict(row["error_json"]) if row["error_json"] is not None else None,
        created_at=_datetime(row["created_at"]),
        updated_at=_datetime(row["updated_at"]),
        started_at=_optional_datetime(row["started_at"]),
        finished_at=_optional_datetime(row["finished_at"]),
        schema_version=int(row["schema_version"]),
    )


def _action_projection(row: RowMapping) -> P115ExecutionAction:
    effect = P115ExecutionEffect(
        effect_id=UUID(str(row["effect_id"])),
        effect_key=str(row["effect_key"]),
        request_hash=str(row["effect_request_hash"]),
        request=dict(row["request_json"] or {}),
        kind=str(row["kind"]),
        status=P115ExecutionEffectStatus(str(row["effect_status"])),
        attempts=int(row["attempts"]),
        provider_backend=str(row["effect_provider_backend"]),
        available_at=_datetime(row["available_at"]),
        last_attempt_at=_optional_datetime(row["last_attempt_at"]),
        sponsor_execution_id=_optional_uuid(row["sponsor_execution_id"]),
        sponsor_action_index=(int(row["sponsor_action_index"]) if row["sponsor_action_index"] is not None else None),
        sponsor_account_id=(str(row["sponsor_account_id"]) if row["sponsor_account_id"] is not None else None),
        claimed_by=str(row["claimed_by"]) if row["claimed_by"] is not None else None,
        claim_token=_optional_uuid(row["claim_token"]),
        claimed_at=_optional_datetime(row["claimed_at"]),
        lease_expires_at=_optional_datetime(row["lease_expires_at"]),
        provider_call_started_at=_optional_datetime(row["provider_call_started_at"]),
        receipt=dict(row["receipt_json"]) if row["receipt_json"] is not None else None,
        receipt_persisted_at=_optional_datetime(row["receipt_persisted_at"]),
        readback=dict(row["readback_json"]) if row["readback_json"] is not None else None,
        readback_at=_optional_datetime(row["readback_at"]),
        resolved_object_id=(str(row["resolved_object_id"]) if row["resolved_object_id"] is not None else None),
        resolved_parent_cid=(str(row["resolved_parent_cid"]) if row["resolved_parent_cid"] is not None else None),
        completion_mode=str(row["completion_mode"]) if row["completion_mode"] is not None else None,
        resolution_status=(str(row["resolution_status"]) if row["resolution_status"] is not None else None),
        result=(dict(row["effect_result_json"]) if row["effect_result_json"] is not None else None),
        error=(dict(row["effect_error_json"]) if row["effect_error_json"] is not None else None),
        finished_at=_optional_datetime(row["effect_finished_at"]),
        created_at=_optional_datetime(row["effect_created_at"]),
        updated_at=_optional_datetime(row["effect_updated_at"]),
    )
    return P115ExecutionAction(
        plan_id=str(row["plan_id"]),
        action_index=int(row["action_index"]),
        action_id=str(row["action_id"]),
        kind=str(row["kind"]),
        effect_key=str(row["effect_key"]),
        binding=dict(row["binding_json"] or {}),
        disposition=str(row["disposition"]),
        status=P115ExecutionActionStatus(str(row["status"])),
        verification_status=str(row["verification_status"]),
        verification=dict(row["verification_json"] or {}),
        effect=effect,
    )


def _load_action_projection(
    uow: SqlAlchemyUnitOfWork,
    execution_id: UUID,
    action_id: str,
) -> P115ExecutionAction | None:
    execution = _load_execution(uow, execution_id)
    if execution is None:
        return None
    return next((action for action in execution.actions if action.action_id == action_id), None)


def _reconciliation_by_intent(
    uow: SqlAlchemyUnitOfWork,
    approved_intent_id: str,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text(
                "SELECT * FROM p115_organize_reconciliation_intents "
                "WHERE approved_intent_id = :approved_intent_id" + suffix
            ),
            {"approved_intent_id": approved_intent_id},
        )
        .mappings()
        .one_or_none()
    )


def _reconciliation_row(
    uow: SqlAlchemyUnitOfWork,
    reconciliation_id: UUID,
    *,
    for_update: bool = False,
) -> RowMapping | None:
    suffix = " FOR UPDATE" if for_update else ""
    return (
        uow.session.execute(
            text(
                "SELECT * FROM p115_organize_reconciliation_intents "
                "WHERE reconciliation_id = :reconciliation_id" + suffix
            ),
            {"reconciliation_id": reconciliation_id},
        )
        .mappings()
        .one_or_none()
    )


def reconciliation_action_id(uow: SqlAlchemyUnitOfWork, row: Mapping[Any, Any]) -> str:
    action_id = uow.session.scalar(
        text(
            """
            SELECT action_id
            FROM p115_organize_execution_actions
            WHERE execution_id = :execution_id AND action_index = :action_index
            """
        ),
        {
            "execution_id": row["execution_id"],
            "action_index": row["action_index"],
        },
    )
    if action_id is None:
        raise P115ExecutionStateError("reconciliation points to a missing execution action")
    return str(action_id)


def _reconciliation_projection(row: Mapping[Any, Any]) -> P115Reconciliation:
    return P115Reconciliation(
        reconciliation_id=UUID(str(row["reconciliation_id"])),
        approved_intent_id=str(row["approved_intent_id"]),
        request_hash=str(row["request_hash"]),
        execution_id=UUID(str(row["execution_id"])),
        action_index=int(row["action_index"]),
        effect_key=str(row["effect_key"]),
        status=str(row["status"]),
        outcome=str(row["outcome"]) if row["outcome"] is not None else None,
        actor=str(row["actor"]) if row["actor"] is not None else None,
        evidence=dict(row["evidence_json"]) if row["evidence_json"] is not None else None,
        created_at=_datetime(row["created_at"]),
        finished_at=_optional_datetime(row["finished_at"]),
    )


def _append_event(
    uow: SqlAlchemyUnitOfWork,
    *,
    effect_key: str,
    execution_id: UUID | None,
    event_type: str,
    previous_status: str | None,
    new_status: str | None,
    evidence: Mapping[str, Any],
    attempt_no: int | None = None,
    claim_token: UUID | None = None,
    reconciliation_id: UUID | None = None,
    actor: object = None,
    reason: str | None = None,
) -> None:
    effect = _effect_row(uow, effect_key, for_update=True)
    if effect is None:
        raise P115ExecutionStateError(f"effect {effect_key} is missing while appending an event")
    event_seq = int(
        uow.session.scalar(
            text(
                """
                SELECT COALESCE(MAX(event_seq), 0) + 1
                FROM p115_organize_effect_events
                WHERE effect_key = :effect_key
                """
            ),
            {"effect_key": effect_key},
        )
        or 1
    )
    normalized_evidence = normalize_json_object(dict(evidence), field="p115 effect event evidence")
    uow.session.execute(
        text(
            """
            INSERT INTO p115_organize_effect_events (
                event_id, effect_key, execution_id, event_type, event_seq,
                previous_status, new_status, attempt_no, claim_token,
                reconciliation_id, actor, reason, evidence_json
            ) VALUES (
                :event_id, :effect_key, :execution_id, :event_type, :event_seq,
                :previous_status, :new_status, :attempt_no, :claim_token,
                :reconciliation_id, :actor, :reason, CAST(:evidence_json AS jsonb)
            )
            """
        ),
        {
            "event_id": uuid4(),
            "effect_key": effect_key,
            "execution_id": execution_id,
            "event_type": event_type,
            "event_seq": event_seq,
            "previous_status": previous_status,
            "new_status": new_status,
            "attempt_no": attempt_no,
            "claim_token": claim_token,
            "reconciliation_id": reconciliation_id,
            "actor": str(actor) if actor is not None else None,
            "reason": reason,
            "evidence_json": canonical_json(normalized_evidence),
        },
    )


def _lock_owned_job_attempt(
    uow: SqlAlchemyUnitOfWork,
    execution: Mapping[Any, Any],
    owner: P115JobAttemptOwner,
) -> tuple[RowMapping, RowMapping]:
    if UUID(str(execution["job_id"])) != owner.job_id:
        raise P115ExecutionStateError("execution is not owned by the supplied P4 Job")
    repository = SqlAlchemyJobRepository(uow.session)
    job = repository.get_job_row(owner.job_id, for_update=True)
    attempt = repository.get_attempt_row(owner.attempt_id, for_update=True)
    if job is None or attempt is None:
        raise P115ExecutionStateError("P4 Job attempt ownership is missing")
    if UUID(str(attempt["job_id"])) != owner.job_id:
        raise P115ExecutionStateError("P4 attempt does not belong to the execution Job")
    if str(attempt["worker_id"] or "") != owner.worker_id:
        raise P115ExecutionStateError("P4 attempt belongs to another Worker")
    if int(job["current_attempt"]) != int(attempt["attempt_no"]):
        raise P115ExecutionStateError("P4 attempt is no longer current")
    if AttemptStatus(str(attempt["status"])) not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
        raise P115ExecutionStateError("P4 attempt is not active")
    return job, attempt


def _terminalize_p4(
    uow: SqlAlchemyUnitOfWork,
    owner: P115JobAttemptOwner,
    execution: P115OrganizeExecution,
) -> None:
    repository = SqlAlchemyJobRepository(uow.session)
    job = repository.get_job_row(owner.job_id, for_update=True)
    attempt = repository.get_attempt_row(owner.attempt_id, for_update=True)
    if job is None or attempt is None:
        raise P115ExecutionStateError("P4 terminalization target is missing")
    if str(attempt["worker_id"] or "") != owner.worker_id:
        raise P115ExecutionStateError("P4 terminalization token belongs to another Worker")
    attempt_status = AttemptStatus(str(attempt["status"]))
    if attempt_status.terminal:
        expected = (
            AttemptStatus.SUCCEEDED if execution.status is P115ExecutionStatus.CONFIRMED else AttemptStatus.FAILED
        )
        if attempt_status is not expected:
            raise P115ExecutionStateError("P4 terminal status conflicts with P6 execution status")
        return
    if attempt_status not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
        raise P115ExecutionStateError("P4 attempt cannot be terminalized from its current state")
    if execution.status is P115ExecutionStatus.CONFIRMED:
        result = {
            "ok": True,
            "status": "succeeded",
            "message": "115 整理计划执行完成",
            "details": execution.to_dict(),
        }
        repository.mark_attempt_terminal(
            owner.attempt_id,
            AttemptStatus.SUCCEEDED,
            retryable=False,
            error_type=None,
            error_message=None,
            error_details_json=None,
            result_json=canonical_json(result),
        )
        repository.mark_job_terminal(
            owner.job_id,
            JobStatus.SUCCEEDED,
            result_json=canonical_json(result),
        )
    else:
        error = execution.error or {
            "type": execution.status.value,
            "message": "115 整理计划未达到确认终态",
        }
        result = {
            "ok": False,
            "status": "failed",
            "message": str(error.get("message") or "115 整理计划失败"),
            "details": execution.to_dict(),
        }
        repository.mark_attempt_terminal(
            owner.attempt_id,
            AttemptStatus.FAILED,
            retryable=False,
            error_type=str(error.get("type") or execution.status.value),
            error_message=str(error.get("message") or "115 organize execution failed"),
            error_details_json=canonical_json(result),
            result_json=None,
        )
        repository.mark_job_terminal(
            owner.job_id,
            JobStatus.FAILED,
            error_summary_json=canonical_json(error),
        )


def _upsert_organize_records(
    uow: SqlAlchemyUnitOfWork,
    records: Sequence[Mapping[str, Any]],
) -> None:
    for record in records:
        pick_code = record.get("pick_code")
        file_id = str(record.get("file_id") or "")
        if not file_id:
            raise P115ExecutionStateError("organize record is missing file_id")
        if pick_code:
            uow.session.execute(
                text(
                    """
                    DELETE FROM p115_organize_records
                    WHERE pick_code = :pick_code AND file_id <> :file_id
                    """
                ),
                {"pick_code": pick_code, "file_id": file_id},
            )
        uow.session.execute(
            text(
                """
                INSERT INTO p115_organize_records (
                    file_id, pick_code, original_name, renamed_name, status,
                    fail_reason, tmdb_id, media_type, target_cid,
                    category_name, processed_at, season_number
                ) VALUES (
                    :file_id, :pick_code, :original_name, :renamed_name, :status,
                    :fail_reason, :tmdb_id, :media_type, :target_cid,
                    :category_name, NOW(), :season_number
                )
                ON CONFLICT (file_id) DO UPDATE SET
                    pick_code = EXCLUDED.pick_code,
                    original_name = EXCLUDED.original_name,
                    renamed_name = EXCLUDED.renamed_name,
                    status = EXCLUDED.status,
                    fail_reason = EXCLUDED.fail_reason,
                    tmdb_id = EXCLUDED.tmdb_id,
                    media_type = EXCLUDED.media_type,
                    target_cid = EXCLUDED.target_cid,
                    category_name = EXCLUDED.category_name,
                    processed_at = NOW(),
                    season_number = EXCLUDED.season_number
                """
            ),
            dict(record),
        )


def _resolved_identity(
    binding: Mapping[str, Any],
    readback: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    entry = readback.get("entry")
    entry_mapping = entry if isinstance(entry, Mapping) else {}
    object_id = str(
        binding.get("resolved_entry_id") or entry_mapping.get("id") or entry_mapping.get("file_id") or ""
    ).strip()
    parent_id = str(binding.get("parent_id") or entry_mapping.get("parent_id") or "").strip()
    return object_id or None, parent_id or None


def _advisory_lock(uow: SqlAlchemyUnitOfWork, identity: str) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
        {"identity": identity},
    )


def _json_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(value)).encode()).hexdigest()


def _constraint_name(exc: IntegrityError) -> str:
    original = getattr(exc, "orig", None)
    diagnostic = getattr(original, "diag", None)
    return str(getattr(diagnostic, "constraint_name", "") or "")


def _require_execution(
    execution: P115OrganizeExecution | None,
    execution_id: UUID,
) -> P115OrganizeExecution:
    if execution is None:
        raise P115ExecutionNotFound(f"organize execution {execution_id} was not found")
    return execution


def _require_execution_row(row: RowMapping | None, execution_id: UUID) -> RowMapping:
    if row is None:
        raise P115ExecutionNotFound(f"organize execution {execution_id} was not found")
    return row


def _require_action_row(row: RowMapping | None, action_id: str) -> RowMapping:
    if row is None:
        raise P115ExecutionStateError(f"organize execution action {action_id} was not found")
    return row


def _require_action_projection(
    action: P115ExecutionAction | None,
    identity: str,
) -> P115ExecutionAction:
    if action is None:
        raise P115ExecutionStateError(f"organize execution action {identity} was not found")
    return action


def _require_reconciliation_row(row: RowMapping | None, reconciliation_id: UUID) -> RowMapping:
    if row is None:
        raise P115ExecutionNotFound(f"reconciliation {reconciliation_id} was not found")
    return row


def _datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("database timestamp is not a datetime")
    return value


def _optional_datetime(value: Any) -> datetime | None:
    return _datetime(value) if value is not None else None


def _optional_uuid(value: Any) -> UUID | None:
    return UUID(str(value)) if value is not None else None


__all__ = ["SqlAlchemyP115OrganizeExecutionRepository"]
