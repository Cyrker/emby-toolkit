from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from etk.domains.p115.organize_execution import (
    P115ExecutionAction,
    P115ExecutionActionStatus,
    P115ExecutionEffect,
    P115ExecutionEffectStatus,
    P115ExecutionJob,
    P115ExecutionPreflightError,
    P115ExecutionReservation,
    P115ExecutionStart,
    P115ExecutionStatus,
    P115JobAttemptOwner,
    P115OrganizeExecution,
    P115OrganizeExecutionService,
    P115PreparedAction,
    P115ProviderNoEffectError,
    P115Reconciliation,
    P115ReconciliationReservation,
    P115ReconciliationResult,
    P115RemoteEntry,
    _ActionInspection,
    _classify_provider_outcome,
    effect_request_document,
    effect_request_hash,
)
from etk.domains.p115.organize_plan import (
    P115OrganizePlan,
    P115PlanAction,
    P115PlanCommitPreconditions,
    P115PlanningSupportSnapshot,
)

FIXED_NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _action(
    *,
    index: int,
    kind: str,
    subject: Mapping[str, Any],
    destination: Mapping[str, Any],
    preconditions: Mapping[str, Any],
    dependencies: Sequence[str] = (),
) -> P115PlanAction:
    action = P115PlanAction(
        action_index=index,
        action_id=_hash_json({"action": index, "kind": kind, "destination": destination}),
        kind=kind,
        subject=dict(subject),
        destination=dict(destination),
        dependencies=tuple(dependencies),
        preconditions={"account_id": "115001", **dict(preconditions)},
        effect_key="0" * 64,
        decision_basis={"sorting_rule_id": "movies"},
        risk={"remote_mutation": True, "requires_readback": True},
        destructive=False,
        expected_postcondition={"authoritative_readback": True},
    )
    return replace(action, effect_key=_hash_json(effect_request_document(action)))


def _plan(*, ensure_only: bool = False) -> P115OrganizePlan:
    ensure = _action(
        index=0,
        kind="ENSURE_DIRECTORY",
        subject={"parent_cid": "target"},
        destination={
            "parent_cid": "target",
            "target_root_cid": "target",
            "name": "Movie",
            "path": "电影/Movie",
        },
        preconditions={},
    )
    actions = [ensure]
    if not ensure_only:
        source = {
            "source_id": "file-1",
            "source_parent_id": "source",
            "source_name": "Original.mkv",
            "source_sha1": "A" * 40,
            "source_size": 1024,
            "source_raw_hash": "raw-file-1",
            "destination_must_be_absent": True,
        }
        move = _action(
            index=1,
            kind="MOVE",
            subject={"file_id": "file-1", "current_name": "Original.mkv"},
            destination={
                "parent_action_id": ensure.action_id,
                "target_root_cid": "target",
                "intermediate_name": "Original.mkv",
                "path": "电影/Movie/Original.mkv",
            },
            preconditions=source,
            dependencies=(ensure.action_id,),
        )
        rename = _action(
            index=2,
            kind="RENAME",
            subject={
                "file_id": "file-1",
                "current_name": "Original.mkv",
                "origin_parent_id": "source",
                "origin_name": "Original.mkv",
            },
            destination={
                "parent_action_id": ensure.action_id,
                "target_root_cid": "target",
                "name": "Movie.mkv",
                "path": "电影/Movie/Movie.mkv",
            },
            preconditions={**source, "source_parent_action_id": ensure.action_id},
            dependencies=(move.action_id,),
        )
        actions.extend((move, rename))
    return P115OrganizePlan(
        plan_id="00000000-0000-5000-8000-000000000201" if ensure_only else "00000000-0000-5000-8000-000000000202",
        request_intent_id="plan-intent",
        request_hash="1" * 64,
        input_fingerprint="2" * 64,
        status="ready",
        account_id="115001",
        source_root_cid="source",
        source_root_name="待整理",
        sorting_rules_revision="3" * 64,
        rename_config_revision="4" * 64,
        episode_regex_revision="5" * 64,
        policy_revision="6" * 64,
        runtime_config_revision="7" * 64,
        config_snapshot={"sorting_rules": [{"id": "movies", "name": "电影"}]},
        source_snapshot=(
            {
                "id": "file-1",
                "parent_id": "source",
                "name": "Original.mkv",
                "entry_kind": "file",
                "sha1": "A" * 40,
                "pick_code": "pc-file-1",
                "size": 1024,
                "raw_hash": "raw-file-1",
            },
        ),
        provider_evidence={"complete": True},
        recognition_evidence={},
        warnings=(),
        errors=(),
        risk_summary={"external_writes_executed": 0},
        actions=tuple(actions),
        created_at=FIXED_NOW,
        expires_at=FIXED_NOW + timedelta(hours=1),
    )


class FakePlanRepository:
    def __init__(self, plan: P115OrganizePlan, events: list[str]) -> None:
        self.plan = plan
        self.events = events
        self.raise_current = False

    def get(self, plan_id: str) -> P115OrganizePlan | None:
        self.events.append(f"plan:get:{plan_id}")
        return self.plan if plan_id == self.plan.plan_id else None

    def load_support_snapshot(self, sha1s: Sequence[str]) -> P115PlanningSupportSnapshot:
        del sha1s
        raise AssertionError("execution must not rebuild planning support")

    def save(
        self,
        plan: P115OrganizePlan,
        *,
        preconditions: P115PlanCommitPreconditions,
    ) -> P115OrganizePlan:
        del plan, preconditions
        raise AssertionError("execution must not rewrite the immutable Plan")

    def assert_current(self, plan: P115OrganizePlan) -> None:
        assert plan is self.plan
        self.events.append("plan:assert_current")
        if self.raise_current:
            raise RuntimeError("fixture revision drift")


class FakeMutationClient:
    def __init__(self, events: list[str], *, ensure_exists: bool = False) -> None:
        self.events = events
        self._backend = "openapi"
        self.entries: dict[str, list[P115RemoteEntry]] = {
            "target": [],
            "source": [
                P115RemoteEntry(
                    id="file-1",
                    parent_id="source",
                    name="Original.mkv",
                    entry_kind="file",
                    sha1="A" * 40,
                    pick_code="pc-file-1",
                    size=1024,
                    raw_hash="raw-file-1",
                )
            ],
        }
        if ensure_exists:
            self.entries["target"].append(
                P115RemoteEntry(
                    id="directory-1",
                    parent_id="target",
                    name="Movie",
                    entry_kind="directory",
                )
            )
            self.entries["directory-1"] = []
        self.mutations: list[str] = []

    @property
    def backend(self) -> str:
        return self._backend

    def list_children(self, parent_id: str) -> Sequence[P115RemoteEntry]:
        self.events.append(f"provider:list:{parent_id}")
        return tuple(self.entries.get(parent_id, ()))

    def ensure_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        self.events.append("provider:mutate:ENSURE_DIRECTORY")
        self.mutations.append("ENSURE_DIRECTORY")
        entry = P115RemoteEntry(
            id="directory-1",
            parent_id=parent_id,
            name=name,
            entry_kind="directory",
        )
        self.entries.setdefault(parent_id, []).append(entry)
        self.entries.setdefault(entry.id, [])
        return {"state": True, "data": {"file_id": entry.id}}

    def move(self, file_id: str, parent_id: str) -> Mapping[str, Any]:
        self.events.append("provider:mutate:MOVE")
        self.mutations.append("MOVE")
        entry = self._pop(file_id)
        moved = replace(entry, parent_id=parent_id)
        self.entries.setdefault(parent_id, []).append(moved)
        return {"state": True}

    def rename(self, file_id: str, name: str) -> Mapping[str, Any]:
        self.events.append("provider:mutate:RENAME")
        self.mutations.append("RENAME")
        entry = self._pop(file_id)
        self.entries.setdefault(entry.parent_id, []).append(replace(entry, name=name))
        return {"state": True}

    def _pop(self, entry_id: str) -> P115RemoteEntry:
        for values in self.entries.values():
            for index, entry in enumerate(values):
                if entry.id == entry_id:
                    return values.pop(index)
        raise AssertionError(f"missing fixture entry {entry_id}")


class FakeMutationGateway:
    def __init__(self, client: FakeMutationClient, events: list[str]) -> None:
        self.client = client
        self.events = events
        self.calls: list[tuple[str, str | None]] = []

    def open(
        self,
        *,
        expected_account_id: str,
        expected_backend: str | None = None,
    ) -> FakeMutationClient | None:
        self.events.append(f"gateway:open:{expected_backend or 'select'}")
        self.calls.append((expected_account_id, expected_backend))
        if expected_account_id != "115001":
            return None
        if expected_backend is not None and expected_backend != self.client.backend:
            return None
        return self.client


class FakeExecutionRepository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.execution: P115OrganizeExecution | None = None
        self.reconciliation: P115Reconciliation | None = None

    def find_by_intent(self, approved_intent_id: str) -> P115OrganizeExecution | None:
        self.events.append(f"repository:find:{approved_intent_id}")
        if self.execution and self.execution.approved_intent_id == approved_intent_id:
            return self.execution
        return None

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
        self.events.append("repository:reserve")
        prepared_by_id = {value.action_id: value for value in prepared_actions}
        actions: list[P115ExecutionAction] = []
        for action in plan.actions:
            prepared = prepared_by_id[action.action_id]
            confirmed = prepared.state == "confirmed"
            effect = P115ExecutionEffect(
                effect_id=uuid5(NAMESPACE_URL, f"effect:{action.effect_key}"),
                effect_key=action.effect_key,
                request_hash=effect_request_hash(action),
                request=effect_request_document(action),
                kind=action.kind,
                status=(P115ExecutionEffectStatus.CONFIRMED if confirmed else P115ExecutionEffectStatus.PENDING),
                attempts=0,
                provider_backend=provider_backend,
                available_at=FIXED_NOW,
                resolved_object_id=(str(prepared.binding.get("resolved_entry_id")) if confirmed else None),
                resolved_parent_cid=(str(prepared.binding.get("parent_id")) if confirmed else None),
                completion_mode="observed_preexisting" if confirmed else None,
                readback=dict(prepared.readback) if confirmed else None,
                readback_at=FIXED_NOW if confirmed else None,
                finished_at=FIXED_NOW if confirmed else None,
                created_at=FIXED_NOW,
                updated_at=FIXED_NOW,
            )
            actions.append(
                P115ExecutionAction(
                    plan_id=plan.plan_id,
                    action_index=action.action_index,
                    action_id=action.action_id,
                    kind=action.kind,
                    effect_key=action.effect_key,
                    binding=dict(prepared.binding),
                    disposition="observed_preexisting" if confirmed else "pending",
                    status=(P115ExecutionActionStatus.CONFIRMED if confirmed else P115ExecutionActionStatus.PENDING),
                    verification_status="verified" if confirmed else "pending",
                    verification=dict(prepared.readback),
                    effect=effect,
                )
            )
        self.execution = P115OrganizeExecution(
            execution_id=execution_id,
            plan_id=plan.plan_id,
            approved_intent_id=approved_intent_id,
            request_hash=request_hash,
            status=P115ExecutionStatus.PREPARED,
            account_id=plan.account_id,
            provider_backend=provider_backend,
            preflight_hash=_hash_json(dict(preflight)),
            preflight=dict(preflight),
            actions=tuple(actions),
            job_id=job.job_id,
            result=None,
            error=None,
            created_at=FIXED_NOW,
            updated_at=FIXED_NOW,
        )
        return P115ExecutionReservation(self.execution, True)

    def get(self, execution_id: UUID) -> P115OrganizeExecution | None:
        return self.execution if self.execution and self.execution.execution_id == execution_id else None

    def recover_orphaned(self, execution_id: UUID) -> P115OrganizeExecution | None:
        return self.get(execution_id)

    def start(self, execution_id: UUID, owner: P115JobAttemptOwner) -> P115ExecutionStart:
        del owner
        self.events.append("repository:start")
        execution = self._require(execution_id)
        if execution.status.terminal:
            return P115ExecutionStart(execution, True)
        self.execution = replace(execution, status=P115ExecutionStatus.RUNNING, started_at=FIXED_NOW)
        return P115ExecutionStart(self.execution)

    def observe_confirmed(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        del owner
        self.events.append(f"repository:observe:{action_id}")
        return self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                binding=dict(binding),
                verification_status="verified",
                verification=dict(readback),
            ),
        )

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
        del owner
        self.events.append(f"repository:drift:{action_id}")
        self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                status=P115ExecutionActionStatus.MANUAL_REVIEW,
                disposition="blocked",
                binding=dict(binding),
                verification_status="postcondition_drift",
                verification=dict(verification),
            ),
        )
        assert self.execution is not None
        self.execution = replace(
            self.execution,
            status=P115ExecutionStatus.MANUAL_REVIEW,
            error=dict(error),
            finished_at=FIXED_NOW,
        )
        return self.execution

    def claim_effect(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        lease_seconds: int,
    ) -> P115ExecutionAction | None:
        del owner, lease_seconds
        self.events.append(f"repository:claim:{action_id}")
        token = uuid4()
        execution = self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                effect=replace(
                    action.effect,
                    status=P115ExecutionEffectStatus.CLAIMED,
                    attempts=action.effect.attempts + 1,
                    claim_token=token,
                    claimed_by="worker-p6",
                    claimed_at=FIXED_NOW,
                    lease_expires_at=FIXED_NOW + timedelta(minutes=5),
                ),
            ),
        )
        return next(action for action in execution.actions if action.action_id == action_id)

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
        del owner, effect_claim_token
        self.events.append(f"repository:confirm_without_request:{action_id}")
        return self._confirm(execution_id, action_id, binding=binding, readback=readback, mode="observed_preexisting")

    def mark_requesting(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        binding: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        del owner, effect_claim_token
        self.events.append(f"repository:requesting:{action_id}")
        return self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                binding=dict(binding),
                effect=replace(
                    action.effect,
                    status=P115ExecutionEffectStatus.REQUESTING,
                    provider_call_started_at=FIXED_NOW,
                ),
            ),
        )

    def persist_receipt(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        effect_claim_token: UUID,
        *,
        receipt: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        del owner, effect_claim_token
        self.events.append(f"repository:receipt:{action_id}")
        return self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                effect=replace(
                    action.effect,
                    status=P115ExecutionEffectStatus.AWAITING_READBACK,
                    receipt=dict(receipt),
                    receipt_persisted_at=FIXED_NOW,
                ),
            ),
        )

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
        del owner, effect_claim_token
        self.events.append(f"repository:complete:{action_id}:{status.value}")
        if status is P115ExecutionEffectStatus.CONFIRMED:
            return self._confirm(execution_id, action_id, binding=binding, readback=readback, mode="provider_applied")
        mapping_status = (
            P115ExecutionActionStatus.INDETERMINATE
            if status is P115ExecutionEffectStatus.INDETERMINATE
            else P115ExecutionActionStatus.REJECTED
        )
        self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                status=mapping_status,
                verification_status="unknown"
                if mapping_status is P115ExecutionActionStatus.INDETERMINATE
                else "rejected",
                verification=dict(readback),
                effect=replace(
                    action.effect,
                    status=status,
                    claim_token=None,
                    claimed_by=None,
                    claimed_at=None,
                    lease_expires_at=None,
                    readback=dict(readback),
                    readback_at=FIXED_NOW,
                    result=dict(result) if result else None,
                    error=dict(error) if error else None,
                    finished_at=FIXED_NOW if status.terminal else None,
                ),
            ),
        )
        assert self.execution is not None
        self.execution = replace(
            self.execution,
            status=(
                P115ExecutionStatus.INDETERMINATE
                if status is P115ExecutionEffectStatus.INDETERMINATE
                else P115ExecutionStatus.REJECTED
            ),
            error=dict(error) if error else None,
            finished_at=FIXED_NOW,
        )
        return self.execution

    def stop_before_request(
        self,
        execution_id: UUID,
        action_id: str,
        owner: P115JobAttemptOwner,
        *,
        error: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        del owner
        self.events.append(f"repository:stop:{action_id}")
        self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                status=P115ExecutionActionStatus.REJECTED,
                verification_status="rejected",
                effect=(
                    replace(action.effect, status=P115ExecutionEffectStatus.RETRYABLE, error=dict(error))
                    if action.effect.status is P115ExecutionEffectStatus.PENDING
                    else action.effect
                ),
            ),
        )
        assert self.execution is not None
        self.execution = replace(
            self.execution,
            status=P115ExecutionStatus.REJECTED,
            error=dict(error),
            finished_at=FIXED_NOW,
        )
        return self.execution

    def finish(
        self,
        execution_id: UUID,
        owner: P115JobAttemptOwner,
        *,
        records: Sequence[Mapping[str, Any]],
        result: Mapping[str, Any],
    ) -> P115OrganizeExecution:
        del owner, records
        self.events.append("repository:finish")
        execution = self._require(execution_id)
        assert all(action.status is P115ExecutionActionStatus.CONFIRMED for action in execution.actions)
        self.execution = replace(
            execution,
            status=P115ExecutionStatus.CONFIRMED,
            result=dict(result),
            finished_at=FIXED_NOW,
        )
        return self.execution

    def begin_reconciliation(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        request_hash: str,
        *,
        actor: str | None,
    ) -> P115ReconciliationReservation:
        execution = self._require(execution_id)
        action = next(
            value
            for value in execution.actions
            if value.status in {P115ExecutionActionStatus.INDETERMINATE, P115ExecutionActionStatus.MANUAL_REVIEW}
        )
        self.events.append("repository:reconciliation:start")
        self.reconciliation = P115Reconciliation(
            reconciliation_id=uuid4(),
            approved_intent_id=approved_intent_id,
            request_hash=request_hash,
            execution_id=execution_id,
            action_index=action.action_index,
            effect_key=action.effect_key,
            status="running",
            outcome=None,
            actor=actor,
            evidence=None,
            created_at=FIXED_NOW,
            finished_at=None,
        )
        return P115ReconciliationReservation(self.reconciliation, execution, action, True)

    def complete_reconciliation(
        self,
        reconciliation_id: UUID,
        *,
        outcome: str,
        evidence: Mapping[str, Any],
        records: Sequence[Mapping[str, Any]],
    ) -> P115ReconciliationResult:
        del records
        assert self.reconciliation is not None and self.reconciliation.reconciliation_id == reconciliation_id
        self.events.append(f"repository:reconciliation:{outcome}")
        execution = self._require(self.reconciliation.execution_id)
        action_index = self.reconciliation.action_index
        updated_actions = list(execution.actions)
        action = updated_actions[action_index]
        updated_actions[action_index] = replace(
            action,
            status=(
                P115ExecutionActionStatus.CONFIRMED if outcome == "applied" else P115ExecutionActionStatus.MANUAL_REVIEW
            ),
            verification_status=f"reconciled_{outcome}" if outcome in {"applied", "absent"} else outcome,
            verification=dict(evidence),
        )
        status = (
            P115ExecutionStatus.CONFIRMED
            if all(value.status is P115ExecutionActionStatus.CONFIRMED for value in updated_actions)
            else P115ExecutionStatus.MANUAL_REVIEW
        )
        self.execution = replace(execution, actions=tuple(updated_actions), status=status, finished_at=FIXED_NOW)
        self.reconciliation = replace(
            self.reconciliation,
            status="completed",
            outcome=outcome,
            evidence=dict(evidence),
            finished_at=FIXED_NOW,
        )
        return P115ReconciliationResult(self.reconciliation, self.execution)

    def _confirm(
        self,
        execution_id: UUID,
        action_id: str,
        *,
        binding: Mapping[str, Any],
        readback: Mapping[str, Any],
        mode: str,
    ) -> P115OrganizeExecution:
        return self._update_action(
            execution_id,
            action_id,
            lambda action: replace(
                action,
                status=P115ExecutionActionStatus.CONFIRMED,
                verification_status="verified",
                binding=dict(binding),
                verification=dict(readback),
                effect=replace(
                    action.effect,
                    status=P115ExecutionEffectStatus.CONFIRMED,
                    claim_token=None,
                    claimed_by=None,
                    claimed_at=None,
                    lease_expires_at=None,
                    readback=dict(readback),
                    readback_at=FIXED_NOW,
                    resolved_object_id=str(binding.get("resolved_entry_id") or "") or None,
                    resolved_parent_cid=str(binding.get("parent_id") or "") or None,
                    completion_mode=mode,
                    finished_at=FIXED_NOW,
                ),
            ),
        )

    def _update_action(
        self,
        execution_id: UUID,
        action_id: str,
        update,
    ) -> P115OrganizeExecution:
        execution = self._require(execution_id)
        actions = list(execution.actions)
        index = next(index for index, action in enumerate(actions) if action.action_id == action_id)
        actions[index] = update(actions[index])
        self.execution = replace(execution, actions=tuple(actions), updated_at=FIXED_NOW)
        return self.execution

    def _require(self, execution_id: UUID) -> P115OrganizeExecution:
        if self.execution is None or self.execution.execution_id != execution_id:
            raise AssertionError("fixture execution not found")
        return self.execution


class FakeProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        return None


def _service(
    plan: P115OrganizePlan,
    *,
    ensure_exists: bool = False,
) -> tuple[
    P115OrganizeExecutionService,
    FakePlanRepository,
    FakeExecutionRepository,
    FakeMutationClient,
    FakeMutationGateway,
    list[str],
]:
    events: list[str] = []
    plans = FakePlanRepository(plan, events)
    repository = FakeExecutionRepository(events)
    client = FakeMutationClient(events, ensure_exists=ensure_exists)
    gateway = FakeMutationGateway(client, events)
    return (
        P115OrganizeExecutionService(
            plans=plans,
            repository=repository,
            mutations=gateway,
            clock=lambda: FIXED_NOW,
        ),
        plans,
        repository,
        client,
        gateway,
        events,
    )


def test_prepare_completes_whole_plan_preflight_before_atomic_reservation() -> None:
    plan = _plan()
    service, _, repository, client, gateway, events = _service(plan)

    reservation = service.prepare(plan.plan_id, "approved-execution-intent")

    assert reservation.created
    assert repository.execution is not None
    assert len(repository.execution.actions) == 3
    assert client.mutations == []
    assert gateway.calls == [("115001", None)]
    assert events.count("plan:assert_current") == 2
    assert events.index("repository:reserve") > max(
        index for index, event in enumerate(events) if event.startswith("provider:list:")
    )
    assert events.index("repository:reserve") > max(
        index for index, event in enumerate(events) if event == "plan:assert_current"
    )


def test_failed_whole_plan_preflight_exposes_no_execution_or_effect_for_claim() -> None:
    plan = _plan(ensure_only=True)
    service, _, repository, client, _, events = _service(plan)
    client.entries["target"].append(
        P115RemoteEntry(
            id="foreign-file",
            parent_id="target",
            name="Movie",
            entry_kind="file",
            sha1="B" * 40,
            size=1024,
        )
    )

    with pytest.raises(P115ExecutionPreflightError, match="同名文件占用"):
        service.prepare(plan.plan_id, "approved-execution-intent")

    assert repository.execution is None
    assert "repository:reserve" not in events
    assert client.mutations == []


def test_execute_uses_fixed_order_and_commits_requesting_and_receipt_before_provider_readback() -> None:
    plan = _plan()
    service, _, repository, client, gateway, events = _service(plan)
    reservation = service.prepare(plan.plan_id, "approved-execution-intent")
    events.clear()
    progress = FakeProgress()

    result = service.execute(
        reservation.execution.execution_id,
        progress,
        job_id=reservation.execution.job_id,
        attempt_id=uuid4(),
        worker_id="worker-p6",
    )

    assert result.status is P115ExecutionStatus.CONFIRMED
    assert result.p4_terminalized
    assert client.mutations == ["ENSURE_DIRECTORY", "MOVE", "RENAME"]
    assert gateway.calls[-1] == ("115001", "openapi")
    for action in plan.actions:
        requesting = events.index(f"repository:requesting:{action.action_id}")
        mutation = events.index(f"provider:mutate:{action.kind}")
        receipt = events.index(f"repository:receipt:{action.action_id}")
        complete = events.index(f"repository:complete:{action.action_id}:confirmed")
        assert requesting < mutation < receipt < complete
        assert any(
            receipt < index < complete and event.startswith("provider:list:") for index, event in enumerate(events)
        )
    assert events[-1] == "repository:finish"
    assert progress.updates[-1][0] == 100


@pytest.mark.parametrize(
    ("response", "error", "no_effect", "readback_state", "expected"),
    [
        ({"state": True}, None, False, "confirmed", P115ExecutionEffectStatus.CONFIRMED),
        (None, P115ProviderNoEffectError("not sent"), True, "not_applied", P115ExecutionEffectStatus.RETRYABLE),
        ({"state": False}, None, False, "not_applied", P115ExecutionEffectStatus.REJECTED),
        (None, RuntimeError("lost response"), False, "not_applied", P115ExecutionEffectStatus.INDETERMINATE),
        (None, RuntimeError("lost response"), False, "confirmed", P115ExecutionEffectStatus.CONFIRMED),
    ],
)
def test_provider_outcomes_require_authoritative_readback(
    response: Mapping[str, Any] | None,
    error: Exception | None,
    no_effect: bool,
    readback_state: str,
    expected: P115ExecutionEffectStatus,
) -> None:
    status, _, _ = _classify_provider_outcome(
        response,
        error,
        no_effect_proved=no_effect,
        readback=_ActionInspection(
            readback_state,
            {"parent_id": "target"},
            {"state": readback_state},
        ),
    )
    assert status is expected


def test_historical_confirmed_effect_requires_fresh_readback_and_drift_only_changes_overlay() -> None:
    plan = _plan(ensure_only=True)
    service, _, repository, client, _, _ = _service(plan, ensure_exists=True)
    reservation = service.prepare(plan.plan_id, "approved-execution-intent")
    assert reservation.execution.actions[0].effect.status is P115ExecutionEffectStatus.CONFIRMED
    client.entries["target"].clear()

    result = service.execute(
        reservation.execution.execution_id,
        FakeProgress(),
        job_id=reservation.execution.job_id,
        attempt_id=uuid4(),
        worker_id="worker-p6",
    )

    assert result.status is P115ExecutionStatus.MANUAL_REVIEW
    assert client.mutations == []
    assert repository.execution is not None
    assert repository.execution.actions[0].status is P115ExecutionActionStatus.MANUAL_REVIEW
    assert repository.execution.actions[0].effect.status is P115ExecutionEffectStatus.CONFIRMED


def test_reconciliation_is_read_only_and_never_rewrites_the_global_terminal_effect() -> None:
    plan = _plan(ensure_only=True)
    service, _, repository, client, _, _ = _service(plan, ensure_exists=True)
    reservation = service.prepare(plan.plan_id, "approved-execution-intent")
    client.entries["target"].clear()
    drifted = service.execute(
        reservation.execution.execution_id,
        FakeProgress(),
        job_id=reservation.execution.job_id,
        attempt_id=uuid4(),
        worker_id="worker-p6",
    )
    assert drifted.status is P115ExecutionStatus.MANUAL_REVIEW
    client.entries["target"].append(
        P115RemoteEntry(
            id="directory-1",
            parent_id="target",
            name="Movie",
            entry_kind="directory",
        )
    )
    mutations_before = list(client.mutations)

    result = service.reconcile(
        reservation.execution.execution_id,
        "approved-reconciliation-intent",
        actor="fixture-admin",
    )

    assert result.reconciliation.outcome == "applied"
    assert result.execution.status is P115ExecutionStatus.CONFIRMED
    assert client.mutations == mutations_before == []
    assert repository.execution is not None
    assert repository.execution.actions[0].effect.status is P115ExecutionEffectStatus.CONFIRMED
    assert result.to_dict()["external_mutations_executed"] == 0
