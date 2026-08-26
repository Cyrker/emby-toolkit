from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from etk.domains.p115 import (
    P115EpisodeRegexSnapshot,
    P115ExecutionConflict,
    P115ExecutionEffectStatus,
    P115ExecutionStateError,
    P115ExecutionStatus,
    P115OrganizePlan,
    P115PlanAction,
    P115PlanCommitPreconditions,
    P115RenameConfigSnapshot,
    P115SortingRulesSnapshot,
    SqlAlchemyP115OrganizeExecutionRepository,
    SqlAlchemyP115OrganizePlanRepository,
)
from etk.domains.p115.organize_execution import (
    P115JobAttemptOwner,
    P115PreparedAction,
    effect_request_document,
    execution_job_for,
)
from etk.domains.p115.organize_plan import (
    P115_PLANNING_POLICY_KEYS,
    policy_revision,
    runtime_config_revision,
)
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import AttemptStatus, JobStatus, PostgresJobStore

REPO_ROOT = Path(__file__).resolve().parents[3]


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _action(*, plan_id: str, name: str = "Movie", effect_key: str | None = None) -> P115PlanAction:
    action = P115PlanAction(
        action_index=0,
        action_id=_hash_json({"plan_id": plan_id, "action": 0}),
        kind="ENSURE_DIRECTORY",
        subject={"parent_cid": "target"},
        destination={
            "parent_cid": "target",
            "target_root_cid": "target",
            "name": name,
            "path": f"电影/{name}",
        },
        dependencies=(),
        preconditions={"account_id": "115001"},
        effect_key="0" * 64,
        decision_basis={"sorting_rule_id": "movies"},
        risk={"remote_mutation": True, "requires_readback": True},
        destructive=False,
        expected_postcondition={"visible_child_name": name},
    )
    return replace(action, effect_key=effect_key or _hash_json(effect_request_document(action)))


def _plan(
    runtime: DatabaseRuntime,
    *,
    plan_id: str,
    name: str = "Movie",
    effect_key: str | None = None,
    nested: bool = False,
) -> P115OrganizePlan:
    sorting = P115SortingRulesSnapshot((), "")
    rename = P115RenameConfigSnapshot({})
    episode = P115EpisodeRegexSnapshot(())
    current_policy = {key: None for key in P115_PLANNING_POLICY_KEYS}
    action = _action(plan_id=plan_id, name=name, effect_key=effect_key)
    actions = [action]
    if nested:
        nested_action = P115PlanAction(
            action_index=1,
            action_id=_hash_json({"plan_id": plan_id, "action": 1}),
            kind="ENSURE_DIRECTORY",
            subject={"parent_action_id": action.action_id},
            destination={
                "parent_action_id": action.action_id,
                "target_root_cid": "target",
                "name": "Season 01",
                "path": f"电影/{name}/Season 01",
            },
            dependencies=(action.action_id,),
            preconditions={"account_id": "115001"},
            effect_key="0" * 64,
            decision_basis={"sorting_rule_id": "movies"},
            risk={"remote_mutation": True, "requires_readback": True},
            destructive=False,
            expected_postcondition={"visible_child_name": "Season 01"},
        )
        actions.append(
            replace(
                nested_action,
                effect_key=_hash_json(effect_request_document(nested_action)),
            )
        )
    now = datetime.now(UTC)
    plan = P115OrganizePlan(
        plan_id=plan_id,
        request_intent_id=f"plan-intent:{plan_id}",
        request_hash=_hash_json({"plan_id": plan_id, "name": name}),
        input_fingerprint=_hash_json({"source": plan_id}),
        status="ready",
        account_id="115001",
        source_root_cid="source",
        source_root_name="待整理",
        sorting_rules_revision=sorting.revision,
        rename_config_revision=rename.revision,
        episode_regex_revision=episode.revision,
        policy_revision=policy_revision(current_policy),
        runtime_config_revision=runtime_config_revision({}),
        config_snapshot={"sorting_rules": []},
        source_snapshot=(),
        provider_evidence={"complete": True},
        recognition_evidence={},
        warnings=(),
        errors=(),
        risk_summary={"external_writes_executed": 0, "requires_human_review": True},
        actions=tuple(actions),
        created_at=now,
        expires_at=now + timedelta(hours=1),
    )
    repository = SqlAlchemyP115OrganizePlanRepository(runtime.unit_of_work)
    return repository.save(
        plan,
        preconditions=P115PlanCommitPreconditions(
            account_id="115001",
            sorting_rules_revision=sorting.revision,
            rename_config_revision=rename.revision,
            episode_regex_revision=episode.revision,
            policy_revision=plan.policy_revision,
            runtime_config_revision=plan.runtime_config_revision,
            media_info_revisions={},
        ),
    )


def _prepared(action: P115PlanAction, *, confirmed: bool = False) -> P115PreparedAction:
    entry = (
        {
            "id": "directory-1",
            "parent_id": "target",
            "name": str(action.destination["name"]),
            "entry_kind": "directory",
            "sha1": "",
            "pick_code": "",
            "size": 0,
            "updated_at": "",
            "raw_hash": "",
        }
        if confirmed
        else None
    )
    binding = {
        "parent_id": str(action.destination.get("parent_cid") or "") or None,
        "parent_action_id": str(action.destination.get("parent_action_id") or ""),
        "target_root_cid": "target",
        "target_path": str(action.destination["path"]),
        "target_name": str(action.destination["name"]),
        "resolved_entry_id": entry["id"] if entry else None,
        "source": None,
        "provider_request_required": not confirmed,
    }
    readback = {
        "state": "confirmed" if confirmed else "not_applied",
        "entry": entry,
        "entries": [],
        "message": "",
        "error": None,
    }
    return P115PreparedAction(action.action_id, "confirmed" if confirmed else "needs_request", binding, readback)


def _reserve(
    runtime: DatabaseRuntime,
    plan: P115OrganizePlan,
    *,
    approved_intent_id: str = "approved-execution-1",
    confirmed: bool = False,
):
    execution_id = UUID(plan.plan_id)
    request_hash = _hash_json(
        {
            "plan_id": plan.plan_id,
            "approved_execution_intent_id": approved_intent_id,
        }
    )
    job = execution_job_for(execution_id, requested_by_user_id=None, trace_id="trace-p6")
    repository = SqlAlchemyP115OrganizeExecutionRepository(runtime.unit_of_work)
    reservation = repository.reserve(
        plan,
        execution_id=execution_id,
        approved_intent_id=approved_intent_id,
        request_hash=request_hash,
        provider_backend="openapi",
        preflight={
            "account_id": "115001",
            "provider_backend": "openapi",
            "external_mutations_executed": 0,
        },
        prepared_actions=tuple(_prepared(action, confirmed=confirmed) for action in plan.actions),
        job=job,
    )
    return repository, reservation, job, request_hash


def _start(
    runtime: DatabaseRuntime,
    repository: SqlAlchemyP115OrganizeExecutionRepository,
    execution_id: UUID,
    job,
    *,
    worker_id: str = "worker-p6",
) -> tuple[PostgresJobStore, P115JobAttemptOwner]:
    store = PostgresJobStore(runtime.unit_of_work)
    claim = store.claim_attempt(
        job_id=job.job_id,
        attempt_id=job.attempt_id,
        worker_id=worker_id,
        lease_seconds=300,
    )
    assert claim.claimed
    store.start_attempt(
        job_id=job.job_id,
        attempt_id=job.attempt_id,
        worker_id=worker_id,
        lease_seconds=300,
    )
    owner = P115JobAttemptOwner(job.job_id, job.attempt_id, worker_id)
    started = repository.start(execution_id, owner)
    assert started.execution.status is P115ExecutionStatus.RUNNING
    return store, owner


def test_postgres18_execution_schema_has_ledgers_fencing_and_active_reconciliation(
    p6_runtime: DatabaseRuntime,
) -> None:
    inspector = inspect(p6_runtime.engine)
    assert {
        "p115_organize_plan_executions",
        "p115_organize_effects",
        "p115_organize_execution_actions",
        "p115_organize_reconciliation_intents",
        "p115_organize_effect_events",
    }.issubset(inspector.get_table_names())
    assert {item["name"] for item in inspector.get_check_constraints("p115_organize_effects")} >= {
        "ck_p115_organize_effects_claim",
        "ck_p115_organize_effects_confirmed_evidence",
        "ck_p115_organize_effects_request_started",
        "ck_p115_organize_effects_terminal_finished",
    }
    assert {item["name"] for item in inspector.get_indexes("p115_organize_effects")} >= {
        "ix_p115_organize_effects_active_lease",
        "ix_p115_organize_effects_claimable",
    }
    assert {item["name"] for item in inspector.get_indexes("p115_organize_reconciliation_intents")} >= {
        "uq_p115_organize_reconciliation_active_effect"
    }
    assert {item["name"] for item in inspector.get_unique_constraints("p115_organize_plan_actions")} >= {
        "uq_p115_organize_plan_action_effect"
    }
    sponsor = next(
        item
        for item in inspector.get_foreign_keys("p115_organize_effects")
        if item["name"] == "fk_p115_organize_effects_sponsor_action"
    )
    assert sponsor["constrained_columns"] == ["sponsor_execution_id", "sponsor_action_index"]
    assert sponsor["referred_table"] == "p115_organize_execution_actions"


def test_reservation_atomically_creates_execution_job_attempt_outbox_and_effect(
    p6_runtime: DatabaseRuntime,
) -> None:
    plan = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000101",
    )
    repository, reservation, job, request_hash = _reserve(p6_runtime, plan)

    assert reservation.created
    assert reservation.execution.job_id == job.job_id
    with p6_runtime.engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM p115_organize_plan_executions),
                    (SELECT COUNT(*) FROM p115_organize_execution_actions),
                    (SELECT COUNT(*) FROM p115_organize_effects),
                    (SELECT COUNT(*) FROM p115_organize_effect_events),
                    (SELECT COUNT(*) FROM job_runs),
                    (SELECT COUNT(*) FROM job_attempts),
                    (SELECT COUNT(*) FROM outbox_events)
                """
            )
        ).one()
    assert counts == (1, 1, 1, 1, 1, 1, 1)

    replay = repository.reserve(
        plan,
        execution_id=reservation.execution.execution_id,
        approved_intent_id="approved-execution-1",
        request_hash=request_hash,
        provider_backend="openapi",
        preflight=reservation.execution.preflight,
        prepared_actions=(_prepared(plan.actions[0]),),
        job=job,
    )
    assert not replay.created
    assert replay.execution.execution_id == reservation.execution.execution_id

    with pytest.raises(P115ExecutionConflict, match="already been consumed"):
        repository.reserve(
            plan,
            execution_id=uuid4(),
            approved_intent_id="approved-execution-2",
            request_hash="f" * 64,
            provider_backend="openapi",
            preflight={"safe": True},
            prepared_actions=(_prepared(plan.actions[0]),),
            job=execution_job_for(uuid4(), requested_by_user_id=None, trace_id=None),
        )


def test_effect_claim_fencing_and_p4_p6_terminal_state_commit_together(
    p6_runtime: DatabaseRuntime,
) -> None:
    plan = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000102",
    )
    repository, reservation, job, _ = _reserve(p6_runtime, plan)
    _, owner = _start(p6_runtime, repository, reservation.execution.execution_id, job)
    action_id = plan.actions[0].action_id

    claimed = repository.claim_effect(
        reservation.execution.execution_id,
        action_id,
        owner,
        lease_seconds=300,
    )
    assert claimed is not None and claimed.effect.claim_token is not None
    token = claimed.effect.claim_token
    with pytest.raises(P115ExecutionStateError, match="stale effect claim"):
        repository.mark_requesting(
            reservation.execution.execution_id,
            action_id,
            owner,
            uuid4(),
            binding=claimed.binding,
        )

    repository.mark_requesting(
        reservation.execution.execution_id,
        action_id,
        owner,
        token,
        binding=claimed.binding,
    )
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT status FROM p115_organize_effects")) == "requesting"
        assert connection.scalar(text("SELECT provider_call_started_at IS NOT NULL FROM p115_organize_effects"))

    repository.persist_receipt(
        reservation.execution.execution_id,
        action_id,
        owner,
        token,
        receipt={"received_response": True, "response": {"state": True}},
    )
    binding = dict(claimed.binding)
    binding["resolved_entry_id"] = "directory-1"
    readback = {
        "state": "confirmed",
        "entry": {
            "id": "directory-1",
            "parent_id": "target",
            "name": "Movie",
            "entry_kind": "directory",
        },
        "entries": [],
        "message": "",
        "error": None,
    }
    completed = repository.complete_effect(
        reservation.execution.execution_id,
        action_id,
        owner,
        token,
        status=P115ExecutionEffectStatus.CONFIRMED,
        binding=binding,
        readback=readback,
        result={"external_result": "authoritative_readback_confirmed"},
        error=None,
    )
    assert completed.status is P115ExecutionStatus.RUNNING

    finished = repository.finish(
        reservation.execution.execution_id,
        owner,
        records=(
            {
                "file_id": "file-1",
                "pick_code": "pc-file-1",
                "original_name": "Movie.mkv",
                "renamed_name": "Movie.mkv",
                "status": "success",
                "fail_reason": None,
                "tmdb_id": "1",
                "media_type": "movie",
                "target_cid": "target",
                "category_name": "电影",
                "season_number": None,
            },
        ),
        result={"confirmed_actions": 1},
    )
    assert finished.status is P115ExecutionStatus.CONFIRMED
    with p6_runtime.engine.connect() as connection:
        terminal = connection.execute(
            text(
                """
                SELECT execution.status, effect.status, mapping.status,
                       job.status, attempt.status
                FROM p115_organize_plan_executions AS execution
                JOIN p115_organize_execution_actions AS mapping
                  ON mapping.execution_id = execution.execution_id
                JOIN p115_organize_effects AS effect ON effect.effect_key = mapping.effect_key
                JOIN job_runs AS job ON job.id = execution.job_id
                JOIN job_attempts AS attempt ON attempt.job_id = job.id
                """
            )
        ).one()
        assert terminal == (
            "confirmed",
            "confirmed",
            "confirmed",
            JobStatus.SUCCEEDED.value,
            AttemptStatus.SUCCEEDED.value,
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_records")) == 1


def test_effect_claim_respects_the_immutable_action_dependency_gate(
    p6_runtime: DatabaseRuntime,
) -> None:
    plan = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000109",
        nested=True,
    )
    repository, reservation, job, _ = _reserve(p6_runtime, plan)
    _, owner = _start(p6_runtime, repository, reservation.execution.execution_id, job)
    first, second = plan.actions

    blocked = repository.claim_effect(
        reservation.execution.execution_id,
        second.action_id,
        owner,
        lease_seconds=300,
    )
    assert blocked is None

    claimed = repository.claim_effect(
        reservation.execution.execution_id,
        first.action_id,
        owner,
        lease_seconds=300,
    )
    assert claimed is not None and claimed.effect.claim_token is not None
    binding = dict(claimed.binding)
    binding["resolved_entry_id"] = "directory-1"
    readback = {
        "state": "confirmed",
        "entry": {
            "id": "directory-1",
            "parent_id": "target",
            "name": "Movie",
            "entry_kind": "directory",
        },
        "entries": [],
        "message": "",
        "error": None,
    }
    repository.confirm_without_request(
        reservation.execution.execution_id,
        first.action_id,
        owner,
        claimed.effect.claim_token,
        binding=binding,
        readback=readback,
    )
    unblocked = repository.claim_effect(
        reservation.execution.execution_id,
        second.action_id,
        owner,
        lease_seconds=300,
    )
    assert unblocked is not None and unblocked.effect.status is P115ExecutionEffectStatus.CLAIMED


def test_pre_request_stop_releases_the_current_pending_effect_for_a_new_plan(
    p6_runtime: DatabaseRuntime,
) -> None:
    plan = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000103",
    )
    repository, reservation, job, _ = _reserve(p6_runtime, plan)
    _, owner = _start(p6_runtime, repository, reservation.execution.execution_id, job)

    stopped = repository.stop_before_request(
        reservation.execution.execution_id,
        plan.actions[0].action_id,
        owner,
        error={"type": "preflight_drift", "message": "fixture drift"},
    )
    assert stopped.status is P115ExecutionStatus.REJECTED
    assert stopped.actions[0].status.value == "rejected"
    assert stopped.actions[0].effect.status is P115ExecutionEffectStatus.RETRYABLE
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT status FROM job_runs")) == JobStatus.FAILED.value
        assert connection.scalar(text("SELECT status FROM job_attempts")) == AttemptStatus.FAILED.value

    replanned = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000108",
    )
    reopened_repository, reopened, reopened_job, _ = _reserve(
        p6_runtime,
        replanned,
        approved_intent_id="approved-execution-retry",
    )
    assert reopened.execution.actions[0].disposition == "reopened"
    assert reopened.execution.actions[0].effect.status is P115ExecutionEffectStatus.RETRYABLE
    _, reopened_owner = _start(
        p6_runtime,
        reopened_repository,
        reopened.execution.execution_id,
        reopened_job,
        worker_id="worker-p6-retry",
    )
    claimed = reopened_repository.claim_effect(
        reopened.execution.execution_id,
        replanned.actions[0].action_id,
        reopened_owner,
        lease_seconds=300,
    )
    assert claimed is not None and claimed.effect.status is P115ExecutionEffectStatus.CLAIMED


def test_requesting_crash_recovers_indeterminate_and_reconciliation_never_rewrites_global_effect(
    p6_runtime: DatabaseRuntime,
) -> None:
    plan = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000104",
    )
    repository, reservation, job, _ = _reserve(p6_runtime, plan)
    store, owner = _start(p6_runtime, repository, reservation.execution.execution_id, job)
    claimed = repository.claim_effect(
        reservation.execution.execution_id,
        plan.actions[0].action_id,
        owner,
        lease_seconds=300,
    )
    assert claimed is not None and claimed.effect.claim_token is not None
    repository.mark_requesting(
        reservation.execution.execution_id,
        plan.actions[0].action_id,
        owner,
        claimed.effect.claim_token,
        binding=claimed.binding,
    )
    store.fail_attempt(
        job_id=job.job_id,
        attempt_id=job.attempt_id,
        worker_id=owner.worker_id,
        error_type="fixture_crash",
        error_message="worker crashed",
        retryable=False,
    )

    recovered = repository.recover_orphaned(reservation.execution.execution_id)
    assert recovered is not None and recovered.status is P115ExecutionStatus.INDETERMINATE
    assert recovered.actions[0].effect.status is P115ExecutionEffectStatus.INDETERMINATE
    reconciliation = repository.begin_reconciliation(
        recovered.execution_id,
        "approved-reconciliation-1",
        "a" * 64,
        actor="fixture-admin",
    )
    result = repository.complete_reconciliation(
        reconciliation.reconciliation.reconciliation_id,
        outcome="absent",
        evidence={"readback": {"state": "not_applied"}, "external_mutations_executed": 0},
        records=(),
    )
    assert result.execution.status is P115ExecutionStatus.MANUAL_REVIEW
    assert result.execution.actions[0].effect.status is P115ExecutionEffectStatus.INDETERMINATE
    replay = repository.begin_reconciliation(
        recovered.execution_id,
        "approved-reconciliation-1",
        "a" * 64,
        actor="fixture-admin",
    )
    assert replay.reconciliation.status == "completed"
    assert not replay.created


def test_finalization_crash_uses_overlay_reconciliation_and_preserves_confirmed_effect(
    p6_runtime: DatabaseRuntime,
) -> None:
    plan = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000105",
    )
    repository, reservation, job, _ = _reserve(p6_runtime, plan, confirmed=True)
    store, owner = _start(p6_runtime, repository, reservation.execution.execution_id, job)
    store.fail_attempt(
        job_id=job.job_id,
        attempt_id=job.attempt_id,
        worker_id=owner.worker_id,
        error_type="fixture_finalization_crash",
        error_message="worker crashed before record commit",
        retryable=False,
    )

    recovered = repository.recover_orphaned(reservation.execution.execution_id)
    assert recovered is not None and recovered.status is P115ExecutionStatus.MANUAL_REVIEW
    assert recovered.actions[0].status.value == "manual_review"
    assert recovered.actions[0].effect.status is P115ExecutionEffectStatus.CONFIRMED
    reconciliation = repository.begin_reconciliation(
        recovered.execution_id,
        "approved-reconciliation-finalization",
        "b" * 64,
        actor="fixture-admin",
    )
    result = repository.complete_reconciliation(
        reconciliation.reconciliation.reconciliation_id,
        outcome="applied",
        evidence={"readback": {"state": "confirmed"}, "external_mutations_executed": 0},
        records=(),
    )
    assert result.execution.status is P115ExecutionStatus.CONFIRMED
    assert result.execution.actions[0].effect.status is P115ExecutionEffectStatus.CONFIRMED


def test_global_effect_key_rejects_a_different_semantic_request(
    p6_runtime: DatabaseRuntime,
) -> None:
    first = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000106",
    )
    _, first_reservation, _, _ = _reserve(p6_runtime, first)
    second = _plan(
        p6_runtime,
        plan_id="00000000-0000-5000-8000-000000000107",
        name="Different",
        effect_key=first.actions[0].effect_key,
    )
    repository = SqlAlchemyP115OrganizeExecutionRepository(p6_runtime.unit_of_work)
    second_execution_id = UUID(second.plan_id)
    with pytest.raises(P115ExecutionConflict, match="different semantic request"):
        repository.reserve(
            second,
            execution_id=second_execution_id,
            approved_intent_id="approved-execution-conflict",
            request_hash="c" * 64,
            provider_backend="openapi",
            preflight={"safe": True},
            prepared_actions=(_prepared(second.actions[0]),),
            job=execution_job_for(second_execution_id, requested_by_user_id=None, trace_id=None),
        )
    assert first_reservation.execution.actions[0].effect.status is P115ExecutionEffectStatus.PENDING


def test_execution_revision_downgrades_cleanly_and_reapplies_as_the_single_head(
    p6_runtime: DatabaseRuntime,
) -> None:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    try:
        command.downgrade(config, "0005_p6_organize_plans")
        inspector = inspect(p6_runtime.engine)
        assert {
            "p115_organize_plan_executions",
            "p115_organize_effects",
            "p115_organize_execution_actions",
            "p115_organize_reconciliation_intents",
            "p115_organize_effect_events",
        }.isdisjoint(inspector.get_table_names())
        assert "uq_p115_organize_plan_action_effect" not in {
            item["name"] for item in inspector.get_unique_constraints("p115_organize_plan_actions")
        }
    finally:
        command.upgrade(config, "head")
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
            "0012_p6_intro_detection"
        )
