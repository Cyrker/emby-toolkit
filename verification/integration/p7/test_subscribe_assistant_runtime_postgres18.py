from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

import config_manager  # noqa: F401
from etk.domains.subscription.assistant import (
    AssistantMaintenanceClaimLostError,
    SqlAlchemyAssistantMaintenanceRepository,
)
from etk.domains.subscription.moviepilot import (
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationStatus,
    SubscriptionQuotaRequirement,
)
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore
from etk.processes.p7_subscription_effects import PersistentMoviePilotEffectGateway
from services.subscribe_assistant import store as assistant_store

PHASES = ("delete_records_cleanup", "snapshot_cleanup")


def test_distinct_delete_occurrences_create_two_operations_and_retry_reuses_first(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    gateway = PersistentMoviePilotEffectGateway(
        repository,
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
    )
    identity = {
        "task_key": "subscribe-assistant-maintenance",
        "job_id": "00000000-0000-0000-0000-000000000751",
        "attempt_id": "00000000-0000-0000-0000-000000000752",
        "worker_id": "fixture-worker",
    }
    reservations = []
    gateway._identity = lambda: dict(identity)  # type: ignore[method-assign]

    def reserve(intent):
        reservations.append(repository.reserve(intent))
        return True

    gateway._execute = reserve  # type: ignore[method-assign]

    assert gateway.delete_download_tasks(["ABC"], operation_occurrence="download-monitor:abc:initial")
    assert gateway.delete_download_tasks(["ABC"], operation_occurrence="download-monitor:abc:post-search")
    assert gateway.delete_download_tasks(["ABC"], operation_occurrence="download-monitor:abc:initial")

    assert [reservation.created for reservation in reservations] == [True, True, False]
    assert reservations[0].operation.id == reservations[2].operation.id
    assert reservations[0].operation.id != reservations[1].operation.id
    with p7_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 2


def test_transfer_history_retry_reuses_original_snapshot_when_record_drifts(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    gateway = PersistentMoviePilotEffectGateway(
        repository,
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
    )
    gateway._identity = lambda: {  # type: ignore[method-assign]
        "task_key": "process-watchlist",
        "job_id": "00000000-0000-0000-0000-000000000753",
        "attempt_id": "00000000-0000-0000-0000-000000000754",
        "worker_id": "fixture-worker",
    }
    reservations = []

    def reserve(intent):
        reservations.append(repository.reserve(intent))
        return True

    gateway._execute = reserve  # type: ignore[method-assign]
    occurrence = "episode-residue:101:2:history:7"

    assert gateway.delete_transfer_history(
        {"id": 7, "name": "first"},
        operation_occurrence=occurrence,
    )
    assert gateway.delete_transfer_history(
        {"id": 7, "name": "changed"},
        operation_occurrence=occurrence,
    )

    assert [reservation.created for reservation in reservations] == [True, False]
    assert reservations[0].operation.id == reservations[1].operation.id
    assert reservations[1].operation.request["payload"]["record"] == {"id": 7, "name": "first"}


def test_snapshot_claim_schema_is_current_and_rejects_orphan_job_identity(
    p7_runtime: DatabaseRuntime,
) -> None:
    with p7_runtime.engine.connect() as connection:
        head = connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version"))
        constraints = set(
            connection.execute(
                text(
                    """
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'public'
                      AND table_name = 'subscribe_assistant_snapshots'
                    """
                )
            ).scalars()
        )
        attempt_constraints = set(
            connection.execute(
                text(
                    """
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'public'
                      AND table_name = 'job_attempts'
                    """
                )
            ).scalars()
        )
    assert head == "0019_p9_proxy_leases"
    assert {
        "ck_sas_snapshot_claim",
        "fk_sas_snapshot_job",
        "fk_sas_snapshot_attempt_job",
    } <= constraints
    assert "uq_job_attempts_id_job" in attempt_constraints

    with pytest.raises(IntegrityError):
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO subscribe_assistant_snapshots (
                        tmdb_id, item_type, scope_total, scope_json, subscribe_json, created_at,
                        job_id, attempt_id, worker_id, claim_token, claimed_at, lease_expires_at
                    ) VALUES (
                        'orphan-fixture', 'Series', 1, '{}'::jsonb, '{}'::jsonb, NOW(),
                        :job_id, :attempt_id, 'worker-a', :claim_token,
                        NOW(), NOW() + INTERVAL '1 minute'
                    )
                    """
                ),
                {"job_id": uuid4(), "attempt_id": uuid4(), "claim_token": uuid4()},
            )

    _, job_a, _ = _start_job(p7_runtime, worker_id="worker-a")
    _, job_b, attempt_b = _start_job(
        p7_runtime,
        worker_id="worker-b",
        parent_job_id=job_a,
        workflow_step=1,
    )
    assert job_a != job_b
    with pytest.raises(IntegrityError):
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO subscribe_assistant_snapshots (
                        tmdb_id, item_type, scope_total, scope_json, subscribe_json, created_at,
                        job_id, attempt_id, worker_id, claim_token, claimed_at, lease_expires_at
                    ) VALUES (
                        'mismatched-fixture', 'Series', 1, '{}'::jsonb, '{}'::jsonb, NOW(),
                        :job_id, :attempt_id, 'worker-a', :claim_token,
                        NOW(), NOW() + INTERVAL '1 minute'
                    )
                    """
                ),
                {"job_id": job_a, "attempt_id": attempt_b, "claim_token": uuid4()},
            )


def _start_job(
    runtime: DatabaseRuntime,
    *,
    worker_id: str,
    max_attempts: int = 3,
    parent_job_id: UUID | None = None,
    workflow_step: int | None = None,
) -> tuple[PostgresJobStore, UUID, UUID]:
    jobs = PostgresJobStore(runtime.unit_of_work)
    created = jobs.create_job(
        task_key="subscribe-assistant-maintenance",
        payload={},
        max_attempts=max_attempts,
        parent_job_id=parent_job_id,
        workflow_step=workflow_step,
    ).projection
    attempt_id = created.current_attempt.id
    claim = jobs.claim_attempt(
        job_id=created.job.id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_seconds=60,
    )
    assert claim.claimed is True
    jobs.start_attempt(
        job_id=created.job.id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_seconds=60,
    )
    return jobs, created.job.id, attempt_id


def _start_attempt(
    jobs: PostgresJobStore,
    *,
    job_id: UUID,
    attempt_id: UUID,
    worker_id: str,
) -> None:
    claim = jobs.claim_attempt(
        job_id=job_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_seconds=60,
    )
    assert claim.claimed is True
    jobs.start_attempt(
        job_id=job_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_seconds=60,
    )


def _legacy_connection(runtime: DatabaseRuntime):
    dsn = runtime.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)

    @contextmanager
    def connect() -> Iterator[Any]:
        connection = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
        try:
            yield connection
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    return connect


def test_maintenance_checkpoint_rejects_wrong_worker_and_expired_lease(
    p7_runtime: DatabaseRuntime,
) -> None:
    _, job_id, attempt_id = _start_job(p7_runtime, worker_id="worker-a")
    repository = SqlAlchemyAssistantMaintenanceRepository(p7_runtime.unit_of_work)
    repository.load_or_create(job_id, attempt_id, "worker-a", PHASES)

    with pytest.raises(AssistantMaintenanceClaimLostError, match="no longer current"):
        repository.checkpoint_phase(job_id, attempt_id, "worker-b", PHASES[0], 1)

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE job_attempts SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = :id"),
            {"id": attempt_id},
        )
    with pytest.raises(AssistantMaintenanceClaimLostError, match="no longer current"):
        repository.checkpoint_phase(job_id, attempt_id, "worker-a", PHASES[0], 1)


def test_replacement_attempt_fences_stale_checkpoint_and_resumes_frozen_phases(
    p7_runtime: DatabaseRuntime,
) -> None:
    jobs, job_id, first_attempt = _start_job(p7_runtime, worker_id="worker-a")
    repository = SqlAlchemyAssistantMaintenanceRepository(p7_runtime.unit_of_work)
    repository.load_or_create(job_id, first_attempt, "worker-a", PHASES)
    repository.checkpoint_phase(job_id, first_attempt, "worker-a", PHASES[0], 1)
    jobs.fail_attempt(
        job_id=job_id,
        attempt_id=first_attempt,
        worker_id="worker-a",
        error_type="fixture_restart",
        error_message="fixture restart",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id
    _start_attempt(
        jobs,
        job_id=job_id,
        attempt_id=second_attempt,
        worker_id="worker-b",
    )

    with pytest.raises(AssistantMaintenanceClaimLostError, match="no longer current"):
        repository.checkpoint_phase(job_id, first_attempt, "worker-a", PHASES[1], 1)
    resumed = repository.load_or_create(job_id, second_attempt, "worker-b", PHASES)

    assert resumed.completed_phases == (PHASES[0],)
    assert resumed.current_attempt_id == second_attempt


def test_maintenance_retention_deletes_terminal_and_orphan_runs_but_keeps_active_run(
    p7_runtime: DatabaseRuntime,
) -> None:
    terminal_jobs, terminal_job_id, terminal_attempt_id = _start_job(
        p7_runtime,
        worker_id="worker-terminal",
    )
    terminal_jobs.fail_attempt(
        job_id=terminal_job_id,
        attempt_id=terminal_attempt_id,
        worker_id="worker-terminal",
        error_type="fixture_terminal",
        error_message="fixture terminal",
        retryable=False,
        honor_cancel_request=False,
    )
    _, active_job_id, _ = _start_job(p7_runtime, worker_id="worker-active")
    _, current_job_id, current_attempt_id = _start_job(
        p7_runtime,
        worker_id="worker-current",
        parent_job_id=active_job_id,
        workflow_step=1,
    )
    orphan_job_id = uuid4()
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE job_runs
                SET finished_at = NOW() - INTERVAL '31 days',
                    updated_at = NOW() - INTERVAL '31 days'
                WHERE id = :job_id
                """
            ),
            {"job_id": terminal_job_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES
                    ('maintenance:terminal-old', CAST(:terminal AS jsonb), NOW() - INTERVAL '31 days'),
                    ('maintenance:active-old', CAST(:active AS jsonb), NOW() - INTERVAL '31 days'),
                    ('maintenance:orphan-old', CAST(:orphan AS jsonb), NOW() - INTERVAL '31 days')
                """
            ),
            {
                "terminal": json.dumps({"job_id": str(terminal_job_id), "completed": False}),
                "active": json.dumps({"job_id": str(active_job_id), "completed": False}),
                "orphan": json.dumps({"job_id": str(orphan_job_id), "completed": False}),
            },
        )

    SqlAlchemyAssistantMaintenanceRepository(p7_runtime.unit_of_work).load_or_create(
        current_job_id,
        current_attempt_id,
        "worker-current",
        PHASES,
    )

    with p7_runtime.engine.connect() as connection:
        keys = set(
            connection.execute(
                text("SELECT state_key FROM subscribe_assistant_state WHERE state_key LIKE 'maintenance:%'")
            ).scalars()
        )
    assert "maintenance:terminal-old" not in keys
    assert "maintenance:orphan-old" not in keys
    assert "maintenance:active-old" in keys
    assert f"maintenance:{current_job_id}" in keys


def test_snapshot_same_job_replacement_reclaims_immediately_and_fences_stale_token(
    p7_runtime: DatabaseRuntime,
    monkeypatch,
) -> None:
    monkeypatch.setattr(assistant_store, "get_db_connection", _legacy_connection(p7_runtime))
    jobs, job_id, first_attempt = _start_job(p7_runtime, worker_id="worker-a")
    assistant_store.upsert_snapshot(tmdb_id="101", season_number=2, scope_total=12)
    first = assistant_store.claim_next_snapshot(
        1,
        job_id=job_id,
        attempt_id=first_attempt,
        worker_id="worker-a",
        lease_seconds=600,
    )
    assert first is not None

    jobs.fail_attempt(
        job_id=job_id,
        attempt_id=first_attempt,
        worker_id="worker-a",
        error_type="fixture_restart",
        error_message="fixture restart",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id
    _start_attempt(jobs, job_id=job_id, attempt_id=second_attempt, worker_id="worker-b")
    second = assistant_store.claim_next_snapshot(
        1,
        job_id=job_id,
        attempt_id=second_attempt,
        worker_id="worker-b",
        lease_seconds=600,
    )

    assert second is not None
    assert second.claim_token != first.claim_token
    with pytest.raises(assistant_store.SnapshotClaimLostError, match="no longer current"):
        assistant_store.mark_snapshot_checked(first)
    assistant_store.mark_snapshot_checked(second)
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT last_checked_at, claim_token, job_id, attempt_id, worker_id, state_revision
                    FROM subscribe_assistant_snapshots
                    WHERE id = :id
                    """
                ),
                {"id": second.snapshot_id},
            )
            .mappings()
            .one()
        )
    assert row["last_checked_at"] is not None
    assert row["claim_token"] is row["job_id"] is row["attempt_id"] is row["worker_id"] is None
    assert row["state_revision"] == 3


def test_snapshot_different_job_waits_for_lease_expiry(
    p7_runtime: DatabaseRuntime,
    monkeypatch,
) -> None:
    monkeypatch.setattr(assistant_store, "get_db_connection", _legacy_connection(p7_runtime))
    first_jobs, first_job, first_attempt = _start_job(p7_runtime, worker_id="worker-a")
    assistant_store.upsert_snapshot(tmdb_id="202", season_number=1, scope_total=8)
    first = assistant_store.claim_next_snapshot(
        1,
        job_id=first_job,
        attempt_id=first_attempt,
        worker_id="worker-a",
        lease_seconds=600,
    )
    first_jobs.fail_attempt(
        job_id=first_job,
        attempt_id=first_attempt,
        worker_id="worker-a",
        error_type="fixture_terminal",
        error_message="fixture terminal",
        retryable=False,
        honor_cancel_request=False,
    )
    _, second_job, second_attempt = _start_job(
        p7_runtime,
        worker_id="worker-b",
        parent_job_id=first_job,
        workflow_step=1,
    )

    assert first is not None
    blocked = assistant_store.claim_next_snapshot(
        1,
        job_id=second_job,
        attempt_id=second_attempt,
        worker_id="worker-b",
        lease_seconds=600,
    )
    assert blocked is None

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_snapshots
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE id = :id
                """
            ),
            {"id": first.snapshot_id},
        )
    recovered = assistant_store.claim_next_snapshot(
        1,
        job_id=second_job,
        attempt_id=second_attempt,
        worker_id="worker-b",
        lease_seconds=600,
    )
    assert recovered is not None
    assert recovered.job_id == second_job


def test_snapshot_renew_and_mark_reject_stale_revision_wrong_worker_and_expired_lease(
    p7_runtime: DatabaseRuntime,
    monkeypatch,
) -> None:
    monkeypatch.setattr(assistant_store, "get_db_connection", _legacy_connection(p7_runtime))
    _, job_id, attempt_id = _start_job(p7_runtime, worker_id="worker-a")
    assistant_store.upsert_snapshot(tmdb_id="303", season_number=1, scope_total=10)
    claim = assistant_store.claim_next_snapshot(
        1,
        job_id=job_id,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=600,
    )
    assert claim is not None

    with pytest.raises(assistant_store.SnapshotClaimLostError, match="no longer current"):
        assistant_store.renew_snapshot_claim(replace(claim, worker_id="worker-b"), lease_seconds=600)

    renewed = assistant_store.renew_snapshot_claim(claim, lease_seconds=600)
    assert renewed.state_revision == claim.state_revision + 1
    with pytest.raises(assistant_store.SnapshotClaimLostError, match="no longer current"):
        assistant_store.mark_snapshot_checked(claim)

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_snapshots
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE id = :id
                """
            ),
            {"id": renewed.snapshot_id},
        )
    with pytest.raises(assistant_store.SnapshotClaimLostError, match="no longer current"):
        assistant_store.mark_snapshot_checked(renewed)


def test_snapshot_retention_deletes_expired_claim_but_keeps_active_claim(
    p7_runtime: DatabaseRuntime,
    monkeypatch,
) -> None:
    monkeypatch.setattr(assistant_store, "get_db_connection", _legacy_connection(p7_runtime))
    _, first_job, first_attempt = _start_job(p7_runtime, worker_id="worker-a")
    _, second_job, second_attempt = _start_job(
        p7_runtime,
        worker_id="worker-b",
        parent_job_id=first_job,
        workflow_step=1,
    )
    assistant_store.upsert_snapshot(tmdb_id="401", season_number=1, scope_total=10)
    assistant_store.upsert_snapshot(tmdb_id="402", season_number=1, scope_total=10)
    expired = assistant_store.claim_next_snapshot(
        1,
        job_id=first_job,
        attempt_id=first_attempt,
        worker_id="worker-a",
        lease_seconds=600,
    )
    active = assistant_store.claim_next_snapshot(
        1,
        job_id=second_job,
        attempt_id=second_attempt,
        worker_id="worker-b",
        lease_seconds=600,
    )
    assert expired is not None and active is not None
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_snapshots
                SET created_at = NOW() - INTERVAL '31 days'
                WHERE id IN (:expired_id, :active_id)
                """
            ),
            {"expired_id": expired.snapshot_id, "active_id": active.snapshot_id},
        )
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_snapshots
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE id = :expired_id
                """
            ),
            {"expired_id": expired.snapshot_id},
        )

    assert assistant_store.cleanup_snapshots(30) == 1
    with p7_runtime.engine.connect() as connection:
        remaining = set(connection.execute(text("SELECT id FROM subscribe_assistant_snapshots")).scalars())
    assert expired.snapshot_id not in remaining
    assert active.snapshot_id in remaining


class _Gateway:
    def __init__(self, query_results: Sequence[MoviePilotQueryResult]) -> None:
        self.query_results = list(query_results)
        self.writes = 0

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        del tmdb_id, season
        return self.query_results.pop(0)

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        del payload
        self.writes += 1
        return MoviePilotWriteResult("create-subscription", 201)

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        del payload
        raise AssertionError("unexpected update")

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult:
        del tmdb_id, status, season, total_episodes
        raise AssertionError("unexpected status update")

    def delete_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotWriteResult:
        del tmdb_id, season
        raise AssertionError("unexpected delete")

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult:
        del subscription_id
        raise AssertionError("unexpected delete by id")

    def search_subscription(self, subscription_id: int | str) -> MoviePilotWriteResult:
        del subscription_id
        raise AssertionError("unexpected search")

    def delete_download_tasks(self, hashes: Sequence[str]) -> MoviePilotWriteResult:
        del hashes
        raise AssertionError("unexpected download delete")

    def delete_transfer_history(self, record: Mapping[str, Any]) -> MoviePilotWriteResult:
        del record
        raise AssertionError("unexpected transfer history delete")

    def redo_transfer_history(self, history_ids: Sequence[int]) -> MoviePilotWriteResult:
        del history_ids
        raise AssertionError("unexpected history redo")


def test_parent_job_cancel_after_reserve_prevents_provider_request_and_releases_quota(
    p7_runtime: DatabaseRuntime,
) -> None:
    jobs, origin_job_id, _ = _start_job(p7_runtime, worker_id="worker-a")
    repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.CREATE,
        scope_key=f"p7:subscribe-assistant-maintenance:{origin_job_id}:create:909:1",
        request={
            "action": "create",
            "tmdb_id": "909",
            "season": 1,
            "payload": {"tmdbid": 909, "season": 1, "name": "Fixture"},
            "expected": {},
            "origin": {"task_key": "subscribe-assistant-maintenance", "job_id": str(origin_job_id)},
        },
        quota=SubscriptionQuotaRequirement(now.date(), daily_cap=1),
    )
    operation_id = repository.reserve(intent).operation.id
    jobs.request_cancel(origin_job_id)
    gateway = _Gateway([])

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        operation_id,
        worker_id="worker-a",
        lease_seconds=60,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.CANCELLED
    assert result.operation.provider_request_started_at is None
    assert gateway.writes == 0
    assert gateway.query_results == []
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                    SELECT
                        (SELECT status FROM subscription_quota_reservations
                         WHERE operation_id = :operation_id) AS quota_status,
                        (SELECT status FROM outbox_events
                         WHERE aggregate_id = :aggregate_id) AS outbox_status
                    """
                ),
                {"operation_id": operation_id, "aggregate_id": str(operation_id)},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"quota_status": "released", "outbox_status": "processed"}


def test_provider_checkpoint_restart_reuses_operation_and_only_reads_back(
    p7_runtime: DatabaseRuntime,
) -> None:
    _, origin_job_id, _ = _start_job(p7_runtime, worker_id="worker-a")
    repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    intent = SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.CREATE,
        scope_key=f"p7:subscribe-assistant-maintenance:{origin_job_id}:create:101:2",
        request={
            "action": "create",
            "tmdb_id": "101",
            "season": 2,
            "payload": {"tmdbid": 101, "season": 2, "name": "Fixture"},
            "expected": {},
            "origin": {"task_key": "subscribe-assistant-maintenance", "job_id": str(origin_job_id)},
        },
    )
    gateway = _Gateway(
        [
            MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None),
            MoviePilotQueryResult(
                MoviePilotQueryDisposition.FOUND,
                {"id": 11, "tmdbid": 101, "season": 2},
            ),
        ]
    )
    executor = SubscriptionOperationExecutor(repository, gateway, readback_delay_seconds=0)
    first_id = repository.reserve(intent).operation.id

    first = executor.execute(first_id, worker_id="worker-a", lease_seconds=60)
    replay_id = repository.reserve(intent).operation.id
    recovered = executor.execute(replay_id, worker_id="worker-b", lease_seconds=60)

    assert first is not None and first.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert replay_id == first_id
    assert recovered is not None and recovered.operation.status is SubscriptionOperationStatus.CONFIRMED
    assert gateway.writes == 1
