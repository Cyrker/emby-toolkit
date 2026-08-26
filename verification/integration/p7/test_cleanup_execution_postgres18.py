from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.subscription.cleanup_repository import (
    CleanupExecutionConflictError,
    SqlAlchemyCleanupRepository,
)
from etk.platform.db import DatabaseRuntime


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyCleanupRepository:
    return SqlAlchemyCleanupRepository(runtime.unit_of_work, SqlAlchemyCleanupMediaProjection())


def _insert_cleanup(runtime: DatabaseRuntime, task_id: int, *, suffix: str = "") -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO cleanup_index (
                    id, tmdb_id, item_type, status, versions_info_json, best_version_json
                ) VALUES (
                    :id, :tmdb_id, 'Movie', 'pending',
                    CAST(:versions AS jsonb), CAST(:best AS jsonb)
                )
                """
            ),
            {
                "id": task_id,
                "tmdb_id": f"cleanup-{task_id}{suffix}",
                "versions": '[{"id":"emby-old","path":"/fixture/old.mkv"},{"id":"emby-best"}]',
                "best": '"emby-best"',
            },
        )


def _counts(runtime: DatabaseRuntime) -> dict[str, int]:
    with runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT
                    (SELECT COUNT(*) FROM cleanup_execution_items) AS items,
                    (SELECT COUNT(*) FROM job_runs WHERE task_key = 'p7-cleanup-execution') AS jobs,
                    (SELECT COUNT(*) FROM job_attempts) AS attempts,
                    (SELECT COUNT(*) FROM outbox_events WHERE topic = 'job.dispatch.requested') AS outbox,
                    (SELECT COUNT(*) FROM idempotency_records
                     WHERE scope = 'subscription.cleanup.execute') AS idempotency
                """
                )
            )
            .mappings()
            .one()
        )
    return {key: int(value) for key, value in row.items()}


def test_cleanup_staging_atomically_claims_snapshot_job_attempt_outbox_and_idempotency(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 701)
    repository = _repository(p7_runtime)

    first = repository.stage_cleanup([701, "701"])
    replay = repository.stage_cleanup([701])

    assert replay.job_id == first.job_id
    assert replay.execution_key == first.execution_key
    assert replay.reused is True
    assert _counts(p7_runtime) == {
        "items": 1,
        "jobs": 1,
        "attempts": 1,
        "outbox": 1,
        "idempotency": 1,
    }
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                SELECT item.status AS item_status,
                       job.status AS job_status,
                       attempt.status AS attempt_status,
                       event.status AS outbox_status,
                       item.snapshot_json->>'tmdb_id' AS snapshot_tmdb_id
                FROM cleanup_execution_items AS item
                JOIN job_runs AS job ON job.id = item.job_id
                JOIN job_attempts AS attempt ON attempt.job_id = job.id AND attempt.attempt_no = 1
                JOIN outbox_events AS event
                  ON event.aggregate_type = 'job_attempt'
                 AND event.aggregate_id = attempt.id::text
                WHERE item.job_id = :job_id
                """
                ),
                {"job_id": first.job_id},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {
        "item_status": "queued",
        "job_status": "queued",
        "attempt_status": "created",
        "outbox_status": "pending",
        "snapshot_tmdb_id": "cleanup-701",
    }


@pytest.mark.parametrize("mutation", ["ignore", "delete"])
def test_cleanup_staging_wins_against_late_ignore_or_delete(
    p7_runtime: DatabaseRuntime,
    mutation: str,
) -> None:
    task_id = 702 if mutation == "ignore" else 703
    _insert_cleanup(p7_runtime, task_id)
    repository = _repository(p7_runtime)
    repository.stage_cleanup([task_id])

    with pytest.raises(CleanupExecutionConflictError, match="already claimed"):
        if mutation == "ignore":
            repository.ignore_tasks([task_id])
        else:
            repository.delete_tasks([task_id])

    with p7_runtime.engine.connect() as connection:
        status = connection.scalar(
            text("SELECT status FROM cleanup_index WHERE id = :id"),
            {"id": task_id},
        )
    assert status == "pending"


@pytest.mark.parametrize("mutation", ["ignore", "delete"])
def test_cleanup_execute_and_mutation_have_one_serialized_winner(
    p7_runtime: DatabaseRuntime,
    mutation: str,
) -> None:
    task_id = 704 if mutation == "ignore" else 705
    _insert_cleanup(p7_runtime, task_id)

    def stage() -> str:
        try:
            _repository(p7_runtime).stage_cleanup([task_id])
        except CleanupExecutionConflictError:
            return "stage-conflict"
        return "staged"

    def mutate() -> str:
        try:
            repository = _repository(p7_runtime)
            changed = repository.ignore_tasks([task_id]) if mutation == "ignore" else repository.delete_tasks([task_id])
        except CleanupExecutionConflictError:
            return "mutation-conflict"
        return f"mutated:{changed}"

    with ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(stage)
        mutation_future = executor.submit(mutate)
        outcomes = {stage_future.result(), mutation_future.result()}

    assert outcomes in (
        {"staged", "mutation-conflict"},
        {"stage-conflict", "mutated:1"},
    )
    with p7_runtime.engine.connect() as connection:
        row_status = connection.scalar(
            text("SELECT status FROM cleanup_index WHERE id = :id"),
            {"id": task_id},
        )
        active_items = int(
            connection.scalar(
                text(
                    """
                    SELECT COUNT(*)
                    FROM cleanup_execution_items
                    WHERE item_key = :item_key
                      AND status IN ('queued','claimed','effect_started')
                    """
                ),
                {"item_key": f"cleanup:{task_id}"},
            )
            or 0
        )
    assert (row_status, active_items) in {("pending", 1), ("ignored", 0), (None, 0)}


def test_cleanup_staging_rolls_back_every_record_when_job_insert_fails(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 706)
    repository = _repository(p7_runtime)

    with pytest.raises(CleanupExecutionConflictError):
        repository.stage_cleanup([706], requested_by_user_id="missing-user")

    assert _counts(p7_runtime) == {
        "items": 0,
        "jobs": 0,
        "attempts": 0,
        "outbox": 0,
        "idempotency": 0,
    }
    with p7_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT status FROM cleanup_index WHERE id = 706")) == "pending"


def test_cleanup_duplicate_concurrent_submission_reuses_one_job(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 707)

    def stage() -> Any:
        return _repository(p7_runtime).stage_cleanup([707])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: stage(), range(2)))

    assert len({result.job_id for result in results}) == 1
    assert _counts(p7_runtime) == {
        "items": 1,
        "jobs": 1,
        "attempts": 1,
        "outbox": 1,
        "idempotency": 1,
    }
