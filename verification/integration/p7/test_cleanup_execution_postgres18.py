from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from queue import Queue
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.media.subscription_projection import SubscriptionMediaKey
from etk.domains.sharing import (
    PostgresContributionLedger,
    PostgresSharingOperationRepository,
    SharingService,
    SqlAlchemySharingProjectionRepository,
)
from etk.domains.subscription.cleanup_adapters import (
    SettingsCleanupExecutionPolicy,
    SqlAlchemyCleanupMediaGateway,
)
from etk.domains.subscription.cleanup_execution import (
    CleanupEffectResult,
    CleanupExecutionPending,
    CleanupExecutionService,
    cleanup_job_max_attempts,
)
from etk.domains.subscription.cleanup_repository import (
    CleanupExecutionConflictError,
    SqlAlchemyCleanupRepository,
)
from etk.domains.subscription.cleanup_runtime_repository import (
    CleanupExecutionClaimLostError,
    SqlAlchemyCleanupExecutionRepository,
)
from etk.domains.subscription.core import SqlAlchemySubscriptionCoreRepository, SubscriptionStateConflictError
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime, SqlAlchemyUnitOfWork
from etk.platform.jobs import OutboxStatus, PersistentExecutionCancelled, PostgresJobStore
from etk.processes.worker import _ExecutionControl, _ExecutionProgress


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyCleanupRepository:
    return SqlAlchemyCleanupRepository(
        runtime.unit_of_work,
        SqlAlchemyCleanupMediaProjection(),
        SettingsCleanupExecutionPolicy(
            lambda: Settings(
                local_strm_root="/fixture",
                emby_server_url="http://emby.fixture.invalid",
                emby_api_key=SecretStr("fixture-token"),
            )
        ),
    )


class _ObservedUnitOfWork(SqlAlchemyUnitOfWork):
    def __init__(self, runtime: DatabaseRuntime, pids: Queue[int]) -> None:
        super().__init__(runtime.session_factory)
        self._pids = pids

    def __enter__(self) -> SqlAlchemyUnitOfWork:
        entered = super().__enter__()
        self._pids.put(int(entered.session.scalar(text("SELECT pg_backend_pid()")) or 0))
        return entered


def _insert_cleanup(
    runtime: DatabaseRuntime,
    task_id: int,
    *,
    suffix: str = "",
    emby_id: str = "emby-old",
) -> None:
    path_name = "old" if emby_id == "emby-old" else emby_id
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
                "versions": (
                    f'[{json.dumps({"id": emby_id, "path": f"/fixture/{path_name}.mkv"})},{{"id":"emby-best"}}]'
                ),
                "best": '"emby-best"',
            },
        )


def _insert_multi_version_cleanup(runtime: DatabaseRuntime, task_id: int) -> None:
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
                "tmdb_id": f"cleanup-{task_id}",
                "versions": (
                    '[{"id":"emby-old-a","path":"/fixture/old-a.mkv"},'
                    '{"id":"emby-old-b","path":"/fixture/old-b.mkv"},'
                    '{"id":"emby-best"}]'
                ),
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
                       item.snapshot_json->>'tmdb_id' AS snapshot_tmdb_id,
                       job.request_payload_json->>'native_task' AS native_task,
                       job.request_payload_json ? 'legacy_operation' AS has_legacy_operation
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
        "native_task": "p7-cleanup-execution",
        "has_legacy_operation": False,
    }


def test_native_media_cleanup_atomically_removes_aligned_version_and_replays_absence(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"native-media-{uuid4().hex}"
    old_path = f"/fixture/{tmdb_id}-old.mkv"
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    file_sha1_json, file_pickcode_json, asset_details_json
                ) VALUES (
                    :tmdb_id, 'Movie', TRUE, '["emby-old","emby-best","emby-spare"]'::jsonb,
                    '["OLD-SHA1","BEST-SHA1","SPARE-SHA1"]'::jsonb,
                    '["old-pick","best-pick","spare-pick"]'::jsonb,
                    CAST(:assets AS jsonb)
                )
                """
            ),
            {
                "tmdb_id": tmdb_id,
                "assets": json.dumps(
                    [
                        {"emby_item_id": "emby-old", "path": old_path},
                        {"emby_item_id": "emby-best", "path": f"/fixture/{tmdb_id}-best.mkv"},
                        {"emby_item_id": "emby-spare", "path": f"/fixture/{tmdb_id}-spare.mkv"},
                    ]
                ),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO cleanup_index (
                    id, tmdb_id, item_type, status, versions_info_json, best_version_json
                ) VALUES (
                    :id, :tmdb_id, 'Movie', 'pending',
                    CAST(:versions AS jsonb), '"emby-best"'::jsonb
                )
                """
            ),
            {
                "id": 900_001,
                "tmdb_id": tmdb_id,
                "versions": json.dumps([{"id": "emby-old"}, {"id": "emby-best"}, {"id": "emby-spare"}]),
            },
        )
        virtual_id = connection.scalar(
            text(
                """
                INSERT INTO shared_virtual_imports (
                    source_kind, source_id, tmdb_id, item_type, strm_paths_json
                ) VALUES ('rapid', :source_id, :tmdb_id, 'Movie', CAST(:paths AS jsonb))
                RETURNING id
                """
            ),
            {"source_id": tmdb_id, "tmdb_id": tmdb_id, "paths": json.dumps([old_path])},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, pick_code, account_id, entry_kind
                ) VALUES (:id, :parent_id, 'virtual.strm', :pick_code, '1', 'file')
                """
            ),
            {
                "id": f"virtual:{virtual_id}:file",
                "parent_id": f"virtual:{virtual_id}",
                "pick_code": f"virtual-{virtual_id}",
            },
        )
        shared_source_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources (
                    source_key, source_kind, tmdb_id, item_type, title,
                    sha1, status, center_status
                ) VALUES (
                    :source_key, 'movie', :tmdb_id, 'Movie', 'Fixture',
                    'OLD-SHA1', 'active', 'local'
                )
                RETURNING id
                """
            ),
            {"source_key": f"cleanup-source:{tmdb_id}", "tmdb_id": tmdb_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO shared_rapid_source_files (
                    local_source_id, fid, pick_code, sha1, file_name,
                    tmdb_id, item_type
                ) VALUES (
                    :source_id, 'fixture-fid', 'old-pick', 'OLD-SHA1',
                    'old.mkv', :tmdb_id, 'Movie'
                )
                """
            ),
            {"source_id": shared_source_id, "tmdb_id": tmdb_id},
        )
        cancelled_source_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources (
                    source_key, source_kind, tmdb_id, item_type, title,
                    sha1, status, center_status
                ) VALUES (
                    :source_key, 'movie', :tmdb_id, 'Movie', 'Cancelled Fixture',
                    'OLD-SHA1', 'cancelled', 'local'
                )
                RETURNING id
                """
            ),
            {"source_key": f"cleanup-cancelled-source:{tmdb_id}", "tmdb_id": tmdb_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO shared_rapid_source_files (
                    local_source_id, fid, pick_code, sha1, file_name,
                    tmdb_id, item_type
                ) VALUES (
                    :source_id, 'cancelled-fixture-fid', 'old-pick', 'OLD-SHA1',
                    'cancelled-old.mkv', :tmdb_id, 'Movie'
                )
                """
            ),
            {"source_id": cancelled_source_id, "tmdb_id": tmdb_id},
        )

    lifecycle = SharingService(
        PostgresContributionLedger(p7_runtime.unit_of_work),
        object(),
        PostgresSharingOperationRepository(p7_runtime.unit_of_work),
        unit_of_work=p7_runtime.unit_of_work,
        projection=SqlAlchemySharingProjectionRepository(p7_runtime.unit_of_work),
    )
    gateway = SqlAlchemyCleanupMediaGateway(p7_runtime.unit_of_work, lifecycle)
    target = {
        "emby_id": "emby-old",
        "tmdb_id": tmdb_id,
        "item_type": "Movie",
        "local": {"files": [{"sha1": "OLD-SHA1", "pick_code": "old-pick"}]},
    }
    first = gateway.reconcile(target, operation_id="native-media-fixture")
    replay = gateway.reconcile(target, operation_id="native-media-fixture")

    assert first.status.value == replay.status.value == "confirmed"
    assert first.evidence["remaining_versions"] == 2
    assert replay.evidence["already_absent"] is True
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT emby_item_ids_json, asset_details_json,
                       file_sha1_json, file_pickcode_json, in_library
                FROM media_metadata
                WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                """
                ),
                {"tmdb_id": tmdb_id},
            )
            .mappings()
            .one()
        )
        cleanup_row = (
            connection.execute(
                text(
                    """
                SELECT versions_info_json, best_version_json
                FROM cleanup_index
                WHERE tmdb_id = :tmdb_id
                """
                ),
                {"tmdb_id": tmdb_id},
            )
            .mappings()
            .one()
        )
        virtual_count = connection.scalar(
            text("SELECT COUNT(*) FROM shared_virtual_imports WHERE id = :id"),
            {"id": virtual_id},
        )
        cache_count = connection.scalar(
            text("SELECT COUNT(*) FROM p115_filesystem_cache WHERE parent_id = :parent_id"),
            {"parent_id": f"virtual:{virtual_id}"},
        )
        shared_source_count = connection.scalar(
            text("SELECT COUNT(*) FROM shared_rapid_sources WHERE id = :id"),
            {"id": shared_source_id},
        )
        cancelled_source_count = connection.scalar(
            text("SELECT COUNT(*) FROM shared_rapid_sources WHERE id = :id"),
            {"id": cancelled_source_id},
        )
    assert dict(row) == {
        "emby_item_ids_json": ["emby-best", "emby-spare"],
        "asset_details_json": [
            {"path": f"/fixture/{tmdb_id}-best.mkv", "emby_item_id": "emby-best"},
            {"path": f"/fixture/{tmdb_id}-spare.mkv", "emby_item_id": "emby-spare"},
        ],
        "file_sha1_json": ["BEST-SHA1", "SPARE-SHA1"],
        "file_pickcode_json": ["best-pick", "spare-pick"],
        "in_library": True,
    }
    assert dict(cleanup_row) == {
        "versions_info_json": [{"id": "emby-best"}, {"id": "emby-spare"}],
        "best_version_json": "emby-best",
    }
    assert virtual_count == cache_count == shared_source_count == 0
    assert cancelled_source_count == 1


def test_native_episode_cleanup_cascades_last_episode_offline_to_season_and_series(
    p7_runtime: DatabaseRuntime,
) -> None:
    suffix = uuid4().hex
    series_id = f"native-series-{suffix}"
    season_id = f"{series_id}-s1"
    episode_id = f"{series_id}-s1e1"
    emby_id = f"emby-{episode_id}"
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, parent_series_tmdb_id,
                    season_number, episode_number, watching_status,
                    emby_item_ids_json, file_sha1_json, file_pickcode_json,
                    asset_details_json
                ) VALUES
                    (
                        :series_id, 'Series', 'Fixture Series', TRUE, NULL,
                        NULL, NULL, 'Watching', '[]'::jsonb, '[]'::jsonb,
                        '[]'::jsonb, NULL
                    ),
                    (
                        :season_id, 'Season', 'Fixture Season', TRUE, :series_id,
                        1, NULL, 'Watching', '[]'::jsonb, '[]'::jsonb,
                        '[]'::jsonb, NULL
                    ),
                    (
                        :episode_id, 'Episode', 'Fixture Episode', TRUE, :series_id,
                        1, 1, 'Watching', CAST(:emby_ids AS jsonb),
                        '["EPISODE-SHA1"]'::jsonb, '["episode-pick"]'::jsonb,
                        CAST(:assets AS jsonb)
                    )
                """
            ),
            {
                "series_id": series_id,
                "season_id": season_id,
                "episode_id": episode_id,
                "emby_ids": json.dumps([emby_id]),
                "assets": json.dumps([{"emby_item_id": emby_id, "path": f"/fixture/{episode_id}.mkv"}]),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO cleanup_index (
                    id, tmdb_id, item_type, status, versions_info_json, best_version_json
                ) VALUES
                    (900011, :series_id, 'Series', 'pending', '[]'::jsonb, NULL),
                    (900012, :season_id, 'Season', 'ignored', '[]'::jsonb, NULL),
                    (
                        900013, :episode_id, 'Episode', 'pending',
                        CAST(:versions AS jsonb), CAST(:best AS jsonb)
                    )
                """
            ),
            {
                "series_id": series_id,
                "season_id": season_id,
                "episode_id": episode_id,
                "versions": json.dumps([{"id": emby_id}]),
                "best": json.dumps(emby_id),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (
                    tmdb_id, item_type, season_number, status
                ) VALUES (:series_id, 'Season', 1, 'needed')
                """
            ),
            {"series_id": series_id},
        )

    result = SqlAlchemyCleanupMediaGateway(p7_runtime.unit_of_work).reconcile(
        {
            "emby_id": emby_id,
            "tmdb_id": episode_id,
            "item_type": "Episode",
            "series_id": series_id,
            "season_number": 1,
            "episode_number": 1,
            "local": {"files": [{"sha1": "EPISODE-SHA1", "pick_code": "episode-pick"}]},
        },
        operation_id="native-episode-cascade-fixture",
    )

    assert result.status.value == "confirmed"
    assert result.evidence["remaining_versions"] == 0
    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                SELECT tmdb_id, item_type, in_library, watching_status,
                       emby_item_ids_json, file_sha1_json, file_pickcode_json,
                       asset_details_json
                FROM media_metadata
                WHERE tmdb_id IN (:series_id, :season_id, :episode_id)
                ORDER BY item_type, tmdb_id
                """
                ),
                {"series_id": series_id, "season_id": season_id, "episode_id": episode_id},
            )
            .mappings()
            .all()
        )
        cleanup_count = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM cleanup_index
                WHERE tmdb_id IN (:series_id, :season_id, :episode_id)
                """
            ),
            {"series_id": series_id, "season_id": season_id, "episode_id": episode_id},
        )
        resubscribe_count = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM resubscribe_index
                WHERE tmdb_id = :series_id AND item_type = 'Season'
                """
            ),
            {"series_id": series_id},
        )

    assert len(rows) == 3
    assert all(row["in_library"] is False for row in rows)
    assert all(row["emby_item_ids_json"] == [] for row in rows)
    assert all(row["file_sha1_json"] == [] for row in rows)
    assert all(row["file_pickcode_json"] == [] for row in rows)
    assert all(row["asset_details_json"] is None for row in rows)
    assert {(row["item_type"], row["watching_status"]) for row in rows if row["item_type"] in {"Series", "Season"}} == {
        ("Series", "NONE"),
        ("Season", "NONE"),
    }
    assert cleanup_count == 0
    assert resubscribe_count == 0


def test_native_media_cleanup_fails_closed_for_unaligned_version_arrays(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"native-media-unaligned-{uuid4().hex}"
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    file_sha1_json, file_pickcode_json, asset_details_json
                ) VALUES (
                    :tmdb_id, 'Movie', TRUE, '["emby-old"]'::jsonb,
                    '[]'::jsonb, '["old-pick"]'::jsonb,
                    '[{"emby_item_id":"emby-old","path":"/fixture/old.mkv"}]'::jsonb
                )
                """
            ),
            {"tmdb_id": tmdb_id},
        )

    result = SqlAlchemyCleanupMediaGateway(p7_runtime.unit_of_work).reconcile(
        {"emby_id": "emby-old", "tmdb_id": tmdb_id, "item_type": "Movie"},
        operation_id="native-media-unaligned-fixture",
    )

    assert result.status.value == "indeterminate"
    assert result.error == {
        "type": "media_cleanup_versions_unaligned",
        "tmdb_id": tmdb_id,
        "item_type": "Movie",
    }


def test_resubscribe_version_cleanup_freezes_identity_as_child_and_parent_cancel_fences_effect(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 726)
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    file_sha1_json, file_pickcode_json, asset_details_json
                ) VALUES (
                    'cleanup-726', 'Movie', TRUE, '["emby-old"]'::jsonb,
                    '["fixture-sha1"]'::jsonb, '["fixture-pick-726"]'::jsonb,
                    '[{"emby_item_id":"emby-old","path":"/fixture/old.mkv"}]'::jsonb
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, pick_code, account_id, entry_kind
                ) VALUES (
                    'fixture-fid-726', 'fixture-parent-726', 'old.mkv',
                    'fixture-pick-726', '726', 'file'
                )
                """
            )
        )
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")

    submission = _repository(p7_runtime).stage_version_deletion(
        "emby-old",
        parent_job_id=parent.job.id,
        parent_attempt_id=parent_attempt_id,
        parent_worker_id="worker-parent",
    )
    child_attempt_id = _current_attempt(p7_runtime, submission.job_id)

    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT job.parent_job_id, job.workflow_step, item.snapshot_json,
                           (SELECT COUNT(*) FROM job_attempts WHERE job_id = job.id) AS attempts,
                           (SELECT COUNT(*) FROM outbox_events
                            WHERE aggregate_id = CAST(:attempt_id AS text)
                              AND topic = 'job.dispatch.requested') AS dispatches
                    FROM job_runs AS job
                    JOIN cleanup_execution_items AS item ON item.job_id = job.id
                    WHERE job.id = :job_id
                    """
                ),
                {"job_id": submission.job_id, "attempt_id": child_attempt_id},
            )
            .mappings()
            .one()
        )
    snapshot = row["snapshot_json"]
    assert row["parent_job_id"] == parent.job.id
    assert row["workflow_step"] == 1
    assert int(row["attempts"]) == int(row["dispatches"]) == 1
    assert snapshot["target"]["local"]["files"][0]["path"] == "/fixture/old.mkv"
    assert snapshot["target"]["p115"]["nodes"] == [
        {
            "fid": "fixture-fid-726",
            "parent_id": "fixture-parent-726",
            "account_id": "726",
            "pick_code": "fixture-pick-726",
            "sha1": "",
            "size": 0,
        }
    ]

    jobs.request_cancel(parent.job.id)
    _start_attempt(p7_runtime, submission.job_id, child_attempt_id, "worker-child")
    progress = _ExecutionProgress(
        store=jobs,
        job_id=submission.job_id,
        attempt_id=child_attempt_id,
        worker_id="worker-child",
        lease_seconds=60,
        control=_ExecutionControl(),
        task_key="p7-cleanup-execution",
    )
    with pytest.raises(PersistentExecutionCancelled):
        progress.checkpoint()


@pytest.mark.parametrize("parent_state", ["expired", "cancel_requested", "retry_wait", "wrong_worker"])
def test_resubscribe_cleanup_staging_requires_the_live_parent_attempt(
    p7_runtime: DatabaseRuntime,
    parent_state: str,
) -> None:
    task_id = {"expired": 727, "cancel_requested": 734, "retry_wait": 735, "wrong_worker": 745}[parent_state]
    _insert_cleanup(p7_runtime, task_id)
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")
    if parent_state == "expired":
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text("UPDATE job_attempts SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = :id"),
                {"id": parent_attempt_id},
            )
    elif parent_state == "cancel_requested":
        jobs.request_cancel(parent.job.id)
    else:
        jobs.fail_attempt(
            job_id=parent.job.id,
            attempt_id=parent_attempt_id,
            worker_id="worker-parent",
            error_type="fixture_parent_retry",
            error_message="fixture parent retry",
            retryable=True,
            next_attempt_at=datetime.now(UTC) + timedelta(minutes=1),
            honor_cancel_request=False,
        )

    with pytest.raises(CleanupExecutionConflictError, match="attempt-stale"):
        _repository(p7_runtime).stage_version_deletion(
            "emby-old",
            parent_job_id=parent.job.id,
            parent_attempt_id=parent_attempt_id,
            parent_worker_id="worker-other" if parent_state == "wrong_worker" else "worker-parent",
        )

    with p7_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM job_runs WHERE task_key = 'p7-cleanup-execution'")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM cleanup_execution_items")) == 0


def test_resubscribe_cleanup_children_keep_first_persisted_workflow_steps(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 728, emby_id="emby-a")
    _insert_cleanup(p7_runtime, 729, emby_id="emby-b")
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(task_key="resubscribe-library", payload={}, max_attempts=2).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")
    repository = _repository(p7_runtime)

    first_a = repository.stage_version_deletion(
        "emby-a",
        parent_job_id=parent.job.id,
        parent_attempt_id=parent_attempt_id,
        parent_worker_id="worker-parent",
    )
    jobs.fail_attempt(
        job_id=parent.job.id,
        attempt_id=parent_attempt_id,
        worker_id="worker-parent",
        error_type="fixture_parent_retry",
        error_message="fixture parent retry after staging A",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    retry_attempt_id = retries[0].id
    _start_attempt(p7_runtime, parent.job.id, retry_attempt_id, "worker-parent-retry")
    first_b = repository.stage_version_deletion(
        "emby-b",
        parent_job_id=parent.job.id,
        parent_attempt_id=retry_attempt_id,
        parent_worker_id="worker-parent-retry",
    )
    replay_a = repository.stage_version_deletion(
        "emby-a",
        parent_job_id=parent.job.id,
        parent_attempt_id=retry_attempt_id,
        parent_worker_id="worker-parent-retry",
    )

    assert replay_a.job_id == first_a.job_id
    assert replay_a.reused is True
    with p7_runtime.engine.connect() as connection:
        children = connection.execute(
            text(
                """
                SELECT id, workflow_step,
                       (SELECT COUNT(*) FROM job_attempts WHERE job_id = job_runs.id) AS attempts,
                       (SELECT COUNT(*) FROM outbox_events
                        WHERE aggregate_type = 'job_attempt'
                          AND aggregate_id IN (
                              SELECT id::text FROM job_attempts WHERE job_id = job_runs.id
                          )) AS dispatches
                FROM job_runs
                WHERE parent_job_id = :parent_job_id
                ORDER BY workflow_step
                """
            ),
            {"parent_job_id": parent.job.id},
        ).all()
    assert children == [
        (first_a.job_id, 1, 1, 1),
        (first_b.job_id, 2, 1, 1),
    ]


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
                    WHERE cleanup_index_id = :cleanup_index_id
                      AND status IN ('queued','claimed','effect_started')
                    """
                ),
                {"cleanup_index_id": task_id},
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


class _Progress:
    def __init__(self, *, cancelled: bool = False) -> None:
        self.cancelled = cancelled

    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        if self.cancelled:
            raise RuntimeError("fixture cancellation")

    def control_state(self) -> dict[str, bool]:
        return {"cancel_requested": self.cancelled, "timed_out": False}


class _Local:
    def __init__(self) -> None:
        self.calls = 0

    def reconcile(self, target: Any) -> CleanupEffectResult:
        del target
        self.calls += 1
        return CleanupEffectResult.confirmed({"absent": True})


class _Media:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def reconcile(self, target: Any, *, operation_id: str) -> CleanupEffectResult:
        del target
        self.calls.append(operation_id)
        return CleanupEffectResult.confirmed({"receipt": operation_id})


class _Provider:
    def __init__(
        self,
        *,
        first: CleanupEffectResult | None = None,
        readback: CleanupEffectResult | None = None,
    ) -> None:
        self.first = first or CleanupEffectResult.confirmed()
        self.readback_result = readback or CleanupEffectResult.confirmed({"absent": True})
        self.requests = 0
        self.readbacks = 0

    def request(self, target: Any) -> CleanupEffectResult:
        del target
        self.requests += 1
        return self.first

    def readback(self, target: Any) -> CleanupEffectResult:
        del target
        self.readbacks += 1
        return self.readback_result


class _PerItemRecoveryProvider:
    def __init__(self) -> None:
        self.requests: dict[str, int] = {}
        self.readbacks: dict[str, int] = {}

    def request(self, target: Any) -> CleanupEffectResult:
        emby_id = str(target["emby_id"])
        self.requests[emby_id] = self.requests.get(emby_id, 0) + 1
        return CleanupEffectResult.retryable({"type": "timeout_unknown"})

    def readback(self, target: Any) -> CleanupEffectResult:
        emby_id = str(target["emby_id"])
        count = self.readbacks.get(emby_id, 0) + 1
        self.readbacks[emby_id] = count
        if emby_id == "emby-old-a" and count == 1:
            return CleanupEffectResult.retryable({"type": "replica_lag"})
        return CleanupEffectResult.confirmed({"absent": True})


def _start_attempt(runtime: DatabaseRuntime, job_id: UUID, attempt_id: UUID, worker_id: str = "worker-a") -> None:
    jobs = PostgresJobStore(runtime.unit_of_work)
    claim = jobs.claim_attempt(
        job_id=job_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_seconds=60,
    )
    assert claim.claimed is True
    jobs.start_attempt(job_id=job_id, attempt_id=attempt_id, worker_id=worker_id, lease_seconds=60)


def _current_attempt(runtime: DatabaseRuntime, job_id: UUID) -> UUID:
    projection = PostgresJobStore(runtime.unit_of_work).get_job(job_id)
    assert projection is not None
    return projection.current_attempt.id


def _execution_service(
    runtime: DatabaseRuntime,
    *,
    p115: _Provider | None = None,
    readback_budget: int = 3,
) -> tuple[CleanupExecutionService, _Local, _Media, _Provider, _Provider]:
    local = _Local()
    media = _Media()
    p115 = p115 or _Provider()
    emby = _Provider()
    return (
        CleanupExecutionService(
            SqlAlchemyCleanupExecutionRepository(runtime.unit_of_work),
            local=local,
            media=media,
            p115=p115,
            emby=emby,
            readback_budget=readback_budget,
        ),
        local,
        media,
        p115,
        emby,
    )


def test_cleanup_runtime_confirms_receipt_and_projection_under_current_attempt(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 708)
    submission = _repository(p7_runtime).stage_cleanup([708])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    service, local, media, p115, emby = _execution_service(p7_runtime)

    result = service.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        progress=_Progress(),
    )
    PostgresJobStore(p7_runtime.unit_of_work).succeed_attempt(
        job_id=submission.job_id,
        attempt_id=attempt_id,
        worker_id="worker-a",
        result=result,
    )

    assert result["status"] == "confirmed"
    assert local.calls == len(media.calls) == p115.requests == emby.requests == 1
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT item.status, item.receipt_json, item.current_attempt_id,
                           idx.status AS cleanup_status, job.status AS job_status
                    FROM cleanup_execution_items AS item
                    JOIN cleanup_index AS idx ON idx.id = item.cleanup_index_id
                    JOIN job_runs AS job ON job.id = item.job_id
                    WHERE item.job_id = :job_id
                    """
                ),
                {"job_id": submission.job_id},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "confirmed"
    assert row["receipt_json"]["phases"]["emby"]["status"] == "confirmed"
    assert row["current_attempt_id"] is None
    assert row["cleanup_status"] == "processed"
    assert row["job_status"] == "succeeded"


def test_cleanup_runtime_restart_rebinds_attempt_and_only_reads_back_checkpointed_provider(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 709)
    submission = _repository(p7_runtime).stage_cleanup([709])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    first_attempt = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, first_attempt)
    p115 = _Provider(first=CleanupEffectResult.retryable({"type": "timeout_unknown"}))
    service, *_ = _execution_service(p7_runtime, p115=p115)

    with pytest.raises(CleanupExecutionPending):
        service.execute(
            job_id=submission.job_id,
            execution_key=submission.execution_key,
            attempt_id=first_attempt,
            worker_id="worker-a",
            progress=_Progress(),
        )
    jobs.fail_attempt(
        job_id=submission.job_id,
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
    _start_attempt(p7_runtime, submission.job_id, second_attempt, "worker-b")

    result = service.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=second_attempt,
        worker_id="worker-b",
        progress=_Progress(cancelled=True),
    )

    assert result["status"] == "confirmed"
    assert p115.requests == 1
    assert p115.readbacks == 1
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                SELECT status, current_attempt_id, claimed_by, claim_token
                FROM cleanup_execution_items
                WHERE job_id = :job_id
                """
                ),
                {"job_id": submission.job_id},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {
        "status": "confirmed",
        "current_attempt_id": None,
        "claimed_by": None,
        "claim_token": None,
    }


def test_cleanup_runtime_rejects_stale_revision_and_cancels_only_before_effect(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 710)
    submission = _repository(p7_runtime).stage_cleanup([710])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claimed = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert claimed is not None
    updated = repository.save_receipt(
        claimed,
        {
            "schema_version": 1,
            "item_id": str(claimed.id),
            "item_key": claimed.item_key,
            "phases": {},
        },
        start_effect=False,
        lease_seconds=60,
    )
    with pytest.raises(CleanupExecutionClaimLostError, match="stale"):
        repository.save_receipt(
            replace(updated, claim_token=uuid4()),
            updated.receipt or {},
            start_effect=False,
            lease_seconds=60,
        )
    with pytest.raises(CleanupExecutionClaimLostError, match="stale"):
        repository.save_receipt(
            claimed,
            updated.receipt or {},
            start_effect=False,
            lease_seconds=60,
        )

    repository.release_for_retry(updated, {"type": "fixture"})
    service, local, media, p115, emby = _execution_service(p7_runtime)
    result = service.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        progress=_Progress(cancelled=True),
    )
    assert result["status"] == "cancelled"
    assert local.calls == len(media.calls) == p115.requests == emby.requests == 0


def test_cleanup_runtime_fences_expired_attempt_and_refreshes_only_an_expired_item_lease(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 730)
    submission = _repository(p7_runtime).stage_cleanup([730])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    first_claim = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert first_claim is not None
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE cleanup_execution_items "
                "SET claimed_at = NOW() - INTERVAL '2 seconds', "
                "lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = :item_id"
            ),
            {"item_id": first_claim.id},
        )

    refreshed = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert refreshed is not None
    assert refreshed.revision == first_claim.revision + 1
    assert refreshed.claim_token != first_claim.claim_token
    with pytest.raises(CleanupExecutionClaimLostError, match="stale"):
        repository.save_receipt(
            first_claim,
            {"schema_version": 1, "item_id": str(first_claim.id), "item_key": first_claim.item_key},
            start_effect=True,
            lease_seconds=60,
        )

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE job_attempts SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = :id"),
            {"id": attempt_id},
        )
    with pytest.raises(CleanupExecutionClaimLostError, match="Attempt is no longer current"):
        repository.save_receipt(
            refreshed,
            {"schema_version": 1, "item_id": str(refreshed.id), "item_key": refreshed.item_key},
            start_effect=True,
            lease_seconds=60,
        )
    with pytest.raises(CleanupExecutionClaimLostError, match="Attempt is no longer current"):
        repository.claim_next(
            job_id=submission.job_id,
            execution_key=submission.execution_key,
            attempt_id=attempt_id,
            worker_id="worker-a",
            lease_seconds=60,
        )
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT effect_started_at FROM cleanup_execution_items WHERE id = :id"),
                {"id": refreshed.id},
            )
            is None
        )


@pytest.mark.parametrize("cancel_target", ["parent", "child"])
def test_cleanup_cancel_committed_before_first_effect_fences_the_adapter_checkpoint(
    p7_runtime: DatabaseRuntime,
    cancel_target: str,
) -> None:
    _insert_cleanup(p7_runtime, 731 if cancel_target == "parent" else 736)
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")
    submission = _repository(p7_runtime).stage_version_deletion(
        "emby-old",
        parent_job_id=parent.job.id,
        parent_attempt_id=parent_attempt_id,
        parent_worker_id="worker-parent",
    )
    child_attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, child_attempt_id, "worker-child")
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claim = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=child_attempt_id,
        worker_id="worker-child",
        lease_seconds=60,
    )
    assert claim is not None
    jobs.request_cancel(parent.job.id if cancel_target == "parent" else submission.job_id)

    with pytest.raises(CleanupExecutionClaimLostError, match="fenced by Job state"):
        repository.save_receipt(
            claim,
            {"schema_version": 1, "item_id": str(claim.id), "item_key": claim.item_key},
            start_effect=True,
            lease_seconds=60,
        )
    with p7_runtime.engine.connect() as connection:
        row = connection.execute(
            text("SELECT status, effect_started_at FROM cleanup_execution_items WHERE id = :id"),
            {"id": claim.id},
        ).one()
    assert row == ("claimed", None)


@pytest.mark.parametrize("cancel_target", ["parent", "child"])
def test_cleanup_effect_checkpoint_committed_before_cancel_allows_only_recovery_updates(
    p7_runtime: DatabaseRuntime,
    cancel_target: str,
) -> None:
    _insert_cleanup(p7_runtime, 732 if cancel_target == "parent" else 737)
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")
    submission = _repository(p7_runtime).stage_version_deletion(
        "emby-old",
        parent_job_id=parent.job.id,
        parent_attempt_id=parent_attempt_id,
        parent_worker_id="worker-parent",
    )
    child_attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, child_attempt_id, "worker-child")
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claim = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=child_attempt_id,
        worker_id="worker-child",
        lease_seconds=60,
    )
    assert claim is not None
    receipt = {"schema_version": 1, "item_id": str(claim.id), "item_key": claim.item_key}
    started = repository.save_receipt(claim, receipt, start_effect=True, lease_seconds=60)
    jobs.request_cancel(parent.job.id if cancel_target == "parent" else submission.job_id)

    recovered = repository.save_receipt(started, receipt, start_effect=True, lease_seconds=60)

    assert recovered.effect_started is True
    assert recovered.revision == started.revision + 1


@pytest.mark.parametrize(
    ("parent_state", "task_id", "effect_allowed"),
    [
        ("succeeded", 738, True),
        ("retry_wait", 739, False),
        ("failed", 740, False),
        ("timed_out", 741, False),
    ],
)
def test_cleanup_first_effect_accepts_only_a_running_or_succeeded_parent(
    p7_runtime: DatabaseRuntime,
    parent_state: str,
    task_id: int,
    effect_allowed: bool,
) -> None:
    _insert_cleanup(p7_runtime, task_id)
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(
        task_key="resubscribe-library",
        payload={},
        max_attempts=2,
    ).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")
    submission = _repository(p7_runtime).stage_version_deletion(
        "emby-old",
        parent_job_id=parent.job.id,
        parent_attempt_id=parent_attempt_id,
        parent_worker_id="worker-parent",
    )
    child_attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, child_attempt_id, "worker-child")
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claim = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=child_attempt_id,
        worker_id="worker-child",
        lease_seconds=60,
    )
    assert claim is not None
    if parent_state == "succeeded":
        jobs.succeed_attempt(
            job_id=parent.job.id,
            attempt_id=parent_attempt_id,
            worker_id="worker-parent",
            result={"status": "deferred"},
            honor_cancel_request=False,
        )
    elif parent_state == "retry_wait":
        jobs.fail_attempt(
            job_id=parent.job.id,
            attempt_id=parent_attempt_id,
            worker_id="worker-parent",
            error_type="fixture_retry",
            error_message="fixture retry",
            retryable=True,
            next_attempt_at=datetime.now(UTC) + timedelta(minutes=1),
            honor_cancel_request=False,
        )
    elif parent_state == "failed":
        jobs.fail_attempt(
            job_id=parent.job.id,
            attempt_id=parent_attempt_id,
            worker_id="worker-parent",
            error_type="fixture_failure",
            error_message="fixture failure",
            retryable=False,
            honor_cancel_request=False,
        )
    else:
        jobs.timeout_attempt(
            job_id=parent.job.id,
            attempt_id=parent_attempt_id,
            worker_id="worker-parent",
            retryable=False,
            honor_cancel_request=False,
        )
    receipt = {"schema_version": 1, "item_id": str(claim.id), "item_key": claim.item_key}

    if effect_allowed:
        started = repository.save_receipt(claim, receipt, start_effect=True, lease_seconds=60)
        assert started.effect_started is True
    else:
        with pytest.raises(CleanupExecutionClaimLostError, match="fenced by Job state"):
            repository.save_receipt(claim, receipt, start_effect=True, lease_seconds=60)


def test_cleanup_confirmation_and_single_staging_share_cleanup_index_then_item_lock_order(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 742)
    submission = _repository(p7_runtime).stage_cleanup([742])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    runtime_repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claim = runtime_repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert claim is not None
    receipt = {"schema_version": 1, "item_id": str(claim.id), "item_key": claim.item_key}
    started = runtime_repository.save_receipt(claim, receipt, start_effect=True, lease_seconds=60)
    stage_pids: Queue[int] = Queue()
    confirm_pids: Queue[int] = Queue()

    def observed_stage_uow() -> SqlAlchemyUnitOfWork:
        return _ObservedUnitOfWork(p7_runtime, stage_pids)

    def observed_confirm_uow() -> SqlAlchemyUnitOfWork:
        return _ObservedUnitOfWork(p7_runtime, confirm_pids)

    staging_repository = SqlAlchemyCleanupRepository(
        observed_stage_uow,
        SqlAlchemyCleanupMediaProjection(),
        SettingsCleanupExecutionPolicy(
            lambda: Settings(
                local_strm_root="/fixture",
                emby_server_url="http://emby.fixture.invalid",
                emby_api_key=SecretStr("fixture-token"),
            )
        ),
    )
    confirming_repository = SqlAlchemyCleanupExecutionRepository(observed_confirm_uow)
    guard = p7_runtime.engine.connect()
    transaction = guard.begin()
    guard.execute(text("SELECT id FROM cleanup_index WHERE id = 742 FOR UPDATE"))

    def wait_until_blocked(pid: int) -> None:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            blockers = guard.scalar(text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid})
            if int(blockers or 0) > 0:
                return
            time.sleep(0.01)
        raise AssertionError(f"PostgreSQL backend {pid} did not enter the expected row-lock wait")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            staged = executor.submit(staging_repository.stage_version_deletion, "emby-old")
            wait_until_blocked(stage_pids.get(timeout=5))
            confirmed = executor.submit(confirming_repository.mark_confirmed, started, receipt)
            wait_until_blocked(confirm_pids.get(timeout=5))
            transaction.commit()
            with pytest.raises(CleanupExecutionConflictError, match="already claimed"):
                staged.result(timeout=10)
            confirmed.result(timeout=10)
    finally:
        if transaction.is_active:
            transaction.rollback()
        guard.close()

    with p7_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT item.status, idx.status
                FROM cleanup_execution_items AS item
                JOIN cleanup_index AS idx ON idx.id = item.cleanup_index_id
                WHERE item.id = :item_id
                """
            ),
            {"item_id": started.id},
        ).one()
    assert row == ("confirmed", "processed")


@pytest.mark.parametrize(("winner", "task_id"), [("cancel", 743), ("effect", 744)])
def test_cleanup_parent_cancel_and_first_effect_checkpoint_are_database_linearized(
    p7_runtime: DatabaseRuntime,
    winner: str,
    task_id: int,
) -> None:
    _insert_cleanup(p7_runtime, task_id)
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_attempt_id = parent.current_attempt.id
    _start_attempt(p7_runtime, parent.job.id, parent_attempt_id, "worker-parent")
    submission = _repository(p7_runtime).stage_version_deletion(
        "emby-old",
        parent_job_id=parent.job.id,
        parent_attempt_id=parent_attempt_id,
        parent_worker_id="worker-parent",
    )
    child_attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, child_attempt_id, "worker-child")
    claim_repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claim = claim_repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=child_attempt_id,
        worker_id="worker-child",
        lease_seconds=60,
    )
    assert claim is not None
    receipt = {"schema_version": 1, "item_id": str(claim.id), "item_key": claim.item_key}
    cancel_pids: Queue[int] = Queue()
    effect_pids: Queue[int] = Queue()

    def observed_cancel_uow() -> SqlAlchemyUnitOfWork:
        return _ObservedUnitOfWork(p7_runtime, cancel_pids)

    def observed_effect_uow() -> SqlAlchemyUnitOfWork:
        return _ObservedUnitOfWork(p7_runtime, effect_pids)

    cancelling_jobs = PostgresJobStore(observed_cancel_uow)
    effect_repository = SqlAlchemyCleanupExecutionRepository(observed_effect_uow)
    guard = p7_runtime.engine.connect()
    transaction = guard.begin()
    guard.execute(text("SELECT id FROM job_runs WHERE id = :job_id FOR UPDATE"), {"job_id": parent.job.id})

    def wait_until_blocked(pid: int) -> None:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            blockers = guard.scalar(text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid})
            if int(blockers or 0) > 0:
                return
            time.sleep(0.01)
        raise AssertionError(f"PostgreSQL backend {pid} did not enter the expected row-lock wait")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            if winner == "cancel":
                cancel_future = executor.submit(cancelling_jobs.request_cancel, parent.job.id)
                wait_until_blocked(cancel_pids.get(timeout=5))
                effect_future = executor.submit(
                    effect_repository.save_receipt,
                    claim,
                    receipt,
                    start_effect=True,
                    lease_seconds=60,
                )
                wait_until_blocked(effect_pids.get(timeout=5))
            else:
                effect_future = executor.submit(
                    effect_repository.save_receipt,
                    claim,
                    receipt,
                    start_effect=True,
                    lease_seconds=60,
                )
                wait_until_blocked(effect_pids.get(timeout=5))
                cancel_future = executor.submit(cancelling_jobs.request_cancel, parent.job.id)
                wait_until_blocked(cancel_pids.get(timeout=5))
            transaction.commit()
            cancel_future.result(timeout=10)
            if winner == "cancel":
                with pytest.raises(CleanupExecutionClaimLostError, match="fenced by Job state"):
                    effect_future.result(timeout=10)
            else:
                assert effect_future.result(timeout=10).effect_started is True
    finally:
        if transaction.is_active:
            transaction.rollback()
        guard.close()

    with p7_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT item.status, item.effect_started_at IS NOT NULL, parent.status
                FROM cleanup_execution_items AS item
                JOIN job_runs AS child ON child.id = item.job_id
                JOIN job_runs AS parent ON parent.id = child.parent_job_id
                WHERE item.id = :item_id
                """
            ),
            {"item_id": claim.id},
        ).one()
    assert row == (
        "claimed" if winner == "cancel" else "effect_started",
        winner == "effect",
        "cancel_requested",
    )


def test_cleanup_batch_and_single_version_share_one_physical_conflict_key(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 711)
    repository = _repository(p7_runtime)
    staged = repository.stage_cleanup([711])

    with pytest.raises(CleanupExecutionConflictError, match="already claimed"):
        repository.stage_version_deletion("emby-old")

    with p7_runtime.engine.connect() as connection:
        item_key = connection.scalar(
            text("SELECT item_key FROM cleanup_execution_items WHERE job_id = :job_id"),
            {"job_id": staged.job_id},
        )
    assert item_key == "version:emby-old"


def test_cleanup_terminal_job_reconciliation_closes_pre_and_post_effect_items(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 712)
    submission = _repository(p7_runtime).stage_cleanup([712])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claimed = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert claimed is not None
    started = repository.save_receipt(
        claimed,
        {
            "schema_version": 1,
            "item_id": str(claimed.id),
            "item_key": claimed.item_key,
            "phases": {},
        },
        start_effect=True,
        lease_seconds=60,
    )
    del started
    PostgresJobStore(p7_runtime.unit_of_work).fail_attempt(
        job_id=submission.job_id,
        attempt_id=attempt_id,
        worker_id="worker-a",
        error_type="fixture_terminal",
        error_message="fixture terminal",
        retryable=False,
        honor_cancel_request=False,
    )

    assert repository.reconcile_terminal_jobs() == (0, 1)
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT status, error_json FROM cleanup_execution_items WHERE job_id = :job_id"),
                {"job_id": submission.job_id},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "indeterminate"
    assert row["error_json"]["type"] == "cleanup_job_terminal_after_effect"


def test_cleanup_stale_pre_effect_claim_rebinds_and_fences_the_old_attempt(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 713)
    submission = _repository(p7_runtime).stage_cleanup([713])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    first_attempt = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, first_attempt)
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    first_claim = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=first_attempt,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert first_claim is not None

    jobs.fail_attempt(
        job_id=submission.job_id,
        attempt_id=first_attempt,
        worker_id="worker-a",
        error_type="fixture_restart_before_effect",
        error_message="fixture restart before effect",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id
    _start_attempt(p7_runtime, submission.job_id, second_attempt, "worker-b")
    rebound = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=second_attempt,
        worker_id="worker-b",
        lease_seconds=60,
    )
    assert rebound is not None
    assert rebound.attempt_id == second_attempt
    assert rebound.claim_token != first_claim.claim_token
    assert rebound.effect_started is False

    with pytest.raises(CleanupExecutionClaimLostError, match="Attempt is no longer current"):
        repository.save_receipt(
            first_claim,
            {
                "schema_version": 1,
                "item_id": str(first_claim.id),
                "item_key": first_claim.item_key,
                "phases": {},
            },
            start_effect=False,
            lease_seconds=60,
        )
    assert (
        repository.cancel_unstarted_job(
            submission.job_id,
            {"type": "fixture_cancel_after_rebind"},
        )
        == 1
    )


def test_cleanup_recovery_budget_exhaustion_is_indeterminate_without_provider_resend(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 714)
    submission = _repository(p7_runtime).stage_cleanup([714])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    first_attempt = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, first_attempt)
    p115 = _Provider(
        first=CleanupEffectResult.retryable({"type": "timeout_unknown"}),
        readback=CleanupEffectResult.retryable({"type": "still_present"}),
    )
    service, *_ = _execution_service(p7_runtime, p115=p115, readback_budget=1)

    with pytest.raises(CleanupExecutionPending):
        service.execute(
            job_id=submission.job_id,
            execution_key=submission.execution_key,
            attempt_id=first_attempt,
            worker_id="worker-a",
            progress=_Progress(),
        )
    jobs.fail_attempt(
        job_id=submission.job_id,
        attempt_id=first_attempt,
        worker_id="worker-a",
        error_type="fixture_readback",
        error_message="fixture readback",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id
    _start_attempt(p7_runtime, submission.job_id, second_attempt, "worker-b")

    result = service.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=second_attempt,
        worker_id="worker-b",
        progress=_Progress(cancelled=True),
    )
    assert result["status"] == "indeterminate"
    jobs.fail_attempt(
        job_id=submission.job_id,
        attempt_id=second_attempt,
        worker_id="worker-b",
        error_type="CleanupExecutionIndeterminate",
        error_message="fixture recovery budget exhausted",
        retryable=False,
        honor_cancel_request=False,
    )

    assert p115.requests == 1
    assert p115.readbacks == 1
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT item.status, item.error_json, idx.status AS cleanup_status,
                           job.status AS job_status
                    FROM cleanup_execution_items AS item
                    JOIN cleanup_index AS idx ON idx.id = item.cleanup_index_id
                    JOIN job_runs AS job ON job.id = item.job_id
                    WHERE item.job_id = :job_id
                    """
                ),
                {"job_id": submission.job_id},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "indeterminate"
    assert row["error_json"]["recovery_budget_exhausted"] is True
    assert row["cleanup_status"] == "pending"
    assert row["job_status"] == "failed"


def test_cleanup_dead_dispatch_closes_unstarted_item_without_effect(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 715)
    submission = _repository(p7_runtime).stage_cleanup([715])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET max_publish_attempts = 1
                WHERE aggregate_type = 'job_attempt'
                  AND aggregate_id = :attempt_id
                """
            ),
            {"attempt_id": str(_current_attempt(p7_runtime, submission.job_id))},
        )
    claims = jobs.claim_outbox(publisher_id="fixture-publisher", limit=1)
    assert len(claims) == 1
    status = jobs.mark_outbox_failed(
        claims[0].event.id,
        claims[0].claim_token,
        error="fixture broker unavailable",
        backoff_seconds=0,
    )
    assert status is OutboxStatus.DEAD

    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    assert repository.reconcile_terminal_jobs() == (1, 0)
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT item.status AS item_status, item.error_json,
                           job.status AS job_status, attempt.status AS attempt_status,
                           event.status AS outbox_status
                    FROM cleanup_execution_items AS item
                    JOIN job_runs AS job ON job.id = item.job_id
                    JOIN job_attempts AS attempt ON attempt.job_id = job.id
                    JOIN outbox_events AS event
                      ON event.aggregate_type = 'job_attempt'
                     AND event.aggregate_id = attempt.id::text
                    WHERE item.job_id = :job_id
                    """
                ),
                {"job_id": submission.job_id},
            )
            .mappings()
            .one()
        )
    assert row["item_status"] == "cancelled"
    assert row["error_json"]["type"] == "cleanup_job_terminal_before_effect"
    assert row["job_status"] == "failed"
    assert row["attempt_status"] == "failed"
    assert row["outbox_status"] == "dead"


def test_cleanup_recovery_budget_is_per_item_in_a_multi_item_job(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_multi_version_cleanup(p7_runtime, 716)
    submission = _repository(p7_runtime).stage_cleanup([716])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    projection = jobs.get_job(submission.job_id)
    assert projection is not None
    assert projection.job.max_attempts == cleanup_job_max_attempts(2)
    provider = _PerItemRecoveryProvider()
    local = _Local()
    media = _Media()
    service = CleanupExecutionService(
        SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work),
        local=local,
        media=media,
        p115=provider,
        emby=_Provider(),
    )

    attempt_id = projection.current_attempt.id
    worker_id = "worker-a"
    for attempt_no in range(1, 5):
        _start_attempt(p7_runtime, submission.job_id, attempt_id, worker_id)
        if attempt_no < 4:
            with pytest.raises(CleanupExecutionPending):
                service.execute(
                    job_id=submission.job_id,
                    execution_key=submission.execution_key,
                    attempt_id=attempt_id,
                    worker_id=worker_id,
                    progress=_Progress(),
                )
            jobs.fail_attempt(
                job_id=submission.job_id,
                attempt_id=attempt_id,
                worker_id=worker_id,
                error_type="fixture_readback",
                error_message="fixture readback",
                retryable=True,
                next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
                honor_cancel_request=False,
            )
            retries = jobs.enqueue_due_retries()
            assert len(retries) == 1
            attempt_id = retries[0].id
            worker_id = f"worker-{attempt_no + 1}"
            continue

        result = service.execute(
            job_id=submission.job_id,
            execution_key=submission.execution_key,
            attempt_id=attempt_id,
            worker_id=worker_id,
            progress=_Progress(),
        )
        jobs.succeed_attempt(
            job_id=submission.job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
            result=result,
            honor_cancel_request=False,
        )

    assert result["status"] == "confirmed"
    assert provider.requests == {"emby-old-a": 1, "emby-old-b": 1}
    assert provider.readbacks == {"emby-old-a": 2, "emby-old-b": 1}
    with p7_runtime.engine.connect() as connection:
        states = connection.execute(
            text(
                """
                SELECT item_key, status
                FROM cleanup_execution_items
                WHERE job_id = :job_id
                ORDER BY item_key
                """
            ),
            {"job_id": submission.job_id},
        ).all()
    assert states == [("version:emby-old-a", "confirmed"), ("version:emby-old-b", "confirmed")]


def test_cleanup_post_effect_cancellation_preserves_readback_and_cancels_untouched_items(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_multi_version_cleanup(p7_runtime, 717)
    submission = _repository(p7_runtime).stage_cleanup([717])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claimed = repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert claimed is not None
    repository.save_receipt(
        claimed,
        {
            "schema_version": 1,
            "item_id": str(claimed.id),
            "item_key": claimed.item_key,
            "phases": {},
        },
        start_effect=True,
        lease_seconds=60,
    )

    assert repository.prepare_post_effect_cancellation(submission.job_id) is True
    with p7_runtime.engine.connect() as connection:
        states = (
            connection.execute(
                text(
                    """
                SELECT item_key, status, error_json
                FROM cleanup_execution_items
                WHERE job_id = :job_id
                ORDER BY item_key
                """
                ),
                {"job_id": submission.job_id},
            )
            .mappings()
            .all()
        )
    by_key = {row["item_key"]: row for row in states}
    assert by_key[claimed.item_key]["status"] == "effect_started"
    untouched_key = ({"version:emby-old-a", "version:emby-old-b"} - {claimed.item_key}).pop()
    assert by_key[untouched_key]["status"] == "cancelled"
    assert by_key[untouched_key]["error_json"]["type"] == "cancelled_after_other_cleanup_effect"


def test_cleanup_retry_wait_cancel_preserves_authoritative_readback(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 746)
    submission = _repository(p7_runtime).stage_cleanup([746])
    cancellation_policy = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    jobs = PostgresJobStore(
        p7_runtime.unit_of_work,
        cancellation_policy=cancellation_policy,
    )
    first_attempt = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, first_attempt)
    p115 = _Provider(first=CleanupEffectResult.retryable({"type": "timeout_unknown"}))
    service, *_ = _execution_service(p7_runtime, p115=p115)

    with pytest.raises(CleanupExecutionPending):
        service.execute(
            job_id=submission.job_id,
            execution_key=submission.execution_key,
            attempt_id=first_attempt,
            worker_id="worker-a",
            progress=_Progress(),
        )
    retry_wait = jobs.fail_attempt(
        job_id=submission.job_id,
        attempt_id=first_attempt,
        worker_id="worker-a",
        error_type="fixture_readback",
        error_message="fixture readback",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    assert retry_wait.job.status.value == "retry_wait"

    protected = jobs.request_cancel(submission.job_id)
    assert protected.job.status.value == "retry_wait"
    assert protected.job.cancel_requested_at is not None
    repeated = jobs.request_cancel(submission.job_id)
    assert repeated.job.status.value == "retry_wait"
    assert repeated.job.cancel_requested_at == protected.job.cancel_requested_at
    assert cancellation_policy.reconcile_terminal_jobs() == (0, 0)

    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id
    queued = jobs.request_cancel(submission.job_id)
    assert queued.job.status.value == "queued"
    _start_attempt(p7_runtime, submission.job_id, second_attempt, "worker-b")
    result = service.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=second_attempt,
        worker_id="worker-b",
        progress=_Progress(cancelled=True),
    )
    completed = jobs.succeed_attempt(
        job_id=submission.job_id,
        attempt_id=second_attempt,
        worker_id="worker-b",
        result=result,
        honor_cancel_request=False,
    )

    assert result["status"] == "confirmed"
    assert completed.job.status.value == "succeeded"
    assert p115.requests == 1
    assert p115.readbacks == 1
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                    SELECT status
                    FROM cleanup_execution_items
                    WHERE job_id = :job_id
                    """
                ),
                {"job_id": submission.job_id},
            )
            == "confirmed"
        )


def test_cleanup_pre_effect_retry_wait_cancel_stays_terminal(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 747)
    submission = _repository(p7_runtime).stage_cleanup([747])
    jobs = PostgresJobStore(
        p7_runtime.unit_of_work,
        cancellation_policy=SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work),
    )

    cancelled = jobs.request_cancel(submission.job_id)

    assert cancelled.job.status.value == "cancelled"
    assert SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work).reconcile_terminal_jobs() == (1, 0)


def test_cleanup_stale_post_effect_cancel_is_recovered_before_generic_job_reaper(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_multi_version_cleanup(p7_runtime, 718)
    submission = _repository(p7_runtime).stage_cleanup([718])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    first_attempt = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, first_attempt)
    p115 = _Provider(first=CleanupEffectResult.retryable({"type": "timeout_unknown"}))
    service, local, media, *_ = _execution_service(p7_runtime, p115=p115)
    with pytest.raises(CleanupExecutionPending):
        service.execute(
            job_id=submission.job_id,
            execution_key=submission.execution_key,
            attempt_id=first_attempt,
            worker_id="worker-a",
            progress=_Progress(),
        )

    requested = jobs.request_cancel(submission.job_id)
    assert requested.job.status.value == "cancel_requested"
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE job_attempts
                SET lease_expires_at = NOW() - INTERVAL '1 second'
                WHERE id = :attempt_id
                """
            ),
            {"attempt_id": first_attempt},
        )
    repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    assert repository.protect_stale_post_effect_cancellations() == 1
    recovered = jobs.recover_stale_attempts(retry_delay_seconds=0)
    assert recovered.lost_attempts == 1
    assert recovered.requeued_jobs == 1
    assert recovered.cancelled_jobs == 0
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*)
                FROM cleanup_execution_items
                WHERE job_id = :job_id AND status = 'cancelled'
                """
                ),
                {"job_id": submission.job_id},
            )
            == 1
        )
    retries = jobs.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id
    _start_attempt(p7_runtime, submission.job_id, second_attempt, "worker-b")

    result = service.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=second_attempt,
        worker_id="worker-b",
        progress=_Progress(),
    )
    jobs.fail_attempt(
        job_id=submission.job_id,
        attempt_id=second_attempt,
        worker_id="worker-b",
        error_type="CleanupExecutionPartial",
        error_message="fixture cancellation preserved",
        retryable=False,
        honor_cancel_request=False,
    )

    assert result["status"] == "partial"
    assert p115.requests == 1
    assert p115.readbacks == 1
    assert local.calls == len(media.calls) == 1
    completed = jobs.get_job(submission.job_id)
    assert completed is not None
    assert completed.job.status.value == "failed"


@pytest.mark.parametrize("mutation", ["ignore", "delete"])
def test_single_version_execution_blocks_cleanup_index_mutation(
    p7_runtime: DatabaseRuntime,
    mutation: str,
) -> None:
    task_id = 719 if mutation == "ignore" else 720
    _insert_cleanup(p7_runtime, task_id, suffix=mutation)
    repository = _repository(p7_runtime)
    repository.stage_version_deletion("emby-old")

    with pytest.raises(CleanupExecutionConflictError, match="already claimed"):
        if mutation == "ignore":
            repository.ignore_tasks([task_id])
        else:
            repository.delete_tasks([task_id])


def test_single_version_execution_blocks_subscription_core_batch_delete(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 721)
    _repository(p7_runtime).stage_version_deletion("emby-old")

    with p7_runtime.unit_of_work() as uow:
        with pytest.raises(SubscriptionStateConflictError, match="正在执行清理任务"):
            SqlAlchemySubscriptionCoreRepository._ensure_batch_delete_available(
                uow,
                SubscriptionMediaKey("cleanup-721", "Movie"),
                {"tmdb_id": "cleanup-721", "item_type": "Movie"},
            )


def test_terminal_pre_effect_submission_resumes_same_job_with_new_attempt_and_outbox(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 722)
    repository = _repository(p7_runtime)
    first = repository.stage_cleanup([722])
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET max_publish_attempts = 1 WHERE aggregate_id = :attempt_id"),
            {"attempt_id": str(_current_attempt(p7_runtime, first.job_id))},
        )
    claims = jobs.claim_outbox(publisher_id="fixture-publisher", limit=1)
    assert len(claims) == 1
    assert (
        jobs.mark_outbox_failed(
            claims[0].event.id,
            claims[0].claim_token,
            error="fixture broker unavailable",
            backoff_seconds=0,
        )
        is OutboxStatus.DEAD
    )
    runtime_repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    assert runtime_repository.reconcile_terminal_jobs() == (1, 0)

    resumed = repository.stage_cleanup([722])

    assert resumed.job_id == first.job_id
    assert resumed.reused is True
    assert resumed.resumed is True
    with p7_runtime.engine.connect() as connection:
        attempts = connection.execute(
            text("SELECT attempt_no, status FROM job_attempts WHERE job_id = :job_id ORDER BY attempt_no"),
            {"job_id": first.job_id},
        ).all()
        outboxes = (
            connection.execute(
                text("SELECT status FROM outbox_events WHERE aggregate_type = 'job_attempt' ORDER BY created_at"),
            )
            .scalars()
            .all()
        )
        item_status = connection.scalar(
            text("SELECT status FROM cleanup_execution_items WHERE job_id = :job_id"),
            {"job_id": first.job_id},
        )
    assert attempts == [(1, "failed"), (2, "created")]
    assert outboxes == ["dead", "pending"]
    assert item_status == "queued"


def test_terminal_cleanup_child_reparents_to_a_new_live_parent_without_root_serial_conflict(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 733)
    repository = _repository(p7_runtime)
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    parent_a = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_a_attempt = parent_a.current_attempt.id
    _start_attempt(p7_runtime, parent_a.job.id, parent_a_attempt, "worker-parent-a")
    first = repository.stage_version_deletion(
        "emby-old",
        parent_job_id=parent_a.job.id,
        parent_attempt_id=parent_a_attempt,
        parent_worker_id="worker-parent-a",
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET max_publish_attempts = 1 WHERE aggregate_id = :attempt_id"),
            {"attempt_id": str(_current_attempt(p7_runtime, first.job_id))},
        )
    claims = jobs.claim_outbox(publisher_id="fixture-publisher", limit=1)
    assert len(claims) == 1
    assert (
        jobs.mark_outbox_failed(
            claims[0].event.id,
            claims[0].claim_token,
            error="fixture child dispatch failure",
            backoff_seconds=0,
        )
        is OutboxStatus.DEAD
    )
    assert SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work).reconcile_terminal_jobs() == (1, 0)
    jobs.succeed_attempt(
        job_id=parent_a.job.id,
        attempt_id=parent_a_attempt,
        worker_id="worker-parent-a",
        result={"status": "fixture-complete"},
        honor_cancel_request=False,
    )

    parent_b = jobs.create_job(task_key="resubscribe-library", payload={}).projection
    parent_b_attempt = parent_b.current_attempt.id
    _start_attempt(p7_runtime, parent_b.job.id, parent_b_attempt, "worker-parent-b")
    resumed = repository.stage_version_deletion(
        "emby-old",
        parent_job_id=parent_b.job.id,
        parent_attempt_id=parent_b_attempt,
        parent_worker_id="worker-parent-b",
    )

    assert resumed.job_id == first.job_id
    assert resumed.reused is True
    assert resumed.resumed is True
    with p7_runtime.engine.connect() as connection:
        child = connection.execute(
            text(
                """
                SELECT parent_job_id, workflow_step,
                       (SELECT COUNT(*) FROM job_attempts WHERE job_id = job_runs.id) AS attempts,
                       (SELECT COUNT(*) FROM outbox_events
                        WHERE aggregate_type = 'job_attempt'
                          AND aggregate_id IN (
                              SELECT id::text FROM job_attempts WHERE job_id = job_runs.id
                          )) AS dispatches
                FROM job_runs
                WHERE id = :job_id
                """
            ),
            {"job_id": first.job_id},
        ).one()
    assert child == (parent_b.job.id, 1, 2, 2)


def test_terminal_post_effect_submission_requires_explicit_reconciliation(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 723)
    repository = _repository(p7_runtime)
    submission = repository.stage_cleanup([723])
    attempt_id = _current_attempt(p7_runtime, submission.job_id)
    _start_attempt(p7_runtime, submission.job_id, attempt_id)
    runtime_repository = SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work)
    claimed = runtime_repository.claim_next(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
    )
    assert claimed is not None
    receipt = {
        "schema_version": 1,
        "item_id": str(claimed.id),
        "item_key": claimed.item_key,
        "phases": {},
    }
    started = runtime_repository.save_receipt(claimed, receipt, start_effect=True, lease_seconds=60)
    runtime_repository.mark_indeterminate(started, receipt, {"type": "fixture_unknown_effect"})
    PostgresJobStore(p7_runtime.unit_of_work).fail_attempt(
        job_id=submission.job_id,
        attempt_id=attempt_id,
        worker_id="worker-a",
        error_type="CleanupExecutionIndeterminate",
        error_message="fixture unknown effect",
        retryable=False,
        honor_cancel_request=False,
    )

    with pytest.raises(CleanupExecutionConflictError, match="already claimed"):
        repository.stage_cleanup([723])
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM job_attempts WHERE job_id = :job_id"),
                {"job_id": submission.job_id},
            )
            == 1
        )


def test_cleanup_staging_rejects_ambiguous_p115_pickcode_identity(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 724)
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    file_sha1_json, file_pickcode_json, asset_details_json
                ) VALUES (
                    'cleanup-724', 'Movie', TRUE, '["emby-old"]'::jsonb,
                    '[""]'::jsonb, '["ambiguous-pick"]'::jsonb,
                    '[{"emby_item_id":"emby-old","path":"/fixture/old.mkv"}]'::jsonb
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, pick_code, account_id, entry_kind
                ) VALUES
                    ('ambiguous-fid-a', 'parent-a', 'a.mkv', 'ambiguous-pick', '1', 'file'),
                    ('ambiguous-fid-b', 'parent-b', 'b.mkv', 'ambiguous-pick', '2', 'file')
                """
            )
        )

    with pytest.raises(ValueError, match="ambiguous 115 cache identities"):
        _repository(p7_runtime).stage_cleanup([724])
    assert _counts(p7_runtime) == {
        "items": 0,
        "jobs": 0,
        "attempts": 0,
        "outbox": 0,
        "idempotency": 0,
    }


def test_cleanup_staging_rejects_historical_p115_identity_without_account(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_cleanup(p7_runtime, 725)
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE cleanup_index
                SET versions_info_json = '[{"id":"emby-old-725","path":"/fixture/old-725.mkv"},
                                           {"id":"emby-best-725"}]'::jsonb,
                    best_version_json = '"emby-best-725"'::jsonb
                WHERE id = 725
                """
            )
        )
        connection.execute(
            text(
                """
                ALTER TABLE p115_filesystem_cache
                DROP CONSTRAINT ck_p115_filesystem_cache_account_id_present
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    file_sha1_json, file_pickcode_json, asset_details_json
                ) VALUES (
                    'cleanup-725', 'Movie', TRUE, '["emby-old-725"]'::jsonb,
                    '[""]'::jsonb, '["legacy-pick"]'::jsonb,
                    '[{"emby_item_id":"emby-old-725","path":"/fixture/old-725.mkv"}]'::jsonb
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, pick_code, account_id, entry_kind
                ) VALUES (
                    'legacy-fid', 'legacy-parent', 'legacy.mkv', 'legacy-pick', NULL, 'file'
                )
                """
            )
        )
    try:
        repository = SqlAlchemyCleanupRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
            SettingsCleanupExecutionPolicy(
                lambda: Settings(
                    local_strm_root="/fixture",
                    emby_server_url="http://emby.fixture.invalid",
                    emby_api_key=SecretStr("fixture-token"),
                    p115_enable_sync_delete=True,
                )
            ),
        )

        with pytest.raises(ValueError, match="account identity"):
            repository.stage_cleanup([725])
        assert _counts(p7_runtime) == {
            "items": 0,
            "jobs": 0,
            "attempts": 0,
            "outbox": 0,
            "idempotency": 0,
        }
    finally:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    ALTER TABLE p115_filesystem_cache
                    ADD CONSTRAINT ck_p115_filesystem_cache_account_id_present
                    CHECK (account_id IS NOT NULL) NOT VALID
                    """
                )
            )
