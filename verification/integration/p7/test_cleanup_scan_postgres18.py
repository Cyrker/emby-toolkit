from __future__ import annotations

import json
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Any
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.subscription.cleanup import canonical_cleanup_config
from etk.domains.subscription.cleanup_scan import (
    CleanupScanEntry,
    CleanupScanService,
    evaluate_cleanup_scan_item,
)
from etk.domains.subscription.cleanup_scan_repository import (
    CleanupScanConflict,
    CleanupScanDrift,
    SqlAlchemyCleanupScanRepository,
)
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime, SqlAlchemyUnitOfWork
from etk.platform.jobs import PersistentExecutionCancelled, PostgresJobStore

_PREFIX = "p11-cleanup-scan"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_CUTOVER_CONSTRAINT = "verification_p11_cleanup_config_cutover"
_SETTING_KEYS = (
    "media_cleanup_config",
    "media_cleanup_rules",
    "media_cleanup_library_ids",
    "media_cleanup_keep_one_per_res",
    "media_cleanup_delete_delay",
    "rating_mapping",
)
_CLEANUP_CONFIG_CUTOVER_SQL = str(
    import_module("migrations.versions.0020_p11_cleanup_config_cutover").CLEANUP_CONFIG_CUTOVER_SQL
)


class Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class Inventory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def existing_item_ids(self, item_ids: Sequence[str]) -> frozenset[str]:
        self.calls.append(tuple(item_ids))
        return frozenset(item_ids)


class ObservedUnitOfWork(SqlAlchemyUnitOfWork):
    def __init__(self, runtime: DatabaseRuntime, pids: Queue[int]) -> None:
        super().__init__(runtime.session_factory)
        self._pids = pids

    def __enter__(self) -> SqlAlchemyUnitOfWork:
        entered = super().__enter__()
        self._pids.put(int(entered.session.scalar(text("SELECT pg_backend_pid()")) or 0))
        return entered


def test_cleanup_config_cutover_is_atomic_and_never_overwrites_canonical_on_postgres18(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    migrated = {
        "rules": [{"id": "filesize", "enabled": True, "priority": "asc"}],
        "library_ids": ["legacy-library"],
        "keep_one_per_res": True,
        "delete_delay": 12,
        "manual_delete_without_confirm": False,
    }
    canonical = {
        "rules": [],
        "library_ids": ["canonical-library"],
        "keep_one_per_res": False,
        "delete_delay": 0,
        "manual_delete_without_confirm": True,
    }
    try:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
                {"keys": list(_SETTING_KEYS)},
            )
            connection.execute(text(_CLEANUP_CONFIG_CUTOVER_SQL))
        assert _setting_value(p7_runtime, "media_cleanup_config") == {
            "rules": [],
            "library_ids": [],
            "keep_one_per_res": False,
            "delete_delay": 0,
            "manual_delete_without_confirm": False,
        }

        with p7_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'media_cleanup_config'"))
            for key, value in {
                "media_cleanup_rules": migrated["rules"],
                "media_cleanup_library_ids": migrated["library_ids"],
                "media_cleanup_keep_one_per_res": migrated["keep_one_per_res"],
                "media_cleanup_delete_delay": migrated["delete_delay"],
            }.items():
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        """
                    ),
                    {"key": key, "value": json.dumps(value)},
                )
            connection.execute(text(_CLEANUP_CONFIG_CUTOVER_SQL))

        assert _setting_value(p7_runtime, "media_cleanup_config") == migrated
        assert all(_setting_value(p7_runtime, key) is not None for key in _SETTING_KEYS[1:5])

        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE app_settings
                    SET value_json = CAST(:value AS jsonb), last_updated_at = NOW()
                    WHERE setting_key = 'media_cleanup_config'
                    """
                ),
                {"value": json.dumps(canonical)},
            )
            connection.execute(text(_CLEANUP_CONFIG_CUTOVER_SQL))
        assert _setting_value(p7_runtime, "media_cleanup_config") == canonical

        connection = p7_runtime.engine.connect()
        transaction = connection.begin()
        try:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'media_cleanup_config'"))
            connection.execute(text(_CLEANUP_CONFIG_CUTOVER_SQL))
            assert (
                connection.scalar(
                    text("SELECT value_json FROM app_settings WHERE setting_key = 'media_cleanup_config'")
                )
                == migrated
            )
            transaction.rollback()
        finally:
            if transaction.is_active:
                transaction.rollback()
            connection.close()
        assert _setting_value(p7_runtime, "media_cleanup_config") == canonical

        with p7_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'media_cleanup_config'"))
            connection.execute(
                text(
                    """
                    UPDATE app_settings
                    SET value_json = '{}'::jsonb, last_updated_at = NOW()
                    WHERE setting_key = 'media_cleanup_library_ids'
                    """
                )
            )
            connection.execute(text(_CLEANUP_CONFIG_CUTOVER_SQL))
        malformed = _setting_value(p7_runtime, "media_cleanup_config")
        assert isinstance(malformed, dict)
        assert malformed["library_ids"] == {}
        with pytest.raises(TypeError, match=r"media_cleanup_config\.library_ids"):
            canonical_cleanup_config(malformed)
    finally:
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_config_cutover_runs_through_alembic_and_preserves_one_way_downgrade_on_postgres18(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    config = Config(str(_REPO_ROOT / "alembic.ini"))
    migrated = {
        "rules": [],
        "library_ids": ["legacy-library"],
        "keep_one_per_res": True,
        "delete_delay": 12,
        "manual_delete_without_confirm": False,
    }
    try:
        command.downgrade(config, "0019_p9_proxy_leases")
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
                {"keys": list(_SETTING_KEYS)},
            )
            for key, value in {
                "media_cleanup_rules": migrated["rules"],
                "media_cleanup_library_ids": migrated["library_ids"],
                "media_cleanup_keep_one_per_res": migrated["keep_one_per_res"],
                "media_cleanup_delete_delay": migrated["delete_delay"],
            }.items():
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        """
                    ),
                    {"key": key, "value": json.dumps(value)},
                )
            connection.execute(
                text(
                    f"""
                    ALTER TABLE app_settings ADD CONSTRAINT {_CUTOVER_CONSTRAINT}
                    CHECK (setting_key <> 'media_cleanup_config')
                    """
                )
            )

        with pytest.raises(IntegrityError):
            command.upgrade(config, "head")
        assert _alembic_head(p7_runtime) == "0019_p9_proxy_leases"
        assert _setting_value(p7_runtime, "media_cleanup_config") is None

        with p7_runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT {_CUTOVER_CONSTRAINT}"))
        command.upgrade(config, "head")
        assert _alembic_head(p7_runtime) == "0020_p11_cleanup_config_cutover"
        assert _setting_value(p7_runtime, "media_cleanup_config") == migrated

        command.downgrade(config, "0019_p9_proxy_leases")
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE app_settings
                    SET value_json = '["changed-after-downgrade"]'::jsonb, last_updated_at = NOW()
                    WHERE setting_key = 'media_cleanup_library_ids'
                    """
                )
            )
        command.upgrade(config, "head")
        assert _setting_value(p7_runtime, "media_cleanup_config") == migrated
    finally:
        with p7_runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {_CUTOVER_CONSTRAINT}"))
        command.upgrade(config, "head")
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_config_cutover_waits_for_and_preserves_a_concurrent_canonical_writer_on_postgres18(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    canonical = {
        "rules": [],
        "library_ids": ["concurrent-canonical"],
        "keep_one_per_res": False,
        "delete_delay": 0,
        "manual_delete_without_confirm": True,
    }
    writer = p7_runtime.engine.connect()
    transaction = writer.begin()
    try:
        writer.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(_SETTING_KEYS)},
        )
        writer.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('media_cleanup_config', CAST(:value AS jsonb), NOW())
                """
            ),
            {"value": json.dumps(canonical)},
        )
        pids: Queue[int] = Queue()
        with ThreadPoolExecutor(max_workers=1) as executor:
            cutover = executor.submit(_execute_cleanup_cutover, p7_runtime, pids)
            cutover_pid = pids.get(timeout=10)
            _wait_until_blocked(p7_runtime, cutover_pid)
            transaction.commit()
            cutover.result(timeout=10)
        assert _setting_value(p7_runtime, "media_cleanup_config") == canonical
    finally:
        if transaction.is_active:
            transaction.rollback()
        writer.close()
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_scan_filters_user_scope_and_atomically_reconciles_on_postgres18(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        repository = SqlAlchemyCleanupScanRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
        )
        inventory = Inventory()
        progress = Progress()
        service = CleanupScanService(
            repository,
            inventory,
            lambda: Settings(emby_user_id=f"{_PREFIX}-user"),
        )

        first = service.execute(progress)

        assert first.stats["visible_movies"] == 1
        assert first.stats["visible_series"] == 1
        assert first.stats["scanned_items"] == 2
        assert first.stats["indexed_items"] == 2
        assert first.stats["deleted_pending"] == 1
        assert first.stats["upserted_items"] == 2
        assert first.stats["authoritative_emby_read"] is True
        assert first.stats["atomic_reconcile"] is True
        assert set(inventory.calls[0]) == {
            f"{_PREFIX}-movie-1",
            f"{_PREFIX}-movie-2",
            f"{_PREFIX}-episode-1",
            f"{_PREFIX}-episode-2",
        }
        rows = _cleanup_rows(p7_runtime)
        assert [(row["tmdb_id"], row["item_type"], row["status"]) for row in rows] == [
            (f"{_PREFIX}-episode", "Episode", "pending"),
            (f"{_PREFIX}-ignored", "Movie", "ignored"),
            (f"{_PREFIX}-movie", "Movie", "pending"),
        ]
        by_key = {(row["tmdb_id"], row["item_type"]): row for row in rows}
        assert by_key[(f"{_PREFIX}-movie", "Movie")]["best_version_json"] == f"{_PREFIX}-movie-2"
        assert by_key[(f"{_PREFIX}-episode", "Episode")]["best_version_json"] == (f"{_PREFIX}-episode-2")
        assert len(by_key[(f"{_PREFIX}-movie", "Movie")]["versions_info_json"]) == 2
        assert all(row["tmdb_id"] not in {f"{_PREFIX}-blocked", f"{_PREFIX}-hidden"} for row in rows)

        replay = service.execute(Progress())

        assert replay.stats["deleted_pending"] == 2
        assert replay.stats["upserted_items"] == 2
        assert _row_payloads(_cleanup_rows(p7_runtime)) == _row_payloads(rows)

        snapshot = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        entries = tuple(
            entry
            for item in snapshot.items
            for entry in [
                evaluate_cleanup_scan_item(
                    item,
                    existing_item_ids=frozenset(
                        str(version.get("emby_item_id") or "") for version in item.asset_details
                    ),
                    policy=snapshot.policy,
                )
            ]
            if entry is not None
        )
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET title = 'Drifted title', last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": f"{_PREFIX}-movie"},
            )
        with pytest.raises(CleanupScanDrift):
            repository.reconcile(
                snapshot,
                entries,
                job_id=None,
                cancellation_check=lambda: None,
            )
        assert len([row for row in _cleanup_rows(p7_runtime) if row["status"] == "pending"]) == 2

        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET title = 'Visible Movie', last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": f"{_PREFIX}-movie"},
            )
        current = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        current_entries = _entries(current)
        cleanup_id = next(int(row["id"]) for row in _cleanup_rows(p7_runtime) if row["tmdb_id"] == f"{_PREFIX}-movie")
        job = (
            PostgresJobStore(p7_runtime.unit_of_work)
            .create_job(
                task_key="p7-cleanup-execution",
                payload={"fixture": True},
                trace_id=f"{_PREFIX}-job",
            )
            .projection
        )
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO cleanup_execution_items (
                        id, execution_key, job_id, cleanup_index_id, item_key, snapshot_json, status
                    ) VALUES (
                        :id, :execution_key, :job_id, :cleanup_index_id,
                        :item_key, '{}'::jsonb, 'queued'
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "execution_key": "b" * 64,
                    "job_id": job.job.id,
                    "cleanup_index_id": cleanup_id,
                    "item_key": f"cleanup:{_PREFIX}-movie",
                },
            )
        with pytest.raises(CleanupScanConflict):
            repository.reconcile(
                current,
                current_entries,
                job_id=None,
                cancellation_check=lambda: None,
            )
        assert len([row for row in _cleanup_rows(p7_runtime) if row["status"] == "pending"]) == 2
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_scan_rolls_back_publication_when_cancelled_at_commit_fence(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        repository = SqlAlchemyCleanupScanRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
        )
        snapshot = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        entries = _entries(snapshot)
        before = _row_payloads(_cleanup_rows(p7_runtime))
        checks = 0

        def cancellation_check() -> None:
            nonlocal checks
            checks += 1
            if checks == 2:
                raise PersistentExecutionCancelled("fixture commit-fence cancellation")

        with pytest.raises(PersistentExecutionCancelled, match="commit-fence"):
            repository.reconcile(
                snapshot,
                entries,
                job_id=None,
                cancellation_check=cancellation_check,
            )

        assert checks == 2
        assert _row_payloads(_cleanup_rows(p7_runtime)) == before
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_scan_rejects_a_cancelled_job_before_publication(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        repository = SqlAlchemyCleanupScanRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
        )
        snapshot = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        entries = _entries(snapshot)
        before = _row_payloads(_cleanup_rows(p7_runtime))
        jobs = PostgresJobStore(p7_runtime.unit_of_work)
        job = jobs.create_job(
            task_key="scan-cleanup-issues",
            payload={},
            trace_id=f"{_PREFIX}-job",
        ).projection
        cancelled = jobs.request_cancel(job.job.id)
        checks = 0

        def cancellation_check() -> None:
            nonlocal checks
            checks += 1

        assert cancelled.job.status.value == "cancelled"
        with pytest.raises(PersistentExecutionCancelled, match="before publication"):
            repository.reconcile(
                snapshot,
                entries,
                job_id=job.job.id,
                cancellation_check=cancellation_check,
            )

        assert checks == 0
        assert _row_payloads(_cleanup_rows(p7_runtime)) == before
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_scan_linearizes_a_concurrent_cancel_request_after_publication(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        repository = SqlAlchemyCleanupScanRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
        )
        snapshot = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        entries = _entries(snapshot)
        jobs = PostgresJobStore(p7_runtime.unit_of_work)
        job = jobs.create_job(
            task_key="scan-cleanup-issues",
            payload={},
            trace_id=f"{_PREFIX}-job",
        ).projection
        claimed = jobs.claim_attempt(
            job_id=job.job.id,
            attempt_id=job.current_attempt.id,
            worker_id="p11-cleanup-scan-worker",
            lease_seconds=60,
        )
        assert claimed.claimed is True
        jobs.start_attempt(
            job_id=job.job.id,
            attempt_id=job.current_attempt.id,
            worker_id="p11-cleanup-scan-worker",
            lease_seconds=60,
        )

        publication_ready = Event()
        release_publication = Event()
        cancellation_pids: Queue[int] = Queue()
        checks = 0

        def publication_fence() -> None:
            nonlocal checks
            checks += 1
            if checks == 2:
                publication_ready.set()
                if not release_publication.wait(timeout=10):
                    raise AssertionError("fixture publication fence was not released")

        cancellation_store = PostgresJobStore(lambda: ObservedUnitOfWork(p7_runtime, cancellation_pids))
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                publication = executor.submit(
                    repository.reconcile,
                    snapshot,
                    entries,
                    job_id=job.job.id,
                    cancellation_check=publication_fence,
                )
                assert publication_ready.wait(timeout=10)
                cancellation = executor.submit(cancellation_store.request_cancel, job.job.id)
                cancellation_pid = cancellation_pids.get(timeout=10)
                _wait_until_blocked(p7_runtime, cancellation_pid)
                assert cancellation.done() is False
                release_publication.set()
                publication.result(timeout=10)
                cancelled = cancellation.result(timeout=10)
        finally:
            release_publication.set()

        assert cancelled.job.status.value == "cancel_requested"
        assert len([row for row in _cleanup_rows(p7_runtime) if row["status"] == "pending"]) == 2
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("EnabledFolders", "library-visible"),
        ("ExcludedSubFolders", {"folder": "library-hidden"}),
        ("BlockedTags", ["blocked", {"unexpected": True}]),
        ("BlockUnratedItems", ["Movie", ["Episode"]]),
    ],
)
def test_cleanup_scan_malformed_user_policy_fails_closed_before_emby_read_or_publication(
    p7_runtime: DatabaseRuntime,
    field: str,
    value: object,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE emby_users
                    SET policy_json = jsonb_set(policy_json, ARRAY[:field], CAST(:value AS jsonb), TRUE)
                    WHERE id = :user_id
                    """
                ),
                {
                    "field": field,
                    "value": json.dumps(value),
                    "user_id": f"{_PREFIX}-user",
                },
            )
        before = _row_payloads(_cleanup_rows(p7_runtime))
        inventory = Inventory()
        service = CleanupScanService(
            SqlAlchemyCleanupScanRepository(
                p7_runtime.unit_of_work,
                SqlAlchemyCleanupMediaProjection(),
            ),
            inventory,
            lambda: Settings(emby_user_id=f"{_PREFIX}-user"),
        )

        with pytest.raises(TypeError, match=field):
            service.execute(Progress())

        assert inventory.calls == []
        assert _row_payloads(_cleanup_rows(p7_runtime)) == before
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


@pytest.mark.parametrize("source", ["app_settings", "emby_users", "media_metadata"])
def test_cleanup_scan_holds_source_tables_until_atomic_publication_commits(
    p7_runtime: DatabaseRuntime,
    source: str,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        repository = SqlAlchemyCleanupScanRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
        )
        snapshot = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        entries = _entries(snapshot)
        publication_ready = Event()
        release_publication = Event()
        writer_pids: Queue[int] = Queue()
        checks = 0

        def publication_fence() -> None:
            nonlocal checks
            checks += 1
            if checks == 2:
                publication_ready.set()
                if not release_publication.wait(timeout=10):
                    raise AssertionError("fixture publication fence was not released")

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                publication = executor.submit(
                    repository.reconcile,
                    snapshot,
                    entries,
                    job_id=None,
                    cancellation_check=publication_fence,
                )
                assert publication_ready.wait(timeout=10)
                writer = executor.submit(_update_source, p7_runtime, source, writer_pids)
                writer_pid = writer_pids.get(timeout=10)
                _wait_until_blocked(p7_runtime, writer_pid)
                assert writer.done() is False
                release_publication.set()
                publication.result(timeout=10)
                writer.result(timeout=10)
        finally:
            release_publication.set()

        current = repository.load_snapshot(user_id=f"{_PREFIX}-user")
        assert current.source_fingerprint != snapshot.source_fingerprint
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


def test_cleanup_scan_acquires_source_before_cleanup_index_to_avoid_maintenance_deadlock(
    p7_runtime: DatabaseRuntime,
) -> None:
    prior_settings = _snapshot_settings(p7_runtime)
    _cleanup_fixture(p7_runtime)
    try:
        _insert_fixture(p7_runtime)
        snapshot_repository = SqlAlchemyCleanupScanRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyCleanupMediaProjection(),
        )
        snapshot = snapshot_repository.load_snapshot(user_id=f"{_PREFIX}-user")
        entries = _entries(snapshot)
        scan_pids: Queue[int] = Queue()
        repository = SqlAlchemyCleanupScanRepository(
            lambda: ObservedUnitOfWork(p7_runtime, scan_pids),
            SqlAlchemyCleanupMediaProjection(),
        )
        guard = p7_runtime.engine.connect()
        transaction = guard.begin()
        guard.execute(
            text(
                """
                UPDATE media_metadata
                SET title = 'Maintenance drift', last_updated_at = NOW()
                WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                """
            ),
            {"tmdb_id": f"{_PREFIX}-movie"},
        )
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                publication = executor.submit(
                    repository.reconcile,
                    snapshot,
                    entries,
                    job_id=None,
                    cancellation_check=lambda: None,
                )
                scan_pid = scan_pids.get(timeout=10)
                _wait_until_blocked(p7_runtime, scan_pid)
                guard.execute(text("SET LOCAL lock_timeout = '1s'"))
                guard.execute(text("LOCK TABLE cleanup_index IN ROW EXCLUSIVE MODE"))
                transaction.commit()
                with pytest.raises(CleanupScanDrift):
                    publication.result(timeout=10)
        finally:
            if transaction.is_active:
                transaction.rollback()
            guard.close()
    finally:
        _cleanup_fixture(p7_runtime)
        _restore_settings(p7_runtime, prior_settings)


def _update_source(runtime: DatabaseRuntime, source: str, pids: Queue[int]) -> None:
    statements = {
        "app_settings": (
            """
            UPDATE app_settings
            SET value_json = jsonb_set(value_json, '{keep_one_per_res}', 'true'::jsonb, TRUE),
                last_updated_at = NOW()
            WHERE setting_key = 'media_cleanup_config'
            """,
            {},
        ),
        "emby_users": (
            """
            UPDATE emby_users
            SET policy_json = jsonb_set(policy_json, '{MaxParentalRating}', '9'::jsonb, TRUE)
            WHERE id = :user_id
            """,
            {"user_id": f"{_PREFIX}-user"},
        ),
        "media_metadata": (
            """
            UPDATE media_metadata
            SET title = title || ' drift', last_updated_at = NOW()
            WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
            """,
            {"tmdb_id": f"{_PREFIX}-movie"},
        ),
    }
    statement, parameters = statements[source]
    with runtime.engine.begin() as connection:
        pids.put(int(connection.scalar(text("SELECT pg_backend_pid()")) or 0))
        connection.execute(text(statement), parameters)


def _wait_until_blocked(runtime: DatabaseRuntime, pid: int) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with runtime.engine.connect() as connection:
            blockers = connection.scalar(
                text("SELECT cardinality(pg_blocking_pids(:pid))"),
                {"pid": pid},
            )
        if int(blockers or 0) > 0:
            return
        time.sleep(0.01)
    raise AssertionError(f"PostgreSQL backend {pid} did not enter the expected source-table lock wait")


def _entries(snapshot: Any) -> tuple[CleanupScanEntry, ...]:
    result: list[CleanupScanEntry] = []
    for item in snapshot.items:
        entry = evaluate_cleanup_scan_item(
            item,
            existing_item_ids=frozenset(str(version.get("emby_item_id") or "") for version in item.asset_details),
            policy=snapshot.policy,
        )
        if entry is not None:
            result.append(entry)
    return tuple(result)


def _insert_fixture(runtime: DatabaseRuntime) -> None:
    policy = {
        "EnableAllFolders": True,
        "EnabledFolders": [],
        "ExcludedSubFolders": [],
        "BlockedTags": ["blocked"],
        "BlockUnratedItems": [],
        "MaxParentalRating": 10,
    }
    config = {
        "rules": [{"id": "resolution", "enabled": True, "priority": ["4k", "1080p"]}],
        "library_ids": ["library-visible"],
        "keep_one_per_res": False,
    }
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('media_cleanup_config', CAST(:config AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {"config": json.dumps(config)},
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator, policy_json)
                VALUES (:user_id, 'Cleanup Scan', TRUE, CAST(:policy AS jsonb))
                """
            ),
            {"user_id": f"{_PREFIX}-user", "policy": json.dumps(policy)},
        )
        _insert_media(
            connection,
            tmdb_id=f"{_PREFIX}-movie",
            item_type="Movie",
            title="Visible Movie",
            assets=[
                _asset(f"{_PREFIX}-movie-1", "1080p", "library-visible"),
                _asset(f"{_PREFIX}-movie-2", "4k", "library-visible"),
            ],
        )
        _insert_media(
            connection,
            tmdb_id=f"{_PREFIX}-series",
            item_type="Series",
            title="Visible Series",
            assets=[_asset(f"{_PREFIX}-series-1", "1080p", "library-visible")],
        )
        _insert_media(
            connection,
            tmdb_id=f"{_PREFIX}-episode",
            item_type="Episode",
            title="Visible Episode",
            parent_series_tmdb_id=f"{_PREFIX}-series",
            assets=[
                _asset(f"{_PREFIX}-episode-1", "1080p", "library-visible"),
                _asset(f"{_PREFIX}-episode-2", "4k", "library-visible"),
            ],
        )
        _insert_media(
            connection,
            tmdb_id=f"{_PREFIX}-blocked",
            item_type="Movie",
            title="Blocked Movie",
            tags=["blocked"],
            assets=[
                _asset(f"{_PREFIX}-blocked-1", "1080p", "library-visible"),
                _asset(f"{_PREFIX}-blocked-2", "4k", "library-visible"),
            ],
        )
        _insert_media(
            connection,
            tmdb_id=f"{_PREFIX}-hidden",
            item_type="Movie",
            title="Hidden Library",
            assets=[
                _asset(f"{_PREFIX}-hidden-1", "1080p", "library-hidden"),
                _asset(f"{_PREFIX}-hidden-2", "4k", "library-hidden"),
            ],
        )
        connection.execute(
            text(
                """
                INSERT INTO cleanup_index (
                    tmdb_id, item_type, status, versions_info_json, best_version_json
                ) VALUES
                    (:stale, 'Movie', 'pending', '[]'::jsonb, 'null'::jsonb),
                    (:ignored, 'Movie', 'ignored', '[]'::jsonb, 'null'::jsonb)
                """
            ),
            {"stale": f"{_PREFIX}-stale", "ignored": f"{_PREFIX}-ignored"},
        )


def _insert_media(
    connection: Any,
    *,
    tmdb_id: str,
    item_type: str,
    title: str,
    assets: Sequence[dict[str, Any]],
    parent_series_tmdb_id: str | None = None,
    tags: Sequence[str] = (),
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO media_metadata (
                tmdb_id, item_type, title, parent_series_tmdb_id, in_library,
                original_language, countries_json, tags_json, official_rating_json,
                asset_details_json
            ) VALUES (
                :tmdb_id, :item_type, :title, :parent_series_tmdb_id, TRUE,
                'en', '["US"]'::jsonb, CAST(:tags AS jsonb), '{"US":"PG-13"}'::jsonb,
                CAST(:assets AS jsonb)
            )
            """
        ),
        {
            "tmdb_id": tmdb_id,
            "item_type": item_type,
            "title": title,
            "parent_series_tmdb_id": parent_series_tmdb_id,
            "tags": json.dumps(list(tags)),
            "assets": json.dumps(list(assets)),
        },
    )


def _asset(item_id: str, resolution: str, library_id: str) -> dict[str, Any]:
    return {
        "emby_item_id": item_id,
        "path": f"/fixture/{item_id}.mkv",
        "source_library_id": library_id,
        "ancestor_ids": [library_id],
        "resolution_display": resolution,
        "quality_display": "WEB-DL",
        "effect_display": "SDR",
        "codec_display": "HEVC",
        "size_bytes": 100,
        "video_bitrate_mbps": 10,
        "bit_depth": 10,
        "frame_rate": 24,
        "runtime_minutes": 100,
        "subtitle_languages_raw": ["eng"],
    }


def _cleanup_rows(runtime: DatabaseRuntime) -> list[dict[str, Any]]:
    with runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT id, tmdb_id, item_type, status, versions_info_json, best_version_json
                    FROM cleanup_index
                    WHERE tmdb_id LIKE :prefix
                    ORDER BY tmdb_id, item_type
                    """
                ),
                {"prefix": f"{_PREFIX}%"},
            )
            .mappings()
            .all()
        )
    return [dict(row) for row in rows]


def _row_payloads(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: value for key, value in row.items() if key != "id"} for row in rows]


def _setting_value(runtime: DatabaseRuntime, key: str) -> Any:
    with runtime.engine.connect() as connection:
        return connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": key},
        )


def _alembic_head(runtime: DatabaseRuntime) -> str:
    with runtime.engine.connect() as connection:
        return str(connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) or "")


def _execute_cleanup_cutover(runtime: DatabaseRuntime, pids: Queue[int]) -> None:
    with runtime.engine.begin() as connection:
        pids.put(int(connection.scalar(text("SELECT pg_backend_pid()")) or 0))
        connection.execute(text(_CLEANUP_CONFIG_CUTOVER_SQL))


def _snapshot_settings(runtime: DatabaseRuntime) -> dict[str, tuple[Any, Any]]:
    with runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT setting_key, value_json, last_updated_at
                    FROM app_settings
                    WHERE setting_key = ANY(CAST(:keys AS text[]))
                    """
                ),
                {"keys": list(_SETTING_KEYS)},
            )
            .mappings()
            .all()
        )
    return {str(row["setting_key"]): (row["value_json"], row["last_updated_at"]) for row in rows}


def _restore_settings(runtime: DatabaseRuntime, values: dict[str, tuple[Any, Any]]) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(_SETTING_KEYS)},
        )
        for key, (value, updated_at) in values.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:key, CAST(:value AS jsonb), :updated_at)
                    """
                ),
                {"key": key, "value": json.dumps(value), "updated_at": updated_at},
            )


def _cleanup_fixture(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM cleanup_execution_items
                WHERE item_key LIKE :prefix
                   OR cleanup_index_id IN (
                       SELECT id FROM cleanup_index WHERE tmdb_id LIKE :prefix
                   )
                """
            ),
            {"prefix": f"{_PREFIX}%"},
        )
        job_ids = tuple(
            connection.execute(
                text("SELECT id FROM job_runs WHERE trace_id = :trace_id"),
                {"trace_id": f"{_PREFIX}-job"},
            ).scalars()
        )
        if job_ids:
            attempt_ids = tuple(
                connection.execute(
                    text("SELECT id FROM job_attempts WHERE job_id = ANY(CAST(:ids AS uuid[]))"),
                    {"ids": list(job_ids)},
                ).scalars()
            )
            connection.execute(
                text("DELETE FROM idempotency_records WHERE job_id = ANY(CAST(:ids AS uuid[]))"),
                {"ids": list(job_ids)},
            )
            if attempt_ids:
                connection.execute(
                    text("DELETE FROM outbox_events WHERE aggregate_id = ANY(CAST(:ids AS text[]))"),
                    {"ids": [str(value) for value in attempt_ids]},
                )
            connection.execute(
                text("DELETE FROM job_attempts WHERE job_id = ANY(CAST(:ids AS uuid[]))"),
                {"ids": list(job_ids)},
            )
            connection.execute(
                text("DELETE FROM job_runs WHERE id = ANY(CAST(:ids AS uuid[]))"),
                {"ids": list(job_ids)},
            )
        connection.execute(
            text("DELETE FROM cleanup_index WHERE tmdb_id LIKE :prefix"),
            {"prefix": f"{_PREFIX}%"},
        )
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id LIKE :prefix"),
            {"prefix": f"{_PREFIX}%"},
        )
        connection.execute(
            text("DELETE FROM emby_users WHERE id = :user_id"),
            {"user_id": f"{_PREFIX}-user"},
        )
