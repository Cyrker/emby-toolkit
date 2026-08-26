from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, canonical_json_hash
from etk.platform.jobs.repository import SqlAlchemyJobRepository

from .cleanup_execution import cleanup_job_max_attempts

_CLEANUP_TASK_KEY = "p7-cleanup-execution"
_CLEANUP_OPERATION_MODULE = "etk.processes.p7_cleanup"
_CLEANUP_OPERATION_QUALNAME = "execute_cleanup_job"
_CLEANUP_IDEMPOTENCY_SCOPE = "subscription.cleanup.execute"


class CleanupMediaProjectionPort(Protocol):
    def list_pending_tasks(self, uow: SqlAlchemyUnitOfWork) -> Sequence[Mapping[str, Any]]: ...

    def version_deletion_snapshot(
        self,
        uow: SqlAlchemyUnitOfWork,
        emby_id: str,
    ) -> Mapping[str, Any] | None: ...

    def execution_identity(
        self,
        uow: SqlAlchemyUnitOfWork,
        emby_id: str,
        *,
        fallback_path: str | None = None,
        fallback_tmdb_id: str | None = None,
        fallback_item_type: str | None = None,
    ) -> Mapping[str, Any] | None: ...


class CleanupExecutionPolicyPort(Protocol):
    def freeze_target(self, identity: Mapping[str, Any]) -> Mapping[str, Any]: ...


class CleanupExecutionConflictError(RuntimeError):
    def __init__(self, conflict_ids: Sequence[object] = ()) -> None:
        self.conflict_ids = tuple(str(value) for value in conflict_ids)
        detail = f": {', '.join(self.conflict_ids)}" if self.conflict_ids else ""
        super().__init__(f"cleanup items are already claimed or unavailable{detail}")


@dataclass(frozen=True, slots=True)
class CleanupExecutionSubmission:
    job_id: UUID
    execution_key: str
    item_count: int
    reused: bool = False
    resumed: bool = False


class SqlAlchemyCleanupRepository:
    """P7 transaction owner for cleanup state, durable claims, Job, and Outbox."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media_projection: CleanupMediaProjectionPort,
        execution_policy: CleanupExecutionPolicyPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media_projection = media_projection
        self._execution_policy = execution_policy

    def list_pending_tasks(self) -> tuple[Mapping[str, Any], ...]:
        with self._unit_of_work() as uow:
            rows = self._media_projection.list_pending_tasks(uow)
        return tuple(dict(row) for row in rows)

    def pending_task_ids(self) -> tuple[int, ...]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT idx.id
                        FROM cleanup_index AS idx
                        WHERE idx.status = 'pending'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM cleanup_execution_items AS item
                              WHERE item.cleanup_index_id = idx.id
                                AND item.status IN ('queued','claimed','effect_started')
                          )
                        ORDER BY idx.id
                        """
                    )
                )
                .scalars()
                .all()
            )
        return tuple(int(row) for row in rows)

    def stage_cleanup(
        self,
        task_ids: Sequence[Any],
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> CleanupExecutionSubmission:
        ids = _task_ids(task_ids)
        if not ids:
            raise ValueError("cleanup execution requires at least one task id")
        execution_key: str | None = None
        submission: CleanupExecutionSubmission | None = None
        try:
            with self._unit_of_work() as uow:
                rows = self._lock_cleanup_rows(uow, ids)
                by_id = {int(row["id"]): row for row in rows}
                unavailable = [
                    task_id for task_id in ids if task_id not in by_id or by_id[task_id]["status"] != "pending"
                ]
                if unavailable:
                    raise CleanupExecutionConflictError(unavailable)
                snapshots = self._cleanup_snapshots(uow, ids, by_id)
                if not snapshots:
                    raise CleanupExecutionConflictError(ids)
                execution_key = _execution_key("cleanup_index", snapshots)
                existing = self._existing_submission(uow, execution_key)
                if existing is not None:
                    submission = existing
                    uow.commit()
                else:
                    conflicts = self._active_item_conflicts(
                        uow,
                        [str(snapshot["item_key"]) for snapshot in snapshots],
                        cleanup_index_ids=ids,
                    )
                    if conflicts:
                        raise CleanupExecutionConflictError(conflicts)
                    submission = self._stage_job(
                        uow,
                        execution_key=execution_key,
                        snapshots=snapshots,
                        requested_by_user_id=requested_by_user_id,
                        trace_id=trace_id,
                    )
                    uow.commit()
        except IntegrityError as exc:
            if execution_key is not None:
                existing = self._read_existing_submission(execution_key)
                if existing is not None:
                    return existing
            raise CleanupExecutionConflictError(ids) from exc
        if submission is None:
            raise RuntimeError("cleanup submission was not staged")
        return submission

    def stage_version_deletion(
        self,
        emby_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
        parent_job_id: UUID | None = None,
        parent_attempt_id: UUID | None = None,
        parent_worker_id: str | None = None,
        workflow_step: int | None = None,
    ) -> CleanupExecutionSubmission:
        normalized_id = str(emby_id or "").strip()
        if not normalized_id:
            raise ValueError("cleanup version deletion requires emby_id")
        normalized_parent_worker = str(parent_worker_id or "").strip()
        if parent_job_id is None:
            if parent_attempt_id is not None or normalized_parent_worker or workflow_step is not None:
                raise ValueError("cleanup child identity requires parent_job_id")
        elif parent_attempt_id is None or not normalized_parent_worker:
            raise ValueError("cleanup child staging requires the current parent Attempt and Worker")
        if workflow_step is not None and (isinstance(workflow_step, bool) or workflow_step < 1):
            raise ValueError("cleanup workflow_step must be a positive integer")
        execution_key: str | None = None
        submission: CleanupExecutionSubmission | None = None
        try:
            with self._unit_of_work() as uow:
                if parent_job_id is not None:
                    assert parent_attempt_id is not None
                    self._lock_live_parent_attempt(
                        uow,
                        parent_job_id,
                        parent_attempt_id,
                        normalized_parent_worker,
                    )
                cleanup_rows = (
                    uow.session.execute(
                        text(
                            """
                            SELECT id, tmdb_id, item_type, versions_info_json, best_version_json, last_updated_at
                            FROM cleanup_index
                            WHERE status = 'pending'
                              AND jsonb_typeof(versions_info_json) = 'array'
                              AND EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements(versions_info_json) AS version(value)
                                  WHERE COALESCE(version.value->>'id', version.value->>'emby_item_id') = :emby_id
                              )
                            ORDER BY id
                            FOR UPDATE
                            """
                        ),
                        {"emby_id": normalized_id},
                    )
                    .mappings()
                    .all()
                )
                media = self._media_projection.version_deletion_snapshot(uow, normalized_id)
                if media is None and not cleanup_rows:
                    raise LookupError(normalized_id)
                identity = self._media_projection.execution_identity(uow, normalized_id)
                if identity is None and media is not None:
                    identity = media
                if identity is None:
                    fallback = next(
                        (
                            version
                            for row in cleanup_rows
                            for version in _version_entries(row["versions_info_json"])
                            if _version_id(version) == normalized_id
                        ),
                        None,
                    )
                    if fallback is not None:
                        identity = self._media_projection.execution_identity(
                            uow,
                            normalized_id,
                            fallback_path=_optional_text(fallback.get("path")),
                            fallback_tmdb_id=str(cleanup_rows[0]["tmdb_id"]),
                            fallback_item_type=str(cleanup_rows[0]["item_type"]),
                        )
                if identity is None:
                    raise LookupError(normalized_id)
                frozen_target = self._execution_policy.freeze_target(identity)
                snapshot = {
                    "kind": "single_version",
                    "emby_id": normalized_id,
                    "item_key": f"version:{normalized_id}",
                    "target": _json_value(frozen_target),
                    "cleanup_indexes": [
                        {
                            "id": int(row["id"]),
                            "tmdb_id": str(row["tmdb_id"]),
                            "item_type": str(row["item_type"]),
                            "versions_info": _json_value(row["versions_info_json"]),
                            "best_version": _json_value(row["best_version_json"]),
                            "last_updated_at": _iso_value(row["last_updated_at"]),
                        }
                        for row in cleanup_rows
                    ],
                }
                execution_key = _execution_key("single_version", [snapshot])
                existing = self._existing_submission(
                    uow,
                    execution_key,
                    parent_job_id=parent_job_id,
                    workflow_step=workflow_step,
                )
                if existing is not None:
                    submission = existing
                    uow.commit()
                else:
                    conflicts = self._active_item_conflicts(
                        uow,
                        [f"version:{normalized_id}"],
                        cleanup_index_ids=tuple(int(row["id"]) for row in cleanup_rows),
                    )
                    if conflicts:
                        raise CleanupExecutionConflictError(conflicts)
                    assigned_workflow_step = workflow_step
                    if parent_job_id is not None and assigned_workflow_step is None:
                        assigned_workflow_step = self._next_parent_workflow_step(uow, parent_job_id)
                    submission = self._stage_job(
                        uow,
                        execution_key=execution_key,
                        snapshots=[snapshot],
                        requested_by_user_id=requested_by_user_id,
                        trace_id=trace_id,
                        parent_job_id=parent_job_id,
                        workflow_step=assigned_workflow_step,
                    )
                    uow.commit()
        except IntegrityError as exc:
            if execution_key is not None:
                existing = self._read_existing_submission(
                    execution_key,
                    parent_job_id=parent_job_id,
                    parent_attempt_id=parent_attempt_id,
                    parent_worker_id=normalized_parent_worker or None,
                    workflow_step=workflow_step,
                )
                if existing is not None:
                    return existing
            raise CleanupExecutionConflictError((normalized_id,)) from exc
        if submission is None:
            raise RuntimeError("cleanup version deletion was not staged")
        return submission

    def ignore_tasks(self, task_ids: Sequence[Any]) -> int:
        return self._update_status(task_ids, "ignored")

    def mark_processed(self, task_ids: Sequence[Any]) -> int:
        return self._update_status(task_ids, "processed")

    def delete_tasks(self, task_ids: Sequence[Any]) -> int:
        ids = _task_ids(task_ids)
        if not ids:
            return 0
        with self._unit_of_work() as uow:
            self._lock_cleanup_rows(uow, ids)
            conflicts = self._active_item_conflicts(uow, (), cleanup_index_ids=ids)
            if conflicts:
                raise CleanupExecutionConflictError(conflicts)
            deleted = uow.session.execute(
                text(
                    """
                    DELETE FROM cleanup_index
                    WHERE id = ANY(CAST(:task_ids AS integer[]))
                      AND status = 'pending'
                    """
                ),
                {"task_ids": list(ids)},
            )
            deleted_count = _rowcount(deleted)
            uow.commit()
        return deleted_count

    def remove_deleted_version(self, emby_id: str) -> int:
        affected_count = 0
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, versions_info_json
                        FROM cleanup_index
                        WHERE status = 'pending'
                          AND jsonb_typeof(versions_info_json) = 'array'
                          AND EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements(versions_info_json) AS version(value)
                              WHERE version.value->>'id' = :emby_id
                          )
                        ORDER BY id
                        FOR UPDATE
                        """
                    ),
                    {"emby_id": emby_id},
                )
                .mappings()
                .all()
            )
            for row in rows:
                versions = row.get("versions_info_json")
                if not isinstance(versions, list):
                    continue
                remaining = [version for version in versions if str(_version_mapping(version).get("id")) != emby_id]
                if len(remaining) == len(versions):
                    continue
                task_id = int(row["id"])
                if len(remaining) <= 1:
                    uow.session.execute(
                        text("DELETE FROM cleanup_index WHERE id = :task_id AND status = 'pending'"),
                        {"task_id": task_id},
                    )
                else:
                    uow.session.execute(
                        text(
                            """
                            UPDATE cleanup_index
                            SET versions_info_json = CAST(:versions AS jsonb)
                            WHERE id = :task_id
                              AND status = 'pending'
                            """
                        ),
                        {"task_id": task_id, "versions": json.dumps(remaining, ensure_ascii=False)},
                    )
                affected_count += 1
            uow.commit()
        return affected_count

    def _update_status(self, task_ids: Sequence[Any], status: str) -> int:
        ids = _task_ids(task_ids)
        if not ids:
            return 0
        with self._unit_of_work() as uow:
            self._lock_cleanup_rows(uow, ids)
            conflicts = self._active_item_conflicts(uow, (), cleanup_index_ids=ids)
            if conflicts:
                raise CleanupExecutionConflictError(conflicts)
            updated = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_index
                    SET status = :status,
                        last_updated_at = NOW()
                    WHERE id = ANY(CAST(:task_ids AS integer[]))
                      AND status = 'pending'
                    """
                ),
                {"task_ids": list(ids), "status": status},
            )
            updated_count = _rowcount(updated)
            uow.commit()
        return updated_count

    @staticmethod
    def _lock_cleanup_rows(uow: SqlAlchemyUnitOfWork, task_ids: Sequence[int]) -> tuple[Mapping[str, Any], ...]:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT id, tmdb_id, item_type, status, versions_info_json,
                           best_version_json, last_updated_at
                    FROM cleanup_index
                    WHERE id = ANY(CAST(:task_ids AS integer[]))
                    ORDER BY id
                    FOR UPDATE
                    """
                ),
                {"task_ids": list(task_ids)},
            )
            .mappings()
            .all()
        )
        return tuple(dict(row) for row in rows)

    @staticmethod
    def _active_item_conflicts(
        uow: SqlAlchemyUnitOfWork,
        item_keys: Sequence[str],
        *,
        cleanup_index_ids: Sequence[int] = (),
    ) -> tuple[str, ...]:
        if not item_keys and not cleanup_index_ids:
            return ()
        rows = uow.session.scalars(
            text(
                """
                SELECT item_key
                FROM cleanup_execution_items
                WHERE (
                        item_key = ANY(CAST(:item_keys AS text[]))
                     OR cleanup_index_id = ANY(CAST(:cleanup_index_ids AS integer[]))
                     OR EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(
                                CASE
                                    WHEN jsonb_typeof(snapshot_json->'cleanup_indexes') = 'array'
                                    THEN snapshot_json->'cleanup_indexes'
                                    ELSE '[]'::jsonb
                                END
                            ) AS cleanup_ref(value)
                            WHERE cleanup_ref.value->>'id' = ANY(CAST(:cleanup_index_id_texts AS text[]))
                        )
                )
                  AND status IN ('queued','claimed','effect_started')
                ORDER BY item_key
                FOR UPDATE
                """
            ),
            {
                "item_keys": list(item_keys),
                "cleanup_index_ids": list(cleanup_index_ids),
                "cleanup_index_id_texts": [str(value) for value in cleanup_index_ids],
            },
        ).all()
        return tuple(str(value) for value in rows)

    def _cleanup_snapshots(
        self,
        uow: SqlAlchemyUnitOfWork,
        task_ids: Sequence[int],
        by_id: Mapping[int, Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        snapshots: list[dict[str, Any]] = []
        seen_versions: set[str] = set()
        for task_id in task_ids:
            row = by_id[task_id]
            versions = _version_entries(row["versions_info_json"])
            safe_ids = _best_version_ids(row["best_version_json"])
            if not safe_ids:
                raise ValueError(f"cleanup task {task_id} has no frozen best version")
            for version in versions:
                emby_id = _version_id(version)
                if emby_id in safe_ids:
                    continue
                if not emby_id or emby_id in seen_versions:
                    raise ValueError(f"cleanup task {task_id} contains an invalid or duplicate version identity")
                identity = self._media_projection.execution_identity(
                    uow,
                    emby_id,
                    fallback_path=_optional_text(version.get("path")),
                    fallback_tmdb_id=str(row["tmdb_id"]),
                    fallback_item_type=str(row["item_type"]),
                )
                if identity is None:
                    raise ValueError(f"cleanup version {emby_id!r} has no execution identity")
                seen_versions.add(emby_id)
                snapshots.append(
                    {
                        "kind": "cleanup_version",
                        "cleanup_index_id": task_id,
                        "item_key": f"version:{emby_id}",
                        "emby_id": emby_id,
                        "tmdb_id": str(row["tmdb_id"]),
                        "item_type": str(row["item_type"]),
                        "best_version_ids": sorted(safe_ids),
                        "last_updated_at": _iso_value(row["last_updated_at"]),
                        "target": _json_value(self._execution_policy.freeze_target(identity)),
                    }
                )
        return snapshots

    def _stage_job(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        execution_key: str,
        snapshots: Sequence[Mapping[str, Any]],
        requested_by_user_id: str | None,
        trace_id: str | None,
        parent_job_id: UUID | None = None,
        workflow_step: int | None = None,
    ) -> CleanupExecutionSubmission:
        job_id = uuid4()
        attempt_id = uuid4()
        event_id = uuid4()
        payload = {
            "payload_version": 1,
            "parameters": {"job_id": str(job_id), "execution_key": execution_key},
            "positional_parameters": [],
            "processor_type": "media",
            "display_name": f"执行 {len(snapshots)} 项媒体去重",
            "silent": False,
            "call_style": "direct",
            "legacy_operation": {
                "module": _CLEANUP_OPERATION_MODULE,
                "qualname": _CLEANUP_OPERATION_QUALNAME,
            },
        }
        repository = SqlAlchemyJobRepository(uow.session)
        repository.insert_job(
            {
                "id": job_id,
                "task_key": _CLEANUP_TASK_KEY,
                "trigger_type": "manual",
                "queue_name": "legacy.serial",
                "payload_version": 1,
                "request_payload_json": canonical_json(payload, field="cleanup Job payload"),
                "requested_by_user_id": requested_by_user_id,
                "parent_job_id": parent_job_id,
                "workflow_step": workflow_step,
                "max_attempts": cleanup_job_max_attempts(len(snapshots)),
                "timeout_seconds": 3600,
                "scheduled_for": None,
                "trace_id": trace_id,
            }
        )
        repository.insert_attempt(attempt_id=attempt_id, job_id=job_id, attempt_no=1)
        repository.insert_dispatch_event(event_id=event_id, job_id=job_id, attempt_id=attempt_id)
        key_hash = hashlib.sha256(f"{_CLEANUP_IDEMPOTENCY_SCOPE}\0{execution_key}".encode()).digest()
        request_hash = canonical_json_hash(payload, field="cleanup Job payload")
        repository.insert_idempotency(
            record_id=uuid4(),
            scope=_CLEANUP_IDEMPOTENCY_SCOPE,
            key_hash=key_hash,
            request_hash=request_hash,
            job_id=job_id,
            attempt_id=attempt_id,
            result_json=json.dumps({"job_id": str(job_id)}, separators=(",", ":")),
        )
        for snapshot in snapshots:
            cleanup_index_id = snapshot.get("cleanup_index_id")
            item_key = str(snapshot["item_key"])
            uow.session.execute(
                text(
                    """
                    INSERT INTO cleanup_execution_items (
                        id, execution_key, job_id, cleanup_index_id, item_key, snapshot_json
                    ) VALUES (
                        :id, :execution_key, :job_id, :cleanup_index_id, :item_key,
                        CAST(:snapshot_json AS jsonb)
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "execution_key": execution_key,
                    "job_id": job_id,
                    "cleanup_index_id": cleanup_index_id,
                    "item_key": item_key,
                    "snapshot_json": canonical_json(dict(snapshot), field="cleanup execution snapshot"),
                },
            )
        return CleanupExecutionSubmission(job_id, execution_key, len(snapshots))

    def _existing_submission(
        self,
        uow: SqlAlchemyUnitOfWork,
        execution_key: str,
        *,
        parent_job_id: UUID | None = None,
        workflow_step: int | None = None,
    ) -> CleanupExecutionSubmission | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT item.job_id, COUNT(*) AS item_count, job.status AS job_status,
                           job.parent_job_id, job.workflow_step,
                           BOOL_OR(item.effect_started_at IS NOT NULL) AS any_effect_started
                    FROM cleanup_execution_items AS item
                    JOIN job_runs AS job ON job.id = item.job_id
                    WHERE item.execution_key = :execution_key
                    GROUP BY item.job_id, job.status, job.parent_job_id, job.workflow_step
                    ORDER BY MIN(item.created_at)
                    LIMIT 1
                    """
                ),
                {"execution_key": execution_key},
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        item_count = int(row["item_count"])
        job_status = str(row["job_status"])
        if job_status in {"failed", "timed_out", "cancelled"}:
            if bool(row["any_effect_started"]):
                raise CleanupExecutionConflictError((f"job:{row['job_id']}:{job_status}",))
            resume_step = workflow_step
            if parent_job_id is not None and resume_step is None:
                resume_step = (
                    int(row["workflow_step"])
                    if row["parent_job_id"] == parent_job_id and row["workflow_step"] is not None
                    else self._next_parent_workflow_step(uow, parent_job_id)
                )
            self._resume_pre_effect_submission(
                uow,
                row["job_id"],
                item_count,
                parent_job_id=parent_job_id,
                workflow_step=resume_step,
            )
            return CleanupExecutionSubmission(
                row["job_id"],
                execution_key,
                item_count,
                reused=True,
                resumed=True,
            )
        if job_status != "succeeded" and parent_job_id is not None and row["parent_job_id"] != parent_job_id:
            raise CleanupExecutionConflictError((f"job:{row['job_id']}:different-parent",))
        if job_status != "succeeded" and workflow_step is not None and row["workflow_step"] != workflow_step:
            raise CleanupExecutionConflictError((f"job:{row['job_id']}:different-workflow-step",))
        return CleanupExecutionSubmission(row["job_id"], execution_key, item_count, reused=True)

    @staticmethod
    def _resume_pre_effect_submission(
        uow: SqlAlchemyUnitOfWork,
        job_id: UUID,
        item_count: int,
        *,
        parent_job_id: UUID | None,
        workflow_step: int | None,
    ) -> None:
        repository = SqlAlchemyJobRepository(uow.session)
        job = repository.get_job_row(job_id, for_update=True)
        if job is None or str(job["status"]) not in {"failed", "timed_out", "cancelled"}:
            return
        current_attempt = repository.get_current_attempt_row(
            job_id,
            int(job["current_attempt"]),
            for_update=True,
        )
        if current_attempt is None or str(current_attempt["status"]) not in {
            "succeeded",
            "failed",
            "timed_out",
            "cancelled",
            "lost",
        }:
            raise CleanupExecutionConflictError((f"job:{job_id}:attempt-active",))
        if parent_job_id is None and repository.has_other_running_root(job_id):
            raise CleanupExecutionConflictError((f"job:{job_id}:serial-busy",))
        if parent_job_id is not None:
            occupied = uow.session.scalar(
                text(
                    """
                    SELECT id
                    FROM job_runs
                    WHERE parent_job_id = :parent_job_id
                      AND workflow_step = :workflow_step
                      AND id <> :job_id
                    FOR UPDATE
                    """
                ),
                {
                    "parent_job_id": parent_job_id,
                    "workflow_step": workflow_step,
                    "job_id": job_id,
                },
            )
            if occupied is not None:
                raise CleanupExecutionConflictError((f"job:{job_id}:workflow-step-busy",))
        effect_count = int(
            uow.session.scalar(
                text(
                    """
                    SELECT COUNT(*)
                    FROM cleanup_execution_items
                    WHERE job_id = :job_id
                      AND effect_started_at IS NOT NULL
                    """
                ),
                {"job_id": job_id},
            )
            or 0
        )
        if effect_count:
            raise CleanupExecutionConflictError((f"job:{job_id}:effect-started",))
        attempt_no = int(job["current_attempt"]) + 1
        attempt_id = uuid4()
        repository.insert_attempt(attempt_id=attempt_id, job_id=job_id, attempt_no=attempt_no)
        repository.insert_dispatch_event(event_id=uuid4(), job_id=job_id, attempt_id=attempt_id)
        reset = uow.session.execute(
            text(
                """
                UPDATE cleanup_execution_items
                SET status = 'queued',
                    state_revision = state_revision + 1,
                    current_attempt_id = NULL,
                    claimed_by = NULL,
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    effect_started_at = NULL,
                    receipt_json = NULL,
                    error_json = NULL,
                    finished_at = NULL,
                    updated_at = NOW()
                WHERE job_id = :job_id
                  AND effect_started_at IS NULL
                  AND status IN ('queued','claimed','cancelled')
                """
            ),
            {"job_id": job_id},
        )
        if _rowcount(reset) != item_count:
            raise CleanupExecutionConflictError((f"job:{job_id}:item-state",))
        if not repository.mark_terminal_job_resumed(
            job_id,
            attempt_no=attempt_no,
            max_attempts=max(int(job["max_attempts"]), attempt_no, cleanup_job_max_attempts(item_count)),
        ):
            raise CleanupExecutionConflictError((f"job:{job_id}:resume-raced",))
        uow.session.execute(
            text(
                """
                UPDATE job_runs
                SET parent_job_id = :parent_job_id,
                    workflow_step = :workflow_step,
                    updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
                "parent_job_id": parent_job_id,
                "workflow_step": workflow_step,
            },
        )

    def _read_existing_submission(
        self,
        execution_key: str,
        *,
        parent_job_id: UUID | None = None,
        parent_attempt_id: UUID | None = None,
        parent_worker_id: str | None = None,
        workflow_step: int | None = None,
    ) -> CleanupExecutionSubmission | None:
        with self._unit_of_work() as uow:
            if parent_job_id is not None:
                if parent_attempt_id is None or not parent_worker_id:
                    raise ValueError("cleanup child staging requires the current parent Attempt and Worker")
                self._lock_live_parent_attempt(
                    uow,
                    parent_job_id,
                    parent_attempt_id,
                    parent_worker_id,
                )
            submission = self._existing_submission(
                uow,
                execution_key,
                parent_job_id=parent_job_id,
                workflow_step=workflow_step,
            )
            uow.commit()
            return submission

    @staticmethod
    def _lock_live_parent_attempt(
        uow: SqlAlchemyUnitOfWork,
        parent_job_id: UUID,
        parent_attempt_id: UUID,
        parent_worker_id: str,
    ) -> None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT job.current_attempt, job.status AS job_status,
                           attempt.attempt_no, attempt.status AS attempt_status,
                           attempt.worker_id, attempt.lease_expires_at > NOW() AS lease_active
                    FROM job_runs AS job
                    JOIN job_attempts AS attempt ON attempt.job_id = job.id
                    WHERE job.id = :job_id
                      AND attempt.id = :attempt_id
                    FOR UPDATE OF job, attempt
                    """
                ),
                {"job_id": parent_job_id, "attempt_id": parent_attempt_id},
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["job_status"] != "running"
            or row["attempt_status"] != "running"
            or int(row["attempt_no"]) != int(row["current_attempt"])
            or row["worker_id"] != parent_worker_id
            or row["lease_active"] is not True
        ):
            raise CleanupExecutionConflictError((f"parent:{parent_job_id}:attempt-stale",))

    @staticmethod
    def _next_parent_workflow_step(uow: SqlAlchemyUnitOfWork, parent_job_id: UUID) -> int:
        return int(
            uow.session.scalar(
                text(
                    """
                    SELECT COALESCE(MAX(workflow_step), 0) + 1
                    FROM job_runs
                    WHERE parent_job_id = :parent_job_id
                    """
                ),
                {"parent_job_id": parent_job_id},
            )
            or 1
        )


class PostgresCleanupExecutionGateway:
    def __init__(self, repository: SqlAlchemyCleanupRepository) -> None:
        self._repository = repository

    def submit_cleanup(
        self,
        task_ids: Sequence[Any],
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> CleanupExecutionSubmission:
        return self._repository.stage_cleanup(
            task_ids,
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
        )

    def submit_version_deletion(
        self,
        emby_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> CleanupExecutionSubmission:
        return self._repository.stage_version_deletion(
            emby_id,
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
        )


def _task_ids(values: Sequence[Any]) -> tuple[int, ...]:
    normalized: list[int] = []
    for value in values:
        if isinstance(value, bool):
            raise ValueError("cleanup task ids must be integers")
        try:
            task_id = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("cleanup task ids must be integers") from exc
        if task_id < 1:
            raise ValueError("cleanup task ids must be positive")
        if task_id not in normalized:
            normalized.append(task_id)
    return tuple(sorted(normalized))


def _execution_key(kind: str, snapshots: Sequence[Mapping[str, Any]]) -> str:
    envelope = {"kind": kind, "items": [dict(snapshot) for snapshot in snapshots]}
    return hashlib.sha256(canonical_json(envelope, field="cleanup execution").encode("utf-8")).hexdigest()


def _iso_value(value: Any) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    return str(isoformat()) if callable(isoformat) else str(value)


def _json_value(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def _version_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("cleanup version entry must be an object")
    return value


def _version_entries(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise TypeError("cleanup versions must be an array")
    return tuple(_version_mapping(item) for item in value)


def _version_id(value: Mapping[str, Any]) -> str:
    return str(value.get("id") or value.get("emby_item_id") or "").strip()


def _best_version_ids(value: Any) -> set[str]:
    values = value if isinstance(value, list) else (value,)
    return {str(item).strip() for item in values if str(item or "").strip()}


def _optional_text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _rowcount(result: Any) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


__all__ = [
    "CleanupExecutionConflictError",
    "CleanupExecutionPolicyPort",
    "CleanupExecutionSubmission",
    "CleanupMediaProjectionPort",
    "PostgresCleanupExecutionGateway",
    "SqlAlchemyCleanupRepository",
]
