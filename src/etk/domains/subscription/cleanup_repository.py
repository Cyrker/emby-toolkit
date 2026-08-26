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


class SqlAlchemyCleanupRepository:
    """P7 transaction owner for cleanup state, durable claims, Job, and Outbox."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media_projection: CleanupMediaProjectionPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media_projection = media_projection

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
                              WHERE item.item_key = 'cleanup:' || idx.id::text
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
                snapshots = [
                    {
                        "kind": "cleanup_index",
                        "cleanup_index_id": task_id,
                        "tmdb_id": str(by_id[task_id]["tmdb_id"]),
                        "item_type": str(by_id[task_id]["item_type"]),
                        "versions_info": _json_value(by_id[task_id]["versions_info_json"]),
                        "best_version": _json_value(by_id[task_id]["best_version_json"]),
                        "last_updated_at": _iso_value(by_id[task_id]["last_updated_at"]),
                    }
                    for task_id in ids
                ]
                execution_key = _execution_key("cleanup_index", snapshots)
                existing = self._existing_submission(uow, execution_key)
                if existing is not None:
                    submission = existing
                else:
                    conflicts = self._active_item_conflicts(uow, [f"cleanup:{task_id}" for task_id in ids])
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
    ) -> CleanupExecutionSubmission:
        normalized_id = str(emby_id or "").strip()
        if not normalized_id:
            raise ValueError("cleanup version deletion requires emby_id")
        execution_key: str | None = None
        submission: CleanupExecutionSubmission | None = None
        try:
            with self._unit_of_work() as uow:
                media = self._media_projection.version_deletion_snapshot(uow, normalized_id)
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
                if media is None and not cleanup_rows:
                    raise LookupError(normalized_id)
                snapshot = {
                    "kind": "single_version",
                    "emby_id": normalized_id,
                    "media": (
                        {
                            "tmdb_id": str(media["tmdb_id"]),
                            "item_type": str(media["item_type"]),
                            "title": media["title"],
                            "asset_details": _json_value(media["asset_details"]),
                            "file_pickcodes": _json_value(media["file_pickcodes"]),
                        }
                        if media is not None
                        else None
                    ),
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
                existing = self._existing_submission(uow, execution_key)
                if existing is not None:
                    submission = existing
                else:
                    item_key = f"version:{normalized_id}"
                    conflicts = self._active_item_conflicts(uow, [item_key])
                    if conflicts:
                        raise CleanupExecutionConflictError(conflicts)
                    submission = self._stage_job(
                        uow,
                        execution_key=execution_key,
                        snapshots=[snapshot],
                        requested_by_user_id=requested_by_user_id,
                        trace_id=trace_id,
                    )
                    uow.commit()
        except IntegrityError as exc:
            if execution_key is not None:
                existing = self._read_existing_submission(execution_key)
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
            conflicts = self._active_item_conflicts(uow, [f"cleanup:{task_id}" for task_id in ids])
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
            conflicts = self._active_item_conflicts(uow, [f"cleanup:{task_id}" for task_id in ids])
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
    def _active_item_conflicts(uow: SqlAlchemyUnitOfWork, item_keys: Sequence[str]) -> tuple[str, ...]:
        if not item_keys:
            return ()
        rows = uow.session.scalars(
            text(
                """
                SELECT item_key
                FROM cleanup_execution_items
                WHERE item_key = ANY(CAST(:item_keys AS text[]))
                  AND status IN ('queued','claimed','effect_started')
                ORDER BY item_key
                FOR UPDATE
                """
            ),
            {"item_keys": list(item_keys)},
        ).all()
        return tuple(str(value) for value in rows)

    def _stage_job(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        execution_key: str,
        snapshots: Sequence[Mapping[str, Any]],
        requested_by_user_id: str | None,
        trace_id: str | None,
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
                "parent_job_id": None,
                "workflow_step": None,
                "max_attempts": 3,
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
            item_key = (
                f"cleanup:{int(cleanup_index_id)}"
                if cleanup_index_id is not None
                else f"version:{str(snapshot['emby_id'])}"
            )
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

    @staticmethod
    def _existing_submission(
        uow: SqlAlchemyUnitOfWork,
        execution_key: str,
    ) -> CleanupExecutionSubmission | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT job_id, COUNT(*) AS item_count
                    FROM cleanup_execution_items
                    WHERE execution_key = :execution_key
                    GROUP BY job_id
                    ORDER BY MIN(created_at)
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
        return CleanupExecutionSubmission(row["job_id"], execution_key, int(row["item_count"]), reused=True)

    def _read_existing_submission(self, execution_key: str) -> CleanupExecutionSubmission | None:
        with self._unit_of_work() as uow:
            return self._existing_submission(uow, execution_key)


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


def _rowcount(result: Any) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


__all__ = [
    "CleanupExecutionConflictError",
    "CleanupExecutionSubmission",
    "CleanupMediaProjectionPort",
    "PostgresCleanupExecutionGateway",
    "SqlAlchemyCleanupRepository",
]
