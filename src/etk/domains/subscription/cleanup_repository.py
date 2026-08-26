from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class CleanupMediaProjectionPort(Protocol):
    def list_pending_tasks(self, uow: SqlAlchemyUnitOfWork) -> Sequence[Mapping[str, Any]]: ...


class SqlAlchemyCleanupRepository:
    """P7 transaction owner for the legacy cleanup_index state machine."""

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
                uow.session.execute(text("SELECT id FROM cleanup_index WHERE status = 'pending' ORDER BY id"))
                .scalars()
                .all()
            )
        return tuple(int(row) for row in rows)

    def ignore_tasks(self, task_ids: Sequence[Any]) -> int:
        return self._update_status(task_ids, "ignored")

    def mark_processed(self, task_ids: Sequence[Any]) -> int:
        return self._update_status(task_ids, "processed")

    def delete_tasks(self, task_ids: Sequence[Any]) -> int:
        ids = list(task_ids)
        if not ids:
            return 0
        with self._unit_of_work() as uow:
            deleted = uow.session.execute(
                text(
                    """
                    DELETE FROM cleanup_index
                    WHERE id = ANY(CAST(:task_ids AS integer[]))
                    """
                ),
                {"task_ids": ids},
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
                        {
                            "task_id": task_id,
                            "versions": json.dumps(remaining, ensure_ascii=False),
                        },
                    )
                affected_count += 1
            uow.commit()
        return affected_count

    def _update_status(self, task_ids: Sequence[Any], status: str) -> int:
        ids = list(task_ids)
        if not ids:
            return 0
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_index
                    SET status = :status,
                        last_updated_at = NOW()
                    WHERE id = ANY(CAST(:task_ids AS integer[]))
                    """
                ),
                {"task_ids": ids, "status": status},
            )
            updated_count = _rowcount(updated)
            uow.commit()
        return updated_count


def _version_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("cleanup version entry must be an object")
    return value


def _rowcount(result: Any) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


__all__ = ["CleanupMediaProjectionPort", "SqlAlchemyCleanupRepository"]
