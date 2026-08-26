from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import UnitOfWork, normalize_json_object
from etk.platform.jobs.json_payload import canonical_json

from .cleanup_execution import CleanupExecutionItem, CleanupExecutionSummary


class CleanupExecutionClaimLostError(RuntimeError):
    pass


class SqlAlchemyCleanupExecutionRepository:
    """Durable P7 Cleanup claims and receipts fenced to the current P4 Attempt."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock or (lambda: datetime.now(UTC))

    def claim_next(
        self,
        *,
        job_id: UUID,
        execution_key: str,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> CleanupExecutionItem | None:
        normalized_worker = _required_text(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        now = self._clock()
        with self._unit_of_work() as uow:
            attempt = self._lock_current_attempt(
                uow,
                job_id,
                attempt_id,
                normalized_worker,
                now=now,
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT item.*
                        FROM cleanup_execution_items AS item
                        WHERE item.job_id = :job_id
                          AND item.execution_key = :execution_key
                          AND item.status IN ('queued','claimed','effect_started')
                        ORDER BY item.created_at, item.id
                        LIMIT 1
                        FOR UPDATE
                        """
                    ),
                    {"job_id": job_id, "execution_key": execution_key},
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            current_attempt_id = row["current_attempt_id"]
            if current_attempt_id == attempt_id:
                if row["claimed_by"] != normalized_worker or row["claim_token"] is None:
                    raise CleanupExecutionClaimLostError("cleanup item belongs to another Worker")
                if row["lease_expires_at"] is not None and row["lease_expires_at"] > now:
                    projection = _item(dict(row), int(attempt["attempt_no"]), int(attempt["max_attempts"]))
                    uow.commit()
                    return projection
                refreshed = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE cleanup_execution_items
                            SET state_revision = state_revision + 1,
                                claim_token = :claim_token,
                                claimed_at = :now,
                                lease_expires_at = :lease_expires_at,
                                updated_at = :now
                            WHERE id = :item_id
                              AND state_revision = :revision
                              AND current_attempt_id = :attempt_id
                              AND claimed_by = :worker_id
                              AND claim_token = :old_claim_token
                            RETURNING *
                            """
                        ),
                        {
                            "claim_token": uuid4(),
                            "now": now,
                            "lease_expires_at": now + timedelta(seconds=lease_seconds),
                            "item_id": row["id"],
                            "revision": int(row["state_revision"]),
                            "attempt_id": attempt_id,
                            "worker_id": normalized_worker,
                            "old_claim_token": row["claim_token"],
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if refreshed is None:
                    raise CleanupExecutionClaimLostError("cleanup item lease refresh raced")
                projection = _item(
                    dict(refreshed),
                    int(attempt["attempt_no"]),
                    int(attempt["max_attempts"]),
                )
                uow.commit()
                return projection

            claim_token = uuid4()
            status = "claimed" if row["status"] == "queued" else str(row["status"])
            updated = (
                uow.session.execute(
                    text(
                        """
                        UPDATE cleanup_execution_items
                        SET status = :status,
                            state_revision = state_revision + 1,
                            current_attempt_id = :attempt_id,
                            claimed_by = :worker_id,
                            claim_token = :claim_token,
                            claimed_at = :now,
                            lease_expires_at = :lease_expires_at,
                            updated_at = :now
                        WHERE id = :item_id
                          AND state_revision = :revision
                        RETURNING *
                        """
                    ),
                    {
                        "status": status,
                        "attempt_id": attempt_id,
                        "worker_id": normalized_worker,
                        "claim_token": claim_token,
                        "now": now,
                        "lease_expires_at": now + timedelta(seconds=lease_seconds),
                        "item_id": row["id"],
                        "revision": int(row["state_revision"]),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if updated is None:
                raise CleanupExecutionClaimLostError("cleanup item claim raced with another Worker")
            projection = _item(dict(updated), int(attempt["attempt_no"]), int(attempt["max_attempts"]))
            uow.commit()
        return projection

    def save_receipt(
        self,
        item: CleanupExecutionItem,
        receipt: Mapping[str, Any],
        *,
        start_effect: bool,
        lease_seconds: int,
    ) -> CleanupExecutionItem:
        _positive(lease_seconds, "lease_seconds")
        normalized_receipt = _json(receipt, "cleanup execution receipt")
        now = self._clock()
        with self._unit_of_work() as uow:
            parent_job_id, parent_status = self._lock_parent_job(uow, item.job_id)
            attempt = self._lock_current_attempt(
                uow,
                item.job_id,
                item.attempt_id,
                item.worker_id,
                now=now,
            )
            if attempt["parent_job_id"] != parent_job_id:
                raise CleanupExecutionClaimLostError("cleanup execution parent changed")
            row = self._lock_owned_item(uow, item, now=now)
            first_effect = start_effect and row["effect_started_at"] is None
            if first_effect and (
                attempt["job_status"] != "running"
                or (parent_status is not None and parent_status not in {"running", "succeeded"})
            ):
                raise CleanupExecutionClaimLostError("cleanup effect start was fenced by Job state")
            status = "effect_started" if start_effect or row["status"] == "effect_started" else "claimed"
            updated = (
                uow.session.execute(
                    text(
                        """
                        UPDATE cleanup_execution_items
                        SET status = :status,
                            state_revision = state_revision + 1,
                            effect_started_at = CASE
                                WHEN :start_effect THEN COALESCE(effect_started_at, :now)
                                ELSE effect_started_at
                            END,
                            receipt_json = CAST(:receipt AS jsonb),
                            lease_expires_at = :lease_expires_at,
                            updated_at = :now
                        WHERE id = :item_id
                          AND state_revision = :revision
                          AND current_attempt_id = :attempt_id
                          AND claim_token = :claim_token
                        RETURNING *
                        """
                    ),
                    {
                        "status": status,
                        "start_effect": start_effect,
                        "now": now,
                        "receipt": canonical_json(normalized_receipt, field="cleanup execution receipt"),
                        "lease_expires_at": now + timedelta(seconds=lease_seconds),
                        "item_id": item.id,
                        "revision": item.revision,
                        "attempt_id": item.attempt_id,
                        "claim_token": item.claim_token,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if updated is None:
                raise CleanupExecutionClaimLostError("cleanup receipt checkpoint lost its claim")
            projection = _item(dict(updated), int(attempt["attempt_no"]), int(attempt["max_attempts"]))
            uow.commit()
        return projection

    def mark_confirmed(self, item: CleanupExecutionItem, receipt: Mapping[str, Any]) -> None:
        normalized_receipt = _json(receipt, "cleanup confirmed receipt")
        now = self._clock()
        with self._unit_of_work() as uow:
            self._lock_current_attempt(
                uow,
                item.job_id,
                item.attempt_id,
                item.worker_id,
                now=now,
            )
            self._lock_projection_cleanup_indexes(uow, item)
            row = self._lock_owned_item(uow, item, now=now)
            updated = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_execution_items
                    SET status = 'confirmed',
                        state_revision = state_revision + 1,
                        receipt_json = CAST(:receipt AS jsonb),
                        error_json = NULL,
                        finished_at = :now,
                        current_attempt_id = NULL,
                        claimed_by = NULL,
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = :now
                    WHERE id = :item_id
                      AND state_revision = :revision
                      AND current_attempt_id = :attempt_id
                      AND claim_token = :claim_token
                      AND status = 'effect_started'
                    """
                ),
                {
                    "receipt": canonical_json(normalized_receipt, field="cleanup confirmed receipt"),
                    "now": now,
                    "item_id": item.id,
                    "revision": item.revision,
                    "attempt_id": item.attempt_id,
                    "claim_token": item.claim_token,
                },
            )
            if _rowcount(updated) != 1:
                raise CleanupExecutionClaimLostError("cleanup confirmation lost its claim")
            self._finalize_cleanup_projection(uow, row)
            uow.commit()

    def mark_indeterminate(
        self,
        item: CleanupExecutionItem,
        receipt: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> None:
        self._mark_terminal(item, "indeterminate", receipt=receipt, error=error)

    def release_for_retry(self, item: CleanupExecutionItem, error: Mapping[str, Any]) -> None:
        normalized_error = _json(error, "cleanup retry error")
        now = self._clock()
        with self._unit_of_work() as uow:
            self._lock_current_attempt(
                uow,
                item.job_id,
                item.attempt_id,
                item.worker_id,
                now=now,
            )
            self._lock_owned_item(uow, item, now=now)
            updated = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_execution_items
                    SET status = 'queued',
                        state_revision = state_revision + 1,
                        error_json = CAST(:error AS jsonb),
                        current_attempt_id = NULL,
                        claimed_by = NULL,
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = :now
                    WHERE id = :item_id
                      AND state_revision = :revision
                      AND current_attempt_id = :attempt_id
                      AND claim_token = :claim_token
                      AND status = 'claimed'
                      AND effect_started_at IS NULL
                    """
                ),
                {
                    "error": canonical_json(normalized_error, field="cleanup retry error"),
                    "now": now,
                    "item_id": item.id,
                    "revision": item.revision,
                    "attempt_id": item.attempt_id,
                    "claim_token": item.claim_token,
                },
            )
            if _rowcount(updated) != 1:
                raise CleanupExecutionClaimLostError("cleanup pre-effect retry lost its claim")
            uow.commit()

    def cancel_unstarted_job(self, job_id: UUID, error: Mapping[str, Any]) -> int:
        return self._terminalize_unstarted(job_id, error, "cancelled")

    def abort_unstarted(self, job_id: UUID, error: Mapping[str, Any]) -> int:
        return self._terminalize_unstarted(job_id, error, "cancelled")

    def summary(self, job_id: UUID) -> CleanupExecutionSummary:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT
                            COUNT(*) FILTER (WHERE status = 'confirmed') AS confirmed,
                            COUNT(*) FILTER (WHERE status = 'indeterminate') AS indeterminate,
                            COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                            COUNT(*) FILTER (WHERE status IN ('queued','claimed','effect_started')) AS active
                        FROM cleanup_execution_items
                        WHERE job_id = :job_id
                        """
                    ),
                    {"job_id": job_id},
                )
                .mappings()
                .one()
            )
        return CleanupExecutionSummary(
            int(row["confirmed"] or 0),
            int(row["indeterminate"] or 0),
            int(row["cancelled"] or 0),
            int(row["active"] or 0),
        )

    def protect_stale_post_effect_cancellations(self) -> int:
        """Let P4 recover a stale Attempt when a durable Cleanup effect needs readback."""

        with self._unit_of_work() as uow:
            recovered_job_ids = tuple(
                uow.session.scalars(
                    text(
                        """
                        UPDATE job_runs AS job
                        SET status = 'running',
                            progress_message = '取消发生在媒体清理效果检查点后，继续权威回读',
                            updated_at = NOW()
                        FROM job_attempts AS attempt
                        WHERE job.task_key = 'p7-cleanup-execution'
                          AND job.status = 'cancel_requested'
                          AND attempt.job_id = job.id
                          AND attempt.attempt_no = job.current_attempt
                          AND attempt.status IN ('claimed','running')
                          AND attempt.lease_expires_at <= NOW()
                          AND EXISTS (
                              SELECT 1
                              FROM cleanup_execution_items AS item
                              WHERE item.job_id = job.id
                                AND item.status = 'effect_started'
                          )
                        RETURNING job.id
                        """
                    )
                ).all()
            )
            if recovered_job_ids:
                now = self._clock()
                uow.session.execute(
                    text(
                        """
                        UPDATE cleanup_execution_items
                        SET status = 'cancelled',
                            state_revision = state_revision + 1,
                            error_json = jsonb_build_object('type', 'cancelled_after_other_cleanup_effect'),
                            finished_at = :now,
                            current_attempt_id = NULL,
                            claimed_by = NULL,
                            claim_token = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            updated_at = :now
                        WHERE job_id = ANY(CAST(:job_ids AS uuid[]))
                          AND status IN ('queued','claimed')
                          AND effect_started_at IS NULL
                        """
                    ),
                    {"job_ids": list(recovered_job_ids), "now": now},
                )
            count = len(recovered_job_ids)
            uow.commit()
        return count

    def prepare_post_effect_cancellation(self, job_id: UUID) -> bool:
        """Cancel untouched items while preserving a checkpointed item for readback recovery."""

        with self._unit_of_work() as uow:
            has_started_effect = self._cancel_unstarted_after_effect(uow, job_id)
            uow.commit()
        return has_started_effect

    def preserve_recovery_on_cancel(
        self,
        uow: UnitOfWork,
        *,
        job: Mapping[str, Any],
        attempt: Mapping[str, Any],
    ) -> bool:
        """Keep a queued recovery Attempt alive after a Cleanup effect checkpoint."""

        del attempt
        if job.get("task_key") != "p7-cleanup-execution" or job.get("status") not in {
            "queued",
            "retry_wait",
        }:
            return False
        job_id = UUID(str(job["id"]))
        if not self._cancel_unstarted_after_effect(uow, job_id):
            return False
        uow.session.execute(
            text(
                """
                UPDATE job_runs
                SET cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
                    progress_message = '取消发生在媒体清理效果检查点后，继续权威回读',
                    updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        )
        return True

    def _cancel_unstarted_after_effect(self, uow: UnitOfWork, job_id: UUID) -> bool:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT id, status, effect_started_at
                    FROM cleanup_execution_items
                    WHERE job_id = :job_id
                      AND status IN ('queued','claimed','effect_started')
                    ORDER BY created_at, id
                    FOR UPDATE
                    """
                ),
                {"job_id": job_id},
            )
            .mappings()
            .all()
        )
        has_started_effect = any(
            row["status"] == "effect_started" and row["effect_started_at"] is not None for row in rows
        )
        if not has_started_effect:
            return False
        now = self._clock()
        uow.session.execute(
            text(
                """
                UPDATE cleanup_execution_items
                SET status = 'cancelled',
                    state_revision = state_revision + 1,
                    error_json = jsonb_build_object('type', 'cancelled_after_other_cleanup_effect'),
                    finished_at = :now,
                    current_attempt_id = NULL,
                    claimed_by = NULL,
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    updated_at = :now
                WHERE job_id = :job_id
                  AND status IN ('queued','claimed')
                  AND effect_started_at IS NULL
                """
            ),
            {"job_id": job_id, "now": now},
        )
        return True

    def reconcile_terminal_jobs(self) -> tuple[int, int]:
        """Close durable items that can no longer receive another P4 Attempt."""

        with self._unit_of_work() as uow:
            now = self._clock()
            pre_effect = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_execution_items AS item
                    SET status = 'cancelled',
                        state_revision = state_revision + 1,
                        error_json = jsonb_build_object(
                            'type', 'cleanup_job_terminal_before_effect',
                            'job_status', job.status
                        ),
                        finished_at = :now,
                        current_attempt_id = NULL,
                        claimed_by = NULL,
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = :now
                    FROM job_runs AS job
                    WHERE item.job_id = job.id
                      AND job.status IN ('failed','timed_out','cancelled')
                      AND item.status IN ('queued','claimed')
                      AND item.effect_started_at IS NULL
                    """
                ),
                {"now": now},
            )
            post_effect = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_execution_items AS item
                    SET status = 'indeterminate',
                        state_revision = state_revision + 1,
                        error_json = jsonb_build_object(
                            'type', 'cleanup_job_terminal_after_effect',
                            'job_status', job.status
                        ),
                        finished_at = :now,
                        current_attempt_id = NULL,
                        claimed_by = NULL,
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = :now
                    FROM job_runs AS job
                    WHERE item.job_id = job.id
                      AND job.status IN ('failed','timed_out','cancelled')
                      AND item.status = 'effect_started'
                    """
                ),
                {"now": now},
            )
            counts = (_rowcount(pre_effect), _rowcount(post_effect))
            uow.commit()
        return counts

    def _mark_terminal(
        self,
        item: CleanupExecutionItem,
        status: str,
        *,
        receipt: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> None:
        normalized_receipt = _json(receipt, "cleanup terminal receipt")
        normalized_error = _json(error, "cleanup terminal error")
        now = self._clock()
        with self._unit_of_work() as uow:
            self._lock_current_attempt(
                uow,
                item.job_id,
                item.attempt_id,
                item.worker_id,
                now=now,
            )
            self._lock_owned_item(uow, item, now=now)
            updated = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_execution_items
                    SET status = :status,
                        state_revision = state_revision + 1,
                        receipt_json = CAST(:receipt AS jsonb),
                        error_json = CAST(:error AS jsonb),
                        finished_at = :now,
                        current_attempt_id = NULL,
                        claimed_by = NULL,
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = :now
                    WHERE id = :item_id
                      AND state_revision = :revision
                      AND current_attempt_id = :attempt_id
                      AND claim_token = :claim_token
                      AND status = 'effect_started'
                    """
                ),
                {
                    "status": status,
                    "receipt": canonical_json(normalized_receipt, field="cleanup terminal receipt"),
                    "error": canonical_json(normalized_error, field="cleanup terminal error"),
                    "now": now,
                    "item_id": item.id,
                    "revision": item.revision,
                    "attempt_id": item.attempt_id,
                    "claim_token": item.claim_token,
                },
            )
            if _rowcount(updated) != 1:
                raise CleanupExecutionClaimLostError("cleanup terminal transition lost its claim")
            uow.commit()

    def _terminalize_unstarted(self, job_id: UUID, error: Mapping[str, Any], status: str) -> int:
        normalized_error = _json(error, "cleanup cancellation error")
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_execution_items
                    SET status = :status,
                        state_revision = state_revision + 1,
                        error_json = CAST(:error AS jsonb),
                        finished_at = :now,
                        current_attempt_id = NULL,
                        claimed_by = NULL,
                        claim_token = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = :now
                    WHERE job_id = :job_id
                      AND status IN ('queued','claimed')
                      AND effect_started_at IS NULL
                    """
                ),
                {
                    "status": status,
                    "error": canonical_json(normalized_error, field="cleanup cancellation error"),
                    "now": self._clock(),
                    "job_id": job_id,
                },
            )
            count = _rowcount(updated)
            uow.commit()
        return count

    @staticmethod
    def _lock_current_attempt(
        uow: SqlAlchemyUnitOfWork,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        *,
        now: datetime,
    ) -> Mapping[str, Any]:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT attempt.*, job.max_attempts, job.current_attempt,
                           job.status AS job_status, job.parent_job_id,
                           attempt.lease_expires_at > :now AS lease_active
                    FROM job_runs AS job
                    JOIN job_attempts AS attempt
                      ON attempt.job_id = job.id
                     AND attempt.attempt_no = job.current_attempt
                    WHERE job.id = :job_id
                      AND attempt.id = :attempt_id
                    FOR UPDATE OF job, attempt
                    """
                ),
                {"job_id": job_id, "attempt_id": attempt_id, "now": now},
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["job_status"] not in {"running", "cancel_requested"}
            or row["status"] != "running"
            or row["worker_id"] != worker_id
            or row["lease_active"] is not True
        ):
            raise CleanupExecutionClaimLostError("cleanup execution Attempt is no longer current")
        return dict(row)

    @staticmethod
    def _lock_owned_item(
        uow: SqlAlchemyUnitOfWork,
        item: CleanupExecutionItem,
        *,
        now: datetime,
    ) -> Mapping[str, Any]:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT *, lease_expires_at > :now AS lease_active
                    FROM cleanup_execution_items
                    WHERE id = :item_id
                    FOR UPDATE
                    """
                ),
                {"item_id": item.id, "now": now},
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or int(row["state_revision"]) != item.revision
            or row["current_attempt_id"] != item.attempt_id
            or row["claimed_by"] != item.worker_id
            or row["claim_token"] != item.claim_token
            or row["status"] not in {"claimed", "effect_started"}
            or row["lease_active"] is not True
        ):
            raise CleanupExecutionClaimLostError("cleanup execution item claim is stale")
        return dict(row)

    @staticmethod
    def _lock_parent_job(
        uow: SqlAlchemyUnitOfWork,
        child_job_id: UUID,
    ) -> tuple[UUID | None, str | None]:
        parent_job_id = uow.session.scalar(
            text("SELECT parent_job_id FROM job_runs WHERE id = :job_id"),
            {"job_id": child_job_id},
        )
        if parent_job_id is None:
            return None, None
        parent = (
            uow.session.execute(
                text("SELECT id, status FROM job_runs WHERE id = :job_id FOR UPDATE"),
                {"job_id": parent_job_id},
            )
            .mappings()
            .one_or_none()
        )
        if parent is None:
            raise CleanupExecutionClaimLostError("cleanup execution parent disappeared")
        return parent_job_id, str(parent["status"])

    @staticmethod
    def _lock_projection_cleanup_indexes(
        uow: SqlAlchemyUnitOfWork,
        item: CleanupExecutionItem,
    ) -> None:
        cleanup_index_ids: set[int] = set()
        if item.cleanup_index_id is not None:
            cleanup_index_ids.add(int(item.cleanup_index_id))
        raw_indexes = item.snapshot.get("cleanup_indexes")
        if isinstance(raw_indexes, list):
            cleanup_index_ids.update(
                int(index["id"]) for index in raw_indexes if isinstance(index, Mapping) and index.get("id") is not None
            )
        if not cleanup_index_ids:
            return
        locked = tuple(
            uow.session.scalars(
                text(
                    """
                    SELECT id
                    FROM cleanup_index
                    WHERE id = ANY(CAST(:cleanup_index_ids AS integer[]))
                    ORDER BY id
                    FOR UPDATE
                    """
                ),
                {"cleanup_index_ids": sorted(cleanup_index_ids)},
            ).all()
        )
        if set(int(value) for value in locked) != cleanup_index_ids:
            raise CleanupExecutionClaimLostError("cleanup projection row disappeared")

    @staticmethod
    def _finalize_cleanup_projection(uow: SqlAlchemyUnitOfWork, row: Mapping[str, Any]) -> None:
        snapshot = row["snapshot_json"]
        if not isinstance(snapshot, Mapping):
            raise RuntimeError("cleanup execution snapshot is invalid")
        kind = str(snapshot.get("kind") or "")
        emby_id = str(snapshot.get("emby_id") or "").strip()
        cleanup_index_id = row["cleanup_index_id"]
        if kind == "cleanup_version" and cleanup_index_id is not None:
            remaining = int(
                uow.session.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM cleanup_execution_items
                        WHERE job_id = :job_id
                          AND cleanup_index_id = :cleanup_index_id
                          AND status <> 'confirmed'
                        """
                    ),
                    {"job_id": row["job_id"], "cleanup_index_id": cleanup_index_id},
                )
                or 0
            )
            if remaining == 0:
                updated_at = snapshot.get("last_updated_at")
                uow.session.execute(
                    text(
                        """
                        UPDATE cleanup_index
                        SET status = 'processed', last_updated_at = NOW()
                        WHERE id = :cleanup_index_id
                          AND status = 'pending'
                          AND (
                                (CAST(:last_updated_at AS timestamptz) IS NULL AND last_updated_at IS NULL)
                             OR last_updated_at = CAST(:last_updated_at AS timestamptz)
                          )
                        """
                    ),
                    {"cleanup_index_id": cleanup_index_id, "last_updated_at": updated_at},
                )
            return
        if kind != "single_version" or not emby_id:
            return
        indexes = snapshot.get("cleanup_indexes")
        if not isinstance(indexes, list):
            return
        for index in indexes:
            if not isinstance(index, Mapping):
                continue
            task_id = int(index["id"])
            current = (
                uow.session.execute(
                    text(
                        """
                        SELECT versions_info_json
                        FROM cleanup_index
                        WHERE id = :task_id AND status = 'pending'
                        FOR UPDATE
                        """
                    ),
                    {"task_id": task_id},
                )
                .mappings()
                .one_or_none()
            )
            if current is None or not isinstance(current["versions_info_json"], list):
                continue
            remaining_versions = [
                version
                for version in current["versions_info_json"]
                if not isinstance(version, Mapping)
                or str(version.get("id") or version.get("emby_item_id") or "").strip() != emby_id
            ]
            if len(remaining_versions) <= 1:
                uow.session.execute(
                    text("DELETE FROM cleanup_index WHERE id = :task_id AND status = 'pending'"),
                    {"task_id": task_id},
                )
            else:
                uow.session.execute(
                    text(
                        """
                        UPDATE cleanup_index
                        SET versions_info_json = CAST(:versions AS jsonb), last_updated_at = NOW()
                        WHERE id = :task_id AND status = 'pending'
                        """
                    ),
                    {"task_id": task_id, "versions": json.dumps(remaining_versions, ensure_ascii=False)},
                )


def _item(row: Mapping[str, Any], attempt_no: int, max_attempts: int) -> CleanupExecutionItem:
    snapshot = row["snapshot_json"]
    if not isinstance(snapshot, Mapping):
        raise RuntimeError("cleanup execution snapshot is invalid")
    receipt = row["receipt_json"]
    if receipt is not None and not isinstance(receipt, Mapping):
        raise RuntimeError("cleanup execution receipt is invalid")
    if row["current_attempt_id"] is None or row["claim_token"] is None or row["claimed_by"] is None:
        raise RuntimeError("cleanup execution active item has no claim")
    return CleanupExecutionItem(
        id=row["id"],
        job_id=row["job_id"],
        execution_key=str(row["execution_key"]),
        cleanup_index_id=int(row["cleanup_index_id"]) if row["cleanup_index_id"] is not None else None,
        item_key=str(row["item_key"]),
        snapshot=dict(snapshot),
        status=str(row["status"]),
        revision=int(row["state_revision"]),
        attempt_id=row["current_attempt_id"],
        worker_id=str(row["claimed_by"]),
        claim_token=row["claim_token"],
        attempt_no=attempt_no,
        max_attempts=max_attempts,
        effect_started=row["effect_started_at"] is not None,
        receipt=dict(receipt) if receipt is not None else None,
    )


def _json(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    return normalize_json_object(dict(value), field=field)


def _rowcount(result: Any) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


def _required_text(value: str, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _positive(value: int, field: str) -> None:
    if value < 1:
        raise ValueError(f"{field} must be positive")


__all__ = [
    "CleanupExecutionClaimLostError",
    "SqlAlchemyCleanupExecutionRepository",
]
