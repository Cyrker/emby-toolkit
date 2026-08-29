from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from etk.platform.tasks import TaskOutcome

from .effect_execution import IdentityOperationExecution, IdentityUserMutationCoordinator
from .operation_repository import IdentityOperationScopeBusyError
from .operations import IdentityOperation, IdentityOperationReservation, IdentityOperationStatus


class UserExpiryProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class UserExpiryRepository(Protocol):
    def expired_user_ids(self, parent_job_id: UUID) -> Sequence[str]: ...

    def reserve_expired_user(
        self,
        parent_job_id: UUID,
        user_id: str,
        *,
        outbox_delay_seconds: float = 0,
        now: datetime | None = None,
    ) -> IdentityOperationReservation | None: ...

    def read(self, operation_id: UUID) -> IdentityOperation | None: ...


class IdentityUserExpiryService:
    """Reserve and execute durable per-user expiry operations for one P8 Job."""

    def __init__(
        self,
        repository: UserExpiryRepository,
        mutations: IdentityUserMutationCoordinator,
        *,
        lease_seconds: Callable[[], float],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._mutations = mutations
        self._lease_seconds = lease_seconds
        self._clock = clock

    def execute(
        self,
        progress: UserExpiryProgress,
        *,
        parent_job_id: UUID,
        worker_id: str,
    ) -> TaskOutcome:
        progress.checkpoint()
        progress.update(0, "正在检查过期用户")
        try:
            candidates = tuple(str(user_id).strip() for user_id in self._repository.expired_user_ids(parent_job_id))
        except Exception:
            return TaskOutcome.failed(
                "查询过期用户失败",
                {"parent_job_id": str(parent_job_id)},
            )
        candidates = tuple(user_id for user_id in candidates if user_id)
        if not candidates:
            progress.update(100, "任务完成：未发现过期用户")
            return TaskOutcome.skipped(
                "未发现过期用户",
                {"parent_job_id": str(parent_job_id), "candidate_count": 0},
            )

        operations: list[IdentityOperation] = []
        busy_user_ids: list[str] = []
        stale_user_ids: list[str] = []
        failed_user_ids: list[str] = []
        total = len(candidates)
        for index, user_id in enumerate(candidates, 1):
            progress.checkpoint()
            try:
                reservation = self._repository.reserve_expired_user(
                    parent_job_id,
                    user_id,
                    outbox_delay_seconds=0,
                    now=self._clock(),
                )
            except IdentityOperationScopeBusyError:
                busy_user_ids.append(user_id)
                progress.update(min(99, int(index / total * 100)), f"用户 {user_id} 有其他变更，跳过")
                continue
            except Exception:
                failed_user_ids.append(user_id)
                progress.update(min(99, int(index / total * 100)), f"用户 {user_id} 预留失败")
                continue
            if reservation is None:
                stale_user_ids.append(user_id)
                progress.update(min(99, int(index / total * 100)), f"用户 {user_id} 状态已变化，跳过")
                continue

            # A cancellation observed after reservation leaves the identity Outbox
            # intact; the reserved operation can still be recovered independently.
            progress.checkpoint()
            operation = reservation.operation
            current = self._repository.read(operation.id) or operation
            if not current.terminal:
                try:
                    execution = self._mutations.execute(
                        current.id,
                        worker_id=f"{worker_id}:expiry",
                        lease_seconds=self._lease_seconds(),
                        now=self._clock(),
                    )
                    current = _executed_operation(execution) or current
                except Exception:
                    failed_user_ids.append(user_id)
                    current = self._repository.read(current.id) or current
            operations.append(current)
            progress.update(
                min(99, int(index / total * 100)),
                f"正在处理过期用户 {index}/{total}",
            )

        details = _details(
            parent_job_id,
            candidates=candidates,
            operations=tuple(operations),
            busy_user_ids=tuple(busy_user_ids),
            stale_user_ids=tuple(stale_user_ids),
            failed_user_ids=tuple(failed_user_ids),
        )
        if details["failed"] or details["pending"] or details["busy"]:
            progress.update(100, "过期用户处理部分完成")
            return TaskOutcome.partial("过期用户处理部分完成", details)
        if details["successful"]:
            progress.update(100, "过期用户处理完成")
            return TaskOutcome.succeeded("过期用户处理完成", details)
        progress.update(100, "过期用户状态已变化，无需处理")
        return TaskOutcome.skipped("过期用户状态已变化，无需处理", details)


def _executed_operation(execution: IdentityOperationExecution | None) -> IdentityOperation | None:
    return execution.operation if execution is not None else None


def _details(
    parent_job_id: UUID,
    *,
    candidates: tuple[str, ...],
    operations: tuple[IdentityOperation, ...],
    busy_user_ids: tuple[str, ...],
    stale_user_ids: tuple[str, ...],
    failed_user_ids: tuple[str, ...],
) -> dict[str, object]:
    confirmed = [operation for operation in operations if operation.status is IdentityOperationStatus.CONFIRMED]
    failed = [
        operation
        for operation in operations
        if operation.status
        in {
            IdentityOperationStatus.REJECTED,
            IdentityOperationStatus.COMPENSATED,
            IdentityOperationStatus.INDETERMINATE,
        }
    ]
    pending = [operation for operation in operations if not operation.terminal]
    cancelled = [operation for operation in operations if operation.status is IdentityOperationStatus.CANCELLED]
    operation_failed_ids = [str(operation.request.get("user_id") or "") for operation in failed]
    pending_ids = [str(operation.request.get("user_id") or "") for operation in pending]
    cancelled_ids = [str(operation.request.get("user_id") or "") for operation in cancelled]
    all_failed_ids = list(dict.fromkeys([*failed_user_ids, *operation_failed_ids]))
    all_stale_ids = list(dict.fromkeys([*stale_user_ids, *cancelled_ids]))
    return {
        "parent_job_id": str(parent_job_id),
        "candidate_count": len(candidates),
        "total": len(candidates),
        "successful": len(confirmed),
        "failed": len(all_failed_ids),
        "pending": len(pending_ids),
        "busy": len(busy_user_ids),
        "skipped_due_to_state_change": len(all_stale_ids),
        "failed_user_ids": all_failed_ids,
        "pending_user_ids": pending_ids,
        "busy_user_ids": list(busy_user_ids),
        "skipped_user_ids": all_stale_ids,
        "operation_ids": [str(operation.id) for operation in operations],
    }


__all__ = [
    "IdentityUserExpiryService",
    "UserExpiryProgress",
    "UserExpiryRepository",
]
