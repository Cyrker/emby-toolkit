from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from etk.domains.subscription.cleanup_execution import CleanupExecutionService

_lock = threading.RLock()
_executor: CleanupExecutionService | None = None


def install_cleanup_executor(executor: CleanupExecutionService) -> None:
    global _executor
    with _lock:
        if _executor is not None and _executor is not executor:
            raise RuntimeError("a different P7 Cleanup executor is already installed")
        _executor = executor


def clear_cleanup_executor(executor: CleanupExecutionService | None = None) -> None:
    global _executor
    with _lock:
        if executor is None or _executor is executor:
            _executor = None


def execute_cleanup_job(job_id: str, execution_key: str) -> Mapping[str, Any]:
    from task_manager import current_persistent_execution_identity

    identity = current_persistent_execution_identity()
    if identity is None:
        raise RuntimeError("P7 Cleanup execution requires a persistent Worker context")
    parsed_job_id = UUID(str(job_id))
    if identity["job_id"] != str(parsed_job_id):
        raise RuntimeError("P7 Cleanup payload Job identity does not match the Worker context")
    with _lock:
        executor = _executor
    if executor is None:
        raise RuntimeError("P7 Cleanup executor is not installed")
    return executor.execute(
        job_id=parsed_job_id,
        execution_key=str(execution_key),
        attempt_id=UUID(identity["attempt_id"]),
        worker_id=identity["worker_id"],
        progress=_TaskManagerCleanupProgress(),
    )


class _TaskManagerCleanupProgress:
    def update(self, progress: int, message: str) -> None:
        from task_manager import update_status_from_thread

        update_status_from_thread(progress, message)

    def checkpoint(self) -> None:
        from task_manager import persistent_execution_checkpoint

        persistent_execution_checkpoint()

    def control_state(self) -> Mapping[str, Any]:
        from task_manager import persistent_execution_control_state

        return persistent_execution_control_state()


__all__ = [
    "clear_cleanup_executor",
    "execute_cleanup_job",
    "install_cleanup_executor",
]
