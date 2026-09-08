from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from etk.domains.subscription.cleanup_repository import SqlAlchemyCleanupRepository


class PersistentResubscribeCleanupGateway:
    """Stage resubscribe delete rules into the durable P7 cleanup runtime."""

    def __init__(self, repository: SqlAlchemyCleanupRepository) -> None:
        self._repository = repository

    def stage_version_deletion(self, emby_id: str, *, progress: Any | None = None) -> dict[str, Any]:
        identity = _progress_identity(progress)
        if identity is None:
            # Compatibility handlers do not receive the native progress object.
            import task_manager

            identity = task_manager.current_persistent_execution_identity()
        if not isinstance(identity, Mapping):
            raise RuntimeError("resubscribe cleanup requires a persistent Worker identity")
        parent_job_id = UUID(str(identity.get("job_id") or ""))
        parent_attempt_id = UUID(str(identity.get("attempt_id") or ""))
        parent_worker_id = str(identity.get("worker_id") or "").strip()
        if not parent_worker_id:
            raise RuntimeError("resubscribe cleanup Worker identity is incomplete")
        submission = self._repository.stage_version_deletion(
            emby_id,
            parent_job_id=parent_job_id,
            parent_attempt_id=parent_attempt_id,
            parent_worker_id=parent_worker_id,
        )
        return {
            "job_id": str(submission.job_id),
            "execution_key": submission.execution_key,
            "item_count": submission.item_count,
            "reused": submission.reused,
            "resumed": submission.resumed,
        }


def _progress_identity(progress: Any | None) -> dict[str, str] | None:
    if progress is None:
        return None
    values = {
        "job_id": getattr(progress, "job_id", None),
        "attempt_id": getattr(progress, "attempt_id", None),
        "worker_id": getattr(progress, "worker_id", None),
    }
    if not all(values.values()):
        return None
    return {key: str(value) for key, value in values.items()}


__all__ = ["PersistentResubscribeCleanupGateway"]
