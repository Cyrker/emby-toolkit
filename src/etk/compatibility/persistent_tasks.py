from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from etk.domains.operations.jobs import (
    JobSubmissionService,
    TaskBusyError,
    UnsupportedLegacyOperationError,
)
from etk.platform.jobs import InvalidJsonPayloadError
from etk.platform.tasks import ProcessorIdentity

logger = logging.getLogger(__name__)


class PersistentTaskManagerGateway:
    """Keep legacy ``task_manager.submit_task`` callers off the API process."""

    def __init__(self, service: JobSubmissionService) -> None:
        self._service_provider: Callable[[], JobSubmissionService] = lambda: service

    def submit(
        self,
        task_function: Callable[..., Any],
        task_name: str,
        processor_type: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        silent: bool,
        max_attempts: int = 1,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
    ) -> bool:
        try:
            self._service_provider().submit_legacy_callable(
                task_function,
                task_name,
                ProcessorIdentity(processor_type),
                args,
                kwargs,
                silent=silent,
                queue_when_busy=True,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
            )
        except (TaskBusyError, UnsupportedLegacyOperationError, InvalidJsonPayloadError, ValueError):
            logger.warning("Legacy task submission was rejected", exc_info=True)
            return False
        return True

    def is_busy(self) -> bool:
        return self._service_provider().is_busy()

    def status(self) -> dict[str, Any]:
        return self._service_provider().legacy_status()

    def submit_key(
        self,
        task_key: str,
        task_name: str,
        processor_type: str,
        parameters: dict[str, Any],
        *,
        silent: bool,
        max_attempts: int = 1,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
    ) -> bool:
        del task_name
        try:
            self._service_provider().submit_task(
                task_key,
                parameters,
                trigger_type="system",
                silent=silent,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
                queue_name="legacy.backlog",
            )
        except (TaskBusyError, UnsupportedLegacyOperationError, InvalidJsonPayloadError, ValueError):
            logger.warning("Catalog task submission was rejected", exc_info=True)
            return False
        return True


class LazyPersistentTaskManagerGateway(PersistentTaskManagerGateway):
    """Install the facade at startup without opening a database connection."""

    def __init__(self, provider: Callable[[], JobSubmissionService]) -> None:
        self._service_provider = provider


__all__ = ["LazyPersistentTaskManagerGateway", "PersistentTaskManagerGateway"]
