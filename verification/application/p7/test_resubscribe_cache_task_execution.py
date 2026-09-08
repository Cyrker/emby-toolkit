from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from etk.domains.subscription.resubscribe_cache import (
    RESUBSCRIBE_CACHE_TASK_KEY,
    NativeResubscribeCacheTaskService,
)
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


class _Repository:
    def __init__(self, stats: Mapping[str, Any]) -> None:
        self.stats = dict(stats)
        self.checkpoints = 0

    def refresh_index(self, checkpoint: Any) -> Mapping[str, int]:
        checkpoint()
        self.checkpoints += 1
        return self.stats


def test_cache_task_service_is_provider_free_and_reports_repository_stats() -> None:
    repository = _Repository({"candidates": 3, "matched": 2, "purged": 1, "failed": 0})
    progress = _Progress()
    service = NativeResubscribeCacheTaskService(repository)

    outcome = service.execute(RESUBSCRIBE_CACHE_TASK_KEY, progress, {})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {"candidates": 3, "matched": 2, "purged": 1, "failed": 0}
    assert repository.checkpoints == 1
    assert progress.checkpoints == 2
    assert progress.updates[0][0] == 5
    assert progress.updates[-1][0] == 100


def test_cache_task_service_rejects_other_task_keys() -> None:
    service = NativeResubscribeCacheTaskService(_Repository({}))

    with pytest.raises(LookupError, match="unsupported resubscribe cache task key"):
        service.execute("resubscribe-library", _Progress(), {})


def test_router_uses_native_cache_owner_without_a_processor() -> None:
    repository = _Repository({"candidates": 0, "matched": 0, "purged": 0, "failed": 0})
    router = P7SubscriptionTaskRouter(
        assistant=None,
        resubscribe_cache=NativeResubscribeCacheTaskService(repository),
    )
    message = TaskMessage.create(
        task_key=RESUBSCRIBE_CACHE_TASK_KEY,
        processor=ProcessorIdentity.MEDIA,
        payload={},
    )

    outcome = router.execute(message, processor=None, progress=_Progress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["compatibility_fallback"] is False
    assert outcome.details["native_handler"] == "NativeResubscribeCacheTaskService"
