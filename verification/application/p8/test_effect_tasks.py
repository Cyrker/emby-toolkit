from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from etk.domains.identity.user_sync import UserSyncService
from etk.domains.operations.review_reprocess import (
    REVIEW_REPROCESS_ALL_TASK_KEY,
    REVIEW_REPROCESS_ONE_TASK_KEY,
)
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus, TaskRegistry
from etk.platform.tasks.catalog import P8_NATIVE_TASK_KEYS
from etk.processes.p8_effect_tasks import (
    P8EffectsCompatibilityGateway,
    P8EffectTaskRouter,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification/fixtures/tasks/ordered-task-catalog.json"


class FakeCompatibility:
    def __init__(self, result: object = True) -> None:
        self.result = result
        self.calls: list[tuple[str, object, dict[str, Any]]] = []

    def execute(self, task_key: str, processor: object, parameters: Mapping[str, Any]) -> object:
        self.calls.append((task_key, processor, dict(parameters)))
        return self.result


class FakeUserSync:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, progress: FakeProgress) -> TaskOutcome:
        self.calls += 1
        progress.update(100, "done")
        return TaskOutcome.succeeded("sync complete", {"synchronized": 2})


class FakeProgress:
    def __init__(self) -> None:
        self.last_progress: int | None = None
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        del message
        self.last_progress = progress

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def success_reported(self) -> bool:
        return self.last_progress == 100


def _message(task_key: str, payload: Mapping[str, object] | None = None) -> TaskMessage:
    return TaskMessage.create(
        task_key=task_key,
        processor=ProcessorIdentity.MEDIA,
        payload=payload,
    )


def _router(
    compatibility: FakeCompatibility | None = None,
    user_sync: FakeUserSync | None = None,
) -> tuple[P8EffectTaskRouter, FakeCompatibility, FakeUserSync]:
    gateway = compatibility or FakeCompatibility()
    sync = user_sync or FakeUserSync()
    return (
        P8EffectTaskRouter(
            cast(P8EffectsCompatibilityGateway, gateway),
            cast(UserSyncService, sync),
        ),
        gateway,
        sync,
    )


def test_fixture_marks_exactly_fourteen_p8_catalog_tasks_native() -> None:
    registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    actual = {definition.key for definition in registry.definitions() if definition.target_stage == "P8"}

    assert actual == P8_NATIVE_TASK_KEYS
    assert len(actual) == 14


def test_none_result_requires_explicit_terminal_progress() -> None:
    router, _gateway, _sync = _router(FakeCompatibility(None))
    progress = FakeProgress()

    failed = router.execute(_message("refresh-collections"), object(), progress)
    progress.update(100, "legacy terminal")
    succeeded = router.execute(_message("refresh-collections"), object(), progress)

    assert failed.status is TaskOutcomeStatus.FAILED
    assert succeeded.status is TaskOutcomeStatus.SUCCEEDED


def test_system_update_is_native_disabled_without_calling_legacy_or_docker() -> None:
    router, gateway, _sync = _router()

    outcome = router.execute(_message("system-auto-update"), None, FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details["compatibility_fallback"] is False
    assert gateway.calls == []


def test_user_sync_uses_native_service_without_media_processor() -> None:
    router, gateway, sync = _router()

    outcome = router.execute(_message("sync-all-user-data"), None, FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["synchronized"] == 2
    assert outcome.details["compatibility_fallback"] is False
    assert sync.calls == 1
    assert gateway.calls == []


def test_processor_free_router_does_not_require_a_compatibility_gateway() -> None:
    sync = FakeUserSync()
    router = P8EffectTaskRouter(None, cast(UserSyncService, sync))

    synced = router.execute(_message("sync-all-user-data"), None, FakeProgress())
    disabled_update = router.execute(_message("system-auto-update"), None, FakeProgress())

    assert synced.status is TaskOutcomeStatus.SUCCEEDED
    assert disabled_update.status is TaskOutcomeStatus.SKIPPED
    assert sync.calls == 1
    with pytest.raises(RuntimeError, match="compatibility execution is unavailable"):
        router.execute(_message("refresh-collections"), object(), FakeProgress())


def test_manual_auto_tagging_payload_is_strict_and_server_selected() -> None:
    router, gateway, _sync = _router()

    outcome = router.execute(
        _message(
            "execute-auto-tagging-rules",
            {
                "operation": "add",
                "library_ids": ["library-a", "library-a"],
                "tags": ["tag-a"],
                "rating_filters": ["PG-13"],
            },
        ),
        object(),
        FakeProgress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert gateway.calls[0][0] == "execute-auto-tagging-rules"
    assert gateway.calls[0][2] == {
        "operation": "add",
        "library_ids": ["library-a"],
        "tags": ["tag-a"],
        "rating_filters": ["PG-13"],
    }
    with pytest.raises(ValueError, match="requires tags"):
        router.execute(
            _message("execute-auto-tagging-rules", {"operation": "add", "tags": []}),
            object(),
            FakeProgress(),
        )


def test_single_collection_task_rejects_client_selected_executable_fields() -> None:
    router, _gateway, _sync = _router()

    with pytest.raises(ValueError, match="requires exactly"):
        router.execute(
            _message(
                "process-single-custom-collection",
                {"custom_collection_id": 7, "module": "tasks.media"},
            ),
            object(),
            FakeProgress(),
        )


def test_review_reprocess_internal_jobs_use_fixed_worker_allowlist() -> None:
    router, gateway, _sync = _router()

    single = router.execute_review(
        REVIEW_REPROCESS_ONE_TASK_KEY,
        {
            "item_id": "item-1",
            "item_name_for_ui": "Fixture",
            "failure_reason": "Missing",
        },
        object(),
        FakeProgress(),
    )
    batch = router.execute_review(
        REVIEW_REPROCESS_ALL_TASK_KEY,
        {"reason_filter": None},
        object(),
        FakeProgress(),
    )

    assert single.status is TaskOutcomeStatus.SUCCEEDED
    assert batch.status is TaskOutcomeStatus.SUCCEEDED
    assert gateway.calls == [
        (
            REVIEW_REPROCESS_ONE_TASK_KEY,
            gateway.calls[0][1],
            {
                "item_id": "item-1",
                "item_name_for_ui": "Fixture",
                "failure_reason": "Missing",
            },
        ),
        (
            REVIEW_REPROCESS_ALL_TASK_KEY,
            gateway.calls[1][1],
            {"reason_filter": None},
        ),
    ]
    with pytest.raises(ValueError, match="invalid parameter envelope"):
        router.execute_review(
            REVIEW_REPROCESS_ONE_TASK_KEY,
            {"item_id": "item-1", "module": "tasks.media"},
            object(),
            FakeProgress(),
        )
