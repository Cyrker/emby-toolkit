from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast
from uuid import UUID

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
    P8TaskProgress,
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


class FakeUserExpiry:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("expiry complete", {"successful": 2})
        self.calls: list[tuple[UUID, str]] = []

    def execute(
        self,
        progress: P8TaskProgress,
        *,
        parent_job_id: UUID,
        worker_id: str,
    ) -> TaskOutcome:
        progress.update(100, "done")
        self.calls.append((parent_job_id, worker_id))
        return self.outcome


class FakeDailyTheme:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(
        self,
        progress: P8TaskProgress,
        *,
        publication_id: str,
        mode: str = "daily",
    ) -> TaskOutcome:
        self.calls.append(mode)
        assert publication_id == "00000000-0000-0000-0000-000000000293"
        progress.update(100, "published")
        return TaskOutcome.succeeded("published", {"publication_revision": 3})


class FakeMediaEmbeddings:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, progress: P8TaskProgress) -> TaskOutcome:
        self.calls += 1
        progress.update(100, "embedded")
        return TaskOutcome.succeeded("embedded", {"generated": 4})


class FakeNativeCollectionRefresh:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, progress: P8TaskProgress) -> TaskOutcome:
        self.calls += 1
        progress.update(100, "refreshed")
        return TaskOutcome.succeeded("refreshed", {"refreshed": 3})


class FakeAutoTagging:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(
        self,
        progress: P8TaskProgress,
        *,
        operation_key: str,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        self.calls.append((operation_key, dict(parameters)))
        progress.update(100, "tagged")
        return TaskOutcome.succeeded("tagged", {"confirmed": 2})


class FakeStudioImages:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, progress: P8TaskProgress, *, operation_key: str) -> TaskOutcome:
        self.calls.append(operation_key)
        progress.update(100, "studio images")
        return TaskOutcome.succeeded("studio images", {"confirmed": 3})


class FakeProgress:
    def __init__(self) -> None:
        self.last_progress: int | None = None
        self.checkpoints = 0
        self.effects: list[tuple[str, str]] = []

    def update(self, progress: int, message: str) -> None:
        del message
        self.last_progress = progress

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def success_reported(self) -> bool:
        return self.last_progress == 100

    def record_started_effect(self, label: str) -> None:
        self.effects.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.effects.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.effects.append(("abandoned", label))


def _message(
    task_key: str,
    payload: Mapping[str, object] | None = None,
    *,
    metadata: Mapping[str, object] | None = None,
) -> TaskMessage:
    return TaskMessage.create(
        task_key=task_key,
        processor=ProcessorIdentity.MEDIA,
        payload=payload,
        metadata=metadata,
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

    failed = router.execute(_message("custom-collections"), object(), progress)
    progress.update(100, "legacy terminal")
    succeeded = router.execute(_message("custom-collections"), object(), progress)

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


def test_user_expiry_is_processor_free_and_requires_worker_owned_metadata() -> None:
    gateway = FakeCompatibility()
    sync = FakeUserSync()
    expiry = FakeUserExpiry()
    router = P8EffectTaskRouter(
        cast(P8EffectsCompatibilityGateway, gateway),
        cast(UserSyncService, sync),
        user_expiry=expiry,
    )
    job_id = UUID("00000000-0000-0000-0000-000000000292")
    message = _message(
        "check-expired-users",
        metadata={"job_id": str(job_id), "worker_id": "fixture-worker"},
    )

    outcome = router.execute(message, None, FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["successful"] == 2
    assert outcome.details["compatibility_fallback"] is False
    assert expiry.calls == [(job_id, "fixture-worker")]
    assert gateway.calls == []
    assert sync.calls == 0

    with pytest.raises(ValueError, match="job_id must be a UUID"):
        router.execute(
            _message(
                "check-expired-users",
                metadata={"job_id": "not-a-uuid", "worker_id": "fixture-worker"},
            ),
            None,
            FakeProgress(),
        )
    with pytest.raises(ValueError, match="worker_id must not be empty"):
        router.execute(
            _message("check-expired-users", metadata={"job_id": str(job_id)}),
            None,
            FakeProgress(),
        )
    with pytest.raises(ValueError, match="does not accept parameters"):
        router.execute(
            _message(
                "check-expired-users",
                {"user_id": "client-selected"},
                metadata={"job_id": str(job_id), "worker_id": "fixture-worker"},
            ),
            None,
            FakeProgress(),
        )


def test_processor_free_router_does_not_require_a_compatibility_gateway() -> None:
    sync = FakeUserSync()
    daily_theme = FakeDailyTheme()
    embeddings = FakeMediaEmbeddings()
    collection_refresh = FakeNativeCollectionRefresh()
    auto_tagging = FakeAutoTagging()
    studio_images = FakeStudioImages()
    router = P8EffectTaskRouter(
        None,
        cast(UserSyncService, sync),
        daily_theme=daily_theme,
        media_embeddings=embeddings,
        native_collection_refresh=collection_refresh,
        auto_tagging=auto_tagging,
        studio_images=studio_images,
    )

    synced = router.execute(_message("sync-all-user-data"), None, FakeProgress())
    disabled_update = router.execute(_message("system-auto-update"), None, FakeProgress())
    published = router.execute(
        _message(
            "update-daily-theme",
            metadata={"job_id": "00000000-0000-0000-0000-000000000293"},
        ),
        None,
        FakeProgress(),
    )
    embedded = router.execute(_message("generate_embeddings"), None, FakeProgress())
    refreshed = router.execute(_message("refresh-collections"), None, FakeProgress())
    tagged = router.execute(
        _message(
            "execute-auto-tagging-rules",
            {"operation": "add", "tags": ["A"]},
            metadata={"job_id": "00000000-0000-0000-0000-000000000294"},
        ),
        None,
        FakeProgress(),
    )
    studio_images_outcome = router.execute(
        _message(
            "fill-studio-images",
            metadata={"job_id": "00000000-0000-0000-0000-000000000295"},
        ),
        None,
        FakeProgress(),
    )

    assert synced.status is TaskOutcomeStatus.SUCCEEDED
    assert disabled_update.status is TaskOutcomeStatus.SKIPPED
    assert published.status is TaskOutcomeStatus.SUCCEEDED
    assert embedded.status is TaskOutcomeStatus.SUCCEEDED
    assert embedded.details["generated"] == 4
    assert refreshed.status is TaskOutcomeStatus.SUCCEEDED
    assert refreshed.details["refreshed"] == 3
    assert tagged.status is TaskOutcomeStatus.SUCCEEDED
    assert tagged.details["confirmed"] == 2
    assert studio_images_outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert studio_images_outcome.details["confirmed"] == 3
    assert published.details["publication_revision"] == 3
    assert published.details["compatibility_fallback"] is False
    assert sync.calls == 1
    assert daily_theme.calls == ["daily"]
    assert embeddings.calls == 1
    assert collection_refresh.calls == 1
    assert auto_tagging.calls == [
        (
            "auto-tagging:00000000-0000-0000-0000-000000000294",
            {"operation": "add", "library_ids": None, "tags": ["A"], "rating_filters": None},
        )
    ]
    assert studio_images.calls == ["studio-images:00000000-0000-0000-0000-000000000295"]
    with pytest.raises(RuntimeError, match="compatibility execution is unavailable"):
        router.execute(_message("custom-collections"), object(), FakeProgress())


def test_manual_auto_tagging_payload_is_strict_and_server_selected() -> None:
    gateway = FakeCompatibility()
    sync = FakeUserSync()
    auto_tagging = FakeAutoTagging()
    router = P8EffectTaskRouter(
        cast(P8EffectsCompatibilityGateway, gateway),
        cast(UserSyncService, sync),
        auto_tagging=auto_tagging,
    )

    outcome = router.execute(
        _message(
            "execute-auto-tagging-rules",
            {
                "operation": "add",
                "library_ids": ["library-a", "library-a"],
                "tags": ["tag-a"],
                "rating_filters": ["PG-13"],
            },
            metadata={"job_id": "00000000-0000-0000-0000-000000000001"},
        ),
        object(),
        FakeProgress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert auto_tagging.calls == [
        (
            "auto-tagging:00000000-0000-0000-0000-000000000001",
            {
                "operation": "add",
                "library_ids": ["library-a"],
                "tags": ["tag-a"],
                "rating_filters": ["PG-13"],
            },
        )
    ]
    assert gateway.calls == []
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
