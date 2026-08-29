from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import pytest

from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus
from etk.platform.tasks.catalog import P5_NATIVE_TASK_KEYS
from etk.processes import p5_media_tasks
from etk.processes.p5_media_tasks import (
    LegacyP5MediaCompatibilityGateway,
    P5MediaTaskRouter,
)


class FakeCompatibility:
    def __init__(self, result: object = None) -> None:
        self.result = result
        self.calls: list[tuple[str, object, Mapping[str, Any]]] = []

    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object:
        self.calls.append((task_key, processor, dict(parameters)))
        return self.result


class FakeProgress:
    def update(self, progress: int, message: str) -> None:
        del progress, message
        return None

    def checkpoint(self) -> None:
        return None


class FakeRestore:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("restore complete", {"restored": 1})
        self.calls: list[tuple[object, str, bool]] = []

    def execute(
        self,
        progress: object,
        *,
        operation_key: str,
        force_full_update: bool = False,
    ) -> TaskOutcome:
        self.calls.append((progress, operation_key, force_full_update))
        return self.outcome


def _message(task_key: str, payload: Mapping[str, object] | None = None) -> TaskMessage:
    return TaskMessage.create(
        task_key=task_key,
        processor=ProcessorIdentity.MEDIA,
        payload=payload,
        metadata={"job_id": "00000000-0000-0000-0000-000000000501"},
    )


@pytest.mark.parametrize(
    ("task_key", "payload"),
    [
        ("populate-metadata", {"batch_size": 5, "force_full_update": False}),
        ("backfill-media-metadata", {}),
        ("role-translation", {"force_full_update": True}),
        ("actor-translation", {}),
        ("restore-nfo-and-images", {}),
    ],
)
def test_all_p5_catalog_keys_execute_through_the_native_router(
    task_key: str,
    payload: Mapping[str, object],
) -> None:
    processor = object()
    compatibility = FakeCompatibility()
    router = P5MediaTaskRouter(compatibility)

    outcome = router.execute(_message(task_key, payload), processor)

    assert task_key in P5_NATIVE_TASK_KEYS
    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "task_key": task_key,
        "native_handler": "P5MediaTaskRouter",
        "compatibility_fallback": True,
        "fallback_retirement_stage": "P11",
    }
    assert compatibility.calls == [(task_key, processor, payload)]


def test_restore_mediainfo_keeps_p5_envelope_but_executes_native_business_use_case() -> None:
    compatibility = FakeCompatibility()
    restore = FakeRestore()
    progress = FakeProgress()
    router = P5MediaTaskRouter(compatibility, restore)

    outcome = router.execute(
        _message("restore_mediainfo", {"force_full_update": True}),
        None,
        progress,
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "restored": 1,
        "task_key": "restore_mediainfo",
        "native_handler": "FakeRestore",
        "envelope_owner": "P5MediaTaskRouter",
        "compatibility_fallback": False,
    }
    assert restore.calls == [
        (
            progress,
            "restore_mediainfo:00000000-0000-0000-0000-000000000501",
            True,
        )
    ]
    assert compatibility.calls == []


def test_restore_mediainfo_executes_with_no_compatibility_gateway() -> None:
    restore = FakeRestore()
    progress = FakeProgress()
    outcome = P5MediaTaskRouter(None, restore).execute(
        _message("restore_mediainfo", {"force_full_update": False}),
        None,
        progress,
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["compatibility_fallback"] is False
    assert restore.calls == [
        (
            progress,
            "restore_mediainfo:00000000-0000-0000-0000-000000000501",
            False,
        )
    ]


def test_targeted_backfill_normalizes_and_deduplicates_json_targets() -> None:
    processor = object()
    compatibility = FakeCompatibility()
    router = P5MediaTaskRouter(compatibility)

    outcome = router.execute(
        _message(
            "backfill-media-metadata",
            {
                "targets": [
                    {"tmdb_id": 100, "media_type": "MOVIE"},
                    {"tmdb_id": "100", "media_type": "movie"},
                    {"tmdb_id": "200", "media_type": "tv"},
                ]
            },
        ),
        processor,
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert compatibility.calls == [
        (
            "backfill-media-metadata",
            processor,
            {
                "targets": [
                    {"tmdb_id": "100", "media_type": "movie"},
                    {"tmdb_id": "200", "media_type": "tv"},
                ]
            },
        )
    ]


def test_explicit_empty_backfill_targets_skip_without_running_the_full_scan() -> None:
    compatibility = FakeCompatibility()
    outcome = P5MediaTaskRouter(compatibility).execute(
        _message("backfill-media-metadata", {"targets": []}),
        object(),
    )

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.message == "没有缺失元数据需要补齐。"
    assert compatibility.calls == []

    native_only_outcome = P5MediaTaskRouter(None).execute(
        _message("backfill-media-metadata", {"targets": []}),
        object(),
    )
    assert native_only_outcome.status is TaskOutcomeStatus.SKIPPED


def test_non_restore_p5_task_fails_closed_without_compatibility_gateway() -> None:
    with pytest.raises(RuntimeError, match="compatibility execution is unavailable"):
        P5MediaTaskRouter(None).execute(
            _message("populate-metadata", {"batch_size": 5}),
            object(),
        )


def test_router_preserves_typed_outcomes_and_never_returns_legacy_none() -> None:
    partial = TaskOutcome.partial("one item failed", {"failed": 1})
    partial_outcome = P5MediaTaskRouter(FakeCompatibility(partial)).execute(
        _message("populate-metadata"),
        object(),
    )
    failed_restore = FakeRestore(TaskOutcome.failed("restore failed"))
    false_outcome = P5MediaTaskRouter(FakeCompatibility(False), failed_restore).execute(
        _message("restore_mediainfo"),
        None,
        FakeProgress(),
    )
    mapping_outcome = P5MediaTaskRouter(FakeCompatibility({"ok": False, "message": "provider unavailable"})).execute(
        _message("actor-translation"),
        object(),
    )
    indeterminate_outcome = P5MediaTaskRouter(FakeCompatibility({"status": "indeterminate"})).execute(
        _message("restore-nfo-and-images"),
        object(),
    )

    assert partial_outcome.status is TaskOutcomeStatus.PARTIAL
    assert partial_outcome.details["failed"] == 1
    assert false_outcome.status is TaskOutcomeStatus.FAILED
    assert mapping_outcome.status is TaskOutcomeStatus.FAILED
    assert mapping_outcome.message == "provider unavailable"
    assert indeterminate_outcome.status is TaskOutcomeStatus.FAILED


@pytest.mark.parametrize(
    ("task_key", "payload", "error"),
    [
        ("populate-metadata", {"batch_size": 0}, "positive integer"),
        ("populate-metadata", {"force_full_update": 1}, "must be boolean"),
        ("actor-translation", {"force_full_update": False}, "unsupported parameters"),
        ("backfill-media-metadata", {"targets": "100"}, "must be a list"),
        (
            "backfill-media-metadata",
            {"targets": [{"tmdb_id": "100", "media_type": "series"}]},
            "must be movie or tv",
        ),
    ],
)
def test_native_payload_validation_fails_before_compatibility(
    task_key: str,
    payload: Mapping[str, object],
    error: str,
) -> None:
    compatibility = FakeCompatibility()

    with pytest.raises((TypeError, ValueError), match=error):
        P5MediaTaskRouter(compatibility).execute(_message(task_key, payload), object())

    assert compatibility.calls == []


def test_router_rejects_wrong_processor_and_payload_version() -> None:
    router = P5MediaTaskRouter(FakeCompatibility())

    with pytest.raises(ValueError, match="requires the media processor"):
        router.execute(
            TaskMessage.create(task_key="populate-metadata", processor=ProcessorIdentity.ACTOR),
            object(),
        )
    with pytest.raises(ValueError, match="payload version"):
        router.execute(
            TaskMessage.create(
                task_key="populate-metadata",
                processor=ProcessorIdentity.MEDIA,
                payload_version=2,
            ),
            object(),
        )


def test_legacy_gateway_uses_batch_callable_when_targets_key_is_present(monkeypatch) -> None:
    calls: list[tuple[str, object, Mapping[str, Any]]] = []

    def full(processor: object, **parameters: Any) -> str:
        calls.append(("full", processor, parameters))
        return "full"

    def batch(processor: object, **parameters: Any) -> str:
        calls.append(("batch", processor, parameters))
        return "batch"

    module = SimpleNamespace(
        task_backfill_media_metadata=full,
        task_backfill_media_metadata_batch=batch,
    )
    monkeypatch.setattr(p5_media_tasks, "import_module", lambda _name: module)
    processor = object()
    gateway = LegacyP5MediaCompatibilityGateway()

    assert gateway.execute("backfill-media-metadata", processor, {}) == "full"
    assert gateway.execute("backfill-media-metadata", processor, {"targets": []}) == "batch"
    assert calls == [
        ("full", processor, {}),
        ("batch", processor, {"targets": []}),
    ]


def test_legacy_gateway_rejects_native_restore_without_importing_legacy_callable(monkeypatch) -> None:
    monkeypatch.setattr(
        p5_media_tasks,
        "import_module",
        lambda _name: (_ for _ in ()).throw(AssertionError("native restore must not import tasks.media")),
    )

    with pytest.raises(LookupError, match="native-only"):
        LegacyP5MediaCompatibilityGateway().execute("restore_mediainfo", object(), {})
