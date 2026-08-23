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


def _message(task_key: str, payload: Mapping[str, object] | None = None) -> TaskMessage:
    return TaskMessage.create(
        task_key=task_key,
        processor=ProcessorIdentity.MEDIA,
        payload=payload,
    )


@pytest.mark.parametrize(
    ("task_key", "payload"),
    [
        ("populate-metadata", {"batch_size": 5, "force_full_update": False}),
        ("backfill-media-metadata", {}),
        ("role-translation", {"force_full_update": True}),
        ("actor-translation", {}),
        ("restore_mediainfo", {"force_full_update": False}),
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


def test_router_preserves_typed_outcomes_and_never_returns_legacy_none() -> None:
    partial = TaskOutcome.partial("one item failed", {"failed": 1})
    partial_outcome = P5MediaTaskRouter(FakeCompatibility(partial)).execute(
        _message("populate-metadata"),
        object(),
    )
    false_outcome = P5MediaTaskRouter(FakeCompatibility(False)).execute(
        _message("restore_mediainfo"),
        object(),
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
