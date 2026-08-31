from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

from etk.domains.media.auto_tagging import AutoTaggingService
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus
from etk.platform.tasks import TaskOutcomeStatus


class FakeSettings:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self.values = dict(values)

    def load(self, key: str) -> Any:
        return self.values.get(key)


class FakeInventory:
    def __init__(self) -> None:
        self.libraries: Sequence[Mapping[str, Any]] = (
            {"Id": "library-1", "CollectionType": "movies"},
            {"Id": "ignored", "CollectionType": "boxsets"},
        )
        self.items = {
            "library-1": (
                {"Id": "item-1", "OfficialRating": "US: PG-13"},
                {"Id": "item-2", "CustomRating": "R"},
            )
        }

    def list_libraries(self) -> Sequence[Mapping[str, Any]]:
        return self.libraries

    def list_items(self, library_id: str) -> Sequence[Mapping[str, Any]]:
        return self.items.get(library_id, ())


class FakeEffects:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.status = MediaEffectStatus.CONFIRMED

    def apply(
        self,
        *,
        effect_key: str,
        item_id: str,
        operation: str,
        tags: Sequence[str],
    ) -> MediaEffectProjection:
        self.calls.append({"effect_key": effect_key, "item_id": item_id, "operation": operation, "tags": tuple(tags)})
        return cast(
            MediaEffectProjection,
            SimpleNamespace(id=UUID(int=len(self.calls)), status=self.status),
        )


class FakeProgress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []
        self.started_effects: list[str] = []
        self.committed_effects: list[str] = []
        self.abandoned_effects: list[str] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def record_started_effect(self, label: str) -> None:
        self.started_effects.append(label)

    def record_committed_effect(self, label: str) -> None:
        self.committed_effects.append(label)

    def record_abandoned_effect(self, label: str) -> None:
        self.abandoned_effects.append(label)


def _settings(rules: Any = None) -> FakeSettings:
    return FakeSettings(
        {
            "auto_tagging_rules": rules,
            "rating_mapping": {"US": [{"code": "PG-13", "label": "青少年"}]},
        }
    )


def test_auto_tagging_uses_server_rules_rating_filter_and_stable_effects() -> None:
    effects = FakeEffects()
    progress = FakeProgress()
    service = AutoTaggingService(
        _settings([{"tags": ["Family"], "rating_filters": ["青少年"]}]),
        FakeInventory(),
        effects,
    )

    outcome = service.execute(progress, operation_key="auto-tagging:job-1", parameters={})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["items"] == 2
    assert outcome.details["confirmed"] == 1
    assert outcome.details["filtered"] == 1
    assert effects.calls[0]["item_id"] == "item-1"
    assert effects.calls[0]["operation"] == "add"
    assert effects.calls[0]["tags"] == ("Family",)
    assert effects.calls[0]["effect_key"].startswith("auto-tagging:job-1:rule:0:item:item-1:")
    assert progress.started_effects == [effects.calls[0]["effect_key"]]
    assert progress.committed_effects == progress.started_effects
    assert progress.abandoned_effects == []
    assert progress.updates[-1][0] == 100


def test_auto_tagging_manual_remove_and_indeterminate_result_are_explicit() -> None:
    effects = FakeEffects()
    effects.status = MediaEffectStatus.INDETERMINATE
    service = AutoTaggingService(_settings(), FakeInventory(), effects)

    progress = FakeProgress()
    outcome = service.execute(
        progress,
        operation_key="auto-tagging:job-2",
        parameters={
            "operation": "remove",
            "library_ids": ["library-1"],
            "tags": ["Old"],
            "rating_filters": [],
        },
    )

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["indeterminate"] == 2
    assert {call["operation"] for call in effects.calls} == {"remove"}
    assert len(progress.started_effects) == 2
    assert progress.committed_effects == []
    assert progress.abandoned_effects == []


def test_auto_tagging_claimed_effect_is_reported_as_indeterminate_not_failed() -> None:
    effects = FakeEffects()
    effects.status = MediaEffectStatus.CLAIMED
    outcome = AutoTaggingService(_settings(), FakeInventory(), effects).execute(
        FakeProgress(),
        operation_key="auto-tagging:job-claimed",
        parameters={"operation": "add", "library_ids": ["library-1"], "tags": ["Fixture"]},
    )

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["failed"] == 0
    assert outcome.details["indeterminate"] == 2


def test_auto_tagging_without_rules_does_not_query_emby() -> None:
    inventory = FakeInventory()
    inventory.libraries = ()
    outcome = AutoTaggingService(_settings([]), inventory, FakeEffects()).execute(
        FakeProgress(),
        operation_key="auto-tagging:job-3",
        parameters={},
    )

    assert outcome.status is TaskOutcomeStatus.SKIPPED
