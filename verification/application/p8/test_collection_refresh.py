from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from etk.domains.collection.refresh import (
    NativeCollectionPublishResult,
    NativeCollectionReference,
    NativeCollectionRefreshService,
)
from etk.platform.tasks import TaskOutcomeStatus


class FakeProvider:
    def __init__(self, references: Sequence[NativeCollectionReference]) -> None:
        self.references = tuple(references)
        self.failures: set[str] = set()
        self.calls: list[str] = []

    def list_collections(self) -> Sequence[NativeCollectionReference]:
        return self.references

    def collection_details(self, tmdb_collection_id: str) -> Mapping[str, Any]:
        self.calls.append(tmdb_collection_id)
        if tmdb_collection_id in self.failures:
            raise RuntimeError("fixture provider failure")
        return {"id": int(tmdb_collection_id), "parts": [{"id": 1}]}


class FakeRepository:
    def __init__(self) -> None:
        self.reconciled: list[tuple[str, ...]] = []
        self.published: list[tuple[str, bool, str]] = []

    def reconcile_active(self, emby_collection_ids: Sequence[str], *, observed_at: datetime) -> int:
        self.reconciled.append((*tuple(emby_collection_ids), observed_at.isoformat()))
        return 2

    def publish(
        self,
        reference: NativeCollectionReference,
        details: Mapping[str, Any],
        *,
        auto_subscribe: bool,
        today: Any,
        observed_at: datetime,
    ) -> NativeCollectionPublishResult:
        assert str(details["id"]) == reference.tmdb_collection_id
        assert observed_at.tzinfo is not None
        self.published.append((reference.emby_collection_id, auto_subscribe, today.isoformat()))
        return NativeCollectionPublishResult(movie_count=3, subscription_candidates=2)


class FakeProgress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


def _reference(emby_id: str, tmdb_id: str) -> NativeCollectionReference:
    return NativeCollectionReference(emby_id, tmdb_id, f"Collection {tmdb_id}", "library-1")


def test_native_collection_refresh_reconciles_authoritative_emby_scan_and_publishes() -> None:
    provider = FakeProvider((_reference("emby-1", "11"), _reference("emby-2", "22")))
    repository = FakeRepository()
    progress = FakeProgress()

    outcome = NativeCollectionRefreshService(
        provider,
        repository,
        auto_subscribe=lambda: True,
        clock=lambda: datetime(2026, 9, 1, tzinfo=UTC),
    ).execute(progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "collections": 2,
        "refreshed": 2,
        "deactivated": 2,
        "movies": 6,
        "subscription_candidates": 4,
        "failed": [],
    }
    assert repository.reconciled == [("emby-1", "emby-2", "2026-09-01T00:00:00+00:00")]
    assert repository.published == [
        ("emby-1", True, "2026-09-01"),
        ("emby-2", True, "2026-09-01"),
    ]
    assert progress.checkpoints == 3
    assert progress.updates[-1][0] == 100


def test_native_collection_refresh_retains_failed_collection_and_reports_partial() -> None:
    provider = FakeProvider((_reference("emby-1", "11"), _reference("emby-2", "22")))
    provider.failures.add("22")
    repository = FakeRepository()

    outcome = NativeCollectionRefreshService(
        provider,
        repository,
        auto_subscribe=lambda: False,
    ).execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["refreshed"] == 1
    assert outcome.details["failed"] == [
        {
            "emby_collection_id": "emby-2",
            "tmdb_collection_id": "22",
            "error_type": "RuntimeError",
        }
    ]
    assert repository.reconciled[0][:2] == ("emby-1", "emby-2")
    assert repository.published[0][:2] == ("emby-1", False)


def test_native_collection_refresh_can_authoritatively_deactivate_all() -> None:
    repository = FakeRepository()

    outcome = NativeCollectionRefreshService(
        FakeProvider(()),
        repository,
        auto_subscribe=lambda: True,
    ).execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["collections"] == 0
    assert outcome.details["deactivated"] == 2
    assert len(repository.reconciled) == 1
    assert len(repository.reconciled[0]) == 1
