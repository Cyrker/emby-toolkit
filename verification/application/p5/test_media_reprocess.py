from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from etk.domains.media import reprocess as reprocess_module
from etk.domains.media.reprocess import NativeMediaItemReprocessOperation, NativeMediaItemReprocessService
from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus


class Progress:
    job_id = "job-1"

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint", None))

    def update(self, progress: int, message: str) -> None:
        self.events.append(("update", (progress, message)))


class Catalog:
    def __init__(self, items: Mapping[str, Mapping[str, Any] | None]) -> None:
        self.items = dict(items)
        self.calls: list[str] = []

    def item(self, item_id: str) -> Mapping[str, Any] | None:
        self.calls.append(item_id)
        return self.items.get(item_id)


class Metadata:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("metadata", {"updated": 1})
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    def execute(self, task_key: str, progress: Any, parameters: Mapping[str, Any]) -> TaskOutcome:
        del progress
        self.calls.append((task_key, parameters))
        return self.outcome


class MediaInfo:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("mediainfo", {"updated": 1})
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        progress: Any,
        *,
        operation_key: str,
        force_full_update: bool = False,
        emby_item_ids: Sequence[str] | None = None,
    ) -> TaskOutcome:
        del progress
        self.calls.append(
            {
                "operation_key": operation_key,
                "force_full_update": force_full_update,
                "emby_item_ids": emby_item_ids,
            }
        )
        return self.outcome


def test_media_reprocess_validates_item_and_handles_missing_catalog_entry() -> None:
    service = NativeMediaItemReprocessService(Catalog({}), Metadata(), MediaInfo())
    progress = Progress()
    with pytest.raises(ValueError, match="must not be empty"):
        service.execute("", progress, operation_key="op")

    missing = service.execute("missing", progress, operation_key="op")
    assert missing.status is TaskOutcomeStatus.FAILED
    assert missing.details == {"item_id": "missing"}


def test_media_reprocess_can_skip_restore_and_return_media_info_only() -> None:
    service = NativeMediaItemReprocessService(
        Catalog({"movie-1": {"Id": "movie-1", "Type": "Movie"}}),
        Metadata(),
        MediaInfo(),
    )
    outcome = service.execute(
        "movie-1",
        Progress(),
        operation_key="op",
        media_info_only=True,
        restore_media_info=False,
    )
    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details["metadata_status"] == "not_requested"


def test_media_reprocess_episode_requires_series_and_uses_root_tmdb_identity() -> None:
    catalog = Catalog(
        {
            "episode-missing": {"Id": "episode-missing", "Type": "Episode"},
            "episode-1": {"Id": "episode-1", "Type": "Episode", "SeriesId": "series-1"},
            "series-1": {"Id": "series-1", "Type": "Series", "ProviderIds": {"Tmdb": "200"}},
        }
    )
    metadata = Metadata()
    mediainfo = MediaInfo()
    service = NativeMediaItemReprocessService(catalog, metadata, mediainfo)

    missing = service.execute("episode-missing", Progress(), operation_key="op")
    assert missing.status is TaskOutcomeStatus.FAILED
    assert missing.details["mediainfo_status"] == "succeeded"

    completed = service.execute("episode-1", Progress(), operation_key="op")
    assert completed.status is TaskOutcomeStatus.SUCCEEDED
    assert catalog.calls[-2:] == ["episode-1", "series-1"]
    assert metadata.calls[-1] == (
        "backfill-media-metadata",
        {"targets": [{"tmdb_id": "200", "media_type": "tv"}]},
    )
    assert mediainfo.calls[-1]["emby_item_ids"] == ("episode-1",)


def test_media_reprocess_reports_missing_root_identity_and_movie_identity() -> None:
    catalog = Catalog(
        {
            "unknown": {"Id": "unknown", "Type": "Video", "ProviderIds": {}},
            "movie-1": {"Id": "movie-1", "Type": "Movie", "ProviderIds": {"tmdb": "100"}},
        }
    )
    metadata = Metadata()
    service = NativeMediaItemReprocessService(catalog, metadata, MediaInfo())

    unknown = service.execute("unknown", Progress(), operation_key="op")
    assert unknown.status is TaskOutcomeStatus.PARTIAL
    assert unknown.details["metadata_status"] == "failed"

    movie = service.execute("movie-1", Progress(), operation_key="op")
    assert movie.status is TaskOutcomeStatus.SUCCEEDED
    assert metadata.calls[-1][1] == {"targets": [{"tmdb_id": "100", "media_type": "movie"}]}


def test_media_reprocess_operation_validates_schema_and_returns_serialized_outcome() -> None:
    service = NativeMediaItemReprocessService(
        Catalog({"movie-1": {"Type": "Movie", "ProviderIds": {"Tmdb": "100"}}}),
        Metadata(),
        MediaInfo(),
    )
    operation = NativeMediaItemReprocessOperation(service)
    progress = Progress()

    with pytest.raises(ValueError, match="unsupported fields"):
        operation.execute({"item_id": "movie-1", "extra": True}, progress)
    for field in ("media_info_only", "restore_media_info"):
        with pytest.raises(TypeError, match="must be boolean"):
            operation.execute({"item_id": "movie-1", field: "yes"}, progress)

    result = operation.execute(
        {"item_id": "movie-1", "media_info_only": False, "restore_media_info": True},
        progress,
    )
    assert result["status"] == "succeeded"
    assert result["details"]["metadata_status"] == "succeeded"


@pytest.mark.parametrize(
    ("mediainfo", "metadata", "expected"),
    [
        (TaskOutcome.failed("m1"), TaskOutcome.failed("m2"), TaskOutcomeStatus.FAILED),
        (TaskOutcome.failed("m1"), TaskOutcome.succeeded("m2"), TaskOutcomeStatus.PARTIAL),
        (TaskOutcome.skipped("m1"), TaskOutcome.skipped("m2"), TaskOutcomeStatus.SKIPPED),
        (TaskOutcome.skipped("m1"), TaskOutcome.succeeded("m2"), TaskOutcomeStatus.SUCCEEDED),
    ],
)
def test_media_reprocess_combines_owner_outcomes(
    mediainfo: TaskOutcome,
    metadata: TaskOutcome,
    expected: TaskOutcomeStatus,
) -> None:
    outcome = reprocess_module._combine("movie-1", mediainfo, metadata)
    assert outcome.status is expected
    assert outcome.details["item_id"] == "movie-1"


def test_media_reprocess_with_details_preserves_original_status() -> None:
    outcome = reprocess_module._with_details(
        TaskOutcome.partial("partial", {"one": 1}),
        "movie-1",
        metadata=TaskOutcome.failed("failed"),
    )
    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "item_id": "movie-1",
        "mediainfo_status": "partial",
        "metadata_status": "failed",
        "mediainfo": {"one": 1},
    }
