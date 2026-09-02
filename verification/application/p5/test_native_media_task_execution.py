from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from etk.domains.media.task_execution import (
    NativeMediaMetadataTaskService,
    NativeMediaScreenshotBackfillService,
)
from etk.platform import Settings
from etk.platform.tasks import TaskOutcomeStatus


class Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class Catalog:
    def __init__(self, items: Sequence[Mapping[str, Any]]) -> None:
        self.items = tuple(dict(item) for item in items)

    def list_items(self, library_ids: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        assert tuple(library_ids) == ("library-1",)
        return self.items


class Metadata:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def details(self, tmdb_id: str, media_type: str) -> Mapping[str, Any]:
        self.calls.append((tmdb_id, media_type))
        return {
            "id": int(tmdb_id),
            "title": f"Title {tmdb_id}",
            "overview": "fixture overview",
            "release_date": "2024-01-02",
            "genres": [{"id": 1, "name": "Drama"}],
            "credits": {"cast": [{"id": 7, "name": "Actor", "character": "Lead"}]},
        }


class MetadataRepository:
    def __init__(self) -> None:
        self.upserts: list[tuple[Mapping[str, Any], Mapping[str, Any], bool]] = []
        self.role_rows: Sequence[Mapping[str, Any]] = ()
        self.roles: list[tuple[str, str, Sequence[Mapping[str, Any]]]] = []
        self.offline_ids: list[str] = []

    def list_backfill_candidates(self) -> Sequence[Mapping[str, Any]]:
        return ({"tmdb_id": "8", "item_type": "Movie", "emby_item_ids_json": '["emby-8"]'},)

    def upsert_metadata(
        self,
        item: Mapping[str, Any],
        details: Mapping[str, Any],
        *,
        preserve_people: bool,
    ) -> bool:
        self.upserts.append((dict(item), dict(details), preserve_people))
        return True

    def list_role_candidates(self) -> Sequence[Mapping[str, Any]]:
        return tuple(self.role_rows)

    def update_roles(self, tmdb_id: str, item_type: str, actors: Sequence[Mapping[str, Any]]) -> bool:
        self.roles.append((tmdb_id, item_type, tuple(dict(actor) for actor in actors)))
        return True

    def mark_offline(self, emby_ids: Sequence[str]) -> int:
        self.offline_ids = list(emby_ids)
        return 1


class Translation:
    def translate(self, values: Sequence[str]) -> Mapping[str, str]:
        return {value: f"译文-{value}" for value in values}


def test_populate_and_targeted_backfill_are_processor_free() -> None:
    settings = Settings(libraries_to_process=["library-1"])
    catalog = Catalog(
        [
            {"Id": "emby-1", "Type": "Movie", "Name": "One", "ProviderIds": {"Tmdb": "1"}},
            {"Id": "emby-series", "Type": "Series", "Name": "Series", "ProviderIds": {"Tmdb": "2"}},
            {"Id": "ignored", "Type": "Episode", "ProviderIds": {}},
        ]
    )
    metadata = Metadata()
    repository = MetadataRepository()
    service = NativeMediaMetadataTaskService(repository, catalog, metadata, settings=lambda: settings)

    populate = service.execute("populate-metadata", Progress(), {})
    assert populate.status is TaskOutcomeStatus.SUCCEEDED
    assert populate.details == {"updated": 2, "offline": 1, "failed": 0, "scanned": 2}
    assert metadata.calls == [("1", "movie"), ("2", "tv")]

    backfill = service.execute(
        "backfill-media-metadata",
        Progress(),
        {"targets": [{"tmdb_id": "8", "media_type": "movie"}]},
    )
    assert backfill.status is TaskOutcomeStatus.SUCCEEDED
    assert repository.upserts[-1][0]["Type"] == "Movie"
    assert repository.upserts[-1][2] is True


def test_role_translation_reports_updates_and_checkpoints() -> None:
    settings = Settings(ai_translate_actor_role=True)
    repository = MetadataRepository()
    repository.role_rows = (
        {"tmdb_id": "10", "item_type": "Movie", "actors_json": [{"name": "Actor", "character": "Lead"}]},
    )
    progress = Progress()
    service = NativeMediaMetadataTaskService(
        repository,
        Catalog(()),
        Metadata(),
        Translation(),
        settings=lambda: settings,
    )

    result = service.execute("role-translation", progress, {})

    assert result.status is TaskOutcomeStatus.SUCCEEDED
    assert result.details["updated"] == 1
    assert repository.roles == [("10", "Movie", ({"name": "Actor", "character": "译文-Lead"},))]
    assert progress.checkpoints == 1


class ScreenshotRepository:
    def __init__(self) -> None:
        self.items = ({"tmdb_id": "10", "item_type": "Episode", "emby_item_id": "emby-10", "path": "/video.strm"},)
        self.records: list[tuple[Mapping[str, Any], str]] = []

    def list_candidates(self) -> Sequence[Mapping[str, Any]]:
        return self.items

    def record_screenshot(self, item: Mapping[str, Any], *, content_hash: str) -> bool:
        self.records.append((item, content_hash))
        return True


class Capture:
    def capture(self, path: str) -> bytes | None:
        assert path == "/video.strm"
        return b"fixture-jpeg"


class Upload:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes]] = []
        self.readbacks = 0

    def upload(self, item_id: str, content: bytes) -> bool:
        self.uploads.append((item_id, content))
        return True

    def readback(self, item_id: str) -> bool:
        assert item_id == "emby-10"
        self.readbacks += 1
        return self.readbacks > 1


def test_screenshot_backfill_requires_upload_readback_before_local_commit() -> None:
    repository = ScreenshotRepository()
    upload = Upload()
    progress = Progress()
    service = NativeMediaScreenshotBackfillService(repository, Capture(), upload)

    result = service.execute(progress)

    assert result.status is TaskOutcomeStatus.SUCCEEDED
    assert result.details["uploaded"] == 1
    assert len(upload.uploads) == 1
    assert repository.records[0][1] == "e24d2697f7e8076d90b33db0018f3fbc1cee3a50041b95bceb51a810e6b5a76c"


def test_screenshot_backfill_does_not_resend_when_readback_already_confirms() -> None:
    repository = ScreenshotRepository()

    class AlreadyUploaded(Upload):
        def readback(self, item_id: str) -> bool:
            assert item_id == "emby-10"
            return True

    upload = AlreadyUploaded()
    result = NativeMediaScreenshotBackfillService(repository, Capture(), upload).execute(Progress())

    assert result.status is TaskOutcomeStatus.SUCCEEDED
    assert upload.uploads == []
    assert len(repository.records) == 1
