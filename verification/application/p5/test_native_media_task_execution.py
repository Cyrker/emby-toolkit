from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.media.task_execution import (
    HttpxMediaCatalogAdapter,
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


@pytest.mark.parametrize("entrypoint", ["populate", "reprocess", "prepare"])
def test_scraping_translates_people_before_persisting_without_mutating_provider(entrypoint):
    from copy import deepcopy

    settings = Settings(ai_translate_actor_role=True, libraries_to_process=["library-1"], max_actors_to_process=2)
    item = {"Id": "1", "Type": "Series", "ProviderIds": {"Tmdb": "1"}, "Path": "/fixture/show"}
    details = {
        "id": 1, "name": "剧名", "overview": "简介",
        "aggregate_credits": {
            "cast": [
                {"id": 1, "name": "Actor", "order": 0, "roles": [{"character": "Doctor"}]},
                {"id": 2, "name": "已有中文", "order": 1, "roles": [{"character": "已有角色"}]},
                {"id": 3, "name": "Outside limit", "order": 2},
            ],
            "crew": [
                {"id": 4, "name": "Mike Listo", "jobs": [{"job": "Director"}]},
                {"id": 5, "name": "Writer", "jobs": [{"job": "Writer"}]},
            ],
        },
    }
    original = deepcopy(details)
    calls = []

    class Provider:
        def details(self, *_):
            return details

    class Translator:
        def translate(self, values):
            calls.append(tuple(values))
            return {v: f"译文-{v}" for v in values}

    repository = MetadataRepository()
    service = NativeMediaMetadataTaskService(
        repository, Catalog([item]), Provider(), Translator(), settings=lambda: settings,
    )
    if entrypoint == "populate":
        service.execute("populate-metadata", Progress(), {})
    elif entrypoint == "reprocess":
        service.reprocess_item_metadata(item, Progress())
    else:
        service.prepare_reprocess_metadata(item, item, {})
    saved = repository.upserts[0][1]["aggregate_credits"]
    assert saved["cast"][0]["name"] == "译文-Actor"
    assert saved["cast"][0]["roles"][0]["character"] == "译文-Doctor"
    assert saved["crew"][0]["name"] == "译文-Mike Listo"
    assert saved["cast"][1]["name"] == "已有中文"
    assert saved["cast"][2]["name"] == "Outside limit"
    assert saved["crew"][1]["name"] == "Writer"
    assert set(calls[0]) == {"Actor", "Doctor", "Mike Listo"}
    assert details == original


def test_people_translation_disabled_and_incomplete_response_do_not_write_wrong_names():
    item = {"Id": "1", "Type": "Movie", "ProviderIds": {"Tmdb": "1"}, "Path": "/fixture/movie.mkv"}
    repo = MetadataRepository()
    settings = Settings(ai_translate_actor_role=False)

    class Incomplete:
        def translate(self, _):
            return {"Actor": "Actor"}

    service = NativeMediaMetadataTaskService(repo, Catalog(()), Metadata(), Incomplete(), settings=lambda: settings)
    service.reprocess_item_metadata(item, Progress())
    assert repo.upserts[0][1]["credits"]["cast"][0]["name"] == "Actor"
    repo.upserts.clear()
    settings = Settings(ai_translate_actor_role=True)
    with pytest.raises(RuntimeError, match="翻译不完整"):
        service.reprocess_item_metadata(item, Progress())
    assert not repo.upserts


def test_episode_limit_and_cached_english_people_are_not_treated_as_complete():
    from types import SimpleNamespace

    calls = []
    settings = Settings(ai_translate_actor_role=True, max_actors_to_process=2, max_episode_actors_to_process=1)
    cached = {
        "tmdb_id": "3", "title": "第三集", "overview": "剧情", "release_year": 2026,
        "actors_json": [{"name": "Actor", "character": "Doctor"}],
    }
    repo = SimpleNamespace(reprocess_cache=lambda *_: cached)
    translator = SimpleNamespace(translate=lambda values: calls.append(values) or {v: "中文" for v in values})
    service = NativeMediaMetadataTaskService(repo, None, None, translator, settings=lambda: settings)
    item = {"Type": "Episode"}
    assert service.cached_reprocess_metadata(item, {}) is None
    result = service._translate_people(item, {"credits": {"cast": [
        {"name": "Second", "order": 1}, {"name": "First", "order": 0},
    ]}})
    assert calls == [("First",)]
    assert result["credits"]["cast"][0]["name"] == "Second"


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


def _http_catalog(response: httpx.Response, *, settings: Settings | None = None) -> HttpxMediaCatalogAdapter:
    def respond(_request: httpx.Request) -> httpx.Response:
        return response

    return HttpxMediaCatalogAdapter(
        lambda: (
            settings
            or Settings(
                emby_server_url="http://emby.fixture.invalid",
                emby_user_id="fixture-user",
                emby_api_key=SecretStr("fixture-token"),
            )
        ),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(respond)),
    )


def test_http_media_catalog_item_validates_identity_before_request() -> None:
    catalog = _http_catalog(httpx.Response(200, json={"Id": "item-1"}))
    assert catalog.item("") is None

    with pytest.raises(RuntimeError, match="configured Emby authorization"):
        _http_catalog(httpx.Response(200, json={}), settings=Settings()).item("item-1")


def test_http_media_catalog_item_handles_absent_valid_and_invalid_readback() -> None:
    assert _http_catalog(httpx.Response(404)).item("item-1") is None
    assert _http_catalog(httpx.Response(200, json={"Id": "item-1"})).item("item-1") == {"Id": "item-1"}
    with pytest.raises(RuntimeError, match="invalid object"):
        _http_catalog(httpx.Response(200, json=[])).item("item-1")


def test_http_media_catalog_requests_library_addition_dates_for_inventory_and_items() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        item = {"Id": "item-1", "DateCreated": "2026-10-17T12:14:52Z"}
        payload = {"Items": [item], "TotalRecordCount": 1} if request.url.path.endswith("/Items") else item
        return httpx.Response(200, json=payload)

    catalog = HttpxMediaCatalogAdapter(
        lambda: Settings(
            emby_server_url="http://emby.fixture.invalid",
            emby_user_id="fixture-user",
            emby_api_key=SecretStr("fixture-token"),
        ),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(respond)),
    )
    assert catalog.list_items(["library-1"])[0]["DateCreated"] == "2026-10-17T12:14:52Z"
    actual = catalog.item("item-1")
    assert actual is not None and actual["DateCreated"] == "2026-10-17T12:14:52Z"
    assert len(requests) == 2
    assert all("DateCreated" in request.url.params["Fields"].split(",") for request in requests)
