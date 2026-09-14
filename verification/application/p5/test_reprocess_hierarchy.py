from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from etk.domains.media.models import MediaIdentity, MediaItemType, MetadataAggregate
from etk.domains.media.policies import MediaPresentationPolicy, format_metadata_payload
from etk.domains.media.reprocess import NativeMediaItemReprocessService
from etk.domains.media.reprocess_metadata import identify_root, prepare_metadata
from etk.domains.media.task_execution import (
    NativeMediaMetadataTaskService,
    _actors_json,
    _directors_json,
    _metadata_values,
)
from etk.platform.tasks import TaskOutcome


class Provider:
    def __init__(self):
        self.calls = []
        self.results = [{"id": 200, "name": "测试剧", "first_air_date": "2021-01-01"}]

    def search(self, name, kind):
        return {"results": self.results, "total_pages": 1}

    def season_details(self, tmdb_id, season):
        self.calls.append((tmdb_id, season))
        return {
            "id": 202,
            "season_number": season,
            "name": "第2季",
            "episodes": [
                {
                    "id": 203,
                    "season_number": season,
                    "episode_number": 1,
                    "name": "分集正式标题",
                    "overview": "分集简介",
                    "air_date": "2023-07-10",
                    "still_path": "/still.jpg",
                },
            ],
        }


def test_identification_requires_unique_exact_title_and_year():
    provider = Provider()
    item = {"Type": "Series", "Name": "测试剧 (2021)", "ProviderIds": {"Imdb": "tt1"}}
    resolved = identify_root(item, provider)
    assert resolved["ProviderIds"] == {"Imdb": "tt1", "Tmdb": "200"}
    assert "Tmdb" not in item["ProviderIds"]
    provider.results.append({"id": 201, "name": "测试剧", "first_air_date": "2021-05-01"})
    with pytest.raises(ValueError, match="人工确认"):
        identify_root(item, provider)
    with pytest.raises(ValueError, match="明确片名和年份"):
        identify_root({**item, "Name": "测试剧"}, provider)
    provider.results = [{"id": 201, "name": "其他作品", "first_air_date": "2021-05-01"}]
    with pytest.raises(ValueError, match="人工确认"):
        identify_root(item, provider)


def test_season_and_episode_use_distinct_ids_numbers_and_one_season_query():
    provider = Provider()
    root = {"Type": "Series", "ProviderIds": {"Tmdb": "200"}}
    cache = {}
    season, data = prepare_metadata({"Type": "Season", "IndexNumber": 2}, root, provider, cache)
    assert data["id"] == 202 and season["_season_number"] == 2
    episode, data = prepare_metadata(
        {"Type": "Episode", "ParentIndexNumber": 2, "IndexNumber": 1}, root, provider, cache
    )
    assert data["id"] == 203 and data["name"] == "分集正式标题"
    assert episode["_series_tmdb_id"] == "200" and episode["_season_number"] == 2 and episode["_episode_number"] == 1
    assert provider.calls == [("200", 2)]
    with pytest.raises(ValueError, match="S02E00"):
        prepare_metadata({"Type": "Episode", "ParentIndexNumber": 2, "IndexNumber": 0}, root, provider, cache)
    with pytest.raises(ValueError, match="集号"):
        prepare_metadata({"Type": "Episode", "ParentIndexNumber": 2}, root, provider, cache)


class Progress:
    def __init__(self):
        self.messages = []

    def checkpoint(self):
        pass

    def update(self, _, message):
        self.messages.append(message)


class Catalog:
    def __init__(self):
        self.items = {
            "root": {"Id": "root", "Type": "Series", "ProviderIds": {"Tmdb": "200"}},
            "season": {"Id": "season", "Type": "Season", "SeriesId": "root", "IndexNumber": 2},
            "episode": {
                "Id": "episode",
                "Type": "Episode",
                "SeriesId": "root",
                "ParentIndexNumber": 2,
                "IndexNumber": 1,
            },
            "special": {
                "Id": "special",
                "Type": "Episode",
                "SeriesId": "root",
                "ParentIndexNumber": 2,
                "IndexNumber": 0,
            },
        }

    def item(self, item_id):
        return self.items[item_id]

    def descendants(self, item_id):
        keys = ["season", "episode", "special"] if item_id == "root" else ["episode", "special"]
        return [self.items[key] for key in keys]


class Metadata:
    def __init__(self):
        self.saved = []

    def identify_reprocess_root(self, item):
        return item

    def prepare_reprocess_metadata(self, item, root, cache):
        if item["Id"] == "special":
            raise ValueError("TMDb 未找到 S02E00")
        self.saved.append(item["Id"])
        return {"Name": "已刮削" + item["Id"], "Overview": "简介"}


class MediaInfo:
    def execute(self, *_args, **_kwargs):
        return TaskOutcome.skipped()


@pytest.mark.parametrize("complete", [True, False])
def test_only_missing_reuses_cache_and_skips_complete_emby_items(complete):
    catalog = Catalog()
    catalog.items["episode"].update(Name="手动标题", ProviderIds={"Tmdb": "203"})
    if complete:
        catalog.items["episode"]["Overview"] = "手动简介"
    metadata = Metadata()
    metadata.cached_reprocess_metadata = lambda *_: {"Name": "缓存标题", "Overview": "缓存简介", "_tmdb_id": "203"}
    refreshed = []

    def refresh(item, _):
        refreshed.append(item["Id"])
        catalog.items[item["Id"]].update(Name="缓存标题", Overview="缓存简介")
        return TaskOutcome.succeeded()

    result = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh).execute(
        "episode",
        Progress(),
        operation_key="fill",
        only_missing=True,
    )
    assert result.status.value == "succeeded"
    assert metadata.saved == []
    assert refreshed == ([] if complete else ["episode"])
    assert result.details["metadata"]["cache_reused"] == 1
    assert result.details["metadata"]["skipped"] == int(complete)
    if complete:
        assert catalog.items["episode"]["Name"] == "手动标题"


def test_only_missing_scrapes_only_missing_items_in_selected_season():
    catalog = Catalog()
    del catalog.items["special"]
    catalog.descendants = lambda _: [catalog.items["episode"]]
    catalog.items["season"].update(Name="季", Overview="简介", ProviderIds={"Tmdb": "202"})
    metadata = Metadata()
    metadata.cached_reprocess_metadata = lambda item, _: (
        {"Name": "季", "Overview": "简介", "_tmdb_id": "202"} if item["Id"] == "season" else None
    )
    refreshed = []

    def refresh(item, _):
        refreshed.append(item["Id"])
        catalog.items[item["Id"]].update(Name="已刮削episode", Overview="简介")
        return TaskOutcome.succeeded()

    result = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh).execute(
        "season",
        Progress(),
        operation_key="fill",
        only_missing=True,
    )
    assert result.status.value == "succeeded"
    assert metadata.saved == refreshed == ["episode"]
    assert result.details["metadata"]["skipped"] == 1


@pytest.mark.parametrize("kind", ["Movie", "Series", "Season", "Episode"])
def test_whole_item_lock_allows_scraping_without_unlocking_or_expanding_scope(kind):
    catalog = Catalog()
    selected = {"Movie": "root", "Series": "root", "Season": "season", "Episode": "episode"}[kind]
    catalog.items["root"]["Type"] = "Movie" if kind == "Movie" else "Series"
    for item in catalog.items.values():
        item.update(LockData=True, LockedFields=[], Path=f"/fixture/{item['Id']}")
    before = deepcopy(catalog.items)
    saved = []

    class LockedProvider(Provider):
        def details(self, tmdb_id, media_type):
            return {"id": int(tmdb_id), "name": "正式标题", "overview": "正式简介"}

    def save(item, details, **kwargs):
        saved.append((deepcopy(item), deepcopy(details)))
        return True

    metadata = NativeMediaMetadataTaskService(SimpleNamespace(upsert_metadata=save), catalog, LockedProvider())
    refreshed = []

    def refresh(item, _):
        prepared, details = next((target, data) for target, data in saved if target["Id"] == item["Id"])
        assert prepared["LockData"] is True and prepared["LockedFields"] == []
        assert item["LockData"] is True and item["LockedFields"] == []
        refreshed.append(item["Id"])
        catalog.items[item["Id"]].update(
            Name=details["name"],
            Overview=details.get("overview", ""),
            ProviderIds={"Tmdb": str(details["id"])},
            ImageTags={"Primary": "fixture-image"},
        )
        return TaskOutcome.succeeded()

    result = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh).execute(
        selected, Progress(), operation_key="locked-item", restore_media_info=False
    )
    expected = {
        "Movie": ["root"],
        "Series": ["root", "season", "episode"],
        "Season": ["season", "episode"],
        "Episode": ["episode"],
    }[kind]
    assert refreshed == expected
    assert result.details["metadata"]["verified"] == len(expected)
    # 特别篇仍因 TMDb 无对应集号失败，不因允许整项锁而放宽身份校验。
    assert result.status.value == ("partial" if kind in {"Series", "Season"} else "succeeded")
    for key, item in catalog.items.items():
        assert item["LockData"] is True and item["LockedFields"] == []
        assert item["Path"] == before[key]["Path"]
        if key not in expected:
            assert item == before[key]


@pytest.mark.parametrize("field", ["Name", "Overview"])
def test_explicit_metadata_field_lock_still_blocks_reprocess(field):
    catalog = Catalog()
    item = {**catalog.items["episode"], "Path": "/fixture/episode", "LockData": True, "LockedFields": [field]}
    provider = Provider()
    metadata = NativeMediaMetadataTaskService(SimpleNamespace(), catalog, provider)
    with pytest.raises(ValueError, match="锁定"):
        metadata.prepare_reprocess_metadata(item, catalog.items["root"], {})
    assert provider.calls == []
    assert item["LockData"] is True and item["LockedFields"] == [field]


@pytest.mark.parametrize(
    "selected,expected",
    [("root", ["root", "season", "episode"]), ("season", ["season", "episode"]), ("episode", ["episode"])],
)
def test_scope_is_exact_and_partial_is_not_success(selected, expected):
    catalog = Catalog()
    metadata = Metadata()
    refreshed = []

    def refresh(item, key):
        assert metadata.saved == expected
        refreshed.append(item["Id"])
        catalog.items[item["Id"]].update(Name="已刮削" + item["Id"], Overview="简介")
        return TaskOutcome.succeeded()

    before = deepcopy(catalog.items["root"])
    service = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh)
    progress = Progress()
    result = service.execute(selected, progress, operation_key="test")
    assert refreshed == expected
    assert result.details["metadata"]["verified"] == len(expected)
    assert result.status.value == ("succeeded" if selected == "episode" else "partial")
    if selected != "root":
        assert catalog.items["root"] == before
    assert any("回读已确认" in message for message in progress.messages)


def test_accepted_refresh_without_matching_readback_is_not_success(monkeypatch):
    monkeypatch.setattr("etk.domains.media.reprocess.time.sleep", lambda _: None)
    service = NativeMediaItemReprocessService(
        Catalog(), Metadata(), MediaInfo(), refresh=lambda *_: TaskOutcome.succeeded()
    )
    result = service.execute("episode", Progress(), operation_key="test")
    assert result.status.value != "succeeded"
    assert result.details["metadata"]["verified"] == 0
    assert result.details["metadata"]["failures"][0]["stage"] == "回读校验"


def test_new_episode_with_unidentified_series_also_prepares_parents_not_siblings(monkeypatch):
    catalog = Catalog()
    catalog.items["root"]["ProviderIds"] = {}
    catalog.items["episode"]["SeasonId"] = "season"
    metadata = Metadata()
    monkeypatch.setattr(metadata, "identify_reprocess_root", lambda item: {**item, "ProviderIds": {"Tmdb": "200"}})
    refreshed = []

    def refresh(item, _):
        assert metadata.saved == ["root", "season", "episode"]
        refreshed.append(item["Id"])
        catalog.items[item["Id"]].update(Name="已刮削" + item["Id"], Overview="简介")
        return TaskOutcome.succeeded()

    result = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh).execute(
        "episode", Progress(), operation_key="new-episode"
    )
    assert result.status.value == "succeeded"
    assert refreshed == ["root", "season", "episode"]
    assert "Name" not in catalog.items["special"]


def test_episode_cast_combines_regulars_and_guests_without_duplicates():
    class CastProvider(Provider):
        def season_details(self, tmdb_id, season):
            data = super().season_details(tmdb_id, season)
            data["credits"] = {"cast": [{"id": 1, "name": "主演", "profile_path": "/main.jpg"}]}
            data["episodes"][0]["guest_stars"] = [
                {"id": 1, "name": "主演"},
                {"id": 2, "name": "客串", "profile_path": "/guest.jpg"},
            ]
            return data

    _, data = prepare_metadata(
        {"Type": "Episode", "ParentIndexNumber": 2, "IndexNumber": 1},
        {"ProviderIds": {"Tmdb": "200"}},
        CastProvider(),
        {},
    )
    assert [actor["id"] for actor in data["credits"]["cast"]] == [1, 2]
    assert data["credits"]["cast"][0]["profile_path"] == "/main.jpg"


def test_aggregate_cast_keeps_roles_and_portraits_and_directors():
    data = {
        "aggregate_credits": {
            "cast": [
                {
                    "id": 1,
                    "name": "演员",
                    "profile_path": "/a.jpg",
                    "order": 0,
                    "roles": [{"character": "主角"}, {"character": "主角"}, {"character": "少年主角"}],
                }
            ],
            "crew": [{"id": 2, "name": "导演", "profile_path": "/d.jpg", "jobs": [{"job": "Director"}]}],
        }
    }
    assert _actors_json(data["aggregate_credits"])[0] == {
        "tmdb_id": 1,
        "name": "演员",
        "profile_path": "/a.jpg",
        "order": 0,
        "character": "主角 / 少年主角",
    }
    assert _directors_json(data) == [{"tmdb_id": 2, "name": "导演", "profile_path": "/d.jpg"}]


def test_ratings_keywords_imdb_and_runtime_are_not_discarded():
    import json

    data = {
        "id": 200,
        "name": "测试剧",
        "first_air_date": "2021-01-01",
        "external_ids": {"imdb_id": "tt200"},
        "episode_run_time": [24],
        "keywords": {"results": [{"id": 1, "name": "关键词"}]},
        "content_ratings": {"results": [{"iso_3166_1": "US", "rating": "TV-MA"}]},
    }
    values = _metadata_values({}, data, "200", "Series", [])
    assert values["imdb_id"] == "tt200" and values["runtime_minutes"] == 24
    assert json.loads(values["official_rating_json"]) == {"US": "TV-MA"}
    assert json.loads(values["keywords_json"]) == [{"id": 1, "name": "关键词"}]


def test_legacy_episode_thumb_is_also_available_as_primary_without_rewriting_cache():
    row = {"thumb_path": "/episode.jpg", "poster_path": None}
    payload = format_metadata_payload(
        MetadataAggregate(MediaIdentity("200", "tv", MediaItemType.EPISODE, 3, 9), MediaItemType.EPISODE, row),
        MediaPresentationPolicy(),
    )
    assert payload["images"]["primary"].endswith("/episode.jpg")
    assert payload["images"]["thumb"].endswith("/episode.jpg")
    assert row["poster_path"] is None


@pytest.mark.parametrize("missing", ["_actor_count", "_actor_images", "_primary_image", "OfficialRating"])
def test_missing_cast_portraits_rating_or_primary_is_not_success(monkeypatch, missing):
    monkeypatch.setattr("etk.domains.media.reprocess.time.sleep", lambda _: None)
    catalog = Catalog()
    metadata = Metadata()
    prepare = metadata.prepare_reprocess_metadata

    def with_expected(item, root, cache):
        return {
            **prepare(item, root, cache),
            missing: "TV-MA" if missing == "OfficialRating" else True if missing == "_primary_image" else 1,
        }

    monkeypatch.setattr(metadata, "prepare_reprocess_metadata", with_expected)

    def refresh(item, _):
        catalog.items[item["Id"]].update(Name="已刮削" + item["Id"], Overview="简介")
        return TaskOutcome.succeeded()

    result = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh).execute(
        "episode", Progress(), operation_key="test"
    )
    assert result.status.value != "succeeded"
    assert result.details["metadata"]["verified"] == 0
    assert result.details["metadata"]["failures"][0]["stage"] == "回读校验"
