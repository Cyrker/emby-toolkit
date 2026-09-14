from __future__ import annotations

from unittest.mock import Mock

import httpx
from pydantic import SecretStr
from sqlalchemy import text

from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.media.final_refresh_effects import (
    EmbyMetadataRefreshEffectDelivery,
    PersistentReprocessMetadataRefresh,
)
from etk.domains.media.library_notifications import (
    PersistentLibraryResultNotification,
    TelegramLibraryNotificationDelivery,
)
from etk.domains.media.models import MediaIdentity, MediaItemType
from etk.domains.media.policies import MediaPresentationPolicy, format_metadata_payload
from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.domains.media.reprocess import NativeMediaItemReprocessService
from etk.domains.media.task_execution import (
    NativeMediaMetadataTaskService,
    SqlAlchemyMediaMetadataTaskRepository,
    SqlAlchemyScreenshotRepository,
)
from etk.integrations.http_policy import ProviderDisposition, ProviderResponse
from etk.platform import Settings
from etk.platform.tasks import TaskOutcome


def test_pg18_metadata_task_repository_preserves_file_projection_and_updates_roles(p5_runtime) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, overview, in_library,
                    emby_item_ids_json, file_sha1_json, file_pickcode_json, actors_json,
                    metadata_schema_version
                ) VALUES (
                    '100', 'Movie', 'Old title', 'Old overview', TRUE,
                    '[\"emby-100\"]'::jsonb, '[\"sha1\"]'::jsonb, '[\"pick\"]'::jsonb,
                    '[{\"name\":\"Existing\",\"character\":\"Lead\"}]'::jsonb, 0
                )
                """
            )
        )
    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)

    assert repository.list_backfill_candidates()[0]["tmdb_id"] == "100"
    assert repository.upsert_metadata(
        {"Id": "emby-100", "Type": "Movie", "ProviderIds": {"Tmdb": "100"}},
        {
            "id": 100,
            "title": "New title",
            "overview": "New overview",
            "release_date": "2024-01-01",
            "genres": [],
        },
        preserve_people=True,
    )
    rows = repository.list_role_candidates()
    assert rows and rows[0]["actors_json"] == [{"name": "Existing", "character": "Lead"}]
    assert repository.update_roles(
        "100",
        "Movie",
        [{"name": "Existing", "character": "主角"}],
    )
    with p5_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT file_sha1_json, file_pickcode_json, actors_json, title
                FROM media_metadata
                WHERE tmdb_id = '100' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
    assert row["file_sha1_json"] == ["sha1"]
    assert row["file_pickcode_json"] == ["pick"]
    assert row["actors_json"] == [{"name": "Existing", "character": "主角"}]
    assert row["title"] == "New title"


def test_reprocess_cache_requires_complete_metadata_and_matching_episode_identity(p5_runtime):
    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    item = {
        "Id": "episode-cache",
        "Type": "Episode",
        "Path": "/fixture/episode.mkv",
        "SeriesId": "series",
        "ParentIndexNumber": 2,
        "IndexNumber": 3,
        "_series_tmdb_id": "200",
        "_season_number": 2,
        "_episode_number": 3,
    }
    root = {"Type": "Series", "ProviderIds": {"Tmdb": "200"}}
    details = {
        "id": 203,
        "name": "第三集",
        "overview": "简介",
        "air_date": "2026-09-01",
        "credits": {"cast": [{"id": 1, "name": "演员", "profile_path": "/actor.jpg"}]},
    }
    assert repository.reprocess_cache(item, root) is None
    repository.upsert_metadata(item, details, preserve_people=False)
    cached = repository.reprocess_cache(item, root)
    assert cached is not None and cached["title"] == "第三集"
    service = NativeMediaMetadataTaskService(repository, None, None)
    expected = service.cached_reprocess_metadata(item, root)
    assert expected["ProductionYear"] == 2026 and expected["_actor_images"] == 1
    assert repository.reprocess_cache({**item, "IndexNumber": 4}, root) is None
    assert repository.reprocess_cache({**item, "Path": "/other/path.mkv"}, root) is None
    assert repository.reprocess_cache(item, {**root, "ProviderIds": {"Tmdb": "999"}}) is None
    assert repository.reprocess_cache({**item, "ProviderIds": {"Tmdb": "999"}}, root) is None
    repository.upsert_metadata(item, {**details, "overview": ""}, preserve_people=False)
    assert repository.reprocess_cache(item, root) is None


def test_library_result_notification_is_persistent_and_not_resent(p5_runtime):
    settings = Settings(telegram_bot_token="fixture-token", telegram_notify_types=["library_new"])
    bot = Mock()
    bot.send_message.return_value = ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True})
    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        TelegramLibraryNotificationDelivery(settings=lambda: settings, telegram=bot, admin_chat_ids=lambda: ["1"]),
        worker_id="notification-test",
    )
    notification = PersistentLibraryResultNotification(executor, lambda _: {"Name": "测试媒体", "Type": "Movie"})
    result = TaskOutcome.succeeded("完成", {"metadata": {"verified": 1, "failed": 0}})
    assert notification("1", result, "job-1")["status"] == "confirmed"
    assert notification("1", result, "job-1")["status"] == "confirmed"
    assert bot.send_message.call_count == 1


def test_people_without_person_cache_and_episode_primary_reach_bridge(p5_runtime):
    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    item = {
        "Id": "episode-9",
        "Type": "Episode",
        "Path": "/media/series/s3/e9.mkv",
        "_series_tmdb_id": "200",
        "_season_number": 3,
        "_episode_number": 9,
    }
    details = {
        "id": 209,
        "name": "分集九",
        "air_date": "2026-08-24",
        "still_path": "/still9.jpg",
        "credits": {
            "cast": [{"id": 1, "name": "演员", "profile_path": "/actor.jpg", "character": "角色"}],
            "crew": [{"id": 2, "name": "导演", "profile_path": "/director.jpg", "job": "Director"}],
        },
    }
    assert repository.upsert_metadata(item, details, preserve_people=False)
    bridge = SqlAlchemyMediaRepository(p5_runtime.unit_of_work)
    identity = MediaIdentity("200", "tv", MediaItemType.EPISODE, 3, 9)
    aggregate = bridge.load_metadata(identity, MediaItemType.EPISODE)
    payload = format_metadata_payload(aggregate, MediaPresentationPolicy())
    assert payload["images"]["primary"].endswith("/still9.jpg")
    assert [(p["name"], p["type"]) for p in payload["people"]] == [("演员", "Actor"), ("导演", "Director")]
    assert all(p["image_url"] for p in payload["people"]) and payload["actors_ready"]
    with p5_runtime.engine.begin() as connection:
        assert connection.scalar(text("SELECT count(*) FROM person_metadata")) == 0
        connection.execute(text("INSERT INTO person_metadata(tmdb_person_id,primary_name) VALUES (1,'人工校正姓名')"))
    aggregate = bridge.load_metadata(identity, MediaItemType.EPISODE)
    assert aggregate.actors[0]["name"] == "人工校正姓名"
    assert aggregate.actors[0]["profile_path"] == "/actor.jpg"


def test_reprocess_only_fills_missing_episode_images_without_replacing_existing(p5_runtime):
    settings = Settings(
        emby_server_url="http://emby.fixture.invalid", emby_user_id="user", emby_api_key=SecretStr("fixture-token")
    )
    calls = []

    def respond(request):
        calls.append(dict(request.url.params))
        return httpx.Response(204)

    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        EmbyMetadataRefreshEffectDelivery(
            lambda: settings, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(respond))
        ),
    )
    refresh = PersistentReprocessMetadataRefresh(executor, lambda: settings)
    refresh({"Id": "9", "Type": "Episode", "ImageTags": {"Thumb": "old"}}, "missing")
    refresh({"Id": "8", "Type": "Episode", "ImageTags": {"Primary": "manual"}}, "existing")
    assert [call["ImageRefreshMode"] for call in calls] == ["FullRefresh", "ValidationOnly"]
    assert all(call["ReplaceAllImages"] == "false" and call["Recursive"] == "false" for call in calls)


def test_pg18_screenshot_repository_reads_asset_paths_and_records_hash(p5_runtime) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json, asset_details_json
                ) VALUES (
                    '200', 'Episode', TRUE, '[\"emby-200\"]'::jsonb,
                    '[{\"path\":\"/media/episode.strm\"}]'::jsonb
                )
                """
            )
        )
    repository = SqlAlchemyScreenshotRepository(p5_runtime.unit_of_work)

    candidates = repository.list_candidates()
    assert candidates == (
        {
            "tmdb_id": "200",
            "item_type": "Episode",
            "emby_item_id": "emby-200",
            "path": "/media/episode.strm",
        },
    )
    assert repository.record_screenshot(candidates[0], content_hash="a" * 64)
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT screenshot_hash FROM media_metadata WHERE tmdb_id = '200'")) == "a" * 64


def test_pg18_metadata_offline_reconcile_is_scoped_to_selected_libraries(p5_runtime) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, emby_item_ids_json, asset_details_json
                ) VALUES
                    ('301', 'Movie', 'Library One', TRUE, '["emby-301"]'::jsonb,
                     '[{"emby_item_id":"emby-301","source_library_id":"library-1"}]'::jsonb),
                    ('302', 'Movie', 'Library Two', TRUE, '["emby-302"]'::jsonb,
                     '[{"emby_item_id":"emby-302","source_library_id":"library-2"}]'::jsonb)
                """
            )
        )

    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    assert repository.mark_offline([], library_ids=["library-1"]) == 0
    assert repository.mark_offline(["emby-seen"], library_ids=["library-1"]) == 1
    with p5_runtime.engine.connect() as connection:
        states = connection.execute(
            text(
                """
                SELECT tmdb_id, in_library
                FROM media_metadata
                WHERE tmdb_id IN ('301', '302')
                ORDER BY tmdb_id
                """
            )
        ).all()
    assert states == [("301", False), ("302", True)]


def test_old_series_path_binding_and_targeted_refresh_are_replay_safe(p5_runtime) -> None:
    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    item = {
        "Id": "123",
        "Type": "Series",
        "Path": "/media/series",
        "ProviderIds": {"Tmdb": "200"},
        "_source_library_id": "library-1",
    }
    details = {"id": 200, "name": "Series", "overview": "Recovered overview"}
    repository.upsert_metadata({**item, "Path": "/media/other-copy"}, details, preserve_people=False)
    assert repository.is_stale(item)
    repository.upsert_metadata(item, details, preserve_people=False)
    repository.upsert_metadata(item, details, preserve_people=False)
    with p5_runtime.engine.connect() as connection:
        assets = connection.scalar(text("SELECT asset_details_json FROM media_metadata WHERE tmdb_id='200'"))
    assert len(assets) == 2
    assert assets[-1]["source_library_id"] == "library-1"
    bridge = SqlAlchemyMediaRepository(p5_runtime.unit_of_work)
    identity = bridge.resolve_by_path(item["Path"], MediaItemType.SERIES)
    assert identity is not None
    assert bridge.load_metadata(identity, MediaItemType.SERIES).row["overview"] == "Recovered overview"
    settings = Settings(
        emby_server_url="http://emby.fixture.invalid", emby_user_id="user", emby_api_key=SecretStr("fixture-token")
    )
    calls = []

    def deliver(request):
        calls.append(request)
        assert request.url.params["Recursive"] == "false"
        assert request.url.params["ReplaceAllImages"] == "false"
        return httpx.Response(204)

    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        EmbyMetadataRefreshEffectDelivery(
            lambda: settings, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(deliver))
        ),
    )
    refresh = PersistentReprocessMetadataRefresh(executor, lambda: settings)
    assert refresh(item, "op1").status.value == "succeeded"
    assert refresh(item, "op1").status.value == "succeeded"
    assert len(calls) == 1


def test_full_season_episode_metadata_and_readback_use_distinct_identity(p5_runtime):
    class Provider:
        def search(self, name, kind):
            return {"results": [{"id": 200, "name": "测试剧", "first_air_date": "2021-01-01"}]}

        def details(self, tmdb_id, kind):
            return {"id": 200, "name": "测试剧", "overview": "根简介"}

        def season_details(self, tmdb_id, season):
            return {
                "id": 202,
                "name": "第2季",
                "season_number": 2,
                "episodes": [
                    {
                        "id": 203,
                        "name": "失意的魔术师",
                        "overview": "分集简介",
                        "season_number": 2,
                        "episode_number": 1,
                        "air_date": "2023-07-10",
                        "still_path": "/still.jpg",
                        "runtime": 24,
                    },
                ],
            }

    class Catalog:
        def __init__(self):
            self.items = {
                "1": {"Id": "1", "Type": "Series", "Path": "/media/series", "Name": "测试剧", "ProductionYear": 2021},
                "2": {"Id": "2", "Type": "Season", "SeriesId": "1", "Path": "/media/series/s2", "IndexNumber": 2},
                "3": {
                    "Id": "3",
                    "Type": "Episode",
                    "SeriesId": "1",
                    "Path": "/media/series/s2/e1.mkv",
                    "ParentIndexNumber": 2,
                    "IndexNumber": 1,
                    "ProviderIds": {"Tmdb": "200"},
                },
            }

        def item(self, item_id):
            return self.items[item_id]

        def descendants(self, item_id):
            return [self.items["2"], self.items["3"]]

    class Progress:
        def checkpoint(self):
            pass

        def update(self, *_):
            pass

    class MediaInfo:
        def execute(self, *_, **kwargs):
            return TaskOutcome.skipped()

    catalog = Catalog()
    bridge = SqlAlchemyMediaRepository(p5_runtime.unit_of_work)
    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    metadata = NativeMediaMetadataTaskService(repository, catalog, Provider())

    def refresh(item, operation):
        kind = MediaItemType.parse(item["Type"])
        identity = bridge.resolve_by_path(
            item["Path"],
            kind,
            season_number=item.get("ParentIndexNumber") if item["Type"] == "Episode" else item.get("IndexNumber"),
            episode_number=item.get("IndexNumber") if item["Type"] == "Episode" else None,
        )
        payload = format_metadata_payload(bridge.load_metadata(identity, kind), MediaPresentationPolicy())
        catalog.items[item["Id"]].update(
            Name=payload["name"],
            Overview=payload["overview"],
            ProviderIds={"Tmdb": payload["tmdb_id"]},
            ImageTags={"Primary": "fixture-primary"} if payload["images"]["primary"] else {},
        )
        return TaskOutcome.succeeded()

    service = NativeMediaItemReprocessService(catalog, metadata, MediaInfo(), refresh=refresh)
    outcome = service.execute("1", Progress(), operation_key="hierarchy")
    assert outcome.status.value == "succeeded"
    assert outcome.details["metadata"]["verified"] == 3
    assert catalog.items["3"]["Name"] == "失意的魔术师"
    assert catalog.items["3"]["ProviderIds"]["Tmdb"] == "203"
    with p5_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT parent_series_tmdb_id,season_number,episode_number,release_year,thumb_path "
                "FROM media_metadata WHERE tmdb_id='203' AND item_type='Episode'"
            )
        ).one()
    assert tuple(row) == ("200", 2, 1, 2023, "/still.jpg")
