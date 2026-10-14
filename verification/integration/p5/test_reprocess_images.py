from unittest.mock import Mock

import httpx
import pytest
from sqlalchemy import text

from etk.domains.media.reprocess_images import NativeReprocessImagePreparation
from etk.domains.media.task_execution import HttpxTmdbMetadataAdapter, SqlAlchemyMediaMetadataTaskRepository
from etk.platform import Settings


def setup(p5_runtime, kind="Series", options=None):
    item = {"Id": "item", "Type": kind, "Path": "/media/fixture", "ProviderIds": {"Tmdb": "200"}, "ImageTags": {}}
    root = item if kind in {"Movie", "Series"} else {"ProviderIds": {"Tmdb": "100"}}
    if kind in {"Season", "Episode"}:
        item.update(_series_tmdb_id="100", _season_number=1, _episode_number=2 if kind == "Episode" else None)
    repo = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    repo.upsert_metadata(
        item,
        {"id": 200, "name": "示例", "poster_path": "/cached-poster.jpg", "backdrop_path": "/cached-bg.jpg"},
        preserve_people=False,
    )
    queries = []

    def http(request):
        queries.append(request.url.path)
        return httpx.Response(
            200,
            json=[
                {
                    "Locations": ["/media"],
                    "LibraryOptions": {
                        "TypeOptions": [{"Type": kind, "ImageFetchers": ["ETK Images"], "ImageOptions": options or []}]
                    },
                }
            ],
        )

    tmdb = Mock()
    tmdb.images.return_value = {
        "logos": [
            {"file_path": "/en-logo.png", "iso_639_1": "en", "width": 800},
            {"file_path": "/zh-logo.png", "iso_639_1": "zh", "width": 800},
        ],
        "posters": [{"file_path": "/new-poster.jpg", "width": 1000}],
        "stills": [{"file_path": "/still.jpg", "width": 1920}],
    }
    service = NativeReprocessImagePreparation(
        settings=lambda: Settings(emby_server_url="http://emby.fixture", emby_api_key="fixture"),
        unit_of_work=p5_runtime.unit_of_work,
        tmdb=tmdb,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(http)),
    )
    return service, tmdb, item, root, repo, queries


@pytest.mark.parametrize("kind", ["Movie", "Series"])
def test_prepares_root_logo_without_replacing_cached_artwork(p5_runtime, kind):
    service, tmdb, item, root, repo, queries = setup(p5_runtime, kind)
    flags = service.prepare(item, root, {})
    assert flags == {"_primary_image": True, "_backdrop_image": True, "_logo_image": True}
    tmdb.images.assert_called_once_with("200", kind, season=None, episode=None)
    with p5_runtime.engine.connect() as c:
        row = c.execute(
            text(
                "SELECT poster_path,backdrop_path,logo_path FROM media_metadata WHERE tmdb_id='200' AND item_type=:kind"
            ),
            {"kind": kind},
        ).one()
    assert tuple(row) == ("/cached-poster.jpg", "/cached-bg.jpg", "/zh-logo.png")
    assert queries == ["/Library/VirtualFolders"]
    repo.upsert_metadata(
        {**item, "_preserve_cached_images": True},
        {"id": 200, "poster_path": "/other.jpg", "backdrop_path": "/other-bg.jpg"},
        preserve_people=False,
    )
    with p5_runtime.engine.connect() as c:
        assert (
            c.execute(
                text("SELECT poster_path FROM media_metadata WHERE tmdb_id='200' AND item_type=:kind"), {"kind": kind}
            ).scalar()
            == "/cached-poster.jpg"
        )


@pytest.mark.parametrize("kind", ["Season", "Episode"])
def test_child_images_are_scoped_to_selected_season_or_episode(p5_runtime, kind):
    service, tmdb, item, root, _, queries = setup(p5_runtime, kind)
    cache = {}
    flags = service.prepare(item, root, cache)
    assert flags["_primary_image"] is True
    assert "_logo_image" not in flags and "_backdrop_image" not in flags
    if kind == "Episode":
        tmdb.images.assert_called_once_with("100", kind, season=1, episode=2)
        assert flags["_thumb_image"] is True
    else:
        tmdb.images.assert_not_called()
    service.prepare(item, root, cache)
    assert queries == ["/Library/VirtualFolders"]


@pytest.mark.parametrize("unavailable", [True, False])
def test_unavailable_or_disabled_logo_is_not_required(p5_runtime, unavailable):
    service, tmdb, item, root, _, _ = setup(p5_runtime, options=[] if unavailable else [{"Type": "Logo", "Limit": 0}])
    if unavailable:
        tmdb.images.return_value = {"logos": []}
    flags = service.prepare(item, root, {})
    assert "_logo_image" not in flags
    if not unavailable:
        tmdb.images.assert_not_called()


def test_tmdb_image_adapter_omits_text_language_and_scopes_endpoint():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"logos": []})

    provider = HttpxTmdbMetadataAdapter(
        lambda: Settings(tmdb_api_key="fixture"),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    provider.images("100", "Episode", season=1, episode=2)
    assert requests[0].url.path.endswith("/tv/100/season/1/episode/2/images")
    assert "language" not in requests[0].url.params
