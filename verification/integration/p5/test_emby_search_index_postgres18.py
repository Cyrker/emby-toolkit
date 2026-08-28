from __future__ import annotations

from sqlalchemy import text

from etk.domains.media.search_index import EmbySearchIndexService, SqlAlchemyEmbySearchRepository
from etk.platform.db import DatabaseRuntime


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyEmbySearchRepository:
    return SqlAlchemyEmbySearchRepository(runtime.unit_of_work)


def _clear(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE emby_search_rebuild_state, emby_search_index"))


def test_postgres18_search_index_upsert_search_filter_pinyin_delete_and_rebuild(
    p5_runtime: DatabaseRuntime,
) -> None:
    _clear(p5_runtime)
    try:
        repository = _repository(p5_runtime)
        service = EmbySearchIndexService(repository)
        assert repository.search("！！！", [], 300) == {"ready": True, "items": []}
        upserted = service.update(
            {
                "mode": "incremental",
                "items": [
                    {"id": 1, "type": "Movie", "title": "Fixture Alpha"},
                    {"id": 2, "type": "Series", "title": "甄嬛传"},
                    {"id": 3, "type": "Episode", "title": "甄嬛传 Episode 1", "series_name": "甄嬛传"},
                    {"id": 4, "type": "Movie", "title": "Fixture Beta"},
                ],
            }
        )
        assert upserted.status_code == 200
        assert upserted.payload == {"ok": True, "indexed": 4}

        direct = repository.search("alpha", [], 300)
        assert direct["ready"] is True
        assert [item["id"] for item in direct["items"]] == [1]
        assert repository.search("fixture", ["Movie"], 1)["items"] == [
            {"id": 4, "type": "Movie", "title": "Fixture Beta", "rank": 889}
        ]
        assert [item["id"] for item in repository.search("zhz", [], 300)["items"]] == [2]
        assert repository.search("fixture", [], 1)["items"][0]["id"] == 4
        assert repository.delete_items([4, "not-an-id", 4]) == 1
        assert repository.search("beta", [], 300)["items"] == []

        repository.start_rebuild("fixture-rebuild")
        assert repository.search("alpha", [], 300)["ready"] is True
        assert service.update(
            {"mode": "batch", "items": [{"id": 5, "type": "Movie", "title": "Fixture Gamma"}]}
        ).payload == {"ok": True, "indexed": 1}
        assert repository.complete_rebuild("fixture-rebuild") == 3
        assert repository.search("alpha", [], 300)["items"] == []
        assert [item["id"] for item in repository.search("gamma", [], 300)["items"]] == [5]

        repository.start_rebuild("fixture-abort")
        repository.abort_rebuild("fixture-abort")
        assert repository.search("gamma", [], 300)["ready"] is True
    finally:
        _clear(p5_runtime)


def test_postgres18_search_index_empty_rebuild_is_not_ready_and_expired_tokens_are_cleaned(
    p5_runtime: DatabaseRuntime,
) -> None:
    _clear(p5_runtime)
    try:
        repository = _repository(p5_runtime)
        repository.start_rebuild("empty-rebuild")
        assert repository.search("fixture", [], 300) == {"ready": False, "items": []}

        with p5_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO emby_search_rebuild_state(token, started_at)
                    VALUES ('expired-token', NOW() - INTERVAL '31 minutes')
                    """
                )
            )
        repository.start_rebuild("fresh-rebuild")
        with p5_runtime.engine.connect() as connection:
            tokens = (
                connection.execute(text("SELECT token FROM emby_search_rebuild_state ORDER BY token")).scalars().all()
            )
        assert tokens == ["empty-rebuild", "fresh-rebuild"]
    finally:
        _clear(p5_runtime)
