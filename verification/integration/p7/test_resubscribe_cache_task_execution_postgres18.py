from __future__ import annotations

import json
from collections.abc import Iterator, Mapping

import pytest
from sqlalchemy import text

from etk.domains.subscription.resubscribe_cache import SqlAlchemyResubscribeCacheRepository
from etk.platform.db import DatabaseRuntime

PREFIX = "p11-cache-"
RULE_ID = 9831


@pytest.fixture(autouse=True)
def _clean_rows(p7_runtime: DatabaseRuntime) -> Iterator[None]:
    def clean() -> None:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM resubscribe_index WHERE tmdb_id LIKE :prefix"), {"prefix": f"{PREFIX}%"}
            )
            connection.execute(text("DELETE FROM resubscribe_rules WHERE id = :rule_id"), {"rule_id": RULE_ID})
            connection.execute(
                text("DELETE FROM media_metadata WHERE tmdb_id LIKE :prefix OR parent_series_tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )

    clean()
    try:
        yield
    finally:
        clean()


def _insert_rule(runtime: DatabaseRuntime, *, scope: Mapping[str, object], **values: object) -> None:
    row: dict[str, object] = {
        "id": RULE_ID,
        "name": "cache fixture rule",
        "enabled": True,
        "sort_order": 0,
        "rule_type": "resubscribe",
        "resubscribe_source": "moviepilot",
        "scope_rules": json.dumps([dict(scope)]),
        "resubscribe_resolution_enabled": False,
        "filter_missing_episodes_enabled": False,
        "filter_rating_enabled": False,
        "filter_rating_min": 0,
    }
    row.update(values)
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_rules (
                    id, name, enabled, sort_order, rule_type, resubscribe_source,
                    scope_rules, resubscribe_resolution_enabled, resubscribe_resolution_threshold,
                    filter_missing_episodes_enabled, filter_rating_enabled, filter_rating_min
                ) VALUES (
                    :id, :name, :enabled, :sort_order, :rule_type, :resubscribe_source,
                    CAST(:scope_rules AS jsonb), :resubscribe_resolution_enabled,
                    :resubscribe_resolution_threshold, :filter_missing_episodes_enabled,
                    :filter_rating_enabled, :filter_rating_min
                )
                """
            ),
            {
                **row,
                "resubscribe_resolution_threshold": int(row.get("resubscribe_resolution_threshold", 1920)),
            },
        )


def _insert_media(runtime: DatabaseRuntime, values: Mapping[str, object]) -> None:
    row: dict[str, object] = {
        "tmdb_id": "",
        "item_type": "Movie",
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "title": None,
        "original_title": None,
        "original_language": "eng",
        "in_library": True,
        "force_ended": False,
        "emby_item_ids_json": json.dumps([]),
        "asset_details_json": json.dumps([]),
        "total_episodes": 0,
        "total_episodes_locked": False,
        "watching_status": "NONE",
    }
    row.update(values)
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number, title,
                    original_title, original_language, in_library, force_ended,
                    emby_item_ids_json, asset_details_json, total_episodes,
                    total_episodes_locked, watching_status
                ) VALUES (
                    :tmdb_id, :item_type, :parent_series_tmdb_id, :season_number, :episode_number, :title,
                    :original_title, :original_language, :in_library, :force_ended,
                    CAST(:emby_item_ids_json AS jsonb), CAST(:asset_details_json AS jsonb),
                    :total_episodes, :total_episodes_locked, :watching_status
                )
                """
            ),
            row,
        )


def test_cache_refresh_is_atomic_and_purges_only_after_a_frozen_match(p7_runtime: DatabaseRuntime) -> None:
    _insert_rule(
        p7_runtime,
        scope={"field": "title", "operator": "contains", "value": "Needs"},
        resubscribe_resolution_enabled=True,
        resubscribe_resolution_threshold=1920,
    )
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{PREFIX}movie",
            "title": "Needs refresh",
            "asset_details_json": json.dumps([{"resolution_display": "1280p"}]),
        },
    )
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{PREFIX}stale",
            "title": "Already healthy",
            "asset_details_json": json.dumps([{"resolution_display": "4k"}]),
        },
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (tmdb_id, item_type, season_number, status, reason)
                VALUES (:tmdb_id, 'Movie', -1, 'needed', 'stale')
                """
            ),
            {"tmdb_id": f"{PREFIX}stale"},
        )

    stats = SqlAlchemyResubscribeCacheRepository(p7_runtime.unit_of_work).refresh_index(lambda: None)

    assert stats == {"candidates": 2, "matched": 1, "purged": 1, "failed": 0}
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT tmdb_id, status, matched_rule_id FROM resubscribe_index WHERE tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )
            .mappings()
            .one()
        )
    assert row == {"tmdb_id": f"{PREFIX}movie", "status": "needed", "matched_rule_id": RULE_ID}


def test_cache_refresh_preserves_terminal_index_status_and_publishes_missing_seasons(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}series"
    season_id = f"{series_id}:S1"
    _insert_rule(
        p7_runtime,
        scope={"field": "title", "operator": "contains", "value": "Fixture"},
        filter_rating_enabled=True,
        filter_rating_min=9,
        rule_type="delete",
    )
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{PREFIX}movie",
            "title": "Fixture movie",
            "asset_details_json": json.dumps([{"resolution_display": "4k"}]),
        },
    )
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": series_id,
            "item_type": "Series",
            "title": "Fixture series",
            "in_library": True,
            "asset_details_json": json.dumps([]),
        },
    )
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": season_id,
            "item_type": "Season",
            "parent_series_tmdb_id": series_id,
            "season_number": 1,
            "title": "Fixture season",
            "asset_details_json": json.dumps([]),
            "total_episodes": 3,
            "total_episodes_locked": True,
        },
    )
    for number in (1, 3):
        _insert_media(
            p7_runtime,
            {
                "tmdb_id": f"{season_id}:E{number}",
                "item_type": "Episode",
                "parent_series_tmdb_id": series_id,
                "season_number": 1,
                "episode_number": number,
                "in_library": True,
                "title": f"Episode {number}",
                "asset_details_json": json.dumps([{"resolution_display": "4k", "codec_display": "hevc"}]),
                "emby_item_ids_json": json.dumps([f"emby-{number}"]),
            },
        )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (tmdb_id, item_type, season_number, status, reason)
                VALUES (:tmdb_id, 'Movie', -1, 'subscribed', 'keep terminal')
                """
            ),
            {"tmdb_id": f"{PREFIX}movie"},
        )

    stats = SqlAlchemyResubscribeCacheRepository(p7_runtime.unit_of_work).refresh_index(lambda: None)
    assert stats["matched"] == 2
    with p7_runtime.engine.connect() as connection:
        movie = connection.execute(
            text("SELECT status FROM resubscribe_index WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
            {"tmdb_id": f"{PREFIX}movie"},
        ).scalar_one()
        season = connection.execute(
            text(
                """
                SELECT status, matched_rule_id
                FROM resubscribe_index
                WHERE tmdb_id = :tmdb_id AND item_type = 'Season'
                """
            ),
            {"tmdb_id": series_id},
        ).one()
        missing = connection.execute(
            text("SELECT watchlist_missing_info_json FROM media_metadata WHERE tmdb_id = :tmdb_id"),
            {"tmdb_id": season_id},
        ).scalar_one()
    assert movie == "subscribed"
    assert season == ("needed", RULE_ID)
    assert missing == {"missing_episodes": [2]}


def test_cache_refresh_rolls_back_when_cancelled_before_publication(p7_runtime: DatabaseRuntime) -> None:
    _insert_rule(p7_runtime, scope={"field": "title", "value": "Fixture"}, resubscribe_resolution_enabled=True)
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{PREFIX}movie",
            "title": "Fixture",
            "asset_details_json": json.dumps([{"resolution_display": "1280p"}]),
        },
    )
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{PREFIX}old",
            "title": "Old",
            "asset_details_json": json.dumps([{"resolution_display": "4k"}]),
        },
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (tmdb_id, item_type, season_number, status, reason)
                VALUES (:tmdb_id, 'Movie', -1, 'needed', 'must survive cancellation')
                """
            ),
            {"tmdb_id": f"{PREFIX}old"},
        )

    calls = 0

    def cancel() -> None:
        nonlocal calls
        calls += 1
        if calls >= 4:
            raise RuntimeError("fixture cancellation")

    with pytest.raises(RuntimeError, match="fixture cancellation"):
        SqlAlchemyResubscribeCacheRepository(p7_runtime.unit_of_work).refresh_index(cancel)

    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text("SELECT tmdb_id, reason FROM resubscribe_index WHERE tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )
            .mappings()
            .all()
        )
    assert rows == [{"tmdb_id": f"{PREFIX}old", "reason": "must survive cancellation"}]
