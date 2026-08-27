from __future__ import annotations

from sqlalchemy import text

from etk.domains.media.discover_projection import SqlAlchemyDiscoverProjection
from etk.platform.db import DatabaseRuntime


def test_discover_projection_enriches_and_filters_authoritative_local_state(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    subscription_status, parent_series_tmdb_id, season_number
                ) VALUES
                    ('1', 'Movie', TRUE, '["emby-1"]'::jsonb, 'WANTED', NULL, NULL),
                    ('2', 'Movie', FALSE, '[]'::jsonb, 'REQUESTED', NULL, NULL),
                    ('season-1', 'Season', TRUE, '["emby-season-1"]'::jsonb,
                     'WANTED', '100', 1),
                    ('season-2', 'Season', FALSE, '[]'::jsonb,
                     'REQUESTED', '100', 2)
                """
            )
        )
    projection = SqlAlchemyDiscoverProjection(p8_runtime.unit_of_work)
    payload = {
        "page": 1,
        "total_pages": 4,
        "total_results": 999,
        "results": [
            {"id": 1, "poster_path": "/1.jpg"},
            {"id": 2, "poster_path": "/2.jpg"},
            {"id": 3, "poster_path": None},
        ],
    }

    visible = projection.enrich(payload, item_type="Movie", hide_in_library=False)
    hidden = projection.enrich(payload, item_type="Movie", hide_in_library=True)
    seasons = projection.enrich_tv_seasons(
        "100",
        {
            "id": 100,
            "seasons": [
                {"id": 1000, "season_number": 0},
                {"id": 1001, "season_number": 1},
                {"id": 1002, "season_number": 2},
                {"id": 1003},
            ],
        },
    )

    assert visible["results"] == [
        {
            "id": 1,
            "poster_path": "/1.jpg",
            "in_library": True,
            "emby_item_id": ["emby-1"],
            "subscription_status": "WANTED",
            "media_type": "movie",
        },
        {
            "id": 2,
            "poster_path": "/2.jpg",
            "in_library": False,
            "emby_item_id": [],
            "subscription_status": "REQUESTED",
            "media_type": "movie",
        },
    ]
    assert visible["total_results"] == 2
    assert visible["total_pages"] == 4
    assert hidden["results"] == [visible["results"][1]]
    assert hidden["total_results"] == 1
    assert seasons["seasons"] == [
        {"id": 1000, "season_number": 0, "in_library": False, "subscription_status": "NONE"},
        {"id": 1001, "season_number": 1, "in_library": True, "subscription_status": "WANTED"},
        {"id": 1002, "season_number": 2, "in_library": False, "subscription_status": "REQUESTED"},
        {"id": 1003},
    ]
