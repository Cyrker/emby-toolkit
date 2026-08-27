from __future__ import annotations

from sqlalchemy import text

from etk.domains.media.user_projection import SqlAlchemyUserMediaProjection
from etk.platform.db import DatabaseRuntime


def test_dashboard_aggregation_maps_episode_to_series_and_preserves_movie(
    p8_runtime: DatabaseRuntime,
) -> None:
    projection = SqlAlchemyUserMediaProjection(p8_runtime.unit_of_work)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, poster_path, emby_item_ids_json
                ) VALUES
                    (
                        'series-101', 'Series', 'Fixture Series', '/series.jpg',
                        '["emby-series-101"]'::jsonb
                    ),
                    (
                        'episode-101', 'Episode', 'Fixture Episode', '/episode.jpg',
                        '["emby-episode-101"]'::jsonb
                    ),
                    (
                        'movie-202', 'Movie', 'Fixture Movie', '/movie.jpg',
                        '["emby-movie-202"]'::jsonb
                    )
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET parent_series_tmdb_id = 'series-101'
                WHERE tmdb_id = 'episode-101' AND item_type = 'Episode'
                """
            )
        )

    assert projection.dashboard_aggregation(["emby-episode-101", "emby-movie-202", "emby-episode-101", "missing"]) == {
        "emby-episode-101": {
            "id": "series-101",
            "name": "Fixture Series",
            "poster_path": "/series.jpg",
            "type": "Series",
            "emby_id": "emby-series-101",
        },
        "emby-movie-202": {
            "id": "movie-202",
            "name": "Fixture Movie",
            "poster_path": "/movie.jpg",
            "type": "Movie",
            "emby_id": "emby-movie-202",
        },
    }


def test_dashboard_aggregation_returns_empty_without_provider_ids(
    p8_runtime: DatabaseRuntime,
) -> None:
    projection = SqlAlchemyUserMediaProjection(p8_runtime.unit_of_work)

    assert projection.dashboard_aggregation([]) == {}
    assert projection.dashboard_aggregation(["", "   "]) == {}
