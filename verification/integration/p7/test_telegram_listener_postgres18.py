from __future__ import annotations

from sqlalchemy import text

from etk.domains.subscription.telegram_listener import SqlAlchemyTelegramSubscriptionInterest
from etk.platform.db import DatabaseRuntime


def test_telegram_listener_subscription_interest_and_existing_episode_fence(
    p7_runtime: DatabaseRuntime,
) -> None:
    fixture_ids = ["997100", "997101"]
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = ANY(CAST(:ids AS text[]))"),
            {"ids": fixture_ids},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, subscription_status, watching_status,
                    in_library, parent_series_tmdb_id, season_number, episode_number,
                    subscription_sources_json, last_updated_at
                ) VALUES
                    ('997100', 'Movie', 'Fixture Movie', 'WANTED', NULL,
                     FALSE, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('997101', 'Series', 'Fixture Series', 'NONE', 'Watching',
                     FALSE, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('997101', 'Episode', 'Fixture S01E01', 'NONE', NULL,
                     TRUE, '997101', 1, 1, '[]'::jsonb, NOW())
                """
            )
        )

    repository = SqlAlchemyTelegramSubscriptionInterest(p7_runtime.unit_of_work)
    assert repository.wants({"tmdb_id": "997100", "media_type": "movie"}) is True
    assert (
        repository.wants(
            {
                "tmdb_id": "997101",
                "media_type": "tv",
                "season_number": 1,
                "episode_number": 1,
            }
        )
        is False
    )
    assert (
        repository.wants(
            {
                "tmdb_id": "997101",
                "media_type": "tv",
                "season_number": 1,
                "episode_number": 2,
            }
        )
        is True
    )

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = ANY(CAST(:ids AS text[]))"),
            {"ids": fixture_ids},
        )
