from __future__ import annotations

from sqlalchemy import text

from etk.domains.media.subscription_projection import SqlAlchemySubscriptionMediaProjection
from etk.domains.subscription.auto_subscribe_repository import SqlAlchemyAutoSubscriptionRepository
from etk.platform.db import DatabaseRuntime


def test_auto_subscription_worklist_and_lifecycle_projection(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    fixture_ids = ["990000", "990001", "990002", "990003", "990010", "990011", "990012"]
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = ANY(CAST(:ids AS text[]))"),
            {"ids": fixture_ids},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, release_date, subscription_status,
                    in_library, first_requested_at, last_subscribed_at,
                    parent_series_tmdb_id, season_number, episode_number,
                    subscription_sources_json, last_updated_at
                ) VALUES
                    ('990000', 'Series', 'Fixture Series', '2020-01-01', 'NONE', FALSE,
                     NOW() - INTERVAL '20 days', NULL, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('990001', 'Movie', 'Pending Movie', CURRENT_DATE - 1, 'PENDING_RELEASE', FALSE,
                     NOW() - INTERVAL '10 days', NULL, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('990002', 'Movie', 'Wanted Movie', '2020-01-01', 'WANTED', FALSE,
                     NOW() - INTERVAL '9 days', NULL, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('990003', 'Movie', 'Subscribed Movie', '2020-01-01', 'SUBSCRIBED', FALSE,
                     NOW() - INTERVAL '8 days', NOW() - INTERVAL '8 days', NULL, NULL, NULL,
                     '[]'::jsonb, NOW()),
                    ('990010', 'Season', 'Fixture S02', '2020-01-01', 'SUBSCRIBED', TRUE,
                     NOW() - INTERVAL '7 days', NOW() - INTERVAL '7 days', '990000', 2, NULL,
                     '[]'::jsonb, NOW()),
                    ('990011', 'Episode', 'Fixture S02E01', CURRENT_DATE - 1, 'NONE', FALSE,
                     NULL, NULL, '990000', 2, 1, '[]'::jsonb, NOW()),
                    ('990012', 'Episode', 'Fixture S02E02', CURRENT_DATE + 1, 'NONE', FALSE,
                     NULL, NULL, '990000', 2, 2, '[]'::jsonb, NOW())
                """
            )
        )

    assert repository.promote_pending_to_wanted() == 1
    worklist = [dict(item) for item in repository.worklist()]

    assert [(item["tmdb_id"], item["item_type"]) for item in worklist] == [
        ("990001", "Movie"),
        ("990002", "Movie"),
        ("990010", "Season"),
        ("990003", "Movie"),
    ]
    season = next(item for item in worklist if item["tmdb_id"] == "990010")
    assert season["missing_episode_numbers"] == [1]
    assert season["missing_episode_count"] == 1
    assert repository.series_title("990000") == "Fixture Series"

    repository.mark_movie_paused("990003", pause_days=7)
    with p7_runtime.engine.connect() as connection:
        paused = (
            connection.execute(
                text(
                    """
                    SELECT subscription_status, paused_until > NOW() AS future_pause
                    FROM media_metadata
                    WHERE tmdb_id = '990003' AND item_type = 'Movie'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert dict(paused) == {"subscription_status": "PAUSED", "future_pause": True}

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET paused_until = NOW() - INTERVAL '1 minute'
                WHERE tmdb_id = '990003' AND item_type = 'Movie'
                """
            )
        )
    assert [item["tmdb_id"] for item in repository.movies_to_revive()] == ["990003"]
    repository.mark_movie_revived("990003")
    repository.mark_subscribed(worklist[1])

    with p7_runtime.engine.connect() as connection:
        rows = {
            row["tmdb_id"]: dict(row)
            for row in connection.execute(
                text(
                    """
                    SELECT tmdb_id, subscription_status, paused_until
                    FROM media_metadata
                    WHERE tmdb_id IN ('990002', '990003')
                    ORDER BY tmdb_id
                    """
                )
            ).mappings()
        }
    assert rows["990002"]["subscription_status"] == "SUBSCRIBED"
    assert rows["990003"]["subscription_status"] == "SUBSCRIBED"
    assert rows["990003"]["paused_until"] is None


def test_auto_subscription_pause_query_uses_bounded_window(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '990020'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, release_date, subscription_status,
                    in_library, last_subscribed_at, subscription_sources_json, last_updated_at
                ) VALUES (
                    '990020', 'Movie', 'Pause Candidate', '2020-01-01', 'SUBSCRIBED',
                    FALSE, NOW() - INTERVAL '2 days', '[]'::jsonb, NOW()
                )
                """
            )
        )

    assert [item["tmdb_id"] for item in repository.movies_to_pause(search_window_days=1)] == ["990020"]
    assert repository.movies_to_pause(search_window_days=3) == ()
