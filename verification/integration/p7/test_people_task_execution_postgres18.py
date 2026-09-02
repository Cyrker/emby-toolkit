from __future__ import annotations

import json

from sqlalchemy import text

from etk.domains.people.task_execution import SqlAlchemyPersonMetadataRepository


def test_pg18_people_repository_upserts_metadata_and_actor_scan(p7_runtime) -> None:
    repository = SqlAlchemyPersonMetadataRepository(p7_runtime.unit_of_work)

    assert repository.upsert(
        {
            "id": 100,
            "name": "Fixture Person",
            "original_name": "Fixture Original",
            "profile_path": "/fixture.jpg",
            "gender": 2,
            "adult": False,
            "popularity": 4.5,
            "also_known_as": ["Alias"],
        }
    )
    enrichment = repository.list_for_enrichment(force=True)
    assert enrichment and enrichment[0]["tmdb_person_id"] == 100

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO actor_subscriptions (tmdb_person_id, actor_name, status)
                VALUES (100, 'Fixture Person', 'active')
                """
            )
        )
    subscriptions = repository.list_actor_subscriptions()
    assert len(subscriptions) == 1
    subscription_id = int(subscriptions[0]["id"])
    assert repository.record_actor_scan(subscription_id, ["200", "200", "201"]) == 1

    with p7_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT last_scanned_tmdb_ids_json
                FROM actor_subscriptions
                WHERE id = :id
                """
            ),
            {"id": subscription_id},
        ).scalar_one()
    assert row == ["200", "201"]


def test_pg18_people_repository_resolves_tmdb_imdb_identity_conflict(p7_runtime) -> None:
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO person_metadata (tmdb_person_id, primary_name, imdb_id, douban_celebrity_id)
                VALUES (1001, 'Existing', 'nm-existing', 'db-existing'),
                       (1002, 'Stale', NULL, 'db-stale')
                """
            )
        )

    repository = SqlAlchemyPersonMetadataRepository(p7_runtime.unit_of_work)
    assert repository.upsert(
        {
            "id": 1002,
            "name": "New name",
            "original_name": "New original",
            "profile_path": "/new.jpg",
            "gender": 1,
            "adult": False,
            "popularity": 2.0,
            "external_ids": {"imdb_id": "nm-existing"},
        }
    )

    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT tmdb_person_id, primary_name, imdb_id, douban_celebrity_id
                    FROM person_metadata
                    WHERE tmdb_person_id IN (1001, 1002)
                    ORDER BY tmdb_person_id
                    """
                )
            )
            .mappings()
            .all()
        )
    assert rows == [
        {
            "tmdb_person_id": 1001,
            "primary_name": "Existing",
            "imdb_id": "nm-existing",
            "douban_celebrity_id": "db-existing",
        }
    ]


def test_pg18_actor_scan_reconciles_sources_and_release_status_in_one_uow(p7_runtime) -> None:
    with p7_runtime.engine.begin() as connection:
        subscription_id = connection.scalar(
            text(
                """
                INSERT INTO actor_subscriptions (
                    tmdb_person_id, actor_name, config_start_year, config_media_types,
                    config_min_rating, config_min_vote_count, status, last_scanned_tmdb_ids_json
                ) VALUES (777, 'Fixture Actor', 2020, 'Movie,TV', 7.0, 10, 'active', '[\"old\"]'::jsonb)
                RETURNING id
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, subscription_status, subscription_sources_json
                ) VALUES (
                    'old', 'Movie', 'Old title', FALSE, 'WANTED',
                    CAST(:source AS jsonb)
                )
                """
            ),
            {
                "source": json.dumps(
                    [
                        {
                            "type": "actor_subscription",
                            "id": int(subscription_id),
                            "name": "Fixture Actor",
                            "person_id": 777,
                        }
                    ]
                )
            },
        )
    repository = SqlAlchemyPersonMetadataRepository(p7_runtime.unit_of_work)
    subscription = {
        "id": int(subscription_id),
        "tmdb_person_id": 777,
        "actor_name": "Fixture Actor",
    }
    works = (
        {
            "id": 100,
            "media_type": "movie",
            "title": "中文电影",
            "release_date": "2024-01-01",
            "poster_path": "/100.jpg",
            "genre_ids": [18],
            "vote_average": 8.0,
            "vote_count": 100,
        },
        {
            "id": 101,
            "media_type": "movie",
            "title": "未来电影",
            "release_date": "2099-01-01",
            "poster_path": "/101.jpg",
            "genre_ids": [18],
            "vote_average": 8.0,
            "vote_count": 100,
        },
    )

    result = repository.reconcile_actor_scan(subscription, works, ["100", "101"])

    assert result == {"added": 2, "removed": 1, "scanned": 2}
    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT tmdb_id, subscription_status, subscription_sources_json
                    FROM media_metadata
                    WHERE tmdb_id IN ('old', '100', '101')
                    ORDER BY tmdb_id
                    """
                )
            )
            .mappings()
            .all()
        )
        scanned = connection.execute(
            text("SELECT last_scanned_tmdb_ids_json FROM actor_subscriptions WHERE id = :id"),
            {"id": int(subscription_id)},
        ).scalar_one()
    assert [row["tmdb_id"] for row in rows] == ["100", "101"]
    assert rows[0]["subscription_status"] == "WANTED"
    assert rows[1]["subscription_status"] == "PENDING_RELEASE"
    assert rows[0]["subscription_sources_json"][0]["type"] == "actor_subscription"
    assert scanned == ["100", "101"]


def test_pg18_actor_scan_publishes_series_seasons_and_removes_stale_seasons(p7_runtime) -> None:
    with p7_runtime.engine.begin() as connection:
        subscription_id = connection.scalar(
            text(
                """
                INSERT INTO actor_subscriptions (
                    tmdb_person_id, actor_name, config_start_year, config_media_types, status
                ) VALUES (888, 'Season Actor', 2020, 'TV', 'active')
                RETURNING id
                """
            )
        )
        source = json.dumps(
            [{"type": "actor_subscription", "id": int(subscription_id), "name": "Season Actor", "person_id": 888}]
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, subscription_status,
                    parent_series_tmdb_id, season_number, subscription_sources_json
                ) VALUES ('3999', 'Season', '旧季', FALSE, 'WANTED', '999', 9, CAST(:source AS jsonb))
                """
            ),
            {"source": source},
        )

    repository = SqlAlchemyPersonMetadataRepository(p7_runtime.unit_of_work)
    subscription = {"id": int(subscription_id), "tmdb_person_id": 888, "actor_name": "Season Actor"}
    works = (
        {
            "id": 999,
            "media_type": "tv",
            "name": "示例剧集",
            "first_air_date": "2024-01-01",
            "poster_path": "/series.jpg",
            "seasons": [
                {
                    "id": 3001,
                    "season_number": 1,
                    "name": "第一季",
                    "air_date": "2024-01-01",
                    "poster_path": "/season.jpg",
                },
                {
                    "id": 3002,
                    "season_number": 2,
                    "name": "第二季",
                    "air_date": "2099-01-01",
                },
            ],
        },
    )

    result = repository.reconcile_actor_scan(subscription, works, ["999"])

    assert result == {"added": 2, "removed": 1, "scanned": 1}
    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, season_number, subscription_status,
                           parent_series_tmdb_id
                    FROM media_metadata
                    WHERE (tmdb_id IN ('3001', '3002', '999') AND item_type IN ('Season', 'Series'))
                       OR tmdb_id = '3999'
                    ORDER BY item_type, tmdb_id
                    """
                )
            )
            .mappings()
            .all()
        )
    assert [row["tmdb_id"] for row in rows] == ["3001", "3002", "999"]
    assert rows[0]["subscription_status"] == "WANTED"
    assert rows[1]["subscription_status"] == "PENDING_RELEASE"
    assert rows[2]["item_type"] == "Series"
