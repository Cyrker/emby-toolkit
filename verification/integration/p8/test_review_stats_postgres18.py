from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etk.domains.operations.database import DatabaseReadService, SqlAlchemyDatabaseReadRepository
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime

NOW = datetime(2026, 8, 27, 8, 30, tzinfo=UTC)


def _service(runtime: DatabaseRuntime) -> DatabaseReadService:
    settings = Settings.model_validate(
        {
            "mp_config": {"resubscribe_daily_cap": 25},
        }
    )
    return DatabaseReadService(
        SqlAlchemyDatabaseReadRepository(runtime.unit_of_work),
        quota=PostgresSubscriptionRuntimeRepository(runtime.unit_of_work),
        settings=lambda: settings,
        runtime_status=lambda: {"is_running": False},
        clock=lambda: NOW,
    )


def test_subscription_stats_are_read_only_and_use_p7_quota_snapshot(
    p8_runtime: DatabaseRuntime,
) -> None:
    quota = PostgresSubscriptionRuntimeRepository(p8_runtime.unit_of_work)
    committed = quota.reserve_quota(
        "p8-stats-committed",
        quota_date=date(2026, 8, 27),
        daily_cap=25,
        units=3,
        now=NOW,
    )
    quota.commit_quota(committed.id, now=NOW)
    quota.reserve_quota(
        "p8-stats-reserved",
        quota_date=date(2026, 8, 27),
        daily_cap=25,
        units=2,
        now=NOW,
    )

    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO actor_subscriptions (tmdb_person_id, actor_name, status)
                VALUES (101, 'Fixture Actor', 'active')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (tmdb_id, item_type, status)
                VALUES ('resub-1', 'Movie', 'needed')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, watching_status,
                    in_library, subscription_sources_json
                ) VALUES
                    ('watching-1', 'Series', 'Watching', 'Watching', FALSE, '[]'::jsonb),
                    ('paused-1', 'Series', 'Paused', 'Paused', FALSE, '[]'::jsonb),
                    ('completed-1', 'Series', 'Completed', 'Completed', FALSE, '[]'::jsonb),
                    ('actor-work', 'Movie', 'Actor Work', 'NONE', TRUE,
                     '[{"type":"actor_subscription"}]'::jsonb),
                    ('native-present', 'Movie', 'Present Movie', 'NONE', TRUE, '[]'::jsonb),
                    ('custom-present', 'Series', 'Present Series', 'NONE', TRUE, '[]'::jsonb)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO collections_info (
                    tmdb_collection_id, emby_collection_id, name,
                    is_active, all_tmdb_ids_json
                ) VALUES (
                    'native-collection', 'emby-boxset', 'Native Fixture',
                    TRUE, '["native-present","native-missing"]'::jsonb
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO custom_collections (
                    name, type, definition_json, status, generated_media_info_json
                ) VALUES (
                    'Custom Fixture', 'list', '{}'::jsonb, 'active',
                    '[
                      {"tmdb_id":"custom-present","media_type":"Series"},
                      {"tmdb_id":"custom-missing","media_type":"Movie"},
                      {"title":"No id"}
                    ]'::jsonb
                )
                """
            )
        )

    with p8_runtime.engine.connect() as connection:
        quota_rows_before = connection.execute(
            text(
                """
                SELECT id, status, state_revision, updated_at
                FROM subscription_quota_reservations
                ORDER BY id
                """
            )
        ).all()
        projection_before = connection.execute(
            text(
                """
                SELECT value_json, last_updated_at
                FROM app_settings
                WHERE setting_key = 'subscription_quota_state'
                """
            )
        ).one()

    stats = _service(p8_runtime).subscription_stats()

    assert stats == {
        "watchlist": {"watching": 1, "paused": 1, "completed": 1},
        "actors": {"subscriptions": 1, "tracked_in_library": 1},
        "resubscribe": {"pending": 1},
        "native_collections": {"total": 1, "count": 1, "missing_items": 1},
        "custom_collections": {"total": 1, "count": 1, "missing_items": 1},
        "quota": {"mp": {"available": 20, "consumed": 5}},
    }
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    """
                SELECT id, status, state_revision, updated_at
                FROM subscription_quota_reservations
                ORDER BY id
                """
                )
            ).all()
            == quota_rows_before
        )
        assert (
            connection.execute(
                text(
                    """
                SELECT value_json, last_updated_at
                FROM app_settings
                WHERE setting_key = 'subscription_quota_state'
                """
                )
            ).one()
            == projection_before
        )


def test_review_mutations_commit_and_storage_failure_rolls_back(p8_runtime: DatabaseRuntime) -> None:
    repository = SqlAlchemyDatabaseReadRepository(p8_runtime.unit_of_work)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO failed_log (item_id, item_name, reason)
                VALUES ('item-a', 'Item A', 'fixture'), ('item-b', 'Item B', 'fixture')
                """
            )
        )

    assert repository.mark_review_item_processed("missing") is False
    assert repository.mark_review_item_processed("item-a") is True
    assert repository.clear_review_items() == 1

    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO failed_log (item_id, item_name, reason)
                VALUES ('rollback-item', 'Rollback Item', 'fixture')
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_review_clear() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'fixture review clear rejected';
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_review_clear
                BEFORE DELETE ON failed_log
                FOR EACH ROW EXECUTE FUNCTION p8_reject_review_clear()
                """
            )
        )
    try:
        with pytest.raises(DBAPIError):
            repository.clear_review_items()
        with p8_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM failed_log WHERE item_id = 'rollback-item'")) == 1
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_review_clear ON failed_log"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_review_clear()"))
