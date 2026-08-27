from __future__ import annotations

from datetime import UTC, datetime
from itertools import count

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etk.domains.identity.users_repository import (
    SqlAlchemyUsersRepository,
    UserTemplateNameConflictError,
)
from etk.domains.media.user_projection import SqlAlchemyUserMediaProjection
from etk.platform.db import DatabaseRuntime

NOW = datetime(2026, 8, 27, 8, 30, tzinfo=UTC)


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyUsersRepository:
    sequence = count(1)
    return SqlAlchemyUsersRepository(
        runtime.unit_of_work,
        clock=lambda: NOW,
        token_factory=lambda: f"fixture-token-{next(sequence)}",
    )


def _insert_user(runtime: DatabaseRuntime, user_id: str, name: str = "Fixture User") -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator)
                VALUES (:user_id, :name, FALSE)
                """
            ),
            {"user_id": user_id, "name": name},
        )


def _create_template(repository: SqlAlchemyUsersRepository, name: str = "Fixture Template") -> int:
    return repository.create_template(
        name=name,
        description="含 UTF-8 的模板",
        policy={"EnableAllFolders": False, "EnabledFolders": ["library-a"]},
        default_expiration_days=30,
        source_emby_user_id="template-source",
        configuration={"AudioLanguagePreference": "jpn"},
        allow_unrestricted_subscriptions=True,
        personal_play_account_only=True,
        max_concurrent_streams=2,
    )


def test_template_jsonb_unique_conflict_and_invitation_cascade(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    template_id = _create_template(repository)

    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT emby_policy_json, emby_configuration_json
                FROM user_templates
                WHERE id = :template_id
                """
                ),
                {"template_id": template_id},
            )
            .mappings()
            .one()
        )
    assert row["emby_policy_json"] == {
        "EnableAllFolders": False,
        "EnabledFolders": ["library-a"],
    }
    assert row["emby_configuration_json"] == {"AudioLanguagePreference": "jpn"}
    assert repository.list_templates() == [
        {
            "id": template_id,
            "name": "Fixture Template",
            "description": "含 UTF-8 的模板",
            "default_expiration_days": 30,
            "source_emby_user_id": "template-source",
            "allow_unrestricted_subscriptions": True,
            "personal_play_account_only": True,
            "max_concurrent_streams": 2,
        }
    ]

    with pytest.raises(UserTemplateNameConflictError):
        _create_template(repository)

    tokens = repository.create_invitations(
        template_id=template_id,
        expiration_days=None,
        link_expires_in_days=7,
        count=2,
    )
    assert tokens == ["fixture-token-1", "fixture-token-2"]
    invitations = repository.list_invitations()
    assert {row["token"] for row in invitations} == set(tokens)
    assert {row["expiration_days"] for row in invitations} == {30}
    assert {row["template_name"] for row in invitations} == {"Fixture Template"}

    assert repository.delete_template(template_id) is True
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM invitations")) == 0


def test_invitation_status_and_local_profile_updates(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    _insert_user(p8_runtime, "fixture-user")
    template_id = _create_template(repository)
    repository.create_invitations(
        template_id=template_id,
        expiration_days=14,
        link_expires_in_days=3,
        count=1,
    )

    active = repository.invitation_status("fixture-token-1")
    assert active is not None
    assert active["status"] == "active"
    assert active["expires_at"] > NOW
    assert repository.invitation_status("missing-token") is None

    repository.set_expiration("fixture-user", "2026-09-30T00:00:00+00:00")
    repository.set_telegram_chat_id("fixture-user", "telegram-fixture")
    account = repository.account_info("fixture-user")
    assert account is not None
    assert account["id"] == "fixture-user"
    assert account["expiration_date"] == datetime(2026, 9, 30, tzinfo=UTC)
    assert account["telegram_chat_id"] == "telegram-fixture"


def test_delete_local_user_cleans_activity_and_rolls_back_on_failure(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    for user_id in ("deleted-user", "rollback-user"):
        _insert_user(p8_runtime, user_id, user_id)
        with p8_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO emby_favorite_items (user_id, item_id)
                    VALUES (:user_id, 'item-a')
                    """
                ),
                {"user_id": user_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO user_media_data (user_id, item_id, played)
                    VALUES (:user_id, 'item-a', TRUE)
                    """
                ),
                {"user_id": user_id},
            )

    assert repository.delete_local_user("deleted-user") is True
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_favorite_items WHERE user_id = 'deleted-user'")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM user_media_data WHERE user_id = 'deleted-user'")) == 0

    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_fixture_user_delete() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF OLD.id = 'rollback-user' THEN
                        RAISE EXCEPTION 'fixture delete rejected';
                    END IF;
                    RETURN OLD;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_fixture_user_delete
                BEFORE DELETE ON emby_users
                FOR EACH ROW EXECUTE FUNCTION p8_reject_fixture_user_delete()
                """
            )
        )
    try:
        with pytest.raises(DBAPIError):
            repository.delete_local_user("rollback-user")
        with p8_runtime.engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT COUNT(*) FROM emby_favorite_items WHERE user_id = 'rollback-user'")) == 1
            )
            assert connection.scalar(text("SELECT COUNT(*) FROM user_media_data WHERE user_id = 'rollback-user'")) == 1
            assert connection.scalar(text("SELECT COUNT(*) FROM emby_users WHERE id = 'rollback-user'")) == 1
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_fixture_user_delete ON emby_users"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_fixture_user_delete()"))


def test_portal_history_stats_and_library_search_projection(p8_runtime: DatabaseRuntime) -> None:
    projection = SqlAlchemyUserMediaProjection(p8_runtime.unit_of_work)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, subscription_status,
                    subscription_sources_json, first_requested_at
                ) VALUES (
                    'series-1', 'Series', 'Fixture Series', 'NONE',
                    '[]'::jsonb, :requested_at
                )
                """
            ),
            {"requested_at": NOW},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    subscription_status, subscription_sources_json, first_requested_at
                ) VALUES (
                    'season-1', 'Season', 'Raw Season Title', 'series-1', 2,
                    'REQUESTED', '[{"type":"user_request","user_id":"fixture-user"}]'::jsonb,
                    :requested_at
                )
                """
            ),
            {"requested_at": NOW},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, release_year,
                    in_library, emby_item_ids_json, asset_details_json,
                    subscription_status, subscription_sources_json, first_requested_at
                ) VALUES (
                    'movie-1', 'Movie', 'Fixture Movie', 'Fixture Original', 2026,
                    TRUE, '["emby-cut-a","emby-cut-b"]'::jsonb,
                    '[{"resolution_display":"1080p","quality_display":"WEB-DL"},
                      {"resolution_display":"2160p","quality_display":"Remux"}]'::jsonb,
                    'SUBSCRIBED', '[{"type":"user_request","user_id":"fixture-user"}]'::jsonb,
                    :requested_at
                )
                """
            ),
            {"requested_at": NOW},
        )

    history, total = projection.request_history(
        "fixture-user",
        page=1,
        page_size=20,
        status="all",
    )
    assert total == 2
    assert {row["title"] for row in history} == {"Fixture Series 第 2 季", "Fixture Movie"}
    assert {row["status"] for row in history} == {"REQUESTED", "completed"}
    assert projection.request_stats("fixture-user") == {
        "total": 2,
        "completed": 1,
        "processing": 0,
        "pending": 1,
        "failed": 0,
    }

    search = projection.search_library("Fixture")
    assert search["total_items"] == 2
    assert [item["item_id"] for item in search["items"]] == ["emby-cut-a", "emby-cut-b"]
    assert [item["item_name"] for item in search["items"]] == [
        "Fixture Movie [1080p WEB-DL]",
        "Fixture Movie [2160p Remux]",
    ]
