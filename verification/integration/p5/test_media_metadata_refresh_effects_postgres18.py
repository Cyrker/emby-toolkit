from __future__ import annotations

from typing import Any, cast

import httpx
import psycopg
from psycopg.rows import dict_row
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url

from etk.domains.identity.post_authorization import identity_authorization_revision
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import MediaEffectDbApiCursor, MediaEffectStatus, PostgresMediaEffectLedger
from etk.domains.media.final_refresh_effects import (
    EmbyMetadataRefreshEffectDelivery,
    stage_final_metadata_refresh,
)
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime


def _connection_url(value: str) -> str:
    return make_url(value).set(drivername="postgresql").render_as_string(hide_password=False)


def _settings() -> Settings:
    return Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-user",
    )


def _stage(url: str, *, operation_id: str = "job-1") -> Any:
    with psycopg.connect(_connection_url(url), row_factory=cast(Any, dict_row)) as connection:
        with connection.cursor() as cursor:
            reservation = stage_final_metadata_refresh(
                cast(MediaEffectDbApiCursor, cursor),
                operation_id=operation_id,
                item_id="movie-1",
                item_type="Movie",
                replace_all_metadata=True,
                replace_all_images=False,
                metadata_only=False,
                authorization_revision=identity_authorization_revision(_settings()),
            )
        connection.commit()
    return reservation


def test_pg18_metadata_and_final_refresh_intent_commit_atomically_and_replay_once(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
) -> None:
    calls: list[str] = []

    def confirm(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(204, request=request)

    with psycopg.connect(
        _connection_url(migrated_p5_postgres_url),
        row_factory=cast(Any, dict_row),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, metadata_ready,
                    in_library, emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                ) VALUES (
                    '1001', 'Movie', 'Fixture Movie', 'Fixture Movie', TRUE,
                    TRUE, '["movie-1"]'::jsonb,
                    '[{"path":"/fixture/movie.mkv"}]'::jsonb,
                    '[""]'::jsonb, '[""]'::jsonb
                )
                """
            )
            first = stage_final_metadata_refresh(
                cast(MediaEffectDbApiCursor, cursor),
                operation_id="job-atomic",
                item_id="movie-1",
                item_type="Movie",
                replace_all_metadata=True,
                replace_all_images=False,
                metadata_only=False,
                authorization_revision=identity_authorization_revision(_settings()),
            )
        connection.commit()

    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        EmbyMetadataRefreshEffectDelivery(
            _settings,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(confirm)),
        ),
        worker_id="fixture-final-refresh",
    )
    confirmed = executor.execute(first.effect_id)
    replay = _stage(migrated_p5_postgres_url, operation_id="job-atomic")
    confirmed_replay = executor.execute(replay.effect_id)

    assert first.created is True
    assert replay.created is False
    assert confirmed.status is MediaEffectStatus.CONFIRMED
    assert confirmed_replay.status is MediaEffectStatus.CONFIRMED
    assert first.effect_id == replay.effect_id
    assert len(calls) == 1
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_metadata WHERE tmdb_id = '1001'")) == 1
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert (
            connection.scalar(text("SELECT status FROM outbox_events WHERE topic = 'media.effect.requested'"))
            == "processed"
        )


def test_pg18_final_refresh_transport_loss_is_terminal_indeterminate_without_replay(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
) -> None:
    calls = 0

    def timeout(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("fixture response lost", request=request)

    reservation = _stage(migrated_p5_postgres_url, operation_id="job-indeterminate")
    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        EmbyMetadataRefreshEffectDelivery(
            _settings,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(timeout)),
        ),
        worker_id="fixture-final-refresh",
    )

    first = executor.execute(reservation.effect_id)
    replay = executor.execute(reservation.effect_id)

    assert first.status is MediaEffectStatus.INDETERMINATE
    assert replay.status is MediaEffectStatus.INDETERMINATE
    assert calls == 1


def test_pg18_metadata_and_final_refresh_intent_roll_back_together(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
) -> None:
    with psycopg.connect(
        _connection_url(migrated_p5_postgres_url),
        row_factory=cast(Any, dict_row),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title,
                    emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                ) VALUES (
                    'rollback', 'Movie', 'Rollback', 'Rollback', '[]'::jsonb,
                    '[]'::jsonb, '[]'::jsonb, '[]'::jsonb
                )
                """
            )
            stage_final_metadata_refresh(
                cast(MediaEffectDbApiCursor, cursor),
                operation_id="job-rollback",
                item_id="movie-rollback",
                item_type="Movie",
                replace_all_metadata=True,
                replace_all_images=False,
                metadata_only=False,
                authorization_revision=identity_authorization_revision(_settings()),
            )
        connection.rollback()

    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_metadata WHERE tmdb_id = 'rollback'")) == 0
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0
        assert connection.scalar(text("SELECT count(*) FROM outbox_events WHERE topic = 'media.effect.requested'")) == 0
