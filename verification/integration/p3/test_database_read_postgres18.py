from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.database import DatabaseReadService, SqlAlchemyDatabaseReadRepository
from etk.domains.operations.database_router import _legacy_jsonable
from etk.platform.db import DatabaseRuntime, EngineConfig
from scripts.refactor.compare_schema import collect_catalog

TIMEZONE = "Asia/Shanghai"
LEGACY_TABLE_COUNT = 39
GREENFIELD_TABLES = {
    "cleanup_execution_items",
    "idempotency_records",
    "job_attempts",
    "job_runs",
    "media_effects",
    "outbox_events",
    "p115_organize_effect_events",
    "p115_organize_effects",
    "p115_organize_execution_actions",
    "p115_organize_plan_actions",
    "p115_organize_plan_executions",
    "p115_organize_plans",
    "p115_organize_reconciliation_intents",
    "p115_media_probe_items",
    "p115_strm_sync_plans",
    "ingestion_monitor_items",
    "ingestion_pending_bindings",
    "p115_upload_monitor_controls",
    "p115_upload_operations",
    "p115_playback_operations",
    "p115_playback_artifacts",
    "p115_playback_leases",
    "p115_playback_qr_sessions",
    "p115_intro_detection_operations",
    "subscription_operations",
    "subscription_quota_reservations",
    "subscription_runtime_states",
}
EXPECTED_TABLE_COUNT = LEGACY_TABLE_COUNT + len(GREENFIELD_TABLES)
EXPECTED_MIGRATION_HEAD = "0015_p7_assistant_claims"
_FIXTURE_TABLES = (
    "failed_log",
    "processed_log",
    "translation_cache",
    "p115_mediainfo_cache",
    "media_metadata",
)


@pytest.fixture
def database_runtime(migrated_postgres18_url: str) -> Iterator[DatabaseRuntime]:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_postgres18_url,
            application_name="etk-p3-database-read-integration",
        )
    )
    _truncate_fixture_tables(runtime)
    try:
        yield runtime
    finally:
        _truncate_fixture_tables(runtime)
        runtime.shutdown()


def _truncate_fixture_tables(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(text(f"TRUNCATE {', '.join(_FIXTURE_TABLES)} CASCADE"))


def _service(runtime: DatabaseRuntime) -> DatabaseReadService:
    repository = SqlAlchemyDatabaseReadRepository(
        runtime.unit_of_work,
        timezone=TIMEZONE,
    )
    return DatabaseReadService(repository)


def test_pg18_empty_migrated_schema_executes_all_database_read_queries(
    migrated_postgres18_url: str,
    database_runtime: DatabaseRuntime,
) -> None:
    catalog = collect_catalog(migrated_postgres18_url)
    assert catalog["server_version_num"].startswith("18")
    assert len(catalog["tables"]) == EXPECTED_TABLE_COUNT

    with database_runtime.engine.connect() as connection:
        assert connection.scalar(text("SHOW server_version_num")).startswith("18")
        assert (
            connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == EXPECTED_MIGRATION_HEAD
        )

    service = _service(database_runtime)
    assert service.core_stats() == {
        "media_cached_total": 0,
        "mediainfo_backed_up_total": 0,
        "mediainfo_hits_total": 0,
    }

    library = service.library_stats()
    assert library["movies_in_library"] == 0
    assert library["series_in_library"] == 0
    assert library["episodes_in_library"] == 0
    assert library["resolution_stats"] == []
    assert len(library["weekly_added_stats"]) == 7
    assert all(row["count"] == row["movies"] == row["episodes"] == 0 for row in library["weekly_added_stats"])

    assert service.system_stats() == {
        "translation_cache_count": 0,
        "processed_log_count": 0,
        "failed_log_count": 0,
    }
    assert service.rankings() == {
        "release_group_ranking": [],
        "historical_release_group_ranking": [],
    }

    table_names = service.table_names()
    assert len(table_names) == EXPECTED_TABLE_COUNT
    assert table_names == sorted(table_names)
    assert {"failed_log", "media_metadata", "p115_mediainfo_cache"}.issubset(table_names)
    assert GREENFIELD_TABLES.issubset(table_names)
    assert service.review_items(page=1, per_page=20, query="", reason="") == {
        "items": [],
        "total_items": 0,
        "total_pages": 0,
        "current_page": 1,
        "per_page": 20,
        "query": "",
        "reason": "",
    }
    assert service.review_reasons() == []


def test_pg18_jsonb_timezone_rankings_and_review_contracts(
    database_runtime: DatabaseRuntime,
) -> None:
    local_zone = ZoneInfo(TIMEZONE)
    local_midday = datetime.combine(datetime.now(local_zone).date(), time(12), tzinfo=local_zone)
    now_utc = local_midday.astimezone(UTC)
    old_utc = now_utc - timedelta(days=2)
    current_text = now_utc.replace(tzinfo=None).isoformat()
    old_text = old_utc.replace(tzinfo=None).isoformat()
    movie_assets = [
        {
            "resolution_display": "4K",
            "release_group_raw": ["FixtureGroup", "SecondGroup"],
            "date_added_to_library": current_text,
        },
        {
            "resolution_display": "1080p",
            "release_group_raw": ["FixtureGroup"],
            "date_added_to_library": current_text,
        },
        {
            "resolution_display": "720p",
            "release_group_raw": ["OldGroup"],
            "date_added_to_library": old_text,
        },
        {
            "resolution_display": "720p",
            "release_group_raw": ["OldGroup"],
            "date_added_to_library": old_text,
        },
    ]
    episode_assets = [
        {
            "resolution_display": "1080p",
            "release_group_raw": ["FixtureGroup"],
            "date_added_to_library": current_text,
        }
    ]
    _seed_minimal_data(database_runtime, movie_assets=movie_assets, episode_assets=episode_assets)

    service = _service(database_runtime)
    assert service.core_stats() == {
        "media_cached_total": 2,
        "mediainfo_backed_up_total": 2,
        "mediainfo_hits_total": 7,
    }

    library = service.library_stats()
    assert library["movies_in_library"] == 1
    assert library["series_in_library"] == 1
    assert library["episodes_in_library"] == 1
    assert {row["resolution"]: row["count"] for row in library["resolution_stats"]} == {
        "4K": 1,
        "1080p": 2,
        "720p": 2,
    }

    weekly = {row["date"]: row for row in library["weekly_added_stats"]}
    current_date = now_utc.astimezone(local_zone).date().isoformat()
    old_date = old_utc.astimezone(local_zone).date().isoformat()
    assert weekly[current_date] == {
        "date": current_date,
        "date_label": now_utc.astimezone(local_zone).strftime("%m-%d"),
        "count": 3,
        "movies": 2,
        "episodes": 1,
    }
    assert weekly[old_date]["count"] == 2
    assert weekly[old_date]["movies"] == 2
    assert weekly[old_date]["episodes"] == 0

    assert service.system_stats() == {
        "translation_cache_count": 1,
        "processed_log_count": 1,
        "failed_log_count": 3,
    }
    rankings = service.rankings()
    assert rankings["release_group_ranking"] == [
        {"release_group": "FixtureGroup", "count": 3},
        {"release_group": "SecondGroup", "count": 1},
    ]
    assert rankings["historical_release_group_ranking"] == [
        {"release_group": "FixtureGroup", "count": 3},
        {"release_group": "OldGroup", "count": 2},
        {"release_group": "SecondGroup", "count": 1},
    ]

    filtered = service.review_items(
        page=1,
        per_page=1,
        query="100%_Fixture'; SELECT",
        reason="缺失媒体信息",
    )
    assert filtered["total_items"] == 1
    assert filtered["total_pages"] == 1
    assert filtered["items"][0]["item_id"] == "fixture-review-1"
    assert filtered["items"][0]["failed_at"].tzinfo is not None

    second_page = service.review_items(page=2, per_page=2, query="", reason="")
    assert second_page["total_items"] == 3
    assert second_page["total_pages"] == 2
    assert [item["item_id"] for item in second_page["items"]] == ["fixture-review-3"]
    assert service.review_reasons() == ["处理评分", "缺失媒体信息", "自定义原因"]

    legacy_payload = _legacy_jsonable(filtered)
    assert legacy_payload["items"][0]["failed_at"] == "Sun, 23 Aug 2026 12:00:00 GMT"
    assert legacy_payload["items"][0]["score"] == pytest.approx(8.5)


def test_pg18_default_app_factory_serves_all_database_read_routes(
    migrated_postgres18_url: str,
    database_runtime: DatabaseRuntime,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    del database_runtime
    secret_key = b"p3-pg18-default-app-session-key"
    monkeypatch.setenv("ETK_DATABASE_URL", migrated_postgres18_url)
    data_dir = tmp_path / "config"
    app = create_app(
        start_runtime=False,
        data_dir=data_dir,
        secret_key=secret_key,
        supervisor=RuntimeSupervisor(),
    )
    app.state.container.settings = app.state.container.settings.with_updates({"auth_enabled": False})
    cookie = LegacySessionCodec.from_secret_key(secret_key).dumps({"emby_user_id": "fixture-user"})

    with TestClient(app) as client:
        client.cookies.set("session", cookie)
        responses = {
            path: client.get(path)
            for path in (
                "/api/database/stats/core",
                "/api/database/stats/library",
                "/api/database/stats/system",
                "/api/database/stats/rankings",
                "/api/database/tables",
                "/api/review_items",
                "/api/review_reasons",
            )
        }

    assert all(response.status_code == 200 for response in responses.values())
    assert len(responses["/api/database/tables"].json()) == EXPECTED_TABLE_COUNT
    assert GREENFIELD_TABLES.issubset(responses["/api/database/tables"].json())
    assert responses["/api/review_items"].json()["items"] == []
    assert data_dir.exists() is False


def _seed_minimal_data(
    runtime: DatabaseRuntime,
    *,
    movie_assets: list[dict[str, object]],
    episode_assets: list[dict[str, object]],
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json, hit_count)
                VALUES
                    (:sha1_a, CAST(:mediainfo AS jsonb), :hits_a),
                    (:sha1_b, CAST(:mediainfo AS jsonb), :hits_b)
                """
            ),
            {
                "sha1_a": "A" * 40,
                "sha1_b": "B" * 40,
                "mediainfo": json.dumps({"fixture": True}),
                "hits_a": 3,
                "hits_b": 4,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, asset_details_json, title
                ) VALUES
                    (:movie_id, :movie_type, TRUE, CAST(:movie_assets AS jsonb), :movie_title),
                    (:series_id, :series_type, TRUE, CAST(:empty_assets AS jsonb), :series_title),
                    (:episode_id, :episode_type, TRUE, CAST(:episode_assets AS jsonb), :episode_title)
                """
            ),
            {
                "movie_id": "fixture-movie",
                "movie_type": "Movie",
                "movie_assets": json.dumps(movie_assets, ensure_ascii=False),
                "movie_title": "Fixture Movie",
                "series_id": "fixture-series",
                "series_type": "Series",
                "empty_assets": "[]",
                "series_title": "Fixture Series",
                "episode_id": "fixture-episode",
                "episode_type": "Episode",
                "episode_assets": json.dumps(episode_assets, ensure_ascii=False),
                "episode_title": "Fixture Episode",
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO translation_cache (original_text, translated_text, engine_used)
                VALUES (:original, :translated, :engine)
                """
            ),
            {"original": "fixture-original", "translated": "fixture-translated", "engine": "fixture"},
        )
        connection.execute(
            text(
                """
                INSERT INTO processed_log (item_id, item_name, score)
                VALUES (:item_id, :item_name, :score)
                """
            ),
            {"item_id": "fixture-processed", "item_name": "Fixture Processed", "score": 9.0},
        )
        connection.execute(
            text(
                """
                INSERT INTO failed_log (
                    item_id, item_name, reason, failed_at, item_type, score
                ) VALUES
                    (:id_1, :name_1, :reason_1, :failed_1, :type_1, :score_1),
                    (:id_2, :name_2, :reason_2, :failed_2, :type_2, :score_2),
                    (:id_3, :name_3, :reason_3, :failed_3, :type_3, :score_3)
                """
            ),
            {
                "id_1": "fixture-review-1",
                "name_1": "100%_Fixture'; SELECT",
                "reason_1": "缺失媒体信息: S01E01",
                "failed_1": datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
                "type_1": "Episode",
                "score_1": 8.5,
                "id_2": "fixture-review-2",
                "name_2": "Fixture Rating",
                "reason_2": "处理评分: low confidence",
                "failed_2": datetime(2026, 8, 23, 11, 0, tzinfo=UTC),
                "type_2": "Movie",
                "score_2": 4.0,
                "id_3": "fixture-review-3",
                "name_3": "Fixture Custom",
                "reason_3": "自定义原因",
                "failed_3": datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
                "type_3": "Series",
                "score_3": 6.0,
            },
        )
