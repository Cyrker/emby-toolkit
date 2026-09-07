from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from etk.domains.sharing import PostgresSharingOperationRepository
from etk.domains.sharing.execution import SharingOperationExecutor
from etk.domains.sharing.operation_delivery import SharingProviderOperationDelivery
from etk.domains.subscription.moviepilot import MoviePilotListResult, MoviePilotQueryDisposition, MoviePilotQueryResult
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.watchlist_task_execution import (
    NativeWatchlistMoviePilotGateway,
    NativeWatchlistP115ReclassificationGateway,
    NativeWatchlistSharedMetadataGateway,
    NativeWatchlistTaskEffects,
    SqlAlchemyWatchlistTaskRepository,
)
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from etk.processes.p7_subscription_effects import PersistentMoviePilotEffectGateway

PREFIX = "p11-watchlist-native-"


@pytest.fixture(autouse=True)
def _clean_rows(p7_runtime: DatabaseRuntime) -> Iterator[None]:
    def clean() -> None:
        with p7_runtime.engine.begin() as connection:
            media_effect_ids = tuple(
                str(value)
                for value in connection.scalars(
                    text("SELECT id FROM media_effects WHERE aggregate_id = :aggregate_id"),
                    {"aggregate_id": "emby-effect"},
                )
            )
            if media_effect_ids:
                connection.execute(
                    text("DELETE FROM outbox_events WHERE aggregate_id = ANY(CAST(:ids AS text[]))"),
                    {"ids": list(media_effect_ids)},
                )
            connection.execute(
                text("DELETE FROM media_effects WHERE aggregate_id = :aggregate_id"),
                {"aggregate_id": "emby-effect"},
            )
            operation_ids = tuple(
                str(value)
                for value in connection.scalars(
                    text("SELECT id FROM sharing_operations WHERE scope_key LIKE :prefix"),
                    {"prefix": f"watchlist-display:{PREFIX}%"},
                )
            )
            if operation_ids:
                connection.execute(
                    text("DELETE FROM outbox_events WHERE aggregate_id = ANY(CAST(:ids AS text[]))"),
                    {"ids": list(operation_ids)},
                )
            connection.execute(
                text("DELETE FROM sharing_operations WHERE scope_key LIKE :prefix"),
                {"prefix": f"watchlist-display:{PREFIX}%"},
            )
            connection.execute(
                text("DELETE FROM p115_organize_records WHERE tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )
            connection.execute(
                text("DELETE FROM p115_filesystem_cache WHERE id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )
            connection.execute(
                text(
                    "DELETE FROM media_metadata "
                    "WHERE tmdb_id LIKE :prefix OR parent_series_tmdb_id LIKE :prefix "
                    "OR tmdb_id IN ('99119001', '99119002') "
                    "OR parent_series_tmdb_id IN ('99119001', '99119002')"
                ),
                {"prefix": f"{PREFIX}%"},
            )
            connection.execute(
                text(
                    "DELETE FROM subscribe_assistant_snapshots "
                    "WHERE tmdb_id LIKE :prefix OR tmdb_id IN ('99119001', '99119002')"
                ),
                {"prefix": f"{PREFIX}%"},
            )
            connection.execute(
                text(
                    """
                    UPDATE subscribe_assistant_state
                    SET state_json = CASE
                        WHEN state_key = 'subscribes' THEN state_json - '72'
                        WHEN state_key = 'torrents' THEN state_json - 'fixture-download'
                        WHEN state_key = 'snapshot_restore_suppressions'
                            THEN state_json - 'tmdb:99119002:S1' - 'sub:72'
                        ELSE state_json
                    END,
                    updated_at = NOW()
                    WHERE state_key IN ('subscribes', 'torrents', 'snapshot_restore_suppressions')
                    """
                )
            )

    clean()
    try:
        yield
    finally:
        clean()


def _insert_media(runtime: DatabaseRuntime, **values: Any) -> None:
    row = {
        "tmdb_id": "",
        "item_type": "Series",
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "title": None,
        "in_library": False,
        "emby_item_ids_json": [],
        "asset_details_json": None,
        "watching_status": "NONE",
        "force_ended": False,
        "total_episodes": 0,
        "total_episodes_locked": False,
        "subscription_status": "NONE",
        "subscription_sources_json": [],
        "active_washing": False,
    }
    row.update(values)
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                    title, in_library, emby_item_ids_json, asset_details_json,
                    watching_status, force_ended, total_episodes, total_episodes_locked,
                    subscription_status, subscription_sources_json, active_washing
                ) VALUES (
                    :tmdb_id, :item_type, :parent_series_tmdb_id, :season_number, :episode_number,
                    :title, :in_library, CAST(:emby_item_ids_json AS jsonb), CAST(:asset_details_json AS jsonb),
                    :watching_status, :force_ended, :total_episodes, :total_episodes_locked,
                    :subscription_status, CAST(:subscription_sources_json AS jsonb), :active_washing
                )
                """
            ),
            {
                **row,
                "emby_item_ids_json": json.dumps(row["emby_item_ids_json"]),
                "asset_details_json": json.dumps(row["asset_details_json"])
                if row["asset_details_json"] is not None
                else None,
                "subscription_sources_json": json.dumps(row["subscription_sources_json"]),
            },
        )


def _details(series_id: str, *, status: str = "Returning Series") -> dict[str, Any]:
    return {
        "name": "原生追剧",
        "original_name": "Native Watchlist",
        "original_language": "en",
        "overview": "详情",
        "tagline": "标语",
        "first_air_date": "2026-01-02",
        "last_air_date": "2026-09-01",
        "status": status,
        "number_of_episodes": 2,
        "vote_average": 8.5,
        "poster_path": "/poster.jpg",
        "backdrop_path": "/backdrop.jpg",
        "images": {"logos": [{"file_path": "/logo.png"}]},
        "external_ids": {"imdb_id": "tt1234567"},
        "content_ratings": {"results": [{"iso_3166_1": "US", "rating": "TV-14"}]},
        "genres": [{"id": 18, "name": "Drama"}],
        "keywords": {"results": [{"id": 9, "name": "fixture"}]},
        "production_companies": [{"id": 7, "name": "Studio", "logo_path": "/studio.png"}],
        "networks": [{"id": 8, "name": "Network", "logo_path": "/network.png"}],
        "origin_country": ["US"],
        "credits": {"crew": [{"id": 6, "name": "Director", "job": "Director"}]},
        "last_episode_to_air": {"season_number": 1, "episode_number": 2, "air_date": "2026-09-01"},
        "seasons": [
            {
                "id": f"{series_id}:S1",
                "season_number": 1,
                "episode_count": 2,
                "name": "Season 1",
                "overview": "季详情",
                "poster_path": "/season.jpg",
                "air_date": "2026-01-02",
            }
        ],
    }


def _season_details(series_id: str) -> tuple[Mapping[str, Any], ...]:
    return (
        {
            "id": f"{series_id}:S1",
            "season_number": 1,
            "episode_count": 2,
            "episodes": [
                {
                    "id": f"{series_id}:S1:E1",
                    "season_number": 1,
                    "episode_number": 1,
                    "name": "One",
                    "still_path": "/one.jpg",
                    "runtime": 45,
                    "air_date": "2026-01-02",
                },
                {
                    "id": f"{series_id}:S1:E2",
                    "season_number": 1,
                    "episode_number": 2,
                    "name": "Two",
                    "still_path": "/two.jpg",
                    "runtime": 46,
                    "air_date": "2026-09-01",
                },
            ],
        },
    )


def test_repository_publishes_complete_series_season_episode_and_image_projection(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}projection"
    _insert_media(
        p7_runtime,
        tmdb_id=series_id,
        title="旧标题",
        watching_status="Watching",
        in_library=True,
        emby_item_ids_json=["emby-series"],
    )
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1:E1",
        item_type="Episode",
        parent_series_tmdb_id=series_id,
        season_number=1,
        episode_number=1,
        in_library=True,
    )

    repository = SqlAlchemyWatchlistTaskRepository(p7_runtime.unit_of_work)
    assert repository.refresh_series(series_id, _details(series_id), season_details=_season_details(series_id))

    with p7_runtime.engine.connect() as connection:
        series = (
            connection.execute(
                text("SELECT * FROM media_metadata WHERE tmdb_id = :id AND item_type = 'Series'"),
                {"id": series_id},
            )
            .mappings()
            .one()
        )
        season = (
            connection.execute(
                text("SELECT * FROM media_metadata WHERE tmdb_id = :id AND item_type = 'Season'"),
                {"id": f"{series_id}:S1"},
            )
            .mappings()
            .one()
        )
        episode = (
            connection.execute(
                text("SELECT * FROM media_metadata WHERE tmdb_id = :id AND item_type = 'Episode'"),
                {"id": f"{series_id}:S1:E2"},
            )
            .mappings()
            .one()
        )

    assert series["title"] == "原生追剧"
    assert series["original_title"] == "Native Watchlist"
    assert series["poster_path"] == "/poster.jpg"
    assert series["backdrop_path"] == "/backdrop.jpg"
    assert series["logo_path"] == "/logo.png"
    assert series["imdb_id"] == "tt1234567"
    assert series["official_rating_json"] == {"US": "TV-14"}
    assert series["genres_json"] == [{"id": 18, "name": "Drama"}]
    assert series["directors_json"] == [{"id": 6, "name": "Director"}]
    assert series["watching_status"] == "Watching"
    assert season["poster_path"] == "/season.jpg"
    assert season["total_episodes"] == 2
    assert season["watching_status"] == "Watching"
    assert season["watchlist_missing_info_json"] == {"missing_episodes": [2]}
    assert episode["poster_path"] == "/two.jpg"
    assert episode["runtime_minutes"] == 46


def test_completed_series_revival_is_published_atomically(p7_runtime: DatabaseRuntime) -> None:
    series_id = f"{PREFIX}revival"
    _insert_media(p7_runtime, tmdb_id=series_id, watching_status="Completed", in_library=True)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        watching_status="Completed",
        total_episodes=10,
    )
    details = _details(series_id)
    today = datetime.now(UTC).date().isoformat()
    details["seasons"] = [
        {"id": f"{series_id}:S1", "season_number": 1, "episode_count": 10, "air_date": "2025-01-01"},
        {"id": f"{series_id}:S2", "season_number": 2, "episode_count": 8, "air_date": today},
    ]

    repository = SqlAlchemyWatchlistTaskRepository(p7_runtime.unit_of_work)
    assert repository.refresh_series(series_id, details, season_details=())

    with p7_runtime.engine.connect() as connection:
        series_status = connection.scalar(
            text("SELECT watching_status FROM media_metadata WHERE tmdb_id = :id AND item_type = 'Series'"),
            {"id": series_id},
        )
        revived = (
            connection.execute(
                text(
                    """
                SELECT watching_status, subscription_status, subscription_sources_json
                FROM media_metadata
                WHERE tmdb_id = :id AND item_type = 'Season'
                """
                ),
                {"id": f"{series_id}:S2"},
            )
            .mappings()
            .one()
        )
    assert series_status == "Watching"
    assert revived["watching_status"] == "Watching"
    assert revived["subscription_status"] == "WANTED"
    assert revived["subscription_sources_json"] == [
        {"type": "revived_season", "reason": "watchlist_revival", "series_tmdb_id": series_id}
    ]


def test_add_all_import_preserves_existing_user_state_and_recalculates_children(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}import"
    _insert_media(p7_runtime, tmdb_id=series_id, watching_status="Paused", in_library=False)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        total_episodes=1,
        watching_status="Watching",
    )
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1:E1",
        item_type="Episode",
        parent_series_tmdb_id=series_id,
        season_number=1,
        episode_number=1,
        in_library=True,
        watching_status="Watching",
    )

    imported = SqlAlchemyWatchlistTaskRepository(p7_runtime.unit_of_work).import_library_series(
        [{"Type": "Series", "Id": "emby-import", "Name": "Imported", "ProviderIds": {"Tmdb": series_id}}]
    )

    with p7_runtime.engine.connect() as connection:
        rows = {
            row["item_type"]: row
            for row in connection.execute(
                text(
                    """
                    SELECT item_type, watching_status, in_library, emby_item_ids_json
                    FROM media_metadata
                    WHERE tmdb_id = :series OR parent_series_tmdb_id = :series
                    """
                ),
                {"series": series_id},
            ).mappings()
        }
    assert imported == 1
    assert rows["Series"]["watching_status"] == "Paused"
    assert rows["Series"]["emby_item_ids_json"] == ["emby-import"]
    assert rows["Season"]["watching_status"] == "Completed"
    assert rows["Episode"]["watching_status"] == "NONE"


def test_emby_refresh_effect_replay_reuses_the_same_durable_reservation(p7_runtime: DatabaseRuntime) -> None:
    series_id = f"{PREFIX}emby-effect"
    _insert_media(
        p7_runtime,
        tmdb_id=series_id,
        watching_status="Watching",
        in_library=True,
        emby_item_ids_json=["emby-effect"],
    )
    effects = NativeWatchlistTaskEffects(
        p7_runtime.unit_of_work,
        lambda: Settings(
            emby_server_url="http://emby.invalid",
            emby_user_id="user",
            emby_api_key=SecretStr("token"),
        ),
    )

    first = effects.publish_refresh("process-watchlist", series_id, {}, _details(series_id), (), {}, lambda: None)
    second = effects.publish_refresh("process-watchlist", series_id, {}, _details(series_id), (), {}, lambda: None)

    assert first["emby"]["staged"] == 1
    assert second["emby"]["staged"] == 0
    with p7_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM media_effects WHERE aggregate_id = 'emby-effect'")) == 1


class _Settings:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self.values = dict(values)

    def load(self, key: str) -> Any:
        return self.values.get(key)


class _Provider:
    def __init__(self, subscriptions: Sequence[Mapping[str, Any]]) -> None:
        self.subscriptions = tuple(dict(value) for value in subscriptions)

    def list_subscriptions(self) -> MoviePilotListResult:
        return MoviePilotListResult(self.subscriptions)

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        value = next(
            (
                item
                for item in self.subscriptions
                if str(item.get("tmdbid")) == str(tmdb_id) and item.get("season") == season
            ),
            None,
        )
        return MoviePilotQueryResult(
            MoviePilotQueryDisposition.FOUND if value is not None else MoviePilotQueryDisposition.EMPTY,
            value,
        )


def test_moviepilot_status_full_washing_and_snapshot_replay_are_durable(p7_runtime: DatabaseRuntime) -> None:
    series_id = "99119001"
    _insert_media(p7_runtime, tmdb_id=series_id, title="Durable", watching_status="Completed", in_library=True)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        total_episodes=1,
        watching_status="Completed",
    )
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1:E1",
        item_type="Episode",
        parent_series_tmdb_id=series_id,
        season_number=1,
        episode_number=1,
        in_library=True,
        asset_details_json=[{"resolution_display": "2160p", "codec_display": "HEVC"}],
    )
    settings = _Settings(
        {
            "watchlist_config": {"auto_pending": {"enabled": False}},
            "mp_config": {
                "moviepilot_url": "https://moviepilot.invalid",
                "subscribe_assistant": {
                    "enabled": True,
                    "pending_enhanced_enabled": False,
                    "best_version_type": "completed_full",
                },
            },
            "subscription_strategy_config": {"subscription_sources": ["mp"]},
        }
    )
    operation_repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    persistent = PersistentMoviePilotEffectGateway(operation_repository, object(), settings)  # type: ignore[arg-type]
    persistent._identity = lambda: {  # type: ignore[method-assign]
        "task_key": "process-watchlist",
        "job_id": "00000000-0000-0000-0000-000000009001",
        "attempt_id": "00000000-0000-0000-0000-000000009002",
        "worker_id": "watchlist-fixture",
    }
    reservations = []

    def reserve(intent: Any) -> bool:
        reservations.append(operation_repository.reserve(intent))
        return True

    persistent._execute = reserve  # type: ignore[method-assign]
    gateway = NativeWatchlistMoviePilotGateway(
        p7_runtime.unit_of_work,
        settings,
        _Provider([{"id": 71, "tmdbid": int(series_id), "season": 1, "state": "R"}]),
        persistent,
        clock=lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    details = _details(series_id, status="Ended")
    details["seasons"][0]["episode_count"] = 1
    season_details = _season_details(series_id)
    season_details[0]["episodes"] = season_details[0]["episodes"][:1]  # type: ignore[index]

    first = gateway.sync_series(series_id, {}, details, season_details, {}, lambda: None)
    second = gateway.sync_series(series_id, {}, details, season_details, {}, lambda: None)

    assert first["snapshots"] == 1
    assert second["snapshots"] == 0
    assert [reservation.created for reservation in reservations] == [True, True, False, False]
    with p7_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 2
        assert (
            connection.scalar(
                text(
                    "SELECT active_washing FROM media_metadata "
                    "WHERE parent_series_tmdb_id = :id AND item_type = 'Season'"
                ),
                {"id": series_id},
            )
            is True
        )
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM subscribe_assistant_snapshots WHERE tmdb_id = :id"),
                {"id": series_id},
            )
            == 1
        )


class _ConfirmedMoviePilotEffects:
    def __init__(self) -> None:
        self.deleted: list[int] = []

    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool:
        del payload, consume_quota, apply_local_continuation
        return True

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool:
        del tmdb_id, season, status, total_episodes
        return True

    def update_subscription(self, payload: Mapping[str, Any]) -> bool:
        del payload
        return True

    def delete_subscription_by_id(self, subscription_id: int | str) -> bool:
        self.deleted.append(int(subscription_id))
        return True


def test_consistent_episode_washing_delete_closes_local_state_atomically(p7_runtime: DatabaseRuntime) -> None:
    series_id = "99119002"
    subscribe_id = 72
    _insert_media(p7_runtime, tmdb_id=series_id, title="Consistent", watching_status="Completed", in_library=True)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        total_episodes=1,
        watching_status="Completed",
        active_washing=True,
    )
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1:E1",
        item_type="Episode",
        parent_series_tmdb_id=series_id,
        season_number=1,
        episode_number=1,
        in_library=True,
        asset_details_json=[{"resolution_display": "2160p", "codec_display": "HEVC"}],
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES
                    ('subscribes', CAST(:subscriptions AS jsonb), NOW()),
                    ('torrents', CAST(:torrents AS jsonb), NOW())
                ON CONFLICT (state_key) DO UPDATE SET
                    state_json = subscribe_assistant_state.state_json || EXCLUDED.state_json,
                    updated_at = NOW()
                """
            ),
            {
                "subscriptions": json.dumps(
                    {
                        str(subscribe_id): {
                            "subscribe_id": subscribe_id,
                            "tmdb_id": series_id,
                            "season": 1,
                            "full_washing": True,
                            "full_washing_started_at": 1,
                        }
                    }
                ),
                "torrents": json.dumps({"fixture-download": {"subscribe_id": subscribe_id}}),
            },
        )
    settings = _Settings(
        {
            "watchlist_config": {"auto_pending": {"enabled": False}},
            "mp_config": {
                "moviepilot_url": "https://moviepilot.invalid",
                "subscribe_assistant": {
                    "enabled": True,
                    "pending_enhanced_enabled": False,
                    "best_version_type": "tv_episode",
                    "best_version_episode_to_full": False,
                    "best_version_full_consistency_check_enabled": True,
                    "snapshot_retention_days": 30,
                },
            },
            "subscription_strategy_config": {"subscription_sources": ["mp"]},
        }
    )
    effects = _ConfirmedMoviePilotEffects()
    gateway = NativeWatchlistMoviePilotGateway(
        p7_runtime.unit_of_work,
        settings,
        _Provider([{"id": subscribe_id, "tmdbid": int(series_id), "season": 1, "state": "R"}]),
        effects,
        clock=lambda: datetime(2026, 9, 8, tzinfo=UTC),
    )
    details = _details(series_id, status="Ended")
    details["seasons"][0]["episode_count"] = 1
    season_details = _season_details(series_id)
    season_details[0]["episodes"] = season_details[0]["episodes"][:1]  # type: ignore[index]

    result = gateway.sync_series(series_id, {}, details, season_details, {}, lambda: None)

    with p7_runtime.engine.connect() as connection:
        active_washing = connection.scalar(
            text(
                "SELECT active_washing FROM media_metadata WHERE parent_series_tmdb_id = :id AND item_type = 'Season'"
            ),
            {"id": series_id},
        )
        states = {
            row["state_key"]: row["state_json"]
            for row in connection.execute(
                text(
                    "SELECT state_key, state_json FROM subscribe_assistant_state "
                    "WHERE state_key IN ('subscribes', 'torrents', 'snapshot_restore_suppressions')"
                )
            ).mappings()
        }
    subscription = states["subscribes"][str(subscribe_id)]
    assert result["full_washing"] == 0
    assert effects.deleted == [subscribe_id]
    assert active_washing is False
    assert subscription["deleted"] is True
    assert subscription["snapshot_restore_suppressed"] is True
    assert "full_washing" not in subscription
    assert "fixture-download" not in states["torrents"]
    assert {"tmdb:99119002:S1", "sub:72"} <= set(states["snapshot_restore_suppressions"])


def test_shared_display_metadata_replay_reserves_one_operation(p7_runtime: DatabaseRuntime) -> None:
    series_id = f"{PREFIX}sharing"
    _insert_media(p7_runtime, tmdb_id=series_id, title="Shared", watching_status="Watching", in_library=True)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        total_episodes=2,
        total_episodes_locked=True,
        watching_status="Watching",
    )
    gateway = NativeWatchlistSharedMetadataGateway(
        p7_runtime.unit_of_work,
        _Settings(
            {
                "shared_resource_config": {"p115_shared_resource_enabled": True},
                "watchlist_config": {},
            }
        ),
        PostgresSharingOperationRepository(p7_runtime.unit_of_work),
        lambda: "fixture-server-hash",
    )

    first = gateway.stage(
        series_id,
        _details(series_id),
        _season_details(series_id),
        skip_logical_share_dispatch=True,
    )
    second = gateway.stage(
        series_id,
        _details(series_id),
        _season_details(series_id),
        skip_logical_share_dispatch=True,
    )

    assert first["replayed"] is False
    assert second["replayed"] is True
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT kind, request_json FROM sharing_operations WHERE scope_key = :scope"),
                {"scope": f"watchlist-display:{series_id}"},
            )
            .mappings()
            .one()
        )
    assert row["kind"] == "display_metadata"
    assert row["request_json"]["action"] == "upsert_display_metadata"


class _IndeterminateDisplayCenter:
    def __init__(self) -> None:
        self.calls = 0

    def validate_configuration(self) -> None:
        return None

    def upsert_display_metadata(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        assert payload["items"]
        self.calls += 1
        return ProviderResponse(ProviderDisposition.INDETERMINATE, 503, None)


def test_shared_display_metadata_indeterminate_write_is_not_resent(p7_runtime: DatabaseRuntime) -> None:
    series_id = f"{PREFIX}sharing-indeterminate"
    _insert_media(p7_runtime, tmdb_id=series_id, title="Shared", watching_status="Watching", in_library=True)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}:S1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        total_episodes=2,
        watching_status="Watching",
    )
    repository = PostgresSharingOperationRepository(p7_runtime.unit_of_work)
    gateway = NativeWatchlistSharedMetadataGateway(
        p7_runtime.unit_of_work,
        _Settings(
            {
                "shared_resource_config": {"p115_shared_resource_enabled": True},
                "watchlist_config": {},
            }
        ),
        repository,
        lambda: "fixture-server-hash",
    )
    staged = gateway.stage(
        series_id,
        _details(series_id),
        _season_details(series_id),
        skip_logical_share_dispatch=True,
    )
    center = _IndeterminateDisplayCenter()
    executor = SharingOperationExecutor(
        repository,
        SharingProviderOperationDelivery(shared_center=center, hdhive=object()),  # type: ignore[arg-type]
    )

    first = executor.execute(UUID(staged["operation_id"]), worker_id="display-worker", lease_seconds=60)
    second = executor.execute(UUID(staged["operation_id"]), worker_id="display-worker-2", lease_seconds=60)

    assert first is not None and first.operation.status.value == "indeterminate"
    assert second is not None and second.claimed is False
    assert second.operation.status.value == "indeterminate"
    assert center.calls == 1


class _Plan:
    def __init__(self) -> None:
        self.calls: list[Mapping[str, Any]] = []

    def create(self, raw_payload: object) -> Any:
        assert isinstance(raw_payload, Mapping)
        self.calls.append(dict(raw_payload))
        return SimpleNamespace(
            payload={"success": True, "data": {"status": "ready", "plan_id": "plan"}}, status_code=201
        )


class _Execution:
    def prepare(self, plan_id: str, approved_intent_id: str, **kwargs: Any) -> Any:
        del approved_intent_id, kwargs
        return SimpleNamespace(
            created=True,
            execution=SimpleNamespace(execution_id="execution", plan_id=plan_id, job_id="job"),
        )


class _ExecutionRepository:
    def find_by_intent(self, approved_intent_id: str) -> Any:
        del approved_intent_id
        return None


class _CompletedP115Gateway(NativeWatchlistP115ReclassificationGateway):
    def _series_status(self, tmdb_id: str) -> str:
        del tmdb_id
        return "Completed"


def test_p115_reclassification_reads_native_records_and_excludes_mp_direct(p7_runtime: DatabaseRuntime) -> None:
    series_id = f"{PREFIX}p115"
    with p7_runtime.engine.begin() as connection:
        for file_id, name in ((f"{PREFIX}direct", "Direct.S01E01.mkv"), (f"{PREFIX}move", "Move.S01E02.mkv")):
            connection.execute(
                text(
                    """
                    INSERT INTO p115_filesystem_cache (
                        id, parent_id, name, account_id, entry_kind
                    ) VALUES (:id, :parent, :name, '123', 'file')
                    """
                ),
                {"id": file_id, "parent": f"{PREFIX}parent", "name": name},
            )
        connection.execute(
            text(
                """
                INSERT INTO p115_organize_records (
                    file_id, original_name, status, tmdb_id, media_type,
                    target_cid, category_name, season_number
                ) VALUES
                    (:direct, 'Direct.S01E01.mkv', 'success', :tmdb_id, 'tv', '1', 'MP直出', 1),
                    (:move, 'Move.S01E02.mkv', 'success', :tmdb_id, 'tv', '2', '连载', 1)
                """
            ),
            {"direct": f"{PREFIX}direct", "move": f"{PREFIX}move", "tmdb_id": series_id},
        )
    plan = _Plan()
    gateway = _CompletedP115Gateway(
        p7_runtime.unit_of_work,
        plan,
        _Execution(),
        _ExecutionRepository(),
    )

    result = gateway.stage_reclassification(
        series_id,
        {"watching_status": "Watching"},
        {"name": "Fixture"},
        lambda: None,
    )

    assert result["mp_direct_skipped"] == 1
    assert plan.calls[0]["source_file_ids"] == [f"{PREFIX}move"]
