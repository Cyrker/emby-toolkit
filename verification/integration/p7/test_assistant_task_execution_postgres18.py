from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from etk.domains.subscription.assistant import (
    SqlAlchemyAssistantMaintenanceRepository,
    SubscribeAssistantMaintenanceService,
)
from etk.domains.subscription.assistant_task_execution import NativeSubscribeAssistantMaintenanceGateway
from etk.domains.subscription.moviepilot import (
    MoviePilotActiveDownload,
    MoviePilotDownloadSnapshot,
    MoviePilotListResult,
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
)
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore

PREFIX = "p11-native-assistant-"


@pytest.fixture(autouse=True)
def _clean_rows(p7_runtime: DatabaseRuntime) -> Iterator[None]:
    def clean() -> None:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM media_metadata WHERE tmdb_id LIKE :prefix OR parent_series_tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )

    clean()
    try:
        yield
    finally:
        clean()


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        del progress, message
        return None


def _insert_media(runtime: DatabaseRuntime, values: Mapping[str, Any]) -> None:
    row = {
        "tmdb_id": "",
        "item_type": "Series",
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "title": None,
        "in_library": False,
        "emby_item_ids_json": [],
        "watching_status": "NONE",
        "force_ended": False,
        "watchlist_tmdb_status": None,
        "last_air_date": None,
        "last_episode_to_air_json": None,
        "total_episodes": 0,
        "total_episodes_locked": False,
        "subscription_status": "NONE",
        "subscription_sources_json": [],
        "asset_details_json": None,
    }
    row.update(values)
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                    title, in_library, emby_item_ids_json, watching_status, force_ended,
                    watchlist_tmdb_status, last_air_date, last_episode_to_air_json,
                    total_episodes, total_episodes_locked, subscription_status,
                    subscription_sources_json, asset_details_json
                ) VALUES (
                    :tmdb_id, :item_type, :parent_series_tmdb_id, :season_number, :episode_number,
                    :title, :in_library, CAST(:emby_item_ids_json AS jsonb), :watching_status,
                    :force_ended, :watchlist_tmdb_status, :last_air_date,
                    CAST(:last_episode_to_air_json AS jsonb), :total_episodes,
                    :total_episodes_locked, :subscription_status,
                    CAST(:subscription_sources_json AS jsonb), CAST(:asset_details_json AS jsonb)
                )
                """
            ),
            {
                **row,
                "emby_item_ids_json": json.dumps(row["emby_item_ids_json"]),
                "last_episode_to_air_json": json.dumps(row["last_episode_to_air_json"])
                if row["last_episode_to_air_json"] is not None
                else None,
                "subscription_sources_json": json.dumps(row["subscription_sources_json"]),
                "asset_details_json": json.dumps(row["asset_details_json"])
                if row["asset_details_json"] is not None
                else None,
            },
        )


class _EmptyAssistantProvider:
    def list_subscriptions(self) -> MoviePilotListResult:
        return MoviePilotListResult(())

    def list_transfer_history(
        self,
        *,
        status: bool | None = None,
        limit: int = 100,
        page: int = 1,
    ) -> Sequence[Mapping[str, Any]]:
        del status, limit, page
        return ()

    def active_downloads(self) -> Sequence[Any]:
        return ()

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        del tmdb_id, season
        return MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None)


class _AssistantProvider(_EmptyAssistantProvider):
    def __init__(
        self,
        *,
        query: MoviePilotQueryResult | Exception | None = None,
        subscriptions: Sequence[Mapping[str, Any]] = (),
        downloads: MoviePilotDownloadSnapshot | None = None,
    ) -> None:
        self.query = query
        self.subscriptions = tuple(dict(value) for value in subscriptions)
        self.downloads = downloads or MoviePilotDownloadSnapshot((), False)
        self.query_calls: list[tuple[int | str, int | None]] = []

    def list_subscriptions(self) -> MoviePilotListResult:
        return MoviePilotListResult(self.subscriptions)

    def monitor_downloads(self) -> MoviePilotDownloadSnapshot:
        return self.downloads

    def active_downloads(self) -> Sequence[MoviePilotActiveDownload]:
        return self.downloads.downloads

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult | None:
        self.query_calls.append((tmdb_id, season))
        if isinstance(self.query, Exception):
            raise self.query
        return self.query


class _AssistantEffects:
    def __init__(self) -> None:
        self.deleted_subscriptions: list[int | str] = []
        self.deleted_downloads: list[tuple[str, ...]] = []
        self.searches: list[int | str] = []
        self.status_updates: list[tuple[int | str, int | None, str, int | None]] = []
        self.created: list[Mapping[str, Any]] = []

    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool,
    ) -> bool:
        del consume_quota, apply_local_continuation
        self.created.append(dict(payload))
        return True

    def update_subscription(self, payload: Mapping[str, Any]) -> bool:
        del payload
        return True

    def delete_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
        expected_subscription_id: int | str | None = None,
    ) -> bool:
        del tmdb_id, season
        self.deleted_subscriptions.append(expected_subscription_id or "")
        return True

    def delete_subscription_by_id(self, subscription_id: int | str) -> bool:
        self.deleted_subscriptions.append(subscription_id)
        return True

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool:
        self.status_updates.append((tmdb_id, season, status, total_episodes))
        return True

    def search_subscription(self, subscription_id: int | str) -> bool:
        self.searches.append(subscription_id)
        return True

    def delete_download_tasks(
        self,
        hashes: Sequence[str],
        *,
        operation_occurrence: str | None = None,
    ) -> bool:
        del operation_occurrence
        self.deleted_downloads.append(tuple(hashes))
        return True

    def delete_transfer_history(
        self,
        record: Mapping[str, Any],
        *,
        operation_occurrence: str | None = None,
    ) -> bool:
        del record, operation_occurrence
        return True

    def redo_transfer_history(self, history_ids: Sequence[int]) -> bool:
        del history_ids
        return True


def _start_assistant_job(runtime: DatabaseRuntime) -> tuple[PostgresJobStore, UUID, UUID]:
    jobs = PostgresJobStore(runtime.unit_of_work)
    created = jobs.create_job(task_key="subscribe-assistant-maintenance", payload={}, max_attempts=1).projection
    attempt_id = created.current_attempt.id
    assert jobs.claim_attempt(
        job_id=created.job.id,
        attempt_id=attempt_id,
        worker_id="fixture-assistant",
        lease_seconds=60,
    ).claimed
    jobs.start_attempt(
        job_id=created.job.id,
        attempt_id=attempt_id,
        worker_id="fixture-assistant",
        lease_seconds=60,
    )
    return jobs, created.job.id, attempt_id


def _write_assistant_state(runtime: DatabaseRuntime, key: str, value: Mapping[str, Any]) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (:key, CAST(:value AS jsonb), NOW())
                ON CONFLICT (state_key) DO UPDATE SET state_json = EXCLUDED.state_json, updated_at = NOW()
                """
            ),
            {"key": key, "value": json.dumps(dict(value), ensure_ascii=False)},
        )


def _insert_assistant_snapshot(
    runtime: DatabaseRuntime,
    *,
    tmdb_id: str,
    subscribe_id: int = 7,
    scope_total: int = 2,
) -> int:
    with runtime.engine.begin() as connection:
        return int(
            connection.scalar(
                text(
                    """
                    INSERT INTO subscribe_assistant_snapshots (
                        tmdb_id, item_type, season_number, subscribe_id, scope_total,
                        scope_json, subscribe_json, created_at
                    ) VALUES (
                        :tmdb_id, 'Season', 1, :subscribe_id, :scope_total,
                        '{}'::jsonb, '{"type":"电视剧","name":"Fixture"}'::jsonb, NOW() - INTERVAL '2 days'
                    )
                    RETURNING id
                    """
                ),
                {"tmdb_id": tmdb_id, "subscribe_id": subscribe_id, "scope_total": scope_total},
            )
        )


def test_native_assistant_completes_frozen_phases_without_processor(
    p7_runtime: DatabaseRuntime,
) -> None:
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    gateway = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=_EmptyAssistantProvider(),
        configuration=lambda: {
            "full_washing_timeout_hours": 0,
            "download_monitor_enabled": False,
            "verify_enabled": False,
        },
    )
    service = SubscribeAssistantMaintenanceService(
        SqlAlchemyAssistantMaintenanceRepository(p7_runtime.unit_of_work),
        gateway,
    )

    run = service.execute(
        job_id=job_id,
        attempt_id=attempt_id,
        worker_id="fixture-assistant",
        processor=None,
        progress=_Progress(),
    )

    assert run.completed is True
    assert run.completed_phases == (
        "delete_records_cleanup",
        "snapshot_cleanup",
        "washing_timeout",
        "history_reconcile",
        "moviepilot_subscription_sync",
        "no_download_check",
    )
    with p7_runtime.engine.connect() as connection:
        state = connection.execute(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = :state_key"),
            {"state_key": f"maintenance:{job_id}"},
        ).scalar_one()
    assert state["completed"] is True


def test_native_assistant_timeout_without_provider_keeps_snapshot_pending(
    p7_runtime: DatabaseRuntime,
) -> None:
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_snapshots (
                    tmdb_id, item_type, season_number, subscribe_id, scope_total,
                    scope_json, subscribe_json, created_at
                ) VALUES (
                    :tmdb_id, 'Season', 1, 7, 2, '{}'::jsonb,
                    '{\"full_washing_started_at\": 1}'::jsonb,
                    NOW() - INTERVAL '2 days'
                )
                """
            ),
            {"tmdb_id": f"{PREFIX}timeout"},
        )

    gateway = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=None,
        configuration=lambda: {"full_washing_timeout_hours": 1},
    )
    session = gateway.begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")
    assert session.run_phase("washing_timeout", lambda: None) == 0

    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT last_checked_at, subscribe_json
                    FROM subscribe_assistant_snapshots
                    WHERE tmdb_id = :tmdb_id
                    """
                ),
                {"tmdb_id": f"{PREFIX}timeout"},
            )
            .mappings()
            .one()
        )
    assert row["last_checked_at"] is None
    assert row["subscribe_json"] == {"full_washing_started_at": 1}


def test_native_assistant_snapshot_cleanup_preserves_active_claim(
    p7_runtime: DatabaseRuntime,
) -> None:
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    claim_token = uuid4()
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_snapshots (
                    tmdb_id, item_type, scope_total, scope_json, subscribe_json, created_at,
                    job_id, attempt_id, worker_id, claim_token, claimed_at, lease_expires_at
                ) VALUES (
                    :tmdb_id, 'Series', 1, '{}'::jsonb, '{}'::jsonb,
                    NOW() - INTERVAL '2 days', :job_id, :attempt_id, 'fixture-assistant',
                    :claim_token, NOW(), NOW() + INTERVAL '5 minutes'
                )
                """
            ),
            {
                "tmdb_id": f"{PREFIX}claimed",
                "job_id": job_id,
                "attempt_id": attempt_id,
                "claim_token": claim_token,
            },
        )

    gateway = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=None,
        configuration=lambda: {"snapshot_retention_days": 1},
    )
    session = gateway.begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")
    assert session.run_phase("snapshot_cleanup", lambda: None) == 0

    with p7_runtime.engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM subscribe_assistant_snapshots WHERE tmdb_id = :tmdb_id"),
                {"tmdb_id": f"{PREFIX}claimed"},
            ).scalar_one()
            == 1
        )


def test_native_assistant_washing_timeout_closes_consistent_season_without_provider_delete(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}consistent"
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{series_id}:S1",
            "item_type": "Season",
            "parent_series_tmdb_id": series_id,
            "season_number": 1,
            "total_episodes": 2,
        },
    )
    for episode in (1, 2):
        _insert_media(
            p7_runtime,
            {
                "tmdb_id": f"{series_id}:S1:E{episode}",
                "item_type": "Episode",
                "parent_series_tmdb_id": series_id,
                "season_number": 1,
                "episode_number": episode,
                "in_library": True,
                "asset_details_json": [
                    {
                        "resolution_display": "2160p",
                        "codec_display": "HEVC",
                        "release_group_raw": "Fixture",
                        "effect_display": "HDR",
                    }
                ],
            },
        )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE media_metadata SET active_washing = TRUE WHERE tmdb_id = :tmdb_id AND item_type = 'Season'"),
            {"tmdb_id": f"{series_id}:S1"},
        )
    _write_assistant_state(
        p7_runtime,
        "subscribes",
        {
            "7": {
                "subscribe_id": 7,
                "tmdb_id": series_id,
                "season": 1,
                "full_washing": True,
                "full_washing_started_at": 1,
                "subscribe_info": {"total_episode": 2},
            }
        },
    )
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    provider = _AssistantProvider(query=AssertionError("consistent season must not query MoviePilot"))
    effects = _AssistantEffects()
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
        effects=effects,
        configuration=lambda: {"full_washing_timeout_hours": 1},
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    assert session.run_phase("washing_timeout", lambda: None) == 1

    with p7_runtime.engine.connect() as connection:
        state = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        active_washing = connection.scalar(
            text("SELECT active_washing FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Season'"),
            {"tmdb_id": f"{series_id}:S1"},
        )
    assert "full_washing" not in state["7"]
    assert state["7"].get("deleted") is not True
    assert active_washing is False
    assert provider.query_calls == []
    assert effects.deleted_subscriptions == []


def test_native_assistant_history_sync_preserves_manual_subscription_states(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}paused"
    _insert_media(
        p7_runtime,
        {
            "tmdb_id": f"{series_id}:S1",
            "item_type": "Season",
            "parent_series_tmdb_id": series_id,
            "season_number": 1,
            "subscription_status": "PAUSED",
        },
    )
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    provider = _AssistantProvider(
        subscriptions=({"id": 7, "tmdbid": series_id, "season": 1, "type": "电视剧", "state": "R"},)
    )
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    session.run_phase("moviepilot_subscription_sync", lambda: None)

    with p7_runtime.engine.connect() as connection:
        status = connection.scalar(
            text("SELECT subscription_status FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Season'"),
            {"tmdb_id": f"{series_id}:S1"},
        )
    assert status == "PAUSED"


def test_native_assistant_washing_timeout_reserves_cycle_and_deletes_matching_provider_subscription(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"{PREFIX}washing-timeout"
    _write_assistant_state(
        p7_runtime,
        "subscribes",
        {
            "7": {
                "subscribe_id": 7,
                "tmdb_id": tmdb_id,
                "season": 1,
                "full_washing": True,
                "full_washing_started_at": 1,
                "subscribe_info": {"total_episode": 2},
            }
        },
    )
    provider = _AssistantProvider(
        query=MoviePilotQueryResult(
            MoviePilotQueryDisposition.FOUND,
            {"id": 7, "tmdbid": tmdb_id, "season": 1, "state": "R"},
        )
    )
    effects = _AssistantEffects()
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
        effects=effects,
        configuration=lambda: {"full_washing_timeout_hours": 1, "snapshot_retention_days": 7},
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    assert session.run_phase("washing_timeout", lambda: None) == 1

    with p7_runtime.engine.connect() as connection:
        subscription = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )["7"]
        suppressions = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'snapshot_restore_suppressions'")
        )
    assert effects.deleted_subscriptions == [7]
    assert subscription["deleted"] is True
    assert "washing_timeout_cycle" not in subscription
    assert {f"tmdb:{tmdb_id}:S1", "sub:7"} <= set(suppressions)
    assert all(value["reason"] == "full_washing_timeout" for value in suppressions.values())


def test_native_assistant_download_monitor_deletes_stalled_task_and_replay_uses_receipt(
    p7_runtime: DatabaseRuntime,
) -> None:
    task = {
        "hash": "stalled-hash",
        "subscribe_id": 7,
        "tmdb_id": f"{PREFIX}download",
        "season": 1,
        "episodes": [1],
        "title": "Fixture",
        "baseline_progress": 10,
        "baseline_at": 1,
        "retry_count": 0,
        "time": 1,
    }
    _write_assistant_state(p7_runtime, "torrents", {"stalled-hash": task})
    _write_assistant_state(
        p7_runtime,
        "subscribes",
        {"7": {"subscribe_id": 7, "active_sources": {"download_pending": "pending"}}},
    )
    downloads = MoviePilotDownloadSnapshot(
        (
            MoviePilotActiveDownload(
                "stalled-hash",
                7,
                f"{PREFIX}download",
                1,
                progress=10,
                state="downloading",
            ),
        ),
        True,
    )
    provider = _AssistantProvider(downloads=downloads)
    effects = _AssistantEffects()
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
        effects=effects,
        configuration=lambda: {
            "download_timeout_minutes": 1,
            "download_progress_threshold": 10,
            "download_retry_limit": 3,
            "auto_search_when_delete": True,
            "skip_deletion": True,
        },
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    assert session.run_phase("download_check", lambda: None) == 1
    assert effects.deleted_downloads == [("stalled-hash",), ("stalled-hash",)]
    assert effects.searches == [7]

    _write_assistant_state(p7_runtime, "torrents", {"stalled-hash": task})
    assert session.run_phase("download_check", lambda: None) == 1
    assert effects.deleted_downloads == [("stalled-hash",), ("stalled-hash",)]
    assert effects.searches == [7]
    with p7_runtime.engine.connect() as connection:
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
        subscriptions = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        delete_records = connection.scalar(text("SELECT COUNT(*) FROM subscribe_assistant_delete_records"))
    assert torrents == {}
    assert "download_pending" not in subscriptions["7"]["active_sources"]
    assert delete_records == 1


def test_native_assistant_no_download_action_records_expected_provider_update(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"{PREFIX}no-download"
    _write_assistant_state(
        p7_runtime,
        "subscribes",
        {
            "7": {
                "subscribe_id": 7,
                "tmdb_id": tmdb_id,
                "season": 1,
                "created_at": 1,
                "mp_state": "R",
                "subscribe_info": {"state": "R", "total_episode": 12},
            }
        },
    )
    effects = _AssistantEffects()
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=_AssistantProvider(),
        effects=effects,
        configuration=lambda: {"tv_no_download_days": 1, "no_download_actions": ["search", "pending"]},
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    assert session.run_phase("no_download_check", lambda: None) == 1
    assert session.run_phase("no_download_check", lambda: None) == 0

    with p7_runtime.engine.connect() as connection:
        subscription = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )["7"]
    assert effects.searches == [7]
    assert effects.status_updates == [(tmdb_id, 1, "P", None)]
    assert subscription["mp_state"] == "P"
    assert subscription["expected_mp_update"]["fields"] == ["state"]
    assert subscription["expected_mp_update"]["state"] == "P"


def test_native_assistant_partial_downloader_snapshot_never_treats_absence_as_manual_delete(
    p7_runtime: DatabaseRuntime,
) -> None:
    task = {
        "hash": "preserved-hash",
        "subscribe_id": 7,
        "baseline_progress": 0,
        "baseline_at": 1,
        "time": 1,
    }
    _write_assistant_state(p7_runtime, "torrents", {"preserved-hash": task})
    provider = _AssistantProvider(
        downloads=MoviePilotDownloadSnapshot(
            (MoviePilotActiveDownload("other-hash", None, None, None, moviepilot_active=False),),
            False,
        )
    )
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
        effects=_AssistantEffects(),
        configuration=lambda: {"manual_delete_listen": True},
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    assert session.run_phase("download_check", lambda: None) == 0

    with p7_runtime.engine.connect() as connection:
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    assert torrents == {"preserved-hash": task}


def test_native_assistant_snapshot_query_failure_releases_claim_without_marking_checked(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"{PREFIX}query-failure"
    snapshot_id = _insert_assistant_snapshot(p7_runtime, tmdb_id=tmdb_id)
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=_AssistantProvider(query=RuntimeError("fixture provider failure")),
        effects=_AssistantEffects(),
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    with pytest.raises(RuntimeError, match="fixture provider failure"):
        session.run_phase("snapshot_verify", lambda: None)

    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT last_checked_at, claim_token, subscribe_json "
                    "FROM subscribe_assistant_snapshots WHERE id = :id"
                ),
                {"id": snapshot_id},
            )
            .mappings()
            .one()
        )
    assert row["last_checked_at"] is None
    assert row["claim_token"] is None
    assert row["subscribe_json"]["last_verify_error"] == "RuntimeError"


def test_native_assistant_snapshot_suppression_skips_provider_restore(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"{PREFIX}suppressed"
    snapshot_id = _insert_assistant_snapshot(p7_runtime, tmdb_id=tmdb_id)
    _write_assistant_state(
        p7_runtime,
        "snapshot_restore_suppressions",
        {f"tmdb:{tmdb_id}:S1": {"expires_at": 4_102_444_800}},
    )
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    provider = _AssistantProvider(query=AssertionError("suppressed snapshot must not query MoviePilot"))
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
        effects=_AssistantEffects(),
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    assert session.run_phase("snapshot_verify", lambda: None) == 1

    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT last_checked_at, subscribe_json FROM subscribe_assistant_snapshots WHERE id = :id"),
                {"id": snapshot_id},
            )
            .mappings()
            .one()
        )
    assert row["last_checked_at"] is not None
    assert row["subscribe_json"]["last_verify_skipped"] == "restore_suppressed"
    assert provider.query_calls == []


def test_native_assistant_snapshot_claim_loss_fails_closed_before_completion(
    p7_runtime: DatabaseRuntime,
) -> None:
    tmdb_id = f"{PREFIX}claim-loss"
    snapshot_id = _insert_assistant_snapshot(p7_runtime, tmdb_id=tmdb_id, scope_total=3)

    class ClaimStealingEffects(_AssistantEffects):
        def update_subscription_status(
            self,
            tmdb_id: int | str,
            *,
            season: int | None,
            status: str,
            total_episodes: int | None,
        ) -> bool:
            result = super().update_subscription_status(
                tmdb_id,
                season=season,
                status=status,
                total_episodes=total_episodes,
            )
            with p7_runtime.engine.begin() as connection:
                connection.execute(
                    text("UPDATE subscribe_assistant_snapshots SET claim_token = :replacement WHERE id = :id"),
                    {"replacement": uuid4(), "id": snapshot_id},
                )
            return result

    provider = _AssistantProvider(
        query=MoviePilotQueryResult(
            MoviePilotQueryDisposition.FOUND,
            {"id": 7, "tmdbid": tmdb_id, "season": 1, "state": "R", "total_episode": 1},
        )
    )
    _jobs, job_id, attempt_id = _start_assistant_job(p7_runtime)
    session = NativeSubscribeAssistantMaintenanceGateway(
        p7_runtime.unit_of_work,
        provider=provider,
        effects=ClaimStealingEffects(),
        configuration=lambda: {"auto_search_when_delete": False},
    ).begin(None, job_id=job_id, attempt_id=attempt_id, worker_id="fixture-assistant")

    with pytest.raises(RuntimeError, match="snapshot claim was lost"):
        session.run_phase("snapshot_verify", lambda: None)

    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT last_checked_at, claim_token FROM subscribe_assistant_snapshots WHERE id = :id"),
                {"id": snapshot_id},
            )
            .mappings()
            .one()
        )
    assert row["last_checked_at"] is None
    assert row["claim_token"] is not None
