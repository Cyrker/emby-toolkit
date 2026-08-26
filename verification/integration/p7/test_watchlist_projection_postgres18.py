from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.watchlist_projection import SqlAlchemyWatchlistMediaProjectionRepository
from etk.platform.db import DatabaseRuntime

PREFIX = "p7-watchlist-"


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyWatchlistMediaProjectionRepository:
    return SqlAlchemyWatchlistMediaProjectionRepository(runtime.unit_of_work)


@pytest.fixture(autouse=True)
def _clean_projection_rows(p7_runtime: DatabaseRuntime) -> Iterator[None]:
    def clean() -> None:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM media_metadata
                    WHERE tmdb_id LIKE :prefix
                       OR parent_series_tmdb_id LIKE :prefix
                    """
                ),
                {"prefix": f"{PREFIX}%"},
            )

    clean()
    try:
        yield
    finally:
        clean()


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False)


def _insert_media(runtime: DatabaseRuntime, **values: Any) -> None:
    row: dict[str, Any] = {
        "tmdb_id": "",
        "item_type": "Series",
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "title": None,
        "release_date": None,
        "in_library": False,
        "emby_item_ids_json": [],
        "first_requested_at": None,
        "watching_status": "NONE",
        "paused_until": None,
        "force_ended": False,
        "watchlist_last_checked_at": None,
        "watchlist_tmdb_status": None,
        "watchlist_next_episode_json": None,
        "watchlist_missing_info_json": None,
        "total_episodes": 0,
        "total_episodes_locked": False,
        "subscription_status": "NONE",
        "subscription_sources_json": [],
        "ignore_reason": None,
    }
    row.update(values)
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id,
                    item_type,
                    parent_series_tmdb_id,
                    season_number,
                    episode_number,
                    title,
                    release_date,
                    in_library,
                    emby_item_ids_json,
                    first_requested_at,
                    watching_status,
                    paused_until,
                    force_ended,
                    watchlist_last_checked_at,
                    watchlist_tmdb_status,
                    watchlist_next_episode_json,
                    watchlist_missing_info_json,
                    total_episodes,
                    total_episodes_locked,
                    subscription_status,
                    subscription_sources_json,
                    ignore_reason
                ) VALUES (
                    :tmdb_id,
                    :item_type,
                    :parent_series_tmdb_id,
                    :season_number,
                    :episode_number,
                    :title,
                    :release_date,
                    :in_library,
                    CAST(:emby_item_ids_json AS jsonb),
                    :first_requested_at,
                    :watching_status,
                    :paused_until,
                    :force_ended,
                    :watchlist_last_checked_at,
                    :watchlist_tmdb_status,
                    CAST(:watchlist_next_episode_json AS jsonb),
                    CAST(:watchlist_missing_info_json AS jsonb),
                    :total_episodes,
                    :total_episodes_locked,
                    :subscription_status,
                    CAST(:subscription_sources_json AS jsonb),
                    :ignore_reason
                )
                """
            ),
            {
                **row,
                "emby_item_ids_json": _json(row["emby_item_ids_json"]),
                "watchlist_next_episode_json": _json(row["watchlist_next_episode_json"]),
                "watchlist_missing_info_json": _json(row["watchlist_missing_info_json"]),
                "subscription_sources_json": _json(row["subscription_sources_json"]),
            },
        )


def _insert_episode(
    runtime: DatabaseRuntime,
    *,
    parent: str,
    season_number: int,
    suffix: str,
    in_library: bool,
    watching_status: str = "Watching",
) -> None:
    _insert_media(
        runtime,
        tmdb_id=f"{parent}-e{season_number}-{suffix}",
        item_type="Episode",
        parent_series_tmdb_id=parent,
        season_number=season_number,
        episode_number=int(suffix) if suffix.isdigit() else 1,
        title=f"Episode {suffix}",
        in_library=in_library,
        watching_status=watching_status,
    )


def _projection_rows(runtime: DatabaseRuntime) -> dict[tuple[str, str], Mapping[str, Any]]:
    with runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT *
                    FROM media_metadata
                    WHERE tmdb_id LIKE :prefix
                       OR parent_series_tmdb_id LIKE :prefix
                    """
                ),
                {"prefix": f"{PREFIX}%"},
            )
            .mappings()
            .all()
        )
    return {(str(row["tmdb_id"]), str(row["item_type"])): dict(row) for row in rows}


def _dirty_watchlist_values() -> dict[str, Any]:
    return {
        "watching_status": "Paused",
        "paused_until": date(2026, 9, 30),
        "force_ended": True,
        "watchlist_last_checked_at": datetime(2026, 8, 20, tzinfo=UTC),
        "watchlist_tmdb_status": "Returning Series",
        "watchlist_next_episode_json": {"episode_number": 9},
        "watchlist_missing_info_json": {"seasons_with_gaps": [1]},
        "subscription_status": "SUBSCRIBED",
        "subscription_sources_json": [{"source": "fixture"}],
        "ignore_reason": "fixture-ignore",
    }


def _assert_watchlist_reset(row: Mapping[str, Any]) -> None:
    assert row["watching_status"] == "NONE"
    assert row["paused_until"] is None
    assert row["force_ended"] is False
    assert row["watchlist_last_checked_at"] is None
    assert row["watchlist_tmdb_status"] is None
    assert row["watchlist_next_episode_json"] is None
    assert row["watchlist_missing_info_json"] is None
    assert row["subscription_status"] == "NONE"
    assert row["subscription_sources_json"] == []
    assert row["ignore_reason"] is None


def test_pg18_list_projects_legacy_seasons_filters_counts_and_order(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_a = f"{PREFIX}list-a"
    series_b = f"{PREFIX}list-b"
    inactive = f"{PREFIX}list-inactive"
    checked_at = datetime(2026, 8, 25, 12, 30, tzinfo=UTC)
    _insert_media(
        p7_runtime,
        tmdb_id=series_a,
        title="Alpha",
        watching_status="Watching",
        first_requested_at=datetime(2026, 8, 20, tzinfo=UTC),
        watchlist_last_checked_at=checked_at,
        watchlist_tmdb_status="Returning Series",
        watchlist_next_episode_json={"episode_number": 4},
        watchlist_missing_info_json={"seasons_with_gaps": [1]},
        emby_item_ids_json=["alpha-series"],
        total_episodes=30,
    )
    _insert_media(
        p7_runtime,
        tmdb_id=series_b,
        title="Beta",
        watching_status="Paused",
        first_requested_at=datetime(2026, 8, 24, tzinfo=UTC),
        total_episodes=20,
    )
    _insert_media(
        p7_runtime,
        tmdb_id=inactive,
        title="Inactive",
        watching_status="NONE",
        first_requested_at=datetime(2026, 8, 26, tzinfo=UTC),
    )

    seasons = (
        (f"{series_a}-s0", series_a, 0, "Watching", False, False, 1),
        (f"{series_a}-s1", series_a, 1, "Watching", False, False, 3),
        (f"{series_a}-s2", series_a, 2, "Completed", False, False, 1),
        (f"{series_a}-s3", series_a, 3, "Completed", False, True, 1),
        (f"{series_b}-s1", series_b, 1, "Completed", True, False, 1),
        (f"{series_b}-s2", series_b, 2, "Watching", False, False, 1),
        (f"{inactive}-s1", inactive, 1, "Watching", True, False, 1),
    )
    for tmdb_id, parent, number, status, in_library, forced, total in seasons:
        _insert_media(
            p7_runtime,
            tmdb_id=tmdb_id,
            item_type="Season",
            parent_series_tmdb_id=parent,
            season_number=number,
            title=f"Season {number}",
            release_date=date(2026, max(number, 1), 1),
            in_library=in_library,
            watching_status=status,
            force_ended=forced,
            total_episodes=total,
            total_episodes_locked=number == 1,
        )

    _insert_episode(p7_runtime, parent=series_a, season_number=0, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_a, season_number=1, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_a, season_number=1, suffix="2", in_library=True)
    _insert_episode(p7_runtime, parent=series_a, season_number=2, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_a, season_number=3, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_b, season_number=1, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_b, season_number=2, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=inactive, season_number=1, suffix="1", in_library=True)

    items = [dict(item) for item in _repository(p7_runtime).list_watchlist_items()]

    assert [item["tmdb_id"] for item in items] == [
        f"{series_b}-s1",
        f"{series_b}-s2",
        f"{series_a}-s1",
        f"{series_a}-s3",
    ]
    beta_first = items[0]
    assert beta_first == {
        "tmdb_id": f"{series_b}-s1",
        "item_type": "Season",
        "item_name": "Beta 第 1 季",
        "season_number": 1,
        "parent_tmdb_id": series_b,
        "release_year": date(2026, 1, 1),
        "status": "Completed",
        "series_status": "Paused",
        "force_ended": False,
        "last_checked_at": None,
        "next_episode_to_air_json": None,
        "missing_info_json": None,
        "emby_item_ids_json": [],
        "tmdb_status": None,
        "collected_count": 1,
        "total_count": 1,
        "series_collected_count": 2,
        "series_total_episodes": 20,
        "total_episodes_locked": True,
    }
    alpha_first = items[2]
    assert alpha_first["next_episode_to_air_json"] == {"episode_number": 4}
    assert alpha_first["missing_info_json"] == {"seasons_with_gaps": [1]}
    assert alpha_first["last_checked_at"] == checked_at
    assert alpha_first["emby_item_ids_json"] == ["alpha-series"]
    assert alpha_first["collected_count"] == 2
    assert alpha_first["total_count"] == 3
    assert alpha_first["series_collected_count"] == 5


def test_pg18_add_upserts_series_resets_episodes_and_synchronizes_seasons(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}add"
    _insert_media(
        p7_runtime,
        tmdb_id=series_id,
        title="Original title",
        watching_status="Completed",
        force_ended=True,
        paused_until=date(2026, 9, 1),
    )
    for number, forced, total, initial in (
        (0, False, 1, "Paused"),
        (1, True, 10, "Paused"),
        (2, False, 2, "Paused"),
        (3, False, 2, "Completed"),
    ):
        _insert_media(
            p7_runtime,
            tmdb_id=f"{series_id}-s{number}",
            item_type="Season",
            parent_series_tmdb_id=series_id,
            season_number=number,
            watching_status=initial,
            force_ended=forced,
            total_episodes=total,
        )
    _insert_episode(p7_runtime, parent=series_id, season_number=0, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_id, season_number=2, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_id, season_number=2, suffix="2", in_library=True)
    _insert_episode(p7_runtime, parent=series_id, season_number=3, suffix="1", in_library=True)
    _insert_episode(p7_runtime, parent=series_id, season_number=3, suffix="2", in_library=False)

    repository = _repository(p7_runtime)
    repository.add_watchlist_item(series_id, "Replacement title")
    inserted_id = f"{PREFIX}add-new"
    repository.add_watchlist_item(inserted_id, "Inserted title")
    rows = _projection_rows(p7_runtime)

    series = rows[(series_id, "Series")]
    assert series["title"] == "Original title"
    assert series["watching_status"] == "NONE"
    assert series["force_ended"] is False
    assert series["paused_until"] is None
    assert rows[(inserted_id, "Series")]["title"] == "Inserted title"
    assert rows[(inserted_id, "Series")]["watching_status"] == "NONE"
    assert rows[(f"{series_id}-s0", "Season")]["watching_status"] == "Paused"
    assert rows[(f"{series_id}-s1", "Season")]["watching_status"] == "Completed"
    assert rows[(f"{series_id}-s2", "Season")]["watching_status"] == "Completed"
    assert rows[(f"{series_id}-s3", "Season")]["watching_status"] == "Watching"
    episode_rows = [
        row
        for (tmdb_id, item_type), row in rows.items()
        if item_type == "Episode" and tmdb_id.startswith(f"{series_id}-e")
    ]
    assert episode_rows
    assert {row["watching_status"] for row in episode_rows} == {"NONE"}


def test_pg18_single_status_watching_and_pending_clear_series_flags(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}status"
    _insert_media(p7_runtime, tmdb_id=series_id, title="Status", **_dirty_watchlist_values())
    repository = _repository(p7_runtime)

    assert repository.update_watchlist_status(series_id, "Watching") is True
    first = _projection_rows(p7_runtime)[(series_id, "Series")]
    assert first["watching_status"] == "Watching"
    assert first["force_ended"] is False
    assert first["paused_until"] is None
    assert first["watchlist_last_checked_at"] is not None

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET force_ended = TRUE,
                    paused_until = DATE '2026-10-01'
                WHERE tmdb_id = :tmdb_id
                  AND item_type = 'Series'
                """
            ),
            {"tmdb_id": series_id},
        )
    assert repository.update_watchlist_status(series_id, "Pending") is True
    second = _projection_rows(p7_runtime)[(series_id, "Series")]
    assert second["watching_status"] == "Pending"
    assert second["force_ended"] is False
    assert second["paused_until"] is None
    assert repository.update_watchlist_status(f"{PREFIX}missing", "Watching") is False


def test_pg18_single_remove_resets_series_and_all_children(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_id = f"{PREFIX}remove"
    dirty = _dirty_watchlist_values()
    _insert_media(p7_runtime, tmdb_id=series_id, title="Remove", **dirty)
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}-s1",
        item_type="Season",
        parent_series_tmdb_id=series_id,
        season_number=1,
        **dirty,
    )
    _insert_media(
        p7_runtime,
        tmdb_id=f"{series_id}-e1",
        item_type="Episode",
        parent_series_tmdb_id=series_id,
        season_number=1,
        episode_number=1,
        **dirty,
    )

    repository = _repository(p7_runtime)
    assert repository.remove_watchlist_item(series_id) is True
    rows = _projection_rows(p7_runtime)
    for key in (
        (series_id, "Series"),
        (f"{series_id}-s1", "Season"),
        (f"{series_id}-e1", "Episode"),
    ):
        _assert_watchlist_reset(rows[key])
    assert repository.remove_watchlist_item(f"{PREFIX}missing") is False


def test_pg18_force_end_updates_only_positive_seasons(p7_runtime: DatabaseRuntime) -> None:
    target = f"{PREFIX}force-target"
    parent = f"{PREFIX}force-parent"
    special = f"{PREFIX}force-special"
    _insert_media(p7_runtime, tmdb_id=target, item_type="Series", watching_status="Paused")
    _insert_media(
        p7_runtime,
        tmdb_id=target,
        item_type="Season",
        parent_series_tmdb_id=parent,
        season_number=1,
        watching_status="Paused",
        paused_until=date(2026, 9, 1),
    )
    _insert_media(
        p7_runtime,
        tmdb_id=target,
        item_type="Episode",
        parent_series_tmdb_id=parent,
        season_number=1,
        episode_number=1,
        watching_status="Paused",
    )
    _insert_media(
        p7_runtime,
        tmdb_id=special,
        item_type="Season",
        parent_series_tmdb_id=parent,
        season_number=0,
        watching_status="Paused",
    )

    assert _repository(p7_runtime).force_end_watchlist_seasons([target, special]) == 1
    rows = _projection_rows(p7_runtime)
    season = rows[(target, "Season")]
    assert season["watching_status"] == "Completed"
    assert season["force_ended"] is True
    assert season["paused_until"] is None
    assert season["watchlist_last_checked_at"] is not None
    assert rows[(target, "Series")]["watching_status"] == "Paused"
    assert rows[(target, "Episode")]["watching_status"] == "Paused"
    assert rows[(special, "Season")]["watching_status"] == "Paused"


def test_pg18_batch_watching_resets_series_and_only_forced_positive_seasons(
    p7_runtime: DatabaseRuntime,
) -> None:
    first = f"{PREFIX}batch-status-a"
    second = f"{PREFIX}batch-status-b"
    for series_id in (first, second):
        _insert_media(p7_runtime, tmdb_id=series_id, title=series_id, **_dirty_watchlist_values())
    forced = f"{first}-forced"
    regular = f"{first}-regular"
    special = f"{second}-special"
    episode = f"{second}-episode"
    _insert_media(
        p7_runtime,
        tmdb_id=forced,
        item_type="Season",
        parent_series_tmdb_id=first,
        season_number=1,
        **_dirty_watchlist_values(),
    )
    _insert_media(
        p7_runtime,
        tmdb_id=regular,
        item_type="Season",
        parent_series_tmdb_id=first,
        season_number=2,
        **{**_dirty_watchlist_values(), "force_ended": False},
    )
    _insert_media(
        p7_runtime,
        tmdb_id=special,
        item_type="Season",
        parent_series_tmdb_id=second,
        season_number=0,
        **_dirty_watchlist_values(),
    )
    _insert_media(
        p7_runtime,
        tmdb_id=episode,
        item_type="Episode",
        parent_series_tmdb_id=second,
        season_number=1,
        episode_number=1,
        **_dirty_watchlist_values(),
    )

    assert _repository(p7_runtime).batch_update_watchlist_status([first, second], "Watching") == 3
    rows = _projection_rows(p7_runtime)
    for series_id in (first, second):
        series = rows[(series_id, "Series")]
        assert series["watching_status"] == "Watching"
        assert series["force_ended"] is False
        assert series["paused_until"] is None
        assert series["watchlist_last_checked_at"] is not None
    restored = rows[(forced, "Season")]
    assert restored["watching_status"] == "Watching"
    assert restored["force_ended"] is False
    assert restored["paused_until"] is None
    assert rows[(regular, "Season")]["watching_status"] == "Paused"
    assert rows[(special, "Season")]["force_ended"] is True
    assert rows[(episode, "Episode")]["watching_status"] == "Paused"


def test_pg18_batch_remove_resets_two_series_and_every_child(
    p7_runtime: DatabaseRuntime,
) -> None:
    series_ids = (f"{PREFIX}batch-remove-a", f"{PREFIX}batch-remove-b")
    dirty = _dirty_watchlist_values()
    expected_keys: list[tuple[str, str]] = []
    for index, series_id in enumerate(series_ids, start=1):
        rows = (
            (series_id, "Series", None, None),
            (f"{series_id}-s1", "Season", 1, None),
            (f"{series_id}-e1", "Episode", 1, 1),
        )
        for tmdb_id, item_type, season_number, episode_number in rows:
            _insert_media(
                p7_runtime,
                tmdb_id=tmdb_id,
                item_type=item_type,
                parent_series_tmdb_id=None if item_type == "Series" else series_id,
                season_number=season_number,
                episode_number=episode_number,
                title=f"Batch {index}",
                **dirty,
            )
            expected_keys.append((tmdb_id, item_type))
    unrelated = f"{PREFIX}batch-remove-unrelated"
    _insert_media(p7_runtime, tmdb_id=unrelated, title="Unrelated", **dirty)

    assert _repository(p7_runtime).batch_remove_watchlist_items(series_ids) == 6
    stored = _projection_rows(p7_runtime)
    for key in expected_keys:
        _assert_watchlist_reset(stored[key])
    assert stored[(unrelated, "Series")]["watching_status"] == "Paused"


def test_pg18_total_episode_update_sets_value_and_lock(p7_runtime: DatabaseRuntime) -> None:
    season_id = f"{PREFIX}total-s1"
    _insert_media(
        p7_runtime,
        tmdb_id=season_id,
        item_type="Season",
        parent_series_tmdb_id=f"{PREFIX}total",
        season_number=1,
        total_episodes=0,
        total_episodes_locked=False,
    )

    _repository(p7_runtime).update_watchlist_total_episodes(season_id, 12, "Season")
    season = _projection_rows(p7_runtime)[(season_id, "Season")]
    assert season["total_episodes"] == 12
    assert season["total_episodes_locked"] is True


def test_pg18_delete_context_normalizes_emby_ids_and_item_name_lookup(
    p7_runtime: DatabaseRuntime,
) -> None:
    list_id = f"{PREFIX}context-list"
    scalar_id = f"{PREFIX}context-scalar"
    empty_id = f"{PREFIX}context-empty"
    _insert_media(
        p7_runtime,
        tmdb_id=list_id,
        title="Fixture Series",
        emby_item_ids_json=[" emby-1 ", "", "emby-2"],
    )
    _insert_media(
        p7_runtime,
        tmdb_id=scalar_id,
        title=None,
        emby_item_ids_json=" scalar-id ",
    )
    _insert_media(p7_runtime, tmdb_id=empty_id, title="Empty", emby_item_ids_json=[])
    repository = _repository(p7_runtime)

    assert repository.watchlist_delete_context(list_id) == {
        "tmdb_id": list_id,
        "title": "Fixture Series",
        "emby_ids": ["emby-1", "emby-2"],
    }
    assert repository.watchlist_delete_context(scalar_id) == {
        "tmdb_id": scalar_id,
        "title": scalar_id,
        "emby_ids": ["scalar-id"],
    }
    assert repository.watchlist_delete_context(empty_id) == {
        "tmdb_id": empty_id,
        "title": "Empty",
        "emby_ids": [],
    }
    assert repository.watchlist_delete_context(f"{PREFIX}missing") is None
    assert repository.watchlist_item_name(list_id) == "Fixture Series"
    assert repository.watchlist_item_name(f"{PREFIX}missing") is None
