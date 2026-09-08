from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy import text

from etk.domains.p115.deep_delete import SqlAlchemyP115DeepDeleteSnapshotRepository
from etk.domains.p115.deep_delete_adapters import (
    NativeP115MediaCleanupReceiptGateway,
    SqlAlchemyP115DeepDeleteScopeGateway,
)
from etk.domains.subscription.cleanup_adapters import SqlAlchemyCleanupMediaGateway
from etk.platform.db import DatabaseRuntime

ACCOUNT_ID = "115001"
FINGERPRINT = "f" * 64


class _ScopeProvider:
    def __init__(self) -> None:
        self.open_calls: list[dict[str, Any]] = []

    def open(self, **options: Any) -> Any:
        self.open_calls.append(dict(options))
        return SimpleNamespace(
            account_id=ACCOUNT_ID,
            backend="openapi",
            credential_fingerprint=FINGERPRINT,
        )


def _scope(runtime: DatabaseRuntime) -> tuple[SqlAlchemyP115DeepDeleteScopeGateway, _ScopeProvider]:
    credentials = SimpleNamespace(load=lambda: SimpleNamespace(account_id=ACCOUNT_ID))
    provider = _ScopeProvider()
    return (
        SqlAlchemyP115DeepDeleteScopeGateway(
            runtime.unit_of_work,
            cast(Any, credentials),
            cast(Any, provider),
        ),
        provider,
    )


def _insert_media(
    runtime: DatabaseRuntime,
    *,
    tmdb_id: str,
    item_type: str,
    emby_id: str,
    pick_code: str = "",
    sha1: str = "",
    series_id: str = "",
    season_number: int | None = None,
    episode_number: int | None = None,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library,
                    parent_series_tmdb_id, season_number, episode_number,
                    emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                ) VALUES (
                    :tmdb_id, :item_type, :title, TRUE,
                    NULLIF(:series_id, ''), :season_number, :episode_number,
                    CAST(:emby_ids AS jsonb), CAST(:assets AS jsonb),
                    CAST(:sha1s AS jsonb), CAST(:pick_codes AS jsonb)
                )
                """
            ),
            {
                "tmdb_id": tmdb_id,
                "item_type": item_type,
                "title": f"{item_type} {tmdb_id}",
                "series_id": series_id,
                "season_number": season_number,
                "episode_number": episode_number,
                "emby_ids": json.dumps([emby_id]),
                "assets": json.dumps([{"emby_item_id": emby_id, "path": f"/fixture/{tmdb_id}.strm"}]),
                "sha1s": json.dumps([sha1]),
                "pick_codes": json.dumps([pick_code]),
            },
        )


def _insert_p115_file(
    runtime: DatabaseRuntime,
    *,
    file_id: str,
    pick_code: str,
    sha1: str,
    account_id: str = ACCOUNT_ID,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, sha1, pick_code,
                    size, account_id, entry_kind
                ) VALUES (
                    :file_id, :parent_id, :name, :local_path, :sha1, :pick_code,
                    1024, :account_id, 'file'
                )
                """
            ),
            {
                "file_id": file_id,
                "parent_id": f"parent-{file_id}",
                "name": f"{file_id}.mkv",
                "local_path": f"/fixture/{file_id}.mkv",
                "sha1": sha1,
                "pick_code": pick_code,
                "account_id": account_id,
            },
        )


def _snapshot(token: str) -> dict[str, object]:
    return {
        "created_at": 1_700_000_000.0,
        "delete_token": token,
        "item_id": "100",
        "item_name": "Fixture Movie",
        "item_type": "Movie",
        "series_id": "",
        "pickcodes": ["pc-fixture"],
        "p115_file_snapshots": [{"pick_code": "pc-fixture", "fid": "501", "parent_id": "500"}],
    }


def test_pg18_deep_delete_snapshot_is_cross_session_single_claim(p6_runtime: DatabaseRuntime) -> None:
    repository = SqlAlchemyP115DeepDeleteSnapshotRepository(p6_runtime.unit_of_work)
    token = "fixture-cross-session"
    repository.store(token, _snapshot(token))

    first = repository.claim(token)
    replay = repository.claim(token)

    assert first == _snapshot(token)
    assert replay is None


def test_pg18_deep_delete_snapshot_allows_only_one_concurrent_claim(p6_runtime: DatabaseRuntime) -> None:
    repository = SqlAlchemyP115DeepDeleteSnapshotRepository(p6_runtime.unit_of_work)
    token = "fixture-concurrent-claim"
    repository.store(token, _snapshot(token))

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: repository.claim(token), range(2)))

    assert sum(result is not None for result in results) == 1
    assert next(result for result in results if result is not None) == _snapshot(token)


def test_pg18_storing_snapshot_removes_expired_capabilities(p6_runtime: DatabaseRuntime) -> None:
    repository = SqlAlchemyP115DeepDeleteSnapshotRepository(p6_runtime.unit_of_work)
    expired_token = "fixture-expired-row"
    fresh_token = "fixture-fresh-row"
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES (:setting_key, CAST(:value_json AS jsonb), NOW() - INTERVAL '10 minutes')
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = EXCLUDED.last_updated_at
                """
            ),
            {
                "setting_key": f"runtime.deep_delete_snapshot.{expired_token}",
                "value_json": '{"created_at":1}',
            },
        )

    repository.store(fresh_token, _snapshot(fresh_token))

    with p6_runtime.engine.connect() as connection:
        keys = set(
            connection.scalars(
                text(
                    """
                    SELECT setting_key
                    FROM app_settings
                    WHERE setting_key LIKE 'runtime.deep_delete_snapshot.%'
                    """
                )
            )
        )
    assert f"runtime.deep_delete_snapshot.{expired_token}" not in keys
    assert f"runtime.deep_delete_snapshot.{fresh_token}" in keys


def test_pg18_deep_delete_scope_expands_movie_series_and_season(p6_runtime: DatabaseRuntime) -> None:
    with p6_runtime.engine.connect() as connection:
        assert str(connection.scalar(text("SHOW server_version_num"))).startswith("18")

    _insert_media(
        p6_runtime,
        tmdb_id="movie-1",
        item_type="Movie",
        emby_id="100",
        pick_code="pc-movie",
        sha1="A" * 40,
    )
    _insert_media(p6_runtime, tmdb_id="series-1", item_type="Series", emby_id="200")
    _insert_media(
        p6_runtime,
        tmdb_id="series-1-season-1",
        item_type="Season",
        emby_id="201",
        series_id="series-1",
        season_number=1,
    )
    _insert_media(
        p6_runtime,
        tmdb_id="episode-1",
        item_type="Episode",
        emby_id="211",
        pick_code="pc-episode-1",
        sha1="B" * 40,
        series_id="series-1",
        season_number=1,
        episode_number=1,
    )
    _insert_media(
        p6_runtime,
        tmdb_id="episode-2",
        item_type="Episode",
        emby_id="212",
        pick_code="pc-episode-2",
        sha1="C" * 40,
        series_id="series-1",
        season_number=1,
        episode_number=2,
    )
    _insert_media(
        p6_runtime,
        tmdb_id="episode-3",
        item_type="Episode",
        emby_id="221",
        pick_code="pc-episode-3",
        sha1="D" * 40,
        series_id="series-1",
        season_number=2,
        episode_number=1,
    )
    scope, _provider = _scope(p6_runtime)

    target = scope.media_cleanup_target("100", "Movie")

    assert target is not None
    assert target["emby_id"] == "100"
    assert target["tmdb_id"] == "movie-1"
    assert target["item_type"] == "Movie"
    assert target["local"] == {"files": [{"sha1": "A" * 40, "pick_code": "pc-movie"}]}
    assert scope.pickcodes_for_deleted_item("100", "Movie") == ("pc-movie",)
    assert set(scope.pickcodes_for_deleted_item("200", "Series")) == {
        "pc-episode-1",
        "pc-episode-2",
        "pc-episode-3",
    }
    assert set(scope.pickcodes_for_deleted_item("201", "Season")) == {
        "pc-episode-1",
        "pc-episode-2",
    }


def test_pg18_deep_delete_scope_uses_account_bound_sha1_and_freezes_provider_identity(
    p6_runtime: DatabaseRuntime,
) -> None:
    sha1 = "E" * 40
    _insert_media(
        p6_runtime,
        tmdb_id="movie-sha1",
        item_type="Movie",
        emby_id="300",
        sha1=sha1,
    )
    _insert_p115_file(p6_runtime, file_id="501", pick_code="pc-current", sha1=sha1)
    _insert_p115_file(
        p6_runtime,
        file_id="502",
        pick_code="pc-other-account",
        sha1=sha1,
        account_id="115002",
    )
    scope, provider = _scope(p6_runtime)

    pickcodes = scope.pickcodes_for_deleted_item("300", "Movie")
    snapshots = scope.file_snapshots(pickcodes)

    assert pickcodes == ("pc-current",)
    assert snapshots == (
        {
            "pick_code": "pc-current",
            "fid": "501",
            "parent_id": "parent-501",
            "sha1": sha1,
            "account_id": ACCOUNT_ID,
            "backend": "openapi",
            "credential_fingerprint": FINGERPRINT,
        },
    )
    assert provider.open_calls == [{"expected_account_id": ACCOUNT_ID}]


def test_pg18_deep_delete_scope_fails_closed_on_missing_or_ambiguous_identity(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_media(
        p6_runtime,
        tmdb_id="ambiguous-1",
        item_type="Movie",
        emby_id="400",
        pick_code="pc-ambiguous-1",
        sha1="F" * 40,
    )
    _insert_media(
        p6_runtime,
        tmdb_id="ambiguous-2",
        item_type="Movie",
        emby_id="400",
        pick_code="pc-ambiguous-2",
        sha1="1" * 40,
    )
    _insert_p115_file(p6_runtime, file_id="601", pick_code="pc-sha1-a", sha1="2" * 40)
    _insert_p115_file(p6_runtime, file_id="602", pick_code="pc-sha1-b", sha1="2" * 40)
    _insert_p115_file(p6_runtime, file_id="603", pick_code="Pc-Duplicate", sha1="3" * 40)
    _insert_p115_file(p6_runtime, file_id="604", pick_code="pc-duplicate", sha1="4" * 40)
    scope, _provider = _scope(p6_runtime)

    assert scope.media_cleanup_target("missing", "Movie") is None
    assert scope.media_cleanup_target("400", "Movie") is None
    with pytest.raises(RuntimeError, match="ambiguous media identity"):
        scope.pickcodes_for_deleted_item("400", "Movie")
    assert scope.pick_code_by_sha1("2" * 40) is None
    with pytest.raises(RuntimeError, match="identity is incomplete"):
        scope.file_snapshots(["pc-missing"])
    with pytest.raises(RuntimeError, match="ambiguous P115 cache identities"):
        scope.file_snapshots(["pc-duplicate"])


def test_pg18_deep_delete_media_cleanup_fails_closed_on_frozen_version_drift(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_media(
        p6_runtime,
        tmdb_id="movie-drift",
        item_type="Movie",
        emby_id="500",
        pick_code="pc-before",
        sha1="5" * 40,
    )
    scope, _provider = _scope(p6_runtime)
    target = scope.media_cleanup_target("500", "Movie")
    assert target is not None
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET file_sha1_json = '["6666666666666666666666666666666666666666"]'::jsonb,
                    file_pickcode_json = '["pc-after"]'::jsonb
                WHERE tmdb_id = 'movie-drift' AND item_type = 'Movie'
                """
            )
        )

    result = NativeP115MediaCleanupReceiptGateway(SqlAlchemyCleanupMediaGateway(p6_runtime.unit_of_work)).cleanup(
        item_id="500",
        item_name="Movie Drift",
        item_type="Movie",
        series_id="",
        local_only=False,
        operation_id="fixture-version-drift",
        target=target,
    )

    assert result["ok"] is False
    assert result["indeterminate"] is True
    assert result["error"] == {
        "type": "media_cleanup_version_identity_changed",
        "emby_id": "500",
    }
    with p6_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT emby_item_ids_json, file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE tmdb_id = 'movie-drift' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
    assert dict(row) == {
        "emby_item_ids_json": ["500"],
        "file_sha1_json": ["6" * 40],
        "file_pickcode_json": ["pc-after"],
    }
