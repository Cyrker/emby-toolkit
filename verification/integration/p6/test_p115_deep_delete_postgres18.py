from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from etk.domains.p115.deep_delete import SqlAlchemyP115DeepDeleteSnapshotRepository
from etk.platform.db import DatabaseRuntime


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
