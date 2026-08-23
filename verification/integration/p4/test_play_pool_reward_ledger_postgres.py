from __future__ import annotations

import threading

import pytest
from sqlalchemy import text

import config_manager  # noqa: F401  # Initialize legacy database modules in production order.
from database import user_db
from etk.platform.db import DatabaseRuntime


def test_play_pool_reward_receipt_and_entitlement_are_atomic_under_concurrency(
    p4_runtime: DatabaseRuntime,
    monkeypatch,
) -> None:
    assert p4_runtime.raw_pool is not None
    monkeypatch.setattr(user_db, "get_db_connection", p4_runtime.raw_pool.connection)
    user_id = "play-pool-reward-fixture"
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("INSERT INTO emby_users (id, name) VALUES (:id, 'Reward Fixture')"),
            {"id": user_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users_extended (emby_user_id, expiration_date)
                VALUES (:id, NOW() + INTERVAL '10 days')
                """
            ),
            {"id": user_id},
        )
        before = connection.scalar(
            text("SELECT expiration_date FROM emby_users_extended WHERE emby_user_id = :id"),
            {"id": user_id},
        )

    barrier = threading.Barrier(2)
    results = []
    errors = []

    def grant() -> None:
        try:
            barrier.wait(timeout=2)
            results.append(
                user_db.grant_play_pool_reward_once(
                    user_id,
                    "2026-08-23",
                    "play_pool_cookie_daily",
                    0.8,
                    "shared",
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=grant, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert not errors
    assert sorted(result["granted"] for result in results) == [False, True]
    with p4_runtime.engine.connect() as connection:
        after = connection.scalar(
            text("SELECT expiration_date FROM emby_users_extended WHERE emby_user_id = :id"),
            {"id": user_id},
        )
        receipt_count = connection.scalar(
            text(
                """
                SELECT count(*) FROM app_settings
                WHERE setting_key LIKE 'etk:p4:play-pool-reward:v1:%'
                """
            )
        )

    assert before is not None and after is not None
    assert (after - before).total_seconds() == pytest.approx(0.8 * 24 * 60 * 60)
    assert receipt_count == 1
    assert user_db.get_play_pool_reward_summary(user_id) == {
        "total_days": 0.8,
        "last_reward_days": 0.8,
        "last_reward_date": "2026-08-23",
        "last_reward_mode": "shared",
        "last_expiration_date": after.isoformat(),
    }
