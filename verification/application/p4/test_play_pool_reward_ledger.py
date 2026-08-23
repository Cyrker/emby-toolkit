from __future__ import annotations

import config_manager  # noqa: F401  # Initialize legacy database modules in production order.
from handler import p115_play_pool


def test_reward_uses_atomic_ledger_and_projection_failure_cannot_repeat_entitlement(monkeypatch) -> None:
    grants = []
    summary = {
        "total_days": 0.8,
        "last_reward_days": 0.8,
        "last_reward_date": "2026-08-23",
        "last_reward_mode": "shared",
        "last_expiration_date": "2026-09-01T00:00:00+00:00",
    }

    def grant(user_id, reward_date, reward_type, reward_days, reward_mode):
        grants.append((user_id, reward_date, reward_type, reward_days, reward_mode))
        return {
            "granted": len(grants) == 1,
            "expiration_date": summary["last_expiration_date"],
        }

    monkeypatch.setattr(p115_play_pool, "_today_key", lambda: "2026-08-23")
    monkeypatch.setattr(p115_play_pool.user_db, "grant_play_pool_reward_once", grant)
    monkeypatch.setattr(p115_play_pool.user_db, "get_play_pool_reward_summary", lambda _user_id: dict(summary))
    monkeypatch.setattr(
        p115_play_pool.settings_db,
        "save_setting",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("projection unavailable")),
    )
    monkeypatch.setattr(p115_play_pool, "_load_user_rewards", lambda: {})
    account = {
        "owner_type": "user",
        "owner_user_id": "user-fixture",
        "enabled": True,
        "cookie": True,
        "shared": True,
    }

    first = p115_play_pool._reward_user_cookie(account)
    second = p115_play_pool._reward_user_cookie(account)

    assert first["rewarded"] is True
    assert first["reward_days"] == 0.8
    assert second["rewarded"] is False
    assert second["reward_days"] == 0.0
    assert first["summary"] == second["summary"] == summary
    assert grants == [
        ("user-fixture", "2026-08-23", "play_pool_cookie_daily", 0.8, "shared"),
        ("user-fixture", "2026-08-23", "play_pool_cookie_daily", 0.8, "shared"),
    ]


def test_public_reward_prefers_atomic_receipts_over_legacy_projection(monkeypatch) -> None:
    current = {
        "total_days": 1.6,
        "last_reward_days": 0.8,
        "last_reward_date": "2026-08-23",
        "last_reward_mode": "shared",
        "last_expiration_date": "2026-09-02T00:00:00+00:00",
    }
    monkeypatch.setattr(p115_play_pool.user_db, "get_play_pool_reward_summary", lambda _user_id: current)
    monkeypatch.setattr(
        p115_play_pool,
        "_load_user_rewards",
        lambda: {
            "user-fixture": {
                "total_days": 99,
                "last_reward_date": "stale",
            }
        },
    )

    assert p115_play_pool.public_user_reward("user-fixture") == current
