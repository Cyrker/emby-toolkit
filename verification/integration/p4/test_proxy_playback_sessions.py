from __future__ import annotations

from sqlalchemy import text

import reverse_proxy
from etk.platform.db import DatabaseRuntime


def test_playback_stop_clears_the_proxy_session_through_postgres(
    p4_runtime: DatabaseRuntime,
    monkeypatch,
) -> None:
    raw_pool = p4_runtime.raw_pool
    assert raw_pool is not None
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = :key"),
            {"key": reverse_proxy._PLAY_CONCURRENCY_SESSIONS_KEY},
        )

    monkeypatch.setattr(reverse_proxy, "get_db_connection", lambda: raw_pool.connection())
    monkeypatch.setattr(reverse_proxy.user_db, "get_user_max_concurrent_streams", lambda _user_id: 1)
    monkeypatch.setattr(reverse_proxy.user_db, "get_username_by_id", lambda _user_id: "Fixture")
    monkeypatch.setattr(reverse_proxy, "_current_play_concurrency_device_id", lambda: "device-1")
    monkeypatch.setattr(reverse_proxy, "_current_play_concurrency_client_signature", lambda: "client-1")

    assert reverse_proxy._check_and_record_play_concurrency("user-1", "item-1", "session-1") is None
    blocked = reverse_proxy._check_and_record_play_concurrency("user-1", "item-2", "session-2")
    assert blocked is not None and blocked.status_code == 429

    reverse_proxy.clear_play_concurrency_for_playback_stop(
        {
            "UserId": "user-1",
            "ItemId": "item-1",
            "PlaySessionId": "session-1",
            "DeviceId": "device-1",
        }
    )

    assert reverse_proxy._check_and_record_play_concurrency("user-1", "item-2", "session-2") is None
    with p4_runtime.engine.connect() as connection:
        stored = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": reverse_proxy._PLAY_CONCURRENCY_SESSIONS_KEY},
        )
    assert isinstance(stored, dict)
    assert set(stored) == {"user-1|ps:session-2"}
