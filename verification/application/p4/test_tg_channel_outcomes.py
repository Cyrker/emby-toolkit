from __future__ import annotations

from contextlib import nullcontext

import pytest

from etk.compatibility import tg_resource_operations
from handler import p115_service, tg_userbot


def _safe_offline_payload() -> dict[str, object]:
    return {
        "type": "offline_download",
        "resource_kind": "p115_offline",
        "secret_ref": "a" * 64,
        "semantic_source_hash": "b" * 64,
        "delivery_key": "c" * 64,
    }


def _safe_complex_payload(**overrides) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "channel_resource_complex",
        "resource_kind": "p115_share",
        "secret_ref": "a" * 64,
        "semantic_source_hash": "b" * 64,
        "delivery_key": "c" * 64,
        "candidate": {},
        "tmdb_id": "100",
        "title": "Fixture",
        "item_type": "movie",
        "is_brainless": True,
    }
    payload.update(overrides)
    return payload


def test_channel_job_without_p115_client_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        classmethod(lambda _cls: None),
    )

    with pytest.raises(RuntimeError, match="115 客户端未初始化"):
        tg_userbot.process_tg_transfer_payload(_safe_offline_payload())


@pytest.mark.parametrize(
    "payload",
    [
        _safe_offline_payload(),
        _safe_complex_payload(),
    ],
)
def test_channel_external_effect_failure_fails_the_job(monkeypatch, payload) -> None:
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        classmethod(lambda _cls: object()),
    )
    monkeypatch.setattr(
        tg_resource_operations,
        "execute_channel_p115_effect",
        lambda _payload: (_ for _ in ()).throw(RuntimeError("provider failed")),
    )

    with pytest.raises(RuntimeError, match="provider failed"):
        tg_userbot.process_tg_transfer_payload(payload)


def test_channel_business_filter_returns_explicit_skipped_outcome(monkeypatch) -> None:
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        classmethod(lambda _cls: object()),
    )
    payload = _safe_complex_payload(
        is_brainless=False,
        is_subscribe=False,
    )

    outcome = tg_userbot.process_tg_transfer_payload(payload)

    assert outcome == {
        "ok": True,
        "status": "skipped",
        "reason": "not_subscribed",
        "message": "频道资源不在本机订阅或追剧范围",
    }


def test_channel_existing_local_episode_returns_explicit_skipped_outcome(monkeypatch) -> None:
    from database import media_db

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def execute(*_args, **_kwargs):
            return None

        @staticmethod
        def fetchone():
            return {"subscription_status": "SUBSCRIBED"}

    class Connection:
        @staticmethod
        def cursor():
            return Cursor()

    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        classmethod(lambda _cls: object()),
    )
    monkeypatch.setattr(tg_userbot, "get_db_connection", lambda: nullcontext(Connection()))
    monkeypatch.setattr(media_db, "get_series_local_children_info", lambda _tmdb_id: {1: [2]})
    payload = _safe_complex_payload(
        item_type="tv",
        is_brainless=False,
        is_subscribe=True,
        season_number=1,
        episode_number=2,
    )

    outcome = tg_userbot.process_tg_transfer_payload(payload)

    assert outcome["ok"] is True
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "local_episode_exists"
