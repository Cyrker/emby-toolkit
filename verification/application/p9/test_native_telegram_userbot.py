from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from etk.integrations.telegram_userbot import NativeTelegramSearchManager
from etk.platform import Settings


class _Client:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages
        self.connected = False
        self.entities: list[Any] = []

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    async def is_user_authorized(self) -> bool:
        return True

    async def get_entity(self, entity: Any) -> Any:
        self.entities.append(entity)
        return SimpleNamespace(id=-10042, username="fixture_channel", title="Fixture Channel")

    def iter_messages(self, entity: Any, *, search: str, limit: int) -> Any:
        del entity, search, limit

        async def selected():
            for message in self.messages:
                yield message

        return selected()


def _manager(
    session_path: Path,
    client: _Client,
    calls: list[tuple[Any, ...]],
) -> NativeTelegramSearchManager:
    def factory(*args: Any, **kwargs: Any) -> _Client:
        calls.append((*args, kwargs))
        return client

    return NativeTelegramSearchManager(
        lambda: {"enabled": True, "channels": ["@fixture_channel"]},
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        client_factory=factory,
    )


def test_native_userbot_search_reads_authorized_session_and_returns_bounded_candidate(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"
    session_path.touch()
    message = SimpleNamespace(
        id=7,
        raw_text=("电影：Fixture Movie 2026\nTMDB ID: 42\nhttps://115.com/s/private-share\n提取码：2468"),
        entities=[],
        reply_markup=None,
    )
    client = _Client([message])
    calls: list[tuple[Any, ...]] = []
    manager = _manager(session_path, client, calls)

    result = manager.search_channel_resources(
        "Fixture Movie",
        media_type="movie",
        tmdb_id="42",
        year="2026",
        limit=10,
        strict_title_match=True,
    )

    assert result["ok"] is True
    assert len(result["results"]) == 1
    candidate = result["results"][0]
    assert candidate["tmdb_id"] == "42"
    assert candidate["target_link"] == "https://115.com/s/private-share"
    assert candidate["receive_code"] == "2468"
    assert candidate["source_chat_id"] == "-10042"
    assert client.connected is False
    assert client.entities == ["fixture_channel"]
    assert calls == [(str(session_path.resolve()), 30_841_166, "center-managed-login", {"proxy": None})]


def test_native_userbot_search_rejects_title_or_year_drift(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"
    session_path.touch()
    client = _Client(
        [
            SimpleNamespace(
                id=8,
                raw_text="电影：Another Movie 2025\nhttps://115.com/s/private-share",
                entities=[],
                reply_markup=None,
            )
        ]
    )
    manager = _manager(session_path, client, [])

    result = manager.search_channel_resources(
        "Fixture Movie",
        tmdb_id="42",
        year="2026",
        strict_title_match=True,
    )

    assert result == {"ok": True, "results": []}


def test_native_userbot_search_does_not_create_a_missing_session(tmp_path: Path) -> None:
    session_path = tmp_path / "missing.session"
    calls: list[tuple[Any, ...]] = []
    manager = _manager(session_path, _Client([]), calls)

    assert manager.get_status()["status"] == "unauthorized"
    assert manager.search_channel_resources("Fixture") == {
        "ok": False,
        "error_type": "telegram_search_session_missing",
        "results": [],
    }
    assert calls == []
    assert not session_path.exists()
