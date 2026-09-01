from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from etk.integrations import ProviderDisposition, ProviderResponse
from etk.integrations.telegram_userbot import NativeTelegramSearchManager, NativeTelegramUserBotManager
from etk.platform import Settings


class _Client:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages
        self.connected = False
        self.entities: list[Any] = []
        self.event_handler: Any = None
        self.event_filter: Any = None

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

    def add_event_handler(self, callback: Any, event: Any = None) -> None:
        self.event_handler = callback
        self.event_filter = event

    async def run_until_disconnected(self) -> None:
        return None

    async def sign_in(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs

    async def log_out(self) -> None:
        return None


class _SecretStore:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str | None, dict[str, Any]]] = {}

    def create(
        self,
        payload: dict[str, Any],
        *,
        reference: str | None = None,
        owner_key: str | None = None,
        ttl_seconds: float,
    ) -> tuple[str, bool]:
        del ttl_seconds
        assert reference is not None
        self.values[reference] = (owner_key, dict(payload))
        return reference, True

    def consume(self, reference: str, owner_key: str) -> SimpleNamespace:
        owner, payload = self.values[reference]
        assert owner == owner_key
        return SimpleNamespace(payload=dict(payload))

    def destroy(self, reference: str, owner_key: str | None = None) -> bool:
        current = self.values.get(reference)
        if current is None:
            return False
        assert owner_key is None or current[0] == owner_key
        del self.values[reference]
        return True


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


def test_native_userbot_auth_uses_typed_center_and_writes_only_authorized_session(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"

    class Client(_Client):
        def __init__(self, exported: str) -> None:
            super().__init__([])
            self.session = SimpleNamespace(exported=exported)
            self.sign_in_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        async def sign_in(self, *args: Any, **kwargs: Any) -> None:
            self.sign_in_calls.append((args, kwargs))

        async def log_out(self) -> None:
            return None

    clients = [Client("initial-session"), Client("authorized-session")]
    client_calls: list[Any] = []

    def client_factory(*args: Any, **kwargs: Any) -> Client:
        client_calls.append((args, kwargs))
        return clients.pop(0)

    class Center:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def send_telegram_login_code(self, phone: str, telegram_session: str) -> ProviderResponse[dict[str, Any]]:
            self.calls.append((phone, telegram_session))
            return ProviderResponse(
                ProviderDisposition.CONFIRMED,
                200,
                {
                    "success": True,
                    "data": {
                        "telegram_session": "challenge-session",
                        "phone_code_hash": "challenge-hash",
                    },
                },
            )

    written: list[tuple[Path, str]] = []

    def write_session(path: Path, value: str) -> None:
        written.append((path, value))
        path.touch()

    center = Center()
    challenge_store = _SecretStore()
    manager = NativeTelegramUserBotManager(
        lambda: {
            "enabled": True,
            "phone": "+8613800000000",
            "password": "fixture-2fa",
            "channels": [],
        },
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        center,  # type: ignore[arg-type]
        challenge_store,  # type: ignore[arg-type]
        client_factory=client_factory,
        string_session_factory=lambda value: f"string:{value or 'new'}",
        session_exporter=lambda session: str(session.exported),
        session_writer=write_session,
    )

    assert manager.send_login_code() == {"success": True}
    assert manager.get_status() == {"status": "authorizing"}
    assert center.calls == [("+8613800000000", "initial-session")]
    recovered = NativeTelegramUserBotManager(
        lambda: {
            "enabled": True,
            "phone": "+8613800000000",
            "password": "fixture-2fa",
            "channels": [],
        },
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        center,  # type: ignore[arg-type]
        challenge_store,  # type: ignore[arg-type]
        client_factory=client_factory,
        string_session_factory=lambda value: f"string:{value or 'new'}",
        session_exporter=lambda session: str(session.exported),
        session_writer=write_session,
    )
    assert recovered.get_status() == {"status": "authorizing"}
    assert recovered.submit_login_code("12345") == {"success": True}
    assert written == [(session_path.resolve(), "authorized-session")]
    assert recovered.get_status() == {"status": "authorized"}
    assert challenge_store.values == {}
    assert client_calls[0][0][0] == "string:new"
    assert client_calls[1][0][0] == "string:challenge-session"


def test_native_userbot_clear_session_is_local_and_idempotent(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"
    journal_path = Path(f"{session_path}-journal")
    session_path.touch()
    journal_path.touch()

    class Center:
        pass

    manager = NativeTelegramUserBotManager(
        lambda: {"enabled": True, "phone": "+8613800000000"},
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        Center(),  # type: ignore[arg-type]
        _SecretStore(),  # type: ignore[arg-type]
        client_factory=lambda *args, **kwargs: _Client([]),  # type: ignore[arg-type,return-value]
    )

    assert manager.clear_session_files() == {
        "deleted": ["tg_userbot.session", "tg_userbot.session-journal"],
        "missing": [],
    }
    assert manager.clear_session_files() == {
        "deleted": [],
        "missing": ["tg_userbot.session", "tg_userbot.session-journal"],
    }
