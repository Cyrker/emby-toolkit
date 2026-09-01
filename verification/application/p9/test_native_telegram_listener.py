from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from etk.domains.sharing.service import SharingHttpResult
from etk.domains.subscription.telegram_listener import (
    DurableTelegramRealtimeIngress,
    NativeTelegramRealtimeActionGateway,
)
from etk.integrations.telegram_userbot import (
    NativeTelegramRealtimeMessageHandler,
    NativeTelegramUserBotManager,
)
from etk.platform import Settings


class _Actions:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def stage(
        self,
        candidate: Mapping[str, Any],
        *,
        occurrence: str,
        is_brainless: bool,
        is_keyword_matched: bool,
        is_subscribe: bool,
        is_hdhive_push: bool,
    ) -> bool:
        self.calls.append(
            (
                dict(candidate),
                {
                    "occurrence": occurrence,
                    "is_brainless": is_brainless,
                    "is_keyword_matched": is_keyword_matched,
                    "is_subscribe": is_subscribe,
                    "is_hdhive_push": is_hdhive_push,
                },
            )
        )
        return True


class _Event:
    def __init__(
        self,
        text: str,
        *,
        chat_username: str = "fixture_channel",
        chat_id: int = -10042,
        sender_username: str = "fixture_sender",
        sender_id: int = 7,
        message_id: int = 11,
    ) -> None:
        self.raw_text = text
        self.message = SimpleNamespace(
            id=message_id,
            raw_text=text,
            entities=[],
            reply_markup=None,
        )
        self._chat = SimpleNamespace(id=chat_id, username=chat_username, title="Fixture Channel")
        self._sender = SimpleNamespace(id=sender_id, username=sender_username)

    async def get_chat(self) -> Any:
        return self._chat

    async def get_sender(self) -> Any:
        return self._sender


def test_realtime_handler_applies_whitelist_regex_modes_and_stable_occurrence() -> None:
    actions = _Actions()
    configuration = {
        "enabled": True,
        "channels": ["@fixture_channel"],
        "monitor_types": ["tv"],
        "transfer_modes": ["keyword"],
        "transfer_mode_channels": {"keyword": ["@fixture_channel"]},
        "transfer_keywords": [{"pattern": "特供", "channel": "@fixture_channel"}],
        "block_keywords": [{"pattern": "屏蔽", "channel": "@fixture_channel"}],
        "custom_regex": {
            "tmdb": [r"ID=(\d+)"],
            "title_year": [r"片名=(.+?)\s+年份=(\d{4})"],
            "season_episode": [r"季集=(\d+)/(\d+)"],
            "password": [r"口令=([a-z0-9]+)"],
        },
    }
    handler = NativeTelegramRealtimeMessageHandler(lambda: configuration, actions)
    event = _Event("特供 片名=Fixture Series 年份=2026 ID=42 季集=2/3 https://115.com/s/private-share 口令=2468")

    assert asyncio.run(handler.handle(event)) is True
    assert len(actions.calls) == 1
    candidate, flags = actions.calls[0]
    assert candidate["tmdb_id"] == "42"
    assert candidate["title"] == "Fixture Series"
    assert candidate["year"] == 2026
    assert candidate["season_number"] == 2
    assert candidate["episode_number"] == 3
    assert candidate["receive_code"] == "2468"
    assert candidate["media_type"] == "tv"
    assert flags["is_keyword_matched"] is True
    assert flags["is_brainless"] is False
    assert flags["is_subscribe"] is False
    assert flags["occurrence"].startswith("telegram-listener:")
    assert "private-share" not in flags["occurrence"]

    assert asyncio.run(handler.handle(event)) is True
    assert actions.calls[1][1]["occurrence"] == flags["occurrence"]


def test_realtime_handler_drops_unlisted_and_blocked_messages() -> None:
    actions = _Actions()
    configuration = {
        "enabled": True,
        "channels": ["@fixture_channel"],
        "monitor_types": ["movie", "tv"],
        "transfer_modes": ["brainless"],
        "block_keywords": ["屏蔽"],
    }
    handler = NativeTelegramRealtimeMessageHandler(lambda: configuration, actions)

    assert asyncio.run(handler.handle(_Event("电影 https://115.com/s/a", chat_username="other"))) is False
    assert asyncio.run(handler.handle(_Event("屏蔽 https://115.com/s/a"))) is False
    assert actions.calls == []


def test_realtime_handler_parses_default_media_identity_and_resource_metadata() -> None:
    actions = _Actions()
    configuration = {
        "enabled": True,
        "channels": ["@fixture_channel"],
        "transfer_modes": ["brainless"],
    }
    handler = NativeTelegramRealtimeMessageHandler(lambda: configuration, actions)
    event = _Event("[Fixture Series] S01EP02 WEB-DL 1080p https://115.com/s/private-share")

    assert asyncio.run(handler.handle(event)) is True
    candidate = actions.calls[0][0]
    assert candidate["title"] == "Fixture Series"
    assert candidate["media_type"] == "tv"
    assert candidate["season_number"] == 1
    assert candidate["episode_number"] == 2
    assert candidate["resolution"] == "1080P"
    assert candidate["quality_text"]


class _ListenerClient:
    def __init__(self) -> None:
        self.connected = False
        self.session = SimpleNamespace()
        self.handler: Any = None
        self.event: Any = None
        self.disconnected = False
        self._disconnect_event: asyncio.Event | None = None
        self.messages: list[Any] = []

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False
        self.disconnected = True
        if self._disconnect_event is not None:
            self._disconnect_event.set()

    def is_connected(self) -> bool:
        return self.connected

    async def is_user_authorized(self) -> bool:
        return True

    async def get_entity(self, entity: Any) -> Any:
        del entity
        return SimpleNamespace(id=-10042, username="fixture_channel", title="Fixture Channel")

    def iter_messages(self, entity: Any, *, search: str, limit: int) -> Any:
        del entity, search, limit

        async def empty():
            for message in self.messages:
                yield message

        return empty()

    def add_event_handler(self, callback: Any, event: Any = None) -> None:
        self.handler = callback
        self.event = event

    async def run_until_disconnected(self) -> None:
        self._disconnect_event = asyncio.Event()
        await self._disconnect_event.wait()

    async def sign_in(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs

    async def log_out(self) -> None:
        return None


class _SecretStore:
    def create(self, *args: Any, **kwargs: Any) -> tuple[str, bool]:
        del args, kwargs
        return "secret", True

    def consume(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise KeyError("missing")

    def destroy(self, *args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        return False


def test_native_listener_restarts_on_config_change_and_stops_on_leader_shutdown(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"
    session_path.touch()
    configuration: dict[str, Any] = {
        "enabled": True,
        "phone": "+8613800000000",
        "channels": ["@fixture_channel"],
        "monitor_types": ["movie", "tv"],
        "transfer_modes": ["brainless"],
    }
    clients: list[_ListenerClient] = []

    def client_factory(*args: Any, **kwargs: Any) -> _ListenerClient:
        del args, kwargs
        client = _ListenerClient()
        clients.append(client)
        return client

    manager = NativeTelegramUserBotManager(
        lambda: configuration,
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        SimpleNamespace(),  # type: ignore[arg-type]
        _SecretStore(),  # type: ignore[arg-type]
        client_factory=client_factory,
        realtime_actions=_Actions(),
        event_builder_factory=lambda: "fixture-event",
    )

    assert manager.apply_listener_state() == {
        "status": "authorized",
        "msg": "Telegram UserBot 监听中",
    }
    assert len(clients) == 1
    assert clients[0].handler is not None
    assert clients[0].event == "fixture-event"
    clients[0].messages = [
        SimpleNamespace(
            id=12,
            raw_text="电影：Fixture Movie 2026\nTMDB ID: 42\nhttps://115.com/s/private-share",
            entities=[],
            reply_markup=None,
        )
    ]
    search = manager.search_channel_resources(
        "Fixture Movie",
        media_type="movie",
        tmdb_id="42",
        year="2026",
    )
    assert search["ok"] is True
    assert len(search["results"]) == 1
    assert len(clients) == 1

    configuration["channels"] = ["@another_channel"]
    assert manager.apply_listener_state()["status"] == "authorized"
    assert len(clients) == 2
    assert clients[0].disconnected is True

    manager.stop_listener()
    assert clients[1].disconnected is True
    assert manager.get_status() == {
        "status": "stopped",
        "msg": "Telegram UserBot 已授权，监听尚未启动",
    }


def test_native_listener_factory_failure_sets_ready_error_state(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"
    session_path.touch()

    def broken_factory(*args: Any, **kwargs: Any) -> _ListenerClient:
        del args, kwargs
        raise RuntimeError("fixture factory unavailable")

    manager = NativeTelegramUserBotManager(
        lambda: {"enabled": True, "phone": "+8613800000000", "channels": ["@fixture_channel"]},
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        SimpleNamespace(),  # type: ignore[arg-type]
        _SecretStore(),  # type: ignore[arg-type]
        client_factory=broken_factory,
        realtime_actions=_Actions(),
    )

    status = manager.apply_listener_state()

    assert status["status"] == "error"
    assert "RuntimeError" in status["msg"]


def test_native_listener_stop_fences_a_client_created_after_shutdown_started(tmp_path: Path) -> None:
    session_path = tmp_path / "tg_userbot.session"
    session_path.touch()
    factory_entered = threading.Event()
    release_factory = threading.Event()
    client = _ListenerClient()

    def delayed_factory(*args: Any, **kwargs: Any) -> _ListenerClient:
        del args, kwargs
        factory_entered.set()
        assert release_factory.wait(timeout=2)
        return client

    manager = NativeTelegramUserBotManager(
        lambda: {"enabled": True, "phone": "+8613800000000", "channels": ["@fixture_channel"]},
        lambda: Settings(network_proxy_enabled=False),
        session_path,
        SimpleNamespace(),  # type: ignore[arg-type]
        _SecretStore(),  # type: ignore[arg-type]
        client_factory=delayed_factory,
        realtime_actions=_Actions(),
    )
    errors: list[BaseException] = []

    def apply() -> None:
        try:
            manager.apply_listener_state()
        except BaseException as exc:  # pragma: no cover - assertion reports the captured failure
            errors.append(exc)

    def stop() -> None:
        try:
            manager.stop_listener()
        except BaseException as exc:  # pragma: no cover - assertion reports the captured failure
            errors.append(exc)

    apply_thread = threading.Thread(target=apply)
    apply_thread.start()
    assert factory_entered.wait(timeout=2)
    stop_thread = threading.Thread(target=stop)
    stop_thread.start()
    release_factory.set()
    apply_thread.join(timeout=2)
    stop_thread.join(timeout=2)

    assert not apply_thread.is_alive()
    assert not stop_thread.is_alive()
    assert errors == []
    assert client.connected is False


def test_native_listener_challenge_backend_error_is_not_reported_as_missing(tmp_path: Path) -> None:
    class BrokenSecretStore(_SecretStore):
        def consume(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise RuntimeError("fixture database unavailable")

    manager = NativeTelegramUserBotManager(
        lambda: {"enabled": True, "phone": "+8613800000000", "channels": ["@fixture_channel"]},
        lambda: Settings(network_proxy_enabled=False),
        tmp_path / "tg_userbot.session",
        SimpleNamespace(),  # type: ignore[arg-type]
        BrokenSecretStore(),  # type: ignore[arg-type]
        client_factory=lambda *args, **kwargs: _ListenerClient(),
    )

    assert manager.get_status() == {
        "status": "error",
        "msg": "Telegram 登录上下文存储当前不可用",
    }


def test_durable_realtime_ingress_externalizes_candidate_before_enqueuing() -> None:
    class Commands:
        def __init__(self) -> None:
            self.calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

        def enqueue_listener_action_once(
            self,
            payload: Mapping[str, Any],
            *,
            secret_payload: Mapping[str, Any],
            reference: str,
            owner_key: str,
            ttl_seconds: float,
            idempotency_key: str,
        ) -> str:
            assert ttl_seconds > 0
            self.calls.append(
                (
                    dict(payload),
                    {
                        "secret_payload": dict(secret_payload),
                        "reference": reference,
                        "owner_key": owner_key,
                        "idempotency_key": idempotency_key,
                    },
                )
            )
            return "listener-command"

    commands = Commands()
    ingress = DurableTelegramRealtimeIngress(commands)
    occurrence = f"telegram-listener:{'a' * 64}"

    assert ingress.stage(
        {"tmdb_id": "42", "target_link": "https://115.com/s/provider-secret"},
        occurrence=occurrence,
        is_brainless=True,
        is_keyword_matched=False,
        is_subscribe=False,
        is_hdhive_push=False,
    )

    payload, secret = commands.calls[0]
    assert payload["occurrence"] == occurrence
    assert "provider-secret" not in json.dumps(payload)
    assert secret["secret_payload"]["candidate"]["target_link"].endswith("provider-secret")
    assert secret["owner_key"] == hashlib.sha256(secret["idempotency_key"].encode()).hexdigest()


def test_native_realtime_gateway_uses_subscription_gate_and_native_operations() -> None:
    class Provider:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

        def cloud_download(self, payload: dict[str, Any], **kwargs: Any) -> SharingHttpResult:
            self.calls.append(("cloud", payload, kwargs))
            return SharingHttpResult({"success": True, "accepted": True}, 202)

        def hdhive_download(self, payload: dict[str, Any], **kwargs: Any) -> SharingHttpResult:
            self.calls.append(("hdhive", payload, kwargs))
            return SharingHttpResult({"success": True, "accepted": True}, 202)

    class Interests:
        allowed = False

        def wants(self, candidate: dict[str, Any]) -> bool:
            del candidate
            return self.allowed

    provider = Provider()
    interests = Interests()
    gateway = NativeTelegramRealtimeActionGateway(provider, interests)  # type: ignore[arg-type]
    candidate = {
        "tmdb_id": "42",
        "title": "Fixture",
        "media_type": "movie",
        "target_link": "https://115.com/s/private-share",
    }

    assert (
        gateway.stage(
            candidate,
            occurrence="telegram-listener:one",
            is_brainless=False,
            is_keyword_matched=False,
            is_subscribe=True,
            is_hdhive_push=False,
        )
        is False
    )
    interests.allowed = True
    assert (
        gateway.stage(
            candidate,
            occurrence="telegram-listener:one",
            is_brainless=False,
            is_keyword_matched=False,
            is_subscribe=True,
            is_hdhive_push=False,
        )
        is True
    )
    assert provider.calls[0][0] == "cloud"
    assert provider.calls[0][2] == {
        "principal_id": "telegram-listener",
        "idempotency_key": "telegram-listener:one",
    }

    hdhive = {
        **candidate,
        "target_link": "https://hdhive.com/resource/115/12345678-1234-1234-1234-123456789abc",
    }
    assert (
        gateway.stage(
            hdhive,
            occurrence="telegram-listener:two",
            is_brainless=False,
            is_keyword_matched=False,
            is_subscribe=False,
            is_hdhive_push=True,
        )
        is True
    )
    assert provider.calls[1][0] == "hdhive"
    assert provider.calls[1][1]["slug"] == "12345678-1234-1234-1234-123456789abc"
