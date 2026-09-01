from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, unquote, urlsplit

from etk.integrations.http_policy import ProviderDisposition
from etk.platform import Settings

from .shared_center import SharedCenterPort

_TELEGRAM_API_ID = 30_841_166
_TELEGRAM_API_HASH = "center-managed-login"
_MAX_SEARCH_LIMIT = 50
_MAX_SEARCH_TIMEOUT_SECONDS = 120
_LISTENER_START_TIMEOUT_SECONDS = 10
_DEFAULT_TG_REGEX: dict[str, tuple[str, ...]] = {
    "tmdb": (r"TMDB(?:\s*ID)?[:：\s]*(\d+)",),
    "title_year": (
        r"(?:电视剧|电影|影片|剧集|名称)[:：\s]*([^\n]+?)\s*\((\d{4})\)",
        r"^([^\n]+?)\s*\((\d{4})\)",
    ),
    "password": (
        r"(?:password|pwd)=([a-zA-Z0-9]{4})",
        r"(?:password=|访问码|提取码|密码)[:：=\s]*([a-zA-Z0-9]{4})",
    ),
}

logger = logging.getLogger(__name__)


class TelegramHistoryClientPort(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    def is_connected(self) -> bool: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_entity(self, entity: Any) -> Any: ...

    def iter_messages(self, entity: Any, *, search: str, limit: int) -> Any: ...


class TelegramUserBotClientPort(TelegramHistoryClientPort, Protocol):
    session: Any

    def add_event_handler(self, callback: Callable[[Any], Any], event: Any = None) -> Any: ...

    async def run_until_disconnected(self) -> Any: ...

    async def sign_in(self, *args: Any, **kwargs: Any) -> Any: ...

    async def log_out(self) -> Any: ...


class TelegramChallengeRecordPort(Protocol):
    @property
    def payload(self) -> Mapping[str, Any]: ...


class TelegramChallengeSecretStorePort(Protocol):
    def create(
        self,
        payload: Mapping[str, Any],
        *,
        reference: str | None = None,
        owner_key: str | None = None,
        ttl_seconds: float,
    ) -> tuple[str, bool]: ...

    def consume(self, reference: str, owner_key: str) -> TelegramChallengeRecordPort: ...

    def destroy(self, reference: str, owner_key: str | None = None) -> bool: ...


class TelegramRealtimeActionPort(Protocol):
    def stage(
        self,
        candidate: Mapping[str, Any],
        *,
        occurrence: str,
        is_brainless: bool,
        is_keyword_matched: bool,
        is_subscribe: bool,
        is_hdhive_push: bool,
    ) -> bool: ...


class NativeTelegramRealtimeMessageHandler:
    """Normalize one Telethon event and stage only a durable native action."""

    def __init__(
        self,
        configuration: Callable[[], Mapping[str, Any] | None],
        actions: TelegramRealtimeActionPort,
    ) -> None:
        self._configuration = configuration
        self._actions = actions

    async def handle(self, event: Any) -> bool:
        configuration = _listener_configuration(self._configuration())
        if not configuration["enabled"] or not configuration["channels"]:
            return False
        chat = await event.get_chat()
        sender = None
        try:
            sender = await event.get_sender()
        except Exception:
            pass
        chat_username = str(getattr(chat, "username", "") or "")
        chat_id = str(getattr(chat, "id", "") or "")
        sender_username = str(getattr(sender, "username", "") or "")
        sender_id = str(getattr(sender, "id", "") or "")
        message = getattr(event, "message", event)
        text = str(getattr(event, "raw_text", None) or getattr(message, "raw_text", None) or "")
        source_matched = any(
            _channel_matches(channel, username, identity)
            for channel in cast(Sequence[str], configuration["channels"])
            for username, identity in ((chat_username, chat_id), (sender_username, sender_id))
        )
        if not source_matched:
            if "hdhive" in f"{chat_username} {sender_username}".casefold() or "影巢" in text:
                logger.debug(
                    "Ignoring suspected HDHive Telegram message from unlisted source: chat=%s sender=%s",
                    chat_username or chat_id or "-",
                    sender_username or sender_id or "-",
                )
            return False
        if not text:
            return False
        modes = {
            mode: _listener_mode_enabled(
                configuration,
                mode,
                chat_username=chat_username,
                chat_id=chat_id,
                sender_username=sender_username,
                sender_id=sender_id,
            )
            for mode in ("brainless", "subscribe", "keyword", "hdhive_push")
        }
        if not any(modes.values()):
            return False
        if _rule_match(text, configuration["block_keywords"], chat_username, chat_id) is not None:
            return False
        keyword_matched = bool(
            modes["keyword"]
            and _rule_match(text, configuration["transfer_keywords"], chat_username, chat_id) is not None
        )
        candidate = _realtime_candidate(
            message,
            chat,
            text=text,
            chat_username=chat_username,
            chat_id=chat_id,
            custom_regex=cast(Mapping[str, Any], configuration["custom_regex"]),
        )
        if candidate is None:
            return False
        media_type = str(candidate.get("media_type") or candidate.get("item_type") or "movie").casefold()
        hdhive_push = bool(
            modes["hdhive_push"] and "hdhive.com/resource/" in str(candidate.get("target_link") or "").casefold()
        )
        if (
            media_type not in cast(Sequence[str], configuration["monitor_types"])
            and not modes["brainless"]
            and not keyword_matched
            and not hdhive_push
        ):
            return False
        occurrence = _listener_occurrence(chat_id, str(candidate.get("message_id") or ""), candidate)
        return bool(
            await asyncio.to_thread(
                self._actions.stage,
                candidate,
                occurrence=occurrence,
                is_brainless=modes["brainless"],
                is_keyword_matched=keyword_matched,
                is_subscribe=modes["subscribe"],
                is_hdhive_push=hdhive_push,
            )
        )


class NativeTelegramSearchManager:
    """Read an authorized UserBot session without starting a legacy listener."""

    def __init__(
        self,
        configuration: Callable[[], Mapping[str, Any] | None],
        settings: Callable[[], Settings],
        session_path: Path,
        *,
        client_factory: Callable[..., TelegramHistoryClientPort] | None = None,
    ) -> None:
        self._configuration = configuration
        self._settings = settings
        self._session_path = session_path.resolve()
        self._client_factory = client_factory or _default_client

    def get_status(self) -> dict[str, Any]:
        configuration = self._config()
        if not configuration["enabled"]:
            return {"status": "stopped", "msg": "Telegram UserBot 未启用"}
        if not configuration["channels"]:
            return {"status": "unconfigured", "msg": "Telegram UserBot 未配置监听频道"}
        if not self._session_path.is_file():
            return {"status": "unauthorized", "msg": "Telegram UserBot 尚未授权"}
        return {"status": "ready", "msg": "Telegram UserBot 搜索会话已就绪"}

    def search_channel_resources(
        self,
        query: str,
        media_type: str | None = None,
        tmdb_id: str | int | None = None,
        year: str | int | None = None,
        limit: int = 10,
        extra_queries: Sequence[str] | None = None,
        timeout: int = 30,
        include_tmdb_query: bool = False,
        strict_title_match: bool = True,
    ) -> dict[str, Any]:
        del include_tmdb_query
        normalized_query = str(query or "").strip()
        if not normalized_query:
            return {"ok": False, "error_type": "telegram_search_query_required", "results": []}
        normalized_limit = _bounded_int(limit, default=10, minimum=1, maximum=_MAX_SEARCH_LIMIT)
        normalized_timeout = _bounded_int(
            timeout,
            default=30,
            minimum=1,
            maximum=_MAX_SEARCH_TIMEOUT_SECONDS,
        )
        configuration = self._config()
        if not configuration["enabled"] or not configuration["channels"]:
            return {"ok": False, "error_type": "telegram_search_not_configured", "results": []}
        if not self._session_path.is_file():
            return {"ok": False, "error_type": "telegram_search_session_missing", "results": []}
        search_queries = _unique_text((normalized_query, *(extra_queries or ())))
        try:
            return asyncio.run(
                asyncio.wait_for(
                    self._search(
                        configuration,
                        search_queries=search_queries,
                        query=normalized_query,
                        media_type=str(media_type or "").strip().casefold(),
                        tmdb_id=str(tmdb_id or "").strip(),
                        year=str(year or "").strip(),
                        limit=normalized_limit,
                        strict_title_match=bool(strict_title_match),
                    ),
                    timeout=float(normalized_timeout),
                )
            )
        except TimeoutError:
            return {"ok": False, "error_type": "telegram_search_timeout", "results": []}
        except Exception as exc:
            return {
                "ok": False,
                "error_type": "telegram_search_unavailable",
                "error_class": type(exc).__name__,
                "results": [],
            }

    async def _search(
        self,
        configuration: Mapping[str, Any],
        *,
        search_queries: Sequence[str],
        query: str,
        media_type: str,
        tmdb_id: str,
        year: str,
        limit: int,
        strict_title_match: bool,
    ) -> dict[str, Any]:
        client = self._client_factory(
            str(self._session_path),
            _TELEGRAM_API_ID,
            _TELEGRAM_API_HASH,
            proxy=_telethon_proxy(self._settings()),
        )
        try:
            if not client.is_connected():
                await client.connect()
            if not await client.is_user_authorized():
                return {"ok": False, "error_type": "telegram_search_session_unauthorized", "results": []}
            return await self._search_connected(
                client,
                configuration,
                search_queries=search_queries,
                query=query,
                media_type=media_type,
                tmdb_id=tmdb_id,
                year=year,
                limit=limit,
                strict_title_match=strict_title_match,
            )
        finally:
            if client.is_connected():
                await client.disconnect()

    async def _search_connected(
        self,
        client: TelegramHistoryClientPort,
        configuration: Mapping[str, Any],
        *,
        search_queries: Sequence[str],
        query: str,
        media_type: str,
        tmdb_id: str,
        year: str,
        limit: int,
        strict_title_match: bool,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        seen: set[str] = set()
        for channel in cast(Sequence[str], configuration["channels"]):
            entity = await _resolve_entity(client, channel)
            if entity is None:
                continue
            for search_query in search_queries:
                async for message in client.iter_messages(
                    entity,
                    search=search_query,
                    limit=max(limit * 5, 30),
                ):
                    candidate = _message_candidate(
                        message,
                        entity,
                        query=query,
                        media_type=media_type,
                        tmdb_id=tmdb_id,
                        year=year,
                        strict_title_match=strict_title_match,
                    )
                    if candidate is None:
                        continue
                    identity = f"{candidate['source_chat_id']}:{candidate['message_id']}"
                    if identity in seen:
                        continue
                    seen.add(identity)
                    results.append(candidate)
                    if len(results) >= limit:
                        return {"ok": True, "results": results}
        return {"ok": True, "results": results}

    def _config(self) -> dict[str, Any]:
        loaded = self._configuration()
        value = dict(loaded) if isinstance(loaded, Mapping) else {}
        raw_channels = value.get("channels")
        channels = _unique_text(raw_channels if isinstance(raw_channels, list) else ())
        return {
            "enabled": value.get("enabled") is True,
            "channels": channels,
        }


class NativeTelegramUserBotManager(NativeTelegramSearchManager):
    """Coordinator-owned native UserBot login/session manager plus history search."""

    def __init__(
        self,
        configuration: Callable[[], Mapping[str, Any] | None],
        settings: Callable[[], Settings],
        session_path: Path,
        center: SharedCenterPort,
        challenge_store: TelegramChallengeSecretStorePort,
        *,
        client_factory: Callable[..., TelegramUserBotClientPort] | None = None,
        string_session_factory: Callable[[str | None], Any] | None = None,
        session_exporter: Callable[[Any], str] | None = None,
        session_writer: Callable[[Path, str], None] | None = None,
        realtime_actions: TelegramRealtimeActionPort | None = None,
        realtime_executor: TelegramRealtimeActionPort | None = None,
        event_builder_factory: Callable[[], Any] | None = None,
    ) -> None:
        factory: Callable[..., TelegramUserBotClientPort] = client_factory or _default_client
        super().__init__(configuration, settings, session_path, client_factory=factory)
        self._center = center
        self._challenge_store = challenge_store
        self._auth_client_factory = factory
        self._string_session_factory = string_session_factory or _default_string_session
        self._session_exporter = session_exporter or _default_session_exporter
        self._session_writer = session_writer or _default_session_writer
        self._realtime_handler = (
            NativeTelegramRealtimeMessageHandler(configuration, realtime_actions)
            if realtime_actions is not None
            else None
        )
        self._realtime_executor = realtime_executor or realtime_actions
        self._event_builder_factory = event_builder_factory or _default_new_message_event
        self._listener_guard = threading.RLock()
        self._listener_thread: threading.Thread | None = None
        self._listener_loop: asyncio.AbstractEventLoop | None = None
        self._listener_client: TelegramUserBotClientPort | None = None
        self._listener_fingerprint = ""
        self._listener_ready = threading.Event()
        self._listener_stopping = threading.Event()
        self._listener_status: dict[str, Any] = {"status": "stopped"}

    def get_status(self) -> dict[str, Any]:
        configuration = self._auth_config()
        try:
            challenge = self._load_challenge(str(configuration["phone"] or ""))
        except RuntimeError as exc:
            return {"status": "error", "msg": str(exc)}
        if challenge is not None:
            return {"status": "authorizing"}
        if not configuration["enabled"]:
            return {"status": "stopped", "msg": "Telegram UserBot 未启用"}
        if self._session_path.is_file():
            if self._realtime_handler is None:
                return {"status": "authorized"}
            with self._listener_guard:
                status = dict(self._listener_status)
                running = self._listener_thread is not None and self._listener_thread.is_alive()
            if running and status.get("status") == "authorized":
                return status
            if status.get("status") in {"error", "unauthorized"}:
                return status
            return {"status": "stopped", "msg": "Telegram UserBot 已授权，监听尚未启动"}
        return {"status": "unauthorized"}

    def search_channel_resources(
        self,
        query: str,
        media_type: str | None = None,
        tmdb_id: str | int | None = None,
        year: str | int | None = None,
        limit: int = 10,
        extra_queries: Sequence[str] | None = None,
        timeout: int = 30,
        include_tmdb_query: bool = False,
        strict_title_match: bool = True,
    ) -> dict[str, Any]:
        del include_tmdb_query
        with self._listener_guard:
            client = self._listener_client
            loop = self._listener_loop
            listening = self._listener_status.get("status") == "authorized"
        if client is None or loop is None or not loop.is_running() or not listening:
            return super().search_channel_resources(
                query,
                media_type=media_type,
                tmdb_id=tmdb_id,
                year=year,
                limit=limit,
                extra_queries=extra_queries,
                timeout=timeout,
                strict_title_match=strict_title_match,
            )
        normalized_query = str(query or "").strip()
        if not normalized_query:
            return {"ok": False, "error_type": "telegram_search_query_required", "results": []}
        normalized_limit = _bounded_int(limit, default=10, minimum=1, maximum=_MAX_SEARCH_LIMIT)
        normalized_timeout = _bounded_int(
            timeout,
            default=30,
            minimum=1,
            maximum=_MAX_SEARCH_TIMEOUT_SECONDS,
        )
        configuration = self._config()
        search_queries = _unique_text((normalized_query, *(extra_queries or ())))
        future = asyncio.run_coroutine_threadsafe(
            self._search_connected(
                client,
                configuration,
                search_queries=search_queries,
                query=normalized_query,
                media_type=str(media_type or "").strip().casefold(),
                tmdb_id=str(tmdb_id or "").strip(),
                year=str(year or "").strip(),
                limit=normalized_limit,
                strict_title_match=bool(strict_title_match),
            ),
            loop,
        )
        try:
            return future.result(timeout=float(normalized_timeout))
        except TimeoutError:
            future.cancel()
            return {"ok": False, "error_type": "telegram_search_timeout", "results": []}
        except Exception as exc:
            return {
                "ok": False,
                "error_type": "telegram_search_unavailable",
                "error_class": type(exc).__name__,
                "results": [],
            }

    def stage_listener_action(
        self,
        candidate: Mapping[str, Any],
        *,
        occurrence: str,
        is_brainless: bool,
        is_keyword_matched: bool,
        is_subscribe: bool,
        is_hdhive_push: bool,
    ) -> dict[str, Any]:
        executor = self._realtime_executor
        if executor is None:
            raise RuntimeError("Telegram realtime action executor is unavailable")
        staged = executor.stage(
            candidate,
            occurrence=occurrence,
            is_brainless=is_brainless,
            is_keyword_matched=is_keyword_matched,
            is_subscribe=is_subscribe,
            is_hdhive_push=is_hdhive_push,
        )
        return {"success": True, "staged": bool(staged)}

    def apply_listener_state(self) -> dict[str, Any]:
        configuration = _listener_configuration(self._configuration())
        desired = bool(
            self._realtime_handler is not None
            and configuration["enabled"]
            and configuration["channels"]
            and self._session_path.is_file()
        )
        fingerprint = _listener_config_fingerprint(configuration)
        with self._listener_guard:
            running = self._listener_thread is not None and self._listener_thread.is_alive()
            unchanged = running and self._listener_fingerprint == fingerprint
        if not desired:
            self.stop_listener()
            return self.get_status()
        if unchanged:
            return self.get_status()
        self.stop_listener()
        self._listener_ready.clear()
        self._listener_stopping.clear()
        with self._listener_guard:
            self._listener_fingerprint = fingerprint
            self._listener_status = {"status": "starting", "msg": "Telegram UserBot 监听正在启动"}
            thread = threading.Thread(
                target=self._run_listener,
                args=(fingerprint,),
                daemon=True,
                name="ETK-Native-Telegram-Listener",
            )
            self._listener_thread = thread
            thread.start()
        self._listener_ready.wait(timeout=_LISTENER_START_TIMEOUT_SECONDS)
        return self.get_status()

    def stop_listener(self) -> None:
        self._listener_stopping.set()
        with self._listener_guard:
            client = self._listener_client
            loop = self._listener_loop
            thread = self._listener_thread
        if client is not None and loop is not None and loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(client.disconnect(), loop).result(timeout=10)
            except Exception as exc:
                logger.warning("Native Telegram listener disconnect failed: %s", type(exc).__name__)
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=10)
        if thread is not None and thread.is_alive():
            raise RuntimeError("Native Telegram listener did not stop")
        with self._listener_guard:
            if self._listener_thread is thread:
                self._listener_thread = None
            if self._listener_client is client:
                self._listener_client = None
            if self._listener_loop is loop:
                self._listener_loop = None
            self._listener_fingerprint = ""
            if self._listener_status.get("status") not in {"error", "unauthorized"}:
                self._listener_status = {"status": "stopped"}
        self._listener_ready.clear()

    def send_login_code(self) -> dict[str, Any]:
        configuration = self._auth_config()
        phone = str(configuration["phone"] or "").strip()
        if not phone:
            raise ValueError("未配置手机号")
        if not phone.startswith("+"):
            raise ValueError("手机号格式错误，必须以 '+' 号开头")
        self.stop_listener()
        telegram_session = asyncio.run(self._create_login_session())
        response = self._center.send_telegram_login_code(phone, telegram_session)
        if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(
            response.payload,
            Mapping,
        ):
            raise RuntimeError("Telegram 验证码服务未确认请求")
        payload = response.payload.get("data")
        payload = payload if isinstance(payload, Mapping) else response.payload
        updated_session = str(payload.get("telegram_session") or "").strip()
        phone_code_hash = str(payload.get("phone_code_hash") or "").strip()
        if not updated_session or not phone_code_hash:
            raise RuntimeError("Telegram 验证码服务未返回完整登录上下文")
        reference, owner_key = _challenge_identity(phone)
        self._challenge_store.destroy(reference, owner_key)
        self._challenge_store.create(
            {
                "telegram_session": updated_session,
                "phone_code_hash": phone_code_hash,
            },
            reference=reference,
            owner_key=owner_key,
            ttl_seconds=10 * 60,
        )
        return {"success": True}

    def submit_login_code(self, code: str) -> dict[str, Any]:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            raise ValueError("请输入验证码")
        configuration = self._auth_config()
        phone = str(configuration["phone"] or "")
        challenge = self._load_challenge(phone)
        if challenge is None:
            raise RuntimeError("登录上下文已失效，请重新获取验证码")
        result = asyncio.run(self._submit_login_code(normalized_code, configuration, challenge))
        if result.get("success") is not True:
            return result
        authorized_session = str(result.pop("telegram_session") or "").strip()
        if not authorized_session:
            raise RuntimeError("无法保存 Telegram 授权会话")
        self._session_writer(self._session_path, authorized_session)
        reference, owner_key = _challenge_identity(phone)
        self._challenge_store.destroy(reference, owner_key)
        self.apply_listener_state()
        return {"success": True}

    def logout(self) -> dict[str, Any]:
        self.stop_listener()
        if self._session_path.is_file():
            asyncio.run(self._logout_session())
        self._clear_session_files()
        return {"success": True}

    def clear_session_files(self) -> dict[str, Any]:
        self.stop_listener()
        deleted, missing = self._clear_session_files()
        return {"deleted": deleted, "missing": missing}

    def _run_listener(self, fingerprint: str) -> None:
        loop: asyncio.AbstractEventLoop | None = None
        client: TelegramUserBotClientPort | None = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            client = self._auth_client_factory(
                str(self._session_path),
                _TELEGRAM_API_ID,
                _TELEGRAM_API_HASH,
                proxy=_telethon_proxy(self._settings()),
            )
            with self._listener_guard:
                if self._listener_fingerprint != fingerprint or self._listener_stopping.is_set():
                    return
                self._listener_loop = loop
                self._listener_client = client

            async def on_message(event: Any) -> None:
                handler = self._realtime_handler
                if handler is None:
                    return
                attempt = 0
                while not self._listener_stopping.is_set():
                    try:
                        await handler.handle(event)
                    except Exception as exc:
                        attempt += 1
                        logger.warning(
                            "Native Telegram durable ingress failed (attempt %d): %s",
                            attempt,
                            type(exc).__name__,
                        )
                        await asyncio.sleep(min(30.0, 0.5 * (2 ** min(attempt - 1, 6))))
                    else:
                        return

            async def run() -> None:
                assert client is not None
                if self._listener_stopping.is_set():
                    return
                client.add_event_handler(on_message, self._event_builder_factory())
                await asyncio.wait_for(client.connect(), timeout=30)
                if self._listener_stopping.is_set():
                    return
                if not await asyncio.wait_for(client.is_user_authorized(), timeout=10):
                    with self._listener_guard:
                        self._listener_status = {"status": "unauthorized"}
                    self._listener_ready.set()
                    return
                with self._listener_guard:
                    self._listener_status = {"status": "authorized", "msg": "Telegram UserBot 监听中"}
                self._listener_ready.set()
                await client.run_until_disconnected()

            loop.run_until_complete(run())
        except Exception as exc:
            with self._listener_guard:
                self._listener_status = {
                    "status": "error",
                    "msg": f"Telegram UserBot 监听异常：{type(exc).__name__}",
                }
            self._listener_ready.set()
        finally:
            if client is not None and loop is not None and client.is_connected():
                try:
                    loop.run_until_complete(client.disconnect())
                except Exception:
                    pass
            if loop is not None and not loop.is_closed():
                loop.close()
            with self._listener_guard:
                if client is not None and self._listener_client is client:
                    self._listener_client = None
                if loop is not None and self._listener_loop is loop:
                    self._listener_loop = None
                if self._listener_thread is threading.current_thread():
                    self._listener_thread = None
                if self._listener_status.get("status") == "authorized":
                    self._listener_status = {"status": "stopped"}
            self._listener_ready.set()

    async def _create_login_session(self) -> str:
        client = self._auth_client_factory(
            self._string_session_factory(None),
            _TELEGRAM_API_ID,
            _TELEGRAM_API_HASH,
            proxy=_telethon_proxy(self._settings()),
        )
        try:
            await asyncio.wait_for(client.connect(), timeout=30)
            telegram_session = self._session_exporter(client.session)
            if not telegram_session:
                raise RuntimeError("无法生成 Telegram 登录会话")
            return telegram_session
        finally:
            if client.is_connected():
                await client.disconnect()

    async def _submit_login_code(
        self,
        code: str,
        configuration: Mapping[str, Any],
        challenge: Mapping[str, str],
    ) -> dict[str, Any]:
        client = self._auth_client_factory(
            self._string_session_factory(challenge["telegram_session"]),
            _TELEGRAM_API_ID,
            _TELEGRAM_API_HASH,
            proxy=_telethon_proxy(self._settings()),
        )
        try:
            await asyncio.wait_for(client.connect(), timeout=30)
            try:
                await asyncio.wait_for(
                    client.sign_in(
                        str(configuration["phone"]),
                        code,
                        phone_code_hash=challenge["phone_code_hash"],
                    ),
                    timeout=30,
                )
            except Exception as exc:
                if type(exc).__name__ != "SessionPasswordNeededError":
                    raise
                password = str(configuration["password"] or "")
                if not password:
                    return {
                        "success": False,
                        "need_2fa": True,
                        "msg": "需要两步验证密码，请在配置中填写后重试",
                    }
                await asyncio.wait_for(client.sign_in(password=password), timeout=30)
            return {
                "success": True,
                "telegram_session": self._session_exporter(client.session),
            }
        finally:
            if client.is_connected():
                await client.disconnect()

    async def _logout_session(self) -> None:
        client = self._auth_client_factory(
            str(self._session_path),
            _TELEGRAM_API_ID,
            _TELEGRAM_API_HASH,
            proxy=_telethon_proxy(self._settings()),
        )
        try:
            await asyncio.wait_for(client.connect(), timeout=30)
            if await asyncio.wait_for(client.is_user_authorized(), timeout=10):
                await asyncio.wait_for(client.log_out(), timeout=30)
        finally:
            if client.is_connected():
                await client.disconnect()

    def _clear_session_files(self) -> tuple[list[str], list[str]]:
        phone = str(self._auth_config()["phone"] or "")
        if phone:
            reference, owner_key = _challenge_identity(phone)
            self._challenge_store.destroy(reference, owner_key)
        deleted: list[str] = []
        missing: list[str] = []
        for path in (self._session_path, Path(f"{self._session_path}-journal")):
            if path.is_file():
                path.unlink()
                deleted.append(path.name)
            else:
                missing.append(path.name)
        return deleted, missing

    def _load_challenge(self, phone: str) -> dict[str, str] | None:
        normalized_phone = str(phone or "").strip()
        if not normalized_phone:
            return None
        reference, owner_key = _challenge_identity(normalized_phone)
        try:
            record = self._challenge_store.consume(reference, owner_key)
        except Exception as exc:
            if isinstance(exc, (KeyError, LookupError)) or type(exc).__name__ == "TelegramResourceSecretExpiredError":
                return None
            raise RuntimeError("Telegram 登录上下文存储当前不可用") from exc
        payload = record.payload
        telegram_session = str(payload.get("telegram_session") or "").strip()
        phone_code_hash = str(payload.get("phone_code_hash") or "").strip()
        if not telegram_session or not phone_code_hash:
            return None
        return {
            "telegram_session": telegram_session,
            "phone_code_hash": phone_code_hash,
        }

    def _auth_config(self) -> dict[str, Any]:
        loaded = self._configuration()
        value = dict(loaded) if isinstance(loaded, Mapping) else {}
        return {
            "enabled": value.get("enabled") is True,
            "phone": str(value.get("phone") or "").strip(),
            "password": str(value.get("password") or ""),
        }


async def _resolve_entity(client: TelegramHistoryClientPort, channel: str) -> Any | None:
    normalized = str(channel or "").strip().lstrip("@")
    if not normalized:
        return None
    candidates: list[Any]
    if re.fullmatch(r"-?\d+", normalized):
        candidates = [int(normalized)]
        if normalized.startswith("-100"):
            candidates.append(int(normalized[4:]))
    else:
        candidates = [normalized, f"@{normalized}"]
    for candidate in candidates:
        try:
            return await client.get_entity(candidate)
        except Exception:
            continue
    return None


def _message_candidate(
    message: Any,
    entity: Any,
    *,
    query: str,
    media_type: str,
    tmdb_id: str,
    year: str,
    strict_title_match: bool,
) -> dict[str, Any] | None:
    text = str(getattr(message, "raw_text", None) or getattr(message, "message", "") or "")
    if not text:
        return None
    explicit_tmdb = _explicit_tmdb_id(text)
    if tmdb_id and explicit_tmdb and explicit_tmdb != tmdb_id:
        return None
    if strict_title_match and not _title_matches(text, query):
        return None
    if strict_title_match and year and year[:4] not in _years(text):
        return None
    urls = _message_urls(message, text)
    target_link = next(
        (value for value in urls if re.search(r"(?i)(?:115(?:cdn)?\.com/s/[a-z0-9]+|hdhive\.com/resource/)", value)),
        "",
    )
    offline_match = re.search(r"(?i)(magnet:\?[^\s]+|ed2k://\|file\|.*?\|/)", text)
    magnet_url = offline_match.group(1).strip() if offline_match else ""
    if not target_link and not magnet_url:
        return None
    message_id = str(getattr(message, "id", "") or "")
    chat_id = str(getattr(entity, "id", "") or "")
    username = str(getattr(entity, "username", "") or "")
    title = _title_lines(text)[0] if _title_lines(text) else query
    detected_years = sorted(_years(text))
    resolved_year = year[:4] if year[:4].isdigit() else (detected_years[0] if detected_years else "")
    season_number, episode_number = _season_episode(text)
    is_special = _detect_special(text)
    resolved_media_type = media_type or _infer_media_type(
        text,
        season_number=season_number,
        episode_number=episode_number,
        is_special=is_special,
    )
    title = _clean_candidate_title(title) or title
    candidate_evidence: list[str] = []
    if explicit_tmdb or tmdb_id:
        candidate_evidence.append("explicit_tmdb")
    if title:
        candidate_evidence.append("identify_title")
    if resolved_year:
        candidate_evidence.append("year")
    if resolved_media_type == "tv":
        candidate_evidence.append("media_type_tv")
    if season_number is not None:
        candidate_evidence.append("season")
    if episode_number is not None:
        candidate_evidence.append("episode")
    if is_special:
        candidate_evidence.append("special")
    is_completed_pack = bool(re.search(r"(?i)\bcomplete\b|完结|全季|全\s*\d+\s*[集话]|\d+\s*[集话]\s*全", text))
    is_pack = is_completed_pack or bool(
        re.search(
            r"(?i)\b(?:season|pack)\b|S\d+\s*E\d+\s*(?:-|~|至)\s*(?:E|EP)?\s*\d+|全[季集]|合集",
            text,
        )
    )
    return {
        "_tg_source": "channel",
        "source": "channel",
        "source_kind": "channel",
        "title": title,
        "name": title,
        "year": int(resolved_year) if resolved_year else None,
        "text": text,
        "raw_text": text,
        "tmdb_id": explicit_tmdb or tmdb_id,
        "item_type": resolved_media_type,
        "media_type": resolved_media_type,
        "target_link": target_link or None,
        "magnet_url": magnet_url or None,
        "receive_code": _receive_code(target_link, text),
        "source_channel": str(getattr(entity, "title", "") or username or chat_id),
        "source_username": username,
        "source_chat_id": chat_id,
        "message_id": message_id,
        "season_number": season_number,
        "episode_number": episode_number,
        "is_special": is_special,
        "is_pack": is_pack,
        "is_completed_pack": is_completed_pack,
        "quality": _quality_text(text),
        "quality_text": _quality_text(text),
        "resolution": _resolution(text),
        "remark": _normalize_text(text)[:180],
        "confidence": "high" if explicit_tmdb or tmdb_id else ("medium" if title else "low"),
        "evidence": candidate_evidence,
        "matched_rules": list(candidate_evidence),
        "parse_version": "v1",
    }


def _realtime_candidate(
    message: Any,
    entity: Any,
    *,
    text: str,
    chat_username: str,
    chat_id: str,
    custom_regex: Mapping[str, Any],
) -> dict[str, Any] | None:
    candidate = _message_candidate(
        message,
        entity,
        query="",
        media_type="",
        tmdb_id="",
        year="",
        strict_title_match=False,
    )
    if candidate is None:
        return None
    tmdb_match = _rule_match(
        text,
        _rules_with_defaults(custom_regex.get("tmdb"), _DEFAULT_TG_REGEX["tmdb"]),
        chat_username,
        chat_id,
    )
    if tmdb_match is not None and tmdb_match.lastindex:
        candidate["tmdb_id"] = str(tmdb_match.group(1) or "").strip()
    title_match = _rule_match(
        text,
        _rules_with_defaults(custom_regex.get("title_year"), _DEFAULT_TG_REGEX["title_year"]),
        chat_username,
        chat_id,
        flags=0,
    )
    if title_match is not None and title_match.lastindex:
        title = str(title_match.group(1) or "").strip()
        if title:
            candidate.update(title=title, name=title)
        if title_match.lastindex >= 2:
            year = str(title_match.group(2) or "").strip()
            if year.isdigit():
                candidate["year"] = int(year)
    season_match = _rule_match(text, custom_regex.get("season_episode"), chat_username, chat_id)
    if season_match is not None and season_match.lastindex:
        candidate["season_number"] = _optional_positive_int(season_match.group(1))
        if season_match.lastindex >= 2:
            candidate["episode_number"] = _optional_positive_int(season_match.group(2))
        candidate["media_type"] = "tv"
        candidate["item_type"] = "tv"
    password_match = _rule_match(
        f"{candidate.get('target_link') or ''}\n{text}",
        _rules_with_defaults(custom_regex.get("password"), _DEFAULT_TG_REGEX["password"]),
        chat_username,
        chat_id,
    )
    if password_match is not None and password_match.lastindex:
        candidate["receive_code"] = str(password_match.group(1) or "").strip()[:32]
    candidate["source_username"] = chat_username
    candidate["source_chat_id"] = chat_id
    return candidate


def _message_urls(message: Any, text: str) -> list[str]:
    values = re.findall(r"(?i)https?://[^\s\])}>\"']+", text)
    for entity in getattr(message, "entities", None) or ():
        if url := str(getattr(entity, "url", "") or "").strip():
            values.append(url)
    reply_markup = getattr(message, "reply_markup", None)
    for row in getattr(reply_markup, "rows", None) or ():
        for button in getattr(row, "buttons", None) or ():
            if url := str(getattr(button, "url", "") or "").strip():
                values.append(url)
    expanded = list(values)
    for value in values:
        try:
            query = parse_qs(urlsplit(value).query)
        except ValueError:
            continue
        for key in ("url", "u", "target", "redirect", "redirect_url", "to", "link"):
            expanded.extend(unquote(item).strip() for item in query.get(key, ()) if item)
    return _unique_text(expanded)


def _receive_code(target_link: str, text: str) -> str:
    if target_link:
        try:
            for key in ("password", "passcode", "receive_code", "code"):
                values = parse_qs(urlsplit(target_link).query).get(key)
                if values and str(values[0]).strip():
                    return str(values[0]).strip()[:32]
        except ValueError:
            pass
    match = re.search(
        r"(?i)(?:提取码|访问码|接收码|密码|pass(?:word)?|code)\s*[:：=]?\s*([a-z0-9]{2,32})",
        text,
    )
    return match.group(1) if match else ""


def _explicit_tmdb_id(text: str) -> str:
    for pattern in (
        r"(?i)\bTMDB\s*(?:ID)?\s*[:：#-]?\s*(\d{2,10})\b",
        r"(?i)\{\s*tmdb-(\d{2,10})\s*\}",
        r"(?i)\btmdb-(\d{2,10})\b",
    ):
        if match := re.search(pattern, text):
            return match.group(1)
    return ""


def _challenge_identity(phone: str) -> tuple[str, str]:
    phone_hash = hashlib.sha256(str(phone or "").strip().encode()).hexdigest()
    reference = hashlib.sha256(f"telegram-login-challenge\0{phone_hash}".encode()).hexdigest()
    owner_key = hashlib.sha256(f"telegram-login-challenge-owner\0{phone_hash}".encode()).hexdigest()
    return reference, owner_key


def _years(text: str) -> set[str]:
    return {match.group(1) for match in re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text)}


def _season_episode(text: str) -> tuple[int | None, int | None]:
    if re.search(r"(?i)(?:完结|全\s*\d+\s*[集话]|\d+\s*[集话]\s*全)", text):
        complete = re.search(r"(?i)全\s*(\d+)\s*[集话]|(\d+)\s*[集话]\s*全", text)
        total = complete.group(1) or complete.group(2) if complete else None
        return 1, _optional_positive_int(total)
    range_match = re.search(
        r"(?i)\bS\s*0*(\d{1,3})\s*E(?:P|pisode)?\s*0*\d{1,4}\s*(?:-|~|至)\s*(?:E|EP)?\s*0*(\d{1,4})",
        text,
    )
    if range_match:
        return _optional_positive_int(range_match.group(1)), _optional_positive_int(range_match.group(2))
    for pattern in (
        r"(?i)\bS(?:eason)?\s*0*(\d{1,3})(?:\s*E(?:P|pisode)?\s*0*(\d{1,4}))?\b",
        r"第\s*(\d{1,3})\s*季(?:\s*第?\s*(\d{1,4})\s*[集话])?",
    ):
        match = re.search(pattern, text)
        if match:
            return _optional_positive_int(match.group(1)), _optional_positive_int(match.group(2))
    episode = re.search(r"(?i)\b(?:E|EP|Episode)\s*0*(\d{1,4})\b|第\s*(\d{1,4})\s*[集话]", text)
    if episode:
        return 1, _optional_positive_int(episode.group(1) or episode.group(2))
    bulk = re.search(r"(?i)(?:更新至|全|至)(?:第)?\s*(\d{1,4})\s*[集话]", text)
    if bulk:
        return 1, _optional_positive_int(bulk.group(1))
    date_match = re.search(r"\b((?:19|20)\d{2})[.\-_/](\d{1,2})[.\-_/](\d{1,2})\b", text)
    if date_match:
        return _optional_positive_int(1), _optional_positive_int(
            f"{int(date_match.group(2)):02d}{int(date_match.group(3)):02d}"
        )
    return None, None


def _listener_configuration(value: Mapping[str, Any] | None) -> dict[str, Any]:
    loaded = dict(value) if isinstance(value, Mapping) else {}
    raw_channels = loaded.get("channels")
    channels = _unique_text(raw_channels if isinstance(raw_channels, list) else ())
    raw_monitor_types = loaded.get("monitor_types")
    monitor_types = [
        item
        for item in _unique_text(raw_monitor_types if isinstance(raw_monitor_types, list) else ("movie", "tv"))
        if item in {"movie", "tv"}
    ]
    raw_modes = loaded.get("transfer_modes")
    transfer_modes = [
        item
        for item in _unique_text(raw_modes if isinstance(raw_modes, list) else ("subscribe",))
        if item in {"brainless", "subscribe", "keyword", "hdhive_push"}
    ]
    raw_mode_channels = loaded.get("transfer_mode_channels")
    mode_channels: dict[str, list[str]] = {}
    if isinstance(raw_mode_channels, Mapping):
        for mode in transfer_modes:
            values = raw_mode_channels.get(mode)
            if isinstance(values, str):
                values = [values]
            mode_channels[mode] = _unique_text(values if isinstance(values, list) else ())
    custom_regex = loaded.get("custom_regex")
    return {
        "enabled": loaded.get("enabled") is True,
        "channels": channels,
        "monitor_types": monitor_types or ["movie", "tv"],
        "transfer_modes": transfer_modes or ["subscribe"],
        "transfer_mode_channels": mode_channels,
        "transfer_keywords": loaded.get("transfer_keywords")
        if isinstance(loaded.get("transfer_keywords"), list)
        else [],
        "block_keywords": loaded.get("block_keywords") if isinstance(loaded.get("block_keywords"), list) else [],
        "custom_regex": dict(custom_regex) if isinstance(custom_regex, Mapping) else {},
    }


def _channel_matches(target: object, username: str, chat_id: str) -> bool:
    normalized_target = str(target or "").strip().casefold()
    if not normalized_target:
        return True
    normalized_username = str(username or "").strip().casefold().lstrip("@")
    normalized_id = str(chat_id or "").strip()
    target_clean = normalized_target.lstrip("@")
    target_id = target_clean[4:] if target_clean.startswith("-100") else target_clean
    current_id = normalized_id[4:] if normalized_id.startswith("-100") else normalized_id
    return normalized_username == target_clean or normalized_id == normalized_target or current_id == target_id


def _listener_mode_enabled(
    configuration: Mapping[str, Any],
    mode: str,
    *,
    chat_username: str,
    chat_id: str,
    sender_username: str,
    sender_id: str,
) -> bool:
    if mode not in configuration.get("transfer_modes", ()):
        return False
    raw_channels = configuration.get("transfer_mode_channels")
    channels = raw_channels.get(mode, ()) if isinstance(raw_channels, Mapping) else ()
    if not channels:
        return True
    return any(
        _channel_matches(channel, username, identity)
        for channel in cast(Sequence[str], channels)
        for username, identity in ((chat_username, chat_id), (sender_username, sender_id))
    )


def _rule_match(
    text: str,
    rules: object,
    chat_username: str,
    chat_id: str,
    *,
    flags: int = re.IGNORECASE,
) -> re.Match[str] | None:
    values = rules if isinstance(rules, list) else ()
    for value in values:
        if isinstance(value, str):
            pattern = value.strip()
            channel = ""
        elif isinstance(value, Mapping):
            pattern = str(value.get("pattern") or "").strip()
            channel = str(value.get("channel") or "").strip()
        else:
            continue
        if not pattern or not _channel_matches(channel, chat_username, chat_id):
            continue
        try:
            match = re.search(pattern, text, flags)
        except re.error:
            logger.debug("Ignoring malformed Telegram listener regex: %s", pattern)
            continue
        if match is not None:
            return match
    return None


def _listener_occurrence(chat_id: str, message_id: str, candidate: Mapping[str, Any]) -> str:
    source = {
        "chat_id": str(chat_id or "").strip(),
        "message_id": str(message_id or "").strip(),
        "tmdb_id": str(candidate.get("tmdb_id") or "").strip(),
        "season_number": candidate.get("season_number"),
        "episode_number": candidate.get("episode_number"),
        "source_hash": hashlib.sha256(
            f"{candidate.get('target_link') or candidate.get('magnet_url') or ''}".encode()
        ).hexdigest(),
    }
    digest = hashlib.sha256(
        json.dumps(source, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return f"telegram-listener:{digest}"


def _listener_config_fingerprint(configuration: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(configuration),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _optional_positive_int(value: object) -> int | None:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _rules_with_defaults(value: object, defaults: Sequence[str]) -> list[object]:
    custom = value if isinstance(value, list) else []
    return [*custom, *defaults]


def _detect_special(text: str) -> bool:
    return bool(re.search(r"(?i)\b(?:SP|OVA|OAD|SPECIALS?|番外|特别篇|特別篇)\b", text))


def _infer_media_type(
    text: str,
    *,
    season_number: int | None,
    episode_number: int | None,
    is_special: bool,
) -> str:
    if re.search(r"(?:电视剧|剧集|动漫|番剧|动画)[:：]|(?:\[|【)(?:电视剧|剧集|动漫|番剧|动画)(?:\]|】)", text):
        return "tv"
    if re.search(r"(?:电影|影片)[:：]|(?:\[|【)电影(?:\]|】)", text):
        return "movie"
    if season_number is not None or episode_number is not None or is_special:
        return "tv"
    tags = " ".join(re.findall(r"#\w+", text))
    if re.search(r"#(?:电视剧|日剧|韩剧|美剧|英剧|台剧|港剧|泰剧|短剧|动漫|番剧|剧集|动画)", tags, re.IGNORECASE):
        return "tv"
    return "movie"


def _normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _quality_text(text: str) -> str:
    lines = [_normalize_text(line) for line in text.splitlines() if _normalize_text(line)]
    quality_words = (
        "WEB-DL",
        "WEBRIP",
        "BLURAY",
        "REMUX",
        "HDR",
        "DV",
        "HEVC",
        "H265",
        "X265",
        "X264",
        "中字",
        "内嵌",
        "外挂",
        "ATMOS",
        "TRUEHD",
        "DTS",
        "AAC",
    )
    for line in lines:
        if any(word.casefold() in line.casefold() for word in quality_words):
            return line[:120]
    return lines[0][:120] if lines else ""


def _resolution(text: str) -> str:
    upper = text.upper()
    for token in ("8K", "4K", "2160P", "1080P", "720P"):
        if token in upper:
            return "4K" if token == "2160P" else token
    return ""


def _clean_candidate_title(value: object) -> str:
    title = str(value or "").strip()
    if not title:
        return ""
    title = re.sub(r"^\[[^\]]{1,32}\]\s*", "", title).strip()
    title = re.sub(r"^[【\[][^\]】]{1,32}[】\]]\s*", "", title).strip()
    title = re.sub(r"(?i)\bTMDB\s*(?:ID)?\s*[:：#-]?\s*\d{2,10}\b", "", title).strip()
    title = re.sub(r"[\[\(（【](?:19|20)\d{2}[\]\)）】]", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*S\d{1,4}(?:(?:E|EP)\d{1,4})?\b.*$", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*(?:EP?|Episode)\d{1,4}\b.*$", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*Season\s*\d{1,4}\b.*$", "", title).strip()
    title = re.sub(r"(?i)\b(?:WEB[- ]?DL|WEBRIP|BLURAY|REMUX|HDR|DV|HEVC|H265|X265|X264|\d{3,4}P)\b", " ", title)
    title = re.sub(r"\s+", " ", title).strip(" -._")
    return title[:120]


def _title_matches(text: str, query: str) -> bool:
    expected = _normalize_title(query)
    if not expected:
        return True
    for candidate in _title_lines(text):
        normalized = _normalize_title(candidate)
        if expected in normalized or normalized in expected:
            return True
    return False


def _title_lines(text: str) -> list[str]:
    result: list[str] = []
    for line in [re.sub(r"\s+", " ", value).strip() for value in text.splitlines() if value.strip()][:8]:
        match = re.search(r"(?:电影|影片|剧集|电视剧|番剧|动漫|片名|标题|名称)\s*[:：]\s*(.+)$", line)
        if match:
            value = re.sub(r"(?i)https?://\S+", "", match.group(1)).strip()
            if value:
                result.append(value)
            continue
        cleaned = re.sub(r"[#*_`>\[\]【】]+", " ", line).strip()
        cleaned = re.sub(r"(?i)https?://\S+", "", cleaned).strip()
        if cleaned and not re.search(r"(?i)TMDB\s*ID|^(?:简介|分享|标签|大小|质量)\s*[:：]", cleaned):
            result.append(cleaned[:100])
    return _unique_text(result)


def _normalize_title(value: object) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").casefold())


def _unique_text(values: Iterable[object]) -> list[str]:
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _bounded_int(value: object, *, default: int, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError):
        return default
    return min(max(parsed, minimum), maximum)


def _telethon_proxy(settings: Settings) -> dict[str, Any] | None:
    if not settings.network_proxy_enabled or not settings.network_http_proxy_url:
        return None
    parsed = urlsplit(settings.network_http_proxy_url)
    if parsed.scheme.casefold() not in {"http", "https", "socks4", "socks5"}:
        raise ValueError("Telegram proxy scheme is unsupported")
    if not parsed.hostname or parsed.port is None:
        raise ValueError("Telegram proxy address is incomplete")
    proxy: dict[str, Any] = {
        "proxy_type": "http" if parsed.scheme.casefold() in {"http", "https"} else parsed.scheme.casefold(),
        "addr": parsed.hostname,
        "port": parsed.port,
    }
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy


def _default_client(
    session_path: Any,
    api_id: int,
    api_hash: str,
    *,
    proxy: Mapping[str, Any] | None,
) -> TelegramUserBotClientPort:
    from telethon import TelegramClient

    options: dict[str, Any] = {
        "connection_retries": 2,
        "timeout": 10,
    }
    if proxy is not None:
        options["proxy"] = dict(proxy)
    return cast(
        TelegramUserBotClientPort,
        TelegramClient(
            session_path,
            api_id,
            api_hash,
            **options,
        ),
    )


def _default_new_message_event() -> Any:
    from telethon import events

    return events.NewMessage()


def _default_string_session(value: str | None) -> Any:
    from telethon.sessions import StringSession

    return StringSession(value) if value else StringSession()


def _default_session_exporter(session: Any) -> str:
    from telethon.sessions import StringSession

    return str(StringSession.save(session) or "")


def _default_session_writer(path: Path, value: str) -> None:
    from telethon.sessions import SQLiteSession, StringSession

    source = StringSession(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    for candidate in (path, Path(f"{path}-journal")):
        if candidate.is_file():
            candidate.unlink()
    target = SQLiteSession(str(path))
    try:
        target.set_dc(source.dc_id, source.server_address, source.port)
        target.auth_key = source.auth_key
        target.save()
    finally:
        target.close()


__all__ = [
    "NativeTelegramSearchManager",
    "NativeTelegramRealtimeMessageHandler",
    "NativeTelegramUserBotManager",
    "TelegramRealtimeActionPort",
    "TelegramHistoryClientPort",
    "TelegramUserBotClientPort",
]
