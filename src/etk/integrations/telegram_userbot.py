from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, unquote, urlsplit

from etk.platform import Settings

_TELEGRAM_API_ID = 30_841_166
_TELEGRAM_API_HASH = "center-managed-login"
_MAX_SEARCH_LIMIT = 50
_MAX_SEARCH_TIMEOUT_SECONDS = 120


class TelegramHistoryClientPort(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    def is_connected(self) -> bool: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_entity(self, entity: Any) -> Any: ...

    def iter_messages(self, entity: Any, *, search: str, limit: int) -> Any: ...


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
        finally:
            if client.is_connected():
                await client.disconnect()

    def _config(self) -> dict[str, Any]:
        loaded = self._configuration()
        value = dict(loaded) if isinstance(loaded, Mapping) else {}
        raw_channels = value.get("channels")
        channels = _unique_text(raw_channels if isinstance(raw_channels, list) else ())
        return {
            "enabled": value.get("enabled") is True,
            "channels": channels,
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
        (value for value in urls if re.search(r"(?i)115(?:cdn)?\.com/s/[a-z0-9]+", value)),
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
    return {
        "_tg_source": "channel",
        "source": "channel",
        "source_kind": "channel",
        "title": title,
        "name": title,
        "year": int(year[:4]) if year[:4].isdigit() else None,
        "text": text,
        "raw_text": text,
        "tmdb_id": explicit_tmdb or tmdb_id,
        "item_type": media_type or "",
        "media_type": media_type or "",
        "target_link": target_link or None,
        "magnet_url": magnet_url or None,
        "receive_code": _receive_code(target_link, text),
        "source_channel": str(getattr(entity, "title", "") or username or chat_id),
        "source_username": username,
        "source_chat_id": chat_id,
        "message_id": message_id,
    }


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


def _years(text: str) -> set[str]:
    return {match.group(1) for match in re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text)}


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
            result.append(match.group(1).strip())
            continue
        cleaned = re.sub(r"[#*_`>\[\]【】]+", " ", line).strip()
        if cleaned and not re.search(r"(?i)https?://|TMDB\s*ID|^(?:简介|分享|标签|大小|质量)\s*[:：]", cleaned):
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
    session_path: str,
    api_id: int,
    api_hash: str,
    *,
    proxy: Mapping[str, Any] | None,
) -> TelegramHistoryClientPort:
    from telethon import TelegramClient

    options: dict[str, Any] = {
        "connection_retries": 2,
        "timeout": 10,
    }
    if proxy is not None:
        options["proxy"] = dict(proxy)
    return cast(
        TelegramHistoryClientPort,
        TelegramClient(
            session_path,
            api_id,
            api_hash,
            **options,
        ),
    )


__all__ = ["NativeTelegramSearchManager", "TelegramHistoryClientPort"]
