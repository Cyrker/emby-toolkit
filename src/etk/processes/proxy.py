"""Independent ASGI proxy for virtual libraries and playback.

This module intentionally has no import-time dependency on reverse_proxy
or any legacy database module. The local verification stack injects an
in-memory catalog and fake upstream.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

from etk.platform.db import SqlAlchemyUnitOfWork

MIMICKED_ID_BASE = 900_000
DEFAULT_LEASE_TTL_SECONDS = 8 * 60 * 60


@dataclass(frozen=True, slots=True)
class VirtualLibrary:
    id: str
    name: str
    allowed_user_ids: tuple[str, ...] = ()
    item_types: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def virtual_id(self) -> str:
        return virtual_library_id(self.id)


@dataclass(frozen=True, slots=True)
class VirtualItem:
    id: str
    name: str
    item_type: str = "Movie"
    parent_id: str | None = None
    sort_name: str | None = None
    production_year: int | None = None
    source_url: str | None = None
    direct_url: str | None = None
    provider_ids: Mapping[str, str] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def as_emby(self, *, server_id: str = "fixture-server") -> dict[str, Any]:
        payload: dict[str, Any] = {
            "Name": self.name,
            "SortName": self.sort_name or self.name,
            "Id": self.id,
            "ServerId": server_id,
            "Type": self.item_type,
            "ParentId": self.parent_id,
            "ProviderIds": dict(self.provider_ids),
            "UserData": {"PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": False, "Played": False},
            **dict(self.metadata),
        }
        if self.production_year is not None:
            payload["ProductionYear"] = self.production_year
        if self.source_url:
            payload["MediaSources"] = [{"Path": self.source_url, "Name": self.name}]
        return {key: value for key, value in payload.items() if value is not None}


@dataclass(frozen=True, slots=True)
class VirtualPage:
    items: tuple[VirtualItem, ...]
    total: int


@dataclass(frozen=True, slots=True)
class PlaybackTarget:
    direct_url: str
    virtual: bool = True
    content_type: str | None = None


class ProxyCatalogError(RuntimeError):
    """Return a bounded client-facing error from a production catalog adapter."""

    def __init__(self, message: str, *, status_code: int = 502) -> None:
        super().__init__(message)
        self.status_code = max(400, min(int(status_code), 599))


class VirtualLibraryCatalog(Protocol):
    def views(
        self,
        user_id: str,
        *,
        client_signature: str = "",
        access_token: str = "",
    ) -> Sequence[VirtualLibrary]: ...

    def items(
        self,
        library_id: str,
        user_id: str,
        *,
        sort_by: str = "DateCreated",
        descending: bool = True,
        limit: int = 100,
        offset: int = 0,
        access_token: str = "",
    ) -> VirtualPage: ...

    def item(self, item_id: str, user_id: str, *, access_token: str = "") -> VirtualItem | None: ...

    def playback(
        self,
        item_id: str,
        user_id: str,
        *,
        play_session_id: str = "",
        access_token: str = "",
        user_agent: str = "",
    ) -> PlaybackTarget | None: ...


class PlaybackLeaseStore(Protocol):
    def acquire(
        self,
        user_id: str,
        item_id: str,
        *,
        play_session_id: str = "",
        device_id: str = "",
        client_signature: str = "",
        max_streams: int = 0,
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> PlaybackLease | None: ...

    def refresh(
        self,
        *,
        user_id: str,
        item_id: str,
        play_session_id: str = "",
        device_id: str = "",
        client_signature: str = "",
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> bool: ...

    def release(
        self,
        *,
        user_id: str,
        item_id: str = "",
        play_session_id: str = "",
        device_id: str = "",
    ) -> int: ...

    def reclaim_expired(self, *, now: datetime | None = None, limit: int = 1000) -> int: ...


@dataclass(frozen=True, slots=True)
class PlaybackLease:
    id: UUID
    lease_key: str
    user_id: str
    item_id: str
    play_session_id: str
    device_id: str
    client_signature: str
    expires_at: datetime
    created_at: datetime
    updated_at: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)


class InMemoryVirtualLibrary:
    """Deterministic Fake catalog used by proxy contract and pressure tests."""

    def __init__(self, *, server_id: str = "fixture-server") -> None:
        self.server_id = str(server_id or "fixture-server")
        self._libraries: dict[str, VirtualLibrary] = {}
        self._items: dict[str, VirtualItem] = {}
        self._lock = threading.RLock()

    def add_library(
        self,
        library_id: str,
        name: str,
        *,
        allowed_user_ids: Sequence[str] = (),
        item_types: Sequence[str] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> VirtualLibrary:
        library = VirtualLibrary(
            id=_required(library_id, "library_id"),
            name=_required(name, "name"),
            allowed_user_ids=tuple(
                dict.fromkeys(str(value).strip() for value in allowed_user_ids if str(value).strip())
            ),
            item_types=tuple(dict.fromkeys(str(value).strip() for value in item_types if str(value).strip())),
            metadata=dict(metadata or {}),
        )
        with self._lock:
            self._libraries[library.id] = library
        return library

    def add_item(self, item: VirtualItem, *, library_id: str | None = None) -> VirtualItem:
        if not isinstance(item, VirtualItem):
            raise TypeError("item must be a VirtualItem")
        parent = str(library_id or item.parent_id or "").strip()
        if parent and parent in self._libraries:
            item = VirtualItem(
                id=item.id,
                name=item.name,
                item_type=item.item_type,
                parent_id=parent,
                sort_name=item.sort_name,
                production_year=item.production_year,
                source_url=item.source_url,
                direct_url=item.direct_url,
                provider_ids=item.provider_ids,
                metadata=item.metadata,
            )
        with self._lock:
            self._items[item.id] = item
        return item

    def views(
        self,
        user_id: str,
        *,
        client_signature: str = "",
        access_token: str = "",
    ) -> Sequence[VirtualLibrary]:
        del client_signature, access_token
        user = str(user_id or "").strip()
        with self._lock:
            return tuple(
                library
                for library in self._libraries.values()
                if not library.allowed_user_ids or user in library.allowed_user_ids
            )

    def items(
        self,
        library_id: str,
        user_id: str,
        *,
        sort_by: str = "DateCreated",
        descending: bool = True,
        limit: int = 100,
        offset: int = 0,
        access_token: str = "",
    ) -> VirtualPage:
        del access_token
        library_key = from_virtual_library_id(library_id) if is_virtual_library_id(library_id) else str(library_id)
        visible_libraries = {library.id for library in self.views(user_id)}
        if library_key not in visible_libraries:
            return VirtualPage((), 0)
        library = self._libraries.get(library_key)
        with self._lock:
            values = [item for item in self._items.values() if item.parent_id == library_key]
        if library and library.item_types:
            allowed = {value.casefold() for value in library.item_types}
            values = [item for item in values if item.item_type.casefold() in allowed]
        values.sort(key=lambda item: _sort_value(item, sort_by), reverse=descending)
        start = max(offset, 0)
        return VirtualPage(tuple(values[start : start + max(limit, 0)]), len(values))

    def item(self, item_id: str, user_id: str, *, access_token: str = "") -> VirtualItem | None:
        del access_token
        identifier = str(item_id or "").strip()
        with self._lock:
            value = self._items.get(identifier)
            if value is not None and value.parent_id:
                library = self._libraries.get(value.parent_id)
                if library and library.allowed_user_ids and str(user_id) not in library.allowed_user_ids:
                    return None
            return value

    def playback(
        self,
        item_id: str,
        user_id: str,
        *,
        play_session_id: str = "",
        access_token: str = "",
        user_agent: str = "",
    ) -> PlaybackTarget | None:
        del play_session_id, user_agent
        item = self.item(item_id, user_id, access_token=access_token)
        if item is None or not item.direct_url:
            return None
        return PlaybackTarget(item.direct_url, virtual=True)


class InMemoryPlaybackLeaseStore:
    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.RLock()
        self._leases: dict[str, PlaybackLease] = {}

    def acquire(
        self,
        user_id: str,
        item_id: str,
        *,
        play_session_id: str = "",
        device_id: str = "",
        client_signature: str = "",
        max_streams: int = 0,
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> PlaybackLease | None:
        user = _required(user_id, "user_id")
        item = _required(item_id, "item_id")
        ttl = _ttl(ttl_seconds)
        if isinstance(max_streams, bool) or max_streams < 0:
            raise ValueError("max_streams must be a non-negative integer")
        timestamp = _aware(now or self._clock())
        key = playback_lease_key(user, item, play_session_id, device_id, client_signature)
        with self._lock:
            self._reclaim_locked(timestamp)
            existing = self._leases.get(key)
            if existing is not None:
                refreshed = _lease_replace(
                    existing,
                    expires_at=timestamp + timedelta(seconds=ttl),
                    updated_at=timestamp,
                )
                self._leases[key] = refreshed
                return refreshed
            active = sum(1 for lease in self._leases.values() if lease.user_id == user)
            if max_streams and active >= max_streams:
                return None
            lease = PlaybackLease(
                id=uuid5(NAMESPACE_URL, f"etk:proxy-lease:{key}"),
                lease_key=key,
                user_id=user,
                item_id=item,
                play_session_id=str(play_session_id or "").strip(),
                device_id=str(device_id or "").strip(),
                client_signature=str(client_signature or "").strip(),
                expires_at=timestamp + timedelta(seconds=ttl),
                created_at=timestamp,
                updated_at=timestamp,
            )
            self._leases[key] = lease
            return lease

    def refresh(
        self,
        *,
        user_id: str,
        item_id: str,
        play_session_id: str = "",
        device_id: str = "",
        client_signature: str = "",
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> bool:
        key = playback_lease_key(user_id, item_id, play_session_id, device_id, client_signature)
        timestamp = _aware(now or self._clock())
        with self._lock:
            self._reclaim_locked(timestamp)
            for lease_key, lease in self._leases.items():
                if lease_key == key or (
                    lease.user_id == str(user_id).strip()
                    and lease.item_id == str(item_id).strip()
                    and (not play_session_id or lease.play_session_id == str(play_session_id).strip())
                    and (not device_id or lease.device_id == str(device_id).strip())
                    and (not client_signature or lease.client_signature == str(client_signature).strip())
                ):
                    self._leases[lease_key] = _lease_replace(
                        lease,
                        expires_at=timestamp + timedelta(seconds=_ttl(ttl_seconds)),
                        updated_at=timestamp,
                    )
                    return True
        return False

    def release(
        self,
        *,
        user_id: str,
        item_id: str = "",
        play_session_id: str = "",
        device_id: str = "",
    ) -> int:
        user = str(user_id or "").strip()
        item = str(item_id or "").strip()
        session = str(play_session_id or "").strip()
        device = str(device_id or "").strip()
        with self._lock:
            keys = [
                key
                for key, lease in self._leases.items()
                if lease.user_id == user
                and (not item or lease.item_id == item)
                and (not session or lease.play_session_id == session)
                and (not device or lease.device_id == device)
            ]
            for key in keys:
                self._leases.pop(key, None)
            return len(keys)

    def reclaim_expired(self, *, now: datetime | None = None, limit: int = 1000) -> int:
        with self._lock:
            return self._reclaim_locked(_aware(now or self._clock()), limit=max(1, min(int(limit), 10_000)))

    def active_for_user(self, user_id: str) -> tuple[PlaybackLease, ...]:
        with self._lock:
            self._reclaim_locked(_aware(self._clock()))
            return tuple(lease for lease in self._leases.values() if lease.user_id == str(user_id).strip())

    def _reclaim_locked(self, now: datetime, limit: int = 10_000) -> int:
        expired = [key for key, lease in self._leases.items() if lease.expires_at <= now][:limit]
        for key in expired:
            self._leases.pop(key, None)
        return len(expired)


class PostgresPlaybackLeaseStore:
    """Persistent lease store using one PostgreSQL advisory lock per user."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def acquire(
        self,
        user_id: str,
        item_id: str,
        *,
        play_session_id: str = "",
        device_id: str = "",
        client_signature: str = "",
        max_streams: int = 0,
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> PlaybackLease | None:
        from sqlalchemy import text

        user = _required(user_id, "user_id")
        item = _required(item_id, "item_id")
        if isinstance(max_streams, bool) or max_streams < 0:
            raise ValueError("max_streams must be a non-negative integer")
        timestamp = _aware(now or datetime.now(UTC))
        expires = timestamp + timedelta(seconds=_ttl(ttl_seconds))
        key = playback_lease_key(user, item, play_session_id, device_id, client_signature)
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                {"lock_key": f"proxy:{user}"},
            )
            uow.session.execute(
                text("DELETE FROM proxy_playback_leases WHERE user_id = :user_id AND expires_at <= :now"),
                {"user_id": user, "now": timestamp},
            )
            existing = (
                uow.session.execute(
                    text("SELECT * FROM proxy_playback_leases WHERE lease_key = :lease_key FOR UPDATE"),
                    {"lease_key": key},
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                row = (
                    uow.session.execute(
                        text(
                            "UPDATE proxy_playback_leases SET expires_at = :expires_at, updated_at = :now "
                            "WHERE lease_key = :lease_key RETURNING *"
                        ),
                        {"lease_key": key, "expires_at": expires, "now": timestamp},
                    )
                    .mappings()
                    .one()
                )
                result = _lease_from_row(row)
                uow.commit()
                return result
            active = int(
                uow.session.scalar(
                    text("SELECT count(*) FROM proxy_playback_leases WHERE user_id = :user_id AND expires_at > :now"),
                    {"user_id": user, "now": timestamp},
                )
                or 0
            )
            if max_streams and active >= max_streams:
                uow.commit()
                return None
            row = (
                uow.session.execute(
                    text(
                        "INSERT INTO proxy_playback_leases("
                        "id, lease_key, user_id, item_id, play_session_id, device_id, client_signature, "
                        "expires_at, created_at, updated_at, metadata_json) VALUES("
                        ":id, :lease_key, :user_id, :item_id, :play_session_id, :device_id, :client_signature, "
                        ":expires_at, :created_at, :updated_at, '{}'::jsonb) RETURNING *"
                    ),
                    {
                        "id": uuid5(NAMESPACE_URL, f"etk:proxy-lease:{key}"),
                        "lease_key": key,
                        "user_id": user,
                        "item_id": item,
                        "play_session_id": str(play_session_id or "").strip() or None,
                        "device_id": str(device_id or "").strip() or None,
                        "client_signature": str(client_signature or "").strip() or None,
                        "expires_at": expires,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    },
                )
                .mappings()
                .one()
            )
            result = _lease_from_row(row)
            uow.commit()
            return result

    def refresh(
        self,
        *,
        user_id: str,
        item_id: str,
        play_session_id: str = "",
        device_id: str = "",
        client_signature: str = "",
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        now: datetime | None = None,
    ) -> bool:
        from sqlalchemy import text

        timestamp = _aware(now or datetime.now(UTC))
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                {"lock_key": f"proxy:{str(user_id).strip()}"},
            )
            row = (
                uow.session.execute(
                    text(
                        "SELECT id, created_at, updated_at FROM proxy_playback_leases "
                        "WHERE user_id = :user_id AND item_id = :item_id "
                        "AND (:play_session_id = '' OR play_session_id = :play_session_id) "
                        "AND (:device_id = '' OR device_id = :device_id) "
                        "AND (:client_signature = '' OR client_signature = :client_signature) "
                        "AND expires_at > :now ORDER BY updated_at DESC LIMIT 1 FOR UPDATE"
                    ),
                    {
                        "user_id": str(user_id).strip(),
                        "item_id": str(item_id).strip(),
                        "play_session_id": str(play_session_id or "").strip(),
                        "device_id": str(device_id or "").strip(),
                        "client_signature": str(client_signature or "").strip(),
                        "now": timestamp,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                uow.commit()
                return False
            # Tests and recovery tooling may inject a deterministic timestamp
            # later than wall-clock time.  Never move expiry behind the row's
            # creation/update timestamps because PostgreSQL enforces that
            # invariant on every update.
            created_at = _aware(row["created_at"])
            updated_at = _aware(row["updated_at"])
            base = max(timestamp, created_at, updated_at)
            result = uow.session.execute(
                text(
                    "UPDATE proxy_playback_leases SET expires_at = :expires_at, updated_at = :updated_at WHERE id = :id"
                ),
                {
                    "id": row["id"],
                    "expires_at": base + timedelta(seconds=_ttl(ttl_seconds)),
                    "updated_at": base,
                },
            )
            uow.commit()
            return bool(getattr(result, "rowcount", 0))

    def release(
        self,
        *,
        user_id: str,
        item_id: str = "",
        play_session_id: str = "",
        device_id: str = "",
    ) -> int:
        from sqlalchemy import text

        predicates = ["user_id = :user_id"]
        values: dict[str, Any] = {"user_id": str(user_id).strip()}
        for field_name, value in (("item_id", item_id), ("play_session_id", play_session_id), ("device_id", device_id)):
            normalized = str(value or "").strip()
            if normalized:
                predicates.append(f"{field_name} = :{field_name}")
                values[field_name] = normalized
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(f"DELETE FROM proxy_playback_leases WHERE {' AND '.join(predicates)}"),
                values,
            )
            count = int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
            return count

    def reclaim_expired(self, *, now: datetime | None = None, limit: int = 1000) -> int:
        from sqlalchemy import text

        timestamp = _aware(now or datetime.now(UTC))
        bounded = max(1, min(int(limit), 10_000))
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    "DELETE FROM proxy_playback_leases WHERE id IN (SELECT id FROM proxy_playback_leases "
                    "WHERE expires_at <= :now ORDER BY expires_at ASC LIMIT :limit)"
                ),
                {"now": timestamp, "limit": bounded},
            )
            count = int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
            return count


class ProxyUpstreamPort(Protocol):
    async def request(self, request: Request, path: str) -> Response: ...

    async def websocket(self, websocket: WebSocket, path: str) -> None: ...


def create_proxy_app(
    *,
    catalog: VirtualLibraryCatalog | None = None,
    leases: PlaybackLeaseStore | None = None,
    upstream: ProxyUpstreamPort | None = None,
    max_streams: int | Callable[[str], int] = 0,
    server_id: str | Callable[[], str] = "fixture-server",
    allow_transcoding: bool | Callable[[], bool] = False,
    enabled: bool | Callable[[], bool] = True,
    static_dir: str | None = None,
) -> FastAPI:
    """Build the standalone proxy ASGI application without starting a listener."""

    initial_server_id = "fixture-server" if catalog is not None else server_id() if callable(server_id) else server_id
    selected_catalog = catalog or InMemoryVirtualLibrary(server_id=initial_server_id)
    selected_leases = leases or InMemoryPlaybackLeaseStore()
    app = FastAPI(title="ETK Proxy", docs_url=None, redoc_url=None)
    app.state.catalog = selected_catalog
    app.state.leases = selected_leases
    app.state.upstream = upstream
    app.state.server_id = server_id

    def limit_for(user_id: str) -> int:
        value = max_streams(user_id) if callable(max_streams) else max_streams
        if isinstance(value, bool):
            return 0
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    def current_server_id() -> str:
        value = server_id() if callable(server_id) else server_id
        return str(value or "etk-proxy")

    def transcoding_allowed() -> bool:
        value = allow_transcoding() if callable(allow_transcoding) else allow_transcoding
        return bool(value)

    def proxy_enabled() -> bool:
        value = enabled() if callable(enabled) else enabled
        return bool(value)

    @app.get("/api/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
    async def proxy(request: Request, path: str) -> Response:
        normalized_path = "/" + path.lstrip("/")
        if not proxy_enabled():
            return JSONResponse({"status": "error", "message": "Proxy is disabled."}, status_code=503)
        if normalized_path.lower().endswith("/views") and "/users/" in normalized_path.lower():
            return await run_in_threadpool(_views_response, request, selected_catalog, current_server_id())
        parent_id = (
            request.query_params.get("ParentId")
            or request.query_params.get("parentId")
            or request.query_params.get("customViewId")
            or ""
        )
        if (
            normalized_path.lower().endswith("/items")
            and "/users/" in normalized_path.lower()
            and is_virtual_library_id(parent_id)
        ):
            return await run_in_threadpool(_items_response, request, selected_catalog, current_server_id())
        if is_virtual_library_id(parent_id) and normalized_path.lower().endswith("/items/latest"):
            response = await run_in_threadpool(_items_response, request, selected_catalog, current_server_id())
            if response.status_code != 200:
                return response
            payload = json.loads(bytes(response.body))
            return JSONResponse(payload.get("Items", []))
        if is_virtual_library_id(parent_id) and normalized_path.lower().endswith(
            ("/items/prefixes", "/genres", "/studios", "/tags", "/officialratings", "/years")
        ):
            return JSONResponse({"Items": [], "TotalRecordCount": 0})
        if _is_emby_transcode_request(request, normalized_path) and not transcoding_allowed():
            return Response("Emby transcoding fallback is disabled for 115 direct playback.", status_code=409)
        item_id = _path_item_id(normalized_path)
        if item_id and normalized_path.lower().endswith(("/stream", "/original")):
            try:
                playback = await run_in_threadpool(
                    _playback_response,
                    request,
                    item_id,
                    selected_catalog,
                    selected_leases,
                    limit_for,
                )
            except ProxyCatalogError as exc:
                return Response(str(exc), status_code=exc.status_code)
            if playback is not None:
                return playback
            if upstream is not None:
                return await upstream.request(request, normalized_path)
            if not transcoding_allowed():
                return Response("Only ETK virtual playback is supported.", status_code=409)
            return Response("Playback source unavailable.", status_code=503)
        if (
            item_id
            and is_virtual_library_id(item_id)
            and "/items/" in normalized_path.lower()
            and "/images/" not in normalized_path.lower()
            and request.method == "GET"
            and normalized_path.rstrip("/").endswith(f"/{item_id}")
        ):
            user_id = _user_from_path(normalized_path) or request.query_params.get("UserId", "")
            try:
                item = await run_in_threadpool(
                    selected_catalog.item,
                    item_id,
                    user_id,
                    access_token=_request_access_token(request),
                )
            except ProxyCatalogError as exc:
                return JSONResponse({"status": "error", "message": str(exc)}, status_code=exc.status_code)
            return JSONResponse(
                item.as_emby(server_id=current_server_id()) if item else {"status": "error"},
                status_code=200 if item else 404,
            )
        if normalized_path.lower().endswith("/playing/stopped"):
            local_response = await _playback_stopped_response(request, selected_leases)
            return await upstream.request(request, normalized_path) if upstream is not None else local_response
        if normalized_path.lower().endswith("/playing/progress") or normalized_path.lower().endswith("/playing"):
            local_response = await _playback_event_response(request, selected_leases, limit_for)
            if local_response.status_code == 429 or upstream is None:
                return local_response
            return await upstream.request(request, normalized_path)
        if upstream is not None:
            return await upstream.request(request, normalized_path)
        if "/videos/" in normalized_path.lower() and "/stream" in normalized_path.lower() and not transcoding_allowed():
            return Response("Only ETK virtual playback is supported.", status_code=409)
        return JSONResponse({"status": "error", "message": "Proxy route not found."}, status_code=404)

    async def websocket_proxy(websocket: WebSocket) -> None:
        if upstream is not None:
            await upstream.websocket(websocket, websocket.url.path)
            return
        await websocket.accept()
        try:
            while True:
                message = await websocket.receive()
                if message.get("type") == "websocket.disconnect":
                    return
                if message.get("text") is not None:
                    await websocket.send_text(str(message["text"]))
                elif message.get("bytes") is not None:
                    await websocket.send_bytes(bytes(message["bytes"]))
        except WebSocketDisconnect:
            return

    app.websocket("/embywebsocket")(websocket_proxy)
    app.websocket("/emby/embywebsocket")(websocket_proxy)
    if static_dir:
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    return app


def _views_response(request: Request, catalog: VirtualLibraryCatalog, server_id: str) -> JSONResponse:
    user_id = _user_from_path(request.url.path) or request.query_params.get("UserId", "")
    signature = request.headers.get("X-Emby-Client", "")
    items = []
    try:
        libraries = catalog.views(
            user_id,
            client_signature=signature,
            access_token=_request_access_token(request),
        )
    except ProxyCatalogError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=exc.status_code)
    for library in libraries:
        item = {
            "Name": library.name,
            "ServerId": server_id,
            "Id": library.virtual_id,
            "Guid": stable_virtual_token(library.id, "identity"),
            "Etag": stable_virtual_token(library.id, "etag"),
            "Type": "CollectionFolder",
            "IsFolder": True,
            "ParentId": "2",
            "CollectionType": _collection_type(library.item_types, signature),
            "UserData": {"PlaybackPositionTicks": 0, "IsFavorite": False, "Played": False},
            **dict(library.metadata),
        }
        items.append({key: value for key, value in item.items() if value is not None})
    return JSONResponse({"Items": items, "TotalRecordCount": len(items)})


def _items_response(request: Request, catalog: VirtualLibraryCatalog, server_id: str) -> JSONResponse:
    user_id = _user_from_path(request.url.path) or request.query_params.get("UserId", "")
    parent = (
        request.query_params.get("ParentId")
        or request.query_params.get("parentId")
        or request.query_params.get("customViewId")
        or ""
    )
    if not parent:
        return JSONResponse({"Items": [], "TotalRecordCount": 0})
    sort_by = request.query_params.get("SortBy", "DateCreated")
    descending = request.query_params.get("SortOrder", "Descending").casefold() != "ascending"
    limit = _query_int(request, "Limit", 100, 0, 1000)
    offset = _query_int(request, "StartIndex", 0, 0, 10_000_000)
    try:
        page = catalog.items(
            parent,
            user_id,
            sort_by=sort_by,
            descending=descending,
            limit=limit,
            offset=offset,
            access_token=_request_access_token(request),
        )
    except ProxyCatalogError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=exc.status_code)
    return JSONResponse(
        {
            "Items": [item.as_emby(server_id=server_id) for item in page.items],
            "TotalRecordCount": page.total,
        }
    )


def _playback_response(
    request: Request,
    item_id: str,
    catalog: VirtualLibraryCatalog,
    leases: PlaybackLeaseStore,
    max_streams: Callable[[str], int],
) -> Response | None:
    user_id = _user_from_path(request.url.path) or request.query_params.get("UserId", "")
    play_session_id = request.query_params.get("PlaySessionId", "")
    target = catalog.playback(
        item_id,
        user_id,
        play_session_id=play_session_id,
        access_token=_request_access_token(request),
        user_agent=request.headers.get("User-Agent", ""),
    )
    if target is None:
        return None
    lease = leases.acquire(
        user_id,
        item_id,
        play_session_id=play_session_id,
        device_id=request.headers.get("X-Emby-Device-Id", "") or request.query_params.get("DeviceId", ""),
        client_signature=request.headers.get("X-Emby-Client", ""),
        max_streams=max_streams(user_id),
    )
    if lease is None:
        return Response("Playback concurrency limit exceeded.", status_code=429)
    response = RedirectResponse(target.direct_url, status_code=302)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["X-ETK-Playback-Lease"] = str(lease.id)
    return response


async def _playback_event_response(
    request: Request,
    leases: PlaybackLeaseStore,
    max_streams: Callable[[str], int],
) -> Response:
    data = await _request_json(request)
    user_id = str(data.get("UserId") or _user_from_path(request.url.path) or "").strip()
    item_id = str(data.get("ItemId") or (data.get("Item") or {}).get("Id") or "").strip()
    play_session_id = str(
        data.get("PlaySessionId") or (data.get("PlaybackInfo") or {}).get("PlaySessionId") or ""
    ).strip()
    device_id = str(data.get("DeviceId") or request.headers.get("X-Emby-Device-Id", "") or "").strip()
    client_signature = str(data.get("Client") or request.headers.get("X-Emby-Client", "") or "").strip()
    if not user_id or not item_id:
        return JSONResponse({"status": "error", "message": "UserId and ItemId are required."}, status_code=400)
    if request.url.path.lower().endswith("/progress"):
        ok = leases.refresh(
            user_id=user_id,
            item_id=item_id,
            play_session_id=play_session_id,
            device_id=device_id,
            client_signature=client_signature,
        )
        return JSONResponse({"status": "ok" if ok else "missing"}, status_code=200 if ok else 404)
    lease = leases.acquire(
        user_id,
        item_id,
        play_session_id=play_session_id,
        device_id=device_id,
        client_signature=client_signature,
        max_streams=max_streams(user_id),
    )
    if lease is None:
        return JSONResponse(
            {"status": "error", "message": "Playback concurrency limit exceeded."},
            status_code=429,
        )
    return JSONResponse({"status": "ok", "lease_id": str(lease.id)})


async def _playback_stopped_response(request: Request, leases: PlaybackLeaseStore) -> Response:
    data = await _request_json(request)
    user_id = str(data.get("UserId") or _user_from_path(request.url.path) or "").strip()
    item_id = str(data.get("ItemId") or (data.get("Item") or {}).get("Id") or "").strip()
    play_session_id = str(data.get("PlaySessionId") or "").strip()
    device_id = str(data.get("DeviceId") or "").strip()
    removed = leases.release(user_id=user_id, item_id=item_id, play_session_id=play_session_id, device_id=device_id)
    return JSONResponse({"status": "ok", "released": removed})


async def _request_json(request: Request) -> dict[str, Any]:
    try:
        value = await request.json()
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _path_item_id(path: str) -> str:
    parts = [part for part in path.split("/") if part]
    for index, part in enumerate(parts):
        if part.casefold() in {"videos", "items"} and index + 1 < len(parts):
            return parts[index + 1]
    return ""


def _user_from_path(path: str) -> str:
    parts = [part for part in path.split("/") if part]
    for index, part in enumerate(parts):
        if part.casefold() == "users" and index + 1 < len(parts):
            return parts[index + 1]
    return ""


def _query_int(request: Request, key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(request.query_params.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _request_access_token(request: Request) -> str:
    for key in ("api_key", "AccessToken", "X-Emby-Token", "X-MediaBrowser-Token"):
        value = str(request.query_params.get(key) or "").strip()
        if value:
            return value
    for key in ("X-Emby-Token", "X-MediaBrowser-Token"):
        value = str(request.headers.get(key) or "").strip()
        if value:
            return value
    for key in ("Authorization", "X-Emby-Authorization", "X-MediaBrowser-Authorization"):
        value = str(request.headers.get(key) or "").strip()
        if not value:
            continue
        match = re.search(r'(?:Token|token)\s*=\s*"?([^",\s]+)', value)
        if match:
            return match.group(1).strip()
        match = re.search(r"Bearer\s+([^,\s]+)", value, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _is_emby_transcode_request(request: Request, path: str) -> bool:
    normalized = str(path or "").casefold()
    if "/videos/" not in normalized:
        return False
    if any(value in normalized for value in ("master.m3u8", "main.m3u8", ".m3u8", "/hls", "transcode")):
        return True
    return bool(
        request.query_params.get("TranscodeReasons")
        or request.query_params.get("TranscodingMaxAudioChannels")
    )


def _collection_type(item_types: Sequence[str], client_signature: str) -> str | None:
    if "emby web" in client_signature.casefold():
        return "mixed"
    normalized = {str(value).casefold() for value in item_types if str(value).strip()}
    if normalized == {"movie"}:
        return "movies"
    if normalized and normalized <= {"series", "season", "episode"}:
        return "tvshows"
    return None


def _sort_value(item: VirtualItem, sort_by: str) -> Any:
    key = str(sort_by or "").split(",", 1)[0].strip().casefold()
    if key in {"productionyear", "year"}:
        return item.production_year or 0
    if key in {"sortname", "name"}:
        return (item.sort_name or item.name).casefold()
    return (item.metadata.get("DateCreated") or item.metadata.get("date_created") or item.name).casefold()


def virtual_library_id(library_id: str | int) -> str:
    normalized = _required(library_id, "library_id")
    try:
        value = int(normalized)
    except ValueError:
        value = int.from_bytes(hashlib.sha256(normalized.encode()).digest()[:4], "big") % 100_000 + 1
    return str(-(MIMICKED_ID_BASE + value))


def is_virtual_library_id(value: str) -> bool:
    try:
        return int(str(value).strip()) <= -MIMICKED_ID_BASE
    except (TypeError, ValueError):
        return False


def from_virtual_library_id(value: str) -> str:
    if not is_virtual_library_id(value):
        raise ValueError("invalid virtual library id")
    return str(-int(str(value).strip()) - MIMICKED_ID_BASE)


def stable_virtual_token(library_id: str, purpose: str) -> str:
    return uuid5(NAMESPACE_URL, f"emby-toolkit:virtual-library:{library_id}:{purpose}").hex


def playback_lease_key(
    user_id: str,
    item_id: str,
    play_session_id: str = "",
    device_id: str = "",
    client_signature: str = "",
) -> str:
    user = _required(user_id, "user_id")
    item = _required(item_id, "item_id")
    identity = str(play_session_id or "").strip() or (
        f"device:{str(device_id or '').strip()}|client:{str(client_signature or '').strip()}"
    )
    return hashlib.sha256(f"{user}\0{item}\0{identity}".encode()).hexdigest()


def _lease_replace(lease: PlaybackLease, **updates: Any) -> PlaybackLease:
    values = {
        "id": lease.id,
        "lease_key": lease.lease_key,
        "user_id": lease.user_id,
        "item_id": lease.item_id,
        "play_session_id": lease.play_session_id,
        "device_id": lease.device_id,
        "client_signature": lease.client_signature,
        "expires_at": lease.expires_at,
        "created_at": lease.created_at,
        "updated_at": lease.updated_at,
        "metadata": lease.metadata,
    }
    values.update(updates)
    return PlaybackLease(**values)


def _lease_from_row(row: Mapping[Any, Any]) -> PlaybackLease:
    return PlaybackLease(
        id=UUID(str(row["id"])),
        lease_key=str(row["lease_key"]),
        user_id=str(row["user_id"]),
        item_id=str(row["item_id"]),
        play_session_id=str(row.get("play_session_id") or ""),
        device_id=str(row.get("device_id") or ""),
        client_signature=str(row.get("client_signature") or ""),
        expires_at=_aware(row.get("expires_at")),
        created_at=_aware(row.get("created_at")),
        updated_at=_aware(row.get("updated_at")),
        metadata=dict(row.get("metadata_json") or {}),
    )


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _ttl(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("ttl_seconds must be finite and positive") from exc
    if result <= 0 or not (result < float("inf") and result > float("-inf")):
        raise ValueError("ttl_seconds must be finite and positive")
    return min(result, 7 * 24 * 60 * 60)


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.now(UTC)


__all__ = [
    "DEFAULT_LEASE_TTL_SECONDS",
    "InMemoryPlaybackLeaseStore",
    "InMemoryVirtualLibrary",
    "MIMICKED_ID_BASE",
    "PlaybackLease",
    "PlaybackLeaseStore",
    "PlaybackTarget",
    "PostgresPlaybackLeaseStore",
    "ProxyCatalogError",
    "ProxyUpstreamPort",
    "VirtualItem",
    "VirtualLibrary",
    "VirtualLibraryCatalog",
    "VirtualPage",
    "create_proxy_app",
    "from_virtual_library_id",
    "is_virtual_library_id",
    "playback_lease_key",
    "stable_virtual_token",
    "virtual_library_id",
]
