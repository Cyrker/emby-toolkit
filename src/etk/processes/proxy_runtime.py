"""Production wiring for the standalone Native Proxy process."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import httpx
from fastapi import FastAPI
from sqlalchemy import URL, text
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from websockets.asyncio.client import connect as websocket_connect
from websockets.typing import Subprotocol

from etk.domains.collection.custom_task_adapters import SqlAlchemyCustomCollectionLibraryQueryAdapter
from etk.domains.collection.repository import SqlAlchemyCollectionRepository
from etk.domains.p115.credential_repository import SqlAlchemyP115CredentialRepository
from etk.domains.p115.direct_url_transport import P115SdkDirectUrlGateway
from etk.domains.sharing.configuration import SharingServerIdentity
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform import LegacyConfigAdapter, PostgresLegacyConfigStore, Settings
from etk.platform.db import DatabaseRuntime, EngineConfig, SqlAlchemyUnitOfWork

from .proxy import (
    PlaybackTarget,
    PostgresPlaybackLeaseStore,
    ProxyCatalogError,
    ProxyUpstreamPort,
    VirtualItem,
    VirtualLibrary,
    VirtualPage,
    create_proxy_app,
    from_virtual_library_id,
    is_virtual_library_id,
)

logger = logging.getLogger(__name__)

_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_WEBSOCKET_HANDSHAKE_HEADERS = _HOP_BY_HOP_HEADERS | frozenset(
    {
        "host",
        "sec-websocket-accept",
        "sec-websocket-extensions",
        "sec-websocket-key",
        "sec-websocket-protocol",
        "sec-websocket-version",
    }
)
_ITEM_FIELDS = (
    "PrimaryImageAspectRatio,ImageTags,BackdropImageTags,HasPrimaryImage,ProviderIds,"
    "UserData,Name,SortName,ProductionYear,CommunityRating,DateCreated,PremiereDate,"
    "Type,RecursiveItemCount,ChildCount,BasicSyncInfo,MediaSources,Path"
)


class ProxyRuntimeSettings:
    """Keep one process-local Settings snapshot synchronized with PostgreSQL."""

    def __init__(
        self,
        adapter: LegacyConfigAdapter,
        settings: Settings,
        database: DatabaseRuntime,
        *,
        refresh_interval_seconds: float = 1.0,
    ) -> None:
        self._adapter = adapter
        self._settings = settings
        self.database = database
        self._refresh_interval = max(0.1, float(refresh_interval_seconds))
        self._next_refresh = 0.0
        self._lock = threading.Lock()

    @property
    def settings(self) -> Settings:
        with self._lock:
            return self._settings

    def refresh(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        with self._lock:
            if not force and now < self._next_refresh:
                return False
            refreshed = self._adapter.load()
            changed = refreshed != self._settings
            self._settings = refreshed
            self._next_refresh = now + self._refresh_interval
            return changed

    def close(self) -> None:
        self.database.close()


class EmbyCatalogClient:
    """Bounded direct-LAN reads used to build synthetic Emby library responses."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory

    def user_views(self, user_id: str, access_token: str) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"/Users/{quote(_required(user_id, 'user_id'), safe='')}/Views",
            access_token,
        )
        return _mapping_items(payload)

    def user_items(
        self,
        user_id: str,
        access_token: str,
        *,
        item_ids: Sequence[str] = (),
        parent_id: str = "",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "Recursive": "true",
            "Fields": _ITEM_FIELDS,
            "EnableImages": "true",
            "Limit": max(1, min(len(item_ids) or 10_000, 10_000)),
        }
        if item_ids:
            params["Ids"] = ",".join(item_ids)
        if parent_id:
            params["ParentId"] = parent_id
        payload = self._get_json(
            f"/Users/{quote(_required(user_id, 'user_id'), safe='')}/Items",
            access_token,
            params=params,
        )
        return _mapping_items(payload)

    def user(self, user_id: str, access_token: str) -> Mapping[str, Any]:
        return self._get_json(
            f"/Users/{quote(_required(user_id, 'user_id'), safe='')}",
            access_token,
        )

    def playback_info(
        self,
        item_id: str,
        user_id: str,
        access_token: str,
        *,
        play_session_id: str = "",
    ) -> Mapping[str, Any]:
        params: dict[str, Any] = {"MaxStreamingBitrate": 140_000_000}
        if str(user_id or "").strip():
            params["UserId"] = str(user_id).strip()
        if str(play_session_id or "").strip():
            params["PlaySessionId"] = str(play_session_id).strip()
        return self._get_json(
            f"/Items/{quote(_required(item_id, 'item_id'), safe='')}/PlaybackInfo",
            access_token,
            params=params,
        )

    def _get_json(
        self,
        path: str,
        access_token: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        token = _required_token(access_token)
        settings = self._settings()
        target = _emby_target(settings.emby_server_url, path)
        timeout = max(1.0, min(float(settings.emby_api_timeout), 180.0))
        factory = self._client_factory or (
            lambda: httpx.Client(timeout=timeout, follow_redirects=False, trust_env=False)
        )
        try:
            with factory() as client:
                response = client.get(
                    target,
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=dict(params or {}),
                )
        except httpx.TimeoutException as exc:
            raise ProxyCatalogError("Emby request timed out.", status_code=504) from exc
        except httpx.HTTPError as exc:
            raise ProxyCatalogError("Emby request failed.", status_code=502) from exc
        if not 200 <= response.status_code < 300:
            status = response.status_code if 400 <= response.status_code < 500 else 502
            raise ProxyCatalogError("Emby rejected the proxy request.", status_code=status)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ProxyCatalogError("Emby returned invalid JSON.", status_code=502) from exc
        if not isinstance(payload, Mapping):
            raise ProxyCatalogError("Emby returned an invalid response.", status_code=502)
        return payload


class SqlAlchemyProxyCatalog:
    """Production virtual-library catalog backed by PostgreSQL and real Emby reads."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: Callable[[], Settings],
        collections: SqlAlchemyCollectionRepository,
        library_query: SqlAlchemyCustomCollectionLibraryQueryAdapter,
        emby: EmbyCatalogClient,
        identity: SharingServerIdentity,
        p115_direct_url: Callable[[str, str], str] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._collections = collections
        self._library_query = library_query
        self._emby = emby
        self._identity = identity
        self._p115_direct_url = p115_direct_url

    def server_id(self) -> str:
        try:
            return self._identity.server_id()
        except Exception:
            logger.exception("Proxy could not resolve the Emby Server ID")
            return "etk-proxy"

    def views(
        self,
        user_id: str,
        *,
        client_signature: str = "",
        access_token: str = "",
    ) -> Sequence[VirtualLibrary]:
        user = _required(user_id, "user_id")
        raw_native = self._emby.user_views(user, access_token)
        native = tuple(self._native_library(value, user) for value in raw_native)
        if _is_official_ios(client_signature):
            return native

        settings = self._settings()
        virtual = tuple(
            self._virtual_library(row)
            for row in self._collections.list_active_for_execution()
            if str(row.get("emby_collection_id") or "").strip() and _allowed(row, user)
        )
        if not settings.proxy_merge_native_libraries:
            return virtual
        selected = {str(value).strip() for value in settings.proxy_native_view_selection if str(value).strip()}
        selected_native = tuple(
            library for library in native if str(library.metadata.get("Id") or "") in selected
        )
        return (
            (*virtual, *selected_native)
            if settings.proxy_native_view_order == "after"
            else (*selected_native, *virtual)
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
        if not is_virtual_library_id(library_id):
            return VirtualPage((), 0)
        collection_id = int(from_virtual_library_id(library_id))
        collection = self._collections.get_custom(collection_id)
        user = _required(user_id, "user_id")
        if collection is None or str(collection.get("status") or "") != "active" or not _allowed(collection, user):
            return VirtualPage((), 0)

        definition = _mapping(collection.get("definition_json"))
        collection_type = str(collection.get("type") or "").strip()
        item_ids: tuple[str, ...]
        if collection_type == "filter":
            snapshot = self._library_query.query_filter(
                definition,
                user_id=user,
                execution_key=f"proxy:{collection_id}:{user}",
            )
            item_ids = tuple(dict.fromkeys(snapshot.emby_ids))
        elif collection_type == "ai_recommendation":
            snapshot = self._library_query.query_personal_recommendation(
                definition,
                user_id=user,
                execution_key=f"proxy:{collection_id}:{user}",
            )
            item_ids = tuple(dict.fromkeys(snapshot.emby_ids))
        else:
            item_ids = tuple(
                dict.fromkeys(
                    str(item.get("emby_id") or item.get("Id") or "").strip()
                    for item in _mapping_sequence(collection.get("generated_media_info_json"))
                    if str(item.get("emby_id") or item.get("Id") or "").strip()
                )
            )

        raw_items: list[dict[str, Any]] = []
        if item_ids:
            for start in range(0, len(item_ids), 100):
                raw_items.extend(self._emby.user_items(user, access_token, item_ids=item_ids[start : start + 100]))
        elif parent_id := str(collection.get("emby_collection_id") or "").strip():
            raw_items = self._emby.user_items(user, access_token, parent_id=parent_id)

        values = [_virtual_item(value) for value in raw_items if str(value.get("Id") or "").strip()]
        values.sort(key=lambda item: _item_sort_value(item, sort_by), reverse=descending)
        bounded_offset = max(0, int(offset))
        bounded_limit = max(0, min(int(limit), 1000))
        return VirtualPage(tuple(values[bounded_offset : bounded_offset + bounded_limit]), len(values))

    def item(self, item_id: str, user_id: str, *, access_token: str = "") -> VirtualItem | None:
        if not is_virtual_library_id(item_id):
            return None
        user = _required(user_id, "user_id")
        authenticated = self._emby.user(user, access_token)
        if str(authenticated.get("Id") or "").strip() != user:
            raise ProxyCatalogError("Emby user identity does not match the request.", status_code=403)
        collection = self._collections.get_custom(int(from_virtual_library_id(item_id)))
        if collection is None or str(collection.get("status") or "") != "active" or not _allowed(collection, user):
            return None
        library = self._virtual_library(collection)
        return VirtualItem(
            id=item_id,
            name=library.name,
            item_type="CollectionFolder",
            parent_id="2",
            metadata={
                "Id": item_id,
                "Type": "CollectionFolder",
                "IsFolder": True,
                "CollectionType": _collection_type(library.item_types),
                **dict(library.metadata),
            },
        )

    def playback(
        self,
        item_id: str,
        user_id: str,
        *,
        play_session_id: str = "",
        access_token: str = "",
        user_agent: str = "",
    ) -> PlaybackTarget | None:
        try:
            playback_info = self._emby.playback_info(
                item_id,
                user_id,
                access_token,
                play_session_id=play_session_id,
            )
        except ProxyCatalogError:
            return None
        is_p115, pick_code = _p115_playback_identity(playback_info)
        if not is_p115:
            return None
        if not pick_code:
            raise ProxyCatalogError("Virtual 115 direct playback is not available.", status_code=503)
        if self._p115_direct_url is None:
            raise ProxyCatalogError("115 direct playback is not configured.", status_code=503)
        try:
            direct_url = str(self._p115_direct_url(pick_code, user_agent) or "").strip()
        except Exception as exc:
            logger.exception("P115 direct playback resolution failed")
            raise ProxyCatalogError("115 direct playback failed.", status_code=503) from exc
        if not direct_url:
            raise ProxyCatalogError("115 direct playback did not return a URL.", status_code=503)
        return PlaybackTarget(direct_url=direct_url, virtual=False)

    def _native_library(self, value: Mapping[str, Any], user_id: str) -> VirtualLibrary:
        item_id = _required(value.get("Id"), "native library id")
        collection_type = str(value.get("CollectionType") or "").casefold()
        item_types = ("Movie",) if collection_type == "movies" else ("Series",) if collection_type == "tvshows" else ()
        return VirtualLibrary(
            id=f"native:{item_id}",
            name=str(value.get("Name") or item_id),
            allowed_user_ids=(user_id,),
            item_types=item_types,
            metadata=dict(value),
        )

    def _virtual_library(self, row: Mapping[str, Any]) -> VirtualLibrary:
        collection_id = _required(row.get("id"), "collection id")
        definition = _mapping(row.get("definition_json"))
        item_types = _strings(definition.get("item_type")) or _strings(row.get("item_type"))
        return VirtualLibrary(
            id=collection_id,
            name=_required(row.get("name"), "collection name"),
            allowed_user_ids=_strings(row.get("allowed_user_ids")),
            item_types=item_types,
            metadata={
                "DateCreated": "2025-01-01T00:00:00.0000000Z",
                "DateModified": "0001-01-01T00:00:00.0000000Z",
                "CanDelete": False,
                "CanDownload": False,
                "SortName": str(row.get("name") or ""),
                "ServerId": self.server_id(),
            },
        )


class SqlAlchemyProxyUserLimits:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def max_streams(self, user_id: str) -> int:
        user = str(user_id or "").strip()
        if not user:
            return 0
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT CASE
                        WHEN COALESCE(users.is_administrator, FALSE) THEN 0
                        ELSE COALESCE(templates.max_concurrent_streams, 0)
                    END
                    FROM emby_users AS users
                    LEFT JOIN emby_users_extended AS extended ON extended.emby_user_id = users.id
                    LEFT JOIN user_templates AS templates ON templates.id = extended.template_id
                    WHERE users.id = :user_id
                    """
                ),
                {"user_id": user},
            )
        return max(0, int(value or 0))


class HttpxEmbyProxyUpstream(ProxyUpstreamPort):
    """Transparent HTTP/WebSocket forwarding to Emby without environment proxies."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._client = client or httpx.AsyncClient(follow_redirects=False, trust_env=False, timeout=None)
        self._owns_client = client is None

    async def request(self, request: Request, path: str) -> Response:
        settings = self._settings()
        try:
            target = _emby_target(settings.emby_server_url, path)
            body = await request.body()
            upstream_request = self._client.build_request(
                request.method,
                target,
                params=str(request.url.query) or None,
                headers=_request_headers(request),
                content=body,
                timeout=httpx.Timeout(connect=10.0, read=1800.0, write=1800.0, pool=30.0),
            )
            upstream = await self._client.send(upstream_request, stream=True)
        except (httpx.HTTPError, ValueError):
            logger.exception("Emby HTTP proxy request failed")
            return JSONResponse({"status": "error", "message": "Emby upstream unavailable."}, status_code=502)

        response = StreamingResponse(
            upstream.aiter_raw(),
            status_code=upstream.status_code,
            background=BackgroundTask(upstream.aclose),
        )
        response.raw_headers = [
            (name.encode("latin-1"), value.encode("latin-1"))
            for name, value in upstream.headers.multi_items()
            if name.casefold() not in _HOP_BY_HOP_HEADERS
        ]
        return response

    async def websocket(self, websocket: WebSocket, path: str) -> None:
        settings = self._settings()
        try:
            target = _websocket_target(settings.emby_server_url, path, websocket.url.query)
            protocols = tuple(
                Subprotocol(value.strip())
                for value in str(websocket.headers.get("sec-websocket-protocol") or "").split(",")
                if value.strip()
            )
            headers = [
                (name, value)
                for name, value in websocket.headers.items()
                if name.casefold() not in _WEBSOCKET_HANDSHAKE_HEADERS
            ]
            async with websocket_connect(
                target,
                additional_headers=headers,
                subprotocols=protocols or None,
                proxy=None,
                max_size=None,
                open_timeout=10,
                close_timeout=10,
            ) as upstream:
                await websocket.accept(subprotocol=upstream.subprotocol)
                client_to_upstream = asyncio.create_task(_websocket_to_upstream(websocket, upstream))
                upstream_to_client = asyncio.create_task(_upstream_to_websocket(upstream, websocket))
                done, pending = await asyncio.wait(
                    {client_to_upstream, upstream_to_client},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                for task in (*done, *pending):
                    with suppress(asyncio.CancelledError, WebSocketDisconnect):
                        await task
                if websocket.application_state is WebSocketState.CONNECTED:
                    await websocket.close(code=1000)
        except WebSocketDisconnect:
            return
        except Exception:
            logger.exception("Emby WebSocket proxy failed")
            if websocket.application_state is WebSocketState.CONNECTED:
                with suppress(RuntimeError):
                    await websocket.close(code=1011)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def create_production_proxy_app(*, data_dir: Path | None = None) -> FastAPI:
    runtime = _create_runtime(data_dir)

    def settings() -> Settings:
        return runtime.settings

    app_settings = SqlAlchemyAppSettingsRepository(runtime.database.unit_of_work)
    emby = EmbyCatalogClient(settings)
    p115_credentials = SqlAlchemyP115CredentialRepository(runtime.database.unit_of_work)
    p115_direct_urls = P115SdkDirectUrlGateway(
        credentials=p115_credentials,
        priority=lambda: str(settings().p115_api_priority),
    )

    def resolve_p115_direct_url(pick_code: str, user_agent: str) -> str:
        account_id = str(p115_credentials.load().account_id or "").strip()
        if not account_id:
            return ""
        resolved = p115_direct_urls.resolve(
            pick_code,
            expected_account_id=account_id,
            user_agent=user_agent,
        )
        return resolved.url if resolved is not None else ""

    catalog = SqlAlchemyProxyCatalog(
        runtime.database.unit_of_work,
        settings,
        SqlAlchemyCollectionRepository(runtime.database.unit_of_work),
        SqlAlchemyCustomCollectionLibraryQueryAdapter(
            runtime.database.unit_of_work,
            settings,
            app_settings,
        ),
        emby,
        SharingServerIdentity(settings, app_settings),
        resolve_p115_direct_url,
    )
    upstream = HttpxEmbyProxyUpstream(settings)
    limits = SqlAlchemyProxyUserLimits(runtime.database.unit_of_work)
    app = create_proxy_app(
        catalog=catalog,
        leases=PostgresPlaybackLeaseStore(runtime.database.unit_of_work),
        upstream=upstream,
        max_streams=limits.max_streams,
        server_id=catalog.server_id,
        allow_transcoding=lambda: settings().proxy_allow_transcoding,
        enabled=lambda: settings().proxy_enabled,
        static_dir=os.environ.get("ETK_STATIC_DIR"),
    )
    app.state.proxy_runtime = runtime

    @app.middleware("http")
    async def synchronize_proxy_settings(request: Request, call_next: Callable[..., Any]) -> Response:
        if request.url.path != "/api/health":
            try:
                await run_in_threadpool(runtime.refresh)
            except Exception:
                logger.exception("Proxy runtime configuration synchronization failed")
                return JSONResponse(
                    {"status": "error", "message": "Proxy configuration unavailable."},
                    status_code=503,
                )
        return await call_next(request)

    async def close_runtime() -> None:
        try:
            await upstream.close()
        finally:
            await run_in_threadpool(runtime.close)

    app.router.add_event_handler("shutdown", close_runtime)
    return app


def _create_runtime(data_dir: Path | None) -> ProxyRuntimeSettings:
    configured_root = data_dir
    if configured_root is None:
        environment_root = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
        configured_root = Path(environment_root) if environment_root else None
    config_path = configured_root / "config.ini" if configured_root is not None else None
    bootstrap_adapter = LegacyConfigAdapter(config_path, environ=os.environ)
    bootstrap = bootstrap_adapter.load()
    database_url = os.environ.get("ETK_DATABASE_URL") or _database_url(bootstrap)
    store = PostgresLegacyConfigStore(database_url)
    adapter = LegacyConfigAdapter(config_path, environ=os.environ, dynamic_store=store)
    settings = adapter.load()
    database = DatabaseRuntime.create(EngineConfig(database_url, application_name="etk-proxy"))
    database.start()
    return ProxyRuntimeSettings(adapter, settings, database)


def _database_url(settings: Settings) -> str:
    return URL.create(
        "postgresql+psycopg",
        username=settings.db_user,
        password=settings.db_password.get_secret_value(),
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    ).render_as_string(hide_password=False)


def _emby_target(base_url: str, path: str) -> str:
    raw_base = str(base_url or "").strip()
    parsed = urlsplit(raw_base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Emby Server URL is not configured")
    incoming = "/" + str(path or "").lstrip("/")
    base_path = parsed.path.rstrip("/")
    if base_path and (incoming == base_path or incoming.startswith(f"{base_path}/")):
        target_path = incoming
    elif base_path:
        target_path = f"{base_path}{incoming}"
    else:
        target_path = incoming
    return urlunsplit((parsed.scheme, parsed.netloc, target_path, "", ""))


def _websocket_target(base_url: str, path: str, query: str) -> str:
    parsed = urlsplit(_emby_target(base_url, path))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, parsed.path, query, ""))


def _request_headers(request: Request) -> list[tuple[str, str]]:
    return [
        (name, value)
        for name, value in request.headers.items()
        if name.casefold() not in _HOP_BY_HOP_HEADERS | {"host", "content-length"}
    ]


async def _websocket_to_upstream(websocket: WebSocket, upstream: Any) -> None:
    while True:
        message = await websocket.receive()
        if message.get("type") == "websocket.disconnect":
            return
        if message.get("text") is not None:
            await upstream.send(str(message["text"]))
        elif message.get("bytes") is not None:
            await upstream.send(bytes(message["bytes"]))


async def _upstream_to_websocket(upstream: Any, websocket: WebSocket) -> None:
    async for message in upstream:
        if isinstance(message, bytes):
            await websocket.send_bytes(message)
        else:
            await websocket.send_text(str(message))


def _mapping_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = payload.get("Items")
    if not isinstance(values, list):
        raise ProxyCatalogError("Emby response does not contain Items.", status_code=502)
    return [dict(value) for value in values if isinstance(value, Mapping)]


def _p115_playback_identity(payload: Mapping[str, Any]) -> tuple[bool, str]:
    raw_sources = payload.get("MediaSources")
    if not isinstance(raw_sources, list):
        return False, ""
    for raw_source in raw_sources:
        if not isinstance(raw_source, Mapping):
            continue
        source = _strm_source(str(raw_source.get("Path") or ""))
        if not source:
            continue
        decoded_path = unquote(urlsplit(source).path)
        direct = re.search(r"/api/p115/play/([^/?#]+)", decoded_path, re.IGNORECASE)
        if direct:
            return True, unquote(direct.group(1)).strip()
        if re.search(r"/api/p115/virtual-play/", decoded_path, re.IGNORECASE):
            return True, ""
    return False, ""


def _strm_source(value: str) -> str:
    source = str(value or "").strip()
    if not source.casefold().endswith(".strm"):
        return source
    path = Path(source)
    try:
        if not path.is_file() or path.stat().st_size > 16 * 1024:
            return ""
        return path.read_text(encoding="utf-8-sig").strip()
    except (OSError, UnicodeError):
        return ""


def _virtual_item(value: Mapping[str, Any]) -> VirtualItem:
    return VirtualItem(
        id=_required(value.get("Id"), "item id"),
        name=str(value.get("Name") or value.get("Id") or ""),
        item_type=str(value.get("Type") or "Movie"),
        parent_id=str(value.get("ParentId") or "") or None,
        sort_name=str(value.get("SortName") or value.get("Name") or "") or None,
        production_year=_optional_int(value.get("ProductionYear")),
        provider_ids=_mapping(value.get("ProviderIds")),
        metadata=dict(value),
    )


def _item_sort_value(item: VirtualItem, sort_by: str) -> Any:
    key = str(sort_by or "").split(",", 1)[0].strip().casefold()
    if key in {"productionyear", "year"}:
        return item.production_year or 0
    if key in {"communityrating", "criticrating"}:
        try:
            return float(item.metadata.get("CommunityRating") or item.metadata.get("CriticRating") or 0)
        except (TypeError, ValueError):
            return 0.0
    if key in {"sortname", "name"}:
        return (item.sort_name or item.name).casefold()
    return str(
        item.metadata.get("DateLastContentAdded")
        or item.metadata.get("DateCreated")
        or item.metadata.get("PremiereDate")
        or item.name
    ).casefold()


def _allowed(row: Mapping[str, Any], user_id: str) -> bool:
    allowed = _strings(row.get("allowed_user_ids"))
    return not allowed or str(user_id or "").strip() in allowed


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        raw: Sequence[Any] = value.split(",")
    elif isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        raw = value
    else:
        raw = ()
    return tuple(dict.fromkeys(str(item).strip() for item in raw if str(item).strip()))


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _collection_type(item_types: Sequence[str]) -> str | None:
    normalized = {str(value).casefold() for value in item_types if str(value).strip()}
    if normalized == {"movie"}:
        return "movies"
    if normalized and normalized <= {"series", "season", "episode"}:
        return "tvshows"
    return None


def _is_official_ios(client_signature: str) -> bool:
    signature = str(client_signature or "").casefold()
    return "emby for ios" in signature or ("cfnetwork" in signature and "darwin" in signature)


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ProxyCatalogError(f"{field} is required.", status_code=400)
    return normalized


def _required_token(value: str) -> str:
    token = str(value or "").strip()
    if not token or set(token) == {"*"}:
        raise ProxyCatalogError("Emby access token is required.", status_code=401)
    return token


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


__all__ = [
    "EmbyCatalogClient",
    "HttpxEmbyProxyUpstream",
    "ProxyRuntimeSettings",
    "SqlAlchemyProxyCatalog",
    "SqlAlchemyProxyUserLimits",
    "create_production_proxy_app",
]
