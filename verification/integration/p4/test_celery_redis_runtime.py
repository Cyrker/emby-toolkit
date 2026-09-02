from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

import pytest
from p115pickcode import to_pickcode
from pydantic import SecretStr
from redis import Redis
from sqlalchemy import text

from etk.domains.collection.activation import PostgresCollectionActivationRepository
from etk.domains.collection.custom_tasks import CustomCollectionRefreshPlan
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.operations import CollectionOperationIntent, CollectionOperationKind
from etk.domains.collection.repository import SqlAlchemyCollectionRepository
from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.media.daily_theme_repository import SqlAlchemyDailyThemeRepository
from etk.domains.media.effects import PostgresMediaEffectLedger, SqlAlchemyMediaEffectRepository
from etk.domains.p115 import P115ExecutionEffectStatus, P115ExecutionStatus
from etk.domains.sharing import PostgresContributionLedger, SharingLedgerService
from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.subscription.assistant import ASSISTANT_MAINTENANCE_PHASES, _state_json
from etk.domains.subscription.cleanup_adapters import SettingsCleanupExecutionPolicy
from etk.domains.subscription.cleanup_repository import SqlAlchemyCleanupRepository
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import AttemptStatus, JobProjection, JobStatus, PostgresJobStore
from etk.platform.jobs.json_payload import canonical_json
from etk.processes.celery_app import (
    EXECUTE_COLLECTION_OPERATION_TASK,
    EXECUTE_JOB_TASK,
    EXECUTE_MEDIA_EFFECT_TASK,
    EXECUTE_SHARING_OPERATION_TASK,
    create_celery_app,
)
from etk.processes.runtime_config import TaskRuntimeConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
REDIS_IMAGE = "redis:7-alpine@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf"
OPERATION_MODULE = "verification.integration.p4.test_celery_redis_runtime"

pytestmark = pytest.mark.skipif(
    os.environ.get("ETK_RUN_CELERY_REDIS_INTEGRATION") != "1",
    reason="set ETK_RUN_CELERY_REDIS_INTEGRATION=1 to start disposable Redis and Celery processes",
)


def _safe_point_operation(processor: Any, *, marker: str, mode: str) -> dict[str, Any]:
    """Test-only, importable operation with no external service calls."""

    marker_path = Path(marker)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.with_suffix(".started").write_text(str(os.getpid()), encoding="utf-8")
    if mode == "block-once":
        first_marker = marker_path.with_suffix(".first")
        try:
            descriptor = os.open(first_marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return {"second_attempt": True}
        else:
            os.close(descriptor)
    elif mode != "safe-point":
        raise ValueError(f"unknown test operation mode: {mode}")

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if processor is not None and processor.is_stop_requested():
            marker_path.with_suffix(".stopped").write_text("observed", encoding="utf-8")
            return {"stop_observed": True}
        time.sleep(0.05)
    raise TimeoutError("test operation did not observe a cooperative stop before its safety deadline")


@dataclass(slots=True)
class CleanupEmbyStubState:
    notifications: int = 0
    item_readbacks: int = 0


@dataclass(slots=True)
class P115DirectoryStubState:
    requests: list[dict[str, Any]]


@dataclass(slots=True)
class P115FingerprintStubState:
    requests: list[dict[str, Any]]


@dataclass(slots=True)
class MediaInfoRestoreEmbyStubState:
    item_readbacks: list[dict[str, Any]]
    bridge_writes: list[dict[str, Any]]


@dataclass(slots=True)
class CleanupScanEmbyStubState:
    requests: list[dict[str, Any]]


@dataclass(slots=True)
class IdentityExpiryEmbyStubState:
    user: dict[str, Any]
    policy_writes: int = 0
    user_readbacks: int = 0


@dataclass(slots=True)
class DailyThemeTmdbStubState:
    requests: list[str]


@dataclass(slots=True)
class ManualSubscriptionStubState:
    requests: list[str]
    moviepilot_writes: list[dict[str, Any]]


@dataclass(slots=True)
class SharingOperationStubState:
    requests: list[dict[str, Any]]


@dataclass(slots=True)
class NativeP8TaskStubState:
    requests: list[str]


def _start_native_p8_task_stub() -> tuple[
    ThreadingHTTPServer,
    threading.Thread,
    NativeP8TaskStubState,
]:
    state = NativeP8TaskStubState(requests=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            with lock:
                state.requests.append(path)
            if path == "/collection/25":
                query = parse_qs(urlsplit(self.path).query)
                if query.get("api_key") != ["fixture-tmdb-key"]:
                    self.send_response(403)
                    self.end_headers()
                    return
                payload = json.dumps(
                    {
                        "id": 25,
                        "name": "Fixture Activated Collection",
                        "parts": [
                            {
                                "id": 2501,
                                "title": "Fixture Activated Movie",
                                "release_date": "2026-01-01",
                                "poster_path": "/2501.jpg",
                            }
                        ],
                    },
                    separators=(",", ":"),
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.headers.get("X-Emby-Token") != "fixture-p8-token":
                self.send_response(403)
                self.end_headers()
                return
            if path == "/Library/VirtualFolders":
                payload = b'[{"Id":"library-1","CollectionType":"movies"}]'
            elif path == "/Users/fixture-p8-user/Items":
                payload = (
                    b'{"Items":[{"Id":"emby-25","Name":"Fixture Activated Collection","ProviderIds":{"Tmdb":"25"}}]}'
                )
            elif path == "/System/Info":
                payload = b'{"Id":"fixture-server","Version":"4.8.0.0"}'
            else:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-native-p8-task-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_identity_expiry_emby_stub(
    user_id: str,
) -> tuple[ThreadingHTTPServer, threading.Thread, IdentityExpiryEmbyStubState]:
    state = IdentityExpiryEmbyStubState(
        user={
            "Id": user_id,
            "Name": "Expiry Worker Fixture",
            "Policy": {"CanDownload": False, "IsDisabled": False},
            "Configuration": {},
        }
    )
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if self.headers.get("X-Emby-Token") != "fixture-expiry-token" or path != f"/Users/{user_id}":
                self.send_response(403)
                self.end_headers()
                return
            with lock:
                state.user_readbacks += 1
                payload = json.dumps(state.user, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            if self.headers.get("X-Emby-Token") != "fixture-expiry-token" or path != f"/Users/{user_id}/Policy":
                self.send_response(403)
                self.end_headers()
                return
            policy = json.loads(raw)
            if not isinstance(policy, dict):
                self.send_response(400)
                self.end_headers()
                return
            with lock:
                state.policy_writes += 1
                state.user["Policy"] = dict(policy)
            self.send_response(204)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-identity-expiry-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_daily_theme_tmdb_stub() -> tuple[ThreadingHTTPServer, threading.Thread, DailyThemeTmdbStubState]:
    state = DailyThemeTmdbStubState(requests=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            if parse_qs(parsed.query).get("api_key") != ["fixture-tmdb-key"]:
                self.send_response(403)
                self.end_headers()
                return
            with lock:
                state.requests.append(self.path)
            if parsed.path == "/3/discover/movie":
                page = int(parse_qs(parsed.query).get("page", ["1"])[0])
                payload_value: dict[str, Any] = (
                    {
                        "results": [
                            {
                                "id": 810001,
                                "title": "每日主题测试电影",
                                "overview": "脱敏测试简介",
                                "poster_path": "/fixture.jpg",
                            }
                        ]
                    }
                    if page == 1
                    else {"results": []}
                )
            elif parsed.path == "/3/movie/810001":
                payload_value = {
                    "id": 810001,
                    "title": "每日主题测试电影",
                    "overview": "脱敏测试简介",
                    "poster_path": "/fixture.jpg",
                    "release_date": "2026-08-30",
                    "vote_average": 8.1,
                    "credits": {"cast": []},
                }
            else:
                self.send_response(404)
                self.end_headers()
                return
            payload = json.dumps(payload_value, ensure_ascii=False, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-daily-theme-tmdb-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_manual_subscription_stub() -> tuple[
    ThreadingHTTPServer,
    threading.Thread,
    ManualSubscriptionStubState,
]:
    state = ManualSubscriptionStubState(requests=[], moviepilot_writes=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            with lock:
                state.requests.append(self.path)
            movie_titles = {
                820001: "手动订阅测试电影",
                820002: "自动订阅测试电影",
            }
            if parsed.path.startswith("/3/movie/"):
                try:
                    tmdb_id = int(parsed.path.rsplit("/", 1)[-1])
                except ValueError:
                    tmdb_id = 0
                title = movie_titles.get(tmdb_id)
                if title is None:
                    self._respond(404, {})
                    return
                if parse_qs(parsed.query).get("api_key") != ["fixture-tmdb-key"]:
                    self._respond(403, {})
                    return
                self._respond(
                    200,
                    {
                        "id": tmdb_id,
                        "title": title,
                        "release_dates": {
                            "results": [
                                {
                                    "iso_3166_1": "CN",
                                    "release_dates": [{"type": 4, "release_date": "2026-08-01T00:00:00Z"}],
                                }
                            ]
                        },
                    },
                )
                return
            if parsed.path.startswith("/api/v1/subscribe/media/tmdb:"):
                try:
                    tmdb_id = int(parsed.path.rsplit(":", 1)[-1])
                except ValueError:
                    tmdb_id = 0
                title = movie_titles.get(tmdb_id)
                if title is None:
                    self._respond(404, {})
                    return
                if self.headers.get("Authorization") != "Bearer fixture-mp-token":
                    self._respond(401, {})
                    return
                self._respond(
                    200,
                    {"id": 1, "tmdbid": tmdb_id, "type": "电影", "name": title},
                )
                return
            self._respond(404, {})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            with lock:
                state.requests.append(self.path)
            if parsed.path == "/api/v1/login/access-token":
                self._respond(200, {"access_token": "fixture-mp-token"})
                return
            if parsed.path == "/api/v1/subscribe/":
                if self.headers.get("Authorization") != "Bearer fixture-mp-token":
                    self._respond(401, {})
                    return
                payload = json.loads(raw or b"{}")
                if not isinstance(payload, dict):
                    self._respond(400, {})
                    return
                with lock:
                    state.moviepilot_writes.append(dict(payload))
                self._respond(201, {"id": 1})
                return
            self._respond(404, {})

        def _respond(self, status: int, payload_value: Mapping[str, Any]) -> None:
            payload = json.dumps(payload_value, ensure_ascii=False, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-manual-subscription-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_sharing_operation_stub() -> tuple[
    ThreadingHTTPServer,
    threading.Thread,
    SharingOperationStubState,
]:
    state = SharingOperationStubState(requests=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                self.send_response(400)
                self.end_headers()
                return
            path = urlsplit(self.path).path
            with lock:
                state.requests.append(
                    {
                        "path": path,
                        "body": body,
                        "instance_id": self.headers.get("X-ETK-Instance-ID"),
                        "instance_secret": self.headers.get("X-ETK-Instance-Secret"),
                    }
                )
            if path == "/api/v1/transfers/lease":
                payload = {"ok": True, "lease_id": "fixture-lease"}
            elif path == "/api/v1/devices/register":
                payload = {"ok": True, "device_id": "fixture-device"}
            elif path == "/api/etk/register":
                payload = {
                    "success": True,
                    "data": {
                        "instance_id": "fixture-instance",
                        "instance_secret": "fixture-instance-secret",
                    },
                }
            elif path == "/api/v1/transfers/report":
                payload = {"detail": "fixture unknown write"}
                encoded = json.dumps(payload, separators=(",", ":")).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return
            elif path == "/api/hdhive/checkin":
                payload = {"success": True, "id": "fixture-checkin"}
            elif path == "/botfixture-bot-token/sendMessage":
                payload = {"ok": True, "result": {"message_id": 1}}
            else:
                self.send_response(404)
                self.end_headers()
                return
            encoded = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-sharing-operation-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_p115_directory_stub(
    *,
    directory_id: str = "dir-worker-native",
    directory_name: str = "Native",
) -> tuple[ThreadingHTTPServer, threading.Thread, P115DirectoryStubState]:
    state = P115DirectoryStubState(requests=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            query = dict(item.split("=", 1) for item in parsed.query.split("&") if "=" in item)
            authorization = self.headers.get("Authorization")
            with lock:
                state.requests.append(
                    {
                        "path": parsed.path,
                        "query": query,
                        "authorization": authorization,
                    }
                )
            if parsed.path != "/open/ufile/files" or authorization != "Bearer fixture-openapi-token":
                self.send_response(403)
                self.end_headers()
                return
            offset = int(query.get("offset") or 0)
            data = (
                [
                    {"file_id": directory_id, "file_name": directory_name, "type": 0},
                    {"file_id": "movie-worker-native", "file_name": "movie.mkv", "type": 1},
                ]
                if offset == 0
                else []
            )
            payload = json.dumps({"state": True, "count": 2, "data": data}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-p115-directory-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_p115_fingerprint_stub(
    *,
    file_id: str,
    parent_id: str,
    pick_code: str,
    sha1: str,
) -> tuple[ThreadingHTTPServer, threading.Thread, P115FingerprintStubState]:
    state = P115FingerprintStubState(requests=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            authorization = self.headers.get("Authorization")
            cookie = self.headers.get("Cookie")
            with lock:
                state.requests.append(
                    {
                        "path": parsed.path,
                        "file_id": tuple(query.get("file_id", ())),
                        "authorization": authorization,
                        "cookie": cookie,
                    }
                )
            openapi = parsed.path == "/open/folder/get_info" and authorization == "Bearer fixture-openapi-token"
            cookie_api = parsed.path == "/files/file" and str(cookie or "").startswith("UID=115001_A1_1")
            if tuple(query.get("file_id", ())) != (file_id,) or not (openapi or cookie_api):
                self.send_response(403)
                self.end_headers()
                return
            data = {
                "file_id": file_id,
                "file_name": "Fixture.mkv",
                "sha1": sha1,
                "pick_code": pick_code,
                "size": 123,
            }
            if openapi:
                data["parent_id"] = parent_id
            response: dict[str, Any] = {
                "state": True,
                "data": data,
            }
            if cookie_api:
                response["breadcrumb"] = [
                    {"file_id": "0", "file_category": "0"},
                    {"file_id": parent_id, "file_category": "0"},
                    {"file_id": file_id, "file_category": "1"},
                ]
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-p115-fingerprint-stub", daemon=True)
    thread.start()
    return server, thread, state


def _start_mediainfo_restore_emby_stub(
    *,
    item_id: str,
    api_key: str,
) -> tuple[ThreadingHTTPServer, threading.Thread, MediaInfoRestoreEmbyStubState]:
    state = MediaInfoRestoreEmbyStubState(item_readbacks=[], bridge_writes=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            if parsed.path == "/System/Info":
                payload = b'{"Id":"mediainfo-restore-stub"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            query = parse_qs(parsed.query)
            token = self.headers.get("X-Emby-Token")
            requested = tuple(value for value in query.get("Ids", [""])[0].split(",") if value)
            with lock:
                state.item_readbacks.append(
                    {
                        "path": parsed.path,
                        "ids": requested,
                        "token": token,
                    }
                )
            if parsed.path != "/Items" or token != api_key or requested != (item_id,):
                self.send_response(403)
                self.end_headers()
                return
            payload = json.dumps(
                {
                    "Items": [
                        {
                            "Id": item_id,
                            "MediaSources": [{"MediaStreams": [{"Type": "Audio", "Codec": "aac"}]}],
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            token = self.headers.get("X-Emby-Token")
            expected_path = f"/Items/{item_id}/ETKMediaInfo"
            content_length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(content_length))
            except (TypeError, ValueError):
                body = None
            with lock:
                state.bridge_writes.append(
                    {
                        "path": parsed.path,
                        "token": token,
                        "body": body,
                    }
                )
            if parsed.path != expected_path or token != api_key or not isinstance(body, Mapping):
                self.send_response(403)
                self.end_headers()
                return
            payload = b'{"Applied":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-mediainfo-restore-stub", daemon=True)
    thread.start()
    return server, thread, state


def _shutdown_http_stub(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
    assert not thread.is_alive()


def _start_cleanup_scan_emby_stub(
    existing_ids: set[str],
) -> tuple[ThreadingHTTPServer, threading.Thread, CleanupScanEmbyStubState]:
    state = CleanupScanEmbyStubState(requests=[])
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/System/Info":
                payload = b'{"Id":"cleanup-scan-stub"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            token = self.headers.get("X-Emby-Token")
            requested = tuple(value for value in query.get("Ids", [""])[0].split(",") if value)
            with lock:
                state.requests.append(
                    {
                        "path": parsed.path,
                        "ids": requested,
                        "token": token,
                    }
                )
            if parsed.path != "/Items" or token != "fixture-cleanup-token":
                self.send_response(403)
                self.end_headers()
                return
            payload = json.dumps(
                {"Items": [{"Id": item_id} for item_id in requested if item_id in existing_ids]}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-cleanup-scan-emby-stub", daemon=True)
    thread.start()
    return server, thread, state


def _stop_managed_process(
    process: ManagedProcess,
    *,
    expected_code: int,
    allow_sigkill: bool = False,
) -> None:
    code = process.process.poll()
    if code is None:
        code = process.stop()
    elif not process.stream.closed:
        process.stream.close()
    allowed_codes = {expected_code}
    if allow_sigkill:
        allowed_codes.add(-signal.SIGKILL)
    assert code in allowed_codes, process.read_log()


def _start_cleanup_emby_stub() -> tuple[ThreadingHTTPServer, threading.Thread, CleanupEmbyStubState]:
    state = CleanupEmbyStubState()
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
            if urlsplit(self.path).path == "/Library/Media/Updated":
                with lock:
                    state.notifications += 1
            self.send_response(204)
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path == "/Items/emby-old":
                with lock:
                    state.item_readbacks += 1
                    readback = state.item_readbacks
                if readback == 1:
                    payload = b'{"Id":"emby-old"}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self.send_response(404)
                self.end_headers()
                return
            payload = b'{"Items":[]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, name="etk-cleanup-emby-stub", daemon=True)
    thread.start()
    return server, thread, state


@dataclass(slots=True)
class RedisContainer:
    name: str
    url: str

    def start(self) -> None:
        state = _docker(
            "inspect",
            "--format",
            "{{.State.Running}} {{.State.Paused}}",
            self.name,
            check=False,
        )
        if state.returncode != 0:
            raise AssertionError(f"disposable Redis container disappeared: {state.stderr}")
        running, paused = state.stdout.split()
        if running != "true":
            _docker("start", self.name)
        elif paused == "true":
            _docker("unpause", self.name)
        _wait_for_redis(self.url)

    def stop(self) -> None:
        state = _docker(
            "inspect",
            "--format",
            "{{.State.Running}} {{.State.Paused}}",
            self.name,
            check=False,
        )
        if state.returncode == 0:
            running, paused = state.stdout.split()
            if running == "true" and paused != "true":
                # Pausing makes the real Redis endpoint unavailable while preserving
                # Docker Desktop's dynamically allocated loopback-only host port.
                _docker("pause", self.name)

    def flush(self) -> None:
        client = Redis.from_url(self.url, socket_connect_timeout=1, socket_timeout=1)
        try:
            client.flushdb()
        finally:
            client.close()


@dataclass(frozen=True, slots=True)
class RuntimeEnvironment:
    environment: dict[str, str]
    config: TaskRuntimeConfig
    data_dir: Path
    queue_name: str


@dataclass(slots=True)
class ManagedProcess:
    process: subprocess.Popen[str]
    stream: TextIO
    log_path: Path

    def read_log(self) -> str:
        if not self.stream.closed:
            self.stream.flush()
        return self.log_path.read_text(encoding="utf-8", errors="replace") if self.log_path.exists() else ""

    def stop(self, *, timeout: float = 35) -> int:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.kill()
            raise AssertionError(f"process ignored SIGTERM:\n{self.read_log()}") from exc
        finally:
            self.stream.close()
        return code

    def kill(self) -> None:
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.stream.close()


@pytest.fixture(scope="session")
def p4_redis_container() -> Iterator[RedisContainer]:
    info = _docker("info", "--format", "{{.ServerVersion}}", check=False)
    if info.returncode != 0:
        pytest.fail(f"Docker is required for the P4 Celery/Redis integration gate: {info.stderr}")

    name = f"etk-p4-redis-{uuid4().hex[:12]}"
    started = _docker(
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        "com.etk.verification=p4-celery-redis",
        "--publish",
        "127.0.0.1::6379",
        REDIS_IMAGE,
        "redis-server",
        "--appendonly",
        "no",
        "--save",
        "",
        check=False,
    )
    if started.returncode != 0:
        pytest.fail(f"unable to start disposable Redis: {started.stderr}")
    try:
        port = _docker(
            "inspect",
            "--format",
            '{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}',
            name,
        ).stdout.strip()
        controller = RedisContainer(name=name, url=f"redis://127.0.0.1:{port}/15")
        controller.start()
        yield controller
    finally:
        _docker("rm", "--force", "--volumes", name, check=False)


@pytest.fixture()
def p4_runtime_environment(
    p4_redis_container: RedisContainer,
    migrated_p4_postgres_url: str,
    tmp_path: Path,
) -> Iterator[RuntimeEnvironment]:
    try:
        p4_redis_container.start()
        p4_redis_container.flush()
        database = urlsplit(migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1))
        data_dir = tmp_path / "config"
        home_dir = tmp_path / "home"
        data_dir.mkdir()
        home_dir.mkdir()
        queue_name = f"p4.integration.{uuid4().hex}"
        environment = os.environ.copy()
        environment.update(
            {
                "APP_DATA_DIR": str(data_dir),
                "CONFIG_DIR": str(data_dir),
                "DB_HOST": database.hostname or "127.0.0.1",
                "DB_PORT": str(database.port or 5432),
                "DB_USER": database.username or "",
                "DB_PASSWORD": database.password or "",
                "DB_NAME": database.path.lstrip("/"),
                "ETK_DATABASE_URL": migrated_p4_postgres_url,
                "ETK_REDIS_URL": p4_redis_container.url,
                "ETK_TASK_QUEUE": queue_name,
                "ETK_JOB_HEARTBEAT_SECONDS": "1",
                "ETK_JOB_LEASE_SECONDS": "4",
                "ETK_COORDINATOR_INTERVAL_SECONDS": "0.2",
                "ETK_OUTBOX_BATCH_SIZE": "10",
                "ETK_OUTBOX_CLAIM_TIMEOUT_SECONDS": "2",
                "ETK_WORKER_CONCURRENCY": "1",
                "ETK_WORKER_SHUTDOWN_TIMEOUT_SECONDS": "5",
                "ETK_COORDINATOR_SCHEDULES_ENABLED": "false",
                "HOME": str(home_dir),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        config = TaskRuntimeConfig(
            broker_url=p4_redis_container.url,
            queue_name=queue_name,
            heartbeat_seconds=1,
            lease_seconds=4,
            coordinator_interval_seconds=0.2,
            outbox_batch_size=10,
            outbox_claim_timeout_seconds=2,
            worker_concurrency=1,
            worker_shutdown_timeout_seconds=5,
            schedules_enabled=False,
        )
        yield RuntimeEnvironment(environment, config, data_dir, queue_name)
    finally:
        active_error = sys.exception()
        cleanup_errors: list[Exception] = []
        for cleanup_action in (p4_redis_container.start, p4_redis_container.flush):
            try:
                cleanup_action()
            except Exception as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            if active_error is None:
                raise ExceptionGroup("P4 Redis fixture cleanup failed", cleanup_errors)
            active_error.add_note(
                "P4 Redis fixture cleanup also failed: "
                + "; ".join(f"{type(exc).__name__}: {exc}" for exc in cleanup_errors)
            )


def test_redis_outage_preserves_job_and_outbox_then_recovers_delivery(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_redis_container: RedisContainer,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    p4_redis_container.stop()
    created = _create_empty_chain_job(p4_store, p4_runtime_environment.queue_name)
    with p4_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM job_runs")) == 1
        assert connection.scalar(text("SELECT count(*) FROM job_attempts")) == 1
        assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 1
    coordinator = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator")
    worker: ManagedProcess | None = None
    try:

        def publish_failed() -> bool:
            with p4_runtime.engine.connect() as connection:
                row = (
                    connection.execute(text("SELECT status, publish_attempts, last_error FROM outbox_events"))
                    .mappings()
                    .one()
                )
            return row["status"] == "pending" and row["publish_attempts"] >= 1 and bool(row["last_error"])

        _wait_until(publish_failed, description="Coordinator to persist a real Redis publish failure", timeout=20)
        projection = p4_store.get_job(created.job.id)
        assert projection is not None
        assert projection.job.status is JobStatus.QUEUED

        p4_redis_container.start()
        worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
        _wait_for_worker(worker, p4_runtime_environment.config)
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        with p4_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT status FROM outbox_events")) == "processed"
    finally:
        p4_redis_container.start()
        if worker is not None:
            assert worker.stop() == 0
        assert coordinator.stop() == 128 + signal.SIGTERM


def test_duplicate_celery_delivery_for_same_attempt_executes_once(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    created = _create_empty_chain_job(p4_store, p4_runtime_environment.queue_name)
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id, count=2)
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="both duplicate Celery messages to be consumed",
            timeout=15,
        )
        with p4_runtime.engine.connect() as connection:
            attempts = connection.execute(
                text("SELECT attempt_no, status FROM job_attempts WHERE job_id = :job_id ORDER BY attempt_no"),
                {"job_id": created.job.id},
            ).all()
        assert attempts == [(1, "succeeded")]
    finally:
        assert worker.stop() == 0


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_native_p8_task_owners_execute_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    setting_keys = (
        "auto_tagging_rules",
        "studio_mapping",
        "native_collections_config",
        "dynamic_app_config",
    )
    prior_settings = _snapshot_app_settings(p4_runtime, setting_keys)
    server, thread, stub_state = _start_native_p8_task_stub()
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES
                    ('auto_tagging_rules', CAST(:auto_tagging_rules AS jsonb), NOW()),
                    ('studio_mapping', CAST(:studio_mapping AS jsonb), NOW()),
                    ('native_collections_config', CAST(:native_collections_config AS jsonb), NOW()),
                    ('dynamic_app_config', CAST(:dynamic_app_config AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {
                "auto_tagging_rules": "[]",
                "studio_mapping": '{"items":[]}',
                "native_collections_config": '{"auto_sub_enabled":false}',
                "dynamic_app_config": json.dumps(
                    {
                        "emby_server_url": f"http://127.0.0.1:{server.server_port}",
                        "emby_api_key": "fixture-p8-token",
                        "emby_user_id": "fixture-p8-user",
                        "tmdb_api_base_url": f"http://127.0.0.1:{server.server_port}",
                        "tmdb_api_key": "fixture-tmdb-key",
                        "ai_vector": False,
                    },
                    separators=(",", ":"),
                ),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO collections_info (
                    tmdb_collection_id, name, metadata_schema_version,
                    is_active, all_tmdb_ids_json
                ) VALUES ('25', 'Fixture Activated Collection', 1, FALSE, '[]'::jsonb)
                """
            )
        )
    activation = PostgresCollectionActivationRepository(p4_runtime.unit_of_work).reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Fixture Activated Collection",
    )
    assert activation is not None
    custom_definition = {"item_type": ["Movie"], "url": []}
    custom_collection = SqlAlchemyCollectionRepository(p4_runtime.unit_of_work).create_custom(
        name="Fixture Empty Custom Collection",
        collection_type="list",
        definition=custom_definition,
        allowed_user_ids=None,
    )
    custom_collection_id = int(custom_collection["id"])
    worker: ManagedProcess | None = None
    try:
        environment_values = dict(p4_runtime_environment.environment)
        environment_values.update(
            {
                "EMBY_SERVER_URL": f"http://127.0.0.1:{server.server_port}",
                "EMBY_API_KEY": "fixture-p8-token",
                "EMBY_USER_ID": "fixture-p8-user",
                "TMDB_API_BASE_URL": f"http://127.0.0.1:{server.server_port}",
                "TMDB_API_KEY": "fixture-tmdb-key",
                "AI_VECTOR": "false",
            }
        )
        if compatibility_enabled:
            environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        expected_statuses = {
            "generate_embeddings": "skipped",
            "execute-auto-tagging-rules": "skipped",
            "fill-studio-images": "skipped",
            "refresh-collections": "succeeded",
            "custom-collections": "succeeded",
            "process_all_custom_collections": "succeeded",
            "process-single-custom-collection": "succeeded",
        }
        task_parameters = {
            task_key: (
                {"custom_collection_id": custom_collection_id} if task_key == "process-single-custom-collection" else {}
            )
            for task_key in expected_statuses
        }
        created_jobs = {
            task_key: p4_store.create_job(
                task_key=task_key,
                queue_name=worker_environment.queue_name,
                max_attempts=1,
                payload={
                    "payload_version": 1,
                    "parameters": task_parameters[task_key],
                    "processor_type": "media",
                    "display_name": f"P11 {task_key} fixture",
                    "silent": True,
                },
            ).projection
            for task_key in expected_statuses
        }
        worker = _start_worker(worker_environment, tmp_path / f"native-p8-worker-{compatibility_enabled}")
        _wait_for_worker(worker, worker_environment.config)
        for task_key, created in created_jobs.items():
            _send_delivery(
                worker_environment.config,
                created.job.id,
                created.current_attempt.id,
                count=2,
            )
            completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
            assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
            assert completed.job.result is not None
            assert completed.job.result["status"] == expected_statuses[task_key]
            assert completed.job.result["details"]["task_key"] == task_key
            assert completed.job.result["details"]["compatibility_fallback"] is False
        custom_plan = CustomCollectionRefreshPlan(
            collection_id=custom_collection_id,
            collection_name="Fixture Empty Custom Collection",
            collection_type="list",
            definition_digest=hashlib.sha256(canonical_json(custom_definition).encode()).hexdigest(),
            source_snapshot={"kind": "list", "status": "empty", "items": []},
            desired_member_ids=(),
            generated_items=(),
            item_type=("Movie",),
            in_library_count=0,
            expected_emby_collection_id=None,
            allow_empty=True,
            delete_empty_source=True,
        )
        custom_operation = (
            PostgresCollectionOperationRepository(p4_runtime.unit_of_work)
            .reserve(
                CollectionOperationIntent.create(
                    kind=CollectionOperationKind.CUSTOM_REFRESH,
                    scope_key=f"custom-collection:{custom_collection_id}",
                    principal_id="fixture:p4-custom-refresh",
                    request=custom_plan.to_request(),
                    idempotency_key=f"p4-custom-refresh:{custom_collection_id}",
                )
            )
            .operation
        )
        _send_collection_operation(worker_environment.config, activation.operation.id, count=2)
        _send_collection_operation(worker_environment.config, custom_operation.id, count=2)

        def activation_confirmed() -> bool:
            with p4_runtime.engine.connect() as connection:
                return (
                    connection.scalar(
                        text("SELECT status FROM collection_operations WHERE id = :id"),
                        {"id": activation.operation.id},
                    )
                    == "confirmed"
                )

        _wait_until(
            activation_confirmed,
            description="collection activation operation to complete",
            timeout=30,
        )

        def custom_refresh_confirmed() -> bool:
            with p4_runtime.engine.connect() as connection:
                return (
                    connection.scalar(
                        text("SELECT status FROM collection_operations WHERE id = :id"),
                        {"id": custom_operation.id},
                    )
                    == "confirmed"
                )

        _wait_until(
            custom_refresh_confirmed,
            description="custom collection refresh operation to complete",
            timeout=30,
        )
        _wait_until(
            lambda: worker is not None and worker.read_log().count("Duplicate or stale delivery skipped") >= 7,
            description="duplicate P8 task deliveries to be skipped",
            timeout=20,
        )
        assert stub_state.requests.count("/Library/VirtualFolders") == 1
        assert stub_state.requests.count("/Users/fixture-p8-user/Items") == 5
        assert stub_state.requests.count("/collection/25") == 2
        assert set(stub_state.requests) <= {
            "/System/Info",
            "/Library/VirtualFolders",
            "/Users/fixture-p8-user/Items",
            "/collection/25",
        }
        with p4_runtime.engine.connect() as connection:
            assert connection.execute(
                text(
                    """
                    SELECT operation.status, outbox.status
                    FROM collection_operations AS operation
                    JOIN outbox_events AS outbox
                      ON outbox.aggregate_type = 'collection-operation'
                     AND outbox.aggregate_id = operation.id::text
                    WHERE operation.id = :operation_id
                    """
                ),
                {"operation_id": activation.operation.id},
            ).one() == ("confirmed", "processed")
            assert (
                connection.scalar(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM media_metadata
                    WHERE tmdb_id = '2501' AND item_type = 'Movie'
                    """
                    )
                )
                == 1
            )
            assert connection.execute(
                text(
                    """
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'confirmed')
                    FROM collection_operations
                    WHERE kind = 'collection.custom.refresh'
                      AND scope_key = :scope_key
                    """
                ),
                {"scope_key": f"custom-collection:{custom_collection_id}"},
            ).one() == (4, 4)
            assert connection.execute(
                text(
                    """
                    SELECT emby_collection_id, generated_media_info_json, last_synced_at IS NOT NULL
                    FROM custom_collections
                    WHERE id = :collection_id
                    """
                ),
                {"collection_id": custom_collection_id},
            ).one() == (None, [], True)
    finally:
        if worker is not None and worker.process.poll() is None:
            assert worker.stop() == 0
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM outbox_events
                    WHERE aggregate_type = 'collection-operation'
                      AND aggregate_id IN (
                          SELECT id::text
                          FROM collection_operations
                          WHERE scope_key = :scope_key
                      )
                    """
                ),
                {"scope_key": f"custom-collection:{custom_collection_id}"},
            )
            connection.execute(
                text("DELETE FROM collection_operations WHERE scope_key = :scope_key"),
                {"scope_key": f"custom-collection:{custom_collection_id}"},
            )
            connection.execute(
                text("DELETE FROM custom_collections WHERE id = :collection_id"),
                {"collection_id": custom_collection_id},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM outbox_events
                    WHERE aggregate_type = 'collection-operation'
                      AND aggregate_id = :operation_id
                    """
                ),
                {"operation_id": str(activation.operation.id)},
            )
            connection.execute(
                text("DELETE FROM collection_operations WHERE id = :operation_id"),
                {"operation_id": activation.operation.id},
            )
            connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '2501' AND item_type = 'Movie'"))
            connection.execute(text("DELETE FROM collections_info WHERE tmdb_collection_id = '25'"))
        _restore_app_settings(p4_runtime, prior_settings, setting_keys)


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_old_season_backfill_executes_once_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    series_id = f"old-season-worker-{uuid4().hex}"
    missing_id = f"{series_id}-s1"
    latest_id = f"{series_id}-s2"
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, watching_status, in_library
                ) VALUES (
                    :series_id, 'Series', 'Fixture Series', 'Watching', FALSE
                ), (
                    :missing_id, 'Season', 'Fixture S1', 'NONE', FALSE
                ), (
                    :latest_id, 'Season', 'Fixture S2', 'NONE', TRUE
                )
                """
            ),
            {
                "series_id": series_id,
                "missing_id": missing_id,
                "latest_id": latest_id,
            },
        )
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET parent_series_tmdb_id = :series_id,
                    season_number = CASE tmdb_id
                        WHEN :missing_id THEN 1
                        WHEN :latest_id THEN 2
                    END
                WHERE tmdb_id IN (:missing_id, :latest_id)
                  AND item_type = 'Season'
                """
            ),
            {
                "series_id": series_id,
                "missing_id": missing_id,
                "latest_id": latest_id,
            },
        )
    created = p4_store.create_job(
        task_key="scan_old_seasons_backfill",
        queue_name=p4_runtime_environment.queue_name,
        max_attempts=1,
        payload={
            "payload_version": 1,
            "parameters": {},
            "processor_type": "watchlist",
            "display_name": "P11 old-season backfill fixture",
            "silent": True,
        },
    ).projection
    worker_environment_values = dict(p4_runtime_environment.environment)
    if compatibility_enabled:
        worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
    else:
        worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
    worker_environment = RuntimeEnvironment(
        environment=worker_environment_values,
        config=p4_runtime_environment.config,
        data_dir=p4_runtime_environment.data_dir,
        queue_name=p4_runtime_environment.queue_name,
    )
    worker = _start_worker(worker_environment, tmp_path / "old-season-worker")
    try:
        _wait_for_worker(worker, worker_environment.config)
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        assert completed.job.result["details"] == {
            "task_key": "scan_old_seasons_backfill",
            "native_handler": "P7SubscriptionTaskRouter",
            "compatibility_fallback": False,
            "candidate_count": 1,
            "affected_count": 1,
        }
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate old-season delivery to be skipped",
            timeout=15,
        )
        with p4_runtime.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """
                        SELECT subscription_status, subscription_sources_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Season'
                        """
                    ),
                    {"tmdb_id": missing_id},
                )
                .mappings()
                .one()
            )
        assert row["subscription_status"] == "WANTED"
        assert row["subscription_sources_json"] == [
            {
                "type": "scan_old_seasons_backfill",
                "reason": "backfill_old_season",
            }
        ]
    finally:
        if worker.process.poll() is None:
            assert worker.stop() == 0
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM media_metadata
                    WHERE tmdb_id IN (:series_id, :missing_id, :latest_id)
                    """
                ),
                {
                    "series_id": series_id,
                    "missing_id": missing_id,
                    "latest_id": latest_id,
                },
            )


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
@pytest.mark.parametrize("force", [True, False], ids=["forced", "policy-bound"])
def test_intro_backfill_skips_without_legacy_or_provider_effects_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
    force: bool,
) -> None:
    prior_settings = _snapshot_app_settings(p4_runtime, ("watchlist_config",))
    with p4_runtime.engine.begin() as connection:
        operation_count = connection.scalar(text("SELECT count(*) FROM p115_intro_detection_operations"))
        intro_outbox_count = connection.scalar(
            text("SELECT count(*) FROM outbox_events WHERE topic = 'p115.intro-detection.requested'")
        )
        export_effect_count = connection.scalar(
            text("SELECT count(*) FROM media_effects WHERE effect_type = 'sharing.intro.export'")
        )
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('watchlist_config', CAST(:value AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {
                "value": json.dumps(
                    {
                        "intro_detection_trigger_mode": "off",
                        "intro_detection_credits_enabled": True,
                        "intro_detection_library_ids": ["must-not-be-read"],
                    },
                    separators=(",", ":"),
                )
            },
        )

    created = p4_store.create_job(
        task_key="intro-fingerprint-backfill",
        queue_name=p4_runtime_environment.queue_name,
        max_attempts=1,
        payload={
            "payload_version": 1,
            "parameters": {} if force else {"force": False},
            "processor_type": "watchlist",
            "display_name": "P11 native intro backfill disabled fixture",
            "silent": True,
        },
    ).projection
    site_directory = tmp_path / f"intro-backfill-site-{compatibility_enabled}"
    site_directory.mkdir()
    (site_directory / "sitecustomize.py").write_text(
        """
import builtins
import os

guard_file = os.environ.get("ETK_TEST_INTRO_BACKFILL_LEGACY_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "task_manager", "tasks", "watchlist_processor")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"intro backfill Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
""".lstrip(),
        encoding="utf-8",
    )
    import_guard = tmp_path / f"intro-backfill-legacy-import-{compatibility_enabled}.guard"
    worker_environment_values = dict(p4_runtime_environment.environment)
    existing_python_path = worker_environment_values.get("PYTHONPATH", "")
    worker_environment_values["PYTHONPATH"] = os.pathsep.join(
        filter(
            None,
            (
                str(site_directory),
                str(REPO_ROOT / "src"),
                str(REPO_ROOT),
                existing_python_path,
            ),
        )
    )
    worker_environment_values["ETK_TEST_INTRO_BACKFILL_LEGACY_IMPORT_GUARD_FILE"] = str(import_guard)
    if compatibility_enabled:
        worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
    else:
        worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
    worker_environment = RuntimeEnvironment(
        environment=worker_environment_values,
        config=p4_runtime_environment.config,
        data_dir=p4_runtime_environment.data_dir,
        queue_name=p4_runtime_environment.queue_name,
    )
    worker = _start_worker(worker_environment, tmp_path / f"intro-backfill-worker-{compatibility_enabled}")
    try:
        _wait_for_worker(worker, worker_environment.config)
        warmup = _create_empty_chain_job(p4_store, worker_environment.queue_name)
        _send_delivery(
            worker_environment.config,
            warmup.job.id,
            warmup.attempt.id,
        )
        _wait_for_job(p4_store, warmup.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        import_guard.write_text("active", encoding="utf-8")
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == 1
        assert completed.job.result is not None
        assert completed.job.result["status"] == "skipped"
        assert completed.job.result["details"] == {
            "ok": True,
            "queued": False,
            "skipped": True,
            "reason": "disabled",
            "native_intro_detection": True,
            "forced_backfill": force,
            "task_key": "intro-fingerprint-backfill",
            "native_handler": "P6P115TaskRouter",
            "compatibility_fallback": False,
        }
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate intro-backfill delivery to be skipped",
            timeout=15,
        )
        with p4_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM p115_intro_detection_operations")) == operation_count
            assert (
                connection.scalar(
                    text("SELECT count(*) FROM outbox_events WHERE topic = 'p115.intro-detection.requested'")
                )
                == intro_outbox_count
            )
            assert (
                connection.scalar(text("SELECT count(*) FROM media_effects WHERE effect_type = 'sharing.intro.export'"))
                == export_effect_count
            )
    finally:
        import_guard.unlink(missing_ok=True)
        if worker.process.poll() is None:
            assert worker.stop() == 0
        _restore_app_settings(p4_runtime, prior_settings, ("watchlist_config",))


def _snapshot_p115_directory_settings(runtime: DatabaseRuntime) -> dict[str, tuple[Any, datetime]]:
    with runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT setting_key, value_json, last_updated_at
                FROM app_settings
                WHERE setting_key IN (
                    'p115_auth_tokens',
                    'p115_sorting_rules',
                    'p115_sorting_rules_account_id'
                )
                """
            )
        ).mappings()
        return {str(row["setting_key"]): (row["value_json"], row["last_updated_at"]) for row in rows}


def _restore_p115_directory_fixture(
    runtime: DatabaseRuntime,
    *,
    prior_settings: Mapping[str, tuple[Any, datetime]],
    cache_ids: tuple[str, ...],
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM p115_filesystem_cache WHERE id = ANY(CAST(:ids AS text[]))"),
            {"ids": list(cache_ids)},
        )
        connection.execute(
            text(
                """
                DELETE FROM app_settings
                WHERE setting_key IN (
                    'p115_auth_tokens',
                    'p115_sorting_rules',
                    'p115_sorting_rules_account_id'
                )
                """
            )
        )
        for setting_key, (value_json, last_updated_at) in prior_settings.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(value_json, separators=(",", ":")),
                    "last_updated_at": last_updated_at,
                },
            )


def _restore_p115_fingerprint_fixture(
    runtime: DatabaseRuntime,
    *,
    prior_settings: Mapping[str, tuple[Any, datetime]],
    tmdb_id: str,
    file_id: str,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM p115_filesystem_cache WHERE id = :file_id"),
            {"file_id": file_id},
        )
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
            {"tmdb_id": tmdb_id},
        )
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'p115_auth_tokens'"))
        for setting_key, (value_json, last_updated_at) in prior_settings.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(value_json, separators=(",", ":")),
                    "last_updated_at": last_updated_at,
                },
            )


def _restore_mediainfo_restore_fixture(
    runtime: DatabaseRuntime,
    *,
    prior_settings: Mapping[str, tuple[Any, datetime]],
    setting_keys: tuple[str, ...],
    tmdb_id: str,
    file_id: str,
    sha1: str,
    aggregate_id: str,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM outbox_events
                WHERE aggregate_type = 'media_effect'
                  AND aggregate_id IN (
                      SELECT id::text
                      FROM media_effects
                      WHERE aggregate_id = :aggregate_id
                  )
                """
            ),
            {"aggregate_id": aggregate_id},
        )
        connection.execute(
            text("DELETE FROM media_effects WHERE aggregate_id = :aggregate_id"),
            {"aggregate_id": aggregate_id},
        )
        connection.execute(
            text("DELETE FROM p115_media_probe_items WHERE upper(sha1) = :sha1"),
            {"sha1": sha1},
        )
        connection.execute(
            text("DELETE FROM p115_mediainfo_cache WHERE upper(sha1) = :sha1"),
            {"sha1": sha1},
        )
        connection.execute(
            text("DELETE FROM p115_filesystem_cache WHERE id = :file_id"),
            {"file_id": file_id},
        )
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
            {"tmdb_id": tmdb_id},
        )
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(setting_keys)},
        )
        for setting_key, (value_json, last_updated_at) in prior_settings.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(value_json, separators=(",", ":")),
                    "last_updated_at": last_updated_at,
                },
            )


def _p115_directory_cache_rows(runtime: DatabaseRuntime, *, root_id: str) -> list[dict[str, Any]]:
    with runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id, parent_id, name, local_path, account_id, entry_kind
                FROM p115_filesystem_cache
                WHERE parent_id = :root_id
                ORDER BY id
                """
            ),
            {"root_id": root_id},
        ).mappings()
        return [dict(row) for row in rows]


def _prepare_indeterminate_organize_execution(
    runtime: DatabaseRuntime,
    *,
    plan_id: UUID,
    approved_execution_intent_id: str,
) -> UUID:
    from verification.integration.p6.test_p115_organize_execution_postgres import (
        _plan as _organize_plan,
    )
    from verification.integration.p6.test_p115_organize_execution_postgres import (
        _reserve as _reserve_organize_execution,
    )
    from verification.integration.p6.test_p115_organize_execution_postgres import (
        _start as _start_organize_execution,
    )

    plan = _organize_plan(runtime, plan_id=str(plan_id), name="Movie")
    repository, reservation, execution_job, _request_hash = _reserve_organize_execution(
        runtime,
        plan,
        approved_intent_id=approved_execution_intent_id,
    )
    store, owner = _start_organize_execution(
        runtime,
        repository,
        reservation.execution.execution_id,
        execution_job,
    )
    claimed = repository.claim_effect(
        reservation.execution.execution_id,
        plan.actions[0].action_id,
        owner,
        lease_seconds=300,
    )
    assert claimed is not None and claimed.effect.claim_token is not None
    repository.mark_requesting(
        reservation.execution.execution_id,
        plan.actions[0].action_id,
        owner,
        claimed.effect.claim_token,
        binding=claimed.binding,
    )
    store.fail_attempt(
        job_id=execution_job.job_id,
        attempt_id=execution_job.attempt_id,
        worker_id=owner.worker_id,
        error_type="fixture_requesting_crash",
        error_message="fixture Worker crashed after provider request began",
        retryable=False,
    )
    recovered = repository.recover_orphaned(reservation.execution.execution_id)
    assert recovered is not None and recovered.status is P115ExecutionStatus.INDETERMINATE
    assert recovered.actions[0].effect.status is P115ExecutionEffectStatus.INDETERMINATE
    return recovered.execution_id


def _snapshot_app_settings(
    runtime: DatabaseRuntime,
    keys: tuple[str, ...],
) -> dict[str, tuple[Any, datetime]]:
    with runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT setting_key, value_json, last_updated_at
                FROM app_settings
                WHERE setting_key = ANY(CAST(:keys AS text[]))
                """
            ),
            {"keys": list(keys)},
        ).mappings()
        return {str(row["setting_key"]): (row["value_json"], row["last_updated_at"]) for row in rows}


def _restore_app_settings(
    runtime: DatabaseRuntime,
    prior_settings: Mapping[str, tuple[Any, datetime]],
    keys: tuple[str, ...],
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(keys)},
        )
        for setting_key, (value_json, last_updated_at) in prior_settings.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(value_json, separators=(",", ":")),
                    "last_updated_at": last_updated_at,
                },
            )


def _restore_identity_expiry_fixture(
    runtime: DatabaseRuntime,
    *,
    user_id: str,
    prior_dynamic_setting: Mapping[str, tuple[Any, datetime]],
) -> None:
    with runtime.engine.begin() as connection:
        operation_ids = list(
            connection.scalars(
                text(
                    """
                    SELECT id
                    FROM identity_operations
                    WHERE kind = 'expire_user'
                      AND request_json ->> 'user_id' = :user_id
                    """
                ),
                {"user_id": user_id},
            )
        )
        if operation_ids:
            connection.execute(
                text(
                    """
                    DELETE FROM outbox_events
                    WHERE aggregate_type = 'identity-operation'
                      AND aggregate_id = ANY(CAST(:operation_ids AS text[]))
                    """
                ),
                {"operation_ids": [str(operation_id) for operation_id in operation_ids]},
            )
            connection.execute(
                text("DELETE FROM identity_operations WHERE id = ANY(CAST(:operation_ids AS uuid[]))"),
                {"operation_ids": operation_ids},
            )
        connection.execute(text("DELETE FROM emby_users WHERE id = :user_id"), {"user_id": user_id})
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))
        for setting_key, (value_json, last_updated_at) in prior_dynamic_setting.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(value_json, separators=(",", ":")),
                    "last_updated_at": last_updated_at,
                },
            )


def _restore_cleanup_scan_fixture(
    runtime: DatabaseRuntime,
    *,
    keys: tuple[str, ...],
    prior_settings: Mapping[str, tuple[Any, datetime]],
    tmdb_id: str,
    stale_tmdb_id: str,
    user_id: str,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM cleanup_index WHERE tmdb_id IN (:tmdb_id, :stale_tmdb_id)"),
            {"tmdb_id": tmdb_id, "stale_tmdb_id": stale_tmdb_id},
        )
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
            {"tmdb_id": tmdb_id},
        )
        connection.execute(text("DELETE FROM emby_users WHERE id = :user_id"), {"user_id": user_id})
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(keys)},
        )
        for setting_key, (value_json, last_updated_at) in prior_settings.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(value_json, separators=(",", ":")),
                    "last_updated_at": last_updated_at,
                },
            )


def _cleanup_scan_index_rows(runtime: DatabaseRuntime, *, tmdb_id: str) -> list[dict[str, Any]]:
    with runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT tmdb_id, item_type, status, versions_info_json, best_version_json
                FROM cleanup_index
                WHERE tmdb_id = :tmdb_id
                ORDER BY item_type
                """
            ),
            {"tmdb_id": tmdb_id},
        ).mappings()
        return [dict(row) for row in rows]


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_p115_directory_sync_uses_local_stub_through_real_worker_profiles_and_replay(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    fixture_id = uuid4().hex
    root_id = f"root-worker-{fixture_id}"
    directory_id = f"dir-worker-{fixture_id}"
    stale_id = f"stale-worker-{fixture_id}"
    replay_checkpoint = tmp_path / "p115-directory-before-job-result.checkpoint"
    replay_release = tmp_path / "p115-directory-before-job-result.release"

    with ExitStack() as cleanup:
        server, server_thread, stub = _start_p115_directory_stub(directory_id=directory_id)
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        base_url = f"http://127.0.0.1:{server.server_address[1]}"

        prior_settings = _snapshot_p115_directory_settings(p4_runtime)
        cleanup.callback(
            _restore_p115_directory_fixture,
            p4_runtime,
            prior_settings=prior_settings,
            cache_ids=(directory_id, stale_id),
        )

        site_directory = tmp_path / f"p115-directory-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os
import time

import etk.domains.p115.directory_transport as directory_transport

base_url = os.environ["ETK_TEST_P115_DIRECTORY_BASE_URL"].rstrip("/")
directory_transport._OPENAPI_FILES_URL = base_url + "/open/ufile/files"
directory_transport._COOKIE_FILES_URL = base_url + "/files"

guard_file = os.environ.get("ETK_TEST_LEGACY_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "routes", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"native P115 directory Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import

checkpoint_file = os.environ.get("ETK_TEST_P115_DIRECTORY_RESULT_CHECKPOINT")
checkpoint_job_id = os.environ.get("ETK_TEST_P115_DIRECTORY_RESULT_JOB_ID")
release_file = os.environ.get("ETK_TEST_P115_DIRECTORY_RESULT_RELEASE")
if checkpoint_file and checkpoint_job_id and release_file:
    from etk.platform.jobs import PostgresJobStore

    original_succeed_attempt = PostgresJobStore.succeed_attempt

    def succeed_attempt_after_test_checkpoint(self, **kwargs):
        if str(kwargs.get("job_id")) == checkpoint_job_id and not os.path.exists(checkpoint_file):
            try:
                descriptor = os.open(checkpoint_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.write(descriptor, str(os.getpid()).encode())
                os.close(descriptor)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and not os.path.exists(release_file):
                    time.sleep(0.05)
                if not os.path.exists(release_file):
                    raise TimeoutError("P115 directory result checkpoint was not released")
        return original_succeed_attempt(self, **kwargs)

    PostgresJobStore.succeed_attempt = succeed_attempt_after_test_checkpoint
""".lstrip(),
            encoding="utf-8",
        )

        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES
                        ('p115_auth_tokens', CAST(:credentials AS jsonb), NOW()),
                        ('p115_sorting_rules', CAST(:rules AS jsonb), NOW()),
                        ('p115_sorting_rules_account_id', '"115001"'::jsonb, NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "credentials": json.dumps(
                        {
                            "access_token": "fixture-openapi-token",
                            "refresh_token": "fixture-refresh-token",
                            "cookie": None,
                            "app_type": "web",
                            "account_id": "115001",
                            "openapi_account_id": "115001",
                            "cookie_account_id": None,
                        }
                    ),
                    "rules": json.dumps([{"cid": root_id, "category_path": "Movies"}]),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_filesystem_cache (
                        id, parent_id, name, local_path, account_id, entry_kind
                    ) VALUES (
                        :stale_id, :root_id, 'Stale', 'Movies/Stale', '115001', 'directory'
                    )
                    """
                ),
                {"stale_id": stale_id, "root_id": root_id},
            )

        created = p4_store.create_job(
            task_key="sync-115-directory-tree",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1 if compatibility_enabled else 2,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 native P115 directory fixture",
                "silent": True,
            },
        ).projection
        worker_environment_values = dict(p4_runtime_environment.environment)
        worker_environment_values["ETK_TEST_P115_DIRECTORY_BASE_URL"] = base_url
        legacy_import_guard = tmp_path / "p115-directory-legacy-import.guard"
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
            worker_environment_values.pop("ETK_TEST_LEGACY_IMPORT_GUARD_FILE", None)
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
            worker_environment_values["ETK_TEST_LEGACY_IMPORT_GUARD_FILE"] = str(legacy_import_guard)
            worker_environment_values["ETK_TEST_P115_DIRECTORY_RESULT_CHECKPOINT"] = str(replay_checkpoint)
            worker_environment_values["ETK_TEST_P115_DIRECTORY_RESULT_JOB_ID"] = str(created.job.id)
            worker_environment_values["ETK_TEST_P115_DIRECTORY_RESULT_RELEASE"] = str(replay_release)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / "p115-directory-worker")
        cleanup.callback(
            _stop_managed_process,
            worker,
            expected_code=0,
            allow_sigkill=not compatibility_enabled,
        )
        if not compatibility_enabled:
            cleanup.callback(replay_release.touch)
        _wait_for_worker(worker, worker_environment.config)
        if not compatibility_enabled:
            legacy_import_guard.write_text("active", encoding="utf-8")
            coordinator_environment = RuntimeEnvironment(
                environment={
                    **p4_runtime_environment.environment,
                    "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
                },
                config=p4_runtime_environment.config,
                data_dir=p4_runtime_environment.data_dir,
                queue_name=p4_runtime_environment.queue_name,
            )
            coordinator = _start_coordinator(
                coordinator_environment,
                tmp_path / "p115-directory-coordinator",
            )
            cleanup.callback(
                _stop_managed_process,
                coordinator,
                expected_code=128 + signal.SIGTERM,
            )
            _wait_until(
                lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
                description="compatibility Coordinator to acquire recovery leadership",
                timeout=30,
            )
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        active_worker = worker
        expected_cleaned = 1
        expected_offsets = ["0", "2"]
        if not compatibility_enabled:
            _wait_until(
                replay_checkpoint.exists,
                description="P115 directory cache commit before Job result persistence",
                timeout=30,
            )
            running = p4_store.get_job(created.job.id)
            assert running is not None
            assert running.job.status is JobStatus.RUNNING
            assert _p115_directory_cache_rows(p4_runtime, root_id=root_id) == [
                {
                    "id": directory_id,
                    "parent_id": root_id,
                    "name": "Native",
                    "local_path": "Movies/Native",
                    "account_id": "115001",
                    "entry_kind": "directory",
                }
            ]
            worker.kill()
            replacement = _start_worker(
                worker_environment,
                tmp_path / "p115-directory-replacement-worker",
            )
            cleanup.callback(_stop_managed_process, replacement, expected_code=0)
            _wait_for_worker(replacement, worker_environment.config)
            active_worker = replacement
            expected_cleaned = 0
            expected_offsets = ["0", "2", "0", "2"]

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == (1 if compatibility_enabled else 2)
        assert completed.job.result == {
            "ok": True,
            "status": "succeeded",
            "message": f"同步结束：共更新 1 个目录，清理 {expected_cleaned} 个失效缓存",
            "details": {
                "roots_total": 1,
                "roots_completed": 1,
                "roots_failed": 0,
                "cached": 1,
                "cleaned": expected_cleaned,
                "errors": [],
                "account_bound": True,
                "authoritative_cleanup_only": True,
                "task_key": "sync-115-directory-tree",
                "native_handler": "P6P115TaskRouter",
                "compatibility_fallback": False,
            },
        }
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in active_worker.read_log(),
            description="duplicate P115 directory delivery to be skipped",
            timeout=15,
        )
        assert _p115_directory_cache_rows(p4_runtime, root_id=root_id) == [
            {
                "id": directory_id,
                "parent_id": root_id,
                "name": "Native",
                "local_path": "Movies/Native",
                "account_id": "115001",
                "entry_kind": "directory",
            }
        ]
        assert [request["query"]["offset"] for request in stub.requests] == expected_offsets
        if not compatibility_enabled:
            with p4_runtime.engine.connect() as connection:
                attempts = connection.execute(
                    text(
                        """
                        SELECT attempt_no, status, error_type
                        FROM job_attempts
                        WHERE job_id = :job_id
                        ORDER BY attempt_no
                        """
                    ),
                    {"job_id": created.job.id},
                ).all()
                retry_dispatch_status = connection.scalar(
                    text(
                        """
                        SELECT status
                        FROM outbox_events
                        WHERE aggregate_type = 'job_attempt'
                          AND aggregate_id = :attempt_id
                        """
                    ),
                    {"attempt_id": str(completed.current_attempt.id)},
                )
            assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]
            assert retry_dispatch_status == "processed"


@pytest.mark.parametrize(
    ("compatibility_enabled", "provider_backend"),
    [
        (False, "openapi"),
        (True, "openapi"),
        (False, "cookie"),
    ],
    ids=["native-only-openapi", "compatibility-openapi", "native-only-cookie"],
)
def test_p115_fingerprint_repair_replays_committed_slot_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
    provider_backend: str,
) -> None:
    fixture_id = uuid4().hex
    numeric_suffix = int(fixture_id[:7], 16)
    file_id = str(9_000_000_000 + numeric_suffix)
    parent_id = str(8_000_000_000 + numeric_suffix)
    pick_code = str(to_pickcode(int(file_id)))
    sha1 = fixture_id.upper().ljust(40, "A")[:40]
    tmdb_id = f"fingerprint-{fixture_id}"
    replay_checkpoint = tmp_path / "p115-fingerprint-before-job-result.checkpoint"
    replay_release = tmp_path / "p115-fingerprint-before-job-result.release"

    with ExitStack() as cleanup:
        server, server_thread, stub = _start_p115_fingerprint_stub(
            file_id=file_id,
            parent_id=parent_id,
            pick_code=pick_code,
            sha1=sha1,
        )
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        base_url = f"http://127.0.0.1:{server.server_address[1]}"

        prior_settings = _snapshot_app_settings(p4_runtime, ("p115_auth_tokens",))
        cleanup.callback(
            _restore_p115_fingerprint_fixture,
            p4_runtime,
            prior_settings=prior_settings,
            tmdb_id=tmdb_id,
            file_id=file_id,
        )
        credentials = {
            "cookie": "UID=115001_A1_1; CID=fixture; SEID=fixture",
            "app_type": "web",
            "account_id": "115001",
            "cookie_account_id": "115001",
        }
        if provider_backend == "openapi":
            credentials.update(
                {
                    "access_token": "fixture-openapi-token",
                    "refresh_token": "fixture-refresh-token",
                    "openapi_account_id": "115001",
                }
            )
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES ('p115_auth_tokens', CAST(:credentials AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {"credentials": json.dumps(credentials)},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, title, in_library,
                        emby_item_ids_json, asset_details_json,
                        file_sha1_json, file_pickcode_json
                    ) VALUES (
                        :tmdb_id, 'Movie', 'Fingerprint Worker Fixture', TRUE,
                        '["emby-fingerprint-fixture"]'::jsonb,
                        CAST(:assets AS jsonb), '[""]'::jsonb,
                        CAST(:pick_codes AS jsonb)
                    )
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "assets": json.dumps([{"path": (f"https://fixture.invalid/p115/play/{pick_code}/Fixture.mkv")}]),
                    "pick_codes": json.dumps([pick_code]),
                },
            )

        created = p4_store.create_job(
            task_key="repair-p115-fingerprints",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1 if compatibility_enabled else 2,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 native fingerprint fixture",
                "silent": True,
            },
        ).projection

        site_directory = tmp_path / f"p115-fingerprint-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os
import time

import etk.domains.p115.fingerprint_transport as fingerprint_transport

base_url = os.environ["ETK_TEST_P115_FINGERPRINT_BASE_URL"].rstrip("/")
fingerprint_transport._OPENAPI_INFO_URL = base_url + "/open/folder/get_info"
fingerprint_transport._COOKIE_INFO_URL = base_url + "/files/file"

guard_file = os.environ.get("ETK_TEST_P115_FINGERPRINT_IMPORT_GUARD")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "routes", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"native P115 fingerprint Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import

checkpoint_file = os.environ.get("ETK_TEST_P115_FINGERPRINT_RESULT_CHECKPOINT")
checkpoint_job_id = os.environ.get("ETK_TEST_P115_FINGERPRINT_RESULT_JOB_ID")
release_file = os.environ.get("ETK_TEST_P115_FINGERPRINT_RESULT_RELEASE")
if checkpoint_file and checkpoint_job_id and release_file:
    from etk.platform.jobs import PostgresJobStore

    original_succeed_attempt = PostgresJobStore.succeed_attempt

    def succeed_attempt_after_test_checkpoint(self, **kwargs):
        if str(kwargs.get("job_id")) == checkpoint_job_id and not os.path.exists(checkpoint_file):
            try:
                descriptor = os.open(checkpoint_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.write(descriptor, str(os.getpid()).encode())
                os.close(descriptor)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and not os.path.exists(release_file):
                    time.sleep(0.05)
                if not os.path.exists(release_file):
                    raise TimeoutError("P115 fingerprint result checkpoint was not released")
        return original_succeed_attempt(self, **kwargs)

    PostgresJobStore.succeed_attempt = succeed_attempt_after_test_checkpoint
""".lstrip(),
            encoding="utf-8",
        )

        worker_environment_values = dict(p4_runtime_environment.environment)
        worker_environment_values["ETK_TEST_P115_FINGERPRINT_BASE_URL"] = base_url
        worker_environment_values["P115_API_PRIORITY"] = provider_backend
        legacy_import_guard = tmp_path / "p115-fingerprint-legacy-import.guard"
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
            worker_environment_values.pop("ETK_TEST_P115_FINGERPRINT_IMPORT_GUARD", None)
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
            worker_environment_values["ETK_TEST_P115_FINGERPRINT_IMPORT_GUARD"] = str(legacy_import_guard)
            worker_environment_values["ETK_TEST_P115_FINGERPRINT_RESULT_CHECKPOINT"] = str(replay_checkpoint)
            worker_environment_values["ETK_TEST_P115_FINGERPRINT_RESULT_JOB_ID"] = str(created.job.id)
            worker_environment_values["ETK_TEST_P115_FINGERPRINT_RESULT_RELEASE"] = str(replay_release)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / "p115-fingerprint-worker")
        cleanup.callback(
            _stop_managed_process,
            worker,
            expected_code=0,
            allow_sigkill=not compatibility_enabled,
        )
        if not compatibility_enabled:
            cleanup.callback(replay_release.touch)
        _wait_for_worker(worker, worker_environment.config)
        if not compatibility_enabled:
            legacy_import_guard.write_text("active", encoding="utf-8")
            coordinator_environment = RuntimeEnvironment(
                environment={
                    **p4_runtime_environment.environment,
                    "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
                },
                config=p4_runtime_environment.config,
                data_dir=p4_runtime_environment.data_dir,
                queue_name=p4_runtime_environment.queue_name,
            )
            coordinator = _start_coordinator(
                coordinator_environment,
                tmp_path / "p115-fingerprint-coordinator",
            )
            cleanup.callback(
                _stop_managed_process,
                coordinator,
                expected_code=128 + signal.SIGTERM,
            )
            _wait_until(
                lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
                description="compatibility Coordinator to acquire fingerprint recovery leadership",
                timeout=30,
            )

        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )
        active_worker = worker
        if not compatibility_enabled:
            _wait_until(
                replay_checkpoint.exists,
                description="fingerprint slot commit before Job result persistence",
                timeout=30,
            )
            running = p4_store.get_job(created.job.id)
            assert running is not None and running.job.status is JobStatus.RUNNING
            with p4_runtime.engine.connect() as connection:
                committed = connection.execute(
                    text(
                        """
                        SELECT file_sha1_json, file_pickcode_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                ).one()
            assert committed == ([sha1], [pick_code])
            worker.kill()
            replacement = _start_worker(
                worker_environment,
                tmp_path / "p115-fingerprint-replacement-worker",
            )
            cleanup.callback(_stop_managed_process, replacement, expected_code=0)
            _wait_for_worker(replacement, worker_environment.config)
            active_worker = replacement

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == (1 if compatibility_enabled else 2)
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        details = completed.job.result["details"]
        assert details["task_key"] == "repair-p115-fingerprints"
        assert details["native_handler"] == "P6P115TaskRouter"
        assert details["compatibility_fallback"] is False
        assert details["failed_assets"] == 0
        if compatibility_enabled:
            assert details["fixed_assets"] == 1
            assert details["remote_fixed_assets"] == 1
            assert details["verified_assets"] == 0
        else:
            assert details["fixed_assets"] == 0
            assert details["remote_fixed_assets"] == 0
            assert details["verified_assets"] == 1
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in active_worker.read_log(),
            description="duplicate fingerprint delivery to be skipped",
            timeout=15,
        )

        with p4_runtime.engine.connect() as connection:
            media = connection.execute(
                text(
                    """
                    SELECT file_sha1_json, file_pickcode_json
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": tmdb_id},
            ).one()
            cache = connection.execute(
                text(
                    """
                    SELECT id, parent_id, name, sha1, pick_code, size, account_id, entry_kind
                    FROM p115_filesystem_cache
                    WHERE id = :file_id
                    """
                ),
                {"file_id": file_id},
            ).one()
        assert media == ([sha1], [pick_code])
        assert cache == (
            file_id,
            parent_id,
            "Fixture.mkv",
            sha1,
            pick_code,
            123,
            "115001",
            "file",
        )
        if provider_backend == "cookie":
            expected_requests = [
                {
                    "path": "/files/file",
                    "file_id": (file_id,),
                    "authorization": None,
                    "cookie": "UID=115001_A1_1; CID=fixture; SEID=fixture",
                }
            ]
        else:
            expected_requests = [
                {
                    "path": "/open/folder/get_info",
                    "file_id": (file_id,),
                    "authorization": "Bearer fixture-openapi-token",
                    "cookie": None,
                }
            ]
        assert stub.requests == expected_requests

        if not compatibility_enabled:
            with p4_runtime.engine.connect() as connection:
                attempts = connection.execute(
                    text(
                        """
                        SELECT attempt_no, status, error_type
                        FROM job_attempts
                        WHERE job_id = :job_id
                        ORDER BY attempt_no
                        """
                    ),
                    {"job_id": created.job.id},
                ).all()
            assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_mediainfo_restore_replays_confirmed_bridge_effect_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    fixture_id = uuid4().hex
    numeric_suffix = int(fixture_id[:7], 16)
    file_id = str(7_000_000_000 + numeric_suffix)
    pick_code = str(to_pickcode(int(file_id)))
    sha1 = fixture_id.upper().ljust(40, "A")[:40]
    tmdb_id = f"mediainfo-restore-{fixture_id}"
    item_id = f"emby-mediainfo-{fixture_id}"
    aggregate_id = f"{item_id}:{sha1}"
    api_key = f"fixture-mediainfo-token-{fixture_id}"
    replay_checkpoint = tmp_path / "mediainfo-restore-before-job-result.checkpoint"
    replay_release = tmp_path / "mediainfo-restore-before-job-result.release"
    setting_keys = ("dynamic_app_config", "p115_auth_tokens")

    with ExitStack() as cleanup:
        server, server_thread, stub = _start_mediainfo_restore_emby_stub(
            item_id=item_id,
            api_key=api_key,
        )
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        emby_url = f"http://127.0.0.1:{server.server_address[1]}"
        prior_settings = _snapshot_app_settings(p4_runtime, setting_keys)
        cleanup.callback(
            _restore_mediainfo_restore_fixture,
            p4_runtime,
            prior_settings=prior_settings,
            setting_keys=setting_keys,
            tmdb_id=tmdb_id,
            file_id=file_id,
            sha1=sha1,
            aggregate_id=aggregate_id,
        )
        formatted = [
            {
                "MediaSourceInfo": {
                    "Name": "Fixture.mkv",
                    "MediaStreams": [{"Type": "Video", "Width": 1920, "Height": 1080}],
                },
                "Chapters": [],
            }
        ]
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES
                        ('dynamic_app_config', CAST(:dynamic AS jsonb), NOW()),
                        ('p115_auth_tokens', CAST(:credentials AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "dynamic": json.dumps(
                        {
                            "emby_server_url": emby_url,
                            "emby_api_key": api_key,
                            "emby_user_id": f"emby-user-{fixture_id}",
                            "emby_auth_mode": "user_token",
                            "emby_api_timeout": 5,
                        },
                        separators=(",", ":"),
                    ),
                    "credentials": json.dumps(
                        {
                            "access_token": "fixture-openapi-token",
                            "refresh_token": "fixture-refresh-token",
                            "account_id": "115001",
                            "openapi_account_id": "115001",
                        },
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, title, original_language, in_library,
                        emby_item_ids_json, asset_details_json,
                        file_sha1_json, file_pickcode_json
                    ) VALUES (
                        :tmdb_id, 'Movie', 'MediaInfo restore fixture', 'en', TRUE,
                        CAST(:emby_ids AS jsonb), CAST(:assets AS jsonb),
                        CAST(:sha1s AS jsonb), CAST(:pick_codes AS jsonb)
                    )
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "emby_ids": json.dumps([item_id]),
                    "assets": json.dumps([{"path": f"/fixture/{fixture_id}.mkv", "size_bytes": 4096}]),
                    "sha1s": json.dumps([sha1]),
                    "pick_codes": json.dumps([pick_code]),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_filesystem_cache (
                        id, parent_id, name, sha1, pick_code,
                        size, account_id, entry_kind
                    ) VALUES (
                        :file_id, :parent_id, 'Fixture.mkv', :sha1, :pick_code,
                        4096, '115001', 'file'
                    )
                    """
                ),
                {
                    "file_id": file_id,
                    "parent_id": str(int(file_id) - 1),
                    "sha1": sha1,
                    "pick_code": pick_code,
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                    VALUES (:sha1, CAST(:mediainfo AS jsonb))
                    """
                ),
                {
                    "sha1": sha1,
                    "mediainfo": json.dumps(formatted, separators=(",", ":")),
                },
            )

        created = p4_store.create_job(
            task_key="restore_mediainfo",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1 if compatibility_enabled else 2,
            payload={
                "payload_version": 1,
                "parameters": {"force_full_update": False},
                "processor_type": "media",
                "display_name": "P11 native MediaInfo restore fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / f"mediainfo-restore-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os
import time

guard_file = os.environ.get("ETK_TEST_MEDIAINFO_RESTORE_IMPORT_GUARD")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "routes", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"native MediaInfo restore Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import

checkpoint_file = os.environ.get("ETK_TEST_MEDIAINFO_RESTORE_RESULT_CHECKPOINT")
checkpoint_job_id = os.environ.get("ETK_TEST_MEDIAINFO_RESTORE_RESULT_JOB_ID")
release_file = os.environ.get("ETK_TEST_MEDIAINFO_RESTORE_RESULT_RELEASE")
if checkpoint_file and checkpoint_job_id and release_file:
    from etk.platform.jobs import PostgresJobStore

    original_succeed_attempt = PostgresJobStore.succeed_attempt

    def succeed_attempt_after_test_checkpoint(self, **kwargs):
        if str(kwargs.get("job_id")) == checkpoint_job_id and not os.path.exists(checkpoint_file):
            try:
                descriptor = os.open(checkpoint_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.write(descriptor, str(os.getpid()).encode())
                os.close(descriptor)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and not os.path.exists(release_file):
                    time.sleep(0.05)
                if not os.path.exists(release_file):
                    raise TimeoutError("MediaInfo restore result checkpoint was not released")
        return original_succeed_attempt(self, **kwargs)

    PostgresJobStore.succeed_attempt = succeed_attempt_after_test_checkpoint
""".lstrip(),
            encoding="utf-8",
        )

        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        import_guard = tmp_path / "mediainfo-restore-legacy-import.guard"
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
            worker_environment_values.pop("ETK_TEST_MEDIAINFO_RESTORE_IMPORT_GUARD", None)
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
            worker_environment_values["ETK_TEST_MEDIAINFO_RESTORE_IMPORT_GUARD"] = str(import_guard)
            worker_environment_values["ETK_TEST_MEDIAINFO_RESTORE_RESULT_CHECKPOINT"] = str(replay_checkpoint)
            worker_environment_values["ETK_TEST_MEDIAINFO_RESTORE_RESULT_JOB_ID"] = str(created.job.id)
            worker_environment_values["ETK_TEST_MEDIAINFO_RESTORE_RESULT_RELEASE"] = str(replay_release)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / "mediainfo-restore-worker")
        cleanup.callback(
            _stop_managed_process,
            worker,
            expected_code=0,
            allow_sigkill=not compatibility_enabled,
        )
        if not compatibility_enabled:
            cleanup.callback(replay_release.touch)
        _wait_for_worker(worker, worker_environment.config)
        if not compatibility_enabled:
            import_guard.write_text("active", encoding="utf-8")
            coordinator_environment = RuntimeEnvironment(
                environment={
                    **p4_runtime_environment.environment,
                    "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
                },
                config=p4_runtime_environment.config,
                data_dir=p4_runtime_environment.data_dir,
                queue_name=p4_runtime_environment.queue_name,
            )
            coordinator = _start_coordinator(
                coordinator_environment,
                tmp_path / "mediainfo-restore-coordinator",
            )
            cleanup.callback(
                _stop_managed_process,
                coordinator,
                expected_code=128 + signal.SIGTERM,
            )
            _wait_until(
                lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
                description="compatibility Coordinator to acquire MediaInfo recovery leadership",
                timeout=30,
            )

        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )
        active_worker = worker
        if not compatibility_enabled:
            _wait_until(
                replay_checkpoint.exists,
                description="confirmed Bridge effect before Job result persistence",
                timeout=30,
            )
            running = p4_store.get_job(created.job.id)
            assert running is not None and running.job.status is JobStatus.RUNNING
            with p4_runtime.engine.connect() as connection:
                effect_status = connection.scalar(
                    text(
                        """
                        SELECT status
                        FROM media_effects
                        WHERE aggregate_id = :aggregate_id
                        """
                    ),
                    {"aggregate_id": aggregate_id},
                )
            assert effect_status == "confirmed"
            assert len(stub.bridge_writes) == 1
            worker.kill()
            replacement = _start_worker(
                worker_environment,
                tmp_path / "mediainfo-restore-replacement-worker",
            )
            cleanup.callback(_stop_managed_process, replacement, expected_code=0)
            _wait_for_worker(replacement, worker_environment.config)
            active_worker = replacement

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == (1 if compatibility_enabled else 2)
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        details = completed.job.result["details"]
        assert details["task_key"] == "restore_mediainfo"
        assert details["native_handler"] == "P115MediaInfoRestoreService"
        assert details["compatibility_fallback"] is False
        assert details["restored"] == 1
        assert details["cache_hits"] == 1
        assert details["online_probe_attempts"] == 0
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in active_worker.read_log(),
            description="duplicate MediaInfo restore delivery to be skipped",
            timeout=15,
        )

        with p4_runtime.engine.connect() as connection:
            effect = connection.execute(
                text(
                    """
                    SELECT id, effect_type, status, attempts
                    FROM media_effects
                    WHERE aggregate_id = :aggregate_id
                    """
                ),
                {"aggregate_id": aggregate_id},
            ).one()
            outbox_status = connection.scalar(
                text(
                    """
                    SELECT status
                    FROM outbox_events
                    WHERE aggregate_type = 'media_effect'
                      AND aggregate_id = :effect_id
                    """
                ),
                {"effect_id": str(effect.id)},
            )
        assert (effect.effect_type, effect.status, effect.attempts) == (
            "emby.mediainfo.apply",
            "confirmed",
            1,
        )
        assert outbox_status == "processed"
        assert len(stub.bridge_writes) == 1
        assert stub.bridge_writes[0]["path"] == f"/Items/{item_id}/ETKMediaInfo"
        assert stub.bridge_writes[0]["token"] == api_key
        assert isinstance(stub.bridge_writes[0]["body"], Mapping)
        assert len(stub.item_readbacks) == (1 if compatibility_enabled else 2)

        if not compatibility_enabled:
            with p4_runtime.engine.connect() as connection:
                attempts = connection.execute(
                    text(
                        """
                        SELECT attempt_no, status, error_type
                        FROM job_attempts
                        WHERE job_id = :job_id
                        ORDER BY attempt_no
                        """
                    ),
                    {"job_id": created.job.id},
                ).all()
            assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_p115_organize_reconciliation_replays_committed_result_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    fixture_id = uuid4().hex
    plan_id = uuid4()
    directory_id = f"organize-directory-{fixture_id}"
    approved_execution_intent_id = f"approved-organize-execution-{fixture_id}"
    approved_reconciliation_intent_id = f"approved-organize-reconciliation-{fixture_id}"
    replay_checkpoint = tmp_path / "p115-organize-before-job-result.checkpoint"
    replay_release = tmp_path / "p115-organize-before-job-result.release"

    with ExitStack() as cleanup:
        server, server_thread, stub = _start_p115_directory_stub(
            directory_id=directory_id,
            directory_name="Movie",
        )
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        base_url = f"http://127.0.0.1:{server.server_address[1]}"

        prior_settings = _snapshot_p115_directory_settings(p4_runtime)
        cleanup.callback(
            _restore_p115_directory_fixture,
            p4_runtime,
            prior_settings=prior_settings,
            cache_ids=(),
        )
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES
                        ('p115_auth_tokens', CAST(:credentials AS jsonb), NOW()),
                        ('p115_sorting_rules', '[]'::jsonb, NOW()),
                        ('p115_sorting_rules_account_id', '""'::jsonb, NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "credentials": json.dumps(
                        {
                            "access_token": "fixture-openapi-token",
                            "refresh_token": "fixture-refresh-token",
                            "cookie": None,
                            "app_type": "web",
                            "account_id": "115001",
                            "openapi_account_id": "115001",
                            "cookie_account_id": None,
                        }
                    )
                },
            )

        execution_id = _prepare_indeterminate_organize_execution(
            p4_runtime,
            plan_id=plan_id,
            approved_execution_intent_id=approved_execution_intent_id,
        )
        created = p4_store.create_job(
            task_key="manual-correct-organize-records",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1 if compatibility_enabled else 2,
            payload={
                "payload_version": 1,
                "parameters": {
                    "execution_id": str(execution_id),
                    "approved_reconciliation_intent_id": approved_reconciliation_intent_id,
                },
                "processor_type": "media",
                "display_name": "P11 native organize reconciliation fixture",
                "silent": True,
            },
        ).projection

        site_directory = tmp_path / f"p115-organize-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os
import time

import etk.domains.p115.directory_transport as directory_transport

base_url = os.environ["ETK_TEST_P115_ORGANIZE_BASE_URL"].rstrip("/")
directory_transport._OPENAPI_FILES_URL = base_url + "/open/ufile/files"
directory_transport._COOKIE_FILES_URL = base_url + "/files"

guard_file = os.environ.get("ETK_TEST_P115_ORGANIZE_IMPORT_GUARD")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "routes", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"native P115 organize reconciliation imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import

checkpoint_file = os.environ.get("ETK_TEST_P115_ORGANIZE_RESULT_CHECKPOINT")
checkpoint_job_id = os.environ.get("ETK_TEST_P115_ORGANIZE_RESULT_JOB_ID")
release_file = os.environ.get("ETK_TEST_P115_ORGANIZE_RESULT_RELEASE")
if checkpoint_file and checkpoint_job_id and release_file:
    from etk.platform.jobs import PostgresJobStore

    original_succeed_attempt = PostgresJobStore.succeed_attempt

    def succeed_attempt_after_test_checkpoint(self, **kwargs):
        if str(kwargs.get("job_id")) == checkpoint_job_id and not os.path.exists(checkpoint_file):
            try:
                descriptor = os.open(checkpoint_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.write(descriptor, str(os.getpid()).encode())
                os.close(descriptor)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and not os.path.exists(release_file):
                    time.sleep(0.05)
                if not os.path.exists(release_file):
                    raise TimeoutError("P115 organize result checkpoint was not released")
        return original_succeed_attempt(self, **kwargs)

    PostgresJobStore.succeed_attempt = succeed_attempt_after_test_checkpoint
""".lstrip(),
            encoding="utf-8",
        )

        worker_environment_values = dict(p4_runtime_environment.environment)
        worker_environment_values["ETK_TEST_P115_ORGANIZE_BASE_URL"] = base_url
        legacy_import_guard = tmp_path / "p115-organize-legacy-import.guard"
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
            worker_environment_values.pop("ETK_TEST_P115_ORGANIZE_IMPORT_GUARD", None)
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
            worker_environment_values["ETK_TEST_P115_ORGANIZE_IMPORT_GUARD"] = str(legacy_import_guard)
            worker_environment_values["ETK_TEST_P115_ORGANIZE_RESULT_CHECKPOINT"] = str(replay_checkpoint)
            worker_environment_values["ETK_TEST_P115_ORGANIZE_RESULT_JOB_ID"] = str(created.job.id)
            worker_environment_values["ETK_TEST_P115_ORGANIZE_RESULT_RELEASE"] = str(replay_release)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / "p115-organize-worker")
        cleanup.callback(
            _stop_managed_process,
            worker,
            expected_code=0,
            allow_sigkill=not compatibility_enabled,
        )
        if not compatibility_enabled:
            cleanup.callback(replay_release.touch)
        _wait_for_worker(worker, worker_environment.config)
        if not compatibility_enabled:
            legacy_import_guard.write_text("active", encoding="utf-8")
            coordinator_environment = RuntimeEnvironment(
                environment={
                    **p4_runtime_environment.environment,
                    "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
                },
                config=p4_runtime_environment.config,
                data_dir=p4_runtime_environment.data_dir,
                queue_name=p4_runtime_environment.queue_name,
            )
            coordinator = _start_coordinator(
                coordinator_environment,
                tmp_path / "p115-organize-coordinator",
            )
            cleanup.callback(
                _stop_managed_process,
                coordinator,
                expected_code=128 + signal.SIGTERM,
            )
            _wait_until(
                lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
                description="compatibility Coordinator to acquire organize-reconciliation recovery leadership",
                timeout=30,
            )

        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )
        active_worker = worker
        if not compatibility_enabled:
            _wait_until(
                replay_checkpoint.exists,
                description="organize reconciliation commit before Job result persistence",
                timeout=30,
            )
            running = p4_store.get_job(created.job.id)
            assert running is not None and running.job.status is JobStatus.RUNNING
            with p4_runtime.engine.connect() as connection:
                committed = connection.execute(
                    text(
                        """
                        SELECT status, outcome, actor
                        FROM p115_organize_reconciliation_intents
                        WHERE execution_id = :execution_id
                          AND approved_intent_id = :approved_intent_id
                        """
                    ),
                    {
                        "execution_id": execution_id,
                        "approved_intent_id": approved_reconciliation_intent_id,
                    },
                ).one()
            assert committed == ("completed", "applied", None)
            worker.kill()
            replacement = _start_worker(
                worker_environment,
                tmp_path / "p115-organize-replacement-worker",
            )
            cleanup.callback(_stop_managed_process, replacement, expected_code=0)
            _wait_for_worker(replacement, worker_environment.config)
            active_worker = replacement

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == (1 if compatibility_enabled else 2)
        assert completed.job.result is not None
        assert completed.job.result["ok"] is True
        assert completed.job.result["status"] == "succeeded"
        assert completed.job.result["message"] == "115 整理对账完成"
        details = completed.job.result["details"]
        assert details["task_key"] == "manual-correct-organize-records"
        assert details["native_handler"] == "P6P115TaskRouter"
        assert details["compatibility_fallback"] is False
        assert details["p4_terminalized"] is False
        assert details["external_mutations_executed"] == 0
        assert details["reconciliation"]["approved_reconciliation_intent_id"] == (approved_reconciliation_intent_id)
        assert details["reconciliation"]["status"] == "completed"
        assert details["reconciliation"]["outcome"] == "applied"
        assert details["reconciliation"]["actor"] is None
        assert details["execution"]["status"] == "confirmed"
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in active_worker.read_log(),
            description="duplicate organize reconciliation delivery to be skipped",
            timeout=15,
        )
        assert [request["query"]["offset"] for request in stub.requests] == ["0", "2"]
        assert all(request["path"] == "/open/ufile/files" for request in stub.requests)
        assert all(request["query"]["cid"] == "target" for request in stub.requests)
        assert all(request["authorization"] == "Bearer fixture-openapi-token" for request in stub.requests)

        with p4_runtime.engine.connect() as connection:
            reconciliations = connection.execute(
                text(
                    """
                    SELECT approved_intent_id, status, outcome, actor
                    FROM p115_organize_reconciliation_intents
                    WHERE execution_id = :execution_id
                    ORDER BY created_at
                    """
                ),
                {"execution_id": execution_id},
            ).all()
            execution_state = connection.execute(
                text(
                    """
                    SELECT execution.status, action.status, effect.status
                    FROM p115_organize_plan_executions AS execution
                    JOIN p115_organize_execution_actions AS action
                      ON action.execution_id = execution.execution_id
                    JOIN p115_organize_effects AS effect
                      ON effect.effect_key = action.effect_key
                    WHERE execution.execution_id = :execution_id
                    """
                ),
                {"execution_id": execution_id},
            ).one()
        assert reconciliations == [(approved_reconciliation_intent_id, "completed", "applied", None)]
        assert execution_state == ("confirmed", "confirmed", "indeterminate")

        if not compatibility_enabled:
            with p4_runtime.engine.connect() as connection:
                attempts = connection.execute(
                    text(
                        """
                        SELECT attempt_no, status, error_type
                        FROM job_attempts
                        WHERE job_id = :job_id
                        ORDER BY attempt_no
                        """
                    ),
                    {"job_id": created.job.id},
                ).all()
                retry_dispatch_status = connection.scalar(
                    text(
                        """
                        SELECT status
                        FROM outbox_events
                        WHERE aggregate_type = 'job_attempt'
                          AND aggregate_id = :attempt_id
                        """
                    ),
                    {"attempt_id": str(completed.current_attempt.id)},
                )
            assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]
            assert retry_dispatch_status == "processed"


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_identity_expiry_executes_once_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    user_id = f"expiry-worker-{uuid4().hex}"
    with ExitStack() as cleanup:
        server, server_thread, stub = _start_identity_expiry_emby_stub(user_id)
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        emby_url = f"http://127.0.0.1:{server.server_address[1]}"
        prior_dynamic = _snapshot_app_settings(p4_runtime, ("dynamic_app_config",))
        cleanup.callback(
            _restore_identity_expiry_fixture,
            p4_runtime,
            user_id=user_id,
            prior_dynamic_setting=prior_dynamic,
        )
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES ('dynamic_app_config', CAST(:value AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "value": json.dumps(
                        {
                            "emby_server_url": emby_url,
                            "emby_api_key": "fixture-expiry-token",
                            "emby_api_timeout": 5,
                        },
                        separators=(",", ":"),
                    )
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO emby_users (id, name, is_administrator, policy_json)
                    VALUES (:user_id, 'Expiry Worker Fixture', FALSE, CAST(:policy AS jsonb))
                    """
                ),
                {
                    "user_id": user_id,
                    "policy": json.dumps(
                        {"CanDownload": False, "IsDisabled": False},
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (emby_user_id, status, expiration_date)
                    VALUES (:user_id, 'active', :expiration_date)
                    """
                ),
                {"user_id": user_id, "expiration_date": datetime.now(UTC) - timedelta(days=1)},
            )

        created = p4_store.create_job(
            task_key="check-expired-users",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 native identity expiry fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / f"identity-expiry-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os

guard_file = os.environ.get("ETK_TEST_IDENTITY_EXPIRY_LEGACY_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"identity expiry Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
""".lstrip(),
            encoding="utf-8",
        )
        import_guard = tmp_path / f"identity-expiry-legacy-import-{compatibility_enabled}.guard"
        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        worker_environment_values["ETK_TEST_IDENTITY_EXPIRY_LEGACY_IMPORT_GUARD_FILE"] = str(import_guard)
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / f"identity-expiry-worker-{compatibility_enabled}")
        cleanup.callback(_stop_managed_process, worker, expected_code=0)
        _wait_for_worker(worker, worker_environment.config)
        if not compatibility_enabled:
            import_guard.write_text("active", encoding="utf-8")
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == 1
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        details = completed.job.result["details"]
        assert details["candidate_count"] == 1
        assert details["successful"] == 1
        assert details["failed"] == 0
        assert details["pending"] == 0
        assert details["task_key"] == "check-expired-users"
        assert details["native_handler"] == "P8EffectTaskRouter"
        assert details["compatibility_fallback"] is False
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate identity-expiry delivery to be skipped",
            timeout=15,
        )
        with p4_runtime.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """
                    SELECT operation.status AS operation_status,
                           extended.status AS user_status,
                           outbox.status AS outbox_status
                    FROM identity_operations AS operation
                    JOIN emby_users_extended AS extended
                      ON extended.emby_user_id = operation.request_json ->> 'user_id'
                    JOIN outbox_events AS outbox
                      ON outbox.event_key = 'identity.operation.requested:' || operation.id::text
                    WHERE operation.kind = 'expire_user'
                      AND operation.request_json ->> 'user_id' = :user_id
                    """
                    ),
                    {"user_id": user_id},
                )
                .mappings()
                .one()
            )
            attempts = connection.execute(
                text("SELECT attempt_no, status FROM job_attempts WHERE job_id = :job_id ORDER BY attempt_no"),
                {"job_id": created.job.id},
            ).all()
        assert dict(row) == {
            "operation_status": "confirmed",
            "user_status": "expired",
            "outbox_status": "processed",
        }
        assert attempts == [(1, "succeeded")]
        assert stub.policy_writes == 1
        assert stub.user["Policy"] == {"CanDownload": False, "IsDisabled": True}


def test_identity_expiry_replacement_worker_replays_parent_without_repeating_provider_write(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    user_id = f"expiry-replay-{uuid4().hex}"
    checkpoint = tmp_path / "identity-expiry-before-job-result.checkpoint"
    release = tmp_path / "identity-expiry-before-job-result.release"
    import_guard = tmp_path / "identity-expiry-replay-legacy-import.guard"
    with ExitStack() as cleanup:
        server, server_thread, stub = _start_identity_expiry_emby_stub(user_id)
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        emby_url = f"http://127.0.0.1:{server.server_address[1]}"
        prior_dynamic = _snapshot_app_settings(p4_runtime, ("dynamic_app_config",))
        cleanup.callback(
            _restore_identity_expiry_fixture,
            p4_runtime,
            user_id=user_id,
            prior_dynamic_setting=prior_dynamic,
        )
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES ('dynamic_app_config', CAST(:value AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "value": json.dumps(
                        {
                            "emby_server_url": emby_url,
                            "emby_api_key": "fixture-expiry-token",
                            "emby_api_timeout": 5,
                        },
                        separators=(",", ":"),
                    )
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO emby_users (id, name, is_administrator, policy_json)
                    VALUES (:user_id, 'Expiry Replay Fixture', FALSE, CAST(:policy AS jsonb))
                    """
                ),
                {
                    "user_id": user_id,
                    "policy": json.dumps(
                        {"CanDownload": False, "IsDisabled": False},
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (emby_user_id, status, expiration_date)
                    VALUES (:user_id, 'active', :expiration_date)
                    """
                ),
                {"user_id": user_id, "expiration_date": datetime.now(UTC) - timedelta(days=1)},
            )

        created = p4_store.create_job(
            task_key="check-expired-users",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=2,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 identity expiry replay fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / "identity-expiry-replay-site"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os
import time

guard_file = os.environ.get("ETK_TEST_IDENTITY_EXPIRY_LEGACY_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"identity expiry Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import

checkpoint_file = os.environ.get("ETK_TEST_IDENTITY_EXPIRY_RESULT_CHECKPOINT")
checkpoint_job_id = os.environ.get("ETK_TEST_IDENTITY_EXPIRY_RESULT_JOB_ID")
release_file = os.environ.get("ETK_TEST_IDENTITY_EXPIRY_RESULT_RELEASE")
if checkpoint_file and checkpoint_job_id and release_file:
    from etk.domains.identity.user_expiry import IdentityUserExpiryService

    original_execute = IdentityUserExpiryService.execute

    def execute_after_test_checkpoint(self, progress, *, parent_job_id, worker_id):
        result = original_execute(
            self,
            progress,
            parent_job_id=parent_job_id,
            worker_id=worker_id,
        )
        if str(parent_job_id) == checkpoint_job_id and not os.path.exists(checkpoint_file):
            try:
                descriptor = os.open(checkpoint_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                return result
            os.write(descriptor, str(os.getpid()).encode())
            os.close(descriptor)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not os.path.exists(release_file):
                time.sleep(0.05)
            if not os.path.exists(release_file):
                raise TimeoutError("identity expiry result checkpoint was not released")
        return result

    IdentityUserExpiryService.execute = execute_after_test_checkpoint
""".lstrip(),
            encoding="utf-8",
        )
        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values.update(
            {
                "PYTHONPATH": os.pathsep.join(
                    filter(
                        None,
                        (
                            str(site_directory),
                            str(REPO_ROOT / "src"),
                            str(REPO_ROOT),
                            existing_python_path,
                        ),
                    )
                ),
                "ETK_TEST_IDENTITY_EXPIRY_LEGACY_IMPORT_GUARD_FILE": str(import_guard),
                "ETK_TEST_IDENTITY_EXPIRY_RESULT_CHECKPOINT": str(checkpoint),
                "ETK_TEST_IDENTITY_EXPIRY_RESULT_JOB_ID": str(created.job.id),
                "ETK_TEST_IDENTITY_EXPIRY_RESULT_RELEASE": str(release),
            }
        )
        worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        coordinator_environment = RuntimeEnvironment(
            environment=dict(worker_environment_values),
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / "identity-expiry-replay-worker")
        cleanup.callback(_stop_managed_process, worker, expected_code=0, allow_sigkill=True)
        _wait_for_worker(worker, worker_environment.config)
        import_guard.write_text("active", encoding="utf-8")
        coordinator = _start_coordinator(
            coordinator_environment,
            tmp_path / "identity-expiry-replay-coordinator",
        )
        cleanup.callback(
            _stop_managed_process,
            coordinator,
            expected_code=128 + signal.SIGTERM,
        )
        cleanup.callback(release.touch)
        _wait_until(
            lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
            description="Coordinator to acquire identity expiry recovery leadership",
            timeout=30,
        )
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )
        _wait_until(
            checkpoint.exists,
            description="identity expiry child commit before parent Job result persistence",
            timeout=30,
        )
        running = p4_store.get_job(created.job.id)
        assert running is not None
        assert running.job.status is JobStatus.RUNNING
        with p4_runtime.engine.connect() as connection:
            assert (
                connection.scalar(
                    text("SELECT status FROM emby_users_extended WHERE emby_user_id = :user_id"),
                    {"user_id": user_id},
                )
                == "expired"
            )
            assert (
                connection.scalar(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM identity_operations
                    WHERE kind = 'expire_user'
                      AND request_json ->> 'user_id' = :user_id
                      AND status = 'confirmed'
                    """
                    ),
                    {"user_id": user_id},
                )
                == 1
            )
        assert stub.policy_writes == 1

        worker.kill()
        replacement = _start_worker(
            worker_environment,
            tmp_path / "identity-expiry-replacement-worker",
        )
        cleanup.callback(_stop_managed_process, replacement, expected_code=0)
        _wait_for_worker(replacement, worker_environment.config)
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)

        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == 2
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        assert completed.job.result["details"]["successful"] == 1
        assert stub.policy_writes == 1
        with p4_runtime.engine.connect() as connection:
            attempts = connection.execute(
                text(
                    """
                    SELECT attempt_no, status, error_type
                    FROM job_attempts
                    WHERE job_id = :job_id
                    ORDER BY attempt_no
                    """
                ),
                {"job_id": created.job.id},
            ).all()
            operation_count = connection.scalar(
                text(
                    """
                    SELECT COUNT(*)
                    FROM identity_operations
                    WHERE kind = 'expire_user'
                      AND request_json ->> 'user_id' = :user_id
                    """
                ),
                {"user_id": user_id},
            )
        assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]
        assert operation_count == 1


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_cleanup_scan_uses_native_atomic_path_through_real_worker_profiles_and_replay(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    fixture_id = uuid4().hex
    user_id = f"cleanup-scan-user-{fixture_id}"
    tmdb_id = f"cleanup-scan-movie-{fixture_id}"
    stale_tmdb_id = f"cleanup-scan-stale-{fixture_id}"
    first_item_id = f"cleanup-scan-item-a-{fixture_id}"
    second_item_id = f"cleanup-scan-item-b-{fixture_id}"
    replay_checkpoint = tmp_path / "cleanup-scan-before-job-result.checkpoint"
    replay_release = tmp_path / "cleanup-scan-before-job-result.release"
    setting_keys = ("dynamic_app_config", "media_cleanup_config", "rating_mapping")

    with ExitStack() as cleanup:
        server, server_thread, stub = _start_cleanup_scan_emby_stub({first_item_id, second_item_id})
        cleanup.callback(_shutdown_http_stub, server, server_thread)
        emby_url = f"http://127.0.0.1:{server.server_address[1]}"
        prior_settings = _snapshot_app_settings(p4_runtime, setting_keys)
        cleanup.callback(
            _restore_cleanup_scan_fixture,
            p4_runtime,
            keys=setting_keys,
            prior_settings=prior_settings,
            tmdb_id=tmdb_id,
            stale_tmdb_id=stale_tmdb_id,
            user_id=user_id,
        )

        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES
                        ('dynamic_app_config', CAST(:dynamic AS jsonb), NOW()),
                        ('media_cleanup_config', CAST(:cleanup AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "dynamic": json.dumps(
                        {
                            "emby_server_url": emby_url,
                            "emby_api_key": "fixture-cleanup-token",
                            "emby_user_id": user_id,
                            "emby_api_timeout": 5,
                        },
                        separators=(",", ":"),
                    ),
                    "cleanup": json.dumps(
                        {
                            "rules": [
                                {
                                    "id": "resolution",
                                    "enabled": True,
                                    "priority": ["4k", "1080p"],
                                }
                            ],
                            "library_ids": ["cleanup-library"],
                            "keep_one_per_res": False,
                        },
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO emby_users (id, name, is_administrator, policy_json)
                    VALUES (:user_id, 'Cleanup scanner', TRUE, CAST(:policy AS jsonb))
                    """
                ),
                {
                    "user_id": user_id,
                    "policy": json.dumps(
                        {
                            "EnableAllFolders": True,
                            "EnabledFolders": [],
                            "ExcludedSubFolders": [],
                            "BlockedTags": [],
                            "BlockUnratedItems": [],
                            "MaxParentalRating": 10,
                        },
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, title, in_library, original_language,
                        countries_json, tags_json, official_rating_json, asset_details_json
                    ) VALUES (
                        :tmdb_id, 'Movie', 'Cleanup scanner fixture', TRUE, 'en',
                        '["US"]'::jsonb, '[]'::jsonb, '{"US":"PG-13"}'::jsonb,
                        CAST(:assets AS jsonb)
                    )
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "assets": json.dumps(
                        [
                            {
                                "emby_item_id": first_item_id,
                                "path": f"/fixture/{first_item_id}.mkv",
                                "source_library_id": "cleanup-library",
                                "ancestor_ids": ["cleanup-library"],
                                "resolution_display": "1080p",
                                "quality_display": "WEB-DL",
                                "effect_display": "SDR",
                                "codec_display": "HEVC",
                                "size_bytes": 100,
                            },
                            {
                                "emby_item_id": second_item_id,
                                "path": f"/fixture/{second_item_id}.mkv",
                                "source_library_id": "cleanup-library",
                                "ancestor_ids": ["cleanup-library"],
                                "resolution_display": "4k",
                                "quality_display": "WEB-DL",
                                "effect_display": "SDR",
                                "codec_display": "HEVC",
                                "size_bytes": 200,
                            },
                        ],
                        separators=(",", ":"),
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO cleanup_index (
                        tmdb_id, item_type, status, versions_info_json, best_version_json
                    ) VALUES (:tmdb_id, 'Movie', 'pending', '[]'::jsonb, 'null'::jsonb)
                    """
                ),
                {"tmdb_id": stale_tmdb_id},
            )

        created = p4_store.create_job(
            task_key="scan-cleanup-issues",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1 if compatibility_enabled else 2,
            requested_by_user_id=user_id,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 native cleanup scan fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / f"cleanup-scan-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os
import time

guard_file = os.environ.get("ETK_TEST_CLEANUP_SCAN_LEGACY_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    forbidden = ("database", "extensions", "handler", "routes", "task_manager", "tasks")

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and any(
            name == prefix or name.startswith(prefix + ".") for prefix in forbidden
        ):
            raise AssertionError(f"native cleanup scan Worker imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import

checkpoint_file = os.environ.get("ETK_TEST_CLEANUP_SCAN_RESULT_CHECKPOINT")
checkpoint_job_id = os.environ.get("ETK_TEST_CLEANUP_SCAN_RESULT_JOB_ID")
release_file = os.environ.get("ETK_TEST_CLEANUP_SCAN_RESULT_RELEASE")
if checkpoint_file and checkpoint_job_id and release_file:
    from etk.domains.subscription.cleanup_scan_repository import SqlAlchemyCleanupScanRepository

    original_reconcile = SqlAlchemyCleanupScanRepository.reconcile

    def reconcile_after_test_checkpoint(self, snapshot, entries, *, job_id, cancellation_check):
        result = original_reconcile(
            self,
            snapshot,
            entries,
            job_id=job_id,
            cancellation_check=cancellation_check,
        )
        if not os.path.exists(checkpoint_file):
            try:
                descriptor = os.open(checkpoint_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                return result
            os.write(descriptor, str(os.getpid()).encode())
            os.close(descriptor)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not os.path.exists(release_file):
                time.sleep(0.05)
            if not os.path.exists(release_file):
                raise TimeoutError("cleanup scan result checkpoint was not released")
        return result

    SqlAlchemyCleanupScanRepository.reconcile = reconcile_after_test_checkpoint
""".lstrip(),
            encoding="utf-8",
        )

        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        import_guard = tmp_path / "cleanup-scan-legacy-import.guard"
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
            worker_environment_values["ETK_TEST_CLEANUP_SCAN_LEGACY_IMPORT_GUARD_FILE"] = str(import_guard)
            worker_environment_values["ETK_TEST_CLEANUP_SCAN_RESULT_CHECKPOINT"] = str(replay_checkpoint)
            worker_environment_values["ETK_TEST_CLEANUP_SCAN_RESULT_JOB_ID"] = str(created.job.id)
            worker_environment_values["ETK_TEST_CLEANUP_SCAN_RESULT_RELEASE"] = str(replay_release)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / "cleanup-scan-worker")
        cleanup.callback(
            _stop_managed_process,
            worker,
            expected_code=0,
            allow_sigkill=not compatibility_enabled,
        )
        if not compatibility_enabled:
            cleanup.callback(replay_release.touch)
        _wait_for_worker(worker, worker_environment.config)
        if not compatibility_enabled:
            import_guard.write_text("active", encoding="utf-8")
            coordinator_environment = RuntimeEnvironment(
                environment={
                    **p4_runtime_environment.environment,
                    "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
                },
                config=p4_runtime_environment.config,
                data_dir=p4_runtime_environment.data_dir,
                queue_name=p4_runtime_environment.queue_name,
            )
            coordinator = _start_coordinator(
                coordinator_environment,
                tmp_path / "cleanup-scan-coordinator",
            )
            cleanup.callback(
                _stop_managed_process,
                coordinator,
                expected_code=128 + signal.SIGTERM,
            )
            _wait_until(
                lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
                description="compatibility Coordinator to acquire cleanup scan recovery leadership",
                timeout=30,
            )
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        active_worker = worker
        if not compatibility_enabled:
            _wait_until(
                replay_checkpoint.exists,
                description="cleanup index commit before Job result persistence",
                timeout=30,
            )
            running = p4_store.get_job(created.job.id)
            assert running is not None
            assert running.job.status is JobStatus.RUNNING
            assert _cleanup_scan_index_rows(p4_runtime, tmdb_id=tmdb_id)[0]["best_version_json"] == (second_item_id)
            worker.kill()
            replacement = _start_worker(
                worker_environment,
                tmp_path / "cleanup-scan-replacement-worker",
            )
            cleanup.callback(_stop_managed_process, replacement, expected_code=0)
            _wait_for_worker(replacement, worker_environment.config)
            active_worker = replacement

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == (1 if compatibility_enabled else 2)
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        assert completed.job.result["message"] == "扫描完成！共发现 1 组需要清理的多版本媒体。"
        assert completed.job.result["details"] == {
            "requested_by_user_id": user_id,
            "visible_movies": 1,
            "visible_series": 0,
            "scanned_items": 1,
            "checked_emby_ids": 2,
            "missing_emby_ids": 0,
            "indexed_items": 1,
            "deleted_pending": 1,
            "upserted_items": 1,
            "authoritative_emby_read": True,
            "atomic_reconcile": True,
            "task_key": "scan-cleanup-issues",
            "native_handler": "P8EffectTaskRouter",
            "compatibility_fallback": False,
            "fallback_retirement_stage": "not_applicable",
        }
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in active_worker.read_log(),
            description="duplicate cleanup scan delivery to be skipped",
            timeout=15,
        )
        assert _cleanup_scan_index_rows(p4_runtime, tmdb_id=tmdb_id) == [
            {
                "tmdb_id": tmdb_id,
                "item_type": "Movie",
                "status": "pending",
                "versions_info_json": [
                    {
                        "id": first_item_id,
                        "path": f"/fixture/{first_item_id}.mkv",
                        "filesize": 100,
                        "quality": "web-dl",
                        "resolution": "1080p",
                        "effect": "sdr",
                        "video_bitrate_mbps": 0,
                        "bit_depth": 8,
                        "frame_rate": 0,
                        "runtime_minutes": 0,
                        "codec": "HEVC",
                        "subtitle_count": 0,
                        "subtitle_languages": [],
                    },
                    {
                        "id": second_item_id,
                        "path": f"/fixture/{second_item_id}.mkv",
                        "filesize": 200,
                        "quality": "web-dl",
                        "resolution": "4k",
                        "effect": "sdr",
                        "video_bitrate_mbps": 0,
                        "bit_depth": 8,
                        "frame_rate": 0,
                        "runtime_minutes": 0,
                        "codec": "HEVC",
                        "subtitle_count": 0,
                        "subtitle_languages": [],
                    },
                ],
                "best_version_json": second_item_id,
            }
        ]
        assert len(stub.requests) == (1 if compatibility_enabled else 2)
        assert all(request["ids"] == (first_item_id, second_item_id) for request in stub.requests)
        if not compatibility_enabled:
            with p4_runtime.engine.connect() as connection:
                attempts = connection.execute(
                    text(
                        """
                        SELECT attempt_no, status, error_type
                        FROM job_attempts
                        WHERE job_id = :job_id
                        ORDER BY attempt_no
                        """
                    ),
                    {"job_id": created.job.id},
                ).all()
                retry_dispatch_status = connection.scalar(
                    text(
                        """
                        SELECT status
                        FROM outbox_events
                        WHERE aggregate_type = 'job_attempt'
                          AND aggregate_id = :attempt_id
                        """
                    ),
                    {"attempt_id": str(completed.current_attempt.id)},
                )
            assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]
            assert retry_dispatch_status == "processed"


def test_p7_cleanup_worker_restart_uses_readback_only_after_duplicate_delivery(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    server, server_thread, emby = _start_cleanup_emby_stub()
    emby_url = f"http://127.0.0.1:{server.server_address[1]}"
    local_root = tmp_path / "cleanup-local"
    local_root.mkdir()
    target_path = local_root / "old.mkv"
    settings = Settings(
        local_strm_root=str(local_root),
        emby_server_url=emby_url,
        emby_api_key=SecretStr("fixture-token"),
        p115_enable_sync_delete=False,
    )
    task_id = 900_000 + int(uuid4().hex[:5], 16)
    with p4_runtime.engine.begin() as connection:
        prior_dynamic = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = 'dynamic_app_config'")
        )
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('dynamic_app_config', CAST(:value AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {
                "value": json.dumps(
                    {
                        "local_strm_root": str(local_root),
                        "emby_server_url": emby_url,
                        "emby_api_key": "fixture-token",
                        "p115_enable_sync_delete": False,
                    },
                    separators=(",", ":"),
                )
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    file_sha1_json, file_pickcode_json, asset_details_json, title
                ) VALUES (
                    :tmdb_id, 'Movie', TRUE, CAST(:emby_ids AS jsonb),
                    '["",""]'::jsonb, '["",""]'::jsonb, CAST(:assets AS jsonb), 'Fixture cleanup'
                )
                """
            ),
            {
                "tmdb_id": f"cleanup-process-{task_id}",
                "emby_ids": '["emby-old","emby-best"]',
                "assets": json.dumps(
                    [
                        {"emby_item_id": "emby-old", "path": str(target_path)},
                        {"emby_item_id": "emby-best", "path": ""},
                    ],
                    separators=(",", ":"),
                ),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO cleanup_index (
                    id, tmdb_id, item_type, status, versions_info_json, best_version_json
                ) VALUES (
                    :id, :tmdb_id, 'Movie', 'pending',
                    CAST(:versions AS jsonb), '"emby-best"'::jsonb
                )
                """
            ),
            {
                "id": task_id,
                "tmdb_id": f"cleanup-process-{task_id}",
                "versions": json.dumps(
                    [
                        {"id": "emby-old", "path": str(target_path)},
                        {"id": "emby-best"},
                    ],
                    separators=(",", ":"),
                ),
            },
        )
    submission = SqlAlchemyCleanupRepository(
        p4_runtime.unit_of_work,
        SqlAlchemyCleanupMediaProjection(),
        SettingsCleanupExecutionPolicy(lambda: settings),
    ).stage_cleanup([task_id])
    initial = p4_store.get_job(submission.job_id)
    assert initial is not None

    worker = _start_worker(p4_runtime_environment, tmp_path / "cleanup-worker")
    coordinator: ManagedProcess | None = None
    replacement: ManagedProcess | None = None
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(
            p4_runtime_environment.config,
            submission.job_id,
            initial.current_attempt.id,
            count=2,
        )
        first = _wait_for_job(p4_store, submission.job_id, {JobStatus.RETRY_WAIT}, timeout=30)
        assert first.current_attempt.status is AttemptStatus.FAILED
        assert emby.notifications == 1
        assert emby.item_readbacks == 1
        assert worker.stop() == 0

        coordinator = _start_coordinator(p4_runtime_environment, tmp_path / "cleanup-coordinator")
        replacement = _start_worker(p4_runtime_environment, tmp_path / "cleanup-replacement")
        _wait_for_worker(replacement, p4_runtime_environment.config)
        completed = _wait_for_job(p4_store, submission.job_id, {JobStatus.SUCCEEDED}, timeout=45)

        assert completed.current_attempt.attempt_no == 2
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert emby.notifications == 1
        assert emby.item_readbacks == 2
        with p4_runtime.engine.connect() as connection:
            item = connection.execute(
                text(
                    """
                    SELECT status, receipt_json->'phases'->'emby'->>'status' AS emby_status
                    FROM cleanup_execution_items
                    WHERE job_id = :job_id
                    """
                ),
                {"job_id": submission.job_id},
            ).one()
        assert item == ("confirmed", "confirmed")
    finally:
        if coordinator is not None:
            assert coordinator.stop() == 128 + signal.SIGTERM
        if replacement is not None:
            assert replacement.stop() == 0
        if worker.process.poll() is None:
            worker.kill()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        with p4_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM cleanup_index WHERE id = :id"), {"id": task_id})
            connection.execute(
                text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
                {"tmdb_id": f"cleanup-process-{task_id}"},
            )
            if prior_dynamic is None:
                connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))
            else:
                connection.execute(
                    text(
                        """
                        UPDATE app_settings
                        SET value_json = CAST(:value AS jsonb), last_updated_at = NOW()
                        WHERE setting_key = 'dynamic_app_config'
                        """
                    ),
                    {"value": json.dumps(prior_dynamic, separators=(",", ":"))},
                )


def test_subscribe_assistant_restart_resumes_frozen_phases_through_real_compatibility_worker(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    phases = tuple(phase for phase in ASSISTANT_MAINTENANCE_PHASES if phase != "download_check")
    created = p4_store.create_job(
        task_key="subscribe-assistant-maintenance",
        queue_name=p4_runtime_environment.queue_name,
        max_attempts=2,
        payload={
            "payload_version": 1,
            "parameters": {},
            "processor_type": "watchlist",
            "display_name": "P7 subscribe assistant restart fixture",
            "silent": True,
        },
    ).projection
    first_attempt = created.current_attempt.id
    claim = p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=first_attempt,
        worker_id="fixture-crashed-worker",
        lease_seconds=30,
    )
    assert claim.claimed is True
    p4_store.start_attempt(
        job_id=created.job.id,
        attempt_id=first_attempt,
        worker_id="fixture-crashed-worker",
        lease_seconds=30,
    )
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (:state_key, CAST(:state_json AS jsonb), NOW())
                """
            ),
            {
                "state_key": f"maintenance:{created.job.id}",
                "state_json": _state_json(
                    created.job.id,
                    first_attempt,
                    phases,
                    (phases[0],),
                    {phases[0]: 7},
                    False,
                ),
            },
        )
    p4_store.fail_attempt(
        job_id=created.job.id,
        attempt_id=first_attempt,
        worker_id="fixture-crashed-worker",
        error_type="fixture_worker_lost",
        error_message="fixture worker lost after phase checkpoint",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        honor_cancel_request=False,
    )
    retries = p4_store.enqueue_due_retries()
    assert len(retries) == 1
    second_attempt = retries[0].id

    compatibility_environment = RuntimeEnvironment(
        environment={
            **p4_runtime_environment.environment,
            "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
        },
        config=p4_runtime_environment.config,
        data_dir=p4_runtime_environment.data_dir,
        queue_name=p4_runtime_environment.queue_name,
    )
    worker = _start_worker(compatibility_environment, tmp_path / "subscribe-assistant-worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, second_attempt, count=2)
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)

        assert completed.current_attempt.id == second_attempt
        assert completed.current_attempt.attempt_no == 2
        with p4_runtime.engine.connect() as connection:
            state = connection.scalar(
                text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = :state_key"),
                {"state_key": f"maintenance:{created.job.id}"},
            )
        assert state["completed"] is True
        assert state["completed_phases"] == list(phases)
        assert state["results"][phases[0]] == 7
        assert state["current_attempt_id"] == str(second_attempt)
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate Assistant delivery to be skipped",
            timeout=15,
        )
    finally:
        if worker.process.poll() is None:
            assert worker.stop() == 0
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM subscribe_assistant_state WHERE state_key = :state_key"),
                {"state_key": f"maintenance:{created.job.id}"},
            )


def test_sigkill_worker_child_recovers_expired_lease_and_retries(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "kill-recovery"
    created = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=marker,
        mode="block-once",
        max_attempts=2,
    )
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker", compatibility=True)
    coordinator: ManagedProcess | None = None
    replacement: ManagedProcess | None = None
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        coordinator = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator")
        running = _wait_for_job(p4_store, created.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(marker.with_suffix(".first").exists, description="first worker attempt to enter the test operation")
        worker_id = running.current_attempt.worker_id
        assert worker_id is not None
        execution_pid = int(worker_id.rsplit(":", 1)[1])
        os.kill(execution_pid, signal.SIGKILL)
        if execution_pid == worker.process.pid:
            worker.process.wait(timeout=10)
            worker.stream.close()
            replacement = _start_worker(
                p4_runtime_environment,
                tmp_path / "replacement-worker",
                compatibility=True,
            )
            _wait_for_worker(replacement, p4_runtime_environment.config)

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.attempt_no == 2
        with p4_runtime.engine.connect() as connection:
            attempts = connection.execute(
                text(
                    "SELECT attempt_no, status, error_type FROM job_attempts WHERE job_id = :job_id ORDER BY attempt_no"
                ),
                {"job_id": created.job.id},
            ).all()
        assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]
    finally:
        if coordinator is not None:
            assert coordinator.stop() == 128 + signal.SIGTERM
        if replacement is not None:
            assert replacement.stop() == 0
        elif worker.process.poll() is None:
            assert worker.stop() == 0


@pytest.mark.parametrize(
    ("control", "terminal_status", "attempt_status"),
    [
        ("cancel", JobStatus.CANCELLED, AttemptStatus.CANCELLED),
        ("timeout", JobStatus.TIMED_OUT, AttemptStatus.TIMED_OUT),
    ],
)
def test_worker_cooperative_safe_point_persists_cancel_and_timeout(
    control: str,
    terminal_status: JobStatus,
    attempt_status: AttemptStatus,
    p4_store: PostgresJobStore,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / control
    created = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=marker,
        mode="safe-point",
        timeout_seconds=1 if control == "timeout" else None,
    )
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker", compatibility=True)
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id)
        _wait_for_job(p4_store, created.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(marker.with_suffix(".started").exists, description="test operation to reach its safe point")
        if control == "cancel":
            requested = p4_store.request_cancel(created.job.id)
            assert requested.job.status is JobStatus.CANCEL_REQUESTED
        completed = _wait_for_job(p4_store, created.job.id, {terminal_status}, timeout=20)
        assert completed.current_attempt.status is attempt_status
        assert marker.with_suffix(".stopped").read_text(encoding="utf-8") == "observed"
    finally:
        assert worker.stop() == 0


def test_two_workers_keep_root_backlog_globally_serial(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    first_marker = tmp_path / "serial-first"
    second_marker = tmp_path / "serial-second"
    first = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=first_marker,
        mode="safe-point",
    )
    second = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=second_marker,
        mode="safe-point",
    )
    workers = [
        _start_worker(p4_runtime_environment, tmp_path / "worker-1", compatibility=True),
        _start_worker(p4_runtime_environment, tmp_path / "worker-2", compatibility=True),
    ]
    try:
        for worker in workers:
            _wait_for_worker(worker, p4_runtime_environment.config)

        _send_delivery(p4_runtime_environment.config, first.job.id, first.attempt.id)
        _wait_for_job(p4_store, first.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(first_marker.with_suffix(".started").exists, description="first root operation to start")

        _send_delivery(p4_runtime_environment.config, second.job.id, second.attempt.id)

        def dispatch_was_deferred() -> bool:
            with p4_runtime.engine.connect() as connection:
                row = connection.execute(
                    text(
                        """
                        SELECT status, available_at > created_at
                        FROM outbox_events
                        WHERE aggregate_id = :attempt_id
                        """
                    ),
                    {"attempt_id": str(second.attempt.id)},
                ).one()
            return row == ("pending", True)

        _wait_until(
            dispatch_was_deferred,
            description="second root delivery to be deferred while the first root is running",
        )
        assert not second_marker.with_suffix(".started").exists()
        queued = p4_store.get_job(second.job.id)
        assert queued is not None and queued.job.status is JobStatus.QUEUED

        p4_store.request_cancel(first.job.id)
        _wait_for_job(p4_store, first.job.id, {JobStatus.CANCELLED}, timeout=10)

        _send_delivery(p4_runtime_environment.config, second.job.id, second.attempt.id)
        _wait_for_job(p4_store, second.job.id, {JobStatus.RUNNING}, timeout=20)
        _wait_until(second_marker.with_suffix(".started").exists, description="second root operation to start")
        p4_store.request_cancel(second.job.id)
        _wait_for_job(p4_store, second.job.id, {JobStatus.CANCELLED}, timeout=10)
    finally:
        for worker in workers:
            if worker.process.poll() is None:
                assert worker.stop() == 0


def test_two_real_coordinators_elect_one_leader_and_fail_over_on_sigterm(
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    first = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator-1")
    second = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator-2")
    processes = [first, second]
    try:

        def elected() -> tuple[ManagedProcess, ManagedProcess] | None:
            states = [_health_state(process.log_path.with_suffix(".health.json")) for process in processes]
            if states.count("leader") == 1 and states.count("standby") == 1:
                return processes[states.index("leader")], processes[states.index("standby")]
            return None

        leader, standby = _wait_until(elected, description="exactly one Coordinator leader", timeout=30)
        assert leader.stop() == 128 + signal.SIGTERM
        assert _health_state(leader.log_path.with_suffix(".health.json")) == "stopped"
        _wait_until(
            lambda: _health_state(standby.log_path.with_suffix(".health.json")) == "leader",
            description="standby Coordinator to acquire the released PostgreSQL session lock",
            timeout=20,
        )
        assert standby.stop() == 128 + signal.SIGTERM
        assert _health_state(standby.log_path.with_suffix(".health.json")) == "stopped"
    finally:
        for process in processes:
            if process.process.poll() is None:
                process.kill()


def test_worker_executes_empty_chain_then_stops_cleanly_on_sigterm(
    p4_store: PostgresJobStore,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    created = _create_empty_chain_job(p4_store, p4_runtime_environment.queue_name)
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id)
        _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert worker.stop() == 0
        log = worker.read_log()
        assert "Warm shutdown" in log
        assert "Worker runtime shutdown failed" not in log
        assert "Traceback (most recent call last)" not in log
    finally:
        if worker.process.poll() is None:
            worker.kill()


def test_active_worker_sigterm_reaches_cooperative_safe_point_before_shutdown_timeout(
    p4_store: PostgresJobStore,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "active-sigterm"
    created = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=marker,
        mode="safe-point",
    )
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker", compatibility=True)
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id)
        _wait_for_job(p4_store, created.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(marker.with_suffix(".started").exists, description="active operation to reach its safe point")

        assert worker.stop(timeout=p4_runtime_environment.config.worker_shutdown_timeout_seconds + 3) == 0
        assert marker.with_suffix(".stopped").read_text(encoding="utf-8") == "observed"
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.CANCELLED}, timeout=5)
        assert completed.current_attempt.status is AttemptStatus.CANCELLED
    finally:
        if worker.process.poll() is None:
            worker.kill()


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_daily_theme_executes_once_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    server, server_thread, stub = _start_daily_theme_tmdb_stub()
    worker: ManagedProcess | None = None
    try:
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM app_settings
                    WHERE setting_key = 'recommendation_publication_state'
                    """
                )
            )
            for key, value in (
                ("keyword_mapping", [{"label": "测试主题", "ids": [701]}]),
                ("recommendation_pool", [{"id": 700001}]),
                ("recommendation_theme_index", -1),
                ("recommendation_pool_page", 4),
                (
                    "dynamic_app_config",
                    {
                        "tmdb_api_base_url": f"http://127.0.0.1:{server.server_port}/3",
                        "tmdb_api_key": "fixture-tmdb-key",
                    },
                ),
            ):
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """
                    ),
                    {"key": key, "value": json.dumps(value, ensure_ascii=False)},
                )
        created = p4_store.create_job(
            task_key="update-daily-theme",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 native daily-theme fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / f"daily-theme-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os

guard_file = os.environ.get("ETK_TEST_DAILY_THEME_LEGACY_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and os.path.exists(guard_file) and (
            name == "tasks.discover" or name.startswith("tasks.discover.")
        ):
            raise AssertionError(f"daily-theme Worker imported legacy execution owner {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
""".lstrip(),
            encoding="utf-8",
        )
        import_guard = tmp_path / f"daily-theme-legacy-import-{compatibility_enabled}.guard"
        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        worker_environment_values["ETK_TEST_DAILY_THEME_LEGACY_IMPORT_GUARD_FILE"] = str(import_guard)
        worker_environment_values["TMDB_API_BASE_URL"] = f"http://127.0.0.1:{server.server_port}/3"
        worker_environment_values["TMDB_API_KEY"] = "fixture-tmdb-key"
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / f"daily-theme-worker-{compatibility_enabled}")
        _wait_for_worker(worker, worker_environment.config)
        import_guard.write_text("active", encoding="utf-8")
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.current_attempt.attempt_no == 1
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        details = completed.job.result["details"]
        assert details["task_key"] == "update-daily-theme"
        assert details["native_handler"] == "P8EffectTaskRouter"
        assert details["compatibility_fallback"] is False
        assert details["publication"] == "committed"
        assert details["publication_revision"] == 1
        assert details["theme_label"] == "测试主题"
        assert details["pool_size"] == 1
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate daily-theme delivery to be skipped",
            timeout=15,
        )
        with p4_runtime.engine.connect() as connection:
            values = {
                str(row["setting_key"]): row["value_json"]
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json
                        FROM app_settings
                        WHERE setting_key IN (
                            'recommendation_pool',
                            'recommendation_theme_index',
                            'recommendation_pool_page',
                            'recommendation_publication_state'
                        )
                        """
                    )
                ).mappings()
            }
        assert values["recommendation_pool"][0]["id"] == 810001
        assert values["recommendation_theme_index"] == 0
        assert values["recommendation_pool_page"] == 1
        assert values["recommendation_publication_state"]["revision"] == 1
        assert len(stub.requests) == 3
        assert [urlsplit(value).path for value in stub.requests] == [
            "/3/discover/movie",
            "/3/movie/810001",
            "/3/discover/movie",
        ]

        repository = SqlAlchemyDailyThemeRepository(p4_runtime.unit_of_work)
        later_snapshot = repository.load_snapshot()
        later = repository.publish(
            expected_fingerprint=later_snapshot.fingerprint,
            pool=[{"id": 810002}],
            theme_index=0,
            pool_page=2,
            metadata={
                "publication_id": (
                    "00000000-0000-0000-0000-000000000302"
                    if compatibility_enabled
                    else "00000000-0000-0000-0000-000000000301"
                ),
                "mode": "daily",
                "source_revision": later_snapshot.publication_revision,
                "source_fingerprint": later_snapshot.fingerprint,
                "theme_index": 0,
                "theme_label": "后续主题",
                "pool_page": 2,
                "pool_size": 1,
                "pages_scanned": 2,
                "added_count": 1,
                "published_at": "2026-08-30T00:10:00+00:00",
            },
        )
        assert later is not None and later.revision == 2
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE job_attempts
                    SET status = 'lost', retryable = TRUE,
                        error_type = 'worker_lost', error_message = 'fixture crash window',
                        result_json = NULL, updated_at = NOW()
                    WHERE id = :attempt_id
                    """
                ),
                {"attempt_id": created.current_attempt.id},
            )
            connection.execute(
                text(
                    """
                    UPDATE job_runs
                    SET status = 'failed', result_json = NULL,
                        error_summary_json = '{"type":"worker_lost"}'::jsonb,
                        finished_at = NOW(), updated_at = NOW()
                    WHERE id = :job_id
                    """
                ),
                {"job_id": created.job.id},
            )
        resumed = p4_store.resume_terminal_job(created.job.id)
        assert resumed.resumed is True
        _send_delivery(
            worker_environment.config,
            resumed.projection.job.id,
            resumed.projection.current_attempt.id,
        )
        replayed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert replayed.current_attempt.attempt_no == 2
        assert replayed.job.result is not None
        replay_details = replayed.job.result["details"]
        assert replay_details["publication"] == "already_committed"
        assert replay_details["publication_revision"] == 1
        assert replay_details["replayed"] is True
        assert len(stub.requests) == 3
        current = repository.load_snapshot()
        assert current.publication_revision == 2
        assert current.values["recommendation_pool"] == [{"id": 810002}]

        with p4_runtime.unit_of_work() as uow:
            reservation = SqlAlchemyMediaEffectRepository(uow.session).reserve(
                effect_key=f"fixture-daily-theme-replenishment:{compatibility_enabled}",
                effect_type="daily-theme.replenishment",
                aggregate_type="daily_theme_publication",
                aggregate_id="2",
                payload={"publication_revision": 2},
            )
            uow.commit()
        ledger = PostgresMediaEffectLedger(p4_runtime.unit_of_work)
        assert (
            ledger.claim_by_id(
                reservation.effect.id,
                worker_id="fixture-crashed-before-publication",
                lease_seconds=60,
            )
            is not None
        )
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE media_effects
                    SET claimed_at = NOW() - INTERVAL '2 minutes',
                        lease_expires_at = NOW() - INTERVAL '1 minute'
                    WHERE id = :effect_id
                    """
                ),
                {"effect_id": reservation.effect.id},
            )
        assert (
            ledger.requeue_replay_safe_stale_claims(
                effect_types=("daily-theme.replenishment",),
                limit=10,
            )
            == 1
        )
        _send_media_effect(worker_environment.config, reservation.effect.id)

        def replenishment_confirmed() -> bool:
            with p4_runtime.engine.connect() as connection:
                status = connection.scalar(
                    text("SELECT status FROM media_effects WHERE id = :effect_id"),
                    {"effect_id": reservation.effect.id},
                )
            return status == "confirmed"

        _wait_until(
            replenishment_confirmed,
            description="daily-theme replenishment effect confirmation",
            timeout=45,
        )
        replenished = repository.load_snapshot()
        assert replenished.publication_revision == 3
        assert replenished.values["recommendation_pool"] == [{"id": 810002}]
        assert replenished.values["recommendation_theme_index"] == 0
        assert replenished.values["recommendation_pool_page"] == 0
        assert len(stub.requests) == 4
        assert urlsplit(stub.requests[-1]).path == "/3/discover/movie"
        assert parse_qs(urlsplit(stub.requests[-1]).query)["page"] == ["3"]

        with p4_runtime.unit_of_work() as uow:
            committed_reservation = SqlAlchemyMediaEffectRepository(uow.session).reserve(
                effect_key=f"fixture-daily-theme-committed-replenishment:{compatibility_enabled}",
                effect_type="daily-theme.replenishment",
                aggregate_type="daily_theme_publication",
                aggregate_id="3",
                payload={"publication_revision": 3},
            )
            uow.commit()
        assert (
            ledger.claim_by_id(
                committed_reservation.effect.id,
                worker_id="fixture-crashed-after-publication",
                lease_seconds=60,
            )
            is not None
        )
        committed_snapshot = repository.load_snapshot()
        committed_publication = repository.publish(
            expected_fingerprint=committed_snapshot.fingerprint,
            pool=[{"id": 810002}],
            theme_index=0,
            pool_page=0,
            metadata={
                "publication_id": str(committed_reservation.effect.id),
                "mode": "replenish",
                "source_revision": committed_snapshot.publication_revision,
                "source_fingerprint": committed_snapshot.fingerprint,
                "theme_index": 0,
                "theme_label": "测试主题",
                "pool_page": 0,
                "pool_size": 1,
                "pages_scanned": 1,
                "added_count": 0,
                "published_at": "2026-08-30T00:20:00+00:00",
            },
        )
        assert committed_publication is not None and committed_publication.revision == 4
        with p4_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE media_effects
                    SET claimed_at = NOW() - INTERVAL '2 minutes',
                        lease_expires_at = NOW() - INTERVAL '1 minute'
                    WHERE id = :effect_id
                    """
                ),
                {"effect_id": committed_reservation.effect.id},
            )
        assert (
            ledger.requeue_replay_safe_stale_claims(
                effect_types=("daily-theme.replenishment",),
                limit=10,
            )
            == 1
        )
        requests_before_reconciliation = len(stub.requests)
        _send_media_effect(worker_environment.config, committed_reservation.effect.id)

        def committed_replenishment_confirmed() -> bool:
            with p4_runtime.engine.connect() as connection:
                status = connection.scalar(
                    text("SELECT status FROM media_effects WHERE id = :effect_id"),
                    {"effect_id": committed_reservation.effect.id},
                )
            return status == "confirmed"

        _wait_until(
            committed_replenishment_confirmed,
            description="committed daily-theme replenishment reconciliation",
            timeout=45,
        )
        assert len(stub.requests) == requests_before_reconciliation
        assert repository.load_snapshot().publication_revision == 4
    finally:
        if worker is not None:
            assert worker.stop() == 0
        _shutdown_http_stub(server, server_thread)


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_manual_subscription_executes_once_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    server, server_thread, stub = _start_manual_subscription_stub()
    worker: ManagedProcess | None = None
    replacement: ManagedProcess | None = None
    try:
        with p4_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM outbox_events WHERE topic = 'subscription.operation.requested'"))
            connection.execute(text("DELETE FROM subscription_quota_reservations"))
            connection.execute(text("DELETE FROM subscription_operations"))
            connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '820001' AND item_type = 'Movie'"))
            for key, value in (
                (
                    "mp_config",
                    {
                        "moviepilot_url": f"http://127.0.0.1:{server.server_port}",
                        "moviepilot_username": "fixture-user",
                        "moviepilot_password": "fixture-password",
                        "resubscribe_daily_cap": 2,
                    },
                ),
                ("watchlist_config", {}),
                (
                    "dynamic_app_config",
                    {
                        "tmdb_api_base_url": f"http://127.0.0.1:{server.server_port}/3",
                        "tmdb_api_key": "fixture-tmdb-key",
                    },
                ),
            ):
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """
                    ),
                    {"key": key, "value": json.dumps(value, ensure_ascii=False)},
                )
        created = p4_store.create_job(
            task_key="manual_subscribe_batch",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1,
            payload={
                "payload_version": 1,
                "parameters": {
                    "subscribe_requests": [
                        {
                            "tmdb_id": "820001",
                            "item_type": "Movie",
                            "title": "手动订阅测试电影",
                            "user_id": "fixture-user-id",
                        }
                    ]
                },
                "processor_type": "media",
                "display_name": "P11 native manual subscription fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / f"manual-subscription-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os

guard_file = os.environ.get("ETK_TEST_MANUAL_SUBSCRIPTION_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    compatibility_enabled = os.environ.get("ETK_ENABLE_LEGACY_COMPATIBILITY", "").lower() in {
        "1", "true", "yes", "on"
    }

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and not compatibility_enabled and os.path.exists(guard_file) and (
            name == "tasks.subscriptions"
            or name.startswith("tasks.subscriptions.")
            or name == "handler.moviepilot"
            or name.startswith("handler.moviepilot.")
        ):
            raise AssertionError(f"manual subscription Worker imported legacy execution owner {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
""".lstrip(),
            encoding="utf-8",
        )
        import_guard = tmp_path / f"manual-subscription-import-{compatibility_enabled}.guard"
        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        worker_environment_values["ETK_TEST_MANUAL_SUBSCRIPTION_IMPORT_GUARD_FILE"] = str(import_guard)
        worker_environment_values["TMDB_API_BASE_URL"] = f"http://127.0.0.1:{server.server_port}/3"
        worker_environment_values["TMDB_API_KEY"] = "fixture-tmdb-key"
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / f"manual-worker-{compatibility_enabled}")
        _wait_for_worker(worker, worker_environment.config)
        import_guard.write_text("active", encoding="utf-8")
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        details = completed.job.result["details"]
        assert details["task_key"] == "manual_subscribe_batch"
        assert details["native_handler"] == "P7SubscriptionTaskRouter"
        assert details["compatibility_fallback"] is False
        assert details["processed"] == 1
        assert details["provider_failures"] == []
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate manual subscription delivery to be skipped",
            timeout=15,
        )
        assert worker.stop() == 0
        replacement = _start_worker(
            worker_environment,
            tmp_path / f"manual-replacement-worker-{compatibility_enabled}",
        )
        _wait_for_worker(replacement, worker_environment.config)
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
        )
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in replacement.read_log(),
            description="replacement Worker to reject completed manual subscription delivery",
            timeout=15,
        )
        assert stub.moviepilot_writes == [{"name": "手动订阅测试电影", "tmdbid": 820001, "type": "电影"}]
        with p4_runtime.engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        """
                        SELECT
                            (SELECT subscription_status FROM media_metadata
                             WHERE tmdb_id = '820001' AND item_type = 'Movie') AS media_status,
                            (SELECT COUNT(*) FROM subscription_operations) AS operations,
                            (SELECT COUNT(*) FROM subscription_quota_reservations) AS quota_rows,
                            (SELECT status FROM subscription_quota_reservations LIMIT 1) AS quota_status,
                            (SELECT COUNT(*) FROM outbox_events
                             WHERE topic = 'subscription.operation.requested') AS outbox_rows
                        """
                    )
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "media_status": "SUBSCRIBED",
            "operations": 1,
            "quota_rows": 1,
            "quota_status": "committed",
            "outbox_rows": 1,
        }
    finally:
        if replacement is not None:
            assert replacement.stop() == 0
        if worker is not None and worker.process.poll() is None:
            assert worker.stop() == 0
        _shutdown_http_stub(server, server_thread)


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_auto_subscription_executes_once_through_real_worker_profiles(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    server, server_thread, stub = _start_manual_subscription_stub()
    worker: ManagedProcess | None = None
    replacement: ManagedProcess | None = None
    previous_statuses: list[dict[str, Any]] = []
    previous_auto_state_keys: set[str] = set()
    try:
        with p4_runtime.engine.begin() as connection:
            previous_statuses = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, subscription_status
                        FROM media_metadata
                        WHERE subscription_status IN ('WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED')
                          AND NOT (tmdb_id = '820002' AND item_type = 'Movie')
                        """
                    )
                ).mappings()
            ]
            previous_auto_state_keys = set(
                connection.scalars(
                    text(
                        """
                        SELECT state_key
                        FROM subscription_runtime_states
                        WHERE state_json ->> 'type' = 'auto-subscribe-item'
                        """
                    )
                )
            )
            connection.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = 'NONE'
                    WHERE subscription_status IN ('WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED')
                      AND NOT (tmdb_id = '820002' AND item_type = 'Movie')
                    """
                )
            )
            connection.execute(text("DELETE FROM outbox_events WHERE topic = 'subscription.operation.requested'"))
            connection.execute(text("DELETE FROM subscription_quota_reservations"))
            connection.execute(text("DELETE FROM subscription_operations"))
            connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '820002' AND item_type = 'Movie'"))
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata(
                        tmdb_id, item_type, title, release_date, subscription_status,
                        in_library, first_requested_at, subscription_sources_json, last_updated_at
                    ) VALUES (
                        '820002', 'Movie', '自动订阅测试电影', '2026-08-01', 'WANTED',
                        FALSE, '2026-08-30T00:00:00+00:00', '[]'::jsonb, NOW()
                    )
                    """
                )
            )
            for key, value in (
                (
                    "mp_config",
                    {
                        "moviepilot_url": f"http://127.0.0.1:{server.server_port}",
                        "moviepilot_username": "fixture-user",
                        "moviepilot_password": "fixture-password",
                        "resubscribe_daily_cap": 2,
                        "movie_search_window_days": 0,
                        "movie_pause_days": 0,
                        "delay_subscription_days": 0,
                    },
                ),
                ("watchlist_config", {}),
                ("subscription_strategy_config", {"subscription_sources": ["mp"]}),
                (
                    "dynamic_app_config",
                    {
                        "tmdb_api_base_url": f"http://127.0.0.1:{server.server_port}/3",
                        "tmdb_api_key": "fixture-tmdb-key",
                    },
                ),
            ):
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """
                    ),
                    {"key": key, "value": json.dumps(value, ensure_ascii=False)},
                )
        created = p4_store.create_job(
            task_key="auto-subscribe",
            queue_name=p4_runtime_environment.queue_name,
            max_attempts=1,
            payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "P11 native auto subscription fixture",
                "silent": True,
            },
        ).projection
        site_directory = tmp_path / f"auto-subscription-site-{compatibility_enabled}"
        site_directory.mkdir()
        (site_directory / "sitecustomize.py").write_text(
            """
import builtins
import os

guard_file = os.environ.get("ETK_TEST_AUTO_SUBSCRIPTION_IMPORT_GUARD_FILE")
if guard_file:
    original_import = builtins.__import__
    compatibility_enabled = os.environ.get("ETK_ENABLE_LEGACY_COMPATIBILITY", "").lower() in {
        "1", "true", "yes", "on"
    }

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and not compatibility_enabled and os.path.exists(guard_file) and (
            name == "tasks.subscriptions"
            or name.startswith("tasks.subscriptions.")
            or name == "handler.moviepilot"
            or name.startswith("handler.moviepilot.")
            or name == "task_manager"
            or name.startswith("task_manager.")
        ):
            raise AssertionError(f"auto subscription Worker imported legacy execution owner {name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
""".lstrip(),
            encoding="utf-8",
        )
        import_guard = tmp_path / f"auto-subscription-import-{compatibility_enabled}.guard"
        worker_environment_values = dict(p4_runtime_environment.environment)
        existing_python_path = worker_environment_values.get("PYTHONPATH", "")
        worker_environment_values["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(site_directory),
                    str(REPO_ROOT / "src"),
                    str(REPO_ROOT),
                    existing_python_path,
                ),
            )
        )
        worker_environment_values["ETK_TEST_AUTO_SUBSCRIPTION_IMPORT_GUARD_FILE"] = str(import_guard)
        worker_environment_values["TMDB_API_BASE_URL"] = f"http://127.0.0.1:{server.server_port}/3"
        worker_environment_values["TMDB_API_KEY"] = "fixture-tmdb-key"
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / f"auto-worker-{compatibility_enabled}")
        _wait_for_worker(worker, worker_environment.config)
        import_guard.write_text("active", encoding="utf-8")
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
            count=2,
        )

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        assert completed.job.result is not None
        assert completed.job.result["status"] == "succeeded"
        details = completed.job.result["details"]
        assert details["task_key"] == "auto-subscribe"
        assert details["native_handler"] == "P7SubscriptionTaskRouter"
        assert details["compatibility_fallback"] is False
        assert details["processed"] == 1
        assert details["provider_failures"] == []
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="duplicate auto subscription delivery to be skipped",
            timeout=15,
        )
        assert worker.stop() == 0
        replacement = _start_worker(
            worker_environment,
            tmp_path / f"auto-replacement-worker-{compatibility_enabled}",
        )
        _wait_for_worker(replacement, worker_environment.config)
        _send_delivery(
            worker_environment.config,
            created.job.id,
            created.current_attempt.id,
        )
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in replacement.read_log(),
            description="replacement Worker to reject completed auto subscription delivery",
            timeout=15,
        )
        assert stub.moviepilot_writes == [{"name": "自动订阅测试电影", "tmdbid": 820002, "type": "电影"}]
        with p4_runtime.engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        """
                        SELECT
                            (SELECT subscription_status FROM media_metadata
                             WHERE tmdb_id = '820002' AND item_type = 'Movie') AS media_status,
                            (SELECT COUNT(*) FROM subscription_runtime_states
                             WHERE state_json ->> 'type' = 'auto-subscribe-item'
                               AND state_json ->> 'status' = 'confirmed'
                               AND state_json ->> 'projection_applied' = 'true') AS parent_states,
                            (SELECT COUNT(*) FROM subscription_operations) AS operations,
                            (SELECT COUNT(*) FROM subscription_quota_reservations) AS quota_rows,
                            (SELECT status FROM subscription_quota_reservations LIMIT 1) AS quota_status,
                            (SELECT COUNT(*) FROM outbox_events
                             WHERE topic = 'subscription.operation.requested') AS outbox_rows
                        """
                    )
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "media_status": "SUBSCRIBED",
            "parent_states": 1,
            "operations": 1,
            "quota_rows": 1,
            "quota_status": "committed",
            "outbox_rows": 1,
        }
    finally:
        if replacement is not None:
            assert replacement.stop() == 0
        if worker is not None and worker.process.poll() is None:
            assert worker.stop() == 0
        with p4_runtime.engine.begin() as connection:
            current_auto_state_keys = set(
                connection.scalars(
                    text(
                        """
                        SELECT state_key
                        FROM subscription_runtime_states
                        WHERE state_json ->> 'type' = 'auto-subscribe-item'
                        """
                    )
                )
            )
            for state_key in current_auto_state_keys - previous_auto_state_keys:
                connection.execute(
                    text("DELETE FROM subscription_runtime_states WHERE state_key = :state_key"),
                    {"state_key": state_key},
                )
            connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '820002' AND item_type = 'Movie'"))
            for previous in previous_statuses:
                connection.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET subscription_status = :subscription_status
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    previous,
                )
        _shutdown_http_stub(server, server_thread)


@pytest.mark.parametrize(
    "compatibility_enabled",
    [False, True],
    ids=["native-only", "compatibility-enabled"],
)
def test_sharing_operations_dispatch_once_through_real_worker_profiles(
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
    compatibility_enabled: bool,
) -> None:
    fixture = uuid4().hex
    setting_keys = ("dynamic_app_config", "emby_server_id_cache", "hdhive_config", "shared_resource_config")
    server, server_thread, stub = _start_sharing_operation_stub()
    worker: ManagedProcess | None = None
    replacement: ManagedProcess | None = None
    coordinator: ManagedProcess | None = None
    prior_settings = _snapshot_app_settings(p4_runtime, setting_keys)
    operation_ids: list[UUID] = []
    account_id = f"sharing-worker-{fixture}"
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        with p4_runtime.engine.begin() as connection:
            for key, value in (
                (
                    "dynamic_app_config",
                    {
                        "telegram_api_base_url": base_url,
                        "telegram_bot_token": "fixture-bot-token",
                    },
                ),
                (
                    "shared_resource_config",
                    {"p115_shared_center_url": base_url},
                ),
                ("emby_server_id_cache", f"fixture-server-{fixture}"),
                (
                    "hdhive_config",
                    {
                        "relay_base_url": base_url,
                        "relay_instance_id": "fixture-instance",
                        "relay_instance_secret": "fixture-instance-secret",
                    },
                ),
            ):
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (:key, CAST(:value AS jsonb), NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """
                    ),
                    {"key": key, "value": json.dumps(value, separators=(",", ":"))},
                )

        ledger = PostgresContributionLedger(p4_runtime.unit_of_work)
        operations = PostgresSharingOperationRepository(p4_runtime.unit_of_work)
        ledger.ensure_account(account_id, 5)
        sharing_service = SharingLedgerService(
            ledger,
            operations,
            unit_of_work=p4_runtime.unit_of_work,
        )
        reserved = sharing_service.reserve_transfer(
            account_id=account_id,
            source_id=f"source-{fixture}",
            amount=1,
            principal_id=f"principal-{fixture}",
            idempotency_key=f"lease-{fixture}",
        )
        operation_ids.append(UUID(str(reserved.payload["operation_id"])))
        cancelled_reserved = sharing_service.reserve_transfer(
            account_id=account_id,
            source_id=f"source-cancelled-{fixture}",
            amount=1,
            principal_id=f"principal-{fixture}",
            idempotency_key=f"lease-cancelled-{fixture}",
        )
        cancelled_operation_id = UUID(str(cancelled_reserved.payload["operation_id"]))
        cancelled_operation = operations.request_cancel(cancelled_operation_id)
        assert cancelled_operation is not None
        assert cancelled_operation.status.value == "cancelled"
        operation_ids.append(cancelled_operation_id)
        device_operation = operations.reserve(
            SharingOperationIntent.create(
                kind=SharingOperationKind.REGISTER_DEVICE,
                scope_key=f"device-registration:{fixture}",
                principal_id=f"principal-{fixture}",
                request={
                    "provider": "shared_center",
                    "action": "register_device",
                    "payload": {"name": "Fixture Device", "server_id_hash": "a" * 64},
                    "local_continuation": {"type": "enable_shared_resource"},
                },
                idempotency_key=f"device-{fixture}",
            )
        )
        operation_ids.append(device_operation.id)
        unknown_report = operations.reserve(
            SharingOperationIntent.create(
                kind=SharingOperationKind.TRANSFER,
                scope_key=f"transfer-report:{fixture}",
                principal_id=f"principal-{fixture}",
                request={
                    "provider": "shared_center",
                    "action": "report_transfer",
                    "payload": {"transfer_id": f"transfer-{fixture}", "result": "ok"},
                },
                idempotency_key=f"report-{fixture}",
            )
        )
        operation_ids.append(unknown_report.id)
        hdhive_registration = operations.reserve(
            SharingOperationIntent.create(
                kind=SharingOperationKind.REGISTER_DEVICE,
                scope_key=f"hdhive-instance-registration:{fixture}",
                principal_id=f"principal-{fixture}",
                request={"provider": "hdhive_admin", "action": "register", "payload": {}},
                idempotency_key=f"hdhive-register-{fixture}",
            )
        )
        operation_ids.append(hdhive_registration.id)
        for kind, scope, request in (
            (
                SharingOperationKind.HDHIVE_CHECKIN,
                f"hdhive-checkin:{fixture}",
                {"provider": "hdhive", "action": "checkin", "payload": {"is_gambler": False}},
            ),
            (
                SharingOperationKind.TELEGRAM_DELIVERY,
                f"telegram-delivery:{fixture}",
                {
                    "provider": "telegram",
                    "action": "send_message",
                    "payload": {"chat_id": "fixture-chat", "text": "fixture-message"},
                },
            ),
        ):
            operation_ids.append(
                operations.reserve(
                    SharingOperationIntent.create(
                        kind=kind,
                        scope_key=scope,
                        principal_id=f"principal-{fixture}",
                        request=request,
                        idempotency_key=scope,
                    )
                ).id
            )

        worker_environment_values = dict(p4_runtime_environment.environment)
        if compatibility_enabled:
            worker_environment_values["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
        else:
            worker_environment_values.pop("ETK_ENABLE_LEGACY_COMPATIBILITY", None)
        worker_environment = RuntimeEnvironment(
            environment=worker_environment_values,
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        coordinator_environment = RuntimeEnvironment(
            environment={
                **p4_runtime_environment.environment,
                "ETK_ENABLE_LEGACY_COMPATIBILITY": "true",
            },
            config=p4_runtime_environment.config,
            data_dir=p4_runtime_environment.data_dir,
            queue_name=p4_runtime_environment.queue_name,
        )
        worker = _start_worker(worker_environment, tmp_path / f"sharing-worker-{compatibility_enabled}")
        _wait_for_worker(worker, worker_environment.config)
        coordinator = _start_coordinator(
            coordinator_environment,
            tmp_path / f"sharing-coordinator-{compatibility_enabled}",
        )
        _wait_until(
            lambda: _health_state(coordinator.log_path.with_suffix(".health.json")) == "leader",
            description="sharing Coordinator leadership",
            timeout=30,
        )

        def operations_terminal() -> bool:
            with p4_runtime.engine.connect() as connection:
                rows = connection.execute(
                    text(
                        """
                        SELECT id, status
                        FROM sharing_operations
                        WHERE id = ANY(CAST(:ids AS uuid[]))
                        """
                    ),
                    {"ids": operation_ids},
                ).all()
            status_by_id = {operation_id: status for operation_id, status in rows}
            return len(rows) == len(operation_ids) and all(
                status_by_id.get(operation_id)
                == (
                    "cancelled"
                    if operation_id == cancelled_operation_id
                    else ("indeterminate" if operation_id == unknown_report.id else "confirmed")
                )
                for operation_id in operation_ids
            )

        _wait_until(operations_terminal, description="sharing operation terminal states", timeout=45)
        for operation_id in operation_ids:
            _send_sharing_operation(worker_environment.config, operation_id, count=2)
        time.sleep(1)
        assert worker.stop() == 0
        worker = None
        replacement = _start_worker(
            worker_environment,
            tmp_path / f"sharing-replacement-worker-{compatibility_enabled}",
        )
        _wait_for_worker(replacement, worker_environment.config)
        for operation_id in operation_ids:
            _send_sharing_operation(worker_environment.config, operation_id)
        time.sleep(1)

        assert sorted(request["path"] for request in stub.requests) == sorted(
            [
                "/api/v1/transfers/lease",
                "/api/v1/devices/register",
                "/api/v1/transfers/report",
                "/api/etk/register",
                "/api/hdhive/checkin",
                "/botfixture-bot-token/sendMessage",
            ]
        )
        hdhive_request = next(request for request in stub.requests if request["path"] == "/api/hdhive/checkin")
        assert hdhive_request["instance_id"] == "fixture-instance"
        assert hdhive_request["instance_secret"] == "fixture-instance-secret"
        with p4_runtime.engine.connect() as connection:
            registration_result = connection.scalar(
                text("SELECT result_json FROM sharing_operations WHERE id = :id"),
                {"id": hdhive_registration.id},
            )
            rows = connection.execute(
                text(
                    """
                    SELECT operation.id, operation.status, outbox.status
                    FROM sharing_operations AS operation
                    JOIN outbox_events AS outbox
                      ON outbox.aggregate_type = 'sharing-operation'
                     AND outbox.aggregate_id = operation.id::text
                    WHERE operation.id = ANY(CAST(:ids AS uuid[]))
                    ORDER BY operation.event_sequence
                    """
                ),
                {"ids": operation_ids},
            ).all()
        assert registration_result == {
            "registered": True,
            "instance_id_hash": hashlib.sha256(b"fixture-instance").hexdigest(),
        }
        assert "secret" not in json.dumps(registration_result, sort_keys=True)
        assert rows == [
            (
                operation_id,
                (
                    "cancelled"
                    if operation_id == cancelled_operation_id
                    else ("indeterminate" if operation_id == unknown_report.id else "confirmed")
                ),
                "processed",
            )
            for operation_id in operation_ids
        ]
        assert ledger.balance(account_id) == 4
        with p4_runtime.engine.connect() as connection:
            shared_config = connection.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = 'shared_resource_config'")
            )
        assert shared_config["p115_shared_resource_enabled"] is True
    finally:
        if coordinator is not None:
            assert coordinator.stop() == 128 + signal.SIGTERM
        if replacement is not None:
            assert replacement.stop() == 0
        if worker is not None:
            assert worker.stop() == 0
        with p4_runtime.engine.begin() as connection:
            if operation_ids:
                connection.execute(
                    text(
                        """
                        DELETE FROM outbox_events
                        WHERE aggregate_type = 'sharing-operation'
                          AND aggregate_id = ANY(CAST(:ids AS text[]))
                        """
                    ),
                    {"ids": [str(value) for value in operation_ids]},
                )
                connection.execute(
                    text("DELETE FROM sharing_credit_ledger WHERE operation_id = ANY(CAST(:ids AS uuid[]))"),
                    {"ids": operation_ids},
                )
                connection.execute(
                    text("DELETE FROM sharing_operations WHERE id = ANY(CAST(:ids AS uuid[]))"),
                    {"ids": operation_ids},
                )
            connection.execute(
                text("DELETE FROM sharing_credit_accounts WHERE account_id = :account_id"),
                {"account_id": account_id},
            )
        _restore_app_settings(p4_runtime, prior_settings, setting_keys)
        _shutdown_http_stub(server, server_thread)


def _create_empty_chain_job(store: PostgresJobStore, queue_name: str):
    return store.create_job(
        task_key="task-chain-high-freq",
        queue_name=queue_name,
        payload={
            "payload_version": 1,
            "parameters": {},
            "processor_type": "media",
            "display_name": "P4 empty task-chain smoke",
            "silent": True,
        },
    )


def _create_operation_job(
    store: PostgresJobStore,
    queue_name: str,
    *,
    marker: Path,
    mode: str,
    max_attempts: int = 1,
    timeout_seconds: int | None = None,
):
    return store.create_job(
        task_key="p4-test-safe-point",
        queue_name=queue_name,
        max_attempts=max_attempts,
        timeout_seconds=timeout_seconds,
        payload={
            "payload_version": 1,
            "parameters": {"marker": str(marker), "mode": mode},
            "positional_parameters": [],
            "processor_type": "media",
            "display_name": "P4 test-only safe point",
            "silent": True,
            "legacy_operation": {"module": OPERATION_MODULE, "qualname": "_safe_point_operation"},
        },
    )


def _start_worker(
    runtime: RuntimeEnvironment,
    log_path: Path,
    *,
    compatibility: bool = False,
) -> ManagedProcess:
    environment = dict(runtime.environment)
    if compatibility:
        environment["ETK_ENABLE_LEGACY_COMPATIBILITY"] = "true"
    return _start_process(
        [
            sys.executable,
            "main.py",
            "worker",
            "--log-level",
            "INFO",
            "--concurrency",
            "1",
            "--hostname",
            f"p4-{uuid4().hex[:8]}@%h",
        ],
        environment,
        log_path.with_suffix(".log"),
    )


def _start_coordinator(runtime: RuntimeEnvironment, log_path: Path) -> ManagedProcess:
    health_path = log_path.with_suffix(".health.json")
    return _start_process(
        [sys.executable, "main.py", "coordinator", "--health-file", str(health_path)],
        runtime.environment,
        log_path.with_suffix(".log"),
    )


def _start_process(command: list[str], environment: Mapping[str, str], log_path: Path) -> ManagedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stream = log_path.open("w", encoding="utf-8")
    try:
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=dict(environment),
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )
    except BaseException:
        stream.close()
        raise
    return ManagedProcess(process=process, stream=stream, log_path=log_path)


def _wait_for_worker(worker: ManagedProcess, config: TaskRuntimeConfig) -> None:
    app = create_celery_app(config)
    try:

        def ping() -> bool:
            _assert_running(worker)
            return bool(app.control.ping(timeout=0.5))

        _wait_until(ping, description="Celery Worker ping", timeout=30)
    finally:
        app.close()


def _send_delivery(config: TaskRuntimeConfig, job_id: UUID, attempt_id: UUID, *, count: int = 1) -> None:
    app = create_celery_app(config)
    try:
        for _ in range(count):
            app.send_task(
                EXECUTE_JOB_TASK,
                kwargs={"event_id": str(uuid4()), "job_id": str(job_id), "attempt_id": str(attempt_id)},
                task_id=str(attempt_id),
                queue=config.queue_name,
                routing_key=config.queue_name,
                serializer="json",
            )
    finally:
        app.close()


def _send_media_effect(config: TaskRuntimeConfig, effect_id: UUID) -> None:
    app = create_celery_app(config)
    try:
        app.send_task(
            EXECUTE_MEDIA_EFFECT_TASK,
            kwargs={"effect_id": str(effect_id)},
            task_id=str(effect_id),
            queue=config.queue_name,
            routing_key=config.queue_name,
            serializer="json",
        )
    finally:
        app.close()


def _send_collection_operation(config: TaskRuntimeConfig, operation_id: UUID, *, count: int = 1) -> None:
    app = create_celery_app(config)
    try:
        for _ in range(count):
            app.send_task(
                EXECUTE_COLLECTION_OPERATION_TASK,
                kwargs={"operation_id": str(operation_id)},
                task_id=str(operation_id),
                queue=config.queue_name,
                routing_key=config.queue_name,
                serializer="json",
            )
    finally:
        app.close()


def _send_sharing_operation(config: TaskRuntimeConfig, operation_id: UUID, *, count: int = 1) -> None:
    app = create_celery_app(config)
    try:
        for _ in range(count):
            app.send_task(
                EXECUTE_SHARING_OPERATION_TASK,
                kwargs={"operation_id": str(operation_id)},
                task_id=str(operation_id),
                queue=config.queue_name,
                routing_key=config.queue_name,
                serializer="json",
            )
    finally:
        app.close()


def _wait_for_job(
    store: PostgresJobStore,
    job_id: UUID,
    statuses: set[JobStatus],
    *,
    timeout: float,
) -> JobProjection:
    def selected() -> JobProjection | None:
        projection = store.get_job(job_id)
        if projection is not None and projection.job.status in statuses:
            return projection
        return None

    return _wait_until(
        selected,
        description=f"Job {job_id} to reach {[status.value for status in statuses]}",
        timeout=timeout,
    )


def _health_state(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    state = payload.get("state")
    return str(state) if state is not None else None


def _wait_until(
    predicate: Callable[[], Any],
    *,
    description: str,
    timeout: float = 15,
) -> Any:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
        except BaseException as exc:
            last_error = exc
        else:
            if value:
                return value
        time.sleep(0.1)
    if last_error is not None:
        raise AssertionError(f"timed out waiting for {description}") from last_error
    raise AssertionError(f"timed out waiting for {description}")


def _assert_running(process: ManagedProcess) -> None:
    code = process.process.poll()
    if code is not None:
        raise AssertionError(f"process exited with {code}:\n{process.read_log()}")


def _wait_for_redis(url: str) -> None:
    client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        _wait_until(client.ping, description="disposable Redis PING", timeout=15)
    finally:
        client.close()


def _docker(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if check and result.returncode != 0:
        raise AssertionError(f"docker {' '.join(arguments)} failed:\n{result.stderr}")
    return result
