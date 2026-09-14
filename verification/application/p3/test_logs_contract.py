from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

from fastapi import FastAPI
from flask import Flask
from starlette.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec, configure_legacy_session
from etk.domains.operations.job_logs import JOB_LOG_PREFIX, PersistentJobLogStorage
from etk.domains.operations.log_router import router
from etk.domains.operations.logs import (
    LocalLogStorage,
    LogContextBlock,
    LogReadFailure,
    LogService,
    LogStorage,
    render_log_html,
)
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
LOG_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "log-routes.json"
SECRET_KEY = b"p3-log-session-contract-key"


def _native_storage(tmp_path: Path, rows: list[dict[str, Any]]) -> PersistentJobLogStorage:
    uow = MagicMock()
    uow.__enter__.return_value = uow
    uow.session.execute.return_value.mappings.return_value.all.return_value = rows
    return PersistentJobLogStorage(lambda: uow, LocalLogStorage(tmp_path / "absent-logs"))


def test_native_history_hides_internal_rotation_lock(tmp_path: Path) -> None:
    storage = _native_storage(tmp_path, [])
    directory = tmp_path / "absent-logs"
    directory.mkdir()
    (directory / ".app-log.lock").touch()
    (directory / "app.log").write_text("runtime log")
    assert storage.list_names() == ["app.log"]


def test_history_reverses_records_without_reversing_traceback(tmp_path: Path) -> None:
    log = (
        "2026-09-12 12:00:01,001 - fixture.worker - ERROR - 执行失败\n"
        "Traceback (most recent call last):\n"
        '  File "worker.py", line 12\n'
        "ValueError: invalid <script>alert(1)</script>\n"
        "2026-09-12 12:00:02,001 - fixture.monitor - INFO - 文件监控已启动\n"
    )
    (tmp_path / "app.log").write_text(log)
    _, records = LogService(LocalLogStorage(tmp_path)).view_log_file("app.log")
    assert len(records) == 2
    assert "文件监控已启动" in records[0]
    assert records[1].index("执行失败") < records[1].index("Traceback") < records[1].index("ValueError")
    rendered = render_log_html([LogContextBlock("app.log", "", records)], "")
    assert "2026-09-12 12:00:01" in rendered
    assert '<span class="level">ERROR</span>' in rendered
    assert '<span class="source">fixture.worker</span>' in rendered
    assert "<details><summary>异常堆栈" in rendered
    assert "<script>alert(1)</script>" not in rendered
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rendered


def test_history_health_filter_only_marks_successful_polling():
    lines = (
        '2026-09-12 12:00:01,001 - uvicorn.access - INFO - 127.0.0.1 - "GET /api/health HTTP/1.1" 200\n',
        '2026-09-12 12:00:02,001 - uvicorn.access - INFO - 127.0.0.1 - "GET /api/health HTTP/1.1" 503\n',
        "普通说明，不应被渲染为错误\n",
    )
    rendered = render_log_html([LogContextBlock("app.log", "", lines)], "")
    assert rendered.count('data-health="1"') == 1
    assert 'class="line lvl-RAW"' in rendered
    assert 'id="hide-health"' in rendered


def test_native_task_history_without_legacy_log_directory(tmp_path: Path) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000001")
    storage = _native_storage(
        tmp_path,
        [
            {
                "id": job_id,
                "task_key": "populate-metadata",
                "display_name": "同步媒体数据",
                "status": "succeeded",
                "progress": 100,
                "progress_message": "同步完成",
                "created_at": "2026-09-12T04:00:00+00:00",
                "finished_at": "2026-09-12T04:01:00+00:00",
                "logs_json": [
                    {"timestamp": "2026-09-12T04:00:30+00:00", "level": "info", "message": "正在同步 <test>"}
                ],
            }
        ],
    )
    app, _ = _build_app(tmp_path, storage=storage)
    filename = "app.log"
    with TestClient(app) as client:
        assert client.get("/api/logs/list").json() == [filename]
        response = client.get("/api/logs/view", params={"filename": filename, "format": "html"})
        assert response.status_code == 200
        assert "同步媒体数据" in response.text
        assert "正在同步 &lt;test&gt;" in response.text
        assert "状态=succeeded" in response.text
        assert "[object Object]" not in response.text
        assert "不是完整进程日志" in response.text
        found = client.get("/api/logs/search", params={"q": "正在同步"}).json()
        assert len(found) == 1
        context = client.get("/api/logs/search_context", params={"q": "正在同步", "format": "html"})
        assert 'class="log-block"' in context.text
        assert "同步完成" in context.text
        assert client.get("/api/logs/search_context", params={"q": "不匹配"}).json() == []


def test_native_task_history_validates_names_and_permissions(tmp_path: Path) -> None:
    storage = _native_storage(tmp_path, [])
    app, _ = _build_app(tmp_path, storage=storage)
    with TestClient(app) as client:
        assert client.get("/api/logs/list").json() == []
        assert client.get("/api/logs/view", params={"filename": f"{JOB_LOG_PREFIX}invalid"}).status_code == 403
        missing = f"{JOB_LOG_PREFIX}00000000-0000-0000-0000-000000000002"
        assert client.get("/api/logs/view", params={"filename": missing}).status_code == 404
    protected, _ = _build_app(tmp_path, storage=storage, auth_enabled=True)
    with TestClient(protected) as client:
        assert client.get("/api/logs/list").status_code in {401, 403}
        assert client.get("/api/logs/view", params={"filename": missing}).status_code in {401, 403}


def test_native_history_keeps_terminal_failure_visible(tmp_path: Path) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000001")
    storage = _native_storage(
        tmp_path,
        [
            {
                "id": job_id,
                "task_key": "populate-metadata",
                "display_name": "同步媒体数据",
                "status": "failed",
                "progress": 100,
                "progress_message": "媒体元数据同步完成。",
                "created_at": "2026-09-12T04:00:00+00:00",
                "finished_at": "2026-09-12T04:01:00+00:00",
                "logs_json": [],
                "error_summary_json": {"message": "媒体元数据同步部分完成。"},
            }
        ],
    )
    lines = storage.read_lines(f"{JOB_LOG_PREFIX}{job_id}")
    assert any("ERROR" in line and "部分完成" in line for line in lines)
    assert "状态=failed" in lines[-1]


class FakePermissionReader:
    def __init__(self) -> None:
        self.admins = {"fixture-admin"}

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class BrokenLogStorage:
    def list_names(self) -> list[str]:
        raise OSError("fixture list failure")

    def read_lines(self, filename: str) -> list[str]:
        raise LogReadFailure(filename)


def _codec() -> LegacySessionCodec:
    flask_app = Flask("p3-log-cookie")
    configure_legacy_session(flask_app, Path("."), secret_key=SECRET_KEY)
    return LegacySessionCodec(flask_app)


def _build_app(
    log_directory: Path,
    *,
    auth_enabled: bool = False,
    storage: LogStorage | None = None,
) -> tuple[FastAPI, LegacySessionCodec]:
    codec = _codec()
    service = LogService(storage or LocalLogStorage(log_directory))
    permissions = FakePermissionReader()
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={"operations.logs": lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, codec


def _fixture_cases() -> dict[str, dict[str, Any]]:
    payload = json.loads(LOG_FIXTURE.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def _assert_fixture_response(response, case_id: str) -> None:
    expected = _fixture_cases()[case_id]["response"]
    assert response.status_code == expected["status"]
    assert response.headers["content-type"] == expected["content_type"]
    if "body" in expected:
        if expected["content_type"].startswith("application/json"):
            assert response.json() == expected["body"]
        else:
            assert response.text == expected["body"]
    required = expected.get("body_contains", [])
    if isinstance(required, str):
        required = [required]
    for value in required:
        assert value in response.text
    excluded = expected.get("body_excludes")
    if excluded:
        assert excluded not in response.text


def _write(path: Path, lines: list[str]) -> None:
    path.write_text("".join(lines), encoding="utf-8")


def test_fixture_covers_all_four_log_routes() -> None:
    payload = json.loads(LOG_FIXTURE.read_text(encoding="utf-8"))
    assert {(item["method"], item["path"]) for item in payload["routes"]} == {
        ("GET", "/api/logs/list"),
        ("GET", "/api/logs/view"),
        ("GET", "/api/logs/search"),
        ("GET", "/api/logs/search_context"),
    }
    assert payload["provenance"]["captured_live"] is False
    assert payload["provenance"]["external_validation"] == "pending"


def test_legacy_admin_permission_contract(tmp_path: Path) -> None:
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    (log_directory / "app.log").touch()
    app, codec = _build_app(log_directory, auth_enabled=True)

    with TestClient(app) as client:
        _assert_fixture_response(client.get("/api/logs/list"), "permission-denied")
        client.cookies.set(
            "session",
            codec.dumps({"emby_user_id": "fixture-user", "emby_is_admin": True}),
        )
        _assert_fixture_response(client.get("/api/logs/list"), "permission-denied")
        client.cookies.set(
            "session",
            codec.dumps({"emby_user_id": "fixture-admin", "emby_is_admin": False}),
        )
        assert client.get("/api/logs/list").status_code == 200


def test_list_and_view_preserve_sorting_reverse_order_and_content_types(tmp_path: Path) -> None:
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    oldest = "2026-08-23 12:00:01,000 - fixture.logs - INFO - oldest\n"
    latest = "2026-08-23 12:00:02,000 - fixture.logs - ERROR - latest\n"
    _write(log_directory / "app.log", [oldest, latest])
    (log_directory / "app.log.2.gz").touch()
    (log_directory / "app.log.1.gz").touch()
    (log_directory / "ignored.txt").touch()
    app, _ = _build_app(log_directory)

    with TestClient(app) as client:
        _assert_fixture_response(client.get("/api/logs/list"), "list-success")
        _assert_fixture_response(
            client.get("/api/logs/view", params={"filename": "app.log"}),
            "view-plain-text",
        )
        html_response = client.get(
            "/api/logs/view",
            params={"filename": "app.log", "format": "HTML"},
        )

    assert html_response.status_code == 200
    assert html_response.headers["content-type"] == "text/html; charset=utf-8"
    assert "latest" in html_response.text
    assert html_response.text.index("latest") < html_response.text.index("oldest")


def test_search_preserves_case_insensitive_matching_date_sort_and_shape(tmp_path: Path) -> None:
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    _write(
        log_directory / "app.log",
        [
            "2026-08-23 12:00:01,000 - fixture.logs - INFO - ignored\n",
            "no timestamp NEEDLE middle\n",
            "2026-08-23 12:00:03,000 - fixture.logs - INFO - needle newest\n",
        ],
    )
    _write(
        log_directory / "app.log.1",
        ["2026-08-22 12:00:00,000 - fixture.logs - INFO - needle older\n"],
    )
    app, _ = _build_app(log_directory)

    with TestClient(app) as client:
        response = client.get("/api/logs/search", params={"q": "needle"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    payload = response.json()
    expected = _fixture_cases()["search-success"]["response"]["body"]
    assert payload[0] == expected[0]
    assert payload[1] == expected[1]
    assert payload[2] == {
        "file": "app.log",
        "line_num": 2,
        "content": "no timestamp NEEDLE middle",
        "date": "",
    }


def test_context_preserves_closed_block_filtering_and_html_escaping(tmp_path: Path) -> None:
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    start = "2026-08-23 12:10:00,000 - fixture.logs - INFO - Webhook: 收到入库事件 'Fixture Item'，已分派预检任务。\n"
    dangerous = "2026-08-23 12:10:01,000 - fixture.logs - INFO - Fixture Item <script>alert(1)</script>\n"
    interference = "2026-08-23 12:10:02,000 - fixture.logs - INFO - 项目 'Other Item' 开始处理\n"
    end = (
        "2026-08-23 12:10:03,000 - fixture.logs - INFO - "
        "后台任务 'Webhook入库: Fixture Item' 结束，最终状态: 处理完成。\n"
    )
    _write(log_directory / "app.log", [start, dangerous, interference, end])
    app, _ = _build_app(log_directory)

    with TestClient(app) as client:
        json_response = client.get("/api/logs/search_context", params={"q": "fixture item"})
        html_response = client.get(
            "/api/logs/search_context",
            params={"q": "Fixture Item", "format": "html"},
        )

    assert json_response.status_code == 200
    assert json_response.headers["content-type"] == "application/json"
    assert json_response.json() == [
        {
            "file": "app.log",
            "date": "2026-08-23 12:10:00",
            "lines": [start, dangerous, end],
        }
    ]
    _assert_fixture_response(html_response, "context-html-escaped")


def test_invalid_filename_missing_file_and_query_validation_contract(tmp_path: Path) -> None:
    log_directory = tmp_path / "logs"
    log_directory.mkdir()
    _write(log_directory / "app.log", ["fixture\n"])
    app, _ = _build_app(log_directory)

    with TestClient(app) as client:
        _assert_fixture_response(
            client.get("/api/logs/view", params={"filename": "../app.log"}),
            "view-sanitized-filename",
        )
        forbidden = client.get("/api/logs/view", params={"filename": "fixture.txt"})
        assert forbidden.status_code == 403
        assert forbidden.headers["content-type"] == "text/html; charset=utf-8"
        _assert_fixture_response(
            client.get("/api/logs/view", params={"filename": "app.log.404"}),
            "view-missing",
        )
        _assert_fixture_response(client.get("/api/logs/search", params={"q": "  "}), "search-query-required")
        _assert_fixture_response(
            client.get("/api/logs/search_context"),
            "context-query-required",
        )


def test_internal_failures_preserve_json_and_html_500_contracts(tmp_path: Path) -> None:
    app, _ = _build_app(tmp_path / "unused", storage=BrokenLogStorage())

    with TestClient(app) as client:
        listed = client.get("/api/logs/list")
        searched = client.get("/api/logs/search", params={"q": "fixture"})
        context = client.get("/api/logs/search_context", params={"q": "fixture"})
        viewed = client.get("/api/logs/view", params={"filename": "app.log"})

    assert listed.status_code == 500
    assert listed.headers["content-type"] == "application/json"
    assert listed.json() == {"error": "无法读取日志文件列表"}
    assert searched.status_code == 500
    assert searched.json() == {"error": "搜索过程中发生服务器内部错误"}
    assert context.status_code == 500
    assert context.json() == {"error": "服务器内部错误"}
    assert viewed.status_code == 500
    assert viewed.headers["content-type"] == "text/html; charset=utf-8"
    assert "读取文件 'app.log' 时发生内部错误。" in html.unescape(viewed.text)
