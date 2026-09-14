from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from etk.platform.observability import configure_logging, get_logger, redact_sensitive, trace_context


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    previous_handlers = list(root.handlers)
    previous_level = root.level
    yield
    for handler in tuple(root.handlers):
        if handler not in previous_handlers:
            root.removeHandler(handler)
            handler.close()
    root.handlers[:] = previous_handlers
    root.setLevel(previous_level)
    structlog.reset_defaults()


def test_recursive_redaction_handles_cycles_and_sensitive_key_shapes() -> None:
    value: dict[str, object] = {
        "safe": "visible",
        "db_password": "database-secret",
        "nested": [
            {
                "Authorization": "bearer-secret",
                "clientSecret": "client-secret",
                "monkey": "not-secret",
            }
        ],
    }
    value["self"] = value

    redacted = redact_sensitive(value)

    assert redacted["safe"] == "visible"
    assert redacted["db_password"] == "<redacted>"
    assert redacted["nested"][0]["Authorization"] == "<redacted>"
    assert redacted["nested"][0]["clientSecret"] == "<redacted>"
    assert redacted["nested"][0]["monkey"] == "not-secret"
    assert redacted["self"] == "<recursive>"


def test_json_logging_includes_trace_id_and_filters_secrets() -> None:
    stream = io.StringIO()
    configure_logging(json_output=True, stream=stream, level="INFO")

    with trace_context("trace-123"):
        get_logger("unit.logging").info(
            "configured",
            db_password="database-secret",
            nested={"api_key": "api-secret", "safe": "visible"},
        )

    payload = json.loads(stream.getvalue().strip())
    assert payload["event"] == "configured"
    assert payload["trace_id"] == "trace-123"
    assert payload["db_password"] == "<redacted>"
    assert payload["nested"] == {"api_key": "<redacted>", "safe": "visible"}
    assert "database-secret" not in stream.getvalue()
    assert "api-secret" not in stream.getvalue()


def test_readable_logging_and_reconfiguration_do_not_duplicate_managed_handlers() -> None:
    first = io.StringIO()
    second = io.StringIO()
    configure_logging(json_output=False, stream=first)
    configure_logging(json_output=False, stream=second)

    logging.getLogger("foreign").info({"token": "stdlib-secret", "safe": "visible"})

    assert first.getvalue() == ""
    assert "visible" in second.getvalue()
    assert "stdlib-secret" not in second.getvalue()
    managed = [
        handler for handler in logging.getLogger().handlers if getattr(handler, "_etk_observability_handler", False)
    ]
    assert len(managed) == 1


def test_runtime_events_share_readable_application_file_without_duplicate_handlers(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path))
    configure_logging(json_output=True, stream=io.StringIO())
    configure_logging(json_output=True, stream=io.StringIO())
    logging.getLogger("fixture.download").info("开始下载主图")
    get_logger("fixture.mp").info("收到 MoviePilot 通知", password="fixture-secret")
    content = (tmp_path / "logs" / "app.log").read_text()
    assert content.count("开始下载主图") == 1
    assert content.count("收到 MoviePilot 通知") == 1
    assert "fixture-secret" not in content
    assert len(list((tmp_path / "logs").glob("app.log*"))) == 1


def test_worker_logging_signal_writes_shared_file(tmp_path, monkeypatch):
    from celery.signals import setup_logging

    from etk.processes import celery_app

    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path))
    responses = setup_logging.send(sender=None, loglevel=logging.INFO)
    assert any(receiver is celery_app._configure_worker_logging for receiver, _ in responses)
    assert not any(isinstance(result, Exception) for _, result in responses)
    logging.getLogger("etk.processes.p6_ingestion").info("文件稳定后开始探测")
    assert "文件稳定后开始探测" in (tmp_path / "logs" / "app.log").read_text()


def test_coordinator_entrypoint_configures_shared_file(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from etk.bootstrap.cli import main
    from etk.processes import coordinator

    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        coordinator,
        "CoordinatorRuntime",
        lambda **kwargs: SimpleNamespace(
            run=lambda: logging.getLogger("etk.processes.filesystem_monitor").info("文件监控已启动"),
            request_stop=lambda: None,
        ),
    )
    assert main(["coordinator"]) == 0
    assert "文件监控已启动" in (tmp_path / "logs" / "app.log").read_text()


def test_exception_and_stdlib_messages_redact_embedded_credentials() -> None:
    stream = io.StringIO()
    configure_logging(json_output=True, stream=stream)

    try:
        raise RuntimeError("connection failed for postgresql://user:database-password@db/etk")
    except RuntimeError:
        get_logger("unit.exception").exception("database startup failed")
    logging.getLogger("foreign").error(
        "proxy request failed: %s",
        "http://proxy-user:proxy-password@proxy.invalid/path?api_key=query-secret",
    )

    rendered = stream.getvalue()
    assert "database-password" not in rendered
    assert "proxy-password" not in rendered
    assert "query-secret" not in rendered
    assert rendered.count("<redacted>") >= 3


def test_text_redaction_handles_quoted_spaces_and_authorization_bearer_values() -> None:
    rendered = redact_sensitive(
        "password='two word secret'; Authorization: Bearer bearer-secret; "
        "url=https://api.telegram.org/bottelegram-secret/sendMessage; safe=visible"
    )

    assert "two word secret" not in rendered
    assert "bearer-secret" not in rendered
    assert "telegram-secret" not in rendered
    assert "https://api.telegram.org/bot<redacted>/sendMessage" in rendered
    assert "safe=visible" in rendered
