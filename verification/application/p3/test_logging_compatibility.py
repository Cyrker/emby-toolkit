from __future__ import annotations

import importlib
import io
import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from etk.platform.observability import configure_logging, trace_context


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    httpx_logger = logging.getLogger("httpx")
    previous_handlers = list(root.handlers)
    previous_level = root.level
    previous_httpx_filters = list(httpx_logger.filters)
    yield
    for handler in tuple(root.handlers):
        if handler not in previous_handlers:
            root.removeHandler(handler)
            handler.close()
    root.handlers[:] = previous_handlers
    root.setLevel(previous_level)
    httpx_logger.filters[:] = previous_httpx_filters
    structlog.reset_defaults()


def _reload_legacy_logging():
    module = importlib.import_module("logger_setup")
    return importlib.reload(module)


def _handlers_with(attribute: str) -> list[logging.Handler]:
    return [handler for handler in logging.getLogger().handlers if getattr(handler, attribute, False)]


def test_legacy_initialization_preserves_p2_handler_redaction_trace_and_idempotency() -> None:
    stream = io.StringIO()
    managed = configure_logging(json_output=True, stream=stream, level="INFO")
    legacy = _reload_legacy_logging()
    legacy.frontend_log_queue.clear()

    assert managed in logging.getLogger().handlers
    assert len(_handlers_with("_etk_observability_handler")) == 1
    assert len(_handlers_with("_etk_legacy_console_handler")) == 0
    assert len(_handlers_with("_etk_legacy_frontend_handler")) == 1

    license_secret = "fixture-license-secret"
    api_secret = "fixture-api-secret"
    with trace_context("p3-legacy-trace"):
        logging.getLogger("legacy.compatibility").info(f"正在向云端验证激活码: {license_secret}; api_key={api_secret}")

    rendered = stream.getvalue()
    payload = json.loads(rendered.strip())
    frontend_entry = legacy.frontend_log_queue[-1]
    assert payload["trace_id"] == "p3-legacy-trace"
    assert "p3-legacy-trace" in frontend_entry
    assert license_secret not in rendered
    assert api_secret not in rendered
    assert license_secret not in frontend_entry
    assert api_secret not in frontend_entry
    assert rendered.count("<redacted>") == 2
    assert frontend_entry.count("<redacted>") == 2

    legacy = importlib.reload(legacy)
    assert len(_handlers_with("_etk_observability_handler")) == 1
    assert len(_handlers_with("_etk_legacy_console_handler")) == 0
    assert len(_handlers_with("_etk_legacy_frontend_handler")) == 1
    httpx_filters = [
        item for item in logging.getLogger("httpx").filters if getattr(item, "_etk_legacy_httpx_filter", False)
    ]
    assert len(httpx_filters) == 1

    stream.seek(0)
    stream.truncate(0)
    with trace_context("p3-reload-trace"):
        logging.getLogger("legacy.compatibility").info("reload-visible-once")
    assert stream.getvalue().count("reload-visible-once") == 1
    assert sum("reload-visible-once" in entry for entry in legacy.frontend_log_queue) == 1


def test_legacy_file_handler_is_idempotent_redacted_and_trace_aware(tmp_path) -> None:
    stream = io.StringIO()
    configure_logging(json_output=True, stream=stream, level="INFO")
    legacy = _reload_legacy_logging()
    legacy.frontend_log_queue.clear()

    legacy.add_file_handler(str(tmp_path))
    legacy.add_file_handler(str(tmp_path))
    file_handlers = _handlers_with("_etk_legacy_file_handler")
    assert len(file_handlers) == 1

    secret = "fixture-file-secret"
    with trace_context("p3-file-trace"):
        logging.getLogger("legacy.file").warning(f"credential: {secret}")
    file_handlers[0].flush()

    content = (tmp_path / legacy.LOG_FILE_NAME).read_text(encoding="utf-8")
    assert "p3-file-trace" in content
    assert secret not in content
    assert "<redacted>" in content
    assert secret not in "\n".join(legacy.frontend_log_queue)
