from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any, TextIO

import structlog

from .redaction import REDACTED, is_sensitive_key, redact_sensitive
from .tracing import get_trace_id

_MANAGED_HANDLER_ATTRIBUTE = "_etk_observability_handler"
_STANDARD_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


class SensitiveDataFilter(logging.Filter):
    """Redact structured stdlib logging values before formatters see them."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, (Mapping, str)):
            record.msg = redact_sensitive(record.msg)
        if isinstance(record.args, Mapping):
            record.args = redact_sensitive(record.args)
        elif isinstance(record.args, tuple):
            record.args = tuple(redact_sensitive(value) for value in record.args)

        for key, value in tuple(record.__dict__.items()):
            if key in _STANDARD_RECORD_FIELDS:
                continue
            record.__dict__[key] = REDACTED if is_sensitive_key(key) else redact_sensitive(value)
        record.message = record.getMessage()
        return True


def configure_logging(
    *,
    json_output: bool = False,
    level: int | str = logging.INFO,
    stream: TextIO | None = None,
) -> logging.Handler:
    """Configure stdlib and structlog with one idempotently managed handler."""

    resolved_level = _logging_level(level)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        _add_safe_extras,
        _add_trace_id,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    renderer: Any
    if json_output:
        renderer = structlog.processors.JSONRenderer(serializer=json.dumps, default=str, sort_keys=True)
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            _redact_event,
            renderer,
        ],
    )
    handler = logging.StreamHandler(stream or sys.stderr)
    setattr(handler, _MANAGED_HANDLER_ATTRIBUTE, True)
    handler.setLevel(resolved_level)
    handler.addFilter(SensitiveDataFilter())
    handler.setFormatter(formatter)

    root = logging.getLogger()
    for current in tuple(root.handlers):
        root.removeHandler(current)
        if getattr(current, _MANAGED_HANDLER_ATTRIBUTE, False):
            current.close()
    root.addHandler(handler)
    root.setLevel(resolved_level)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _redact_event,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(resolved_level),
        cache_logger_on_first_use=False,
    )
    return handler


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def _add_trace_id(
    _logger: Any,
    _method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    if "trace_id" not in event_dict:
        trace_id = get_trace_id()
        if trace_id is not None:
            event_dict["trace_id"] = trace_id
    return event_dict


def _add_safe_extras(
    _logger: Any,
    _method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    record = event_dict.get("_record")
    if not isinstance(record, logging.LogRecord):
        return event_dict
    for key, value in record.__dict__.items():
        if key in _STANDARD_RECORD_FIELDS or key == "message":
            continue
        event_dict[key] = REDACTED if is_sensitive_key(key) else redact_sensitive(value)
    return event_dict


def _redact_event(
    _logger: Any,
    _method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    return redact_sensitive(event_dict)


def _logging_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    name = str(level).upper()
    if name == "TRACE":
        return 5
    resolved = logging.getLevelNamesMapping().get(name)
    if resolved is None:
        raise ValueError(f"unknown logging level: {level!r}")
    return resolved
