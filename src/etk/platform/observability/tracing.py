from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from structlog.contextvars import bind_contextvars, reset_contextvars

TRACE_ID_HEADER = "x-trace-id"
_TRACE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_TRACE_ID: ContextVar[str | None] = ContextVar("etk_trace_id", default=None)


def get_trace_id() -> str | None:
    return _TRACE_ID.get()


def new_trace_id() -> str:
    return uuid.uuid4().hex


def normalize_trace_id(value: str | None) -> str:
    candidate = str(value or "").strip()
    if candidate and _TRACE_ID_PATTERN.fullmatch(candidate):
        return candidate
    return new_trace_id()


@contextmanager
def trace_context(trace_id: str | None = None) -> Iterator[str]:
    resolved = normalize_trace_id(trace_id)
    token = _TRACE_ID.set(resolved)
    structlog_tokens = bind_contextvars(trace_id=resolved)
    try:
        yield resolved
    finally:
        reset_contextvars(**structlog_tokens)
        _TRACE_ID.reset(token)


class TraceIdMiddleware:
    """Bind one validated trace ID to each HTTP request and response."""

    def __init__(self, app: ASGIApp, *, header_name: str = TRACE_ID_HEADER) -> None:
        self.app = app
        self.header_name = header_name.casefold()
        self.response_header_name = header_name

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = _header_value(scope, self.header_name)
        with trace_context(incoming) as trace_id:
            state = scope.setdefault("state", {})
            if isinstance(state, dict):
                state["trace_id"] = trace_id

            async def send_with_trace(message: Message) -> None:
                if message["type"] == "http.response.start":
                    headers = MutableHeaders(scope=message)
                    headers[self.response_header_name] = trace_id
                await send(message)

            await self.app(scope, receive, send_with_trace)


def _header_value(scope: Scope, name: str) -> str | None:
    expected = name.encode("latin-1")
    for key, value in scope.get("headers", []):
        if key.lower() == expected:
            return value.decode("latin-1")
    return None
