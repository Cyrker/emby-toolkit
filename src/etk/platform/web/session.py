from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from http.cookies import SimpleCookie
from typing import Any, Protocol

from itsdangerous import BadData
from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class SessionCodec(Protocol):
    def dumps(self, values: Mapping[str, Any]) -> str: ...

    def loads(self, value: str, *, max_age: int | None = None) -> dict[str, Any]: ...


class SessionState(dict[str, Any]):
    """Mutable request session with enough state to reproduce Flask cookie writes."""

    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        dict.__init__(self, dict(values or {}))
        self.modified = False
        self.cleared = False

    def __setitem__(self, key: str, value: Any) -> None:
        dict.__setitem__(self, key, value)
        self.modified = True
        self.cleared = False

    def __delitem__(self, key: str) -> None:
        dict.__delitem__(self, key)
        self.modified = True

    def clear(self) -> None:
        dict.clear(self)
        self.modified = True
        self.cleared = True

    def pop(self, key: str, default: Any = None) -> Any:
        if key in self:
            self.modified = True
        return dict.pop(self, key, default)

    def popitem(self) -> tuple[str, Any]:
        item = dict.popitem(self)
        self.modified = True
        return item

    def setdefault(self, key: str, default: Any = None) -> Any:
        if key not in self:
            self.modified = True
            self.cleared = False
        return dict.setdefault(self, key, default)

    def update(self, values: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
        incoming = dict(values or {}, **kwargs)
        if incoming:
            dict.update(self, incoming)
            self.modified = True
            self.cleared = False


class CookieSessionMiddleware:
    """Read and write Flask-compatible signed sessions for new FastAPI routes."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        codec_provider: Callable[[], SessionCodec],
        cookie_name: str = "session",
        max_age_seconds: int = 31 * 24 * 60 * 60,
    ) -> None:
        if not cookie_name:
            raise ValueError("session cookie name must not be empty")
        if max_age_seconds <= 0:
            raise ValueError("session max age must be positive")
        self.app = app
        self.codec_provider = codec_provider
        self.cookie_name = cookie_name
        self.max_age_seconds = max_age_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        session = self._load_session(request.cookies.get(self.cookie_name))
        state = scope.setdefault("state", {})
        if isinstance(state, dict):
            state["session"] = session

        async def send_with_session(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.add_vary_header("Cookie")
                cookie = self._response_cookie(session)
                if cookie is not None:
                    headers.append("set-cookie", cookie)
            await send(message)

        await self.app(scope, receive, send_with_session)

    def _load_session(self, value: str | None) -> SessionState:
        if not value:
            return SessionState()
        try:
            payload = self.codec_provider().loads(value, max_age=self.max_age_seconds)
        except (BadData, TypeError, ValueError):
            return SessionState()
        return SessionState(payload)

    def _response_cookie(self, session: SessionState) -> str | None:
        if session.cleared:
            return _delete_cookie(self.cookie_name)
        permanent = bool(session.get("_permanent"))
        if not session.modified and not permanent:
            return None
        encoded = self.codec_provider().dumps(session)
        return _set_cookie(
            self.cookie_name,
            encoded,
            max_age_seconds=self.max_age_seconds if permanent else None,
        )


def get_request_session(request: Request) -> SessionState:
    session = getattr(request.state, "session", None)
    if not isinstance(session, SessionState):
        raise RuntimeError("cookie session middleware is not configured")
    return session


def _set_cookie(name: str, value: str, *, max_age_seconds: int | None) -> str:
    cookie = SimpleCookie()
    cookie[name] = value
    morsel = cookie[name]
    morsel["path"] = "/"
    morsel["httponly"] = True
    if max_age_seconds is not None:
        morsel["expires"] = format_datetime(datetime.now(UTC) + timedelta(seconds=max_age_seconds), usegmt=True)
    return morsel.OutputString()


def _delete_cookie(name: str) -> str:
    cookie = SimpleCookie()
    cookie[name] = ""
    morsel = cookie[name]
    morsel["path"] = "/"
    morsel["httponly"] = True
    morsel["expires"] = "Thu, 01 Jan 1970 00:00:00 GMT"
    morsel["max-age"] = 0
    return morsel.OutputString()
