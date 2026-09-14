import asyncio

import anyio
import pytest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse

from etk.bootstrap.config_sync import RuntimeConfigMiddleware


async def disconnected_stream(wrapper):
    first_body = anyio.Event()
    sent = []

    async def chunks():
        yield b"abc"
        await anyio.sleep_forever()

    async def app(scope, receive, send):
        await StreamingResponse(chunks(), headers={"content-length": "6"})(scope, receive, send)

    async def receive():
        await first_body.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body":
            if message.get("body"):
                first_body.set()
            if not message.get("more_body", False):
                size = sum(len(row.get("body", b"")) for row in sent)
                if size != 6:
                    raise RuntimeError("Response content shorter than Content-Length")

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/p115/play/fixture",
        "headers": [],
        "asgi": {"spec_version": "2.0"},
    }
    with anyio.fail_after(2):
        await wrapper(app)(scope, receive, send)
    return sent


def test_legacy_base_middleware_reproduces_premature_response_end():
    async def passthrough(request, call_next):
        return await call_next(request)

    with pytest.raises(RuntimeError, match="shorter than Content-Length"):
        asyncio.run(disconnected_stream(lambda app: BaseHTTPMiddleware(app, dispatch=passthrough)))


def test_config_middleware_does_not_finalize_disconnected_stream_as_complete():
    sent = asyncio.run(disconnected_stream(lambda app: RuntimeConfigMiddleware(app, synchronize=lambda: None)))
    assert sent[0]["headers"] == [(b"content-length", b"6")]
    assert b"".join(row.get("body", b"") for row in sent) == b"abc"
    assert not any(row["type"] == "http.response.body" and not row.get("more_body") for row in sent)


def test_config_middleware_preserves_complete_range_response():
    async def run():
        messages = []

        async def chunks():
            yield b"abc"
            yield b"def"

        async def app(scope, receive, send):
            await StreamingResponse(
                chunks(),
                status_code=206,
                headers={"content-length": "6", "content-range": "bytes 0-5/100"},
            )(scope, receive, send)

        async def receive():
            await anyio.sleep_forever()

        async def send(message):
            messages.append(message)

        await RuntimeConfigMiddleware(app)(
            {"type": "http", "method": "GET", "path": "/api/p115/play/fixture", "asgi": {"spec_version": "2.4"}},
            receive,
            send,
        )
        return messages

    messages = asyncio.run(run())
    assert messages[0]["status"] == 206
    assert dict(messages[0]["headers"])[b"content-length"] == b"6"
    assert dict(messages[0]["headers"])[b"content-range"] == b"bytes 0-5/100"
    assert b"".join(message.get("body", b"") for message in messages) == b"abcdef"
    assert messages[-1]["more_body"] is False
