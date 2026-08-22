from __future__ import annotations

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from etk.platform.observability import TraceIdMiddleware, get_trace_id, trace_context


def test_trace_context_is_nested_and_restores_the_previous_value() -> None:
    assert get_trace_id() is None
    with trace_context("outer"):
        assert get_trace_id() == "outer"
        with trace_context("inner"):
            assert get_trace_id() == "inner"
        assert get_trace_id() == "outer"
    assert get_trace_id() is None


def test_trace_middleware_propagates_valid_header_and_resets_context() -> None:
    async def endpoint(request) -> JSONResponse:
        return JSONResponse({"context": get_trace_id(), "state": request.scope["state"]["trace_id"]})

    app = Starlette(routes=[Route("/", endpoint)])
    with TestClient(TraceIdMiddleware(app)) as client:
        response = client.get("/", headers={"x-trace-id": "request.trace-123"})

    assert response.status_code == 200
    assert response.headers["x-trace-id"] == "request.trace-123"
    assert response.json() == {"context": "request.trace-123", "state": "request.trace-123"}
    assert get_trace_id() is None


def test_trace_middleware_replaces_unsafe_header() -> None:
    async def endpoint(_request) -> JSONResponse:
        return JSONResponse({"trace_id": get_trace_id()})

    app = Starlette(routes=[Route("/", endpoint)])
    with TestClient(TraceIdMiddleware(app)) as client:
        response = client.get("/", headers={"x-trace-id": "unsafe header value"})

    generated = response.json()["trace_id"]
    assert generated != "unsafe header value"
    assert len(generated) == 32
    assert response.headers["x-trace-id"] == generated
