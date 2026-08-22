from __future__ import annotations

from prometheus_client import CollectorRegistry
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Route
from starlette.testclient import TestClient

from etk.platform.observability import MetricsMiddleware, get_metrics, metrics_response


def test_metric_bundle_is_cached_per_registry_and_records_platform_signals() -> None:
    registry = CollectorRegistry()
    first = get_metrics(registry=registry, namespace="unit")
    second = get_metrics(registry=registry, namespace="unit")

    assert first is second
    first.record_api_request(method="get", route="/health", status=200, duration=0.25)
    first.record_db_operation(operation="select", status="ok", duration=0.1)
    first.set_db_pool_connections(state="in_use", value=2)
    first.set_runtime_state(component="api", state="starting")
    first.set_runtime_state(component="api", state="running")

    assert (
        registry.get_sample_value(
            "unit_api_requests_total",
            {"method": "GET", "route": "/health", "status": "200"},
        )
        == 1
    )
    assert registry.get_sample_value("unit_db_operations_total", {"operation": "select", "status": "ok"}) == 1
    assert registry.get_sample_value("unit_db_pool_connections", {"state": "in_use"}) == 2
    assert registry.get_sample_value("unit_runtime_state", {"component": "api", "state": "starting"}) == 0
    assert registry.get_sample_value("unit_runtime_state", {"component": "api", "state": "running"}) == 1


def test_metrics_middleware_records_success_and_metrics_response() -> None:
    registry = CollectorRegistry()
    metrics = get_metrics(registry=registry, namespace="http_unit")

    async def endpoint(_request) -> Response:
        return Response(status_code=204)

    app = Starlette(routes=[Route("/anything", endpoint)])
    with TestClient(MetricsMiddleware(app, metrics=metrics)) as client:
        response = client.get("/anything")

    assert response.status_code == 204
    assert (
        registry.get_sample_value(
            "http_unit_api_requests_total",
            {"method": "GET", "route": "unmatched", "status": "204"},
        )
        == 1
    )
    exposition = metrics_response(registry=registry)
    assert b"http_unit_api_requests_total" in exposition.body
    assert exposition.headers["content-type"].startswith("text/plain")
