from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client.exposition import generate_latest
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send


@dataclass(slots=True)
class ObservabilityMetrics:
    api_requests: Counter
    api_request_duration: Histogram
    api_in_flight: Gauge
    db_operations: Counter
    db_operation_duration: Histogram
    db_pool_connections: Gauge
    runtime_state: Gauge
    _runtime_states: dict[str, str] = field(default_factory=dict, repr=False)
    _runtime_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_api_request(self, *, method: str, route: str, status: int, duration: float) -> None:
        labels = {"method": method.upper(), "route": route, "status": str(status)}
        self.api_requests.labels(**labels).inc()
        self.api_request_duration.labels(method=labels["method"], route=route).observe(max(0.0, duration))

    def record_db_operation(self, *, operation: str, status: str, duration: float) -> None:
        self.db_operations.labels(operation=operation, status=status).inc()
        self.db_operation_duration.labels(operation=operation).observe(max(0.0, duration))

    def set_db_pool_connections(self, *, state: str, value: float) -> None:
        self.db_pool_connections.labels(state=state).set(value)

    def set_runtime_state(self, *, component: str, state: str) -> None:
        with self._runtime_lock:
            previous = self._runtime_states.get(component)
            if previous is not None and previous != state:
                self.runtime_state.labels(component=component, state=previous).set(0)
            self.runtime_state.labels(component=component, state=state).set(1)
            self._runtime_states[component] = state


_CACHE_LOCK = threading.Lock()
_METRICS_CACHE: WeakKeyDictionary[CollectorRegistry, dict[str, ObservabilityMetrics]] = WeakKeyDictionary()


def get_metrics(
    *,
    registry: CollectorRegistry = REGISTRY,
    namespace: str = "etk",
) -> ObservabilityMetrics:
    """Return one metric bundle per registry/namespace without duplicate registration."""

    with _CACHE_LOCK:
        by_namespace = _METRICS_CACHE.setdefault(registry, {})
        existing = by_namespace.get(namespace)
        if existing is not None:
            return existing
        metrics = _build_metrics(registry=registry, namespace=namespace)
        by_namespace[namespace] = metrics
        return metrics


def metrics_response(*, registry: CollectorRegistry = REGISTRY) -> Response:
    return Response(generate_latest(registry), headers={"Content-Type": CONTENT_TYPE_LATEST})


class MetricsMiddleware:
    def __init__(self, app: ASGIApp, *, metrics: ObservabilityMetrics | None = None) -> None:
        self.app = app
        self.metrics = metrics or get_metrics()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = str(scope.get("method") or "UNKNOWN").upper()
        route = _route_label(scope)
        status = 500
        started = time.perf_counter()
        self.metrics.api_in_flight.labels(method=method, route=route).inc()

        async def send_with_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_with_status)
        finally:
            self.metrics.api_in_flight.labels(method=method, route=route).dec()
            self.metrics.record_api_request(
                method=method,
                route=_route_label(scope),
                status=status,
                duration=time.perf_counter() - started,
            )


def _build_metrics(*, registry: CollectorRegistry, namespace: str) -> ObservabilityMetrics:
    return ObservabilityMetrics(
        api_requests=Counter(
            "api_requests_total",
            "Total HTTP API requests.",
            ("method", "route", "status"),
            namespace=namespace,
            registry=registry,
        ),
        api_request_duration=Histogram(
            "api_request_duration_seconds",
            "HTTP API request duration in seconds.",
            ("method", "route"),
            namespace=namespace,
            registry=registry,
        ),
        api_in_flight=Gauge(
            "api_requests_in_flight",
            "HTTP API requests currently in flight.",
            ("method", "route"),
            namespace=namespace,
            registry=registry,
        ),
        db_operations=Counter(
            "db_operations_total",
            "Total database operations.",
            ("operation", "status"),
            namespace=namespace,
            registry=registry,
        ),
        db_operation_duration=Histogram(
            "db_operation_duration_seconds",
            "Database operation duration in seconds.",
            ("operation",),
            namespace=namespace,
            registry=registry,
        ),
        db_pool_connections=Gauge(
            "db_pool_connections",
            "Database pool connections by state.",
            ("state",),
            namespace=namespace,
            registry=registry,
        ),
        runtime_state=Gauge(
            "runtime_state",
            "Current process component state (1 for active state).",
            ("component", "state"),
            namespace=namespace,
            registry=registry,
        ),
    )


def _route_label(scope: Scope) -> str:
    route = scope.get("route")
    template = getattr(route, "path", None)
    return str(template or "unmatched")
