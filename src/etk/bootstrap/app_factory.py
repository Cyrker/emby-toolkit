from __future__ import annotations

import os
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from etk.compatibility.flask_app import (
    LazyCompatibilityApplication,
    create_flask_app,
)
from etk.compatibility.response_contract import fastapi_json_response
from etk.platform import AppContainer, LegacyConfigAdapter, PostgresLegacyConfigStore, Settings
from etk.platform.observability import MetricsMiddleware, TraceIdMiddleware, get_metrics, metrics_response

from .runtime_supervisor import RuntimeRole, RuntimeState, RuntimeSupervisor, get_runtime_supervisor

_LEGACY_DATABASE_ENVIRONMENT = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")


def create_app(
    *,
    start_runtime: bool = True,
    static_dir: Path | None = None,
    data_dir: Path | None = None,
    secret_key: bytes | None = None,
    supervisor: RuntimeSupervisor | None = None,
    container: AppContainer | None = None,
) -> FastAPI:
    """Build the ASGI application without importing or starting legacy runtime modules."""
    runtime = supervisor or get_runtime_supervisor()
    if container is None:
        app_container, config_adapter = _create_default_container(data_dir)
    else:
        app_container, config_adapter = container, None
    compatibility_app = LazyCompatibilityApplication(
        partial(
            create_flask_app,
            static_dir=static_dir,
            data_dir=data_dir,
            secret_key=secret_key,
            supervisor=runtime,
        )
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if start_runtime:
            runtime.start(role=RuntimeRole.API, background=True)
        try:
            if start_runtime:
                compatibility_app.materialize()
            yield
        finally:
            try:
                if start_runtime:
                    runtime.stop()
            finally:
                app_container.close()

    app = FastAPI(title="Emby Toolkit", lifespan=lifespan)
    app.state.runtime_supervisor = runtime
    app.state.container = app_container
    app.state.config_adapter = config_adapter
    app.state.settings = app_container.settings

    platform_metrics = get_metrics()
    app.add_middleware(MetricsMiddleware, metrics=platform_metrics)
    app.add_middleware(TraceIdMiddleware)

    @app.get("/api/health", include_in_schema=False)
    def health() -> Any:
        if start_runtime and runtime.state is not RuntimeState.RUNNING:
            return fastapi_json_response(
                {"status": "error", "message": f"runtime is {runtime.state.value}"},
                status_code=503,
            )
        return fastapi_json_response({"status": "ok"})

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> Any:
        return metrics_response()

    app.state.compatibility_app = compatibility_app

    # The compatibility mount must remain the final route so new FastAPI routes win.
    app.mount("/", compatibility_app, name="legacy-compatibility")
    return app


def _create_default_container(data_dir: Path | None) -> tuple[AppContainer, LegacyConfigAdapter]:
    configured_root = data_dir
    if configured_root is None:
        environment_root = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
        configured_root = Path(environment_root) if environment_root else None
    config_path = configured_root / "config.ini" if configured_root is not None else None

    bootstrap_adapter = LegacyConfigAdapter(config_path, environ=os.environ)
    bootstrap_settings = bootstrap_adapter.load()
    configured_url = os.environ.get("ETK_DATABASE_URL") or None
    if configured_url is None and _has_legacy_database_configuration(bootstrap_settings):
        configured_url = _database_url_from_settings(bootstrap_settings)
    if configured_url is None:
        container = AppContainer(bootstrap_settings, database_provider=_create_database_runtime)
        return container, bootstrap_adapter

    dynamic_store = PostgresLegacyConfigStore(configured_url)
    adapter = LegacyConfigAdapter(config_path, environ=os.environ, dynamic_store=dynamic_store)
    settings = adapter.load()
    container = AppContainer(settings, database_provider=_create_database_runtime)
    return container, adapter


def _has_legacy_database_configuration(settings: Settings) -> bool:
    environment_configured = any(os.environ.get(key) for key in _LEGACY_DATABASE_ENVIRONMENT)
    ini_configured = any(section.casefold() == "database" for section in settings.legacy_ini)
    return environment_configured or ini_configured


def _database_url_from_settings(settings: Settings) -> str:
    from sqlalchemy import URL

    return URL.create(
        "postgresql+psycopg",
        username=settings.db_user,
        password=settings.db_password.get_secret_value(),
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    ).render_as_string(hide_password=False)


def _create_database_runtime(container: AppContainer) -> Any:
    from etk.platform.db import DatabaseRuntime, EngineConfig, PsycopgPoolConfig

    settings: Settings = container.settings
    database_url = os.environ.get("ETK_DATABASE_URL") or _database_url_from_settings(settings)
    runtime = DatabaseRuntime.create(
        EngineConfig(database_url, application_name="etk-api"),
        raw_pool_config=PsycopgPoolConfig(database_url, application_name="etk-api-raw"),
    )
    try:
        runtime.start()
    except BaseException:
        runtime.shutdown()
        raise
    return runtime
