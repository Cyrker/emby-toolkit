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
from etk.compatibility.session_codec import LazyLegacySessionCodec
from etk.compatibility.system_defaults import LegacyStandaloneSettingsDefaults
from etk.domains.identity import (
    IdentityService,
    LegacyEmbyIdentityGateway,
    RuntimeConfigurationWriter,
    SqlAlchemyIdentityRepository,
)
from etk.domains.identity.router import router as identity_router
from etk.domains.operations.database import DatabaseReadService, SqlAlchemyDatabaseReadRepository
from etk.domains.operations.database_router import router as database_read_router
from etk.domains.operations.log_router import router as log_router
from etk.domains.operations.logs import LocalLogStorage, LogService
from etk.domains.operations.runtime import LegacyRuntimeStatusReader, ManifestTaskCatalog
from etk.domains.operations.runtime_router import router as runtime_query_router
from etk.domains.system.compatibility import LegacyConfigurationGateway
from etk.domains.system.configuration import ConfigurationService
from etk.domains.system.configuration_router import router as configuration_router
from etk.domains.system.integration_compatibility import HttpxSystemIntegrationGateway
from etk.domains.system.integration_router import router as system_integration_router
from etk.domains.system.integrations import SystemIntegrationService
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.domains.system.standalone import StandaloneSettingsService
from etk.domains.system.standalone_router import router as standalone_settings_router
from etk.platform import AppContainer, LegacyConfigAdapter, PostgresLegacyConfigStore, Settings
from etk.platform.observability import MetricsMiddleware, TraceIdMiddleware, get_metrics, metrics_response
from etk.platform.web import CookieSessionMiddleware

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
    owns_container = container is None
    if owns_container:
        app_container, config_adapter = _create_default_container(data_dir)
    else:
        assert container is not None
        app_container, config_adapter = container, None
    if owns_container:
        _register_identity_providers(app_container, runtime)
        _register_operation_query_providers(app_container)
        _register_log_providers(app_container, data_dir)
        _register_database_read_providers(app_container)
        _register_system_providers(app_container, runtime)
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
    session_codec = LazyLegacySessionCodec(_resolve_data_dir(data_dir), secret_key=secret_key)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: session_codec)

    app.include_router(identity_router)
    app.include_router(runtime_query_router)
    app.include_router(log_router)
    app.include_router(database_read_router)
    app.include_router(configuration_router)
    app.include_router(standalone_settings_router)
    app.include_router(system_integration_router)

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


def _register_identity_providers(
    container: AppContainer,
    supervisor: RuntimeSupervisor,
) -> None:
    container.register_repository(
        "identity",
        lambda current: SqlAlchemyIdentityRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client("identity", lambda _: LegacyEmbyIdentityGateway())
    container.register_use_case(
        "identity",
        lambda current: IdentityService(
            settings=lambda: current.settings,
            repository=current.repository("identity"),
            gateway=current.client("identity"),
            configuration=RuntimeConfigurationWriter(current, supervisor),
        ),
    )


def _register_operation_query_providers(container: AppContainer) -> None:
    task_manifest = Path(__file__).resolve().parents[3] / "verification" / "contracts" / "task-ownership.yaml"
    container.register_use_case("runtime-status", lambda _: LegacyRuntimeStatusReader())
    container.register_use_case("task-catalog", lambda _: ManifestTaskCatalog(task_manifest))


def _register_log_providers(container: AppContainer, data_dir: Path | None) -> None:
    log_directory = _resolve_data_dir(data_dir) / "logs"
    container.register_use_case(
        "operations.logs",
        lambda _: LogService(LocalLogStorage(log_directory)),
    )


def _register_database_read_providers(container: AppContainer) -> None:
    container.register_repository(
        "operations.database.read",
        lambda current: SqlAlchemyDatabaseReadRepository(
            lambda: current.database.unit_of_work(),
            timezone="Asia/Shanghai",
        ),
    )
    container.register_use_case(
        "operations.database.read",
        lambda current: DatabaseReadService(
            current.repository("operations.database.read"),
        ),
    )


def _register_system_providers(
    container: AppContainer,
    supervisor: RuntimeSupervisor,
) -> None:
    container.register_repository(
        "app-settings",
        lambda current: SqlAlchemyAppSettingsRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client("system-configuration", lambda _: LegacyConfigurationGateway())
    container.register_use_case(
        "system-configuration",
        lambda current: ConfigurationService(
            settings=lambda: current.settings,
            repository=current.repository("app-settings"),
            writer=RuntimeConfigurationWriter(current, supervisor),
            gateway=current.client("system-configuration"),
        ),
    )
    defaults = LegacyStandaloneSettingsDefaults()
    container.register_use_case(
        "system-standalone-settings",
        lambda current: StandaloneSettingsService(
            current.repository("app-settings"),
            defaults,
        ),
    )
    container.register_client(
        "system-integrations",
        lambda current: HttpxSystemIntegrationGateway(lambda: current.settings),
    )
    container.register_use_case(
        "system-integrations",
        lambda current: SystemIntegrationService(
            settings=lambda: current.settings,
            repository=current.repository("app-settings"),
            gateway=current.client("system-integrations"),
        ),
    )


def _resolve_data_dir(data_dir: Path | None) -> Path:
    if data_dir is not None:
        return data_dir.resolve()
    configured = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[3] / "local_data"


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
