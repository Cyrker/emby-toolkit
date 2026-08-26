from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request

from etk.compatibility.flask_app import (
    LazyCompatibilityApplication,
    create_flask_app,
)
from etk.compatibility.intro_detection import (
    LazyPersistentIntroDetectionGateway,
    clear_persistent_intro_detection_gateway,
    install_persistent_intro_detection_gateway,
)
from etk.compatibility.persistent_runtime_state import PostgresRuntimeStateBackend
from etk.compatibility.playback import (
    NativePlaybackCompatibilityGateway,
    clear_persistent_playback_gateway,
    install_persistent_playback_gateway,
)
from etk.compatibility.response_contract import fastapi_json_response
from etk.compatibility.session_codec import LazyLegacySessionCodec
from etk.compatibility.system_defaults import LegacyStandaloneSettingsDefaults
from etk.domains.identity import (
    IdentityService,
    LegacyEmbyIdentityGateway,
    PersistentPostAuthorizationSubmitter,
    RuntimeConfigurationWriter,
    SqlAlchemyIdentityRepository,
)
from etk.domains.identity.router import router as identity_router
from etk.domains.ingestion import PostgresIngestionMonitorLedger
from etk.domains.media.actor_subscription_projection import SqlAlchemyActorSubscriptionProjection
from etk.domains.media.bridge_client import HttpxEmbyBridgeClient, HttpxPluginUpdateGateway
from etk.domains.media.cast_editor import MediaCastEditorService
from etk.domains.media.cast_editor_compatibility import LegacyMediaCastEditorGateway
from etk.domains.media.cast_editor_router import router as media_cast_editor_router
from etk.domains.media.compatibility import (
    LegacyFutureDomainEventGateway,
    LegacyMediaImageGateway,
    LegacyMediaMappingDefaults,
    LegacyMediaRuntimeGateway,
    LegacyP5MediaEventGateway,
)
from etk.domains.media.editing import MediaEditingService
from etk.domains.media.editing_effects import PersistentMediaInfoCommandGateway
from etk.domains.media.editing_router import router as media_editing_router
from etk.domains.media.event_effects import PersistentP5MediaEventGateway
from etk.domains.media.image_editing import MediaImageEditingService
from etk.domains.media.image_editing_compatibility import LegacyMediaImageEditingGateway
from etk.domains.media.image_editing_effects import PersistentMediaImageEditingGateway
from etk.domains.media.image_editing_router import router as media_image_editing_router
from etk.domains.media.image_proxy import MediaImageProxyService
from etk.domains.media.image_proxy_compatibility import LegacyMediaImageProxyGateway
from etk.domains.media.image_proxy_router import router as media_image_proxy_router
from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.domains.media.router import router as media_router
from etk.domains.media.service import MediaService
from etk.domains.media.watchlist_projection import SqlAlchemyWatchlistMediaProjectionRepository
from etk.domains.operations.database import DatabaseReadService, SqlAlchemyDatabaseReadRepository
from etk.domains.operations.database_router import router as database_read_router
from etk.domains.operations.job_router import router as job_router
from etk.domains.operations.jobs import (
    JobSubmissionService,
    LegacyProcessorReadiness,
    PersistentRuntimeStatusReader,
)
from etk.domains.operations.log_router import router as log_router
from etk.domains.operations.logs import LocalLogStorage, LogService
from etk.domains.operations.runtime import ManifestTaskCatalog
from etk.domains.operations.runtime_router import router as runtime_query_router
from etk.domains.operations.system_runtime_router import router as system_runtime_router
from etk.domains.p115 import (
    AtomicP115LocalStrmLibraryGateway,
    HttpxP115AuthorizationGateway,
    HttpxP115BridgeEmbyGateway,
    IntroDetectionSubmissionService,
    LegacyP7IntroRuntimeSource,
    LegacyP115CredentialStore,
    LegacyP115DeepDeleteScopeGateway,
    LegacyP115DirectLinkGateway,
    LegacyP115DirectoryGateway,
    LegacyP115DirectoryTreeGateway,
    LegacyP115IntroSnapshotExporter,
    LegacyP115IntroSyncFeature,
    LegacyP115OrganizeMutationGateway,
    LegacyP115PlanNamingGateway,
    LegacyP115PlaybackProvider,
    LegacyP115PlaybackSourceGateway,
    LegacyP115PlaybackUserGateway,
    LegacyP115RenamePreviewGateway,
    LegacyP115RuleRecognitionGateway,
    LegacyP115SortingRuleDirectoryGateway,
    LegacyP115WashingPriorityConfigGateway,
    NativeP115IntroDetectionGateway,
    P115AuthDirectoryService,
    P115DeepDeleteJobGateway,
    P115DeepDeleteService,
    P115EpisodeRegexService,
    P115IntroBackfillTaskService,
    P115IntroSyncService,
    P115ItemReadyService,
    P115MediaInfoCacheService,
    P115OrganizeExecutionService,
    P115OrganizePlanService,
    P115OrganizeRecordsService,
    P115PlaybackConfigService,
    P115PlaybackQrService,
    P115PlaybackService,
    P115RenameConfigService,
    P115SortingRulesService,
    P115StrmReplaceService,
    PostgresP115IntroDetectionRepository,
    PostgresP115PlaybackRepository,
    PostgresP115StrmSyncCoordinator,
    SqlAlchemyP115DeepDeleteSnapshotRepository,
    SqlAlchemyP115EpisodeRegexRepository,
    SqlAlchemyP115MediaInfoRepository,
    SqlAlchemyP115OrganizeExecutionRepository,
    SqlAlchemyP115OrganizePlanRepository,
    SqlAlchemyP115OrganizeRecordsRepository,
    SqlAlchemyP115RenameConfigRepository,
    SqlAlchemyP115SortingRulesRepository,
)
from etk.domains.p115.playback_router import router as p115_playback_router
from etk.domains.p115.router import router as p115_router
from etk.domains.p115.upload_monitor import P115UploadMonitorConfigService
from etk.domains.p115.upload_repository import PostgresP115UploadMonitorRepository
from etk.domains.subscription.actor import ActorSubscriptionService, SqlAlchemyActorSubscriptionRepository
from etk.domains.subscription.actor_operations import ActorCancellationOperationStager
from etk.domains.subscription.actor_router import router as actor_subscription_router
from etk.domains.subscription.adapters import (
    HttpxEmbyItemDeletionGateway,
    NativeActorRuntimeGateway,
    NativeActorSearchGateway,
    NativeWatchlistExternalDeleteGateway,
    PersistentActorRefreshGateway,
    PersistentWatchlistRefreshGateway,
)
from etk.domains.subscription.moviepilot_configured import ConfiguredMoviePilotGateway
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.watchlist import WatchlistService
from etk.domains.subscription.watchlist_router import router as watchlist_router
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
from etk.platform.jobs import PostgresExecutionGuard, PostgresJobStore
from etk.platform.observability import MetricsMiddleware, TraceIdMiddleware, get_metrics, metrics_response
from etk.platform.tasks import TaskRegistry
from etk.platform.web import CookieSessionMiddleware
from etk.processes.p6_ingestion import LegacyIngestionBindingDelivery, PersistentPendingBindingGateway
from etk.processes.p6_playback import P115PlaybackOperationExecutor

from .runtime_supervisor import RuntimeRole, RuntimeState, RuntimeSupervisor, get_runtime_supervisor

_LEGACY_DATABASE_ENVIRONMENT = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
logger = logging.getLogger(__name__)


def create_app(
    *,
    start_runtime: bool = True,
    static_dir: Path | None = None,
    data_dir: Path | None = None,
    secret_key: bytes | None = None,
    supervisor: RuntimeSupervisor | None = None,
    container: AppContainer | None = None,
    runtime_config_synchronizer: Any = None,
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
        _register_job_providers(app_container)
        _register_operation_query_providers(app_container)
        _register_log_providers(app_container, data_dir)
        _register_database_read_providers(app_container)
        _register_system_providers(app_container, runtime)
        _register_p115_providers(app_container)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        persistent_gateway = None
        background_gateway = None
        playback_gateway = None
        intro_detection_gateway = None
        try:
            if start_runtime:
                from etk.compatibility.background import (
                    PersistentBackgroundGateway,
                    install_persistent_background_gateway,
                )
                from etk.compatibility.persistent_tasks import LazyPersistentTaskManagerGateway
                from task_manager import install_persistent_submission_gateway

                persistent_gateway = LazyPersistentTaskManagerGateway(lambda: app_container.use_case("operations.jobs"))
                background_gateway = PersistentBackgroundGateway(lambda: app_container.use_case("operations.jobs"))
                playback_gateway = NativePlaybackCompatibilityGateway(lambda: app_container.use_case("p115.playback"))
                intro_detection_gateway = LazyPersistentIntroDetectionGateway(
                    lambda: app_container.use_case("p115.intro-detection-gateway")
                )
                install_persistent_submission_gateway(persistent_gateway)
                install_persistent_background_gateway(background_gateway)
                install_persistent_playback_gateway(playback_gateway)
                install_persistent_intro_detection_gateway(intro_detection_gateway)
                runtime.start(role=RuntimeRole.API, background=False)
            yield
        finally:
            try:
                try:
                    if background_gateway is not None:
                        from etk.compatibility.background import clear_persistent_background_gateway

                        clear_persistent_background_gateway(background_gateway)
                    if playback_gateway is not None:
                        clear_persistent_playback_gateway(playback_gateway)
                    if intro_detection_gateway is not None:
                        clear_persistent_intro_detection_gateway(intro_detection_gateway)
                    if persistent_gateway is not None:
                        from task_manager import clear_persistent_submission_gateway

                        clear_persistent_submission_gateway(persistent_gateway)
                finally:
                    if start_runtime:
                        runtime.stop()
            finally:
                app_container.close()

    app = FastAPI(title="Emby Toolkit", lifespan=lifespan)
    configuration_synchronizer = runtime_config_synchronizer
    if configuration_synchronizer is None and start_runtime and owns_container:
        from .config_sync import ApiRuntimeConfigSynchronizer, SqlAlchemyDynamicConfigReader

        snapshot_reader = SqlAlchemyDynamicConfigReader(
            lambda: app_container.database.unit_of_work(),
        )

        def consume_settings(settings: Settings) -> None:
            app_container.settings = settings
            app.state.settings = settings

        configuration_synchronizer = ApiRuntimeConfigSynchronizer(
            snapshot_reader=snapshot_reader.read,
            settings_provider=lambda: app_container.settings,
            settings_consumer=consume_settings,
            runtime=runtime,
        )

    compatibility_app = LazyCompatibilityApplication(
        partial(
            create_flask_app,
            static_dir=static_dir,
            data_dir=data_dir,
            secret_key=secret_key,
            supervisor=runtime,
            configuration_synchronizer=configuration_synchronizer,
        )
    )
    if owns_container:
        _register_media_providers(app_container, compatibility_app)
        _register_subscription_providers(app_container)
    app.state.runtime_supervisor = runtime
    app.state.container = app_container
    app.state.config_adapter = config_adapter
    app.state.settings = app_container.settings
    app.state.configuration_synchronizer = configuration_synchronizer

    @app.middleware("http")
    async def synchronize_runtime_configuration(request: Request, call_next: Any) -> Any:
        if (
            configuration_synchronizer is not None
            and request.url.path.startswith("/api/")
            and request.url.path != "/api/health"
        ):
            try:
                await run_in_threadpool(configuration_synchronizer.synchronize)
            except Exception:
                logger.exception("API runtime configuration synchronization failed")
                return fastapi_json_response(
                    {"status": "error", "message": "运行配置同步失败，请稍后重试"},
                    status_code=503,
                )
        return await call_next(request)

    platform_metrics = get_metrics()
    app.add_middleware(MetricsMiddleware, metrics=platform_metrics)
    app.add_middleware(TraceIdMiddleware)
    session_codec = LazyLegacySessionCodec(_resolve_data_dir(data_dir), secret_key=secret_key)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: session_codec)

    app.include_router(identity_router)
    app.include_router(runtime_query_router)
    app.include_router(job_router)
    app.include_router(system_runtime_router)
    app.include_router(log_router)
    app.include_router(database_read_router)
    app.include_router(configuration_router)
    app.include_router(standalone_settings_router)
    app.include_router(system_integration_router)
    app.include_router(media_router)
    app.include_router(media_editing_router)
    app.include_router(media_image_editing_router)
    app.include_router(media_cast_editor_router)
    app.include_router(media_image_proxy_router)
    app.include_router(p115_router)
    app.include_router(p115_playback_router)
    app.include_router(actor_subscription_router)
    app.include_router(watchlist_router)

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
            post_authorization=PersistentPostAuthorizationSubmitter(lambda: current.use_case("operations.jobs")),
        ),
    )


def _register_operation_query_providers(container: AppContainer) -> None:
    task_manifest = Path(__file__).resolve().parents[3] / "verification" / "contracts" / "task-ownership.yaml"
    container.register_use_case("task-catalog", lambda _: ManifestTaskCatalog(task_manifest))


def _register_job_providers(container: AppContainer) -> None:
    task_fixture = (
        Path(__file__).resolve().parents[3] / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
    )
    container.register_repository(
        "operations.jobs",
        lambda current: PostgresJobStore(
            lambda: current.database.unit_of_work(),
            execution_guard=PostgresExecutionGuard(current.database.raw_pool),
        ),
    )
    container.register_use_case(
        "operations.jobs",
        lambda current: JobSubmissionService(
            current.repository("operations.jobs"),
            TaskRegistry.from_fixture(task_fixture),
        ),
    )
    container.register_use_case(
        "runtime-status",
        lambda current: PersistentRuntimeStatusReader(
            current.use_case("operations.jobs"),
        ),
    )
    container.register_client("operations.job-readiness", lambda _: LegacyProcessorReadiness())


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


def _register_media_providers(
    container: AppContainer,
    compatibility_app: LazyCompatibilityApplication,
) -> None:
    container.register_repository(
        "media",
        lambda current: SqlAlchemyMediaRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "media-images",
        lambda current: LegacyMediaImageGateway(lambda: current.settings),
    )
    container.register_client(
        "media-p5-events-legacy",
        lambda _: LegacyP5MediaEventGateway(compatibility_app),
    )
    container.register_client(
        "media-p5-events",
        lambda current: PersistentP5MediaEventGateway(
            lambda: current.database.unit_of_work(),
            current.client("media-p5-events-legacy"),
            lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
        ),
    )
    container.register_client(
        "media-future-events",
        lambda _: LegacyFutureDomainEventGateway(compatibility_app),
    )
    container.register_client(
        "media-mapping-defaults",
        lambda _: LegacyMediaMappingDefaults(),
    )
    container.register_client(
        "media-runtime",
        lambda _: LegacyMediaRuntimeGateway(),
    )
    container.register_client(
        "media-image-editing-legacy",
        lambda _: LegacyMediaImageEditingGateway(),
    )
    container.register_client(
        "media-image-editing",
        lambda current: PersistentMediaImageEditingGateway(
            lambda: current.database.unit_of_work(),
            current.client("media-image-editing-legacy"),
            lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
        ),
    )
    container.register_client(
        "media-cast-editor",
        lambda _: LegacyMediaCastEditorGateway(),
    )
    container.register_client(
        "media-image-proxy",
        lambda _: LegacyMediaImageProxyGateway(),
    )
    container.register_client(
        "media-plugin-updates",
        lambda current: HttpxPluginUpdateGateway(lambda: current.settings),
    )
    container.register_client(
        "media-emby-bridge",
        lambda current: HttpxEmbyBridgeClient(lambda: current.settings),
    )
    container.register_use_case(
        "media",
        lambda current: MediaService(
            repository=current.repository("media"),
            settings=lambda: current.settings,
            jobs=current.use_case("operations.jobs"),
            images=current.client("media-images"),
            p5_events=current.client("media-p5-events"),
            future_events=current.client("media-future-events"),
            defaults=current.client("media-mapping-defaults"),
        ),
    )
    container.register_use_case(
        "media.editing",
        lambda current: MediaEditingService(
            current.repository("media"),
            processor_ready=current.client("media-runtime").processor_ready,
            commands=PersistentMediaInfoCommandGateway(
                lambda: current.database.unit_of_work(),
                current.client("media-emby-bridge"),
                lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
            ),
        ),
    )
    container.register_use_case(
        "media.cast-editor",
        lambda current: MediaCastEditorService(current.client("media-cast-editor")),
    )
    container.register_use_case(
        "media.image-proxy",
        lambda current: MediaImageProxyService(current.client("media-image-proxy")),
    )
    container.register_use_case(
        "media.image-editing",
        lambda current: MediaImageEditingService(
            current.client("media-image-editing"),
        ),
    )


def _register_p115_providers(container: AppContainer) -> None:
    container.register_repository(
        "ingestion.monitor",
        lambda current: PostgresIngestionMonitorLedger(current.database.unit_of_work),
    )
    container.register_repository(
        "p115.upload-monitor",
        lambda current: PostgresP115UploadMonitorRepository(current.database.unit_of_work),
    )
    container.register_repository(
        "p115.playback",
        lambda current: PostgresP115PlaybackRepository(current.database.unit_of_work),
    )
    container.register_client(
        "ingestion.pending-bindings",
        lambda current: PersistentPendingBindingGateway(
            ledger=current.repository("ingestion.monitor"),
            delivery=LegacyIngestionBindingDelivery(
                jobs=lambda: current.use_case("operations.jobs"),
                uow_factory=current.database.unit_of_work,
                bridge=current.client("media-emby-bridge"),
                lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
            ),
            lease_seconds=max(float(current.settings.emby_api_timeout) + 30, 90),
        ),
    )
    container.register_repository(
        "p115.credentials",
        lambda _: LegacyP115CredentialStore(),
    )
    container.register_client(
        "p115.authorization",
        lambda _: HttpxP115AuthorizationGateway(),
    )
    container.register_client(
        "p115.playback-users",
        lambda _: LegacyP115PlaybackUserGateway(),
    )
    container.register_client(
        "p115.playback-sources",
        lambda _: LegacyP115PlaybackSourceGateway(),
    )
    container.register_client(
        "p115.directories",
        lambda _: LegacyP115DirectoryGateway(),
    )
    container.register_client(
        "p115.directory-tree",
        lambda _: LegacyP115DirectoryTreeGateway(),
    )
    container.register_repository(
        "p115.sorting-rules",
        lambda current: SqlAlchemyP115SortingRulesRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "p115.sorting-rule-directories",
        lambda _: LegacyP115SortingRuleDirectoryGateway(),
    )
    container.register_repository(
        "p115.rename-config",
        lambda current: SqlAlchemyP115RenameConfigRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.episode-regex",
        lambda current: SqlAlchemyP115EpisodeRegexRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.organize-plans",
        lambda current: SqlAlchemyP115OrganizePlanRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.organize-executions",
        lambda current: SqlAlchemyP115OrganizeExecutionRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.organize-records",
        lambda current: SqlAlchemyP115OrganizeRecordsRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.media-info",
        lambda current: SqlAlchemyP115MediaInfoRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.intro-detection",
        lambda current: PostgresP115IntroDetectionRepository(current.database.unit_of_work),
    )
    container.register_repository(
        "p115.deep-delete-snapshots",
        lambda current: SqlAlchemyP115DeepDeleteSnapshotRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "p115.bridge-emby",
        lambda current: HttpxP115BridgeEmbyGateway(lambda: current.settings),
    )
    container.register_client(
        "p115.intro-p7-reads",
        lambda current: LegacyP7IntroRuntimeSource(current.repository("p115.intro-detection")),
    )
    container.register_client(
        "p115.intro-sync-feature",
        lambda _: LegacyP115IntroSyncFeature(),
    )
    container.register_client(
        "p115.intro-snapshot-exporter",
        lambda _: LegacyP115IntroSnapshotExporter(),
    )
    container.register_client(
        "p115.deep-delete-scope",
        lambda _: LegacyP115DeepDeleteScopeGateway(),
    )
    container.register_client(
        "p115.deep-delete-jobs",
        lambda current: P115DeepDeleteJobGateway(lambda: current.use_case("operations.jobs")),
    )
    container.register_client(
        "p115.rename-preview",
        lambda _: LegacyP115RenamePreviewGateway(),
    )
    container.register_client(
        "p115.washing-priority-config",
        lambda _: LegacyP115WashingPriorityConfigGateway(),
    )
    container.register_client(
        "p115.plan-recognition",
        lambda _: LegacyP115RuleRecognitionGateway(),
    )
    container.register_client(
        "p115.plan-naming",
        lambda _: LegacyP115PlanNamingGateway(),
    )
    container.register_client(
        "p115.organize-mutations",
        lambda _: LegacyP115OrganizeMutationGateway(),
    )
    container.register_client(
        "p115.local-strm-library",
        lambda _: AtomicP115LocalStrmLibraryGateway(),
    )
    container.register_client(
        "p115.strm-sync-coordinator",
        lambda current: PostgresP115StrmSyncCoordinator(current.database.engine),
    )
    container.register_use_case(
        "p115.auth-directory",
        lambda current: P115AuthDirectoryService(
            settings=lambda: current.settings,
            authorization=current.client("p115.authorization"),
            credentials=current.repository("p115.credentials"),
            directories=current.client("p115.directories"),
        ),
    )
    container.register_use_case(
        "p115.upload-monitor-config",
        lambda current: P115UploadMonitorConfigService(
            current.repository("p115.upload-monitor"),
            lambda: current.settings.monitor_extensions,
        ),
    )
    container.register_use_case(
        "p115.playback-config",
        lambda current: P115PlaybackConfigService(
            PostgresRuntimeStateBackend(current.database.raw_pool.connection),
            current.client("p115.playback-users"),
        ),
    )
    container.register_client(
        "p115.playback-provider",
        lambda current: LegacyP115PlaybackProvider(current.use_case("p115.playback-config")),
    )
    container.register_client(
        "p115.playback-direct-links",
        lambda current: LegacyP115DirectLinkGateway(current.use_case("p115.playback-config")),
    )
    container.register_use_case(
        "p115.playback-executor",
        lambda current: P115PlaybackOperationExecutor(
            repository=current.repository("p115.playback"),
            provider=current.client("p115.playback-provider"),
            config=current.use_case("p115.playback-config"),
            worker_id=f"api:{os.getpid()}:p115-playback",
        ),
    )
    container.register_use_case(
        "p115.playback",
        lambda current: P115PlaybackService(
            config=current.use_case("p115.playback-config"),
            repository=current.repository("p115.playback"),
            sources=current.client("p115.playback-sources"),
            direct_links=current.client("p115.playback-direct-links"),
            executor=current.use_case("p115.playback-executor"),
        ),
    )
    container.register_use_case(
        "p115.playback-qr",
        lambda current: P115PlaybackQrService(
            current.repository("p115.playback"),
            current.client("p115.authorization"),
        ),
    )
    container.register_use_case(
        "p115.sorting-rules",
        lambda current: P115SortingRulesService(
            settings=lambda: current.settings,
            credentials=current.repository("p115.credentials"),
            repository=current.repository("p115.sorting-rules"),
            directories=current.client("p115.sorting-rule-directories"),
        ),
    )
    container.register_use_case(
        "p115.rename-config",
        lambda current: P115RenameConfigService(
            repository=current.repository("p115.rename-config"),
            preview=current.client("p115.rename-preview"),
            washing_priority=current.client("p115.washing-priority-config"),
        ),
    )
    container.register_use_case(
        "p115.episode-regex",
        lambda current: P115EpisodeRegexService(
            current.repository("p115.episode-regex"),
        ),
    )
    container.register_use_case(
        "p115.organize-plans",
        lambda current: P115OrganizePlanService(
            settings=lambda: current.settings,
            credentials=current.repository("p115.credentials"),
            directories=current.client("p115.directory-tree"),
            sorting_rules=current.repository("p115.sorting-rules"),
            rename_config=current.repository("p115.rename-config"),
            episode_regex=current.repository("p115.episode-regex"),
            repository=current.repository("p115.organize-plans"),
            recognition=current.client("p115.plan-recognition"),
            naming=current.client("p115.plan-naming"),
        ),
    )
    container.register_use_case(
        "p115.organize-executions",
        lambda current: P115OrganizeExecutionService(
            plans=current.repository("p115.organize-plans"),
            repository=current.repository("p115.organize-executions"),
            mutations=current.client("p115.organize-mutations"),
        ),
    )
    container.register_use_case(
        "p115.organize-records",
        lambda current: P115OrganizeRecordsService(
            current.repository("p115.organize-records"),
        ),
    )
    container.register_use_case(
        "p115.strm-replace",
        lambda current: P115StrmReplaceService(
            settings=lambda: current.settings,
            libraries=current.client("p115.local-strm-library"),
            coordinator=current.client("p115.strm-sync-coordinator"),
        ),
    )
    container.register_use_case(
        "p115.media-info",
        lambda current: P115MediaInfoCacheService(
            current.repository("p115.media-info"),
            current.repository("p115.credentials"),
        ),
    )
    container.register_use_case(
        "p115.intro-detection-submission",
        lambda current: IntroDetectionSubmissionService(current.repository("p115.intro-detection")),
    )
    container.register_use_case(
        "p115.intro-detection-gateway",
        lambda current: NativeP115IntroDetectionGateway(
            submission=current.use_case("p115.intro-detection-submission"),
            source=current.client("p115.intro-p7-reads"),
            account_id=lambda: current.repository("p115.credentials").load().account_id,
        ),
    )
    container.register_use_case(
        "p115.intro-backfill",
        lambda current: P115IntroBackfillTaskService(current.use_case("p115.intro-detection-gateway")),
    )
    container.register_use_case(
        "p115.intro-sync",
        lambda current: P115IntroSyncService(
            repository=current.repository("p115.media-info"),
            feature=current.client("p115.intro-sync-feature"),
            verifier=current.client("p115.bridge-emby"),
            exporter=current.client("p115.intro-snapshot-exporter"),
            account_id=lambda: current.repository("p115.credentials").load().account_id,
        ),
    )
    container.register_use_case(
        "p115.deep-delete",
        lambda current: P115DeepDeleteService(
            identities=current.use_case("p115.media-info"),
            verifier=current.client("p115.bridge-emby"),
            emby=current.client("p115.bridge-emby"),
            scope=current.client("p115.deep-delete-scope"),
            snapshots=current.repository("p115.deep-delete-snapshots"),
            submissions=current.client("p115.deep-delete-jobs"),
        ),
    )
    container.register_use_case(
        "p115.item-ready",
        lambda current: P115ItemReadyService(
            identities=current.use_case("p115.media-info"),
            emby=current.client("p115.bridge-emby"),
            pending_bindings=current.client("ingestion.pending-bindings"),
            media=current.use_case("media"),
        ),
    )


def _register_subscription_providers(container: AppContainer) -> None:
    container.register_repository(
        "subscription.operations",
        lambda current: PostgresSubscriptionOperationRepository(current.database.unit_of_work),
    )
    container.register_repository(
        "media.actor-subscription-projection",
        lambda _: SqlAlchemyActorSubscriptionProjection(),
    )
    container.register_repository(
        "subscription.actors",
        lambda current: SqlAlchemyActorSubscriptionRepository(
            current.database.unit_of_work,
            current.repository("media.actor-subscription-projection"),
            ActorCancellationOperationStager(current.repository("subscription.operations")),
        ),
    )
    container.register_repository(
        "media.watchlist-projection",
        lambda current: SqlAlchemyWatchlistMediaProjectionRepository(current.database.unit_of_work),
    )
    container.register_client(
        "subscription.actor-search",
        lambda current: NativeActorSearchGateway(lambda: current.settings),
    )
    container.register_client(
        "subscription.actor-runtime",
        lambda current: NativeActorRuntimeGateway(lambda: current.settings),
    )
    container.register_client(
        "subscription.actor-refresh",
        lambda current: PersistentActorRefreshGateway(lambda: current.use_case("operations.jobs")),
    )
    container.register_client(
        "subscription.moviepilot",
        lambda current: ConfiguredMoviePilotGateway(current.repository("app-settings")),
    )
    container.register_client(
        "subscription.emby-delete",
        lambda current: HttpxEmbyItemDeletionGateway(lambda: current.settings),
    )
    container.register_client(
        "subscription.watchlist-external-delete",
        lambda current: NativeWatchlistExternalDeleteGateway(
            current.repository("app-settings"),
            current.client("subscription.moviepilot"),
            current.client("subscription.emby-delete"),
        ),
    )
    container.register_client(
        "subscription.watchlist-refresh",
        lambda current: PersistentWatchlistRefreshGateway(lambda: current.use_case("operations.jobs")),
    )
    container.register_use_case(
        "subscription.actors",
        lambda current: ActorSubscriptionService(
            repository=current.repository("subscription.actors"),
            default_settings=current.repository("app-settings"),
            search=current.client("subscription.actor-search"),
            refresh_intents=current.client("subscription.actor-refresh"),
            runtime=current.client("subscription.actor-runtime"),
        ),
    )
    container.register_use_case(
        "subscription.watchlist",
        lambda current: WatchlistService(
            repository=current.repository("media.watchlist-projection"),
            settings=current.repository("app-settings"),
            external_delete=current.client("subscription.watchlist-external-delete"),
            refresh=current.client("subscription.watchlist-refresh"),
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
        EngineConfig(database_url, application_name=f"etk-{os.environ.get('ETK_PROCESS_ROLE', 'api')}"),
        raw_pool_config=PsycopgPoolConfig(
            database_url,
            application_name=f"etk-{os.environ.get('ETK_PROCESS_ROLE', 'api')}-raw",
        ),
    )
    try:
        runtime.start()
    except BaseException:
        runtime.shutdown()
        raise
    return runtime
