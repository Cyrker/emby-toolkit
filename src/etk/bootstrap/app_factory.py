from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from starlette.responses import FileResponse, Response
from starlette.staticfiles import StaticFiles

from etk.compatibility.persistent_runtime_state import (
    PersistentRuntimeStateStore,
    PostgresRuntimeStateBackend,
)
from etk.compatibility.response_contract import fastapi_json_response
from etk.domains.collection import (
    CollectionActivationService,
    CollectionService,
    P8LocalCollectionGateway,
    PillowCoverPreviewRenderer,
    PostgresCollectionActivationRepository,
    SqlAlchemyCollectionRepository,
)
from etk.domains.collection.effect_adapters import (
    HttpxCollectionRemoteEffectsAdapter,
    PersistentCollectionDeletionSuppression,
)
from etk.domains.collection.effects import PostgresCollectionEffectGateway
from etk.domains.collection.operation_execution import CollectionOperationReconciler
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.query_adapters import HttpxCollectionQueryAdapter
from etk.domains.collection.router import effects_router as collection_effects_router
from etk.domains.collection.router import router as collection_router
from etk.domains.identity import (
    HttpxEmbyIdentityGateway,
    IdentityService,
    PersistentPostAuthorizationSubmitter,
    RuntimeConfigurationWriter,
    SqlAlchemyCollectionUserProjection,
    SqlAlchemyIdentityRepository,
)
from etk.domains.identity.effect_adapters import (
    HttpxEmbyIdentityEffectsAdapter,
    HttpxTelegramBotInfoGateway,
    InProcessRegistrationSecretPort,
)
from etk.domains.identity.effect_execution import (
    IdentityUserMutationCoordinator,
    RegistrationSagaCoordinator,
)
from etk.domains.identity.operation_repository import PostgresIdentityOperationRepository
from etk.domains.identity.playback import PlaybackReportingService
from etk.domains.identity.playback_adapter import HttpxPlaybackReportingAdapter
from etk.domains.identity.router import router as identity_router
from etk.domains.identity.user_activity import P8UserEventGateway, SqlAlchemyUserActivityRepository
from etk.domains.identity.user_activity_adapters import (
    HttpxEmbyUserActivityAdapter,
    PersistentPolicyPropagationGuard,
)
from etk.domains.identity.users import UsersService
from etk.domains.identity.users_effects import IdentityEffectsService
from etk.domains.identity.users_repository import SqlAlchemyUsersRepository
from etk.domains.identity.users_router import effects_router as users_effects_router
from etk.domains.identity.users_router import router as users_router
from etk.domains.ingestion import PostgresIngestionMonitorLedger
from etk.domains.media.actor_subscription_projection import SqlAlchemyActorSubscriptionProjection
from etk.domains.media.bridge_client import HttpxEmbyBridgeClient, HttpxPluginUpdateGateway
from etk.domains.media.cast_editor import MediaCastEditorService
from etk.domains.media.cast_editor_adapters import (
    HttpxMediaCastEditorGateway,
    SqlAlchemyCastEditorRepository,
)
from etk.domains.media.cast_editor_router import router as media_cast_editor_router
from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.media.collection_projection import SqlAlchemyCollectionMediaProjection
from etk.domains.media.daily_theme import DailyThemeService
from etk.domains.media.daily_theme_repository import SqlAlchemyDailyThemeRepository
from etk.domains.media.discover import DiscoverService
from etk.domains.media.discover_adapters import HttpxDiscoverProviderAdapter
from etk.domains.media.discover_defaults import StaticDiscoverDefaults
from etk.domains.media.discover_effects import DiscoverEffectsService
from etk.domains.media.discover_projection import SqlAlchemyDiscoverProjection
from etk.domains.media.discover_router import effects_router as discover_effects_router
from etk.domains.media.discover_router import router as discover_router
from etk.domains.media.editing import MediaEditingService
from etk.domains.media.editing_effects import PersistentMediaInfoCommandGateway
from etk.domains.media.editing_router import router as media_editing_router
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.media.event_adapters import HttpxP5MediaEventProvider, NativeP5MediaEventGateway
from etk.domains.media.event_effects import PersistentP5MediaEventGateway
from etk.domains.media.future_events import (
    P8CollectionRemovalEventGateway,
    P8FutureDomainEventGateway,
)
from etk.domains.media.image_adapters import HttpxMediaImageGateway, StaticMediaMappingDefaults
from etk.domains.media.image_editing import MediaImageEditingService
from etk.domains.media.image_editing_adapters import HttpxMediaImageEditingGateway
from etk.domains.media.image_editing_effects import PersistentMediaImageEditingGateway
from etk.domains.media.image_editing_router import router as media_image_editing_router
from etk.domains.media.image_proxy import MediaImageProxyService
from etk.domains.media.image_proxy_adapters import HttpxMediaImageProxyGateway
from etk.domains.media.image_proxy_router import router as media_image_proxy_router
from etk.domains.media.image_store import SqlAlchemyMediaImageStore
from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.domains.media.reprocess_identification import (
    ReprocessIdentificationService,
    SqlAlchemyIdentificationRepository,
)
from etk.domains.media.resubscribe_projection import SqlAlchemyResubscribeMediaProjection
from etk.domains.media.router import router as media_router
from etk.domains.media.search_index import EmbySearchIndexService, SqlAlchemyEmbySearchRepository
from etk.domains.media.secondary import SecondaryMediaService
from etk.domains.media.secondary_effect_adapters import HttpxSecondaryEmbyAdapter
from etk.domains.media.secondary_effects import SecondaryEffectsService
from etk.domains.media.secondary_router import effects_router as secondary_media_effects_router
from etk.domains.media.secondary_router import router as secondary_media_router
from etk.domains.media.service import MediaService
from etk.domains.media.subscription_projection import SqlAlchemySubscriptionMediaProjection
from etk.domains.media.task_execution import HttpxMediaCatalogAdapter, HttpxTmdbMetadataAdapter
from etk.domains.media.user_projection import SqlAlchemyUserMediaProjection
from etk.domains.media.watchlist_projection import SqlAlchemyWatchlistMediaProjectionRepository
from etk.domains.operations.database import DatabaseReadService, SqlAlchemyDatabaseReadRepository
from etk.domains.operations.database_maintenance import (
    DatabaseMaintenanceService,
    PostgresDatabaseMaintenanceRepository,
)
from etk.domains.operations.database_router import effects_router as database_effects_router
from etk.domains.operations.database_router import maintenance_router as database_maintenance_router
from etk.domains.operations.database_router import router as database_read_router
from etk.domains.operations.job_logs import PersistentJobLogStorage
from etk.domains.operations.job_router import router as job_router
from etk.domains.operations.jobs import (
    JobSubmissionService,
    PersistentRuntimeReadiness,
    PersistentRuntimeStatusReader,
)
from etk.domains.operations.log_router import router as log_router
from etk.domains.operations.logs import LocalLogStorage, LogService
from etk.domains.operations.review_reprocess import (
    PersistentReviewJobGateway,
    ReviewReprocessService,
    SqlAlchemyReviewReprocessRepository,
)
from etk.domains.operations.runtime import ManifestTaskCatalog
from etk.domains.operations.runtime_router import router as runtime_query_router
from etk.domains.operations.system_runtime_router import router as system_runtime_router
from etk.domains.p115 import (
    AtomicP115LocalStrmLibraryGateway,
    ConfiguredP115IntroSyncFeature,
    HttpxEmbyIntroFavoriteGateway,
    HttpxP115AuthorizationGateway,
    HttpxP115BoundedDownloadGateway,
    HttpxP115BridgeEmbyGateway,
    HttpxP115DirectoryGateway,
    HttpxP115DirectoryTreeGateway,
    HttpxP115OrganizeMutationGateway,
    HttpxP115SortingRuleDirectoryGateway,
    IntroDetectionSubmissionService,
    NativeP115IntroDetectionGateway,
    NativeP115PlanNamingGateway,
    NativeP115RenamePreviewGateway,
    NativeP115RuleRecognitionGateway,
    OsP115LocalDirectoryBrowser,
    P115AuthDirectoryService,
    P115DeepDeleteJobGateway,
    P115DeepDeleteService,
    P115EpisodeRegexService,
    P115IntroBackfillTaskService,
    P115IntroSyncService,
    P115ItemReadyService,
    P115LocalDirectoryService,
    P115LocalSettingsService,
    P115MaintenanceIntentService,
    P115MediaInfoCacheService,
    P115OrganizeExecutionService,
    P115OrganizePlanService,
    P115OrganizeRecordsService,
    P115PlaybackConfigService,
    P115PlaybackQrService,
    P115PlaybackService,
    P115PrimaryOperationsGateway,
    P115QuickDeployIntentService,
    P115RenameConfigService,
    P115RenameMoviePilotService,
    P115SdkDirectUrlGateway,
    P115SortingRulesService,
    P115SpeedtestService,
    P115StrmReplaceService,
    P115WashingPriorityService,
    PersistentP115IntroSnapshotExporter,
    PostgresP115IntroDetectionRepository,
    PostgresP115IntroRuntimeSource,
    PostgresP115PlaybackRepository,
    PostgresP115StrmSyncCoordinator,
    SqlAlchemyP115CredentialRepository,
    SqlAlchemyP115DeepDeleteScopeGateway,
    SqlAlchemyP115DeepDeleteSnapshotRepository,
    SqlAlchemyP115EpisodeRegexRepository,
    SqlAlchemyP115MaintenanceRepository,
    SqlAlchemyP115MediaInfoRepository,
    SqlAlchemyP115OrganizeExecutionRepository,
    SqlAlchemyP115OrganizePlanRepository,
    SqlAlchemyP115OrganizeRecordsRepository,
    SqlAlchemyP115RenameConfigRepository,
    SqlAlchemyP115SortingRulesRepository,
    SqlAlchemyP115WashingPriorityRepository,
    select_intro_provider_identity,
)
from etk.domains.p115.intro_backfill_router import router as p115_intro_backfill_router
from etk.domains.p115.playback_native import (
    NativeP115DirectLinkGateway,
    NativeP115PlaybackProvider,
    NativeP115PlaybackRapidSigner,
    NativeP115PoolPlaybackClientFactory,
    SqlAlchemyP115PlaybackConfigBackend,
    SqlAlchemyP115PlaybackSourceGateway,
)
from etk.domains.p115.playback_router import router as p115_playback_router
from etk.domains.p115.playback_user_repository import (
    SqlAlchemyP115PlaybackUserGateway,
    SqlAlchemyP115PlaybackUserRepository,
)
from etk.domains.p115.router import router as p115_router
from etk.domains.p115.upload_monitor import P115UploadMonitorConfigService
from etk.domains.p115.upload_repository import PostgresP115UploadMonitorRepository
from etk.domains.subscription.actor import ActorSubscriptionService, SqlAlchemyActorSubscriptionRepository
from etk.domains.subscription.actor_operations import ActorCancellationOperationStager
from etk.domains.subscription.actor_router import router as actor_subscription_router
from etk.domains.subscription.adapters import (
    ConfiguredEmbyLibraryGateway,
    ConfiguredResubscribePolicy,
    ConfiguredSubscriptionAvailabilityGateway,
    HttpxEmbyItemDeletionGateway,
    NativeActorRuntimeGateway,
    NativeActorSearchGateway,
    NativeWatchlistExternalDeleteGateway,
    PersistentActorRefreshGateway,
    PersistentResubscribeTaskGateway,
    PersistentWatchlistRefreshGateway,
    StaticAssistantDefaults,
)
from etk.domains.subscription.cleanup import CleanupService
from etk.domains.subscription.cleanup_adapters import SettingsCleanupExecutionPolicy
from etk.domains.subscription.cleanup_repository import (
    PostgresCleanupExecutionGateway,
    SqlAlchemyCleanupRepository,
)
from etk.domains.subscription.cleanup_router import router as cleanup_router
from etk.domains.subscription.cleanup_runtime_repository import SqlAlchemyCleanupExecutionRepository
from etk.domains.subscription.core import (
    SUBSCRIPTION_CORE_USE_CASE_PROVIDER,
    PostgresCoreSubscriptionOperationStager,
    SqlAlchemySubscriptionCoreRepository,
    SubscriptionCoreService,
)
from etk.domains.subscription.core_router import router as subscription_core_router
from etk.domains.subscription.moviepilot_config import (
    MOVIEPILOT_CONFIG_USE_CASE_PROVIDER,
    SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER,
    MoviePilotConfigService,
    SubscriptionStrategyService,
)
from etk.domains.subscription.moviepilot_config_router import router as moviepilot_config_router
from etk.domains.subscription.moviepilot_configured import ConfiguredMoviePilotGateway
from etk.domains.subscription.moviepilot_webhook import MoviePilotWebhookService
from etk.domains.subscription.moviepilot_webhook_router import router as moviepilot_webhook_router
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.portal import (
    PORTAL_SUBSCRIPTION_USE_CASE_PROVIDER,
    PortalSubscriptionRepository,
    PortalSubscriptionService,
)
from etk.domains.subscription.portal_adapter import HttpxPortalTmdbAdapter
from etk.domains.subscription.resubscribe import (
    PostgresResubscribeOperationStager,
    ResubscribeService,
    SqlAlchemyResubscribeRepository,
)
from etk.domains.subscription.resubscribe_router import router as resubscribe_router
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.domains.subscription.watchlist import WatchlistService
from etk.domains.subscription.watchlist_router import router as watchlist_router
from etk.domains.system.configuration import ConfigurationService
from etk.domains.system.configuration_adapters import HttpxConfigurationGateway
from etk.domains.system.configuration_router import router as configuration_router
from etk.domains.system.integration_compatibility import HttpxSystemIntegrationGateway
from etk.domains.system.integration_router import router as system_integration_router
from etk.domains.system.integrations import SystemIntegrationService
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.domains.system.standalone import (
    PlaintextStandaloneSettingsDefaults,
    StandaloneSettingsService,
)
from etk.domains.system.standalone_router import router as standalone_settings_router
from etk.domains.translation import ConfiguredBatchTranslation, SqlAlchemyTranslationCache
from etk.integrations import HttpxTelegramBotAdapter
from etk.platform import AppContainer, LegacyConfigAdapter, PostgresLegacyConfigStore, Settings
from etk.platform.jobs import PostgresExecutionGuard, PostgresJobStore
from etk.platform.observability import MetricsMiddleware, TraceIdMiddleware, get_metrics, metrics_response
from etk.platform.tasks import TaskRegistry
from etk.platform.web import CookieSessionMiddleware
from etk.platform.web.session_codec import LazyNativeSessionCodec
from etk.processes.p6_ingestion import NativeIngestionBindingDelivery, PersistentPendingBindingGateway
from etk.processes.p6_playback import P115PlaybackOperationExecutor
from etk.processes.p8_identity import identity_operation_claim_lease_seconds

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
        assert config_adapter is not None
        configuration_writer = RuntimeConfigurationWriter(
            app_container,
            runtime,
            persistence=config_adapter,
        )
        _register_identity_providers(app_container, configuration_writer)
        _register_job_providers(app_container)
        _register_operation_query_providers(app_container)
        _register_log_providers(app_container, data_dir)
        _register_database_read_providers(app_container)
        _register_system_providers(app_container, configuration_writer)
        _register_collection_providers(app_container)
        _register_users_providers(app_container)
        _register_telegram_notification_provider(app_container)
        _register_p115_providers(app_container)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            if start_runtime:
                runtime.start(role=RuntimeRole.API, background=False)
            yield
        finally:
            try:
                if start_runtime:
                    runtime.stop()
            finally:
                app_container.close()

    app = FastAPI(title="embytool", lifespan=lifespan)
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

    if owns_container:
        _register_media_providers(app_container, _resolve_data_dir(data_dir))
        _register_subscription_providers(app_container)
    app.state.runtime_supervisor = runtime
    app.state.container = app_container
    app.state.config_adapter = config_adapter
    app.state.settings = app_container.settings
    app.state.configuration_synchronizer = configuration_synchronizer

    from .config_sync import RuntimeConfigMiddleware

    app.add_middleware(
        RuntimeConfigMiddleware,
        synchronize=configuration_synchronizer.synchronize if configuration_synchronizer is not None else None,
    )

    platform_metrics = get_metrics()
    app.add_middleware(MetricsMiddleware, metrics=platform_metrics)
    app.add_middleware(TraceIdMiddleware)
    session_codec = LazyNativeSessionCodec(_resolve_data_dir(data_dir), secret_key=secret_key)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: session_codec)

    app.include_router(identity_router)
    app.include_router(runtime_query_router)
    app.include_router(job_router)
    app.include_router(system_runtime_router)
    app.include_router(log_router)
    app.include_router(database_read_router)
    app.include_router(database_effects_router)
    app.include_router(database_maintenance_router)
    app.include_router(configuration_router)
    app.include_router(standalone_settings_router)
    app.include_router(system_integration_router)
    app.include_router(collection_router)
    app.include_router(collection_effects_router)
    app.include_router(users_router)
    app.include_router(users_effects_router)
    app.include_router(media_router)
    app.include_router(secondary_media_router)
    app.include_router(secondary_media_effects_router)
    app.include_router(discover_router)
    app.include_router(discover_effects_router)
    app.include_router(media_editing_router)
    app.include_router(media_image_editing_router)
    app.include_router(media_cast_editor_router)
    app.include_router(media_image_proxy_router)
    app.include_router(p115_intro_backfill_router)
    app.include_router(p115_router)
    app.include_router(p115_playback_router)
    app.include_router(actor_subscription_router)
    app.include_router(watchlist_router)
    app.include_router(cleanup_router)
    app.include_router(resubscribe_router)
    app.include_router(moviepilot_config_router)
    app.include_router(moviepilot_webhook_router)
    app.include_router(subscription_core_router)

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

    resolved_static = (static_dir or _default_static_dir()).resolve()
    assets_dir = resolved_static / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    if resolved_static.is_dir():
        app.mount("/static", StaticFiles(directory=resolved_static), name="static")

    @app.get("/{path:path}", include_in_schema=False)
    async def spa_fallback(path: str) -> Response:
        candidate = resolved_static / path
        if path and candidate.is_file() and resolved_static in candidate.parents:
            return FileResponse(candidate)
        index = resolved_static / "index.html"
        if index.is_file():
            return FileResponse(index)
        return Response(status_code=404)

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
    configuration_writer: RuntimeConfigurationWriter,
) -> None:
    container.register_repository(
        "identity",
        lambda current: SqlAlchemyIdentityRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "identity",
        lambda current: HttpxEmbyIdentityGateway(lambda: current.settings),
    )
    container.register_use_case(
        "identity",
        lambda current: IdentityService(
            settings=lambda: current.settings,
            repository=current.repository("identity"),
            gateway=current.client("identity"),
            configuration=configuration_writer,
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
            cancellation_policy=SqlAlchemyCleanupExecutionRepository(current.database.unit_of_work),
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
    container.register_client("operations.job-readiness", lambda _: PersistentRuntimeReadiness())


def _register_log_providers(container: AppContainer, data_dir: Path | None) -> None:
    log_directory = _resolve_data_dir(data_dir) / "logs"
    container.register_use_case(
        "operations.logs",
        lambda current: LogService(
            PersistentJobLogStorage(current.database.unit_of_work, LocalLogStorage(log_directory))
        ),
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
            quota=current.repository("subscription.runtime"),
            settings=lambda: current.settings,
            runtime_status=lambda: current.use_case("runtime-status").read_status(),
        ),
    )
    container.register_repository(
        "operations.database-maintenance",
        lambda current: PostgresDatabaseMaintenanceRepository(current.database.unit_of_work),
    )
    container.register_use_case(
        "operations.database-maintenance",
        lambda current: DatabaseMaintenanceService(
            current.repository("operations.database-maintenance"),
            unit_of_work=current.database.unit_of_work,
            settings=lambda: current.settings,
            jobs=current.repository("operations.jobs"),
        ),
    )
    container.register_repository(
        "operations.review-reprocess",
        lambda current: SqlAlchemyReviewReprocessRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_use_case(
        "operations.review-reprocess",
        lambda current: ReviewReprocessService(
            current.repository("operations.review-reprocess"),
            PersistentReviewJobGateway(current.use_case("operations.jobs")),
            emby_item=HttpxMediaCatalogAdapter(lambda: current.settings).item,
            identification=ReprocessIdentificationService(
                HttpxMediaCatalogAdapter(lambda: current.settings), HttpxTmdbMetadataAdapter(lambda: current.settings),
                SqlAlchemyIdentificationRepository(current.database.unit_of_work),
            ),
        ),
    )


def _register_system_providers(
    container: AppContainer,
    configuration_writer: RuntimeConfigurationWriter,
) -> None:
    container.register_repository(
        "app-settings",
        lambda current: SqlAlchemyAppSettingsRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "system-configuration",
        lambda current: HttpxConfigurationGateway(
            lambda: current.settings,
            current.repository("app-settings"),
        ),
    )
    container.register_use_case(
        "system-configuration",
        lambda current: ConfigurationService(
            settings=lambda: current.settings,
            repository=current.repository("app-settings"),
            writer=configuration_writer,
            gateway=current.client("system-configuration"),
        ),
    )
    defaults = PlaintextStandaloneSettingsDefaults()
    container.register_use_case(
        "system-standalone-settings",
        lambda current: StandaloneSettingsService(
            current.repository("app-settings"),
            defaults,
        ),
    )
    container.register_client(
        "system-integrations",
        lambda current: HttpxSystemIntegrationGateway(
            lambda: current.settings,
        ),
    )
    container.register_use_case(
        "system-integrations",
        lambda current: SystemIntegrationService(
            settings=lambda: current.settings,
            gateway=current.client("system-integrations"),
        ),
    )


def _register_collection_providers(container: AppContainer) -> None:
    container.register_repository(
        "collection",
        lambda current: SqlAlchemyCollectionRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "collection",
        lambda current: P8LocalCollectionGateway(
            renderer=PillowCoverPreviewRenderer(Path(__file__).resolve().parents[3] / "fonts" / "en_font.ttf"),
        ),
    )
    container.register_client(
        "collection.query",
        lambda current: HttpxCollectionQueryAdapter(lambda: current.settings),
    )
    container.register_repository(
        "collection.operations",
        lambda current: PostgresCollectionOperationRepository(current.database.unit_of_work),
    )
    container.register_client(
        "collection.effects",
        lambda current: PostgresCollectionEffectGateway(current.repository("collection.operations")),
    )
    container.register_client(
        "collection.operation-reconciler",
        lambda current: CollectionOperationReconciler(
            current.repository("collection.operations"),
            HttpxCollectionRemoteEffectsAdapter(lambda: current.settings),
        ),
    )
    container.register_repository(
        "collection.activation",
        lambda current: PostgresCollectionActivationRepository(
            current.database.unit_of_work,
            current.repository("collection.operations"),
        ),
    )
    container.register_use_case(
        "collection.activation",
        lambda current: CollectionActivationService(current.repository("collection.activation")),
    )
    container.register_repository(
        "collection-user-projection",
        lambda current: SqlAlchemyCollectionUserProjection(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "collection-media-projection",
        lambda current: SqlAlchemyCollectionMediaProjection(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_use_case(
        "collection",
        lambda current: CollectionService(
            repository=current.repository("collection"),
            settings=current.repository("app-settings"),
            gateway=current.client("collection"),
            user_projection=current.repository("collection-user-projection"),
            media_projection=current.repository("collection-media-projection"),
            query_gateway=current.client("collection.query"),
            effect_gateway=current.client("collection.effects"),
            operation_repository=current.repository("collection.operations"),
            operation_reconciler=current.client("collection.operation-reconciler"),
        ),
    )


def _register_users_providers(container: AppContainer) -> None:
    container.register_repository(
        "identity.operations",
        lambda current: PostgresIdentityOperationRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "users",
        lambda current: SqlAlchemyUsersRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "users.telegram-bot-info",
        lambda current: HttpxTelegramBotInfoGateway(lambda: current.settings),
    )
    container.register_client(
        "identity.effects.emby",
        lambda current: HttpxEmbyIdentityEffectsAdapter(lambda: current.settings),
    )
    container.register_client(
        "users.playback-reporting",
        lambda current: HttpxPlaybackReportingAdapter(lambda: current.settings),
    )
    container.register_client(
        "identity.registration-secret",
        lambda _: InProcessRegistrationSecretPort(),
    )
    container.register_repository(
        "users-media-projection",
        lambda current: SqlAlchemyUserMediaProjection(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_use_case(
        "users",
        lambda current: UsersService(
            repository=current.repository("users"),
            media=current.repository("users-media-projection"),
            settings=lambda: current.settings,
            telegram=current.client("users.telegram-bot-info"),
        ),
    )
    container.register_use_case(
        "users.playback",
        lambda current: PlaybackReportingService(
            current.client("users.playback-reporting"),
            current.repository("users-media-projection"),
            settings=lambda: current.settings,
        ),
    )
    container.register_use_case(
        "users.effects",
        lambda current: IdentityEffectsService(
            registration=RegistrationSagaCoordinator(
                current.repository("identity.operations"),
                current.client("identity.effects.emby"),
            ),
            mutations=IdentityUserMutationCoordinator(
                current.repository("identity.operations"),
                current.client("identity.effects.emby"),
            ),
            secrets=current.client("identity.registration-secret"),
            repository=current.repository("users"),
            emby=current.client("identity.effects.emby"),
            settings=lambda: current.settings,
            lease_seconds=identity_operation_claim_lease_seconds(current.settings.emby_api_timeout),
        ),
    )
    container.register_use_case(
        "media.secondary",
        lambda current: SecondaryMediaService(
            media=current.repository("users-media-projection"),
            settings=current.repository("app-settings"),
        ),
    )
    container.register_client(
        "media.secondary.effects.emby",
        lambda current: HttpxSecondaryEmbyAdapter(lambda: current.settings),
    )
    container.register_use_case(
        "media.secondary.effects",
        lambda current: SecondaryEffectsService(
            current.client("media.secondary.effects.emby"),
            current.use_case("operations.jobs"),
        ),
    )
    container.register_client("media.discover-defaults", lambda _: StaticDiscoverDefaults())
    container.register_client(
        "media.discover-provider",
        lambda current: HttpxDiscoverProviderAdapter(lambda: current.settings),
    )
    container.register_repository(
        "media.discover-projection",
        lambda current: SqlAlchemyDiscoverProjection(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "media.daily-theme",
        lambda current: SqlAlchemyDailyThemeRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_use_case(
        "media.discover",
        lambda current: DiscoverService(
            settings=current.repository("app-settings"),
            defaults=current.client("media.discover-defaults"),
        ),
    )
    container.register_use_case(
        "media.discover.effects",
        lambda current: DiscoverEffectsService(
            current.client("media.discover-provider"),
            current.repository("media.discover-projection"),
            current.repository("app-settings"),
            current.client("media.discover-defaults"),
            current.use_case("operations.jobs"),
            include_adult=lambda: bool(current.settings.tmdb_include_adult),
        ),
    )
    container.register_use_case(
        "media.daily-theme",
        lambda current: DailyThemeService(
            current.client("media.discover-provider"),
            current.repository("media.daily-theme"),
            current.client("media.discover-defaults"),
        ),
    )


def _register_telegram_notification_provider(container: AppContainer) -> None:
    container.register_client(
        "sharing.telegram-bot",
        lambda current: HttpxTelegramBotAdapter(lambda: current.settings),
    )


def _register_media_providers(
    container: AppContainer,
    data_root: Path,
) -> None:
    container.register_repository(
        "media",
        lambda current: SqlAlchemyMediaRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "media.search-index",
        lambda current: SqlAlchemyEmbySearchRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "media-images",
        lambda current: HttpxMediaImageGateway(
            lambda: current.settings,
            current.repository("media"),
            current.repository("media-image-store"),
            HttpxP5MediaEventProvider(lambda: current.settings),
            lambda tmdb_id: current.client("collection.effects").stage_missing_cleanup(tmdb_id),
        ),
    )
    container.register_client(
        "media-p5-events-native",
        lambda current: NativeP5MediaEventGateway(
            lambda: current.settings,
            current.repository("media"),
            lambda: current.use_case("operations.jobs"),
            HttpxP5MediaEventProvider(lambda: current.settings),
            current.repository("media-image-store"),
        ),
    )
    container.register_client(
        "media-p5-events",
        lambda current: PersistentP5MediaEventGateway(
            lambda: current.database.unit_of_work(),
            current.client("media-p5-events-native"),
            lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
            defer=True,
        ),
    )
    container.register_client(
        "media-future-events",
        lambda current: P8FutureDomainEventGateway(
            P8UserEventGateway(
                repository=SqlAlchemyUserActivityRepository(current.database.unit_of_work),
                resolver=HttpxEmbyUserActivityAdapter(lambda: current.settings),
                policy_guard=PersistentPolicyPropagationGuard(
                    PersistentRuntimeStateStore(PostgresRuntimeStateBackend(current.database.raw_pool.connection))
                ),
            ),
            P8CollectionRemovalEventGateway(
                current.database.unit_of_work,
                PersistentCollectionDeletionSuppression(
                    PersistentRuntimeStateStore(PostgresRuntimeStateBackend(current.database.raw_pool.connection))
                ),
            ),
        ),
    )
    container.register_client(
        "media-mapping-defaults",
        lambda _: StaticMediaMappingDefaults(),
    )
    container.register_client(
        "media-runtime",
        lambda _: PersistentRuntimeReadiness(),
    )
    container.register_repository(
        "media-image-store",
        lambda current: SqlAlchemyMediaImageStore(
            current.database.unit_of_work,
            lambda: current.settings,
            data_root,
        ),
    )
    container.register_client(
        "media-image-editing-native",
        lambda current: HttpxMediaImageEditingGateway(
            lambda: current.settings,
            current.repository("media"),
            current.repository("media-image-store"),
        ),
    )
    container.register_client(
        "media-image-editing",
        lambda current: PersistentMediaImageEditingGateway(
            lambda: current.database.unit_of_work(),
            current.client("media-image-editing-native"),
            lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
            defer=True,
        ),
    )
    container.register_client(
        "media-image-proxy",
        lambda current: HttpxMediaImageProxyGateway(
            lambda: current.settings,
            current.repository("media-image-store"),
        ),
    )
    container.register_repository(
        "media-cast-editor",
        lambda current: SqlAlchemyCastEditorRepository(current.database.unit_of_work),
    )
    container.register_client(
        "media-cast-editor",
        lambda current: HttpxMediaCastEditorGateway(
            lambda: current.settings,
            current.repository("media-cast-editor"),
            lambda: current.use_case("operations.jobs"),
            ConfiguredBatchTranslation(
                lambda: current.settings,
                SqlAlchemyTranslationCache(current.database.unit_of_work),
            ),
        ),
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
        "media.search-index",
        lambda current: EmbySearchIndexService(current.repository("media.search-index")),
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
    container.register_repository(
        "p115.maintenance",
        lambda current: SqlAlchemyP115MaintenanceRepository(current.database.unit_of_work),
    )
    container.register_client(
        "ingestion.pending-bindings",
        lambda current: PersistentPendingBindingGateway(
            ledger=current.repository("ingestion.monitor"),
            delivery=NativeIngestionBindingDelivery(
                jobs=lambda: current.use_case("operations.jobs"),
                uow_factory=current.database.unit_of_work,
                bridge=current.client("media-emby-bridge"),
                media_info=current.repository("p115.media-info"),
                lease_seconds=lambda: max(float(current.settings.emby_api_timeout) + 30, 90),
            ),
            lease_seconds=max(float(current.settings.emby_api_timeout) + 30, 90),
        ),
    )
    container.register_repository(
        "p115.credentials",
        lambda current: SqlAlchemyP115CredentialRepository(current.database.unit_of_work),
    )
    container.register_client(
        "p115.authorization",
        lambda _: HttpxP115AuthorizationGateway(),
    )
    container.register_client(
        "p115.playback-users",
        lambda current: SqlAlchemyP115PlaybackUserGateway(
            SqlAlchemyP115PlaybackUserRepository(current.database.unit_of_work),
            current.client("sharing.telegram-bot"),
        ),
    )
    container.register_client(
        "p115.playback-sources",
        lambda current: SqlAlchemyP115PlaybackSourceGateway(
            current.database.unit_of_work,
            primary_credentials=current.repository("p115.credentials"),
            priority=lambda: str(current.settings.p115_api_priority),
        ),
    )
    container.register_client(
        "p115.directories",
        lambda current: HttpxP115DirectoryGateway(current.client("p115.directory-tree")),
    )
    container.register_client(
        "p115.directory-tree",
        lambda current: HttpxP115DirectoryTreeGateway(
            credentials=current.repository("p115.credentials"),
            priority=lambda: current.settings.p115_api_priority,
            settings=lambda: current.settings,
            direct_urls=current.client("p115.maintenance-direct-urls"),
        ),
    )
    container.register_client(
        "p115.maintenance-operations",
        lambda current: P115PrimaryOperationsGateway(
            credentials=current.repository("p115.credentials"),
            priority=lambda: current.settings.p115_api_priority,
        ),
    )
    container.register_client(
        "p115.maintenance-direct-urls",
        lambda current: P115SdkDirectUrlGateway(
            credentials=current.repository("p115.credentials"),
            priority=lambda: current.settings.p115_api_priority,
        ),
    )
    container.register_client(
        "p115.maintenance-downloads",
        lambda _: HttpxP115BoundedDownloadGateway(),
    )
    container.register_repository(
        "p115.sorting-rules",
        lambda current: SqlAlchemyP115SortingRulesRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_client(
        "p115.sorting-rule-directories",
        lambda current: HttpxP115SortingRuleDirectoryGateway(current.client("p115.directory-tree")),
    )
    container.register_repository(
        "p115.rename-config",
        lambda current: SqlAlchemyP115RenameConfigRepository(
            lambda: current.database.unit_of_work(),
        ),
    )
    container.register_repository(
        "p115.washing-priority",
        lambda current: SqlAlchemyP115WashingPriorityRepository(
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
        "p115.intro-runtime",
        lambda current: PostgresP115IntroRuntimeSource(
            current.database.unit_of_work,
            current.repository("p115.intro-detection"),
            current.repository("app-settings"),
            HttpxEmbyIntroFavoriteGateway(lambda: current.settings),
        ),
    )
    container.register_client(
        "p115.intro-sync-feature",
        lambda current: ConfiguredP115IntroSyncFeature(current.repository("app-settings")),
    )
    container.register_client(
        "p115.intro-snapshot-exporter",
        lambda current: PersistentP115IntroSnapshotExporter(
            PostgresMediaEffectLedger(current.database.unit_of_work),
        ),
    )
    container.register_client(
        "p115.deep-delete-scope",
        lambda current: SqlAlchemyP115DeepDeleteScopeGateway(
            current.database.unit_of_work,
            current.repository("p115.credentials"),
            current.client("p115.maintenance-operations"),
        ),
    )
    container.register_client(
        "p115.deep-delete-jobs",
        lambda current: P115DeepDeleteJobGateway(lambda: current.use_case("operations.jobs")),
    )
    container.register_client(
        "p115.rename-preview",
        lambda _: NativeP115RenamePreviewGateway(),
    )
    container.register_client(
        "p115.washing-priority-config",
        lambda current: current.use_case("p115.washing-priority"),
    )
    container.register_client(
        "p115.plan-recognition",
        lambda _: NativeP115RuleRecognitionGateway(),
    )
    container.register_client(
        "p115.plan-naming",
        lambda _: NativeP115PlanNamingGateway(),
    )
    container.register_client(
        "p115.organize-mutations",
        lambda current: HttpxP115OrganizeMutationGateway(
            credentials=lambda: current.repository("p115.credentials"),
            priority=lambda: current.settings.p115_api_priority,
        ),
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
            SqlAlchemyP115PlaybackConfigBackend(current.database.unit_of_work),
            current.client("p115.playback-users"),
        ),
    )
    container.register_client(
        "p115.playback-provider",
        lambda current: NativeP115PlaybackProvider(
            NativeP115PoolPlaybackClientFactory(
                current.use_case("p115.playback-config"),
                primary_credentials=current.repository("p115.credentials"),
            ),
            NativeP115PlaybackRapidSigner(
                P115SdkDirectUrlGateway(
                    credentials=current.repository("p115.credentials"),
                    priority=lambda: str(current.settings.p115_api_priority),
                ),
                current.repository("p115.credentials"),
            ),
        ),
    )
    container.register_client(
        "p115.playback-direct-links",
        lambda current: NativeP115DirectLinkGateway(
            current.use_case("p115.playback-config"),
            current.repository("p115.credentials"),
            priority=lambda: str(current.settings.p115_api_priority),
        ),
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
        "p115.washing-priority",
        lambda current: P115WashingPriorityService(
            settings=current.repository("app-settings"),
            repository=current.repository("p115.washing-priority"),
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
        "p115.rename-moviepilot",
        lambda current: P115RenameMoviePilotService(
            gateway=ConfiguredMoviePilotGateway(current.repository("app-settings")),
            settings=current.repository("app-settings"),
        ),
    )
    container.register_use_case(
        "p115.local-settings",
        lambda current: P115LocalSettingsService(current.repository("app-settings")),
    )
    container.register_use_case(
        "p115.local-directories",
        lambda _current: P115LocalDirectoryService(OsP115LocalDirectoryBrowser()),
    )
    container.register_use_case(
        "p115.maintenance-intents",
        lambda current: P115MaintenanceIntentService(
            credentials=current.repository("p115.credentials"),
            priority=lambda: current.settings.p115_api_priority,
        ),
    )
    container.register_use_case(
        "p115.quick-deploy-intent",
        lambda current: P115QuickDeployIntentService(
            settings=lambda: current.settings,
            credentials=current.repository("p115.credentials"),
            app_settings=current.repository("app-settings"),
            priority=lambda: current.settings.p115_api_priority,
        ),
    )
    container.register_use_case(
        "p115.speedtest",
        lambda current: P115SpeedtestService(
            credentials=current.repository("p115.credentials"),
            facts=current.repository("p115.maintenance"),
            direct_urls=current.client("p115.maintenance-direct-urls"),
            downloads=current.client("p115.maintenance-downloads"),
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
            source=current.client("p115.intro-runtime"),
            account_id=lambda: current.repository("p115.credentials").load().account_id,
            provider_identity=lambda: select_intro_provider_identity(
                current.repository("p115.credentials"),
                lambda: current.settings.p115_api_priority,
            ),
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
            sync_delete_enabled=lambda: current.settings.p115_enable_sync_delete,
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
        "subscription.runtime",
        lambda current: PostgresSubscriptionRuntimeRepository(current.database.unit_of_work),
    )
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
    container.register_repository(
        "media.cleanup-projection",
        lambda _: SqlAlchemyCleanupMediaProjection(),
    )
    container.register_repository(
        "media.resubscribe-projection",
        lambda _: SqlAlchemyResubscribeMediaProjection(),
    )
    container.register_repository(
        "media.subscription-projection",
        lambda _: SqlAlchemySubscriptionMediaProjection(),
    )
    container.register_repository(
        "subscription.cleanup-index",
        lambda current: SqlAlchemyCleanupRepository(
            current.database.unit_of_work,
            current.repository("media.cleanup-projection"),
            SettingsCleanupExecutionPolicy(
                lambda: current.settings,
                current.repository("p115.credentials"),
            ),
        ),
    )
    container.register_repository(
        "subscription.resubscribe",
        lambda current: SqlAlchemyResubscribeRepository(
            current.database.unit_of_work,
            current.repository("media.resubscribe-projection"),
            PostgresResubscribeOperationStager(),
            current.client("subscription.resubscribe-policy"),
        ),
    )
    container.register_repository(
        "subscription.core",
        lambda current: SqlAlchemySubscriptionCoreRepository(
            current.database.unit_of_work,
            current.repository("media.subscription-projection"),
            PostgresCoreSubscriptionOperationStager(),
            current.repository("app-settings"),
        ),
    )
    container.register_repository(
        "subscription.portal",
        lambda current: PortalSubscriptionRepository(
            current.database.unit_of_work,
            current.repository("app-settings"),
        ),
    )
    container.register_client(
        "subscription.actor-search",
        lambda current: NativeActorSearchGateway(lambda: current.settings),
    )
    container.register_client(
        "subscription.portal-tmdb",
        lambda current: HttpxPortalTmdbAdapter(lambda: current.settings),
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
    container.register_client(
        "subscription.cleanup-execution",
        lambda current: PostgresCleanupExecutionGateway(current.repository("subscription.cleanup-index")),
    )
    container.register_client(
        "subscription.resubscribe-policy",
        lambda current: ConfiguredResubscribePolicy(current.repository("app-settings")),
    )
    container.register_client(
        "subscription.resubscribe-tasks",
        lambda current: PersistentResubscribeTaskGateway(lambda: current.use_case("operations.jobs")),
    )
    container.register_client(
        "subscription.resubscribe-libraries",
        lambda current: ConfiguredEmbyLibraryGateway(lambda: current.settings),
    )
    container.register_client(
        "subscription.assistant-defaults",
        lambda _: StaticAssistantDefaults(),
    )
    container.register_client(
        "subscription.availability",
        lambda current: ConfiguredSubscriptionAvailabilityGateway(current.repository("app-settings")),
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
    container.register_use_case(
        "subscription.cleanup",
        lambda current: CleanupService(
            repository=current.repository("subscription.cleanup-index"),
            settings=current.repository("app-settings"),
            jobs=current.use_case("operations.jobs"),
            readiness=current.client("operations.job-readiness"),
            execution=current.client("subscription.cleanup-execution"),
        ),
    )
    container.register_use_case(
        "subscription.resubscribe",
        lambda current: ResubscribeService(
            repository=current.repository("subscription.resubscribe"),
            tasks=current.client("subscription.resubscribe-tasks"),
            libraries=current.client("subscription.resubscribe-libraries"),
        ),
    )
    container.register_use_case(
        MOVIEPILOT_CONFIG_USE_CASE_PROVIDER,
        lambda current: MoviePilotConfigService(
            current.repository("app-settings"),
            current.client("subscription.assistant-defaults"),
        ),
    )
    container.register_use_case(
        "subscription.moviepilot-webhook",
        lambda current: MoviePilotWebhookService(
            PostgresMediaEffectLedger(current.database.unit_of_work),
            current.repository("app-settings"),
            lambda: current.settings,
            current.repository("p115.credentials"),
        ),
    )
    container.register_use_case(
        SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER,
        lambda current: SubscriptionStrategyService(
            current.repository("app-settings"),
            current.client("subscription.availability"),
        ),
    )
    container.register_use_case(
        SUBSCRIPTION_CORE_USE_CASE_PROVIDER,
        lambda current: SubscriptionCoreService(
            current.repository("subscription.core"),
            current.client("subscription.availability"),
            current.repository("identity"),
        ),
    )
    container.register_use_case(
        PORTAL_SUBSCRIPTION_USE_CASE_PROVIDER,
        lambda current: PortalSubscriptionService(
            current.repository("subscription.portal"),
            current.client("subscription.portal-tmdb"),
        ),
    )


def _resolve_data_dir(data_dir: Path | None) -> Path:
    if data_dir is not None:
        return data_dir.resolve()
    configured = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[3] / "local_data"


def _default_static_dir() -> Path:
    configured = os.environ.get("ETK_STATIC_DIR")
    if configured:
        return Path(configured)
    repository_root = Path(__file__).resolve().parents[3]
    container_static = repository_root / "static"
    if (container_static / "index.html").is_file():
        return container_static
    return repository_root / "frontend" / "dist"


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
