from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any
from uuid import UUID

from etk.bootstrap.app_factory import _create_default_container
from etk.bootstrap.runtime_supervisor import RuntimeRole, RuntimeSupervisor
from etk.compatibility.background import PersistentBackgroundGateway
from etk.compatibility.ingestion_monitor import (
    clear_persistent_ingestion_binding,
    clear_persistent_ingestion_ingress,
    install_persistent_ingestion_binding,
    install_persistent_ingestion_ingress,
)
from etk.compatibility.intro_detection import (
    clear_persistent_intro_detection_gateway,
    install_persistent_intro_detection_gateway,
)
from etk.compatibility.legacy_jobs import LegacyTaskAdapter
from etk.compatibility.media_effects import clear_persistent_media_effect_gateway
from etk.compatibility.p115_credentials import (
    clear_persistent_p115_credential_gateway,
    install_persistent_p115_credential_gateway,
    invalidate_legacy_p115_credential_clients,
)
from etk.compatibility.persistent_runtime_state import (
    PersistentRuntimeStateStore,
    PostgresRuntimeStateBackend,
)
from etk.compatibility.persistent_tasks import PersistentTaskManagerGateway
from etk.compatibility.playback import (
    NativePlaybackCompatibilityGateway,
    clear_persistent_playback_gateway,
    install_persistent_playback_gateway,
)
from etk.compatibility.tg_control_bridge import TelegramControlBridge
from etk.compatibility.tg_resource_effects import TelegramResourceSecretStore
from etk.compatibility.upload_monitor import (
    clear_persistent_upload_source_cleanup,
    install_persistent_upload_source_cleanup,
)
from etk.domains.collection.activation import PostgresCollectionActivationRepository
from etk.domains.collection.custom_recommendation import (
    ConfiguredCustomCollectionDiscoveryProvider,
    SqlAlchemyCustomCollectionRecommendationService,
)
from etk.domains.collection.custom_task_adapters import (
    HttpxCustomCollectionSourceAdapter,
    HttpxCustomCollectionTmdbAdapter,
    NativeCustomCollectionRefreshOperationAdapter,
    SqlAlchemyCustomCollectionLibraryQueryAdapter,
    SqlAlchemyCustomCollectionMediaIndexAdapter,
)
from etk.domains.collection.custom_tasks import CustomCollectionTaskService
from etk.domains.collection.effect_adapters import (
    HttpxCollectionRemoteEffectsAdapter,
    PersistentCollectionDeletionSuppression,
    SqlAlchemyCollectionAssetEffectsAdapter,
)
from etk.domains.collection.operation_execution import CollectionOperationExecutor
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.operations import CollectionOperation
from etk.domains.collection.refresh import (
    NativeCollectionRefreshService,
    SqlAlchemyNativeCollectionRefreshRepository,
)
from etk.domains.collection.refresh_adapter import HttpxNativeCollectionRefreshAdapter
from etk.domains.collection.repository import SqlAlchemyCollectionRepository
from etk.domains.identity.effect_adapters import HttpxEmbyIdentityEffectsAdapter
from etk.domains.identity.effect_execution import (
    IdentityOperationExecutor,
    IdentityUserMutationCoordinator,
    RegistrationSagaCoordinator,
)
from etk.domains.identity.operation_repository import PostgresIdentityOperationRepository
from etk.domains.identity.operations import IdentityOperation
from etk.domains.identity.repository import SqlAlchemyIdentityRepository
from etk.domains.identity.user_activity_adapters import HttpxEmbyUserActivityAdapter
from etk.domains.identity.user_activity_effects import (
    LegacyUserEventEffectsAdapter,
    P8UserEventEffectDelivery,
    SqlAlchemyTemplatePropagationRepository,
    UserPolicyEffectService,
)
from etk.domains.identity.user_expiry import IdentityUserExpiryService
from etk.domains.identity.user_sync import SqlAlchemyUserSyncRepository, UserSyncService
from etk.domains.identity.user_sync_adapter import HttpxEmbyUserSyncAdapter
from etk.domains.ingestion import MonitorItemProjection, PostgresIngestionMonitorLedger
from etk.domains.media.auto_tagging import AutoTaggingService
from etk.domains.media.auto_tagging_adapter import (
    HttpxAutoTaggingAdapter,
    PersistentAutoTaggingEffects,
    PostgresAutoTaggingItemLock,
)
from etk.domains.media.bridge_client import HttpxEmbyBridgeClient
from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.media.compatibility import LegacyP5MediaEventGateway
from etk.domains.media.daily_theme import DailyThemeService
from etk.domains.media.daily_theme_effects import DailyThemeReplenishmentEffectDelivery
from etk.domains.media.daily_theme_repository import SqlAlchemyDailyThemeRepository
from etk.domains.media.discover_adapters import HttpxDiscoverProviderAdapter
from etk.domains.media.discover_compatibility import LegacyDiscoverDefaults
from etk.domains.media.editing_effects import BridgeMediaEffectDelivery, PersistentMediaInfoCommandGateway
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor, RoutedMediaEffectDelivery
from etk.domains.media.effects import MediaEffectProjection, PostgresMediaEffectLedger
from etk.domains.media.embeddings import MediaEmbeddingService, SqlAlchemyMediaEmbeddingRepository
from etk.domains.media.embeddings_adapter import ConfiguredMediaEmbeddingProvider
from etk.domains.media.event_effects import P5MediaEventEffectDelivery
from etk.domains.media.final_refresh_effects import (
    EmbyMetadataRefreshEffectDelivery,
    clear_final_refresh_settings_provider,
    install_final_refresh_settings_provider,
)
from etk.domains.media.image_editing_compatibility import LegacyMediaImageEditingGateway
from etk.domains.media.image_editing_effects import ManualImageEffectDelivery
from etk.domains.media.resubscribe_projection import SqlAlchemyResubscribeMediaProjection
from etk.domains.media.studio_images import StudioImageService
from etk.domains.media.studio_images_adapter import (
    HttpxStudioImageAdapter,
    PersistentStudioImageEffects,
    studio_image_claim_lease_seconds,
)
from etk.domains.media.subscription_projection import SqlAlchemySubscriptionMediaProjection
from etk.domains.media.task_execution import (
    FfmpegScreenshotCapture,
    HttpxEmbyScreenshotUploadAdapter,
    HttpxMediaCatalogAdapter,
    HttpxTmdbMetadataAdapter,
    NativeMediaMetadataTaskService,
    NativeMediaScreenshotBackfillService,
    SqlAlchemyMediaMetadataTaskRepository,
    SqlAlchemyScreenshotRepository,
)
from etk.domains.media.watchlist_projection import SqlAlchemyWatchlistMediaProjectionRepository
from etk.domains.operations.database_maintenance import (
    DatabaseMaintenanceExecutor,
    PostgresDatabaseMaintenanceRepository,
)
from etk.domains.operations.jobs import (
    JobSubmissionService,
    import_legacy_operation,
)
from etk.domains.operations.review_reprocess import P8_REVIEW_TASK_KEYS
from etk.domains.p115 import (
    AtomicP115LocalStrmLibraryGateway,
    ChromaprintProcessAdapter,
    EmbyAuthorizationRevisionAdapter,
    EmbyPathChangeEffectDelivery,
    HttpxEmbyIntroFavoriteGateway,
    HttpxEmbyPathChangeGateway,
    HttpxEmbyQuickDeployGateway,
    HttpxP115DirectoryTreeGateway,
    HttpxP115EpisodeRuntimeGateway,
    HttpxP115FingerprintRemoteGateway,
    HttpxP115OrganizeMutationGateway,
    HttpxP115OrganizeReadbackGateway,
    HttpxP115StrmSyncGateway,
    IntroDetectionExecutionService,
    IntroDetectionOperation,
    IntroDetectionSubmissionService,
    LegacyP115StrmSyncGateway,
    NativeIntroDetectionBatchEngine,
    NativeIntroPolicyAdapter,
    NativeP115IntroDetectionGateway,
    NativeP115IntroDirectUrlGateway,
    P115DirectorySyncService,
    P115FingerprintRepairService,
    P115IntroBackfillTaskService,
    P115IntroFingerprintAdapter,
    P115MkdirOperation,
    P115MoviePilotRenameExportOperation,
    P115OrganizeExecutionService,
    P115OrganizePlanService,
    P115OrganizeRecordTmdbBackfillService,
    P115PlaybackConfigService,
    P115PlaybackService,
    P115PrimaryOperationsGateway,
    P115QuickDeployLock,
    P115QuickDeployOperation,
    P115RecycleBinEmptyOperation,
    P115SdkDirectUrlGateway,
    P115StrmMediaInfoEnrichmentService,
    P115StrmSyncService,
    P115UnrecognizedEmptyOperation,
    P115WashingPriorityRecalculationService,
    PostgresP115IntroDetectionRepository,
    PostgresP115IntroRuntimeSource,
    PostgresP115PlaybackRepository,
    PostgresP115StrmSyncCoordinator,
    SqlAlchemyP115CredentialRepository,
    SqlAlchemyP115DirectoryCacheRepository,
    SqlAlchemyP115EpisodeRegexRepository,
    SqlAlchemyP115MaintenanceRepository,
    SqlAlchemyP115MediaInfoRepository,
    SqlAlchemyP115OrganizeExecutionRepository,
    SqlAlchemyP115OrganizePlanRepository,
    SqlAlchemyP115OrganizeRecordsRepository,
    SqlAlchemyP115QuickDeployConfigurationRepository,
    SqlAlchemyP115RenameConfigRepository,
    SqlAlchemyP115SortingRulesRepository,
    SqlAlchemyP115StrmSyncRepository,
    SqlAlchemyP115WashingPriorityRepository,
    SqlAlchemyP115WashingRecalculationRepository,
    select_intro_provider_identity,
)
from etk.domains.p115.iso_probe import NativeP115IsoProbeGateway
from etk.domains.p115.media_info_adapters import (
    ControlledProcessRunner,
    LocalP115FingerprintHintGateway,
    PersistentP115MediaInfoPolicyProvider,
)
from etk.domains.p115.media_info_restore import HttpxEmbyMediaInfoQueryGateway, P115MediaInfoRestoreService
from etk.domains.p115.media_probe import P115MediaProbeService
from etk.domains.p115.playback import PlaybackOperation
from etk.domains.p115.playback_native import (
    NativeP115DirectLinkGateway,
    NativeP115PlaybackProvider,
    NativeP115PlaybackRapidSigner,
    NativeP115PoolPlaybackClientFactory,
    SqlAlchemyP115PlaybackConfigBackend,
    SqlAlchemyP115PlaybackSourceGateway,
)
from etk.domains.p115.playback_reward_repository import (
    SqlAlchemyP115PlaybackRewardRepository,
    SqlAlchemyP115PlaybackUserGateway,
)
from etk.domains.p115.upload_adapters import (
    LegacyP115UploadProjection,
    LegacyP115UploadProvider,
    NativeP115UploadSourceCleanupGateway,
)
from etk.domains.p115.upload_monitor import P115UploadMonitorConfigService, UploadOperation
from etk.domains.p115.upload_repository import PostgresP115UploadMonitorRepository
from etk.domains.people.task_execution import (
    HttpxPeopleCatalogAdapter,
    HttpxTmdbPersonAdapter,
    NativePeopleTaskService,
    SqlAlchemyPersonMetadataRepository,
)
from etk.domains.sharing import (
    AtomicVirtualImportProjector,
    HttpxEmbyVirtualItemGateway,
    LogicalSeasonShareTransferOperationDelivery,
    NativeP115LogicalSeasonShareGateway,
    NativeP115RapidTransferGateway,
    PostgresContributionLedger,
    PostgresSharingOperationRepository,
    RapidTransferOperationDelivery,
    SharedIntroExportEffectDelivery,
    SharingOperation,
    SharingOperationExecutor,
    SharingProviderOperationDelivery,
    SharingService,
    SqlAlchemySharingProjectionRepository,
    SqlAlchemyVirtualImportRepository,
    VirtualImportOperationDelivery,
    VirtualImportService,
    VirtualImportSourceResolver,
)
from etk.domains.sharing.configuration import SharingServerIdentity
from etk.domains.subscription.adapters import ConfiguredResubscribePolicy
from etk.domains.subscription.assistant import (
    SqlAlchemyAssistantMaintenanceRepository,
    SubscribeAssistantMaintenanceService,
)
from etk.domains.subscription.assistant_task_execution import NativeSubscribeAssistantMaintenanceGateway
from etk.domains.subscription.auto_subscribe import AutoSubscribeService
from etk.domains.subscription.auto_subscribe_adapters import (
    AutoSubscriptionMoviePilotGateway,
    TelegramAutoSubscriptionNotifications,
)
from etk.domains.subscription.auto_subscribe_repository import (
    SqlAlchemyAutoSubscriptionNotificationDirectory,
    SqlAlchemyAutoSubscriptionRepository,
)
from etk.domains.subscription.cleanup_adapters import (
    ConfiguredCleanupEmbyGateway,
    HttpxCleanupEmbyInventoryGateway,
    NativeCleanupP115Gateway,
    SafeLocalCleanupGateway,
    SettingsCleanupExecutionPolicy,
    SqlAlchemyCleanupMediaGateway,
)
from etk.domains.subscription.cleanup_execution import (
    CLEANUP_EXECUTION_TASK_KEY,
    CleanupExecutionPending,
    CleanupExecutionService,
)
from etk.domains.subscription.cleanup_repository import SqlAlchemyCleanupRepository
from etk.domains.subscription.cleanup_runtime_repository import SqlAlchemyCleanupExecutionRepository
from etk.domains.subscription.cleanup_scan import CleanupScanService
from etk.domains.subscription.cleanup_scan_repository import SqlAlchemyCleanupScanRepository
from etk.domains.subscription.manual_batch import (
    ManualSubscriptionBatchService,
    SqlAlchemyManualSubscriptionRepository,
)
from etk.domains.subscription.manual_batch_adapters import (
    HttpxManualSubscriptionTmdbAdapter,
    ManualSubscriptionQuotaLedger,
)
from etk.domains.subscription.moviepilot_configured import ConfiguredMoviePilotGateway
from etk.domains.subscription.moviepilot_transfer import (
    ExplicitMoviePilotRecognitionGateway,
    MoviePilotOriginalNameGateway,
    NativeMoviePilotTransferProcessor,
)
from etk.domains.subscription.moviepilot_webhook import (
    MoviePilotFullWashingOperations,
    MoviePilotWebhookEffectDelivery,
    MoviePilotWebhookSettings,
    SqlAlchemyMoviePilotAssistantEventRepository,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.operations import SubscriptionOperation
from etk.domains.subscription.provider_control import HDHiveRegistrationOperationDelivery
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.subscription.resubscribe import (
    PostgresResubscribeOperationStager,
    SqlAlchemyResubscribeRepository,
)
from etk.domains.subscription.resubscribe_cache import (
    NativeResubscribeCacheTaskService,
    SqlAlchemyResubscribeCacheRepository,
)
from etk.domains.subscription.resubscribe_task_execution import (
    NativeResubscribeHDHiveGateway,
    NativeResubscribeTaskService,
    SqlAlchemyResubscribeExecutionRepository,
)
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.domains.subscription.watchlist import OldSeasonBackfillService
from etk.domains.subscription.watchlist_task_execution import (
    HttpxWatchlistLibraryAdapter,
    HttpxWatchlistMetadataAdapter,
    NativeWatchlistMoviePilotGateway,
    NativeWatchlistP115ReclassificationGateway,
    NativeWatchlistSharedMetadataGateway,
    NativeWatchlistTaskEffects,
    NativeWatchlistTaskService,
    SqlAlchemyWatchlistTaskRepository,
)
from etk.domains.system import SqlAlchemyAppSettingsRepository
from etk.domains.translation import ConfiguredBatchTranslation
from etk.integrations import (
    DurableTelegramUserBotAdapter,
    HttpxHDHiveAdapter,
    HttpxSharedCenterAdapter,
    HttpxTelegramBotAdapter,
)
from etk.platform.jobs import (
    AttemptClaimResult,
    AttemptStatus,
    InvalidJsonPayloadError,
    InvalidStateTransitionError,
    JobProjection,
    JobStatus,
    PersistentEffectIndeterminate,
    PersistentExecutionCancelled,
    PersistentExecutionInterruptedAfterEffect,
    PersistentExecutionTimedOut,
    PostgresExecutionGuard,
    PostgresJobStore,
    normalize_json_object,
)
from etk.platform.observability.redaction import redact_sensitive
from etk.platform.tasks import ProcessorIdentity, TaskOutcome, TaskOutcomeStatus, TaskRegistry
from etk.platform.tasks.catalog import (
    NATIVE_TASK_KEYS,
    P4_NATIVE_TASK_KEYS,
    P5_NATIVE_TASK_KEYS,
    P6_INGESTION_NATIVE_TASK_KEYS,
    P6_NATIVE_TASK_KEYS,
    P6_P115_NATIVE_TASK_KEYS,
    P7_NATIVE_TASK_KEYS,
    P8_NATIVE_TASK_KEYS,
    P9_NATIVE_TASK_KEYS,
)

from .native_operations import NativeOperationRouter
from .p5_media_tasks import LegacyP5MediaCompatibilityGateway, P5MediaTaskRouter
from .p6_ingestion import (
    INGESTION_CLAIM_LEASE_SECONDS,
    IngestionMonitorExecutor,
    LegacyIngestionBindingDelivery,
    LegacyIngestionPreparationGateway,
    PersistentIngestionIngressGateway,
    PersistentPendingBindingGateway,
)
from .p6_ingestion_tasks import P6IngestionScanService, P6IngestionTaskRouter
from .p6_intro_detection import P115IntroDetectionOperationExecutor
from .p6_p115_tasks import P6P115TaskRouter
from .p6_playback import P115PlaybackOperationExecutor
from .p6_upload_monitor import P115UploadOperationExecutor
from .p7_auto_subscribe_sources import (
    AutoSubscriptionTransferOperationDelivery,
    NativeP115ResourceTransferGateway,
    PersistentOrderedAutoSubscriptionSources,
)
from .p7_resubscribe_cleanup import PersistentResubscribeCleanupGateway
from .p7_subscription import SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS
from .p7_subscription_effects import (
    DurableMoviePilotEffectIndeterminateError,
    PersistentMoviePilotEffectGateway,
)
from .p7_subscription_tasks import (
    LegacyP7SubscriptionCompatibilityGateway,
    P7SubscriptionTaskRouter,
)
from .p8_collection_operations import collection_operation_claim_lease_seconds
from .p8_database_maintenance import DATABASE_MAINTENANCE_CLAIM_LEASE_SECONDS
from .p8_effect_tasks import LegacyP8EffectsCompatibilityGateway, P8EffectTaskRouter
from .p8_identity import (
    IDENTITY_OPERATION_CLAIM_LEASE_SECONDS,
    identity_operation_claim_lease_seconds,
)
from .p9_sharing import LegacyP9SharingCompatibilityGateway, P9SharingTaskRouter
from .p9_sharing_operations import SHARING_OPERATION_CLAIM_LEASE_SECONDS
from .runtime_config import TaskRuntimeConfig

logger = logging.getLogger(__name__)

_FORCE_INCREMENTAL_TASKS = frozenset({"role-translation", "enrich-aliases", "populate-metadata", "restore_mediainfo"})
_HIGH_CHAIN = "task-chain-high-freq"
_LOW_CHAIN = "task-chain-low-freq"
_WORKFLOW_DEPTH_LIMIT = 8
_NATIVE_ONLY_EXECUTABLE_TASK_KEYS = (
    P4_NATIVE_TASK_KEYS
    | P6_INGESTION_NATIVE_TASK_KEYS
    | P9_NATIVE_TASK_KEYS
    | {
        "actor-tracking",
        "actor-translation",
        "add-all-series-to-watchlist",
        "auto-subscribe",
        "backfill-media-metadata",
        "backfill-organize-record-tmdb-ids",
        "check-expired-users",
        "custom-collections",
        "execute-auto-tagging-rules",
        "enrich-aliases",
        "fill-studio-images",
        "full-sync-strm",
        "generate_embeddings",
        "intro-fingerprint-backfill",
        "manual-correct-organize-records",
        "manual_subscribe_batch",
        "monitor-115-life-events",
        "play-pool-daily-speedtest",
        "process-watchlist",
        "process-single-custom-collection",
        "process_all_custom_collections",
        "populate-metadata",
        "merge-duplicate-actors",
        "purge-ghost-actors",
        CLEANUP_EXECUTION_TASK_KEY,
        "repair-p115-fingerprints",
        "recalculate_library_washing_priorities",
        "resubscribe-library",
        "refresh-collections",
        "refresh_completed_series",
        "restore_mediainfo",
        "restore-nfo-and-images",
        "role-translation",
        "scan-cleanup-issues",
        "scan-organize-115",
        "scan_old_seasons_backfill",
        "subscribe-assistant-maintenance",
        "sync-115-directory-tree",
        "sync-all-user-data",
        "system-auto-update",
        "update-daily-theme",
        "update-resubscribe-cache",
    }
)


def _app_setting_flag(repository: SqlAlchemyAppSettingsRepository, key: str, field: str) -> bool:
    value = repository.load(key)
    return bool(value.get(field, False)) if isinstance(value, Mapping) else False


def _moviepilot_assistant_config(repository: MoviePilotWebhookSettings) -> Mapping[str, Any]:
    mp_value = repository.load("mp_config")
    watchlist_value = repository.load("watchlist_config")
    mp = mp_value if isinstance(mp_value, Mapping) else {}
    watchlist = watchlist_value if isinstance(watchlist_value, Mapping) else {}
    assistant = mp.get("subscribe_assistant")
    if not isinstance(assistant, Mapping):
        assistant = watchlist.get("subscribe_assistant")
    return assistant if isinstance(assistant, Mapping) else {}


def _moviepilot_full_washing_guard(repository: MoviePilotWebhookSettings) -> bool:
    assistant = _moviepilot_assistant_config(repository)
    value = assistant.get("best_version_full_consistency_check_enabled")
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


def _moviepilot_full_washing_timeout_hours(repository: MoviePilotWebhookSettings) -> int:
    value = _moviepilot_assistant_config(repository).get("full_washing_timeout_hours", 72)
    if isinstance(value, bool):
        return 72
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 72


def _moviepilot_snapshot_retention_days(repository: MoviePilotWebhookSettings) -> int:
    value = _moviepilot_assistant_config(repository).get("snapshot_retention_days", 180)
    if isinstance(value, bool):
        return 180
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return 180


def _p115_native_operation_router(
    database: Any,
    credentials: SqlAlchemyP115CredentialRepository,
    app_settings: SqlAlchemyAppSettingsRepository,
    settings_provider: Callable[[], Any],
) -> NativeOperationRouter:
    provider = P115PrimaryOperationsGateway(
        credentials=credentials,
        priority=lambda: settings_provider().p115_api_priority,
    )
    facts = SqlAlchemyP115MaintenanceRepository(database.unit_of_work)
    return NativeOperationRouter(
        {
            "p115.mkdir": P115MkdirOperation(provider),
            "p115.recycle-bin-empty": P115RecycleBinEmptyOperation(provider),
            "p115.empty-unrecognized": P115UnrecognizedEmptyOperation(
                provider=provider,
                facts=facts,
            ),
            "p115.moviepilot-rename-export": P115MoviePilotRenameExportOperation(
                gateway=ConfiguredMoviePilotGateway(app_settings),
                settings=app_settings,
            ),
            "p115.quick-deploy": P115QuickDeployOperation(
                settings=settings_provider,
                credentials=credentials,
                provider=provider,
                account_lock=P115QuickDeployLock(database.raw_pool.connection),
                app_settings=app_settings,
                local_configuration=SqlAlchemyP115QuickDeployConfigurationRepository(database.unit_of_work),
                emby=HttpxEmbyQuickDeployGateway(settings_provider),
            ),
        }
    )


class _DeferredWatchlistIntroGateway:
    def __init__(self, provider: Callable[[], Any]) -> None:
        self._provider = provider

    def enqueue_imported_episode_ids(
        self,
        episode_ids: Any,
        *,
        allow_playback_followup: bool = False,
    ) -> Mapping[str, Any]:
        gateway = self._provider()
        if gateway is None:
            raise RuntimeError("native intro detection gateway is unavailable")
        return gateway.enqueue_imported_episode_ids(
            episode_ids,
            allow_playback_followup=allow_playback_followup,
        )


def _native_watchlist_task_service(
    *,
    database: Any,
    settings_provider: Callable[[], Any],
    app_settings: SqlAlchemyAppSettingsRepository,
    moviepilot_gateway: ConfiguredMoviePilotGateway,
    moviepilot_effects: PersistentMoviePilotEffectGateway,
    credentials: SqlAlchemyP115CredentialRepository,
    sharing_operations: PostgresSharingOperationRepository,
    sharing_identity: SharingServerIdentity,
    intro: Any,
) -> NativeWatchlistTaskService:
    organize_plans = SqlAlchemyP115OrganizePlanRepository(database.unit_of_work)
    organize_executions = SqlAlchemyP115OrganizeExecutionRepository(database.unit_of_work)
    plan_service = P115OrganizePlanService(
        settings=settings_provider,
        credentials=credentials,
        directories=HttpxP115DirectoryTreeGateway(
            credentials=credentials,
            priority=lambda: str(settings_provider().p115_api_priority),
        ),
        sorting_rules=SqlAlchemyP115SortingRulesRepository(database.unit_of_work),
        rename_config=SqlAlchemyP115RenameConfigRepository(database.unit_of_work),
        episode_regex=SqlAlchemyP115EpisodeRegexRepository(database.unit_of_work),
        repository=organize_plans,
        recognition=ExplicitMoviePilotRecognitionGateway(),
        naming=MoviePilotOriginalNameGateway(),
    )
    execution_service = P115OrganizeExecutionService(
        plans=organize_plans,
        repository=organize_executions,
        mutations=HttpxP115OrganizeMutationGateway(
            credentials=credentials,
            priority=lambda: str(settings_provider().p115_api_priority),
        ),
    )
    return NativeWatchlistTaskService(
        SqlAlchemyWatchlistTaskRepository(database.unit_of_work),
        HttpxWatchlistMetadataAdapter(settings_provider),
        HttpxWatchlistLibraryAdapter(settings_provider),
        library_ids=lambda: tuple(settings_provider().libraries_to_process),
        effects=NativeWatchlistTaskEffects(
            database.unit_of_work,
            settings_provider,
            moviepilot=NativeWatchlistMoviePilotGateway(
                database.unit_of_work,
                app_settings,
                moviepilot_gateway,
                moviepilot_effects,
            ),
            p115=NativeWatchlistP115ReclassificationGateway(
                database.unit_of_work,
                plan_service,
                execution_service,
                organize_executions,
            ),
            sharing=NativeWatchlistSharedMetadataGateway(
                database.unit_of_work,
                app_settings,
                sharing_operations,
                sharing_identity.server_id_hash,
            ),
            intro=intro,
        ),
    )


def _native_resubscribe_task_service(
    *,
    database: Any,
    app_settings: SqlAlchemyAppSettingsRepository,
    hdhive: HttpxHDHiveAdapter,
    sharing_operations: PostgresSharingOperationRepository,
    sharing_executor: SharingOperationExecutor,
    cleanup: PersistentResubscribeCleanupGateway,
    worker_id: str,
) -> NativeResubscribeTaskService:
    policy = ConfiguredResubscribePolicy(app_settings)
    return NativeResubscribeTaskService(
        SqlAlchemyResubscribeExecutionRepository(
            database.unit_of_work,
            SqlAlchemyResubscribeMediaProjection(),
        ),
        SqlAlchemyResubscribeRepository(
            database.unit_of_work,
            SqlAlchemyResubscribeMediaProjection(),
            PostgresResubscribeOperationStager(),
            policy,
        ),
        cleanup,
        NativeResubscribeHDHiveGateway(
            hdhive,
            sharing_operations,
            sharing_executor,
            lambda: app_settings.load("hdhive_config"),
            worker_id=worker_id,
            lease_seconds=SHARING_OPERATION_CLAIM_LEASE_SECONDS,
        ),
        PostgresSubscriptionRuntimeRepository(database.unit_of_work),
        policy,
    )


def _moviepilot_transfer_processor(
    *,
    database: Any,
    settings_provider: Callable[[], Any],
    credentials: SqlAlchemyP115CredentialRepository,
    media_info: P115StrmMediaInfoEnrichmentService,
    plan_repository: SqlAlchemyP115OrganizePlanRepository,
    execution_repository: SqlAlchemyP115OrganizeExecutionRepository,
    jobs: Callable[[], JobSubmissionService],
) -> NativeMoviePilotTransferProcessor:
    def priority() -> str:
        return str(settings_provider().p115_api_priority)

    return NativeMoviePilotTransferProcessor(
        credentials=credentials,
        remote=P115PrimaryOperationsGateway(
            credentials=credentials,
            priority=priority,
        ),
        media_info=media_info,
        plans=P115OrganizePlanService(
            settings=settings_provider,
            credentials=credentials,
            directories=HttpxP115DirectoryTreeGateway(
                credentials=credentials,
                priority=priority,
            ),
            sorting_rules=SqlAlchemyP115SortingRulesRepository(database.unit_of_work),
            rename_config=SqlAlchemyP115RenameConfigRepository(database.unit_of_work),
            episode_regex=SqlAlchemyP115EpisodeRegexRepository(database.unit_of_work),
            repository=plan_repository,
            recognition=ExplicitMoviePilotRecognitionGateway(),
            naming=MoviePilotOriginalNameGateway(),
        ),
        executions=P115OrganizeExecutionService(
            plans=plan_repository,
            repository=execution_repository,
            mutations=HttpxP115OrganizeMutationGateway(
                credentials=credentials,
                priority=priority,
            ),
        ),
        execution_repository=execution_repository,
        jobs=jobs,
    )


class _CooperativeTimeout(RuntimeError):
    pass


class _TaskReportedFailure(RuntimeError):
    pass


class WorkerShutdownRequested(RuntimeError):
    """Reject a delivery that raced with process shutdown."""


class NativeTaskExecutionUnavailable(RuntimeError):
    """A native envelope has no processor-free execution owner yet."""


class _ExecutionControl:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cancel_requested = False
        self._timed_out = False
        self._inflight_effects: list[str] = []
        self._committed_effects: list[str] = []

    def request_cancel(self) -> None:
        with self._lock:
            self._cancel_requested = True

    def request_timeout(self) -> None:
        with self._lock:
            self._timed_out = True

    def record_started_effect(self, label: str) -> None:
        normalized = str(label or "external-effect").strip()[:200] or "external-effect"
        with self._lock:
            if normalized not in self._inflight_effects:
                self._inflight_effects.append(normalized)

    def record_committed_effect(self, label: str) -> None:
        normalized = str(label or "external-effect").strip()[:200] or "external-effect"
        with self._lock:
            if normalized in self._inflight_effects:
                self._inflight_effects.remove(normalized)
            if normalized not in self._committed_effects:
                self._committed_effects.append(normalized)

    def record_abandoned_effect(self, label: str) -> None:
        normalized = str(label or "external-effect").strip()[:200] or "external-effect"
        with self._lock:
            if normalized in self._inflight_effects:
                self._inflight_effects.remove(normalized)

    def checkpoint(self) -> None:
        state = self.state()
        if not state["cancel_requested"] and not state["timed_out"]:
            return
        if state["committed_effects"]:
            reason = "deadline" if state["timed_out"] else "cancellation"
            raise PersistentExecutionInterruptedAfterEffect(
                f"{reason} was observed after confirmed effects: {', '.join(state['committed_effects'])}"
            )
        if state["inflight_effects"]:
            raise PersistentEffectIndeterminate(
                "execution control was observed while external effects were in flight: "
                + ", ".join(state["inflight_effects"])
            )
        if state["timed_out"]:
            raise PersistentExecutionTimedOut("persistent operation exceeded its cooperative deadline")
        raise PersistentExecutionCancelled("persistent operation cancellation was requested")

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "cancel_requested": self._cancel_requested,
                "timed_out": self._timed_out,
                "inflight_effects": list(self._inflight_effects),
                "committed_effects": list(self._committed_effects),
            }


class RuntimeLegacyTaskResolver:
    """Resolve executable state only inside an initialized Worker process."""

    def resolve(self, task_key: str, processor: ProcessorIdentity):
        import extensions

        try:
            task_function, _display_name, registered_processor = extensions.TASK_REGISTRY[task_key]
        except (KeyError, TypeError, ValueError) as exc:
            raise LookupError(f"legacy task registry does not contain {task_key!r}") from exc
        if str(registered_processor) != processor.value:
            raise RuntimeError(f"legacy task {task_key!r} processor identity changed at runtime")
        resolved_processor = _processor(processor)
        if resolved_processor is None:
            raise RuntimeError(f"{processor.value} processor is unavailable")
        return task_function, resolved_processor


@dataclass(slots=True)
class _ExecutionProgress:
    store: PostgresJobStore
    job_id: UUID
    attempt_id: UUID
    worker_id: str
    lease_seconds: int
    control: _ExecutionControl
    task_key: str = ""
    accept_succeeded_terminal_update: bool = False
    _last_reported_progress: int | None = field(default=None, init=False, repr=False)
    _reported_failures: list[str] = field(default_factory=list, init=False, repr=False)
    _failure_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def update(self, progress: int, message: str) -> None:
        normalized_progress = None if progress < 0 else max(0, min(100, int(progress)))
        self._last_reported_progress = normalized_progress
        if progress < 0:
            with self._failure_lock:
                self._reported_failures.append(str(message))
        try:
            self.store.heartbeat(
                job_id=self.job_id,
                attempt_id=self.attempt_id,
                worker_id=self.worker_id,
                progress=normalized_progress,
                message=str(message),
                log_entry={
                    "timestamp": datetime.now(UTC).isoformat(),
                    "level": "error" if progress < 0 else "info",
                    "message": str(message),
                },
                lease_seconds=self.lease_seconds,
            )
        except InvalidStateTransitionError:
            if not self._is_succeeded_terminal_update():
                raise

    def _is_succeeded_terminal_update(self) -> bool:
        if not self.accept_succeeded_terminal_update:
            return False
        projection = self.store.get_job(self.job_id)
        attempt = None if projection is None else projection.current_attempt
        return bool(
            projection is not None
            and projection.job.status is JobStatus.SUCCEEDED
            and attempt is not None
            and attempt.id == self.attempt_id
            and attempt.status is AttemptStatus.SUCCEEDED
        )

    def raise_if_reported_failure(self) -> None:
        with self._failure_lock:
            if not self._reported_failures:
                return
            latest = self._reported_failures[-1]
        raise _TaskReportedFailure(latest or "legacy task reported failure")

    def success_reported(self) -> bool:
        return self._last_reported_progress == 100

    def checkpoint(self) -> None:
        projection = self.store.get_job(self.job_id)
        if projection is not None:
            parent_job_id = projection.job.parent_job_id
            parent = self.store.get_job(parent_job_id) if parent_job_id is not None else None
            if projection.job.status in {JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED} or (
                parent is not None and parent.job.status in {JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED}
            ):
                self.control.request_cancel()
        self.control.checkpoint()

    def record_started_effect(self, label: str) -> None:
        self.control.record_started_effect(label)

    def record_committed_effect(self, label: str) -> None:
        self.control.record_committed_effect(label)

    def record_abandoned_effect(self, label: str) -> None:
        self.control.record_abandoned_effect(label)

    def control_state(self) -> dict[str, Any]:
        return {"persistent": True, "job_id": str(self.job_id), **self.control.state()}


class _HeartbeatMonitor:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        projection: JobProjection,
        worker_id: str,
        config: TaskRuntimeConfig,
        processor: Any,
        control: _ExecutionControl,
        terminate_process: Callable[[], None],
        prepare_post_effect_cancellation: Callable[[UUID], bool] | None = None,
    ) -> None:
        self._store = store
        self._projection = projection
        self._worker_id = worker_id
        self._config = config
        self._processor = processor
        self._control = control
        self._terminate_process = terminate_process
        self._prepare_post_effect_cancellation = prepare_post_effect_cancellation
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="etk-job-heartbeat", daemon=True)
        self.cancel_requested = False
        self.timed_out = False
        self.error: BaseException | None = None
        self._started_monotonic = time.monotonic()
        self._control_requested_at: float | None = None
        self.stop_reason: str | None = None
        self.terminalized = False

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._config.heartbeat_seconds * 2))
        if self._thread.is_alive():
            raise RuntimeError("job heartbeat monitor did not stop")
        self._signal_timeout_if_due()
        if self.error is not None:
            raise RuntimeError("job heartbeat monitor failed") from self.error

    def _run(self) -> None:
        try:
            next_heartbeat = self._started_monotonic + self._config.heartbeat_seconds
            while True:
                now = time.monotonic()
                self._signal_process_shutdown(now)
                self._signal_timeout_if_due(now)
                if self._terminate_if_grace_expired(now):
                    return
                wake_at = next_heartbeat
                timeout = self._projection.job.timeout_seconds
                if timeout is not None and self.stop_reason is None:
                    wake_at = min(wake_at, self._started_monotonic + timeout)
                if self._control_requested_at is not None:
                    wake_at = min(
                        wake_at,
                        self._control_requested_at + self._config.job_termination_grace_seconds,
                    )
                if self._stop.wait(max(0.0, wake_at - now)):
                    return

                now = time.monotonic()
                self._signal_process_shutdown(now)
                self._signal_timeout_if_due(now)
                if self._terminate_if_grace_expired(now):
                    return
                if now < next_heartbeat:
                    continue

                result = self._store.heartbeat(
                    job_id=self._projection.job.id,
                    attempt_id=self._projection.current_attempt.id,
                    worker_id=self._worker_id,
                    lease_seconds=self._config.lease_seconds,
                )
                parent_cancel_requested = False
                parent_job_id = self._projection.job.parent_job_id
                if parent_job_id is not None:
                    parent = self._store.get_job(parent_job_id)
                    parent_cancel_requested = bool(
                        parent is not None and parent.job.status in {JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED}
                    )
                if result.cancel_requested or parent_cancel_requested:
                    self._request_control("cancel", now)
                next_heartbeat = now + self._config.heartbeat_seconds
        except BaseException as exc:
            self.error = exc
            _signal_stop(self._processor)

    def _signal_timeout_if_due(self, now: float | None = None) -> None:
        timeout = self._projection.job.timeout_seconds
        if timeout is None or self.stop_reason is not None:
            return
        observed_at = now if now is not None else time.monotonic()
        if observed_at - self._started_monotonic < timeout:
            return
        self._request_control("timeout", observed_at)

    def _signal_process_shutdown(self, now: float) -> None:
        if not worker_shutdown_requested() or self.stop_reason is not None:
            return
        self._store.request_cancel(self._projection.job.id)
        self._request_control("cancel", now)

    def _request_control(self, reason: str, now: float) -> None:
        if self.stop_reason is not None:
            return
        self.stop_reason = reason
        self._control_requested_at = now
        if reason == "timeout":
            self.timed_out = True
            self._control.request_timeout()
        else:
            self.cancel_requested = True
            self._control.request_cancel()
        _signal_stop(self._processor)

    def _terminate_if_grace_expired(self, now: float) -> bool:
        requested_at = self._control_requested_at
        if requested_at is None:
            return False
        if now - requested_at < self._config.job_termination_grace_seconds:
            return False
        self._terminalize_controlled_attempt()
        self.terminalized = True
        self._stop.set()
        self._terminate_process()
        return True

    def _terminalize_controlled_attempt(self) -> None:
        projection = self._projection
        attempt = projection.current_attempt
        if (
            self.stop_reason == "cancel"
            and self._prepare_post_effect_cancellation is not None
            and self._prepare_post_effect_cancellation(projection.job.id)
        ):
            retryable = attempt.attempt_no < projection.job.max_attempts
            self._store.fail_attempt(
                job_id=projection.job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type="CleanupExecutionPending",
                error_message="cleanup cancellation requires authoritative readback",
                error_details={"control": self._control.state()},
                retryable=retryable,
                next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                honor_cancel_request=False,
            )
            return
        control_state = self._control.state()
        effect_failure = _effect_interruption_failure(control_state, self.stop_reason or "cancellation")
        if effect_failure is not None:
            safe_error = _error_summary(effect_failure)
            self._store.fail_attempt(
                job_id=projection.job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type=type(effect_failure).__name__,
                error_message=str(safe_error["message"]),
                error_details={**safe_error, "control": control_state},
                retryable=False,
                honor_cancel_request=False,
            )
            return
        if self.stop_reason == "timeout":
            retryable = attempt.attempt_no < projection.job.max_attempts
            self._store.timeout_attempt(
                job_id=projection.job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_message="job exceeded its execution deadline and did not stop within the grace period",
                error_details={"control": control_state},
                retryable=retryable,
                next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                honor_cancel_request=False,
            )
            return
        self._store.cancel_attempt(
            job_id=projection.job.id,
            attempt_id=attempt.id,
            worker_id=self._worker_id,
            reason="job did not stop within the cancellation grace period",
        )


class WorkerRuntime:
    def __init__(
        self,
        *,
        config: TaskRuntimeConfig | None = None,
        sleep: Callable[[float], None] = time.sleep,
        terminate_process: Callable[[], None] | None = None,
    ) -> None:
        self.config = config or TaskRuntimeConfig.from_environ()
        self._sleep = sleep
        self._terminate_process = terminate_process or _terminate_current_worker_process
        self._lock = threading.RLock()
        self._started = False
        self._container: Any = None
        self._config_adapter: Any = None
        self._last_settings: dict[str, Any] | None = None
        self._supervisor: RuntimeSupervisor | None = None
        self._store: PostgresJobStore | None = None
        self._registry: TaskRegistry | None = None
        self._service: JobSubmissionService | None = None
        self._adapter: LegacyTaskAdapter | None = None
        self._native_only = False
        self._p115_credentials: SqlAlchemyP115CredentialRepository | None = None
        self._media_effects: PersistentMediaEffectExecutor | None = None
        self._p5_media_tasks = P5MediaTaskRouter(compatibility=None)
        self._p6_p115_tasks: P6P115TaskRouter | None = None
        self._p6_ingestion_tasks: P6IngestionTaskRouter | None = None
        self._p7_subscription_tasks: P7SubscriptionTaskRouter | None = None
        self._p8_effect_tasks: P8EffectTaskRouter | None = None
        self._p9_sharing_tasks: P9SharingTaskRouter | None = None
        self._native_operations: NativeOperationRouter | None = None
        self._p7_cleanup_executor: CleanupExecutionService | None = None
        self._p7_cleanup_repository: SqlAlchemyCleanupExecutionRepository | None = None
        self._ingestion_executor: IngestionMonitorExecutor | None = None
        self._ingestion_binding: PersistentPendingBindingGateway | None = None
        self._ingestion_ingress: PersistentIngestionIngressGateway | None = None
        self._p115_upload_executor: P115UploadOperationExecutor | None = None
        self._p115_playback_executor: P115PlaybackOperationExecutor | None = None
        self._p115_intro_executor: P115IntroDetectionOperationExecutor | None = None
        self._subscription_executor: SubscriptionOperationExecutor | None = None
        self._collection_operation_executor: CollectionOperationExecutor | None = None
        self._identity_executor: IdentityOperationExecutor | None = None
        self._database_maintenance_executor: DatabaseMaintenanceExecutor | None = None
        self._sharing_operation_executor: SharingOperationExecutor | None = None
        self._playback_gateway: NativePlaybackCompatibilityGateway | None = None
        self._intro_detection_gateway: NativeP115IntroDetectionGateway | None = None
        self._p115_upload_source_cleanup: NativeP115UploadSourceCleanupGateway | None = None
        self._gateway: PersistentTaskManagerGateway | None = None
        self._background_gateway: PersistentBackgroundGateway | None = None
        self._final_refresh_settings_provider: Callable[[], Any] | None = None
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._workflow_depth = threading.local()
        self._active_executions: dict[UUID, tuple[Any, _ExecutionControl]] = {}
        self._active_ingestion_executions = 0
        self._active_p115_upload_executions = 0
        self._active_p115_playback_executions = 0
        self._active_p115_intro_executions = 0
        self._active_subscription_executions = 0
        self._active_collection_operation_executions = 0
        self._active_identity_executions = 0
        self._active_database_maintenance_executions = 0
        self._active_sharing_operation_executions = 0
        self._active_media_effect_executions = 0
        self._idle = threading.Event()
        self._idle.set()
        self._shutdown_requested = threading.Event()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            os.environ["ETK_PROCESS_ROLE"] = "worker"
            if not _legacy_compatibility_enabled():
                self._start_native_only()
                return
            container, config_adapter = _create_default_container(None)
            supervisor = RuntimeSupervisor()
            background_gateway: PersistentBackgroundGateway | None = None
            submission_gateway_installed = False
            media_effect_gateway_installed = False
            ingestion_binding_gateway_installed = False
            ingestion_ingress_gateway_installed = False
            final_refresh_settings_provider_installed = False
            upload_source_cleanup_installed = False
            playback_gateway_installed = False
            intro_detection_gateway_installed = False
            p115_credential_gateway_installed = False
            ingestion_binding: PersistentPendingBindingGateway | None = None
            upload_source_cleanup: NativeP115UploadSourceCleanupGateway | None = None
            intro_detection_gateway: NativeP115IntroDetectionGateway | None = None
            try:
                settings_provider = config_adapter.load
                database = container.database
                cleanup_repository = SqlAlchemyCleanupExecutionRepository(database.unit_of_work)
                store = PostgresJobStore(
                    lambda: database.unit_of_work(),
                    execution_guard=PostgresExecutionGuard(database.raw_pool),
                    cancellation_policy=cleanup_repository,
                )
                registry = TaskRegistry.from_fixture(_task_fixture())
                service = JobSubmissionService(store, registry)
                app_settings = SqlAlchemyAppSettingsRepository(database.unit_of_work)
                p115_credentials = SqlAlchemyP115CredentialRepository(
                    database.unit_of_work,
                    on_change=invalidate_legacy_p115_credential_clients,
                )
                native_operations = _p115_native_operation_router(
                    database,
                    p115_credentials,
                    app_settings,
                    settings_provider,
                )
                install_persistent_p115_credential_gateway(p115_credentials)
                p115_credential_gateway_installed = True
                washing_priority_repository = SqlAlchemyP115WashingPriorityRepository(database.unit_of_work)
                washing_recalculation = P115WashingPriorityRecalculationService(
                    repository=SqlAlchemyP115WashingRecalculationRepository(database.unit_of_work),
                    state=washing_priority_repository,
                    settings=settings_provider,
                    episode_runtime=HttpxP115EpisodeRuntimeGateway(settings_provider),
                )
                cleanup_scan = CleanupScanService(
                    SqlAlchemyCleanupScanRepository(
                        database.unit_of_work,
                        SqlAlchemyCleanupMediaProjection(),
                    ),
                    HttpxCleanupEmbyInventoryGateway(settings_provider),
                    settings_provider,
                )
                subscription_operations = PostgresSubscriptionOperationRepository(database.unit_of_work)
                moviepilot_gateway = ConfiguredMoviePilotGateway(app_settings)
                subscription_executor = SubscriptionOperationExecutor(
                    subscription_operations,
                    moviepilot_gateway,
                )
                identity_operations = PostgresIdentityOperationRepository(database.unit_of_work)
                identity_effects = HttpxEmbyIdentityEffectsAdapter(settings_provider)
                identity_registration = RegistrationSagaCoordinator(
                    identity_operations,
                    identity_effects,
                )
                identity_mutations = IdentityUserMutationCoordinator(
                    identity_operations,
                    identity_effects,
                )
                identity_executor = IdentityOperationExecutor(
                    identity_registration,
                    identity_mutations,
                    identity_operations,
                )
                database_maintenance_repository = PostgresDatabaseMaintenanceRepository(database.unit_of_work)
                database_maintenance_executor = DatabaseMaintenanceExecutor(
                    database_maintenance_repository,
                    unit_of_work=database.unit_of_work,
                    worker_id=f"{self._worker_id}:database-maintenance",
                    lease_seconds=DATABASE_MAINTENANCE_CLAIM_LEASE_SECONDS,
                )
                moviepilot_effects = PersistentMoviePilotEffectGateway(
                    subscription_operations,
                    subscription_executor,
                    app_settings,
                    lease_seconds=SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS,
                )
                manual_watchlist = SqlAlchemyWatchlistMediaProjectionRepository(database.unit_of_work)
                manual_batch = ManualSubscriptionBatchService(
                    SqlAlchemyManualSubscriptionRepository(
                        database.unit_of_work,
                        SqlAlchemySubscriptionMediaProjection(),
                    ),
                    app_settings,
                    HttpxManualSubscriptionTmdbAdapter(settings_provider),
                    moviepilot_effects,
                    ManualSubscriptionQuotaLedger(PostgresSubscriptionRuntimeRepository(database.unit_of_work)),
                    manual_watchlist,
                )
                resubscribe_cleanup = PersistentResubscribeCleanupGateway(
                    SqlAlchemyCleanupRepository(
                        database.unit_of_work,
                        SqlAlchemyCleanupMediaProjection(),
                        SettingsCleanupExecutionPolicy(settings_provider, p115_credentials),
                    )
                )
                daily_theme = DailyThemeService(
                    HttpxDiscoverProviderAdapter(settings_provider),
                    SqlAlchemyDailyThemeRepository(database.unit_of_work),
                    LegacyDiscoverDefaults(),
                )
                auto_tagging_delivery = HttpxAutoTaggingAdapter(
                    settings_provider,
                    item_lock=PostgresAutoTaggingItemLock(database.engine),
                )
                studio_images_delivery = HttpxStudioImageAdapter(settings_provider)
                sharing_identity = SharingServerIdentity(settings_provider, app_settings)
                shared_center = HttpxSharedCenterAdapter(
                    settings_provider,
                    configuration=lambda: app_settings.load("shared_resource_config"),
                    server_id_hash=sharing_identity.server_id_hash,
                )
                hdhive = HttpxHDHiveAdapter(
                    settings_provider,
                    configuration=lambda: app_settings.load("hdhive_config"),
                )
                telegram = HttpxTelegramBotAdapter(settings_provider)
                telegram_resource_secrets = TelegramResourceSecretStore(
                    PostgresRuntimeStateBackend(database.raw_pool.connection)
                )
                telegram_userbot = DurableTelegramUserBotAdapter(
                    TelegramControlBridge(
                        connection_factory=database.raw_pool.connection,
                        resource_secrets=telegram_resource_secrets,
                    )
                )
                p115_mediainfo_repository = SqlAlchemyP115MediaInfoRepository(database.unit_of_work)
                sharing_operations = PostgresSharingOperationRepository(database.unit_of_work)
                cleanup_source_lifecycle = SharingService(
                    PostgresContributionLedger(database.unit_of_work),
                    shared_center,
                    sharing_operations,
                    unit_of_work=database.unit_of_work,
                    projection=SqlAlchemySharingProjectionRepository(database.unit_of_work),
                )
                cleanup_executor = CleanupExecutionService(
                    cleanup_repository,
                    local=SafeLocalCleanupGateway(),
                    media=SqlAlchemyCleanupMediaGateway(
                        database.unit_of_work,
                        cleanup_source_lifecycle,
                    ),
                    p115=NativeCleanupP115Gateway(
                        P115PrimaryOperationsGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                        )
                    ),
                    emby=ConfiguredCleanupEmbyGateway(settings_provider),
                    lease_seconds=max(self.config.lease_seconds, 60),
                )
                virtual_imports = VirtualImportService(
                    resolver=VirtualImportSourceResolver(shared_center),
                    repository=SqlAlchemyVirtualImportRepository(database.unit_of_work),
                    projector=AtomicVirtualImportProjector(
                        settings=settings_provider,
                        libraries=AtomicP115LocalStrmLibraryGateway(),
                    ),
                    configuration=lambda: app_settings.load("shared_resource_config"),
                )
                auto_transfer_delivery = AutoSubscriptionTransferOperationDelivery(
                    repository=sharing_operations,
                    hdhive=hdhive,
                    telegram=telegram_userbot,
                    p115=NativeP115ResourceTransferGateway(p115_credentials, settings_provider),
                )
                sharing_operation_executor = SharingOperationExecutor(
                    sharing_operations,
                    SharingProviderOperationDelivery(
                        shared_center=shared_center,
                        hdhive=hdhive,
                        telegram=telegram,
                        raw_ffprobe_loader=partial(_load_sharing_raw_ffprobe, p115_mediainfo_repository),
                        virtual_imports=VirtualImportOperationDelivery(
                            virtual_imports,
                            HttpxEmbyVirtualItemGateway(settings_provider),
                        ),
                        rapid_transfers=RapidTransferOperationDelivery(
                            repository=sharing_operations,
                            virtual_imports=virtual_imports,
                            credentials=p115_credentials,
                            settings=settings_provider,
                            center=shared_center,
                            p115=NativeP115RapidTransferGateway(
                                credentials=p115_credentials,
                                priority=lambda: str(settings_provider().p115_api_priority),
                                target_cid=lambda: str(settings_provider().p115_save_path_cid),
                            ),
                        ),
                        extra_deliveries=(
                            LogicalSeasonShareTransferOperationDelivery(
                                repository=sharing_operations,
                                virtual_imports=virtual_imports,
                                credentials=p115_credentials,
                                settings=settings_provider,
                                center=shared_center,
                                p115=NativeP115LogicalSeasonShareGateway(
                                    credentials=p115_credentials,
                                    priority=lambda: str(settings_provider().p115_api_priority),
                                    target_cid=lambda: str(settings_provider().p115_save_path_cid),
                                ),
                            ),
                            auto_transfer_delivery,
                            HDHiveRegistrationOperationDelivery(hdhive, app_settings),
                        ),
                    ),
                    max_attempts=4,
                )
                auto_tmdb = HttpxManualSubscriptionTmdbAdapter(settings_provider)
                auto_subscribe = AutoSubscribeService(
                    SqlAlchemyAutoSubscriptionRepository(
                        database.unit_of_work,
                        SqlAlchemySubscriptionMediaProjection(),
                    ),
                    app_settings,
                    auto_tmdb,
                    ManualSubscriptionQuotaLedger(PostgresSubscriptionRuntimeRepository(database.unit_of_work)),
                    AutoSubscriptionMoviePilotGateway(moviepilot_effects, app_settings, auto_tmdb),
                    PersistentOrderedAutoSubscriptionSources(
                        center=shared_center,
                        hdhive=hdhive,
                        telegram=telegram_userbot,
                        operations=sharing_operations,
                        executor=sharing_operation_executor,
                        virtual_imports=virtual_imports,
                        shared_configuration=lambda: app_settings.load("shared_resource_config"),
                        telegram_configuration=lambda: app_settings.load("tg_userbot_config"),
                        worker_id=self._worker_id,
                        lease_seconds=SHARING_OPERATION_CLAIM_LEASE_SECONDS,
                    ),
                    TelegramAutoSubscriptionNotifications(
                        SqlAlchemyAutoSubscriptionNotificationDirectory(database.unit_of_work),
                        telegram,
                    ),
                )
                media_task_service, people_task_service, screenshot_backfill = _native_media_people_task_services(
                    database,
                    settings_provider,
                )
                p7_subscription_tasks = P7SubscriptionTaskRouter(
                    compatibility=LegacyP7SubscriptionCompatibilityGateway(),
                    assistant=SubscribeAssistantMaintenanceService(
                        SqlAlchemyAssistantMaintenanceRepository(database.unit_of_work),
                        NativeSubscribeAssistantMaintenanceGateway(
                            database.unit_of_work,
                            provider=moviepilot_gateway,
                            effects=moviepilot_effects,
                            configuration=lambda: _moviepilot_assistant_config(app_settings),
                        ),
                    ),
                    moviepilot_effects=moviepilot_effects,
                    resubscribe_cleanup=resubscribe_cleanup,
                    washing_recalculation=washing_recalculation,
                    old_season_backfill=OldSeasonBackfillService(
                        SqlAlchemyWatchlistMediaProjectionRepository(database.unit_of_work)
                    ),
                    manual_batch=manual_batch,
                    auto_subscribe=auto_subscribe,
                    people_tasks=people_task_service,
                    watchlist_tasks=_native_watchlist_task_service(
                        database=database,
                        settings_provider=settings_provider,
                        app_settings=app_settings,
                        moviepilot_gateway=moviepilot_gateway,
                        moviepilot_effects=moviepilot_effects,
                        credentials=p115_credentials,
                        sharing_operations=sharing_operations,
                        sharing_identity=sharing_identity,
                        intro=_DeferredWatchlistIntroGateway(lambda: self._intro_detection_gateway),
                    ),
                    resubscribe_tasks=_native_resubscribe_task_service(
                        database=database,
                        app_settings=app_settings,
                        hdhive=hdhive,
                        sharing_operations=sharing_operations,
                        sharing_executor=sharing_operation_executor,
                        cleanup=resubscribe_cleanup,
                        worker_id=self._worker_id,
                    ),
                    resubscribe_cache=NativeResubscribeCacheTaskService(
                        SqlAlchemyResubscribeCacheRepository(database.unit_of_work)
                    ),
                )
                p9_sharing_tasks = P9SharingTaskRouter(
                    shared_center=shared_center,
                    hdhive=hdhive,
                    telegram=telegram,
                    compatibility=LegacyP9SharingCompatibilityGateway(),
                )
                ingestion_ledger = PostgresIngestionMonitorLedger(database.unit_of_work)
                upload_repository = PostgresP115UploadMonitorRepository(database.unit_of_work)
                upload_config = P115UploadMonitorConfigService(
                    upload_repository,
                    lambda: config_adapter.load().monitor_extensions,
                )
                upload_source_cleanup = NativeP115UploadSourceCleanupGateway(
                    upload_repository,
                    upload_config.get,
                )
                ingestion_ingress = PersistentIngestionIngressGateway(
                    settings=settings_provider,
                    ledger=ingestion_ledger,
                )
                p6_ingestion_tasks = P6IngestionTaskRouter(
                    P6IngestionScanService(
                        settings=settings_provider,
                        ingress=ingestion_ingress,
                        known_paths=ingestion_ledger,
                    )
                )
                playback_repository = PostgresP115PlaybackRepository(database.unit_of_work)
                playback_users = SqlAlchemyP115PlaybackUserGateway(
                    SqlAlchemyP115PlaybackRewardRepository(database.unit_of_work),
                    telegram,
                )
                playback_config = P115PlaybackConfigService(
                    SqlAlchemyP115PlaybackConfigBackend(database.unit_of_work),
                    playback_users,
                )
                playback_executor = P115PlaybackOperationExecutor(
                    repository=playback_repository,
                    provider=NativeP115PlaybackProvider(
                        NativeP115PoolPlaybackClientFactory(
                            playback_config,
                            primary_credentials=p115_credentials,
                        ),
                        NativeP115PlaybackRapidSigner(
                            P115SdkDirectUrlGateway(
                                credentials=p115_credentials,
                                priority=lambda: str(settings_provider().p115_api_priority),
                            ),
                            p115_credentials,
                        ),
                    ),
                    config=playback_config,
                    worker_id=f"{self._worker_id}:p115-playback",
                )
                playback_service = P115PlaybackService(
                    config=playback_config,
                    repository=playback_repository,
                    sources=SqlAlchemyP115PlaybackSourceGateway(
                        database.unit_of_work,
                        primary_credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                    direct_links=NativeP115DirectLinkGateway(
                        playback_config,
                        p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                    executor=playback_executor,
                )
                playback_gateway = NativePlaybackCompatibilityGateway(lambda: playback_service)
                bridge_client = HttpxEmbyBridgeClient(settings_provider)
                p115_fingerprint_repair = P115FingerprintRepairService(
                    credentials=p115_credentials,
                    repository=p115_mediainfo_repository,
                    hints=LocalP115FingerprintHintGateway(),
                    remote=HttpxP115FingerprintRemoteGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                    local_root=lambda: str(config_adapter.load().local_strm_root or ""),
                )
                processes = ControlledProcessRunner()
                intro_repository = PostgresP115IntroDetectionRepository(database.unit_of_work)
                intro_source = PostgresP115IntroRuntimeSource(
                    database.unit_of_work,
                    intro_repository,
                    app_settings,
                    HttpxEmbyIntroFavoriteGateway(settings_provider),
                )
                intro_detection_gateway = NativeP115IntroDetectionGateway(
                    submission=IntroDetectionSubmissionService(intro_repository),
                    source=intro_source,
                    account_id=lambda: p115_credentials.load().account_id,
                    provider_identity=lambda: select_intro_provider_identity(
                        p115_credentials,
                        lambda: settings_provider().p115_api_priority,
                    ),
                )
                intro_fingerprints = P115IntroFingerprintAdapter(
                    NativeP115IntroDirectUrlGateway(
                        p115_credentials,
                        P115SdkDirectUrlGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                        ),
                    ),
                    ChromaprintProcessAdapter(processes),
                )
                intro_executor = P115IntroDetectionOperationExecutor(
                    repository=intro_repository,
                    service=IntroDetectionExecutionService(
                        NativeIntroDetectionBatchEngine(
                            intro_repository,
                            intro_fingerprints,
                            EmbyAuthorizationRevisionAdapter(bridge_client),
                        ),
                        NativeIntroPolicyAdapter(intro_source),
                    ),
                    worker_id=f"{self._worker_id}:p115-intro",
                )
                mediainfo_policies = PersistentP115MediaInfoPolicyProvider(app_settings)
                media_probe = P115MediaProbeService(
                    direct_urls=P115SdkDirectUrlGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                    processes=processes,
                    iso=NativeP115IsoProbeGateway(processes),
                    policies=mediainfo_policies,
                )
                strm_mediainfo = P115StrmMediaInfoEnrichmentService(
                    credentials=p115_credentials,
                    repository=p115_mediainfo_repository,
                    probe=media_probe,
                    policies=mediainfo_policies,
                )
                organize_plans = SqlAlchemyP115OrganizePlanRepository(database.unit_of_work)
                organize_executions = SqlAlchemyP115OrganizeExecutionRepository(database.unit_of_work)
                p6_p115_tasks = P6P115TaskRouter(
                    P115DirectorySyncService(
                        settings=app_settings,
                        credentials=p115_credentials,
                        directories=HttpxP115DirectoryTreeGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                        ),
                        repository=SqlAlchemyP115DirectoryCacheRepository(database.unit_of_work),
                    ),
                    P115OrganizeExecutionService(
                        plans=organize_plans,
                        repository=organize_executions,
                        mutations=HttpxP115OrganizeMutationGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                        ),
                    ),
                    P115StrmSyncService(
                        settings=config_adapter.load,
                        credentials=p115_credentials,
                        sorting_rules=SqlAlchemyP115SortingRulesRepository(database.unit_of_work),
                        rename_config=SqlAlchemyP115RenameConfigRepository(database.unit_of_work),
                        remote=LegacyP115StrmSyncGateway(config_adapter.load),
                        libraries=AtomicP115LocalStrmLibraryGateway(),
                        repository=SqlAlchemyP115StrmSyncRepository(database.unit_of_work),
                        coordinator=PostgresP115StrmSyncCoordinator(database.engine),
                        mediainfo=strm_mediainfo,
                    ),
                    p115_fingerprint_repair,
                    playback_service,
                    P115IntroBackfillTaskService(intro_detection_gateway),
                    organize_record_tmdb_backfill=P115OrganizeRecordTmdbBackfillService(
                        SqlAlchemyP115OrganizeRecordsRepository(database.unit_of_work)
                    ),
                    organize_reconciliation=P115OrganizeExecutionService(
                        plans=organize_plans,
                        repository=organize_executions,
                        mutations=HttpxP115OrganizeReadbackGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                        ),
                    ),
                )
                from etk.compatibility.flask_app import LazyCompatibilityApplication, create_flask_app

                event_compatibility = LazyCompatibilityApplication(
                    partial(
                        create_flask_app,
                        supervisor=supervisor,
                        blueprint_specs=(("routes.webhook", "webhook_bp"),),
                    )
                )
                collection_operations = PostgresCollectionOperationRepository(database.unit_of_work)
                collection_activation = PostgresCollectionActivationRepository(
                    database.unit_of_work,
                    collection_operations,
                )
                collection_operation_executor = CollectionOperationExecutor(
                    collection_operations,
                    HttpxCollectionRemoteEffectsAdapter(settings_provider),
                    SqlAlchemyCollectionAssetEffectsAdapter(
                        database.unit_of_work,
                        settings_provider,
                        data_root=_worker_data_root(config_adapter),
                    ),
                    PersistentCollectionDeletionSuppression(
                        PersistentRuntimeStateStore(PostgresRuntimeStateBackend(database.raw_pool.connection))
                    ),
                    worker_id=f"{self._worker_id}:collection-operation",
                    lease_seconds=lambda: collection_operation_claim_lease_seconds(
                        config_adapter.load().emby_api_timeout,
                    ),
                    activation_provider=HttpxNativeCollectionRefreshAdapter(settings_provider),
                    activation_completion=collection_activation,
                )
                custom_collection_tasks = _custom_collection_task_service(
                    database,
                    settings_provider,
                    app_settings,
                    collection_operations,
                    collection_operation_executor,
                )
                user_activity_provider = HttpxEmbyUserActivityAdapter(settings_provider)
                user_event_delivery = P8UserEventEffectDelivery(
                    UserPolicyEffectService(
                        user_activity_provider,
                        SqlAlchemyIdentityRepository(database.unit_of_work),
                        SqlAlchemyTemplatePropagationRepository(database.unit_of_work),
                    ),
                    LegacyUserEventEffectsAdapter(settings_provider),
                )
                media_effects_ledger = PostgresMediaEffectLedger(database.unit_of_work)
                media_effects = PersistentMediaEffectExecutor(
                    media_effects_ledger,
                    RoutedMediaEffectDelivery(
                        [
                            BridgeMediaEffectDelivery(bridge_client),
                            EmbyMetadataRefreshEffectDelivery(settings_provider),
                            ManualImageEffectDelivery(LegacyMediaImageEditingGateway()),
                            P5MediaEventEffectDelivery(LegacyP5MediaEventGateway(event_compatibility)),
                            user_event_delivery,
                            SharedIntroExportEffectDelivery(app_settings, shared_center),
                            DailyThemeReplenishmentEffectDelivery(daily_theme),
                            EmbyPathChangeEffectDelivery(
                                settings_provider,
                                HttpxEmbyPathChangeGateway(),
                            ),
                            auto_tagging_delivery,
                            studio_images_delivery,
                            MoviePilotWebhookEffectDelivery(
                                SqlAlchemyMoviePilotAssistantEventRepository(
                                    database.unit_of_work,
                                    full_washing_guard=partial(
                                        _moviepilot_full_washing_guard,
                                        app_settings,
                                    ),
                                    active_downloads=moviepilot_gateway.active_downloads,
                                ),
                                _moviepilot_transfer_processor(
                                    database=database,
                                    settings_provider=settings_provider,
                                    credentials=p115_credentials,
                                    media_info=strm_mediainfo,
                                    plan_repository=organize_plans,
                                    execution_repository=organize_executions,
                                    jobs=lambda: service,
                                ),
                                MoviePilotFullWashingOperations(subscription_operations),
                                full_washing_timeout_hours=partial(
                                    _moviepilot_full_washing_timeout_hours,
                                    app_settings,
                                ),
                                snapshot_retention_days=partial(
                                    _moviepilot_snapshot_retention_days,
                                    app_settings,
                                ),
                            ),
                        ]
                    ),
                    worker_id=f"{self._worker_id}:media-effect",
                    lease_seconds=lambda: studio_image_claim_lease_seconds(config_adapter.load().emby_api_timeout),
                )
                p8_effect_tasks = P8EffectTaskRouter(
                    LegacyP8EffectsCompatibilityGateway(),
                    UserSyncService(
                        HttpxEmbyUserSyncAdapter(settings_provider),
                        SqlAlchemyUserSyncRepository(database.unit_of_work),
                    ),
                    cleanup_scan,
                    IdentityUserExpiryService(
                        identity_operations,
                        identity_mutations,
                        lease_seconds=lambda: identity_operation_claim_lease_seconds(
                            settings_provider().emby_api_timeout
                        ),
                    ),
                    daily_theme=daily_theme,
                    media_embeddings=MediaEmbeddingService(
                        SqlAlchemyMediaEmbeddingRepository(database.unit_of_work),
                        ConfiguredMediaEmbeddingProvider(settings_provider),
                        enabled=lambda: bool(settings_provider().ai_vector),
                    ),
                    native_collection_refresh=NativeCollectionRefreshService(
                        HttpxNativeCollectionRefreshAdapter(settings_provider),
                        SqlAlchemyNativeCollectionRefreshRepository(database.unit_of_work),
                        auto_subscribe=lambda: _app_setting_flag(
                            app_settings, "native_collections_config", "auto_sub_enabled"
                        ),
                    ),
                    auto_tagging=AutoTaggingService(
                        app_settings,
                        auto_tagging_delivery,
                        PersistentAutoTaggingEffects(media_effects),
                    ),
                    studio_images=StudioImageService(
                        app_settings,
                        studio_images_delivery,
                        PersistentStudioImageEffects(media_effects),
                    ),
                    custom_collection_tasks=custom_collection_tasks,
                )
                ingestion_binding = PersistentPendingBindingGateway(
                    ledger=ingestion_ledger,
                    delivery=LegacyIngestionBindingDelivery(
                        jobs=lambda: service,
                        uow_factory=database.unit_of_work,
                        bridge=bridge_client,
                        media_effects=media_effects,
                    ),
                    claimant_id=f"{self._worker_id}:binding",
                    lease_seconds=INGESTION_CLAIM_LEASE_SECONDS,
                )
                ingestion_executor = IngestionMonitorExecutor(
                    uow_factory=database.unit_of_work,
                    ledger=ingestion_ledger,
                    settings=settings_provider,
                    preparation=LegacyIngestionPreparationGateway(settings_provider),
                    pending_bindings=ingestion_binding,
                    media_effects=media_effects,
                    processor=lambda: _processor(ProcessorIdentity.MEDIA),
                    worker_id=f"{self._worker_id}:ingestion",
                    lease_seconds=INGESTION_CLAIM_LEASE_SECONDS,
                )

                def submit_upload_screenshot(operation: UploadOperation, _path: str) -> str:
                    submitted = service.submit_task(
                        "restore-nfo-and-images",
                        {},
                        trigger_type="upload-monitor",
                        idempotency_scope="p115-upload-screenshot",
                        idempotency_key=str(operation.id),
                        max_attempts=3,
                        silent=True,
                    )
                    return str(submitted.projection.job.id)

                p115_upload_executor = P115UploadOperationExecutor(
                    repository=upload_repository,
                    provider=LegacyP115UploadProvider(),
                    projection=LegacyP115UploadProjection(
                        ingestion=ingestion_ingress,
                        submit_screenshot=submit_upload_screenshot,
                    ),
                    worker_id=f"{self._worker_id}:p115-upload",
                )
                p5_media_tasks = P5MediaTaskRouter(
                    compatibility=LegacyP5MediaCompatibilityGateway(),
                    restore_mediainfo=P115MediaInfoRestoreService(
                        credentials=p115_credentials,
                        repository=p115_mediainfo_repository,
                        fingerprints=p115_fingerprint_repair,
                        emby=HttpxEmbyMediaInfoQueryGateway(settings_provider),
                        probe=media_probe,
                        policies=mediainfo_policies,
                        commands=PersistentMediaInfoCommandGateway(
                            database.unit_of_work,
                            bridge_client,
                            executor=media_effects,
                        ),
                    ),
                    media_tasks=media_task_service,
                    people_tasks=people_task_service,
                    screenshot_backfill=screenshot_backfill,
                )
                supervisor.start(role=RuntimeRole.WORKER, background=False)
                adapter = LegacyTaskAdapter(registry, RuntimeLegacyTaskResolver())
                gateway = PersistentTaskManagerGateway(service)
                background_gateway = PersistentBackgroundGateway(lambda: service)
                from etk.compatibility.background import install_persistent_background_gateway
                from etk.compatibility.media_effects import install_persistent_media_effect_gateway
                from task_manager import install_persistent_submission_gateway

                install_persistent_submission_gateway(gateway)
                submission_gateway_installed = True
                install_persistent_background_gateway(background_gateway)
                install_final_refresh_settings_provider(settings_provider)
                final_refresh_settings_provider_installed = True
                install_persistent_media_effect_gateway(media_effects)
                media_effect_gateway_installed = True
                install_persistent_ingestion_binding(ingestion_binding)
                ingestion_binding_gateway_installed = True
                install_persistent_ingestion_ingress(ingestion_ingress)
                ingestion_ingress_gateway_installed = True
                install_persistent_upload_source_cleanup(upload_source_cleanup)
                upload_source_cleanup_installed = True
                install_persistent_playback_gateway(playback_gateway)
                playback_gateway_installed = True
                install_persistent_intro_detection_gateway(intro_detection_gateway)
                intro_detection_gateway_installed = True
            except BaseException:
                try:
                    if p115_credential_gateway_installed:
                        clear_persistent_p115_credential_gateway(p115_credentials)
                    if intro_detection_gateway_installed and intro_detection_gateway is not None:
                        clear_persistent_intro_detection_gateway(intro_detection_gateway)
                    if playback_gateway_installed:
                        clear_persistent_playback_gateway(playback_gateway)
                    if upload_source_cleanup_installed and upload_source_cleanup is not None:
                        clear_persistent_upload_source_cleanup(upload_source_cleanup)
                    if ingestion_ingress_gateway_installed:
                        clear_persistent_ingestion_ingress(ingestion_ingress)
                    if ingestion_binding_gateway_installed and ingestion_binding is not None:
                        clear_persistent_ingestion_binding(ingestion_binding)
                    if media_effect_gateway_installed:
                        clear_persistent_media_effect_gateway(media_effects)
                    if final_refresh_settings_provider_installed:
                        clear_final_refresh_settings_provider(settings_provider)
                    if background_gateway is not None:
                        from etk.compatibility.background import clear_persistent_background_gateway

                        clear_persistent_background_gateway(background_gateway)
                    if submission_gateway_installed:
                        from task_manager import clear_persistent_submission_gateway

                        clear_persistent_submission_gateway(gateway)
                    supervisor.stop()
                finally:
                    container.close()
                raise
            self._container = container
            self._config_adapter = config_adapter
            self._last_settings = container.settings.to_runtime_dict()
            self._supervisor = supervisor
            self._store = store
            self._registry = registry
            self._service = service
            self._adapter = adapter
            self._media_effects = media_effects
            self._p5_media_tasks = p5_media_tasks
            self._p6_p115_tasks = p6_p115_tasks
            self._p6_ingestion_tasks = p6_ingestion_tasks
            self._p7_subscription_tasks = p7_subscription_tasks
            self._p8_effect_tasks = p8_effect_tasks
            self._p9_sharing_tasks = p9_sharing_tasks
            self._native_operations = native_operations
            self._p7_cleanup_executor = cleanup_executor
            self._p7_cleanup_repository = cleanup_repository
            self._ingestion_executor = ingestion_executor
            self._ingestion_binding = ingestion_binding
            self._ingestion_ingress = ingestion_ingress
            self._p115_upload_executor = p115_upload_executor
            self._p115_playback_executor = playback_executor
            self._p115_intro_executor = intro_executor
            self._subscription_executor = subscription_executor
            self._collection_operation_executor = collection_operation_executor
            self._identity_executor = identity_executor
            self._database_maintenance_executor = database_maintenance_executor
            self._sharing_operation_executor = sharing_operation_executor
            self._playback_gateway = playback_gateway
            self._intro_detection_gateway = intro_detection_gateway
            self._p115_upload_source_cleanup = upload_source_cleanup
            self._gateway = gateway
            self._background_gateway = background_gateway
            self._final_refresh_settings_provider = settings_provider
            self._native_only = False
            self._p115_credentials = p115_credentials
            self._started = True

    def _start_native_only(self) -> None:
        """Initialize the current processor-free Worker execution slice."""

        container, config_adapter = _create_default_container(None)
        supervisor: RuntimeSupervisor | None = None
        p115_credentials: SqlAlchemyP115CredentialRepository | None = None
        try:
            settings_provider = config_adapter.load
            database = container.database
            cleanup_repository = SqlAlchemyCleanupExecutionRepository(database.unit_of_work)
            store = PostgresJobStore(
                lambda: database.unit_of_work(),
                execution_guard=PostgresExecutionGuard(database.raw_pool),
                cancellation_policy=cleanup_repository,
            )
            registry = TaskRegistry.from_fixture(_task_fixture())
            service = JobSubmissionService(store, registry)
            app_settings = SqlAlchemyAppSettingsRepository(database.unit_of_work)
            collection_operations = PostgresCollectionOperationRepository(database.unit_of_work)
            collection_activation = PostgresCollectionActivationRepository(
                database.unit_of_work,
                collection_operations,
            )
            collection_operation_executor = CollectionOperationExecutor(
                collection_operations,
                HttpxCollectionRemoteEffectsAdapter(settings_provider),
                SqlAlchemyCollectionAssetEffectsAdapter(
                    database.unit_of_work,
                    settings_provider,
                    data_root=_worker_data_root(config_adapter),
                ),
                PersistentCollectionDeletionSuppression(
                    PersistentRuntimeStateStore(PostgresRuntimeStateBackend(database.raw_pool.connection))
                ),
                worker_id=f"{self._worker_id}:collection-operation",
                lease_seconds=lambda: collection_operation_claim_lease_seconds(
                    config_adapter.load().emby_api_timeout,
                ),
                activation_provider=HttpxNativeCollectionRefreshAdapter(settings_provider),
                activation_completion=collection_activation,
            )
            custom_collection_tasks = _custom_collection_task_service(
                database,
                settings_provider,
                app_settings,
                collection_operations,
                collection_operation_executor,
            )
            p115_credentials = SqlAlchemyP115CredentialRepository(
                database.unit_of_work,
                on_change=invalidate_legacy_p115_credential_clients,
            )
            native_operations = _p115_native_operation_router(
                database,
                p115_credentials,
                app_settings,
                settings_provider,
            )
            install_persistent_p115_credential_gateway(p115_credentials)
            washing_priority_repository = SqlAlchemyP115WashingPriorityRepository(database.unit_of_work)
            washing_recalculation = P115WashingPriorityRecalculationService(
                repository=SqlAlchemyP115WashingRecalculationRepository(database.unit_of_work),
                state=washing_priority_repository,
                settings=settings_provider,
                episode_runtime=HttpxP115EpisodeRuntimeGateway(settings_provider),
            )
            cleanup_scan = CleanupScanService(
                SqlAlchemyCleanupScanRepository(
                    database.unit_of_work,
                    SqlAlchemyCleanupMediaProjection(),
                ),
                HttpxCleanupEmbyInventoryGateway(settings_provider),
                settings_provider,
            )
            subscription_operations = PostgresSubscriptionOperationRepository(database.unit_of_work)
            moviepilot_gateway = ConfiguredMoviePilotGateway(app_settings)
            subscription_executor = SubscriptionOperationExecutor(
                subscription_operations,
                moviepilot_gateway,
            )
            moviepilot_effects = PersistentMoviePilotEffectGateway(
                subscription_operations,
                subscription_executor,
                app_settings,
                lease_seconds=SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS,
            )
            native_assistant = SubscribeAssistantMaintenanceService(
                SqlAlchemyAssistantMaintenanceRepository(database.unit_of_work),
                NativeSubscribeAssistantMaintenanceGateway(
                    database.unit_of_work,
                    provider=moviepilot_gateway,
                    effects=moviepilot_effects,
                    configuration=lambda: _moviepilot_assistant_config(app_settings),
                ),
            )
            manual_watchlist = SqlAlchemyWatchlistMediaProjectionRepository(database.unit_of_work)
            manual_batch = ManualSubscriptionBatchService(
                SqlAlchemyManualSubscriptionRepository(
                    database.unit_of_work,
                    SqlAlchemySubscriptionMediaProjection(),
                ),
                app_settings,
                HttpxManualSubscriptionTmdbAdapter(settings_provider),
                moviepilot_effects,
                ManualSubscriptionQuotaLedger(PostgresSubscriptionRuntimeRepository(database.unit_of_work)),
                manual_watchlist,
            )
            identity_operations = PostgresIdentityOperationRepository(database.unit_of_work)
            identity_effects = HttpxEmbyIdentityEffectsAdapter(settings_provider)
            identity_mutations = IdentityUserMutationCoordinator(identity_operations, identity_effects)
            identity_executor = IdentityOperationExecutor(
                RegistrationSagaCoordinator(identity_operations, identity_effects),
                identity_mutations,
                identity_operations,
            )
            database_maintenance_repository = PostgresDatabaseMaintenanceRepository(database.unit_of_work)
            database_maintenance_executor = DatabaseMaintenanceExecutor(
                database_maintenance_repository,
                unit_of_work=database.unit_of_work,
                worker_id=f"{self._worker_id}:database-maintenance",
                lease_seconds=DATABASE_MAINTENANCE_CLAIM_LEASE_SECONDS,
            )
            ingestion_ledger = PostgresIngestionMonitorLedger(database.unit_of_work)
            ingestion_ingress = PersistentIngestionIngressGateway(
                settings=settings_provider,
                ledger=ingestion_ledger,
            )
            p6_ingestion_tasks = P6IngestionTaskRouter(
                P6IngestionScanService(
                    settings=settings_provider,
                    ingress=ingestion_ingress,
                    known_paths=ingestion_ledger,
                )
            )
            organize_plans = SqlAlchemyP115OrganizePlanRepository(database.unit_of_work)
            organize_executions = SqlAlchemyP115OrganizeExecutionRepository(database.unit_of_work)
            p115_mediainfo_repository = SqlAlchemyP115MediaInfoRepository(database.unit_of_work)
            bridge_client = HttpxEmbyBridgeClient(settings_provider)
            sharing_identity = SharingServerIdentity(settings_provider, app_settings)
            shared_center = HttpxSharedCenterAdapter(
                settings_provider,
                configuration=lambda: app_settings.load("shared_resource_config"),
                server_id_hash=sharing_identity.server_id_hash,
            )
            hdhive = HttpxHDHiveAdapter(
                settings_provider,
                configuration=lambda: app_settings.load("hdhive_config"),
            )
            telegram = HttpxTelegramBotAdapter(settings_provider)
            telegram_resource_secrets = TelegramResourceSecretStore(
                PostgresRuntimeStateBackend(database.raw_pool.connection)
            )
            telegram_userbot = DurableTelegramUserBotAdapter(
                TelegramControlBridge(
                    connection_factory=database.raw_pool.connection,
                    resource_secrets=telegram_resource_secrets,
                )
            )
            playback_users = SqlAlchemyP115PlaybackUserGateway(
                SqlAlchemyP115PlaybackRewardRepository(database.unit_of_work),
                telegram,
            )
            playback_config = P115PlaybackConfigService(
                SqlAlchemyP115PlaybackConfigBackend(database.unit_of_work),
                playback_users,
            )
            playback_repository = PostgresP115PlaybackRepository(database.unit_of_work)
            playback_executor = P115PlaybackOperationExecutor(
                repository=playback_repository,
                provider=NativeP115PlaybackProvider(
                    NativeP115PoolPlaybackClientFactory(
                        playback_config,
                        primary_credentials=p115_credentials,
                    ),
                    NativeP115PlaybackRapidSigner(
                        P115SdkDirectUrlGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                        ),
                        p115_credentials,
                    ),
                ),
                config=playback_config,
                worker_id=f"{self._worker_id}:p115-playback",
            )
            playback_service = P115PlaybackService(
                config=playback_config,
                repository=playback_repository,
                sources=SqlAlchemyP115PlaybackSourceGateway(
                    database.unit_of_work,
                    primary_credentials=p115_credentials,
                    priority=lambda: str(settings_provider().p115_api_priority),
                ),
                direct_links=NativeP115DirectLinkGateway(
                    playback_config,
                    p115_credentials,
                    priority=lambda: str(settings_provider().p115_api_priority),
                ),
                executor=playback_executor,
            )
            sharing_operations = PostgresSharingOperationRepository(database.unit_of_work)
            cleanup_source_lifecycle = SharingService(
                PostgresContributionLedger(database.unit_of_work),
                shared_center,
                sharing_operations,
                unit_of_work=database.unit_of_work,
                projection=SqlAlchemySharingProjectionRepository(database.unit_of_work),
            )
            cleanup_executor = CleanupExecutionService(
                cleanup_repository,
                local=SafeLocalCleanupGateway(),
                media=SqlAlchemyCleanupMediaGateway(
                    database.unit_of_work,
                    cleanup_source_lifecycle,
                ),
                p115=NativeCleanupP115Gateway(
                    P115PrimaryOperationsGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    )
                ),
                emby=ConfiguredCleanupEmbyGateway(settings_provider),
                lease_seconds=max(self.config.lease_seconds, 60),
            )
            virtual_imports = VirtualImportService(
                resolver=VirtualImportSourceResolver(shared_center),
                repository=SqlAlchemyVirtualImportRepository(database.unit_of_work),
                projector=AtomicVirtualImportProjector(
                    settings=settings_provider,
                    libraries=AtomicP115LocalStrmLibraryGateway(),
                ),
                configuration=lambda: app_settings.load("shared_resource_config"),
            )
            auto_transfer_delivery = AutoSubscriptionTransferOperationDelivery(
                repository=sharing_operations,
                hdhive=hdhive,
                telegram=telegram_userbot,
                p115=NativeP115ResourceTransferGateway(p115_credentials, settings_provider),
            )
            sharing_operation_executor = SharingOperationExecutor(
                sharing_operations,
                SharingProviderOperationDelivery(
                    shared_center=shared_center,
                    hdhive=hdhive,
                    telegram=telegram,
                    raw_ffprobe_loader=partial(_load_sharing_raw_ffprobe, p115_mediainfo_repository),
                    virtual_imports=VirtualImportOperationDelivery(
                        virtual_imports,
                        HttpxEmbyVirtualItemGateway(settings_provider),
                    ),
                    rapid_transfers=RapidTransferOperationDelivery(
                        repository=sharing_operations,
                        virtual_imports=virtual_imports,
                        credentials=p115_credentials,
                        settings=settings_provider,
                        center=shared_center,
                        p115=NativeP115RapidTransferGateway(
                            credentials=p115_credentials,
                            priority=lambda: str(settings_provider().p115_api_priority),
                            target_cid=lambda: str(settings_provider().p115_save_path_cid),
                        ),
                    ),
                    extra_deliveries=(
                        LogicalSeasonShareTransferOperationDelivery(
                            repository=sharing_operations,
                            virtual_imports=virtual_imports,
                            credentials=p115_credentials,
                            settings=settings_provider,
                            center=shared_center,
                            p115=NativeP115LogicalSeasonShareGateway(
                                credentials=p115_credentials,
                                priority=lambda: str(settings_provider().p115_api_priority),
                                target_cid=lambda: str(settings_provider().p115_save_path_cid),
                            ),
                        ),
                        auto_transfer_delivery,
                        HDHiveRegistrationOperationDelivery(hdhive, app_settings),
                    ),
                ),
                max_attempts=4,
            )
            auto_tmdb = HttpxManualSubscriptionTmdbAdapter(settings_provider)
            auto_subscribe = AutoSubscribeService(
                SqlAlchemyAutoSubscriptionRepository(
                    database.unit_of_work,
                    SqlAlchemySubscriptionMediaProjection(),
                ),
                app_settings,
                auto_tmdb,
                ManualSubscriptionQuotaLedger(PostgresSubscriptionRuntimeRepository(database.unit_of_work)),
                AutoSubscriptionMoviePilotGateway(moviepilot_effects, app_settings, auto_tmdb),
                PersistentOrderedAutoSubscriptionSources(
                    center=shared_center,
                    hdhive=hdhive,
                    telegram=telegram_userbot,
                    operations=sharing_operations,
                    executor=sharing_operation_executor,
                    virtual_imports=virtual_imports,
                    shared_configuration=lambda: app_settings.load("shared_resource_config"),
                    telegram_configuration=lambda: app_settings.load("tg_userbot_config"),
                    worker_id=self._worker_id,
                    lease_seconds=SHARING_OPERATION_CLAIM_LEASE_SECONDS,
                ),
                TelegramAutoSubscriptionNotifications(
                    SqlAlchemyAutoSubscriptionNotificationDirectory(database.unit_of_work),
                    telegram,
                ),
            )
            daily_theme = DailyThemeService(
                HttpxDiscoverProviderAdapter(settings_provider),
                SqlAlchemyDailyThemeRepository(database.unit_of_work),
                LegacyDiscoverDefaults(),
            )
            auto_tagging_delivery = HttpxAutoTaggingAdapter(
                settings_provider,
                item_lock=PostgresAutoTaggingItemLock(database.engine),
            )
            studio_images_delivery = HttpxStudioImageAdapter(settings_provider)
            p115_fingerprint_repair = P115FingerprintRepairService(
                credentials=p115_credentials,
                repository=p115_mediainfo_repository,
                hints=LocalP115FingerprintHintGateway(),
                remote=HttpxP115FingerprintRemoteGateway(
                    credentials=p115_credentials,
                    priority=lambda: str(settings_provider().p115_api_priority),
                ),
                local_root=lambda: str(config_adapter.load().local_strm_root or ""),
            )
            mediainfo_policies = PersistentP115MediaInfoPolicyProvider(app_settings)
            processes = ControlledProcessRunner()
            media_probe = P115MediaProbeService(
                direct_urls=P115SdkDirectUrlGateway(
                    credentials=p115_credentials,
                    priority=lambda: str(settings_provider().p115_api_priority),
                ),
                processes=processes,
                iso=NativeP115IsoProbeGateway(processes),
                policies=mediainfo_policies,
            )
            strm_mediainfo = P115StrmMediaInfoEnrichmentService(
                credentials=p115_credentials,
                repository=p115_mediainfo_repository,
                probe=media_probe,
                policies=mediainfo_policies,
            )
            media_effects = PersistentMediaEffectExecutor(
                PostgresMediaEffectLedger(database.unit_of_work),
                RoutedMediaEffectDelivery(
                    [
                        BridgeMediaEffectDelivery(bridge_client),
                        SharedIntroExportEffectDelivery(app_settings, shared_center),
                        DailyThemeReplenishmentEffectDelivery(daily_theme),
                        auto_tagging_delivery,
                        studio_images_delivery,
                        MoviePilotWebhookEffectDelivery(
                            SqlAlchemyMoviePilotAssistantEventRepository(
                                database.unit_of_work,
                                full_washing_guard=partial(
                                    _moviepilot_full_washing_guard,
                                    app_settings,
                                ),
                                active_downloads=moviepilot_gateway.active_downloads,
                            ),
                            _moviepilot_transfer_processor(
                                database=database,
                                settings_provider=settings_provider,
                                credentials=p115_credentials,
                                media_info=strm_mediainfo,
                                plan_repository=organize_plans,
                                execution_repository=organize_executions,
                                jobs=lambda: service,
                            ),
                            MoviePilotFullWashingOperations(subscription_operations),
                            full_washing_timeout_hours=partial(
                                _moviepilot_full_washing_timeout_hours,
                                app_settings,
                            ),
                            snapshot_retention_days=partial(
                                _moviepilot_snapshot_retention_days,
                                app_settings,
                            ),
                        ),
                    ]
                ),
                worker_id=f"{self._worker_id}:media-effect",
                lease_seconds=lambda: studio_image_claim_lease_seconds(config_adapter.load().emby_api_timeout),
            )
            intro_repository = PostgresP115IntroDetectionRepository(database.unit_of_work)
            intro_source = PostgresP115IntroRuntimeSource(
                database.unit_of_work,
                intro_repository,
                app_settings,
                HttpxEmbyIntroFavoriteGateway(settings_provider),
            )
            intro_detection_gateway = NativeP115IntroDetectionGateway(
                submission=IntroDetectionSubmissionService(intro_repository),
                source=intro_source,
                account_id=lambda: p115_credentials.load().account_id,
                provider_identity=lambda: select_intro_provider_identity(
                    p115_credentials,
                    lambda: settings_provider().p115_api_priority,
                ),
            )
            intro_fingerprints = P115IntroFingerprintAdapter(
                NativeP115IntroDirectUrlGateway(
                    p115_credentials,
                    P115SdkDirectUrlGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                ),
                ChromaprintProcessAdapter(processes),
            )
            intro_executor = P115IntroDetectionOperationExecutor(
                repository=intro_repository,
                service=IntroDetectionExecutionService(
                    NativeIntroDetectionBatchEngine(
                        intro_repository,
                        intro_fingerprints,
                        EmbyAuthorizationRevisionAdapter(bridge_client),
                    ),
                    NativeIntroPolicyAdapter(intro_source),
                ),
                worker_id=f"{self._worker_id}:p115-intro",
            )
            media_task_service, people_task_service, screenshot_backfill = _native_media_people_task_services(
                database,
                settings_provider,
            )
            p5_media_tasks = P5MediaTaskRouter(
                compatibility=None,
                restore_mediainfo=P115MediaInfoRestoreService(
                    credentials=p115_credentials,
                    repository=p115_mediainfo_repository,
                    fingerprints=p115_fingerprint_repair,
                    emby=HttpxEmbyMediaInfoQueryGateway(settings_provider),
                    probe=media_probe,
                    policies=mediainfo_policies,
                    commands=PersistentMediaInfoCommandGateway(
                        database.unit_of_work,
                        bridge_client,
                        executor=media_effects,
                    ),
                ),
                media_tasks=media_task_service,
                people_tasks=people_task_service,
                screenshot_backfill=screenshot_backfill,
            )
            p6_p115_tasks = P6P115TaskRouter(
                directory_sync=P115DirectorySyncService(
                    settings=app_settings,
                    credentials=p115_credentials,
                    directories=HttpxP115DirectoryTreeGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                    repository=SqlAlchemyP115DirectoryCacheRepository(database.unit_of_work),
                ),
                organize_execution=P115OrganizeExecutionService(
                    plans=organize_plans,
                    repository=organize_executions,
                    mutations=HttpxP115OrganizeMutationGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                ),
                organize_record_tmdb_backfill=P115OrganizeRecordTmdbBackfillService(
                    SqlAlchemyP115OrganizeRecordsRepository(database.unit_of_work)
                ),
                strm_sync=P115StrmSyncService(
                    settings=settings_provider,
                    credentials=p115_credentials,
                    sorting_rules=SqlAlchemyP115SortingRulesRepository(database.unit_of_work),
                    rename_config=SqlAlchemyP115RenameConfigRepository(database.unit_of_work),
                    remote=HttpxP115StrmSyncGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                        settings=settings_provider,
                    ),
                    libraries=AtomicP115LocalStrmLibraryGateway(),
                    repository=SqlAlchemyP115StrmSyncRepository(database.unit_of_work),
                    coordinator=PostgresP115StrmSyncCoordinator(database.engine),
                    mediainfo=strm_mediainfo,
                ),
                fingerprint_repair=p115_fingerprint_repair,
                playback=playback_service,
                intro_backfill=P115IntroBackfillTaskService(intro_detection_gateway),
                organize_reconciliation=P115OrganizeExecutionService(
                    plans=organize_plans,
                    repository=organize_executions,
                    mutations=HttpxP115OrganizeReadbackGateway(
                        credentials=p115_credentials,
                        priority=lambda: str(settings_provider().p115_api_priority),
                    ),
                ),
            )
            resubscribe_cleanup = PersistentResubscribeCleanupGateway(
                SqlAlchemyCleanupRepository(
                    database.unit_of_work,
                    SqlAlchemyCleanupMediaProjection(),
                    SettingsCleanupExecutionPolicy(settings_provider, p115_credentials),
                )
            )
            p7_subscription_tasks = P7SubscriptionTaskRouter(
                None,
                native_assistant,
                moviepilot_effects=moviepilot_effects,
                washing_recalculation=washing_recalculation,
                old_season_backfill=OldSeasonBackfillService(
                    SqlAlchemyWatchlistMediaProjectionRepository(database.unit_of_work)
                ),
                manual_batch=manual_batch,
                auto_subscribe=auto_subscribe,
                people_tasks=people_task_service,
                watchlist_tasks=_native_watchlist_task_service(
                    database=database,
                    settings_provider=settings_provider,
                    app_settings=app_settings,
                    moviepilot_gateway=moviepilot_gateway,
                    moviepilot_effects=moviepilot_effects,
                    credentials=p115_credentials,
                    sharing_operations=sharing_operations,
                    sharing_identity=sharing_identity,
                    intro=_DeferredWatchlistIntroGateway(lambda: self._intro_detection_gateway),
                ),
                resubscribe_tasks=_native_resubscribe_task_service(
                    database=database,
                    app_settings=app_settings,
                    hdhive=hdhive,
                    sharing_operations=sharing_operations,
                    sharing_executor=sharing_operation_executor,
                    cleanup=resubscribe_cleanup,
                    worker_id=self._worker_id,
                ),
                resubscribe_cache=NativeResubscribeCacheTaskService(
                    SqlAlchemyResubscribeCacheRepository(database.unit_of_work)
                ),
            )
            p8_effect_tasks = P8EffectTaskRouter(
                None,
                UserSyncService(
                    HttpxEmbyUserSyncAdapter(settings_provider),
                    SqlAlchemyUserSyncRepository(database.unit_of_work),
                ),
                cleanup_scan,
                IdentityUserExpiryService(
                    identity_operations,
                    identity_mutations,
                    lease_seconds=lambda: identity_operation_claim_lease_seconds(settings_provider().emby_api_timeout),
                ),
                daily_theme=daily_theme,
                media_embeddings=MediaEmbeddingService(
                    SqlAlchemyMediaEmbeddingRepository(database.unit_of_work),
                    ConfiguredMediaEmbeddingProvider(settings_provider),
                    enabled=lambda: bool(settings_provider().ai_vector),
                ),
                native_collection_refresh=NativeCollectionRefreshService(
                    HttpxNativeCollectionRefreshAdapter(settings_provider),
                    SqlAlchemyNativeCollectionRefreshRepository(database.unit_of_work),
                    auto_subscribe=lambda: _app_setting_flag(
                        app_settings, "native_collections_config", "auto_sub_enabled"
                    ),
                ),
                auto_tagging=AutoTaggingService(
                    app_settings,
                    auto_tagging_delivery,
                    PersistentAutoTaggingEffects(media_effects),
                ),
                studio_images=StudioImageService(
                    app_settings,
                    studio_images_delivery,
                    PersistentStudioImageEffects(media_effects),
                ),
                custom_collection_tasks=custom_collection_tasks,
            )
            p9_sharing_tasks = P9SharingTaskRouter(
                shared_center=shared_center,
                hdhive=hdhive,
                telegram=telegram,
                compatibility=None,
            )
            supervisor = RuntimeSupervisor()
            supervisor.start(role=RuntimeRole.WORKER, background=False)
        except BaseException:
            clear_persistent_p115_credential_gateway(p115_credentials)
            if supervisor is not None:
                try:
                    supervisor.stop()
                except Exception:
                    logger.exception("Unable to stop a partially started native Worker supervisor")
            container.close()
            raise
        assert p115_credentials is not None
        self._container = container
        self._config_adapter = config_adapter
        self._last_settings = container.settings.to_runtime_dict()
        self._supervisor = supervisor
        self._store = store
        self._registry = registry
        self._service = service
        self._adapter = None
        self._media_effects = media_effects
        self._p5_media_tasks = p5_media_tasks
        self._p6_p115_tasks = p6_p115_tasks
        self._p6_ingestion_tasks = p6_ingestion_tasks
        self._p7_subscription_tasks = p7_subscription_tasks
        self._p8_effect_tasks = p8_effect_tasks
        self._p9_sharing_tasks = p9_sharing_tasks
        self._native_operations = native_operations
        self._p7_cleanup_executor = cleanup_executor
        self._p7_cleanup_repository = cleanup_repository
        self._ingestion_ingress = ingestion_ingress
        self._subscription_executor = subscription_executor
        self._collection_operation_executor = collection_operation_executor
        self._identity_executor = identity_executor
        self._database_maintenance_executor = database_maintenance_executor
        self._sharing_operation_executor = sharing_operation_executor
        self._p115_intro_executor = intro_executor
        self._intro_detection_gateway = intro_detection_gateway
        self._p115_credentials = p115_credentials
        self._native_only = True
        self._started = True

    def request_shutdown(self) -> None:
        self._shutdown_requested.set()
        with self._lock:
            store = self._store
            active = tuple(self._active_executions.items())
        for job_id, (processor, control) in active:
            if store is not None:
                try:
                    store.request_cancel(job_id)
                except Exception:
                    logger.exception("Unable to persist cooperative Worker shutdown", extra={"job_id": str(job_id)})
            control.request_cancel()
            _signal_stop(processor)

    def close(self) -> None:
        self.request_shutdown()
        if not self._idle.wait(self.config.worker_shutdown_timeout_seconds):
            raise RuntimeError("active Worker execution did not stop before the shutdown timeout")
        with self._lock:
            if not self._started:
                return
            errors: list[Exception] = []
            if self._p115_credentials is not None:
                try:
                    clear_persistent_p115_credential_gateway(self._p115_credentials)
                except Exception as exc:
                    errors.append(exc)
            if self._intro_detection_gateway is not None:
                try:
                    clear_persistent_intro_detection_gateway(self._intro_detection_gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._playback_gateway is not None:
                try:
                    clear_persistent_playback_gateway(self._playback_gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._p115_upload_source_cleanup is not None:
                try:
                    clear_persistent_upload_source_cleanup(self._p115_upload_source_cleanup)
                except Exception as exc:
                    errors.append(exc)
            if self._ingestion_ingress is not None:
                try:
                    clear_persistent_ingestion_ingress(self._ingestion_ingress)
                except Exception as exc:
                    errors.append(exc)
            if self._ingestion_binding is not None:
                try:
                    clear_persistent_ingestion_binding(self._ingestion_binding)
                except Exception as exc:
                    errors.append(exc)
            if self._media_effects is not None:
                try:
                    clear_persistent_media_effect_gateway(self._media_effects)
                except Exception as exc:
                    errors.append(exc)
            if self._final_refresh_settings_provider is not None:
                try:
                    clear_final_refresh_settings_provider(self._final_refresh_settings_provider)
                except Exception as exc:
                    errors.append(exc)
            if self._background_gateway is not None:
                try:
                    from etk.compatibility.background import clear_persistent_background_gateway

                    clear_persistent_background_gateway(self._background_gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._gateway is not None:
                try:
                    from task_manager import clear_persistent_submission_gateway

                    clear_persistent_submission_gateway(self._gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._supervisor is not None:
                try:
                    self._supervisor.stop()
                except Exception as exc:
                    errors.append(exc)
            if self._container is not None:
                try:
                    self._container.close()
                except Exception as exc:
                    errors.append(exc)
            self._container = None
            self._config_adapter = None
            self._last_settings = None
            self._supervisor = None
            self._store = None
            self._registry = None
            self._service = None
            self._adapter = None
            self._native_only = False
            self._p115_credentials = None
            self._media_effects = None
            self._p5_media_tasks = P5MediaTaskRouter(compatibility=None)
            self._p6_p115_tasks = None
            self._p6_ingestion_tasks = None
            self._p7_subscription_tasks = None
            self._p8_effect_tasks = None
            self._p9_sharing_tasks = None
            self._native_operations = None
            self._p7_cleanup_executor = None
            self._p7_cleanup_repository = None
            self._ingestion_executor = None
            self._ingestion_binding = None
            self._ingestion_ingress = None
            self._p115_upload_executor = None
            self._p115_playback_executor = None
            self._p115_intro_executor = None
            self._subscription_executor = None
            self._collection_operation_executor = None
            self._identity_executor = None
            self._database_maintenance_executor = None
            self._sharing_operation_executor = None
            self._playback_gateway = None
            self._intro_detection_gateway = None
            self._p115_upload_source_cleanup = None
            self._gateway = None
            self._background_gateway = None
            self._final_refresh_settings_provider = None
            self._active_executions.clear()
            self._active_ingestion_executions = 0
            self._active_p115_upload_executions = 0
            self._active_p115_playback_executions = 0
            self._active_p115_intro_executions = 0
            self._active_subscription_executions = 0
            self._active_collection_operation_executions = 0
            self._active_identity_executions = 0
            self._active_database_maintenance_executions = 0
            self._active_sharing_operation_executions = 0
            self._active_media_effect_executions = 0
            self._idle.set()
            self._started = False
            if errors:
                raise ExceptionGroup("Worker runtime shutdown failed", errors)

    def execute_media_effect(self, *, effect_id: str) -> MediaEffectProjection:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_effect_id = UUID(str(effect_id))
        except ValueError as exc:
            raise ValueError("media effect id must be a UUID") from exc
        self.start()
        with self._lock:
            if self._shutdown_requested.is_set() or worker_shutdown_requested():
                raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
            executor = self._media_effects
            if executor is None:
                raise RuntimeError("Worker media effect executor is not initialized")
            self._active_media_effect_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_effect_id)
        finally:
            with self._lock:
                self._active_media_effect_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and self._active_database_maintenance_executions == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_ingestion_monitor(self, *, item_id: str) -> MonitorItemProjection | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_item_id = UUID(str(item_id))
        except ValueError as exc:
            raise ValueError("ingestion monitor item id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._ingestion_executor
            if executor is None:
                raise RuntimeError("Worker ingestion monitor executor is not initialized")
            self._active_ingestion_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_item_id)
        finally:
            with self._lock:
                self._active_ingestion_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_p115_upload_operation(self, *, operation_id: str) -> UploadOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("P115 upload operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._p115_upload_executor
            if executor is None:
                raise RuntimeError("Worker P115 upload executor is not initialized")
            self._active_p115_upload_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_p115_upload_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_p115_playback_operation(self, *, operation_id: str) -> PlaybackOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("P115 playback operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._p115_playback_executor
            if executor is None:
                raise RuntimeError("Worker P115 playback executor is not initialized")
            self._active_p115_playback_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_p115_playback_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_p115_intro_detection_operation(
        self,
        *,
        operation_id: str,
    ) -> IntroDetectionOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("P115 intro detection operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._p115_intro_executor
            if executor is None:
                raise RuntimeError("Worker P115 intro detection executor is not initialized")
            self._active_p115_intro_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_p115_intro_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_subscription_operation(self, *, operation_id: str) -> SubscriptionOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("subscription operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._subscription_executor
            if executor is None:
                raise RuntimeError("Worker subscription operation executor is not initialized")
            self._active_subscription_executions += 1
            self._idle.clear()
        try:
            execution = executor.execute(
                parsed_operation_id,
                worker_id=f"{self._worker_id}:subscription",
                lease_seconds=SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS,
            )
            return execution.operation if execution is not None else None
        finally:
            with self._lock:
                self._active_subscription_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_collection_operation(self, *, operation_id: str) -> CollectionOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("collection operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._collection_operation_executor
            if executor is None:
                raise RuntimeError("Worker collection operation executor is not initialized")
            self._active_collection_operation_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_collection_operation_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_identity_operation(self, *, operation_id: str) -> IdentityOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("identity operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._identity_executor
            if executor is None:
                raise RuntimeError("Worker identity operation executor is not initialized")
            config_adapter = getattr(self, "_config_adapter", None)
            self._active_identity_executions += 1
            self._idle.clear()
        try:
            lease_seconds = IDENTITY_OPERATION_CLAIM_LEASE_SECONDS
            if config_adapter is not None:
                lease_seconds = identity_operation_claim_lease_seconds(config_adapter.load().emby_api_timeout)
            execution = executor.execute(
                parsed_operation_id,
                worker_id=f"{self._worker_id}:identity",
                lease_seconds=lease_seconds,
            )
            return execution.operation if execution is not None else None
        finally:
            with self._lock:
                self._active_identity_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_database_maintenance(
        self,
        *,
        operation_id: str,
        artifact_id: str | None = None,
    ) -> Any:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("database maintenance operation id must be a UUID") from exc
        parsed_artifact_id: UUID | None = None
        if artifact_id is not None:
            try:
                parsed_artifact_id = UUID(str(artifact_id))
            except ValueError as exc:
                raise ValueError("database maintenance artifact id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._database_maintenance_executor
            if executor is None:
                raise RuntimeError("Worker database maintenance executor is not initialized")
            self._active_database_maintenance_executions = getattr(self, "_active_database_maintenance_executions", 0)
            self._active_database_maintenance_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id, artifact_id=parsed_artifact_id)
        finally:
            with self._lock:
                self._active_database_maintenance_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and getattr(self, "_active_database_maintenance_executions", 0) == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute_sharing_operation(self, *, operation_id: str) -> SharingOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("sharing operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._sharing_operation_executor
            if executor is None:
                raise RuntimeError("Worker sharing operation executor is not initialized")
            self._active_sharing_operation_executions = getattr(self, "_active_sharing_operation_executions", 0) + 1
            self._idle.clear()
        try:
            execution = executor.execute(
                parsed_operation_id,
                worker_id=f"{self._worker_id}:sharing",
                lease_seconds=SHARING_OPERATION_CLAIM_LEASE_SECONDS,
            )
            return execution.operation if execution is not None else None
        finally:
            with self._lock:
                self._active_sharing_operation_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                    and self._active_collection_operation_executions == 0
                    and self._active_identity_executions == 0
                    and self._active_database_maintenance_executions == 0
                    and getattr(self, "_active_sharing_operation_executions", 0) == 0
                    and self._active_media_effect_executions == 0
                ):
                    self._idle.set()

    def execute(self, *, event_id: str, job_id: str, attempt_id: str) -> None:
        del event_id
        if worker_shutdown_requested() or self._shutdown_requested.is_set():
            raise WorkerShutdownRequested("Worker shutdown is already in progress")
        self.start()
        if worker_shutdown_requested() or self._shutdown_requested.is_set():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        store = self._require_store()
        parsed_job_id = UUID(job_id)
        parsed_attempt_id = UUID(attempt_id)
        with store.execution_guard(parsed_job_id) as acquired:
            if not acquired:
                logger.info("Duplicate delivery skipped because Job execution guard is held", extra={"job_id": job_id})
                return
            if self._shutdown_requested.is_set():
                raise WorkerShutdownRequested("Worker shutdown started before Job claim")
            if worker_shutdown_requested():
                raise WorkerShutdownRequested("process shutdown started before Job claim")
            pending = store.get_job(parsed_job_id)
            if pending is not None and pending.job.parent_job_id is not None:
                parent = store.get_job(pending.job.parent_job_id)
                if parent is not None and parent.job.status in {JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED}:
                    cancelled = store.request_cancel(parsed_job_id)
                    if cancelled.job.status is not JobStatus.QUEUED:
                        return
            if pending is not None and pending.job.parent_job_id is None:
                self._refresh_settings()
            claim = store.claim_attempt(
                job_id=parsed_job_id,
                attempt_id=parsed_attempt_id,
                worker_id=self._worker_id,
                lease_seconds=self.config.lease_seconds,
            )
            if not claim.claimed:
                logger.info("Duplicate or stale delivery skipped", extra={"job_id": job_id, "attempt_id": attempt_id})
                return
            self._execute_claimed(claim)

    def _execute_claimed(self, claim: AttemptClaimResult) -> None:
        store = self._require_store()
        projection = claim.projection
        job = projection.job
        attempt = projection.current_attempt
        registry = self._registry
        adapter = self._adapter
        if registry is None:
            raise RuntimeError("Worker task registry is not initialized")
        try:
            definition = registry.require(job.task_key)
        except KeyError:
            definition = None
        store.start_attempt(
            job_id=job.id,
            attempt_id=attempt.id,
            worker_id=self._worker_id,
            lease_seconds=self.config.lease_seconds,
        )
        try:
            envelope = _request_envelope(job.request_payload)
        except Exception as exc:
            safe_error = _error_summary(exc)
            store.fail_attempt(
                job_id=job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type=type(exc).__name__,
                error_message=str(safe_error["message"]),
                error_details=safe_error,
                retryable=False,
                honor_cancel_request=False,
            )
            return

        # Processor-free operations intentionally do not live in the legacy
        # task catalog.  Their bounded key is carried by the validated
        # envelope and dispatched through the explicit native router below.
        native_operation_key = _native_operation_key_from_envelope(envelope)
        native_identity_error = _native_operation_identity_error(job.task_key, envelope)
        native_task_key = _native_task_key_from_envelope(envelope)
        native_task_identity_error = _native_task_identity_error(job.task_key, envelope)
        if native_identity_error is None:
            native_identity_error = native_task_identity_error
        if native_identity_error is not None:
            store.fail_attempt(
                job_id=job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type="NativeOperationIdentityMismatch",
                error_message=native_identity_error,
                error_details={
                    "task_key": job.task_key,
                    "native_operation": envelope.get("native_operation"),
                    "native_task": envelope.get("native_task"),
                },
                retryable=False,
                honor_cancel_request=False,
            )
            return
        native_task_unavailable = (
            not native_operation_key
            and not native_task_key
            and (
                job.task_key in P8_REVIEW_TASK_KEYS
                or (definition is not None and definition.key not in _NATIVE_ONLY_EXECUTABLE_TASK_KEYS)
            )
        )
        if self._native_only and native_task_unavailable:
            unavailable_key = definition.key if definition is not None else job.task_key
            store.fail_attempt(
                job_id=job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type=NativeTaskExecutionUnavailable.__name__,
                error_message=f"native Worker execution owner is unavailable for task {unavailable_key!r}",
                error_details={"task_key": unavailable_key, "native_only": True},
                retryable=False,
                honor_cancel_request=False,
            )
            return
        processor_identity = ProcessorIdentity(envelope["processor_type"])
        processor = (
            None
            if (
                self._native_only
                or (
                    definition is not None
                    and (
                        definition.key in (P4_NATIVE_TASK_KEYS | P6_NATIVE_TASK_KEYS)
                        or definition.key
                        in {
                            "add-all-series-to-watchlist",
                            "actor-tracking",
                            "actor-translation",
                            "auto-subscribe",
                            "backfill-media-metadata",
                            "check-expired-users",
                            "enrich-aliases",
                            "manual_subscribe_batch",
                            "merge-duplicate-actors",
                            "populate-metadata",
                            "purge-ghost-actors",
                            "process-watchlist",
                            "recalculate_library_washing_priorities",
                            "resubscribe-library",
                            "refresh_completed_series",
                            "restore_mediainfo",
                            "restore-nfo-and-images",
                            "role-translation",
                            "scan-cleanup-issues",
                            "scan_old_seasons_backfill",
                            "subscribe-assistant-maintenance",
                            "sync-all-user-data",
                            "system-auto-update",
                            "update-daily-theme",
                            "update-resubscribe-cache",
                        }
                    )
                )
            )
            or envelope.get("call_style") == "direct"
            else _processor(processor_identity)
        )
        if (
            adapter is None
            and (
                definition is None
                or (definition.key not in NATIVE_TASK_KEYS and job.task_key not in P8_REVIEW_TASK_KEYS)
            )
            and not native_operation_key
            and not native_task_key
        ):
            store.fail_attempt(
                job_id=job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type="LegacyTaskAdapterUnavailable",
                error_message="native Worker cannot execute a legacy task without compatibility mode",
                error_details={"task_key": job.task_key, "native_only": self._native_only},
                retryable=False,
                honor_cancel_request=False,
            )
            return
        is_direct = envelope.get("call_style") == "direct"
        control = _ExecutionControl()
        _clear_stop(processor)
        self._register_execution(job.id, processor, control)
        monitor = _HeartbeatMonitor(
            store=store,
            projection=projection,
            worker_id=self._worker_id,
            config=self.config,
            processor=processor,
            control=control,
            terminate_process=self._terminate_process,
            prepare_post_effect_cancellation=(
                self._p7_cleanup_repository.prepare_post_effect_cancellation
                if job.task_key == CLEANUP_EXECUTION_TASK_KEY and self._p7_cleanup_repository is not None
                else None
            ),
        )
        progress = _ExecutionProgress(
            store=store,
            job_id=job.id,
            attempt_id=attempt.id,
            worker_id=self._worker_id,
            lease_seconds=self.config.lease_seconds,
            control=control,
            task_key=job.task_key,
            accept_succeeded_terminal_update=job.task_key == "scan-organize-115",
        )
        try:
            monitor.start()
        except BaseException:
            _clear_stop(processor)
            self._unregister_execution(job.id)
            raise
        result: Any = None
        failure: Exception | None = None
        p4_terminalization_reported = False
        try:
            if native_task_key:
                if native_task_key != CLEANUP_EXECUTION_TASK_KEY:
                    raise LookupError(f"native task {native_task_key!r} is not registered")
                result = self._execute_cleanup_task(
                    envelope,
                    job_id=job.id,
                    attempt_id=attempt.id,
                    progress=progress,
                )
            elif native_operation_key:
                if self._native_operations is None:
                    raise RuntimeError("native operation router is unavailable")
                result = self._native_operations.execute(
                    native_operation_key,
                    envelope["parameters"],
                    progress,
                )
            elif definition is not None and definition.key in P4_NATIVE_TASK_KEYS:
                result = self._execute_chain(projection)
            elif definition is not None and definition.key in P5_NATIVE_TASK_KEYS:
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata={"job_id": str(job.id), "attempt_id": str(attempt.id)},
                )
                if self._native_only:
                    result = self._p5_media_tasks.execute(message, processor, progress)
                else:
                    from task_manager import persistent_execution

                    with persistent_execution(progress):
                        result = self._p5_media_tasks.execute(message, processor, progress)
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P5 native task {definition.key!r} did not return TaskOutcome")
            elif definition is not None and definition.key in P6_NATIVE_TASK_KEYS:
                metadata = {
                    "job_id": str(job.id),
                    "attempt_id": str(attempt.id),
                    "worker_id": self._worker_id,
                }
                requested_by_user_id = str(getattr(job, "requested_by_user_id", None) or "").strip()
                if requested_by_user_id:
                    metadata["requested_by_user_id"] = requested_by_user_id
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata=metadata,
                )
                if definition.key in P6_INGESTION_NATIVE_TASK_KEYS:
                    if self._p6_ingestion_tasks is None:
                        raise RuntimeError("P6 ingestion task router is unavailable")
                    result = self._p6_ingestion_tasks.execute(message, progress)
                elif definition.key in P6_P115_NATIVE_TASK_KEYS:
                    if self._p6_p115_tasks is None:
                        raise RuntimeError("P6 115 task router is unavailable")
                    result = self._p6_p115_tasks.execute(message, progress)
                else:
                    raise LookupError(f"P6 native task {definition.key!r} has no task router")
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P6 native task {definition.key!r} did not return TaskOutcome")
            elif definition is not None and definition.key in P7_NATIVE_TASK_KEYS:
                metadata = {
                    "job_id": str(job.id),
                    "attempt_id": str(attempt.id),
                    "worker_id": self._worker_id,
                }
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata=metadata,
                )
                if self._p7_subscription_tasks is None:
                    raise RuntimeError("P7 subscription task router is unavailable")
                if self._native_only:
                    result = self._p7_subscription_tasks.execute(message, processor, progress)
                else:
                    from task_manager import persistent_execution

                    with persistent_execution(progress):
                        result = self._p7_subscription_tasks.execute(message, processor, progress)
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P7 native task {definition.key!r} did not return TaskOutcome")
            elif definition is not None and definition.key in P8_NATIVE_TASK_KEYS:
                metadata = {
                    "job_id": str(job.id),
                    "attempt_id": str(attempt.id),
                    "worker_id": self._worker_id,
                }
                requested_by_user_id = str(getattr(job, "requested_by_user_id", None) or "").strip()
                if requested_by_user_id:
                    metadata["requested_by_user_id"] = requested_by_user_id
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata=metadata,
                )
                if self._p8_effect_tasks is None:
                    raise RuntimeError("P8 effect task router is unavailable")
                if self._native_only or definition.key == "check-expired-users":
                    result = self._p8_effect_tasks.execute(message, processor, progress)
                else:
                    from task_manager import persistent_execution

                    with persistent_execution(progress):
                        result = self._p8_effect_tasks.execute(message, processor, progress)
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P8 native task {definition.key!r} did not return TaskOutcome")
            elif definition is not None and definition.key in P9_NATIVE_TASK_KEYS:
                metadata = {
                    "job_id": str(job.id),
                    "attempt_id": str(attempt.id),
                    "worker_id": self._worker_id,
                }
                requested_by_user_id = str(getattr(job, "requested_by_user_id", None) or "").strip()
                if requested_by_user_id:
                    metadata["requested_by_user_id"] = requested_by_user_id
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata=metadata,
                )
                if self._p9_sharing_tasks is None:
                    raise RuntimeError("P9 sharing task router is unavailable")
                if self._native_only:
                    result = self._p9_sharing_tasks.execute(message, processor, progress)
                else:
                    from task_manager import persistent_execution

                    with persistent_execution(progress):
                        result = self._p9_sharing_tasks.execute(message, processor, progress)
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P9 native task {definition.key!r} did not return TaskOutcome")
            elif job.task_key in P8_REVIEW_TASK_KEYS:
                if self._p8_effect_tasks is None:
                    raise RuntimeError("P8 effect task router is unavailable")
                if self._native_only:
                    result = self._p8_effect_tasks.execute_review(
                        job.task_key,
                        envelope["parameters"],
                        processor,
                        progress,
                    )
                else:
                    from task_manager import persistent_execution

                    with persistent_execution(progress):
                        result = self._p8_effect_tasks.execute_review(
                            job.task_key,
                            envelope["parameters"],
                            processor,
                            progress,
                        )
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P8 review task {job.task_key!r} did not return TaskOutcome")
            elif definition is not None:
                if adapter is None:
                    raise RuntimeError("Worker legacy task adapter is not initialized")
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata={"job_id": str(job.id), "attempt_id": str(attempt.id)},
                )
                from task_manager import persistent_execution

                with persistent_execution(progress):
                    result = adapter.execute(message)
            else:
                result = self._execute_importable_operation(envelope, processor, progress)
            p4_terminalization_reported = _p4_terminalized_native_outcome(job.task_key, result)
            progress.raise_if_reported_failure()
            _raise_for_reported_failure(job.task_key, result)
        except Exception as exc:
            failure = exc
        finally:
            try:
                monitor.close()
            except Exception as exc:
                failure = failure or exc

        try:
            if monitor.terminalized:
                return
            latest = store.get_job(job.id)
            if p4_terminalization_reported and _p4_terminalization_matches_outcome(
                latest,
                attempt_id=attempt.id,
                result=result,
            ):
                return
            control_state = control.state()
            cancellation_requested = bool(
                monitor.cancel_requested
                or worker_shutdown_requested()
                or (latest is not None and latest.job.status is JobStatus.CANCEL_REQUESTED)
            )
            control_reason = monitor.stop_reason
            if control_reason is None and (
                isinstance(failure, PersistentExecutionTimedOut)
                or isinstance(failure, _CooperativeTimeout)
                or monitor.timed_out
            ):
                control_reason = "timeout"
            if control_reason is None and (
                isinstance(failure, PersistentExecutionCancelled)
                or cancellation_requested
                or (not is_direct and failure is None and _stop_requested(processor))
            ):
                control_reason = "cancel"

            cleanup_result = (
                result if job.task_key == CLEANUP_EXECUTION_TASK_KEY and isinstance(result, Mapping) else None
            )
            if isinstance(failure, CleanupExecutionPending):
                retryable = attempt.attempt_no < job.max_attempts
                safe_error = _error_summary(failure)
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type=type(failure).__name__,
                    error_message=str(safe_error["message"]),
                    error_details={**safe_error, "phase": failure.phase, "item_id": str(failure.item_id)},
                    retryable=retryable,
                    next_attempt_at=(
                        datetime.now(UTC) + timedelta(seconds=failure.retry_delay_seconds)
                        if retryable and failure.retry_delay_seconds is not None
                        else (_retry_at(attempt.attempt_no) if retryable else None)
                    ),
                    honor_cancel_request=False,
                )
                return
            if (
                cleanup_result is not None
                and cleanup_result.get("status") == "confirmed"
                and cleanup_result.get("durable_effect_started") is True
            ):
                store.succeed_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    result=_result_object(cleanup_result),
                    honor_cancel_request=False,
                )
                return
            if cleanup_result is not None and cleanup_result.get("status") == "indeterminate":
                safe_error = _error_summary(
                    failure or RuntimeError(str(cleanup_result.get("message") or "indeterminate"))
                )
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type="CleanupExecutionIndeterminate",
                    error_message=str(safe_error["message"]),
                    error_details={**safe_error, "task_outcome": _result_object(cleanup_result)},
                    retryable=False,
                    honor_cancel_request=False,
                )
                return
            if cleanup_result is not None and cleanup_result.get("status") == "partial":
                safe_error = _error_summary(
                    failure or RuntimeError(str(cleanup_result.get("message") or "partial cleanup outcome"))
                )
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type="CleanupExecutionPartial",
                    error_message=str(safe_error["message"]),
                    error_details={**safe_error, "task_outcome": _result_object(cleanup_result)},
                    retryable=False,
                    honor_cancel_request=False,
                )
                return

            if control_reason is not None:
                effect_failure = _effect_interruption_failure(control_state, control_reason)
                if effect_failure is not None:
                    safe_error = _error_summary(effect_failure)
                    store.fail_attempt(
                        job_id=job.id,
                        attempt_id=attempt.id,
                        worker_id=self._worker_id,
                        error_type=type(effect_failure).__name__,
                        error_message=str(safe_error["message"]),
                        error_details={**safe_error, "control": control_state},
                        retryable=False,
                        honor_cancel_request=False,
                    )
                    return
                if control_reason == "cancel":
                    store.cancel_attempt(
                        job_id=job.id,
                        attempt_id=attempt.id,
                        worker_id=self._worker_id,
                    )
                    return
                retryable = attempt.attempt_no < job.max_attempts
                store.timeout_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_message=str(failure or "job exceeded its cooperative timeout"),
                    error_details={"control": control_state},
                    retryable=retryable,
                    next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                    honor_cancel_request=False,
                )
                return

            if (
                is_direct
                and failure is not None
                and not isinstance(
                    failure,
                    (
                        PersistentEffectIndeterminate,
                        PersistentExecutionInterruptedAfterEffect,
                    ),
                )
            ):
                failure = _effect_interruption_failure(control_state, "failure") or failure
            if isinstance(
                failure,
                (
                    DurableMoviePilotEffectIndeterminateError,
                    PersistentExecutionInterruptedAfterEffect,
                    PersistentEffectIndeterminate,
                ),
            ):
                safe_error = _error_summary(failure)
                if isinstance(result, TaskOutcome):
                    safe_error["task_outcome"] = result.to_result()
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type=type(failure).__name__,
                    error_message=str(safe_error["message"]),
                    error_details={**safe_error, "control": control.state()},
                    retryable=False,
                    honor_cancel_request=not is_direct,
                )
                return
            if failure is not None:
                retryable = (
                    attempt.attempt_no < job.max_attempts
                    and _reported_failure_is_retryable(result)
                    and not (is_direct and (monitor.timed_out or cancellation_requested))
                )
                safe_error = _error_summary(failure)
                if isinstance(result, TaskOutcome):
                    safe_error["task_outcome"] = result.to_result()
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type=type(failure).__name__,
                    error_message=str(safe_error["message"]),
                    error_details=safe_error,
                    retryable=retryable,
                    next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                    honor_cancel_request=not is_direct,
                )
                if not retryable:
                    logger.error(
                        "Job execution failed",
                        exc_info=(type(failure), failure, failure.__traceback__),
                        extra={"job_id": str(job.id)},
                    )
                return
            store.succeed_attempt(
                job_id=job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                result=_result_object(result),
                honor_cancel_request=not is_direct,
            )
        finally:
            _clear_stop(processor)
            self._unregister_execution(job.id)

    def _execute_cleanup_task(
        self,
        envelope: Mapping[str, Any],
        *,
        job_id: UUID,
        attempt_id: UUID,
        progress: _ExecutionProgress,
    ) -> Mapping[str, Any]:
        executor = self._p7_cleanup_executor
        if executor is None:
            raise RuntimeError("native Cleanup execution service is unavailable")
        parameters = envelope.get("parameters")
        if not isinstance(parameters, Mapping) or set(parameters) != {"job_id", "execution_key"}:
            raise InvalidJsonPayloadError("Cleanup execution parameters must contain only job_id and execution_key")
        try:
            payload_job_id = UUID(str(parameters.get("job_id") or ""))
        except ValueError as exc:
            raise InvalidJsonPayloadError("Cleanup execution job_id must be a UUID") from exc
        if payload_job_id != job_id:
            raise InvalidJsonPayloadError("Cleanup execution payload Job identity does not match the claimed Job")
        execution_key = str(parameters.get("execution_key") or "").strip().lower()
        if len(execution_key) != 64 or any(value not in "0123456789abcdef" for value in execution_key):
            raise InvalidJsonPayloadError("Cleanup execution_key must be a SHA-256 digest")
        return executor.execute(
            job_id=job_id,
            execution_key=execution_key,
            attempt_id=attempt_id,
            worker_id=self._worker_id,
            progress=progress,
        )

    def _execute_importable_operation(
        self,
        envelope: Mapping[str, Any],
        processor: Any,
        progress: _ExecutionProgress,
    ) -> Any:
        operation = envelope.get("legacy_operation")
        if not isinstance(operation, Mapping):
            raise LookupError("Job task key is not present in the frozen task registry")
        task_function = import_legacy_operation(
            str(operation.get("module") or ""),
            str(operation.get("qualname") or ""),
        )
        args = envelope.get("positional_parameters", [])
        if not isinstance(args, list):
            raise InvalidJsonPayloadError("positional_parameters must be a JSON array")
        from task_manager import persistent_execution

        with persistent_execution(progress):
            if envelope.get("call_style") == "direct":
                if progress.task_key != CLEANUP_EXECUTION_TASK_KEY:
                    progress.checkpoint()
                return task_function(*args, **envelope["parameters"])
            return task_function(processor, *args, **envelope["parameters"])

    def _execute_chain(self, parent: JobProjection) -> dict[str, Any]:
        depth = int(getattr(self._workflow_depth, "value", 0))
        if depth >= _WORKFLOW_DEPTH_LIMIT:
            raise RuntimeError("task-chain recursion depth exceeded")
        self._workflow_depth.value = depth + 1
        try:
            return self._execute_chain_at_depth(parent)
        finally:
            self._workflow_depth.value = depth

    def _execute_chain_at_depth(self, parent: JobProjection) -> dict[str, Any]:
        store = self._require_store()
        service = self._service
        if service is None or self._supervisor is None:
            raise RuntimeError("Worker runtime is not initialized")
        config = self._supervisor.config
        if parent.job.task_key == _HIGH_CHAIN:
            sequence = config.get("task_chain_sequence", [])
            max_minutes = int(config.get("task_chain_max_runtime_minutes") or 0)
        elif parent.job.task_key == _LOW_CHAIN:
            sequence = config.get("task_chain_low_freq_sequence", [])
            max_minutes = int(config.get("task_chain_low_freq_max_runtime_minutes") or 0)
        else:
            raise LookupError(f"unknown native chain task {parent.job.task_key!r}")
        if not isinstance(sequence, list):
            raise TypeError("task chain sequence must be a list")
        deadline = time.monotonic() + max_minutes * 60 if max_minutes > 0 else None
        outcomes: list[dict[str, Any]] = []
        for step, raw_key in enumerate(sequence, 1):
            task_key = str(raw_key)
            remaining_seconds = None if deadline is None else int(deadline - time.monotonic())
            if remaining_seconds is not None and remaining_seconds < 1:
                raise _CooperativeTimeout("task chain exceeded its maximum runtime")
            latest_parent = store.get_job(parent.job.id)
            if latest_parent is not None and latest_parent.job.status is JobStatus.CANCEL_REQUESTED:
                return {"status": "cancel_requested", "children": outcomes}
            registry = self._registry
            if registry is None:
                raise RuntimeError("Worker task registry is not initialized")
            try:
                definition = registry.require(task_key)
            except KeyError as exc:
                raise LookupError(f"task chain contains unknown task {task_key!r}") from exc
            if not definition.chain_eligible:
                raise ValueError(f"task chain contains a task that requires a dedicated entrypoint: {task_key!r}")
            if self._native_only and task_key not in _NATIVE_ONLY_EXECUTABLE_TASK_KEYS:
                raise NativeTaskExecutionUnavailable(
                    f"task chain contains task without a native Worker execution owner: {task_key!r}"
                )
            parameters = {"force_full_update": False} if task_key in _FORCE_INCREMENTAL_TASKS else {}
            submitted = service.submit_task(
                task_key,
                parameters,
                trigger_type="workflow",
                parent_job_id=parent.job.id,
                workflow_step=step,
                idempotency_scope="task-chain-child",
                idempotency_key=f"{parent.job.id}:{step}:{task_key}",
                max_attempts=1,
                timeout_seconds=remaining_seconds,
                silent=True,
                dispatch=False,
            )
            child = submitted.projection
            self.execute(
                event_id="inline-workflow",
                job_id=str(child.job.id),
                attempt_id=str(child.current_attempt.id),
            )
            completed = store.get_job(child.job.id)
            if completed is None:
                raise RuntimeError("workflow child disappeared after execution")
            if not completed.job.status.terminal:
                raise RuntimeError("workflow child did not complete during inline execution")
            outcomes.append(
                {
                    "job_id": str(completed.job.id),
                    "task_key": task_key,
                    "status": completed.job.status.value,
                }
            )
            if completed.job.status is JobStatus.TIMED_OUT:
                raise _CooperativeTimeout(f"workflow child {task_key!r} exceeded the remaining deadline")
            if completed.job.status is JobStatus.CANCELLED:
                latest_parent = store.get_job(parent.job.id)
                if latest_parent is not None and latest_parent.job.status is JobStatus.CANCEL_REQUESTED:
                    return {"status": "cancel_requested", "children": outcomes}
                raise RuntimeError(f"workflow child {task_key!r} was cancelled independently")
            if completed.job.status is JobStatus.FAILED:
                raise RuntimeError(f"workflow child {task_key!r} failed")
            store.heartbeat(
                job_id=parent.job.id,
                attempt_id=parent.current_attempt.id,
                worker_id=self._worker_id,
                progress=int(step / max(len(sequence), 1) * 100),
                message=f"任务链进度 {step}/{len(sequence)}: {task_key}",
                lease_seconds=self.config.lease_seconds,
            )
            self._sleep(3.0 if completed.job.status is JobStatus.FAILED else 1.0)
        return {"status": "completed", "children": outcomes}

    def _refresh_settings(self) -> None:
        with self._lock:
            adapter = self._config_adapter
            supervisor = self._supervisor
            if adapter is None or supervisor is None:
                raise RuntimeError("Worker runtime configuration is not initialized")
            settings = adapter.load()
            values = settings.to_runtime_dict()
            if values == self._last_settings:
                return
            supervisor.stop()
            supervisor.start(role=RuntimeRole.WORKER, background=False)
            self._last_settings = values

    def _register_execution(self, job_id: UUID, processor: Any, control: _ExecutionControl) -> None:
        with self._lock:
            self._active_executions[job_id] = (processor, control)
            self._idle.clear()

    def _unregister_execution(self, job_id: UUID) -> None:
        with self._lock:
            self._active_executions.pop(job_id, None)
            if (
                not self._active_executions
                and self._active_ingestion_executions == 0
                and self._active_p115_upload_executions == 0
                and self._active_p115_playback_executions == 0
                and self._active_p115_intro_executions == 0
                and self._active_subscription_executions == 0
                and self._active_collection_operation_executions == 0
                and self._active_identity_executions == 0
                and getattr(self, "_active_database_maintenance_executions", 0) == 0
                and getattr(self, "_active_sharing_operation_executions", 0) == 0
                and self._active_media_effect_executions == 0
            ):
                self._idle.set()

    def _require_store(self) -> PostgresJobStore:
        if self._store is None:
            raise RuntimeError("Worker runtime is not started")
        return self._store


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: WorkerRuntime | None = None
_PROCESS_SHUTDOWN_REQUESTED = False


def get_worker_runtime() -> WorkerRuntime:
    global _RUNTIME
    if worker_shutdown_requested():
        raise WorkerShutdownRequested("Worker shutdown is already in progress")
    with _RUNTIME_LOCK:
        if _RUNTIME is None:
            _RUNTIME = WorkerRuntime()
        return _RUNTIME


def close_worker_runtime() -> None:
    global _RUNTIME
    with _RUNTIME_LOCK:
        runtime, _RUNTIME = _RUNTIME, None
    if runtime is not None:
        runtime.close()


def request_worker_shutdown() -> None:
    """Signal-safe sticky shutdown request used by Celery's POSIX handler."""

    global _PROCESS_SHUTDOWN_REQUESTED
    _PROCESS_SHUTDOWN_REQUESTED = True


def worker_shutdown_requested() -> bool:
    return _PROCESS_SHUTDOWN_REQUESTED


def _load_sharing_raw_ffprobe(
    repository: SqlAlchemyP115MediaInfoRepository,
    sha1: str,
) -> Mapping[str, Any] | None:
    record = repository.find_by_sha1(sha1)
    return dict(record.raw) if record is not None and isinstance(record.raw, Mapping) else None


def _task_fixture() -> Path:
    return Path(__file__).resolve().parents[3] / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


def _request_envelope(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = normalize_json_object(dict(value), field="request_payload")
    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        raise InvalidJsonPayloadError("request_payload.parameters must be a JSON object")
    processor = ProcessorIdentity(str(payload.get("processor_type") or ""))
    version = payload.get("payload_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise InvalidJsonPayloadError("request_payload.payload_version must be a positive integer")
    payload["parameters"] = parameters
    payload["processor_type"] = processor.value
    payload["payload_version"] = version
    return payload


def _native_operation_key_from_envelope(envelope: Mapping[str, Any]) -> str:
    value = envelope.get("native_operation")
    return str(value or "").strip().lower() if isinstance(value, str) else ""


def _native_operation_identity_error(task_key: str, envelope: Mapping[str, Any]) -> str | None:
    task = str(task_key or "").strip()
    native_value = envelope.get("native_operation")
    native_key = _native_operation_key_from_envelope(envelope)
    has_native_field = "native_operation" in envelope
    has_native_task = task.startswith("native-operation:")

    if has_native_field and (not isinstance(native_value, str) or not native_key):
        return "native operation identity is missing or invalid"
    if native_key and task != f"native-operation:{native_key}":
        return "native operation payload does not match task key"
    if has_native_task and task != f"native-operation:{native_key}":
        return "native operation task key does not match payload identity"
    return None


def _native_task_key_from_envelope(envelope: Mapping[str, Any]) -> str:
    value = envelope.get("native_task")
    return str(value or "").strip().lower() if isinstance(value, str) else ""


def _native_task_identity_error(task_key: str, envelope: Mapping[str, Any]) -> str | None:
    task = str(task_key or "").strip().lower()
    native_value = envelope.get("native_task")
    native_key = _native_task_key_from_envelope(envelope)
    has_native_field = "native_task" in envelope
    if has_native_field and (not isinstance(native_value, str) or not native_key):
        return "native task identity is missing or invalid"
    if native_key and "legacy_operation" in envelope:
        return "native task payload must not include a legacy operation"
    if native_key and task != native_key:
        return "native task payload does not match task key"
    if task == CLEANUP_EXECUTION_TASK_KEY and native_key != task:
        return "Cleanup execution Job is missing its native task identity"
    if native_key and native_key != CLEANUP_EXECUTION_TASK_KEY:
        return "native task identity is not allowlisted"
    return None


def _processor(identity: ProcessorIdentity) -> Any:
    import extensions

    return {
        ProcessorIdentity.MEDIA: extensions.media_processor_instance,
        ProcessorIdentity.WATCHLIST: extensions.watchlist_processor_instance,
        ProcessorIdentity.ACTOR: extensions.actor_subscription_processor_instance,
    }[identity]


def _signal_stop(processor: Any) -> None:
    if processor is not None:
        processor.signal_stop()


def _clear_stop(processor: Any) -> None:
    if processor is not None:
        processor.clear_stop_signal()


def _stop_requested(processor: Any) -> bool:
    return bool(processor is not None and processor.is_stop_requested())


def _retry_at(attempt_no: int) -> datetime:
    delay_seconds = min(300, 2 ** max(0, attempt_no - 1))
    return datetime.now(UTC) + timedelta(seconds=delay_seconds)


def _effect_interruption_failure(control_state: Mapping[str, Any], reason: str) -> Exception | None:
    inflight = list(control_state.get("inflight_effects") or ())
    if inflight:
        return PersistentEffectIndeterminate(
            f"{reason} interrupted external effects with an unknown outcome: {', '.join(map(str, inflight))}"
        )
    committed = list(control_state.get("committed_effects") or ())
    if committed:
        return PersistentExecutionInterruptedAfterEffect(
            f"{reason} was observed after confirmed effects: {', '.join(map(str, committed))}"
        )
    return None


def _terminate_current_worker_process() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def _result_object(value: Any) -> dict[str, Any]:
    if isinstance(value, TaskOutcome):
        return value.to_result()
    if value is None:
        return {}
    if isinstance(value, Mapping):
        try:
            return normalize_json_object(dict(value), field="result")
        except InvalidJsonPayloadError:
            pass
    safe = redact_sensitive(value)
    try:
        return normalize_json_object({"value": safe}, field="result")
    except InvalidJsonPayloadError:
        return {"value_repr": str(safe)[:2000]}


def _error_summary(error: Exception) -> dict[str, Any]:
    message = redact_sensitive(str(error))
    return {
        "type": type(error).__name__,
        "message": str(message)[:2000],
    }


def _p4_terminalized_native_outcome(task_key: str, result: Any) -> bool:
    return (
        task_key == "scan-organize-115"
        and isinstance(result, TaskOutcome)
        and result.details.get("p4_terminalized") is True
    )


def _p4_terminalization_matches_outcome(
    projection: JobProjection | None,
    *,
    attempt_id: UUID,
    result: Any,
) -> bool:
    if projection is None or not isinstance(result, TaskOutcome):
        return False
    expected_job_status = JobStatus.SUCCEEDED if result.status.successful else JobStatus.FAILED
    expected_attempt_status = AttemptStatus.SUCCEEDED if result.status.successful else AttemptStatus.FAILED
    return (
        projection.job.status is expected_job_status
        and projection.current_attempt.id == attempt_id
        and projection.current_attempt.status is expected_attempt_status
    )


def _raise_for_reported_failure(task_key: str, result: Any) -> None:
    if isinstance(result, TaskOutcome):
        if result.status.successful:
            return
        raise _TaskReportedFailure(result.message or f"task reported {result.status.value}")
    if not isinstance(result, Mapping):
        return
    status = str(result.get("status") or "").strip().lower()
    if result.get("skipped") is True or status in {TaskOutcomeStatus.SKIPPED.value, "not_applicable"}:
        return
    reported_failure = result.get("ok") is False or status in {
        TaskOutcomeStatus.FAILED.value,
        TaskOutcomeStatus.PARTIAL.value,
        "indeterminate",
    }
    if not reported_failure:
        return
    message = str(result.get("message") or f"task {task_key!r} reported an unsuccessful outcome")
    raise _TaskReportedFailure(message)


def _reported_failure_is_retryable(result: Any) -> bool:
    if not isinstance(result, TaskOutcome):
        return True
    retryable = result.details.get("retryable")
    if isinstance(retryable, bool):
        return retryable
    legacy = result.details.get("legacy_result")
    return not (isinstance(legacy, Mapping) and legacy.get("retryable") is False)


def _worker_data_root(config_adapter: Any) -> Path:
    config_path = getattr(config_adapter, "config_path", None)
    if isinstance(config_path, Path):
        return config_path.parent.resolve()
    configured = str(os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[3] / "local_data"


def _custom_collection_task_service(
    database: Any,
    settings_provider: Callable[[], Any],
    app_settings: SqlAlchemyAppSettingsRepository,
    collection_operations: PostgresCollectionOperationRepository,
    collection_operation_executor: CollectionOperationExecutor,
) -> CustomCollectionTaskService:
    """Build the processor-free owner shared by all custom-collection aliases."""
    return CustomCollectionTaskService(
        SqlAlchemyCollectionRepository(database.unit_of_work),
        HttpxCustomCollectionSourceAdapter(
            settings_provider,
            SqlAlchemyCustomCollectionRecommendationService(
                database.unit_of_work,
                ConfiguredCustomCollectionDiscoveryProvider(settings_provider),
            ),
        ),
        HttpxCustomCollectionTmdbAdapter(settings_provider),
        SqlAlchemyCustomCollectionMediaIndexAdapter(database.unit_of_work),
        SqlAlchemyCustomCollectionLibraryQueryAdapter(
            database.unit_of_work,
            settings_provider,
            app_settings,
        ),
        NativeCustomCollectionRefreshOperationAdapter(collection_operations, collection_operation_executor),
        default_emby_user_id=lambda: str(settings_provider().emby_user_id or ""),
    )


def _native_media_people_task_services(
    database: Any,
    settings_provider: Callable[[], Any],
) -> tuple[NativeMediaMetadataTaskService, NativePeopleTaskService, NativeMediaScreenshotBackfillService]:
    """Build the processor-free P5/P7 people and media task owners."""
    media_tasks = NativeMediaMetadataTaskService(
        repository=SqlAlchemyMediaMetadataTaskRepository(database.unit_of_work),
        catalog=HttpxMediaCatalogAdapter(settings_provider),
        metadata=HttpxTmdbMetadataAdapter(settings_provider),
        translation=ConfiguredBatchTranslation(settings_provider),
        settings=settings_provider,
    )
    people_tasks = NativePeopleTaskService(
        catalog=HttpxPeopleCatalogAdapter(settings_provider),
        provider=HttpxTmdbPersonAdapter(settings_provider),
        repository=SqlAlchemyPersonMetadataRepository(database.unit_of_work),
        translation=ConfiguredBatchTranslation(settings_provider),
        actor_enabled=lambda: bool(settings_provider().ai_translate_actor_role),
    )
    screenshot_backfill = NativeMediaScreenshotBackfillService(
        repository=SqlAlchemyScreenshotRepository(database.unit_of_work),
        capture=FfmpegScreenshotCapture(),
        upload=HttpxEmbyScreenshotUploadAdapter(settings_provider),
    )
    return media_tasks, people_tasks, screenshot_backfill


def _legacy_compatibility_enabled() -> bool:
    return os.environ.get("ETK_ENABLE_LEGACY_COMPATIBILITY", "").strip().casefold() in {"1", "true", "yes", "on"}


__all__ = [
    "RuntimeLegacyTaskResolver",
    "WorkerShutdownRequested",
    "WorkerRuntime",
    "close_worker_runtime",
    "get_worker_runtime",
    "request_worker_shutdown",
    "worker_shutdown_requested",
]
