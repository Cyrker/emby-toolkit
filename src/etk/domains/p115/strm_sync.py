from __future__ import annotations

import hashlib
import json
import posixpath
import re
from collections.abc import Callable, Collection, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import UUID

from etk.domains.media.bridge_client import emby_authorization_revision
from etk.platform import Settings
from etk.platform.jobs import (
    PersistentEffectIndeterminate,
    PersistentExecutionCancelled,
    PersistentExecutionInterruptedAfterEffect,
    PersistentExecutionTimedOut,
)
from etk.platform.tasks import TaskOutcome

from .auth_directory import P115CredentialStore
from .media_probe import P115MediaProbeRequest, P115MediaProbeStatus, media_probe_operation_key
from .models import P115Result
from .rename_config import P115RenameConfigRepository, effective_rename_config
from .sorting_rules import P115SortingRulesRepository

_PAGE_LIMIT = 1000
_MAX_PAGES = 1000
_MAX_SUBTITLE_BYTES = 128 * 1024 * 1024
_LOCAL_LIBRARY_LOCK_SCOPE = "p115-strm-sync:local"
_VIDEO_EXTENSIONS = frozenset({"mp4", "mkv", "avi", "ts", "iso", "rmvb", "wmv", "mov", "m2ts", "flv", "mpg"})
_SUBTITLE_EXTENSIONS = frozenset({"srt", "ass", "ssa", "sub", "vtt", "sup"})
_CONTROL_EXCEPTIONS = (
    PersistentEffectIndeterminate,
    PersistentExecutionCancelled,
    PersistentExecutionInterruptedAfterEffect,
    PersistentExecutionTimedOut,
)


class P115StrmAssetKind(StrEnum):
    STRM = "strm"
    SUBTITLE = "subtitle"


@dataclass(frozen=True, slots=True)
class P115StrmSyncRoot:
    cid: str
    relative_path: str
    media_type: str


@dataclass(frozen=True, slots=True)
class P115StrmCacheEntry:
    id: str
    parent_id: str
    name: str
    local_path: str
    root_id: str
    entry_kind: str
    sha1: str | None = None
    pick_code: str | None = None
    size: int = 0
    strm_content_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class P115StrmCacheSnapshot:
    entries: tuple[P115StrmCacheEntry, ...]
    revision: str


@dataclass(frozen=True, slots=True)
class P115StrmCacheReconcileResult:
    cached: int
    cleaned: int
    effect_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class P115StrmMediaInfoEnrichmentResult:
    """Persistent per-item evidence returned by the P6.6 MediaInfo owner."""

    status: P115MediaProbeStatus
    diagnostic: str = ""
    provider_backend: str = ""
    cache_reused: bool = False

    @property
    def confirmed(self) -> bool:
        return self.status is P115MediaProbeStatus.CONFIRMED


@dataclass(frozen=True, slots=True)
class P115StrmSyncPlan:
    plan_id: UUID
    plan_key: str
    plan: dict[str, Any]
    attempts: int


@dataclass(frozen=True, slots=True)
class P115StrmNotificationRecovery:
    active_plans: tuple[P115StrmSyncPlan, ...] = ()
    modified_paths: tuple[str, ...] = ()
    deleted_paths: tuple[str, ...] = ()
    deleted_path_roots: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class P115DesiredLocalAsset:
    kind: P115StrmAssetKind
    remote_file_id: str
    pick_code: str
    relative_path: str
    content: str | None = None


@dataclass(frozen=True, slots=True)
class P115LocalWriteResult:
    changed: bool
    absolute_path: str


@dataclass(frozen=True, slots=True)
class P115LocalPathState:
    kind: str
    size: int = 0
    sha256: str | None = None
    device: int | None = None
    inode: int | None = None
    mtime_ns: int | None = None


@dataclass(frozen=True, slots=True)
class P115LocalDeleteAction:
    kind: str
    path: str
    absolute_path: str
    source: str
    expected: P115LocalPathState


@dataclass(frozen=True, slots=True)
class P115LocalWriteAction:
    kind: P115StrmAssetKind
    path: str
    absolute_path: str
    remote_file_id: str
    pick_code: str
    content_sha256: str | None
    expected: P115LocalPathState
    write_required: bool


@dataclass(frozen=True, slots=True)
class P115LocalReconcileResult:
    deleted_paths: tuple[str, ...]
    cleaned_directory_paths: tuple[str, ...] = ()
    deferred_paths: tuple[str, ...] = ()
    planned_deletions: tuple[P115LocalDeleteAction, ...] = ()

    @property
    def cleaned_directories(self) -> int:
        return len(self.cleaned_directory_paths)


class P115StrmSyncClient(Protocol):
    @property
    def backend(self) -> str: ...

    def list_page(
        self,
        *,
        cid: str,
        file_type: int,
        limit: int,
        offset: int,
    ) -> Mapping[str, Any]: ...

    def directory_path(self, directory_id: str) -> Sequence[Mapping[str, Any]]: ...

    def download_subtitle(self, pick_code: str, *, max_bytes: int) -> bytes: ...


class P115StrmSyncGateway(Protocol):
    def open(self, *, expected_account_id: str) -> P115StrmSyncClient | None: ...


class P115LocalStrmLibrary(Protocol):
    def root_path(self) -> str: ...

    def absolute_path(self, relative_path: str) -> str: ...

    def path_state(self, relative_path: str) -> P115LocalPathState: ...

    def plan_delete(
        self,
        relative_path: str,
        *,
        kind: str,
        source: str,
        expected_sha256: str | None = None,
    ) -> P115LocalDeleteAction | None: ...

    def exists(self, relative_path: str) -> bool: ...

    def strm_matches(self, relative_path: str, content: str) -> bool: ...

    def write_strm(self, relative_path: str, content: str) -> P115LocalWriteResult: ...

    def create_strm(self, relative_path: str, content: str) -> P115LocalWriteResult: ...

    def write_subtitle(self, relative_path: str, content: bytes) -> P115LocalWriteResult: ...

    def reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
        allowed_deletions: Sequence[P115LocalDeleteAction],
    ) -> P115LocalReconcileResult: ...

    def preview_reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
    ) -> P115LocalReconcileResult: ...

    def replace_strm(self, *, mode: str, search: str, replacement: str) -> tuple[int, int]: ...


class P115LocalStrmLibraryGateway(Protocol):
    def open(self, root: str) -> P115LocalStrmLibrary | None: ...


class P115StrmSyncRepository(Protocol):
    def snapshot(
        self,
        roots: Sequence[P115StrmSyncRoot],
        *,
        account_id: str,
    ) -> P115StrmCacheSnapshot: ...

    def recovery_snapshot(
        self,
        *,
        account_id: str,
        authorization_revision: str,
    ) -> P115StrmNotificationRecovery: ...

    def stage_plan(
        self,
        *,
        account_id: str,
        scope: str,
        provider_backend: str,
        expected_cache_revision: str,
        operation_revision: str,
        plan: Mapping[str, Any],
    ) -> P115StrmSyncPlan: ...

    def reconcile(
        self,
        roots: Sequence[P115StrmSyncRoot],
        entries: Sequence[P115StrmCacheEntry],
        *,
        plan_id: UUID,
        account_id: str,
        expected_revision: str,
        operation_revision: str,
    ) -> P115StrmCacheReconcileResult: ...

    def mark_plan_retryable(
        self,
        plan_id: UUID,
        *,
        local_effects_started: bool,
        error: Mapping[str, Any],
    ) -> None: ...


class P115StrmSyncCoordinator(Protocol):
    def hold(self, scope: str) -> AbstractContextManager[None]: ...


class P115StrmMediaInfoEnricher(Protocol):
    """Return only after the item's success or failure evidence is persistent."""

    def enrich(
        self,
        request: P115MediaProbeRequest,
        *,
        operation_key: str,
        progress: Any,
    ) -> P115StrmMediaInfoEnrichmentResult: ...


class P115StrmSyncService:
    """Converge STRM/subtitle files only after a complete account-bound remote scan."""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore,
        sorting_rules: P115SortingRulesRepository,
        rename_config: P115RenameConfigRepository,
        remote: P115StrmSyncGateway,
        libraries: P115LocalStrmLibraryGateway,
        repository: P115StrmSyncRepository,
        coordinator: P115StrmSyncCoordinator,
        mediainfo: P115StrmMediaInfoEnricher | None = None,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._sorting_rules = sorting_rules
        self._rename_config = rename_config
        self._remote = remote
        self._libraries = libraries
        self._repository = repository
        self._coordinator = coordinator
        self._mediainfo = mediainfo

    def execute(self, progress: Any, *, home_video_only: bool = False) -> TaskOutcome:
        scope = "home_video" if home_video_only else "full"
        start_message = (
            "=== 开始同步家庭视频 STRM 与字幕 ===" if home_video_only else "=== 开始全量同步 STRM 与字幕 ==="
        )
        progress.update(0, start_message)
        local_effects_started = False
        staged_plan: P115StrmSyncPlan | None = None
        try:
            settings = self._settings()
            rules = self._sorting_rules.load()
            roots = _sync_roots(rules.rules, home_video_only=home_video_only)
            if not roots:
                message = (
                    "未配置启用的家庭视频分类，跳过同步。"
                    if home_video_only
                    else "未配置启用且有效的分类规则，无法执行全量同步。"
                )
                progress.update(100 if home_video_only else -1, message)
                details = _outcome_details(scope=scope, roots=0)
                if home_video_only:
                    return TaskOutcome.skipped(message, details)
                return TaskOutcome.failed(message, details)

            account_id = str(self._credentials.load().account_id or "").strip()
            if not account_id or rules.account_id != account_id:
                message = "115 分类规则未绑定当前账号，STRM/字幕未修改。请重新保存分类规则。"
                progress.update(-1, message)
                return TaskOutcome.failed(message, _outcome_details(scope=scope, roots=len(roots)))

            _assert_emby_notification_configured(settings)
            library = self._libraries.open(str(settings.local_strm_root or ""))
            if library is None:
                raise RuntimeError("未配置可用的本地 STRM 根目录")
            client = self._remote.open(expected_account_id=account_id)
            if client is None:
                raise RuntimeError("115 客户端未初始化，STRM/字幕未修改")

            with self._coordinator.hold(_LOCAL_LIBRARY_LOCK_SCOPE):
                self._assert_account(account_id)
                settings = self._settings()
                _assert_emby_notification_configured(settings)
                rename = self._rename_config.load()
                config_revision = _config_revision(settings, rules.revision, rename.revision, scope)
                authorization_revision = emby_authorization_revision(settings)
                cache_before = self._repository.snapshot(roots, account_id=account_id)
                progress.update(5, "开始执行 115 完整分页扫描")
                assets, cache_entries, scan_counts = self._scan(
                    client,
                    roots,
                    settings=settings,
                    rename_config=effective_rename_config(rename.config),
                    progress=progress,
                )
                if client.backend not in {"openapi", "cookie"}:
                    raise RuntimeError("115 STRM 扫描未固定权威 API 后端")
                progress.update(65, f"已固定 115 {client.backend} 后端，远端扫描完成")
                self._assert_unchanged(
                    account_id=account_id,
                    rules_revision=rules.revision,
                    rename_revision=rename.revision,
                    config_revision=config_revision,
                    authorization_revision=authorization_revision,
                    scope=scope,
                )
                operation_revision = _operation_revision(
                    account_id=account_id,
                    backend=client.backend,
                    scope=scope,
                    config_revision=config_revision,
                    entries=cache_entries,
                    assets=assets,
                )
                mediainfo_stats = self._enrich_mediainfo(
                    assets,
                    cache_entries,
                    account_id=account_id,
                    operation_revision=operation_revision,
                    progress=progress,
                )
                self._assert_unchanged(
                    account_id=account_id,
                    rules_revision=rules.revision,
                    rename_revision=rename.revision,
                    config_revision=config_revision,
                    authorization_revision=authorization_revision,
                    scope=scope,
                )
                recovery = self._repository.recovery_snapshot(
                    account_id=account_id,
                    authorization_revision=authorization_revision,
                )
                logical_modified = _logically_changed_paths(cache_before, cache_entries, library)
                logical_deleted_projections = _logically_deleted_projections(
                    cache_before,
                    cache_entries,
                )
                logical_deleted_paths = tuple(path for path, _digest in logical_deleted_projections)
                logical_deleted = {library.absolute_path(relative_path) for relative_path in logical_deleted_paths}
                desired_paths = tuple(asset.relative_path for asset in assets)
                cleanup_enabled = bool(settings.p115_local_cleanup) and bool(assets)
                preview_reconcile = library.preview_reconcile(
                    roots=tuple(root.relative_path for root in roots),
                    desired_paths=desired_paths,
                    cleanup=cleanup_enabled,
                )
                write_actions = tuple(_local_write_action(asset, library) for asset in assets)
                preview_delete_paths = {action.path.casefold() for action in preview_reconcile.planned_deletions}
                delete_actions = _merge_local_delete_actions(
                    preview_reconcile.planned_deletions,
                    tuple(
                        action
                        for relative_path, expected_sha256 in logical_deleted_projections
                        if relative_path.casefold() not in preview_delete_paths
                        if (
                            action := library.plan_delete(
                                relative_path,
                                kind="file",
                                source="cache_projection",
                                expected_sha256=expected_sha256,
                            )
                        )
                        is not None
                    ),
                )
                recovered_modified, recovered_deleted = self._recover_notification_paths(
                    recovery,
                    current_library=library,
                )
                current_modified = set(logical_modified)
                current_modified.update(action.absolute_path for action in write_actions if action.write_required)
                current_deleted = logical_deleted | {action.absolute_path for action in delete_actions}
                recovered_modified_set = set(recovered_modified)
                recovered_deleted_set = set(recovered_deleted)
                recovered_modified_set.difference_update(current_deleted)
                recovered_deleted_set.difference_update(current_modified)
                recovered_modified = tuple(sorted(recovered_modified_set))
                recovered_deleted = tuple(sorted(recovered_deleted_set))
                planned_modified = current_modified | recovered_modified_set
                planned_deleted = current_deleted | recovered_deleted_set
                managed_roots = _managed_local_roots(roots, cache_before)
                staged_plan = self._repository.stage_plan(
                    account_id=account_id,
                    scope=scope,
                    provider_backend=client.backend,
                    expected_cache_revision=cache_before.revision,
                    operation_revision=operation_revision,
                    plan={
                        "schema_version": 2,
                        "library_root": library.root_path(),
                        "desired_cache_revision": cache_snapshot_revision(cache_entries),
                        "roots": [
                            {
                                "cid": root.cid,
                                "relative_path": root.relative_path,
                            }
                            for root in roots
                        ],
                        "managed_roots": list(managed_roots),
                        "writes": [_local_write_action_payload(action) for action in write_actions],
                        "deletes": [_local_delete_action_payload(action) for action in delete_actions],
                        "notifications": {
                            "modified_paths": sorted(planned_modified),
                            "deleted_paths": sorted(planned_deleted),
                            "current_modified_paths": sorted(current_modified),
                            "current_deleted_paths": sorted(current_deleted),
                            "recovered_modified_paths": list(recovered_modified),
                            "recovered_deleted_paths": list(recovered_deleted),
                            "emby_authorization_revision": authorization_revision,
                        },
                    },
                )
                _assert_staged_plan_matches_scan(
                    staged_plan.plan,
                    roots=roots,
                    managed_roots=managed_roots,
                    assets=assets,
                    cache_entries=cache_entries,
                    library_root=library.root_path(),
                )
                _assert_plan_authorization_revision(staged_plan.plan, authorization_revision)
                planned_writes = _planned_local_writes(staged_plan.plan)
                allowed_deletions = _planned_local_deletions(staged_plan.plan)
                _assert_write_preconditions(planned_writes, library)
                written_strm = 0
                downloaded_subtitles = 0
                progress.update(75, "远端扫描完整，开始原子写入本地 STRM/字幕")
                for asset in assets:
                    progress.checkpoint()
                    action = _write_action_for_asset(planned_writes, asset)
                    if not action.write_required:
                        continue
                    if asset.kind is P115StrmAssetKind.STRM:
                        assert asset.content is not None
                        result = library.write_strm(asset.relative_path, asset.content)
                        local_effects_started = local_effects_started or result.changed
                        if result.changed:
                            written_strm += 1
                        continue
                    if library.exists(asset.relative_path):
                        continue
                    subtitle = client.download_subtitle(asset.pick_code, max_bytes=_MAX_SUBTITLE_BYTES)
                    if not subtitle:
                        raise RuntimeError(f"115 字幕下载返回空内容: {asset.relative_path}")
                    result = library.write_subtitle(asset.relative_path, subtitle)
                    local_effects_started = local_effects_started or result.changed
                    if result.changed:
                        downloaded_subtitles += 1

                _assert_write_postconditions(planned_writes, library)

                local_reconcile = library.reconcile(
                    roots=tuple(root.relative_path for root in roots),
                    desired_paths=desired_paths,
                    cleanup=cleanup_enabled,
                    allowed_deletions=allowed_deletions,
                )
                local_effects_started = local_effects_started or bool(
                    local_reconcile.deleted_paths or local_reconcile.cleaned_directory_paths
                )
                allowed_deleted_paths = {action.path for action in allowed_deletions if action.kind == "file"}
                allowed_directory_paths = {action.path for action in allowed_deletions if action.kind == "directory"}
                if not set(local_reconcile.deleted_paths).issubset(allowed_deleted_paths):
                    raise RuntimeError("本地清理执行集合超出已持久化的 STRM action plan")
                if not set(local_reconcile.cleaned_directory_paths).issubset(allowed_directory_paths):
                    raise RuntimeError("本地目录清理执行集合超出已持久化的 STRM action plan")
                planned_deferred = set(local_reconcile.deferred_paths) & (
                    allowed_deleted_paths | allowed_directory_paths
                )
                if planned_deferred:
                    raise RuntimeError(
                        "持久 STRM 删除动作的本地身份已变化，需保留缓存前置状态并重新规划: "
                        + ", ".join(sorted(planned_deferred, key=str.casefold))
                    )
                self._assert_unchanged(
                    account_id=account_id,
                    rules_revision=rules.revision,
                    rename_revision=rename.revision,
                    config_revision=config_revision,
                    authorization_revision=authorization_revision,
                    scope=scope,
                )
                _assert_write_postconditions(planned_writes, library)
                _assert_delete_postconditions(allowed_deletions, library)
                _assert_current_deleted_notifications_absent(current_deleted, library)
                _, verified_recovered_deleted = self._recover_notification_paths(
                    recovery,
                    current_library=library,
                )
                missing_recovered_deleted = set(recovered_deleted) - set(verified_recovered_deleted)
                if missing_recovered_deleted:
                    raise RuntimeError(
                        "待恢复 STRM Deleted 意图提交前路径已重新出现: " + ", ".join(sorted(missing_recovered_deleted))
                    )
                _assert_managed_root_postconditions(managed_roots, library)
                progress.checkpoint()
                reconciled = self._repository.reconcile(
                    roots,
                    cache_entries,
                    plan_id=staged_plan.plan_id,
                    account_id=account_id,
                    expected_revision=cache_before.revision,
                    operation_revision=operation_revision,
                )

            message = (
                f"同步完成：STRM 写入 {written_strm} 个，字幕下载 {downloaded_subtitles} 个，"
                f"本地删除 {len(local_reconcile.deleted_paths)} 个"
            )
            if mediainfo_stats["mediainfo_enrichment_targets"]:
                message += (
                    f"，MediaInfo 补齐 {mediainfo_stats['mediainfo_enrichment_confirmed']} 个，"
                    f"失败 {mediainfo_stats['mediainfo_enrichment_failed']} 个"
                )
            details = _outcome_details(
                scope=scope,
                roots=len(roots),
                backend=client.backend,
                videos=scan_counts["videos"],
                subtitles=scan_counts["subtitles"],
                skipped_small_videos=scan_counts["skipped_small_videos"],
                strm_written=written_strm,
                subtitles_downloaded=downloaded_subtitles,
                local_files_deleted=len(local_reconcile.deleted_paths),
                local_directories_cleaned=local_reconcile.cleaned_directories,
                local_cleanup_deferred=len(local_reconcile.deferred_paths),
                local_cleanup_suppressed_empty_manifest=(bool(settings.p115_local_cleanup) and not assets),
                cache_entries=reconciled.cached,
                cache_entries_cleaned=reconciled.cleaned,
                notification_effect_ids=list(reconciled.effect_ids),
                mediainfo_deferred_to_p6_6=self._mediainfo is None,
                home_video_enrichment_deferred_to_p6_6=(home_video_only and self._mediainfo is None),
                mediainfo_enrichment_before_local_effects=self._mediainfo is not None,
                **mediainfo_stats,
            )
            if mediainfo_stats["mediainfo_enrichment_failed"]:
                progress.update(-1, message)
                return TaskOutcome.partial(message, details)
            progress.update(100, message)
            return TaskOutcome.succeeded(
                message,
                details,
            )
        except _CONTROL_EXCEPTIONS as exc:
            if staged_plan is not None:
                try:
                    self._repository.mark_plan_retryable(
                        staged_plan.plan_id,
                        local_effects_started=local_effects_started,
                        error={"type": type(exc).__name__, "message": str(exc)},
                    )
                except Exception as plan_exc:
                    exc.add_note(f"STRM action plan 状态更新失败: {plan_exc}")
            raise
        except Exception as exc:
            if staged_plan is not None:
                try:
                    self._repository.mark_plan_retryable(
                        staged_plan.plan_id,
                        local_effects_started=local_effects_started,
                        error={"type": type(exc).__name__, "message": str(exc)},
                    )
                except Exception as plan_exc:
                    exc = RuntimeError(f"{exc}; STRM action plan 状态更新失败: {plan_exc}")
            message = f"STRM/字幕同步失败: {exc}"
            progress.update(-1, message)
            return TaskOutcome.failed(
                message,
                _outcome_details(
                    scope=scope,
                    local_effects_may_have_been_applied=local_effects_started,
                    retry_is_idempotent=True,
                ),
            )

    def _enrich_mediainfo(
        self,
        assets: Sequence[P115DesiredLocalAsset],
        cache_entries: Sequence[P115StrmCacheEntry],
        *,
        account_id: str,
        operation_revision: str,
        progress: Any,
    ) -> dict[str, Any]:
        if self._mediainfo is None:
            return {
                "mediainfo_enrichment_targets": 0,
                "mediainfo_enrichment_confirmed": 0,
                "mediainfo_cache_reused": 0,
                "mediainfo_enrichment_failed": 0,
                "mediainfo_failures": [],
            }

        entries_by_id = {entry.id: entry for entry in cache_entries if entry.entry_kind == "file"}
        targets: list[P115StrmCacheEntry] = []
        for asset in assets:
            if asset.kind is not P115StrmAssetKind.STRM:
                continue
            entry = entries_by_id.get(asset.remote_file_id)
            if entry is None or not entry.sha1 or not entry.pick_code:
                raise RuntimeError(f"STRM desired projection 缺少 MediaInfo 稳定身份: {asset.remote_file_id}")
            targets.append(entry)

        confirmed = 0
        cache_reused = 0
        failures: list[dict[str, str]] = []
        for index, entry in enumerate(targets):
            progress.checkpoint()
            progress.update(
                70 + int(index / max(len(targets), 1) * 5),
                f"正在持久化 MediaInfo 补齐结果 ({index + 1}/{len(targets)})...",
            )
            assert entry.sha1 is not None
            assert entry.pick_code is not None
            operation_key = media_probe_operation_key(
                "strm_sync",
                f"{operation_revision}:{entry.root_id}:{entry.id}",
                entry.sha1,
            )
            result = self._mediainfo.enrich(
                P115MediaProbeRequest(
                    account_id=account_id,
                    sha1=entry.sha1,
                    pick_code=entry.pick_code,
                    file_name=entry.name,
                    file_size=entry.size,
                ),
                operation_key=operation_key,
                progress=progress,
            )
            if result.confirmed:
                confirmed += 1
                cache_reused += int(result.cache_reused)
                continue
            failures.append(
                {
                    "file_id": entry.id,
                    "sha1": entry.sha1,
                    "status": result.status.value,
                    "diagnostic": str(result.diagnostic or "")[:500],
                }
            )

        return {
            "mediainfo_enrichment_targets": len(targets),
            "mediainfo_enrichment_confirmed": confirmed,
            "mediainfo_cache_reused": cache_reused,
            "mediainfo_enrichment_failed": len(failures),
            "mediainfo_failures": failures,
        }

    def _scan(
        self,
        client: P115StrmSyncClient,
        roots: Sequence[P115StrmSyncRoot],
        *,
        settings: Settings,
        rename_config: Mapping[str, Any],
        progress: Any,
    ) -> tuple[tuple[P115DesiredLocalAsset, ...], tuple[P115StrmCacheEntry, ...], dict[str, int]]:
        allowed = _allowed_extensions(settings.p115_extensions)
        minimum_size = max(0, int(settings.p115_min_video_size)) * 1024 * 1024
        base_url = _strm_base_url(settings.etk_server_url)
        with_name = str(rename_config.get("strm_url_fmt") or "standard") == "with_name"
        assets: dict[str, P115DesiredLocalAsset] = {}
        path_identities: dict[str, tuple[str, str]] = {}
        cache_entries: dict[str, P115StrmCacheEntry] = {}
        directory_cache: dict[tuple[str, str], tuple[str, tuple[P115StrmCacheEntry, ...]]] = {}
        counts = {"videos": 0, "subtitles": 0, "skipped_small_videos": 0}

        for root_index, root in enumerate(roots):
            progress.checkpoint()
            progress.update(5 + int(root_index / len(roots) * 65), f"正在扫描分类目录: {root.relative_path}")
            file_types = (4, 1) if settings.p115_download_subs else (4,)
            for file_type in file_types:
                for item in self._read_pages(client, root, file_type=file_type, progress=progress):
                    name = _remote_name(item)
                    extension = _extension(name)
                    if file_type == 4 and (extension not in _VIDEO_EXTENSIONS or extension not in allowed):
                        continue
                    if file_type == 1 and (extension not in _SUBTITLE_EXTENSIONS or extension not in allowed):
                        continue
                    parent_id = _first_text(item, "pid", "parent_id", "cid")
                    pick_code = _first_text(item, "pc", "pick_code", "pickcode")
                    remote_id = _first_text(item, "fid", "file_id", "id")
                    if not parent_id or not pick_code:
                        raise RuntimeError(f"115 文件缺少父目录或 PickCode: {name or remote_id or '-'}")
                    relative_directory, directories = self._resolve_directory(
                        client,
                        root,
                        parent_id,
                        directory_cache,
                    )
                    for directory in directories:
                        _insert_cache_entry(cache_entries, directory)
                    safe_name = _safe_component(name, "115 文件名", preserve_whitespace=True)

                    if file_type == 4:
                        if not remote_id:
                            raise RuntimeError(f"115 视频缺少稳定文件 ID: {name}")
                        size = _file_size(item)
                        if 0 < size < minimum_size:
                            counts["skipped_small_videos"] += 1
                            continue
                        sha1 = _sha1(_first_text(item, "sha1", "sha"), name)
                        if size <= 0:
                            raise RuntimeError(f"115 视频缺少有效大小: {name}")
                        remote_relative_path = _join_relative(relative_directory, safe_name)
                        strm_relative_path = _replace_suffix(remote_relative_path, ".strm")
                        content = f"{base_url}/api/p115/play/{pick_code}"
                        if with_name:
                            content = f"{content}/{safe_name}"
                        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                        asset = P115DesiredLocalAsset(
                            kind=P115StrmAssetKind.STRM,
                            remote_file_id=remote_id,
                            pick_code=pick_code,
                            relative_path=strm_relative_path,
                            content=content,
                        )
                        _insert_local_asset(assets, path_identities, asset)
                        _insert_cache_entry(
                            cache_entries,
                            P115StrmCacheEntry(
                                id=remote_id,
                                parent_id=parent_id,
                                name=safe_name,
                                local_path=remote_relative_path,
                                root_id=root.cid,
                                entry_kind="file",
                                sha1=sha1,
                                pick_code=pick_code,
                                size=size,
                                strm_content_sha256=content_hash,
                            ),
                        )
                        counts["videos"] += 1
                        continue

                    subtitle_identity = f"{pick_code}:{relative_directory}:{safe_name}"
                    subtitle_id = remote_id or f"subtitle-{hashlib.sha256(subtitle_identity.encode()).hexdigest()}"
                    asset = P115DesiredLocalAsset(
                        kind=P115StrmAssetKind.SUBTITLE,
                        remote_file_id=subtitle_id,
                        pick_code=pick_code,
                        relative_path=_join_relative(relative_directory, safe_name),
                    )
                    _insert_local_asset(assets, path_identities, asset)
                    counts["subtitles"] += 1

        return (
            tuple(assets.values()),
            tuple(
                sorted(
                    cache_entries.values(),
                    key=lambda entry: (entry.root_id, entry.local_path.casefold(), entry.id),
                )
            ),
            counts,
        )

    def _read_pages(
        self,
        client: P115StrmSyncClient,
        root: P115StrmSyncRoot,
        *,
        file_type: int,
        progress: Any,
    ) -> tuple[Mapping[str, Any], ...]:
        offset = 0
        page = 0
        received = 0
        reported_total: int | None = None
        items: list[Mapping[str, Any]] = []
        seen_ids: set[str] = set()
        seen_pages: set[str] = set()
        while True:
            progress.checkpoint()
            response = client.list_page(
                cid=root.cid,
                file_type=file_type,
                limit=_PAGE_LIMIT,
                offset=offset,
            )
            if not isinstance(response, Mapping) or response.get("state") is not True:
                raise RuntimeError(_provider_error(response) or "115 全量分页返回失败状态")
            if response.get("_etk_authoritative_list_payload") is False:
                raise RuntimeError("115 全量分页缺少权威列表载荷")
            response_path = response.get("path")
            if isinstance(response_path, list) and response_path:
                if not any(isinstance(node, Mapping) and _node_id(node) == root.cid for node in response_path):
                    raise RuntimeError(f"115 全量分页路径不包含目标分类 CID {root.cid}")
            backend = str(response.get("_etk_api_label_lower") or "").strip().lower()
            if backend and backend != client.backend:
                raise RuntimeError("115 全量分页后端与固定后端不一致")
            data = response.get("data")
            if not isinstance(data, list) or any(not isinstance(item, Mapping) for item in data):
                raise RuntimeError("115 全量分页返回了无效 data 结构")
            if len(data) > _PAGE_LIMIT:
                raise RuntimeError("115 全量分页返回数量超过请求上限")
            total = _optional_count(response.get("count"))
            if total is not None:
                if reported_total is None:
                    reported_total = total
                elif reported_total != total:
                    raise RuntimeError("115 全量分页总数发生变化")
            if not data:
                if reported_total is not None and received != reported_total:
                    raise RuntimeError(f"115 全量分页数据不完整：应有 {reported_total} 条，实际收到 {received} 条")
                break
            if page >= _MAX_PAGES:
                raise RuntimeError(f"115 全量分页超过安全上限 {_MAX_PAGES} 个非空页")
            page += 1
            signature = hashlib.sha256(
                json.dumps(data, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode()
            ).hexdigest()
            if signature in seen_pages:
                raise RuntimeError("115 全量分页重复返回相同页面")
            seen_pages.add(signature)
            for raw_item in data:
                item = dict(raw_item)
                identity = _first_text(item, "fid", "file_id", "id") or _first_text(
                    item,
                    "pc",
                    "pick_code",
                    "pickcode",
                )
                if identity:
                    if identity in seen_ids:
                        raise RuntimeError(f"115 全量分页重复返回文件身份: {identity}")
                    seen_ids.add(identity)
                items.append(item)
            received += len(data)
            if reported_total is not None and received > reported_total:
                raise RuntimeError("115 全量分页数量超过 provider count")
            offset += len(data)
        return tuple(items)

    def _resolve_directory(
        self,
        client: P115StrmSyncClient,
        root: P115StrmSyncRoot,
        parent_id: str,
        cache: dict[tuple[str, str], tuple[str, tuple[P115StrmCacheEntry, ...]]],
    ) -> tuple[str, tuple[P115StrmCacheEntry, ...]]:
        key = (root.cid, parent_id)
        if key in cache:
            return cache[key]
        if parent_id == root.cid:
            result = (root.relative_path, ())
            cache[key] = result
            return result
        path = client.directory_path(parent_id)
        nodes = tuple(node for node in path if isinstance(node, Mapping))
        root_index = next((index for index, node in enumerate(nodes) if _node_id(node) == root.cid), -1)
        if root_index < 0:
            raise RuntimeError(f"115 目录 {parent_id} 不属于同步根目录 {root.cid}")
        parent = root.cid
        relative = root.relative_path
        directories: list[P115StrmCacheEntry] = []
        found_parent = False
        for node in nodes[root_index + 1 :]:
            node_id = _node_id(node)
            node_name = _safe_component(_remote_name(node), "115 目录名", preserve_whitespace=True)
            if not node_id or not node_name:
                continue
            relative = _join_relative(relative, node_name)
            directories.append(
                P115StrmCacheEntry(
                    id=node_id,
                    parent_id=parent,
                    name=node_name,
                    local_path=relative,
                    root_id=root.cid,
                    entry_kind="directory",
                )
            )
            parent = node_id
            if node_id == parent_id:
                found_parent = True
                break
        if not found_parent:
            raise RuntimeError(f"115 目录路径未包含目标父目录 {parent_id}")
        result = (relative, tuple(directories))
        cache[key] = result
        return result

    def _assert_account(self, expected_account_id: str) -> None:
        current = str(self._credentials.load().account_id or "").strip()
        if current != expected_account_id:
            raise RuntimeError("115 账号在 STRM/字幕同步期间发生变化")

    def _assert_unchanged(
        self,
        *,
        account_id: str,
        rules_revision: str,
        rename_revision: str,
        config_revision: str,
        authorization_revision: str,
        scope: str,
    ) -> None:
        self._assert_account(account_id)
        current_rules = self._sorting_rules.load()
        if current_rules.account_id != account_id or current_rules.revision != rules_revision:
            raise RuntimeError("115 分类规则在 STRM/字幕同步期间发生变化")
        current_rename = self._rename_config.load()
        if current_rename.revision != rename_revision:
            raise RuntimeError("115 重命名配置在 STRM/字幕同步期间发生变化")
        current_settings = self._settings()
        _assert_emby_notification_configured(current_settings)
        if _config_revision(current_settings, rules_revision, rename_revision, scope) != config_revision:
            raise RuntimeError("STRM/字幕运行配置在同步期间发生变化")
        if emby_authorization_revision(current_settings) != authorization_revision:
            raise RuntimeError("Emby 授权在 STRM/字幕同步期间发生变化")

    def _recover_notification_paths(
        self,
        recovery: P115StrmNotificationRecovery,
        *,
        current_library: P115LocalStrmLibrary,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        modified = set(recovery.modified_paths)
        deleted: set[str] = set()
        libraries: dict[str, P115LocalStrmLibrary] = {current_library.root_path(): current_library}

        def library_for(root: str) -> P115LocalStrmLibrary:
            library = libraries.get(root)
            if library is None:
                library = self._libraries.open(root)
                if library is None or library.root_path() != root:
                    raise RuntimeError(f"待恢复 STRM action plan 的本地库根目录不可安全访问: {root}")
                libraries[root] = library
            return library

        recovered_deleted = set(recovery.deleted_paths)
        deleted_roots: dict[str, str] = {}
        for item in recovery.deleted_path_roots:
            if not isinstance(item, tuple) or len(item) != 2:
                raise RuntimeError("待恢复 STRM Deleted 意图的本地库根目录映射无效")
            path, root = (str(value or "").strip() for value in item)
            if not path or not root or path in deleted_roots:
                raise RuntimeError("待恢复 STRM Deleted 意图的本地库根目录映射为空或重复")
            deleted_roots[path] = root
        if set(deleted_roots) != recovered_deleted:
            raise RuntimeError("待恢复 STRM Deleted 意图缺少完整的本地库根目录映射")
        for absolute_path in sorted(recovered_deleted):
            library = library_for(deleted_roots[absolute_path])
            root_prefix = f"{library.root_path().rstrip('/')}/"
            if not absolute_path.startswith(root_prefix):
                raise RuntimeError("待恢复 STRM Deleted 意图超出其本地库根目录")
            relative_path = _normalize_relative_path(absolute_path.removeprefix(root_prefix))
            if library.absolute_path(relative_path) != absolute_path:
                raise RuntimeError("待恢复 STRM Deleted 意图与其本地库根目录不一致")
            if library.path_state(relative_path).kind == "absent":
                deleted.add(absolute_path)

        for prior in recovery.active_plans:
            root = str(prior.plan.get("library_root") or "").strip()
            if not root:
                raise RuntimeError("待恢复 STRM action plan 缺少本地库根目录")
            library = library_for(root)
            notifications = prior.plan.get("notifications")
            if not isinstance(notifications, Mapping):
                raise RuntimeError("待恢复 STRM action plan 缺少 Emby 通知意图")
            prior_modified = set(_notification_paths(notifications.get("modified_paths"), "modified_paths"))
            prior_deleted = set(_notification_paths(notifications.get("deleted_paths"), "deleted_paths"))
            for action in _planned_local_writes(prior.plan):
                if not action.write_required:
                    continue
                absolute_path = library.absolute_path(action.path)
                if absolute_path != action.absolute_path or absolute_path not in prior_modified:
                    raise RuntimeError("待恢复 STRM action plan 的 Modified 意图与本地动作不一致")
                state = library.path_state(action.path)
                if action.kind is P115StrmAssetKind.STRM:
                    if state.kind == "file" and state.sha256 == action.content_sha256:
                        modified.add(absolute_path)
                elif state.kind == "file":
                    modified.add(absolute_path)
            for action in _planned_local_deletions(prior.plan):
                absolute_path = library.absolute_path(action.path)
                if absolute_path != action.absolute_path or absolute_path not in prior_deleted:
                    raise RuntimeError("待恢复 STRM action plan 的 Deleted 意图与本地动作不一致")
                if library.path_state(action.path).kind == "absent":
                    deleted.add(absolute_path)
        return tuple(sorted(modified)), tuple(sorted(deleted))


class P115StrmReplaceService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        libraries: P115LocalStrmLibraryGateway,
        coordinator: P115StrmSyncCoordinator,
    ) -> None:
        self._settings = settings
        self._libraries = libraries
        self._coordinator = coordinator

    def replace(self, payload: Mapping[str, Any]) -> P115Result:
        raw_mode = payload["mode"] if "mode" in payload else "plain"
        mode = raw_mode if isinstance(raw_mode, str) else ""
        search = str(payload.get("search") or "")
        raw_replacement = payload.get("replace", "")
        replacement = raw_replacement if isinstance(raw_replacement, str) else None
        if not search:
            return P115Result({"success": False, "message": "查找内容不能为空！"}, 400)
        if mode == "regex":
            try:
                re.compile(search)
            except re.error as exc:
                return P115Result({"success": False, "message": f"正则表达式语法错误: {exc}"}, 400)
        library = self._libraries.open(str(self._settings().local_strm_root or ""))
        if library is None:
            return P115Result(
                {"success": False, "message": "未配置本地 STRM 根目录，或该目录在容器中不存在！"},
                400,
            )
        try:
            with self._coordinator.hold(_LOCAL_LIBRARY_LOCK_SCOPE):
                if replacement is None:
                    if mode in {"plain", "regex"}:
                        fixed, skipped = 0, 0
                    else:
                        fixed, skipped = library.replace_strm(
                            mode=mode,
                            search=search,
                            replacement="",
                        )
                else:
                    fixed, skipped = library.replace_strm(
                        mode=mode,
                        search=search,
                        replacement=replacement,
                    )
        except re.error as exc:
            return P115Result({"success": False, "message": f"正则表达式语法错误: {exc}"}, 400)
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)
        message = f"替换完毕！成功修改了 {fixed} 个文件"
        if skipped > 0:
            message += f" (已跳过 {skipped} 个未匹配的文件)"
        return P115Result({"success": True, "message": message})


def _sync_roots(raw_rules: Sequence[Mapping[str, Any]], *, home_video_only: bool) -> tuple[P115StrmSyncRoot, ...]:
    roots: dict[str, P115StrmSyncRoot] = {}
    ordered: list[P115StrmSyncRoot] = []
    paths: dict[str, str] = {}
    for raw_rule in raw_rules:
        rule = dict(raw_rule)
        if not bool(rule.get("enabled", True)):
            continue
        media_type = str(rule.get("media_type") or "all").strip().lower() or "all"
        if home_video_only and media_type != "home_video":
            continue
        cid = str(rule.get("cid") or "").strip()
        if not cid or cid == "0":
            continue
        relative = _normalize_relative_path(rule.get("category_path") or rule.get("dir_name") or "未识别")
        root = P115StrmSyncRoot(cid=cid, relative_path=relative, media_type=media_type)
        previous = roots.get(cid)
        if previous is not None and previous != root:
            raise ValueError(f"115 分类 CID {cid} 配置了冲突路径")
        folded = relative.casefold()
        other = paths.get(folded)
        if other is not None and other != cid:
            raise ValueError(f"多个 115 分类映射到同一本地路径: {relative}")
        if previous is None:
            roots[cid] = root
            ordered.append(root)
        paths[folded] = cid
    return tuple(ordered)


def _allowed_extensions(values: Sequence[str]) -> frozenset[str]:
    normalized = {str(value or "").strip().lower().lstrip(".") for value in values}
    normalized.discard("")
    return frozenset(normalized or (_VIDEO_EXTENSIONS | _SUBTITLE_EXTENSIONS))


def _strm_base_url(value: object) -> str:
    base_url = str(value or "").strip().rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("请配置 http(s) 开头的 embytool 访问地址")
    return base_url


def _normalize_relative_path(value: object) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    normalized = posixpath.normpath(raw)
    if not raw or normalized in {"", "."} or normalized.startswith("/"):
        raise ValueError("STRM 本地相对路径不能为空或为绝对路径")
    if normalized == ".." or normalized.startswith("../"):
        raise ValueError("STRM 本地相对路径不能逃逸根目录")
    for component in normalized.split("/"):
        _safe_component(component, "STRM 路径分量")
    return normalized


def _join_relative(parent: str, child: str) -> str:
    return _normalize_relative_path(f"{parent}/{child}")


def _safe_component(value: object, label: str, *, preserve_whitespace: bool = False) -> str:
    raw = str(value or "")
    component = raw if preserve_whitespace else raw.strip()
    if (
        not component
        or not component.strip()
        or component in {".", ".."}
        or "/" in component
        or "\\" in component
        or "\x00" in component
    ):
        raise ValueError(f"{label}包含不安全路径分量")
    return component


def _replace_suffix(path: str, suffix: str) -> str:
    head, _dot, _extension = path.rpartition(".")
    return f"{head}{suffix}" if head else f"{path}{suffix}"


def _remote_name(item: Mapping[str, Any]) -> str:
    for key in ("fn", "n", "file_name", "name"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return ""


def _extension(name: str) -> str:
    normalized = name.strip()
    return normalized.rpartition(".")[2].lower() if "." in normalized else ""


def _first_text(item: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _node_id(node: Mapping[str, Any]) -> str:
    return _first_text(node, "id", "cid", "file_id", "fid", "parent_id")


def _file_size(item: Mapping[str, Any]) -> int:
    value = item.get("fs") if item.get("fs") not in (None, "") else item.get("size")
    try:
        size = int(float(str(value or 0)))
    except (TypeError, ValueError, OverflowError):
        return 0
    return max(size, 0)


def _sha1(value: object, name: str) -> str:
    sha1 = str(value or "").strip().upper()
    if len(sha1) != 40:
        raise RuntimeError(f"115 视频缺少有效 SHA1: {name}")
    try:
        bytes.fromhex(sha1)
    except ValueError as exc:
        raise RuntimeError(f"115 视频缺少有效 SHA1: {name}") from exc
    return sha1


def _optional_count(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        count = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise RuntimeError("115 全量分页 count 无效") from exc
    if count < 0:
        raise RuntimeError("115 全量分页 count 不能为负数")
    return count


def _provider_error(response: object) -> str:
    if not isinstance(response, Mapping):
        return str(response)
    return str(response.get("message") or response.get("error_msg") or response.get("error") or "")


def _insert_local_asset(
    assets: dict[str, P115DesiredLocalAsset],
    path_identities: dict[str, tuple[str, str]],
    asset: P115DesiredLocalAsset,
) -> None:
    path_key = asset.relative_path.casefold()
    identity = (asset.kind.value, asset.remote_file_id)
    previous_identity = path_identities.get(path_key)
    if previous_identity is not None and previous_identity != identity:
        raise RuntimeError(f"多个 115 文件映射到同一本地路径: {asset.relative_path}")
    asset_key = f"{asset.kind.value}:{asset.remote_file_id}"
    previous_asset = assets.get(asset_key)
    if previous_asset is not None and previous_asset != asset:
        raise RuntimeError(f"115 文件身份映射发生冲突: {asset.remote_file_id}")
    path_identities[path_key] = identity
    assets[asset_key] = asset


def _insert_cache_entry(entries: dict[str, P115StrmCacheEntry], entry: P115StrmCacheEntry) -> None:
    previous = entries.get(entry.id)
    if previous is not None and previous != entry:
        raise RuntimeError(f"115 缓存身份映射发生冲突: {entry.id}")
    entries[entry.id] = entry


def _config_revision(settings: Settings, rules_revision: str, rename_revision: str, scope: str) -> str:
    payload = {
        "etk_server_url": settings.etk_server_url,
        "local_strm_root": settings.local_strm_root,
        "p115_download_subs": settings.p115_download_subs,
        "p115_extensions": sorted(_allowed_extensions(settings.p115_extensions)),
        "p115_local_cleanup": settings.p115_local_cleanup,
        "p115_min_video_size": settings.p115_min_video_size,
        "rename_revision": rename_revision,
        "rules_revision": rules_revision,
        "scope": scope,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _operation_revision(
    *,
    account_id: str,
    backend: str,
    scope: str,
    config_revision: str,
    entries: Sequence[P115StrmCacheEntry],
    assets: Sequence[P115DesiredLocalAsset],
) -> str:
    payload = {
        "account_id": account_id,
        "backend": backend,
        "scope": scope,
        "config_revision": config_revision,
        "entries": [_entry_payload(entry) for entry in entries],
        "assets": [
            {
                "kind": asset.kind.value,
                "remote_file_id": asset.remote_file_id,
                "pick_code": asset.pick_code,
                "relative_path": asset.relative_path,
                "content_sha256": hashlib.sha256((asset.content or "").encode()).hexdigest()
                if asset.content is not None
                else None,
            }
            for asset in assets
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _local_write_action(
    asset: P115DesiredLocalAsset,
    library: P115LocalStrmLibrary,
) -> P115LocalWriteAction:
    expected = library.path_state(asset.relative_path)
    content_bytes = asset.content.encode("utf-8") if asset.content is not None else None
    content_sha256 = hashlib.sha256(content_bytes).hexdigest() if content_bytes is not None else None
    if asset.kind is P115StrmAssetKind.STRM:
        if content_sha256 is None:
            raise RuntimeError("STRM 写入动作缺少目标内容摘要")
        write_required = not (
            expected.kind == "file"
            and expected.sha256 == content_sha256
            and content_bytes is not None
            and expected.size == len(content_bytes)
        )
    else:
        if expected.kind not in {"absent", "file"}:
            raise RuntimeError(f"本地字幕目标不是普通文件: {asset.relative_path}")
        write_required = expected.kind == "absent"
    return P115LocalWriteAction(
        kind=asset.kind,
        path=asset.relative_path,
        absolute_path=library.absolute_path(asset.relative_path),
        remote_file_id=asset.remote_file_id,
        pick_code=asset.pick_code,
        content_sha256=content_sha256,
        expected=expected,
        write_required=write_required,
    )


def _local_write_action_payload(action: P115LocalWriteAction) -> dict[str, Any]:
    return {
        "kind": action.kind.value,
        "path": action.path,
        "absolute_path": action.absolute_path,
        "remote_file_id": action.remote_file_id,
        "pick_code": action.pick_code,
        "content_sha256": action.content_sha256,
        "expected": _local_path_state_payload(action.expected),
        "write_required": action.write_required,
    }


def _local_delete_action_payload(action: P115LocalDeleteAction) -> dict[str, Any]:
    return {
        "kind": action.kind,
        "path": action.path,
        "absolute_path": action.absolute_path,
        "source": action.source,
        "expected": _local_path_state_payload(action.expected),
    }


def _local_path_state_payload(state: P115LocalPathState) -> dict[str, Any]:
    return {
        "kind": state.kind,
        "size": state.size,
        "sha256": state.sha256,
        "device": state.device,
        "inode": state.inode,
        "mtime_ns": state.mtime_ns,
    }


def _local_path_state_from_payload(value: object, *, field: str) -> P115LocalPathState:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"持久 STRM action plan 的 {field} 不是对象")
    kind = str(value.get("kind") or "").strip().lower()
    if kind not in {"absent", "file", "directory"}:
        raise RuntimeError(f"持久 STRM action plan 的 {field} 类型无效")
    try:
        size = int(value.get("size") or 0)
        device = None if value.get("device") is None else int(value["device"])
        inode = None if value.get("inode") is None else int(value["inode"])
        mtime_ns = None if value.get("mtime_ns") is None else int(value["mtime_ns"])
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(f"持久 STRM action plan 的 {field} stat 身份无效") from exc
    sha256 = None if value.get("sha256") is None else str(value["sha256"]).strip().lower()
    if size < 0 or any(number is not None and number < 0 for number in (device, inode, mtime_ns)):
        raise RuntimeError(f"持久 STRM action plan 的 {field} stat 身份无效")
    if kind == "file":
        if len(sha256 or "") != 64:
            raise RuntimeError(f"持久 STRM action plan 的 {field} 缺少文件摘要")
        try:
            bytes.fromhex(sha256 or "")
        except ValueError as exc:
            raise RuntimeError(f"持久 STRM action plan 的 {field} 文件摘要无效") from exc
        if any(number is None for number in (device, inode, mtime_ns)):
            raise RuntimeError(f"持久 STRM action plan 的 {field} 缺少 stat 身份")
    elif kind == "directory":
        if sha256 is not None or any(number is None for number in (device, inode, mtime_ns)):
            raise RuntimeError(f"持久 STRM action plan 的 {field} 目录身份无效")
    elif any((size, sha256, device, inode, mtime_ns)):
        raise RuntimeError(f"持久 STRM action plan 的 {field} 缺失状态携带多余身份")
    return P115LocalPathState(
        kind=kind,
        size=size,
        sha256=sha256,
        device=device,
        inode=inode,
        mtime_ns=mtime_ns,
    )


def _assert_staged_plan_matches_scan(
    plan: Mapping[str, Any],
    *,
    roots: Sequence[P115StrmSyncRoot],
    managed_roots: Sequence[str],
    assets: Sequence[P115DesiredLocalAsset],
    cache_entries: Sequence[P115StrmCacheEntry],
    library_root: str,
) -> None:
    if plan.get("schema_version") != 2:
        raise RuntimeError("持久 STRM action plan 版本不受支持")
    expected_roots = [{"cid": root.cid, "relative_path": root.relative_path} for root in roots]
    if plan.get("roots") != expected_roots:
        raise RuntimeError("持久 STRM action plan 的同步根目录与本次扫描不一致")
    if plan.get("managed_roots") != list(managed_roots):
        raise RuntimeError("持久 STRM action plan 的本地管理根与缓存前置状态不一致")
    if plan.get("library_root") != library_root:
        raise RuntimeError("持久 STRM action plan 的本地库根目录与本次扫描不一致")
    if plan.get("desired_cache_revision") != cache_snapshot_revision(cache_entries):
        raise RuntimeError("持久 STRM action plan 的目标缓存投影与本次扫描不一致")
    writes = _planned_local_writes(plan)
    expected = {
        (asset.kind, asset.remote_file_id): (
            asset.relative_path,
            asset.pick_code,
            hashlib.sha256(asset.content.encode("utf-8")).hexdigest() if asset.content is not None else None,
        )
        for asset in assets
    }
    observed = {
        (action.kind, action.remote_file_id): (
            action.path,
            action.pick_code,
            action.content_sha256,
        )
        for action in writes
    }
    if observed != expected:
        raise RuntimeError("持久 STRM action plan 的写入投影与本次扫描不一致")


def _assert_plan_authorization_revision(plan: Mapping[str, Any], expected: str) -> None:
    notifications = plan.get("notifications")
    if not isinstance(notifications, Mapping):
        raise RuntimeError("持久 STRM action plan 缺少 Emby 通知意图")
    if str(notifications.get("emby_authorization_revision") or "") != expected:
        raise RuntimeError("持久 STRM action plan 的 Emby 授权前置版本不匹配")


def _planned_local_writes(plan: Mapping[str, Any]) -> tuple[P115LocalWriteAction, ...]:
    raw_actions = plan.get("writes")
    if not isinstance(raw_actions, list):
        raise RuntimeError("持久 STRM action plan 缺少本地写入动作")
    actions: list[P115LocalWriteAction] = []
    paths: set[str] = set()
    identities: set[tuple[P115StrmAssetKind, str]] = set()
    for raw_action in raw_actions:
        if not isinstance(raw_action, Mapping):
            raise RuntimeError("持久 STRM action plan 包含无效写入动作")
        try:
            kind = P115StrmAssetKind(str(raw_action.get("kind") or "").strip().lower())
        except ValueError as exc:
            raise RuntimeError("持久 STRM action plan 包含不支持的写入动作") from exc
        raw_path = str(raw_action.get("path") or "")
        path = _normalize_relative_path(raw_path)
        if raw_path != path:
            raise RuntimeError("持久 STRM action plan 包含非规范化写入路径")
        absolute_path = str(raw_action.get("absolute_path") or "")
        remote_file_id = str(raw_action.get("remote_file_id") or "").strip()
        pick_code = str(raw_action.get("pick_code") or "").strip()
        content_sha256 = (
            None if raw_action.get("content_sha256") is None else str(raw_action["content_sha256"]).strip().lower()
        )
        write_required = raw_action.get("write_required")
        if not absolute_path or not remote_file_id or not pick_code or not isinstance(write_required, bool):
            raise RuntimeError("持久 STRM action plan 的写入身份不完整")
        if kind is P115StrmAssetKind.STRM:
            if len(content_sha256 or "") != 64:
                raise RuntimeError("持久 STRM action plan 的 STRM 目标摘要无效")
            try:
                bytes.fromhex(content_sha256 or "")
            except ValueError as exc:
                raise RuntimeError("持久 STRM action plan 的 STRM 目标摘要无效") from exc
        elif content_sha256 is not None:
            raise RuntimeError("持久 STRM action plan 的字幕动作不能声明未知内容摘要")
        path_key = path.casefold()
        identity = (kind, remote_file_id)
        if path_key in paths or identity in identities:
            raise RuntimeError("持久 STRM action plan 包含重复写入动作")
        paths.add(path_key)
        identities.add(identity)
        actions.append(
            P115LocalWriteAction(
                kind=kind,
                path=path,
                absolute_path=absolute_path,
                remote_file_id=remote_file_id,
                pick_code=pick_code,
                content_sha256=content_sha256,
                expected=_local_path_state_from_payload(raw_action.get("expected"), field="写入前置状态"),
                write_required=write_required,
            )
        )
    return tuple(actions)


def _planned_local_deletions(plan: Mapping[str, Any]) -> tuple[P115LocalDeleteAction, ...]:
    raw_actions = plan.get("deletes")
    if not isinstance(raw_actions, list):
        raise RuntimeError("持久 STRM action plan 缺少本地删除动作")
    actions: list[P115LocalDeleteAction] = []
    paths: set[str] = set()
    for raw_action in raw_actions:
        if not isinstance(raw_action, Mapping):
            raise RuntimeError("持久 STRM action plan 包含无效删除动作")
        kind = str(raw_action.get("kind") or "").strip().lower()
        if kind not in {"file", "directory"}:
            raise RuntimeError("持久 STRM action plan 包含不支持的删除动作")
        raw_path = str(raw_action.get("path") or "")
        path = _normalize_relative_path(raw_path)
        if path != raw_path:
            raise RuntimeError("持久 STRM action plan 包含非规范化本地路径")
        path_key = path.casefold()
        if path_key in paths:
            raise RuntimeError("持久 STRM action plan 包含重复删除动作")
        absolute_path = str(raw_action.get("absolute_path") or "")
        if not absolute_path:
            raise RuntimeError("持久 STRM action plan 的删除绝对路径为空")
        source = str(raw_action.get("source") or "").strip().lower()
        if source not in {"reconcile", "cache_projection"}:
            raise RuntimeError("持久 STRM action plan 的删除来源无效")
        expected = _local_path_state_from_payload(raw_action.get("expected"), field="删除前置状态")
        if expected.kind != kind:
            raise RuntimeError("持久 STRM action plan 的删除类型与前置身份不匹配")
        paths.add(path_key)
        actions.append(
            P115LocalDeleteAction(
                kind=kind,
                path=path,
                absolute_path=absolute_path,
                source=source,
                expected=expected,
            )
        )
    return tuple(actions)


def _merge_local_delete_actions(
    *groups: Sequence[P115LocalDeleteAction],
) -> tuple[P115LocalDeleteAction, ...]:
    actions: dict[tuple[str, str], P115LocalDeleteAction] = {}
    for group in groups:
        for action in group:
            key = (action.kind, action.path.casefold())
            previous = actions.get(key)
            if previous is not None and previous != action:
                raise RuntimeError(f"本地删除目标在 action plan 固定期间发生变化: {action.path}")
            actions[key] = action
    files = sorted(
        (action for action in actions.values() if action.kind == "file"),
        key=lambda action: action.path.casefold(),
    )
    directories = sorted(
        (action for action in actions.values() if action.kind == "directory"),
        key=lambda action: (-action.path.count("/"), action.path.casefold()),
    )
    return tuple((*files, *directories))


def _write_action_for_asset(
    actions: Sequence[P115LocalWriteAction],
    asset: P115DesiredLocalAsset,
) -> P115LocalWriteAction:
    matches = tuple(
        action for action in actions if action.kind is asset.kind and action.remote_file_id == asset.remote_file_id
    )
    if len(matches) != 1:
        raise RuntimeError(f"持久 STRM action plan 缺少唯一写入动作: {asset.remote_file_id}")
    return matches[0]


def _assert_write_preconditions(
    actions: Sequence[P115LocalWriteAction],
    library: P115LocalStrmLibrary,
) -> None:
    for action in actions:
        current = library.path_state(action.path)
        if current == action.expected:
            continue
        if action.write_required and action.kind is P115StrmAssetKind.STRM:
            if current.kind == "file" and current.sha256 == action.content_sha256:
                continue
        if action.write_required and action.kind is P115StrmAssetKind.SUBTITLE:
            if action.expected.kind == "absent" and current.kind == "file":
                continue
        raise RuntimeError(f"本地写入目标在 action plan 固定后发生变化: {action.path}")


def _assert_write_postconditions(
    actions: Sequence[P115LocalWriteAction],
    library: P115LocalStrmLibrary,
) -> None:
    for action in actions:
        current = library.path_state(action.path)
        if action.kind is P115StrmAssetKind.STRM:
            if current.kind != "file" or current.sha256 != action.content_sha256:
                raise RuntimeError(f"STRM 原子写入后内容校验失败: {action.path}")
            continue
        if current.kind != "file":
            raise RuntimeError(f"字幕原子写入后状态校验失败: {action.path}")


def _assert_delete_postconditions(
    actions: Sequence[P115LocalDeleteAction],
    library: P115LocalStrmLibrary,
) -> None:
    for action in actions:
        if library.path_state(action.path).kind != "absent":
            raise RuntimeError(f"本地删除动作提交前状态校验失败: {action.path}")


def _assert_current_deleted_notifications_absent(
    paths: Collection[str],
    library: P115LocalStrmLibrary,
) -> None:
    root_prefix = f"{library.root_path().rstrip('/')}/"
    for absolute_path in paths:
        if not absolute_path.startswith(root_prefix):
            raise RuntimeError("当前 STRM Deleted 意图超出本地库根目录")
        relative_path = absolute_path.removeprefix(root_prefix)
        if library.path_state(relative_path).kind != "absent":
            raise RuntimeError(f"STRM Deleted 意图提交前路径已重新出现: {relative_path}")


def _assert_emby_notification_configured(settings: Settings) -> None:
    if not str(settings.emby_server_url or "").strip() or not settings.emby_api_key.get_secret_value().strip():
        raise RuntimeError("Emby URL 或 API token 未配置，STRM/字幕未修改")


def _assert_managed_root_postconditions(
    roots: Sequence[str],
    library: P115LocalStrmLibrary,
) -> None:
    for root in roots:
        state = library.path_state(root)
        if state.kind not in {"absent", "directory"}:
            raise RuntimeError(f"本地 STRM 管理根在数据库提交前不是安全目录: {root}")


def _managed_local_roots(
    roots: Sequence[P115StrmSyncRoot],
    snapshot: P115StrmCacheSnapshot,
) -> tuple[str, ...]:
    managed: dict[str, str] = {root.relative_path.casefold(): root.relative_path for root in roots}
    root_ids = {root.cid for root in roots}
    for entry in snapshot.entries:
        if entry.root_id not in root_ids or entry.parent_id != entry.root_id:
            continue
        previous_root = posixpath.dirname(entry.local_path)
        if previous_root in {"", "."}:
            continue
        normalized = _normalize_relative_path(previous_root)
        managed.setdefault(normalized.casefold(), normalized)
    return tuple(managed.values())


def _entry_payload(entry: P115StrmCacheEntry) -> dict[str, Any]:
    return {
        "id": entry.id,
        "parent_id": entry.parent_id,
        "name": entry.name,
        "local_path": entry.local_path,
        "root_id": entry.root_id,
        "entry_kind": entry.entry_kind,
        "sha1": entry.sha1,
        "pick_code": entry.pick_code,
        "size": entry.size,
        "strm_content_sha256": entry.strm_content_sha256,
    }


def cache_snapshot_revision(entries: Sequence[P115StrmCacheEntry]) -> str:
    payload = [_entry_payload(entry) for entry in sorted(entries, key=lambda value: (value.root_id, value.id))]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _logically_changed_paths(
    snapshot: P115StrmCacheSnapshot,
    desired: Sequence[P115StrmCacheEntry],
    library: P115LocalStrmLibrary,
) -> tuple[str, ...]:
    current = {entry.id: entry for entry in snapshot.entries if entry.entry_kind == "file"}
    changed: list[str] = []
    for entry in desired:
        if entry.entry_kind != "file":
            continue
        previous = current.get(entry.id)
        if previous != entry:
            relative_strm = _replace_suffix(entry.local_path, ".strm")
            changed.append(library.absolute_path(relative_strm))
    return tuple(changed)


def _logically_deleted_projections(
    snapshot: P115StrmCacheSnapshot,
    desired: Sequence[P115StrmCacheEntry],
) -> tuple[tuple[str, str], ...]:
    desired_by_id = {entry.id: entry for entry in desired if entry.entry_kind == "file"}
    deleted: list[tuple[str, str]] = []
    for entry in snapshot.entries:
        if entry.entry_kind != "file":
            continue
        current = desired_by_id.get(entry.id)
        previous_strm = _replace_suffix(entry.local_path, ".strm")
        if current is None or _replace_suffix(current.local_path, ".strm") != previous_strm:
            digest = str(entry.strm_content_sha256 or "").strip().lower()
            if len(digest) != 64:
                raise RuntimeError(f"115 缓存缺少可证明旧 STRM ownership 的内容摘要，拒绝规划删除: {previous_strm}")
            try:
                bytes.fromhex(digest)
            except ValueError as exc:
                raise RuntimeError(
                    f"115 缓存缺少可证明旧 STRM ownership 的内容摘要，拒绝规划删除: {previous_strm}"
                ) from exc
            deleted.append((previous_strm, digest))
    return tuple(dict.fromkeys(deleted))


def _notification_paths(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise RuntimeError(f"持久 STRM action plan 的 {field} 不是数组")
    paths = tuple(str(path or "").strip() for path in value)
    if any(not path for path in paths) or len(set(paths)) != len(paths):
        raise RuntimeError(f"持久 STRM action plan 的 {field} 为空或重复")
    return paths


def _outcome_details(**values: Any) -> dict[str, Any]:
    scope = values.pop("scope", "full")
    return {
        "scope": scope,
        "roots": int(values.pop("roots", 0)),
        "account_bound": True,
        "backend_pinned": True,
        "complete_scan_before_local_effects": True,
        "mediainfo_deferred_to_p6_6": True,
        "watchdog_deferred_to_p6_7": True,
        "home_video_enrichment_deferred_to_p6_6": scope == "home_video",
        **values,
    }


__all__ = [
    "P115DesiredLocalAsset",
    "P115LocalDeleteAction",
    "P115LocalPathState",
    "P115LocalReconcileResult",
    "P115LocalStrmLibrary",
    "P115LocalStrmLibraryGateway",
    "P115LocalWriteResult",
    "P115StrmAssetKind",
    "P115StrmCacheEntry",
    "P115StrmCacheReconcileResult",
    "P115StrmCacheSnapshot",
    "P115StrmMediaInfoEnricher",
    "P115StrmMediaInfoEnrichmentResult",
    "P115StrmNotificationRecovery",
    "P115StrmReplaceService",
    "P115StrmSyncClient",
    "P115StrmSyncCoordinator",
    "P115StrmSyncGateway",
    "P115StrmSyncRepository",
    "P115StrmSyncRoot",
    "P115StrmSyncPlan",
    "P115StrmSyncService",
    "cache_snapshot_revision",
]
