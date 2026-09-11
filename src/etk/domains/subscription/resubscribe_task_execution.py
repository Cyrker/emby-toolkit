"""Processor-free execution for the durable resubscribe task."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.domains.sharing import (
    PostgresSharingOperationRepository,
    SharingOperationExecutor,
)
from etk.domains.sharing.operations import (
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationScopeBusyError,
    SharingOperationStatus,
)
from etk.integrations import ExternalAdapterError, HDHivePort, ProviderDisposition
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome

from .hdhive_policy import filter_hdhive_resources, normalize_filter_config
from .resubscribe import (
    ResubscribeBatchReservation,
    ResubscribeItemKey,
    ResubscribeMediaProjectionPort,
    ResubscribePolicyPort,
    ResubscribePolicySnapshot,
    UnsupportedResubscribeBatchError,
)
from .runtime_repository import (
    SubscriptionQuotaExhaustedError,
    SubscriptionQuotaReservation,
    SubscriptionQuotaStatus,
)

RESUBSCRIBE_TASK_KEY = "resubscribe-library"


class ResubscribeTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ResubscribeExecutionItem:
    key: ResubscribeItemKey
    action: str
    source: str
    generation: str
    item_name: str
    cleanup_emby_ids: tuple[str, ...] = ()


class ResubscribeExecutionRepository(Protocol):
    def list_needed_items(self) -> Sequence[ResubscribeExecutionItem]: ...

    def is_current(self, item: ResubscribeExecutionItem) -> bool: ...

    def mark_status(self, key: ResubscribeItemKey, status: str) -> None: ...


class ResubscribeMoviePilotStager(Protocol):
    def stage_replacements(self, keys: Sequence[ResubscribeItemKey]) -> ResubscribeBatchReservation: ...


class ResubscribeCleanupStager(Protocol):
    def stage_version_deletion(
        self,
        emby_id: str,
        *,
        progress: ResubscribeTaskProgress,
    ) -> Mapping[str, Any]: ...


class ResubscribeQuotaPort(Protocol):
    def reserve_quota(
        self,
        idempotency_key: str,
        *,
        quota_date: date,
        daily_cap: int,
        units: int = 1,
        ttl_seconds: float = 3600,
        operation_id: UUID | None = None,
    ) -> SubscriptionQuotaReservation: ...

    def commit_quota(self, reservation_id: UUID) -> SubscriptionQuotaReservation: ...

    def release_quota(self, reservation_id: UUID) -> SubscriptionQuotaReservation: ...


class ResubscribeHDHiveDisposition(StrEnum):
    CONFIRMED = "confirmed"
    SKIPPED = "skipped"
    RETRYABLE = "retryable"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class ResubscribeHDHiveResult:
    disposition: ResubscribeHDHiveDisposition
    operation_id: UUID | None = None
    error: Mapping[str, Any] | None = None


class ResubscribeHDHivePort(Protocol):
    def execute(
        self,
        item: ResubscribeExecutionItem,
        *,
        occurrence: str,
        progress: ResubscribeTaskProgress,
    ) -> ResubscribeHDHiveResult: ...


class NativeResubscribeTaskService:
    """Dispatch each frozen rule to its existing durable effect owner."""

    def __init__(
        self,
        repository: ResubscribeExecutionRepository,
        moviepilot: ResubscribeMoviePilotStager,
        cleanup: ResubscribeCleanupStager,
        hdhive: ResubscribeHDHivePort,
        quota: ResubscribeQuotaPort,
        policy: ResubscribePolicyPort,
    ) -> None:
        self._repository = repository
        self._moviepilot = moviepilot
        self._cleanup = cleanup
        self._hdhive = hdhive
        self._quota = quota
        self._policy = policy

    def execute(
        self,
        task_key: str,
        progress: ResubscribeTaskProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        if task_key != RESUBSCRIBE_TASK_KEY:
            raise LookupError(f"unsupported resubscribe task key: {task_key!r}")
        if parameters:
            raise ValueError("resubscribe-library does not accept task parameters")

        progress.update(5, "正在读取待洗版项目...")
        progress.checkpoint()
        items = tuple(self._repository.list_needed_items())
        if not items:
            progress.update(100, "没有需要洗版的媒体项目。")
            return TaskOutcome.skipped(
                "没有需要洗版的媒体项目。",
                self._details(requested=0),
            )

        moviepilot_staged = 0
        cleanup_staged = 0
        cleanup_reused = 0
        hdhive_confirmed = 0
        retired_source_skipped = 0
        business_skipped = 0
        quota_exhausted = False
        failures: list[dict[str, Any]] = []

        for index, item in enumerate(items, 1):
            progress.checkpoint()
            progress.update(
                max(5, min(95, int((index - 1) * 90 / len(items)) + 5)),
                f"({index}/{len(items)}) 正在处理: {item.item_name}",
            )
            if not self._repository.is_current(item):
                failures.append(self._failure(item, "resubscribe_rule_changed"))
                continue
            if item.action == "delete":
                staged, reused, item_failures = self._stage_cleanup(item, progress)
                cleanup_staged += staged
                cleanup_reused += reused
                failures.extend(item_failures)
                continue
            if item.action != "resubscribe":
                failures.append(self._failure(item, "unsupported_resubscribe_action"))
                continue
            if item.source == "moviepilot":
                try:
                    reservation = self._moviepilot.stage_replacements((item.key,))
                except SubscriptionQuotaExhaustedError:
                    quota_exhausted = True
                    break
                except UnsupportedResubscribeBatchError:
                    failures.append(self._failure(item, "resubscribe_rule_changed"))
                except Exception as exc:
                    failures.append(
                        self._failure(item, "moviepilot_resubscribe_staging_failed", exc=exc, retryable=True)
                    )
                else:
                    staged_count = len(tuple(reservation.operation_ids))
                    moviepilot_staged += staged_count
                    if staged_count == 0:
                        failures.append(self._failure(item, "moviepilot_resubscribe_not_staged"))
                continue
            if item.source == "hdhive":
                retired_source_skipped += 1
                business_skipped += 1
                continue
            failures.append(self._failure(item, "unsupported_resubscribe_source"))

        details = self._details(
            requested=len(items),
            moviepilot_staged=moviepilot_staged,
            cleanup_staged=cleanup_staged,
            cleanup_reused=cleanup_reused,
            hdhive_confirmed=hdhive_confirmed,
            retired_source_skipped=retired_source_skipped,
            business_skipped=business_skipped,
            quota_exhausted=quota_exhausted,
            failures=failures,
        )
        accepted = moviepilot_staged + cleanup_staged + hdhive_confirmed
        if failures:
            message = "媒体洗版任务部分提交。" if accepted else "媒体洗版任务提交失败。"
            return TaskOutcome.partial(message, details) if accepted else TaskOutcome.failed(message, details)
        if quota_exhausted:
            message = "媒体洗版任务部分提交，订阅配额已用尽。" if accepted else "订阅配额已用尽。"
            return TaskOutcome.partial(message, details) if accepted else TaskOutcome.skipped(message, details)
        if accepted == 0:
            message = (
                "历史影巢洗版项已跳过；影巢功能已下线。" if retired_source_skipped else "没有可提交的媒体洗版操作。"
            )
            progress.update(100, message)
            return TaskOutcome.skipped(message, details)
        progress.update(100, "媒体洗版任务已提交。")
        return TaskOutcome.succeeded("媒体洗版任务已提交。", details)

    def _stage_cleanup(
        self,
        item: ResubscribeExecutionItem,
        progress: ResubscribeTaskProgress,
    ) -> tuple[int, int, list[dict[str, Any]]]:
        if not item.cleanup_emby_ids:
            return 0, 0, [self._failure(item, "cleanup_targets_missing")]
        staged = 0
        reused = 0
        failures: list[dict[str, Any]] = []
        for emby_id in item.cleanup_emby_ids:
            progress.checkpoint()
            try:
                result = self._cleanup.stage_version_deletion(emby_id, progress=progress)
            except Exception as exc:
                failures.append(
                    self._failure(
                        item,
                        "cleanup_staging_failed",
                        exc=exc,
                        retryable=True,
                        emby_id=emby_id,
                    )
                )
                continue
            staged += 1
            reused += int(bool(result.get("reused", False)))
        return staged, reused, failures

    def _execute_hdhive(
        self,
        item: ResubscribeExecutionItem,
        progress: ResubscribeTaskProgress,
        policy: ResubscribePolicySnapshot,
    ) -> tuple[ResubscribeHDHiveResult | None, bool]:
        occurrence = f"resubscribe:{item.key.scope_key}:{item.generation}:hdhive"
        try:
            reservation = self._quota.reserve_quota(
                occurrence,
                quota_date=policy.quota_date,
                daily_cap=policy.daily_cap,
                ttl_seconds=86_400,
            )
        except SubscriptionQuotaExhaustedError:
            return None, True
        except Exception:
            return (
                ResubscribeHDHiveResult(
                    ResubscribeHDHiveDisposition.RETRYABLE,
                    error={"type": "hdhive_quota_reservation_failed"},
                ),
                False,
            )

        outcome = self._hdhive.execute(item, occurrence=occurrence, progress=progress)

        if outcome.disposition is ResubscribeHDHiveDisposition.CONFIRMED:
            self._commit_confirmed_quota(reservation, occurrence, policy)
            self._repository.mark_status(item.key, "subscribed")
        elif outcome.disposition is ResubscribeHDHiveDisposition.INDETERMINATE:
            self._commit_uncertain_quota(reservation)
            self._repository.mark_status(item.key, "indeterminate")
        elif outcome.disposition is ResubscribeHDHiveDisposition.SKIPPED:
            self._release_quota(reservation)
        elif outcome.operation_id is None:
            # Discovery/configuration failures happen before any durable write.
            self._release_quota(reservation)
        return outcome, False

    def _commit_confirmed_quota(
        self,
        reservation: SubscriptionQuotaReservation,
        occurrence: str,
        policy: ResubscribePolicySnapshot,
    ) -> None:
        if reservation.status is SubscriptionQuotaStatus.COMMITTED:
            return
        if reservation.status is SubscriptionQuotaStatus.RESERVED:
            self._quota.commit_quota(reservation.id)
            return
        late = self._quota.reserve_quota(
            f"{occurrence}:confirmed",
            quota_date=policy.quota_date,
            daily_cap=policy.daily_cap,
            ttl_seconds=86_400,
        )
        if late.status is SubscriptionQuotaStatus.RESERVED:
            self._quota.commit_quota(late.id)

    def _commit_uncertain_quota(self, reservation: SubscriptionQuotaReservation) -> None:
        if reservation.status is SubscriptionQuotaStatus.RESERVED:
            self._quota.commit_quota(reservation.id)

    def _release_quota(self, reservation: SubscriptionQuotaReservation) -> None:
        if reservation.status is SubscriptionQuotaStatus.RESERVED:
            self._quota.release_quota(reservation.id)

    @staticmethod
    def _failure(
        item: ResubscribeExecutionItem,
        error_type: str,
        *,
        exc: BaseException | None = None,
        retryable: bool = False,
        indeterminate: bool = False,
        operation_id: UUID | None = None,
        emby_id: str | None = None,
    ) -> dict[str, Any]:
        failure: dict[str, Any] = {
            "item_id": item.key.item_id,
            "action": item.action,
            "source": item.source,
            "error_type": error_type,
            "retryable": retryable,
        }
        if exc is not None:
            failure["error_class"] = type(exc).__name__
        if indeterminate:
            failure["indeterminate"] = True
        if operation_id is not None:
            failure["operation_id"] = str(operation_id)
        if emby_id is not None:
            failure["emby_id"] = emby_id
        return failure

    @staticmethod
    def _details(
        *,
        requested: int,
        moviepilot_staged: int = 0,
        cleanup_staged: int = 0,
        cleanup_reused: int = 0,
        hdhive_confirmed: int = 0,
        retired_source_skipped: int = 0,
        business_skipped: int = 0,
        quota_exhausted: bool = False,
        failures: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        normalized_failures = [dict(value) for value in failures]
        return {
            "requested": requested,
            "moviepilot_staged": moviepilot_staged,
            "cleanup_staged": cleanup_staged,
            "cleanup_reused": cleanup_reused,
            "hdhive_confirmed": hdhive_confirmed,
            "retired_source_skipped": retired_source_skipped,
            "business_skipped": business_skipped,
            "quota_exhausted": quota_exhausted,
            "provider_failures": normalized_failures[:50],
            "failed": len(normalized_failures),
            "retryable": any(value.get("retryable") is True for value in normalized_failures),
        }


class SqlAlchemyResubscribeExecutionRepository:
    """Freeze the rule, local media identity, and cleanup targets for one task run."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media_projection: ResubscribeMediaProjectionPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media_projection = media_projection

    def list_needed_items(self) -> Sequence[ResubscribeExecutionItem]:
        with self._unit_of_work() as uow:
            rows = tuple(
                dict(row)
                for row in uow.session.execute(
                    text(
                        """
                        SELECT idx.tmdb_id, idx.item_type, idx.season_number,
                               idx.last_checked_at, idx.matched_rule_id,
                               rule.enabled, rule.rule_type, rule.resubscribe_source
                        FROM resubscribe_index AS idx
                        JOIN resubscribe_rules AS rule ON rule.id = idx.matched_rule_id
                        WHERE idx.status = 'needed' AND rule.enabled IS TRUE
                        ORDER BY idx.tmdb_id, idx.item_type, idx.season_number
                        """
                    )
                )
                .mappings()
                .all()
            )
            keys = tuple(
                ResubscribeItemKey(
                    str(row["tmdb_id"]),
                    str(row["item_type"]),
                    int(row["season_number"]),
                )
                for row in rows
            )
            descriptions = self._media_projection.describe_items(uow, keys)
            items: list[ResubscribeExecutionItem] = []
            for row, key in zip(rows, keys, strict=True):
                if not key.tmdb_id or key.item_type not in {"Movie", "Season"}:
                    continue
                if key.item_type == "Season" and key.season_number < 1:
                    continue
                action = str(row.get("rule_type") or "missing_rule").strip().casefold()
                source = str(row.get("resubscribe_source") or "moviepilot").strip().casefold()
                description = descriptions.get(key, {})
                cleanup_ids = self._cleanup_emby_ids(uow, key) if action == "delete" else ()
                items.append(
                    ResubscribeExecutionItem(
                        key=key,
                        action=action,
                        source=source,
                        generation=_execution_generation(row),
                        item_name=str(description.get("item_name") or key.item_id),
                        cleanup_emby_ids=cleanup_ids,
                    )
                )
            return tuple(items)

    def is_current(self, item: ResubscribeExecutionItem) -> bool:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT idx.last_checked_at, idx.matched_rule_id,
                               rule.enabled, rule.rule_type, rule.resubscribe_source
                        FROM resubscribe_index AS idx
                        JOIN resubscribe_rules AS rule ON rule.id = idx.matched_rule_id
                        WHERE idx.tmdb_id = :tmdb_id
                          AND idx.item_type = :item_type
                          AND idx.season_number = :season_number
                          AND idx.status = 'needed'
                          AND rule.enabled IS TRUE
                        """
                    ),
                    _key_parameters(item.key),
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return False
        values = dict(row)
        return (
            str(values.get("rule_type") or "missing_rule").strip().casefold() == item.action
            and str(values.get("resubscribe_source") or "moviepilot").strip().casefold() == item.source
            and _execution_generation(values) == item.generation
        )

    def mark_status(self, key: ResubscribeItemKey, status: str) -> None:
        normalized = str(status or "").strip().casefold()
        if normalized not in {"subscribed", "auto_subscribed", "indeterminate"}:
            raise ValueError(f"unsupported resubscribe execution status: {status!r}")
        with self._unit_of_work() as uow:
            current = uow.session.scalar(
                text(
                    """
                    SELECT status
                    FROM resubscribe_index
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                      AND season_number = :season_number
                    FOR UPDATE
                    """
                ),
                _key_parameters(key),
            )
            if current is None or str(current) not in {"needed", "auto_subscribed", normalized}:
                uow.commit()
                return
            if str(current) != normalized:
                uow.session.execute(
                    text(
                        """
                        UPDATE resubscribe_index
                        SET status = :status, last_checked_at = NOW()
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = :item_type
                          AND season_number = :season_number
                        """
                    ),
                    {**_key_parameters(key), "status": normalized},
                )
            uow.commit()

    @staticmethod
    def _cleanup_emby_ids(
        uow: SqlAlchemyUnitOfWork,
        key: ResubscribeItemKey,
    ) -> tuple[str, ...]:
        if key.item_type == "Movie":
            rows = uow.session.execute(
                text(
                    """
                    SELECT emby_item_ids_json, asset_details_json
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    ORDER BY tmdb_id
                    """
                ),
                {"tmdb_id": key.tmdb_id},
            ).mappings()
        else:
            rows = uow.session.execute(
                text(
                    """
                    SELECT emby_item_ids_json, asset_details_json
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = :tmdb_id
                      AND season_number = :season_number
                      AND item_type = 'Episode'
                      AND in_library IS TRUE
                    ORDER BY episode_number NULLS LAST, tmdb_id
                    """
                ),
                {"tmdb_id": key.tmdb_id, "season_number": key.season_number},
            ).mappings()
        identifiers: list[str] = []
        for row in rows:
            for value in _json_list(row.get("emby_item_ids_json")):
                normalized = str(value or "").strip()
                if normalized and normalized not in identifiers:
                    identifiers.append(normalized)
            for value in _json_list(row.get("asset_details_json")):
                if not isinstance(value, Mapping):
                    continue
                normalized = str(value.get("emby_item_id") or value.get("id") or "").strip()
                if normalized and normalized not in identifiers:
                    identifiers.append(normalized)
        return tuple(identifiers)


class NativeResubscribeHDHiveGateway:
    """Select through the typed HDHive read API and execute the existing P9 transfer owner."""

    def __init__(
        self,
        provider: HDHivePort,
        operations: PostgresSharingOperationRepository,
        executor: SharingOperationExecutor,
        configuration: Callable[[], Mapping[str, Any] | None],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> None:
        normalized_worker = str(worker_id or "").strip()
        if not normalized_worker:
            raise ValueError("resubscribe HDHive worker_id must not be empty")
        if float(lease_seconds) <= 0:
            raise ValueError("resubscribe HDHive lease_seconds must be positive")
        self._provider = provider
        self._operations = operations
        self._executor = executor
        self._configuration = configuration
        self._worker_id = normalized_worker
        self._lease_seconds = float(lease_seconds)

    def execute(
        self,
        item: ResubscribeExecutionItem,
        *,
        occurrence: str,
        progress: ResubscribeTaskProgress,
    ) -> ResubscribeHDHiveResult:
        media_type = "movie" if item.key.item_type == "Movie" else "tv"
        season = item.key.season_number if item.key.item_type == "Season" else None
        progress.checkpoint()
        try:
            response = self._provider.get_resources(item.key.tmdb_id, media_type, season=season)
        except ExternalAdapterError as exc:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.RETRYABLE,
                error={"type": "hdhive_resource_read_failed", "disposition": exc.disposition.value},
            )
        except Exception as exc:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.RETRYABLE,
                error={"type": "hdhive_resource_read_failed", "error_class": type(exc).__name__},
            )
        if response.disposition in {ProviderDisposition.NOT_FOUND, ProviderDisposition.REJECTED}:
            return ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.SKIPPED)
        if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.RETRYABLE,
                error={
                    "type": "hdhive_resource_read_unconfirmed",
                    "disposition": response.disposition.value,
                },
            )

        try:
            configuration = self._configuration()
            source = dict(configuration) if isinstance(configuration, Mapping) else {}
            filters = source.get("filter")
            candidates = filter_hdhive_resources(
                _response_rows(response.payload),
                config=normalize_filter_config(filters),
                target_season=season,
                media_type=media_type,
            )
        except Exception as exc:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.RETRYABLE,
                error={"type": "hdhive_filter_failed", "error_class": type(exc).__name__},
            )
        if not candidates:
            return ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.SKIPPED)
        selected = candidates[0] if media_type == "tv" else min(candidates, key=_hdhive_score)
        slug = str(selected.get("slug") or selected.get("resource_id") or "").strip()
        if not slug:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.SKIPPED,
                error={"type": "hdhive_resource_slug_missing"},
            )
        slug_hash = hashlib.sha256(slug.encode("utf-8")).hexdigest()
        intent = SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"resubscribe:hdhive:{slug_hash}",
            principal_id="worker:resubscribe",
            request={
                "provider": "auto_subscription",
                "action": "hdhive_transfer",
                "payload": {
                    "slug": slug,
                    "slug_hash": slug_hash,
                    "tmdb_id": item.key.tmdb_id,
                    "media_type": media_type,
                    "season_number": season,
                },
            },
            idempotency_key=occurrence,
        )
        try:
            operation, _replayed = self._operations.reserve_with_replay(intent)
        except SharingOperationScopeBusyError:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.RETRYABLE,
                error={"type": "hdhive_operation_scope_busy"},
            )
        except SharingOperationConflictError:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.INDETERMINATE,
                operation_id=intent.id,
                error={"type": "hdhive_operation_identity_changed"},
            )
        except Exception as exc:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.RETRYABLE,
                error={"type": "hdhive_operation_reservation_failed", "error_class": type(exc).__name__},
            )

        self._checkpoint_reserved(progress, operation.id)
        execution = self._executor.execute(
            operation.id,
            worker_id=f"{self._worker_id}:resubscribe-hdhive",
            lease_seconds=self._lease_seconds,
            before_request=lambda: self._checkpoint_reserved(progress, operation.id),
        )
        current = execution.operation if execution is not None else self._operations.read(operation.id)
        if current is None:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.INDETERMINATE,
                operation.id,
                {"type": "hdhive_operation_missing"},
            )
        if current.status is SharingOperationStatus.CONFIRMED:
            return ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.CONFIRMED, current.id)
        if current.status in {SharingOperationStatus.REJECTED, SharingOperationStatus.CANCELLED}:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.SKIPPED,
                current.id,
                dict(current.error or {}),
            )
        if current.status is SharingOperationStatus.INDETERMINATE:
            return ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.INDETERMINATE,
                current.id,
                dict(current.error or {"type": "hdhive_operation_indeterminate"}),
            )
        return ResubscribeHDHiveResult(
            ResubscribeHDHiveDisposition.RETRYABLE,
            current.id,
            dict(current.error or {"type": f"hdhive_operation_{current.status.value}"}),
        )

    def _checkpoint_reserved(self, progress: ResubscribeTaskProgress, operation_id: UUID) -> None:
        try:
            progress.checkpoint()
        except BaseException:
            self._operations.request_cancel(operation_id)
            raise


def _key_parameters(key: ResubscribeItemKey) -> dict[str, Any]:
    return {
        "tmdb_id": key.tmdb_id,
        "item_type": key.item_type,
        "season_number": key.season_number,
    }


def _execution_generation(row: Mapping[str, Any]) -> str:
    checked = row.get("last_checked_at")
    last_checked_at = checked.isoformat() if isinstance(checked, datetime) else str(checked or "")
    identity = {
        "last_checked_at": last_checked_at,
        "matched_rule_id": row.get("matched_rule_id"),
        "enabled": row.get("enabled") is True,
        "action": str(row.get("rule_type") or "missing_rule").strip().casefold(),
        "source": str(row.get("resubscribe_source") or "moviepilot").strip().casefold(),
    }
    return hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _json_list(value: Any) -> list[Any]:
    normalized = value
    if isinstance(normalized, str):
        try:
            normalized = json.loads(normalized)
        except (TypeError, ValueError):
            return []
    return list(normalized) if isinstance(normalized, list) else []


def _response_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    queue: list[Any] = [payload]
    visited: set[int] = set()
    while queue:
        value = queue.pop(0)
        if id(value) in visited:
            continue
        visited.add(id(value))
        if isinstance(value, Sequence) and not isinstance(value, str | bytes):
            rows = [dict(item) for item in value if isinstance(item, Mapping)]
            if rows:
                return rows
        if isinstance(value, Mapping):
            for key in ("resources", "items", "results", "data"):
                nested = value.get(key)
                if isinstance(nested, Mapping | list | tuple):
                    queue.append(nested)
    return []


def _hdhive_score(value: Mapping[str, Any]) -> tuple[int, int, int, float, str]:
    try:
        points = max(int(value.get("_effective_points", value.get("unlock_points")) or 0), 0)
    except (TypeError, ValueError):
        points = 0
    try:
        size = max(float(value.get("_size_gb") or 0), 0.0)
    except (TypeError, ValueError):
        size = 0.0
    return (
        0 if bool(value.get("already_owned")) else 1,
        points,
        0 if str(value.get("pan_type") or "115").casefold() == "115" else 1,
        -size,
        str(value.get("slug") or value.get("resource_id") or ""),
    )


__all__ = [
    "NativeResubscribeHDHiveGateway",
    "NativeResubscribeTaskService",
    "RESUBSCRIBE_TASK_KEY",
    "ResubscribeExecutionItem",
    "ResubscribeExecutionRepository",
    "ResubscribeHDHiveDisposition",
    "ResubscribeHDHivePort",
    "ResubscribeHDHiveResult",
    "ResubscribeTaskProgress",
    "SqlAlchemyResubscribeExecutionRepository",
]
