from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import bindparam, text

from etk.domains.media.subscription_projection import SubscriptionMediaKey
from etk.platform.db import SqlAlchemyUnitOfWork

from .operation_repository import PostgresSubscriptionOperationRepository
from .operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationReservation,
)

SUBSCRIPTION_CORE_USE_CASE_PROVIDER = "subscription.core"

_ALLOWED_STATUSES = frozenset({"WANTED", "SUBSCRIBED", "IGNORED", "NONE", "PENDING_RELEASE"})
_ACTIVE_CLEANUP_STATUSES = ("queued", "claimed", "effect_started")
_ACTIVE_RESUBSCRIBE_STATUSES = (
    "ready",
    "claimed",
    "requesting",
    "awaiting_readback",
    "remote_confirmed",
    "retryable",
    "indeterminate",
)

logger = logging.getLogger(__name__)


class SubscriptionStateConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SubscriptionTransition:
    operation_id: UUID | None = None
    mutated: bool = True


@dataclass(frozen=True, slots=True)
class SubscriptionBatchDeleteResult:
    deleted_count: int
    reset_count: int
    operation_count: int


@dataclass(frozen=True, slots=True)
class SubscriptionHttpResult:
    payload: Any
    status_code: int = 200


class SubscriptionMediaProjectionPort(Protocol):
    def list_subscriptions(self, uow: SqlAlchemyUnitOfWork) -> list[dict[str, Any]]: ...

    def lock_item(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
    ) -> Mapping[str, Any] | None: ...

    def set_status(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
        status: str,
        request: Mapping[str, Any],
        source: Mapping[str, Any] | None,
        ignore_reason: str | None,
    ) -> int: ...

    def reset_item(self, uow: SqlAlchemyUnitOfWork, key: SubscriptionMediaKey) -> int: ...

    def delete_item(self, uow: SqlAlchemyUnitOfWork, key: SubscriptionMediaKey) -> int: ...


class SubscriptionOperationStagePort(Protocol):
    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation: ...


class SubscriptionAvailabilityPort(Protocol):
    def status(self) -> Mapping[str, bool]: ...


class SubscriptionSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


class SubscriptionIdentityPort(Protocol):
    def display_names(self, user_ids: Sequence[str]) -> Mapping[str, str]: ...


class SubscriptionCoreRepositoryPort(Protocol):
    def list_subscriptions(self) -> list[dict[str, Any]]: ...

    def transition(self, request: Mapping[str, Any], target_status: str) -> SubscriptionTransition: ...

    def batch_delete(self, values: Sequence[Mapping[str, Any]]) -> SubscriptionBatchDeleteResult: ...


class PostgresCoreSubscriptionOperationStager:
    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation:
        return PostgresSubscriptionOperationRepository.reserve_in_uow(uow, intent)


class SqlAlchemySubscriptionCoreRepository:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media: SubscriptionMediaProjectionPort,
        operations: SubscriptionOperationStagePort,
        settings: SubscriptionSettingsPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media = media
        self._operations = operations
        self._settings = settings

    def list_subscriptions(self) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            subscriptions = self._media.list_subscriptions(uow)
        return subscriptions

    def transition(self, request: Mapping[str, Any], target_status: str) -> SubscriptionTransition:
        key = SubscriptionMediaKey.from_mapping(request)
        target = str(target_status).strip().upper()
        if target not in _ALLOWED_STATUSES:
            raise ValueError(f"unsupported subscription status {target_status!r}")
        mp_configured = self._moviepilot_configured()
        with self._unit_of_work() as uow:
            snapshot = self._media.lock_item(uow, key)
            current_status = str(snapshot.get("subscription_status") or "NONE") if snapshot else "NONE"
            operation_id: UUID | None = None
            if target in {"NONE", "IGNORED"} and current_status == "SUBSCRIBED" and mp_configured:
                assert snapshot is not None
                reservation = self._operations.stage(uow, _delete_intent(key, snapshot))
                operation_id = reservation.operation.id
            source = _source_for_status(request, target)
            ignore_reason = _ignore_reason(request, target)
            mutated = self._media.set_status(uow, key, target, request, source, ignore_reason)
            if mutated <= 0:
                raise SubscriptionStateConflictError("媒体项当前状态不允许执行该状态变更")
            if target == "SUBSCRIBED" and mp_configured:
                current = self._media.lock_item(uow, key)
                if current is not None:
                    reservation = self._operations.stage(uow, _resume_intent(key, current))
                    operation_id = reservation.operation.id
            uow.commit()
        return SubscriptionTransition(operation_id=operation_id, mutated=True)

    def batch_delete(self, values: Sequence[Mapping[str, Any]]) -> SubscriptionBatchDeleteResult:
        keys = tuple(dict.fromkeys(SubscriptionMediaKey.from_mapping(value) for value in values))
        deleted_count = 0
        reset_count = 0
        operation_count = 0
        mp_configured = self._moviepilot_configured()
        with self._unit_of_work() as uow:
            for key in keys:
                snapshot = self._media.lock_item(uow, key)
                if snapshot is None:
                    continue
                self._ensure_batch_delete_available(uow, key, snapshot)
                current_status = str(snapshot.get("subscription_status") or "NONE")
                if current_status == "SUBSCRIBED" and mp_configured:
                    reservation = self._operations.stage(uow, _delete_intent(key, snapshot))
                    operation_count += int(reservation.created)
                if bool(snapshot.get("in_library")):
                    reset_count += self._media.reset_item(uow, key)
                else:
                    self._clear_related_indexes(uow, key, snapshot)
                    deleted_count += self._media.delete_item(uow, key)
            uow.commit()
        return SubscriptionBatchDeleteResult(deleted_count, reset_count, operation_count)

    def _moviepilot_configured(self) -> bool:
        value = self._settings.load("mp_config")
        return isinstance(value, Mapping) and bool(str(value.get("moviepilot_url") or "").strip())

    @staticmethod
    def _ensure_batch_delete_available(
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
        snapshot: Mapping[str, Any],
    ) -> None:
        active_cleanup = uow.session.scalar(
            text(
                """
                SELECT TRUE
                FROM cleanup_execution_items AS execution
                LEFT JOIN cleanup_index AS cleanup
                  ON cleanup.id = execution.cleanup_index_id
                WHERE execution.status IN :statuses
                  AND (
                        (cleanup.tmdb_id = :tmdb_id AND cleanup.item_type = :item_type)
                     OR EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(
                                CASE
                                    WHEN jsonb_typeof(execution.snapshot_json->'cleanup_indexes') = 'array'
                                    THEN execution.snapshot_json->'cleanup_indexes'
                                    ELSE '[]'::jsonb
                                END
                            ) AS cleanup_ref(value)
                            WHERE cleanup_ref.value->>'tmdb_id' = :tmdb_id
                              AND cleanup_ref.value->>'item_type' = :item_type
                        )
                  )
                LIMIT 1
                """
            ).bindparams(bindparam("statuses", expanding=True)),
            {
                "tmdb_id": key.tmdb_id,
                "item_type": key.item_type,
                "statuses": _ACTIVE_CLEANUP_STATUSES,
            },
        )
        if active_cleanup:
            raise SubscriptionStateConflictError("媒体项正在执行清理任务，不能批量删除")
        provider_tmdb_id, season = _provider_identity(key, snapshot)
        prefix = (
            f"resubscribe:series:{provider_tmdb_id}:season:{season}:"
            if season is not None
            else f"resubscribe:movie:{provider_tmdb_id}:"
        )
        active_resubscribe = uow.session.scalar(
            text(
                """
                SELECT TRUE
                FROM subscription_operations
                WHERE left(scope_key, length(:prefix)) = :prefix
                  AND status IN :statuses
                LIMIT 1
                """
            ).bindparams(bindparam("statuses", expanding=True)),
            {"prefix": prefix, "statuses": _ACTIVE_RESUBSCRIBE_STATUSES},
        )
        if active_resubscribe:
            raise SubscriptionStateConflictError("媒体项正在执行洗版订阅操作，不能批量删除")

    @staticmethod
    def _clear_related_indexes(
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
        snapshot: Mapping[str, Any],
    ) -> None:
        uow.session.execute(
            text("DELETE FROM cleanup_index WHERE tmdb_id = :tmdb_id AND item_type = :item_type"),
            {"tmdb_id": key.tmdb_id, "item_type": key.item_type},
        )
        provider_tmdb_id, season = _provider_identity(key, snapshot)
        if season is None:
            uow.session.execute(
                text(
                    """
                    DELETE FROM resubscribe_index
                    WHERE tmdb_id IN (:local_tmdb_id, :provider_tmdb_id)
                      AND item_type = :item_type
                    """
                ),
                {
                    "local_tmdb_id": key.tmdb_id,
                    "provider_tmdb_id": provider_tmdb_id,
                    "item_type": key.item_type,
                },
            )
        else:
            uow.session.execute(
                text(
                    """
                    DELETE FROM resubscribe_index
                    WHERE tmdb_id IN (:local_tmdb_id, :provider_tmdb_id)
                      AND item_type = 'Season'
                      AND season_number = :season_number
                    """
                ),
                {
                    "local_tmdb_id": key.tmdb_id,
                    "provider_tmdb_id": provider_tmdb_id,
                    "season_number": season,
                },
            )


class SubscriptionCoreService:
    def __init__(
        self,
        repository: SubscriptionCoreRepositoryPort,
        availability: SubscriptionAvailabilityPort,
        identities: SubscriptionIdentityPort,
    ) -> None:
        self._repository = repository
        self._availability = availability
        self._identities = identities

    def status(self) -> SubscriptionHttpResult:
        return SubscriptionHttpResult({"success": True, **dict(self._availability.status())})

    def update_status(self, requests: object) -> SubscriptionHttpResult:
        if not isinstance(requests, list) or not requests:
            return SubscriptionHttpResult({"error": "'requests' 必须是一个非空列表"}, 400)
        processed = 0
        errors: list[str] = []
        for raw in requests:
            if not isinstance(raw, Mapping):
                errors.append(f"无效请求项，必须是对象: {raw}")
                continue
            tmdb_id = raw.get("tmdb_id")
            item_type = raw.get("item_type")
            status = str(raw.get("new_status") or "").upper()
            if not tmdb_id or not item_type or not status:
                errors.append(f"无效请求项，缺少 tmdb_id, item_type 或 new_status: {dict(raw)}")
                continue
            if status not in _ALLOWED_STATUSES:
                errors.append(f"无效的状态 '{raw.get('new_status')}' for TMDb ID {tmdb_id}")
                continue
            try:
                transition = self._repository.transition(raw, status)
                processed += int(transition.mutated)
            except Exception as exc:
                logger.exception("Subscription state transition failed for %s", tmdb_id)
                errors.append(f"处理 TMDb ID {tmdb_id} 状态变更时发生错误: {exc}")
        if processed:
            message = f"已成功提交 {processed} 个媒体项的状态变更请求。"
            if errors:
                message += f" 但有 {len(errors)} 个请求处理失败。"
            return SubscriptionHttpResult({"message": message, "errors": errors})
        return SubscriptionHttpResult({"error": "没有有效的媒体项被成功处理。", "errors": errors}, 400)

    def list_subscriptions(self) -> SubscriptionHttpResult:
        try:
            items = deepcopy(self._repository.list_subscriptions())
            user_ids = {
                str(source.get("user_id"))
                for item in items
                for source in _source_mappings(item.get("subscription_sources_json"))
                if source.get("type") == "user_request" and source.get("user_id")
            }
            names = dict(self._identities.display_names(tuple(sorted(user_ids)))) if user_ids else {}
            for item in items:
                sources = item.get("subscription_sources_json")
                if not isinstance(sources, list):
                    continue
                for source in sources:
                    if not isinstance(source, dict) or source.get("type") != "user_request":
                        continue
                    user_id = str(source.get("user_id") or "")
                    if user_id:
                        source["user"] = names.get(user_id, "未知用户")
            return SubscriptionHttpResult(items)
        except Exception:
            logger.exception("Reading subscription management projection failed")
            return SubscriptionHttpResult({"error": "获取订阅列表时发生服务器内部错误"}, 500)

    def batch_delete(self, items: object) -> SubscriptionHttpResult:
        if not isinstance(items, list) or not items:
            return SubscriptionHttpResult({"error": "请求体必须包含非空的 'items' 列表"}, 400)
        normalized = [item for item in items if isinstance(item, Mapping)]
        if not normalized:
            return SubscriptionHttpResult({"error": "请求体必须包含非空的 'items' 列表"}, 400)
        try:
            result = self._repository.batch_delete(normalized)
        except SubscriptionStateConflictError as exc:
            return SubscriptionHttpResult({"error": str(exc)}, 409)
        except Exception:
            logger.exception("Batch subscription deletion failed")
            return SubscriptionHttpResult({"error": "删除操作发生内部错误"}, 500)
        return SubscriptionHttpResult(
            {
                "message": f"成功删除了 {result.deleted_count} 条记录。",
                "deleted_count": result.deleted_count,
            }
        )


def _source_for_status(request: Mapping[str, Any], status: str) -> Mapping[str, Any] | None:
    raw = request.get("source")
    if isinstance(raw, Mapping):
        return dict(raw)
    defaults = {
        "WANTED": "manual_add",
        "SUBSCRIBED": "manual_subscribe",
        "IGNORED": "manual_ignore",
    }
    source_type = defaults.get(status)
    return {"type": source_type} if source_type is not None else None


def _ignore_reason(request: Mapping[str, Any], status: str) -> str | None:
    if status != "IGNORED":
        return None
    return str(request.get("ignore_reason") or request.get("reason") or "手动忽略")


def _provider_identity(
    key: SubscriptionMediaKey,
    snapshot: Mapping[str, Any],
) -> tuple[str, int | None]:
    if key.item_type != "Season":
        return key.tmdb_id, None
    parent = str(snapshot.get("parent_series_tmdb_id") or "").strip()
    season = snapshot.get("season_number")
    if not parent or isinstance(season, bool) or not isinstance(season, int) or season < 0:
        raise SubscriptionStateConflictError("季媒体项缺少父剧集 TMDb ID 或季号")
    return parent, season


def _subscription_generation(snapshot: Mapping[str, Any]) -> str:
    value = (
        snapshot.get("last_subscribed_at")
        or snapshot.get("last_updated_at")
        or snapshot.get("first_requested_at")
        or "legacy"
    )
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def _provider_scope(provider_tmdb_id: str, season: int | None) -> str:
    if season is None:
        return f"movie:{provider_tmdb_id}"
    return f"series:{provider_tmdb_id}:season:{season}"


def _delete_intent(
    key: SubscriptionMediaKey,
    snapshot: Mapping[str, Any],
) -> SubscriptionOperationIntent:
    provider_tmdb_id, season = _provider_identity(key, snapshot)
    generation = _subscription_generation(snapshot)
    request: dict[str, Any] = {
        "action": "delete",
        "tmdb_id": provider_tmdb_id,
        "expected": {},
        "provider": "moviepilot",
        "generation": generation,
    }
    if season is not None:
        request["season"] = season
    scope = _provider_scope(provider_tmdb_id, season)
    return SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.DELETE,
        scope_key=f"subscription-state:{scope}:{generation}:delete",
        request=request,
        available_at=datetime.now(UTC),
    )


def _resume_intent(
    key: SubscriptionMediaKey,
    snapshot: Mapping[str, Any],
) -> SubscriptionOperationIntent:
    provider_tmdb_id, season = _provider_identity(key, snapshot)
    generation = _subscription_generation(snapshot)
    request: dict[str, Any] = {
        "action": "update_status",
        "tmdb_id": provider_tmdb_id,
        "payload": {"status": "R"},
        "expected": {},
        "provider": "moviepilot",
        "generation": generation,
    }
    if season is not None:
        request["season"] = season
    scope = _provider_scope(provider_tmdb_id, season)
    return SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.UPDATE,
        scope_key=f"subscription-state:{scope}:{generation}:resume",
        request=request,
        available_at=datetime.now(UTC),
    )


def _source_mappings(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


__all__ = [
    "PostgresCoreSubscriptionOperationStager",
    "SUBSCRIPTION_CORE_USE_CASE_PROVIDER",
    "SqlAlchemySubscriptionCoreRepository",
    "SubscriptionBatchDeleteResult",
    "SubscriptionCoreService",
    "SubscriptionHttpResult",
    "SubscriptionStateConflictError",
    "SubscriptionTransition",
]
