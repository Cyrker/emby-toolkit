from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.platform.db import SqlAlchemyUnitOfWork

from .operation_repository import PostgresSubscriptionOperationRepository
from .operations import (
    SubscriptionOperation,
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationReservation,
    SubscriptionOperationStatus,
    SubscriptionQuotaRequirement,
)
from .runtime_repository import SubscriptionQuotaExhaustedError

RESUBSCRIBE_USE_CASE_PROVIDER = "subscription.resubscribe"

_JSON_RULE_FIELDS = frozenset(
    {
        "scope_rules",
        "resubscribe_audio_missing_languages",
        "resubscribe_subtitle_missing_languages",
        "resubscribe_quality_include",
        "resubscribe_effect_include",
        "resubscribe_codec_include",
    }
)
_RULE_FIELDS = frozenset(
    {
        "name",
        "enabled",
        "scope_rules",
        "sort_order",
        "resubscribe_resolution_enabled",
        "resubscribe_resolution_threshold",
        "resubscribe_audio_enabled",
        "resubscribe_audio_missing_languages",
        "resubscribe_subtitle_enabled",
        "resubscribe_subtitle_missing_languages",
        "resubscribe_quality_enabled",
        "resubscribe_quality_include",
        "resubscribe_effect_enabled",
        "resubscribe_effect_include",
        "resubscribe_subtitle_effect_only",
        "resubscribe_filesize_enabled",
        "resubscribe_filesize_operator",
        "resubscribe_filesize_threshold_gb",
        "resubscribe_codec_enabled",
        "resubscribe_codec_include",
        "resubscribe_subtitle_skip_if_audio_exists",
        "custom_resubscribe_enabled",
        "consistency_check_enabled",
        "consistency_must_match_resolution",
        "consistency_must_match_group",
        "consistency_must_match_code",
        "consistency_must_match_codec",
        "rule_type",
        "filter_rating_enabled",
        "filter_rating_min",
        "delete_mode",
        "delete_delay_seconds",
        "filter_rating_ignore_zero",
        "filter_missing_episodes_enabled",
        "resubscribe_source",
        "resubscribe_entire_season",
    }
)

logger = logging.getLogger(__name__)


class DuplicateResubscribeRuleError(RuntimeError):
    pass


class ResubscribeTaskBusyError(RuntimeError):
    pass


class UnsupportedResubscribeBatchError(RuntimeError):
    def __init__(self, item_ids: Sequence[str]) -> None:
        self.item_ids = tuple(item_ids)
        super().__init__("batch contains actions owned by a later adapter stage")


@dataclass(frozen=True, slots=True)
class ResubscribeResult:
    payload: Any
    status_code: int = 200


@dataclass(frozen=True, slots=True)
class ResubscribeItemKey:
    tmdb_id: str
    item_type: str
    season_number: int = -1

    @classmethod
    def parse(cls, value: object) -> ResubscribeItemKey | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            parts = value.split("-")
            if len(parts) < 2:
                return None
            season_number = -1
            if parts[1] == "Season" and len(parts) > 2:
                season_number = int(parts[2].replace("S", ""))
            return cls(parts[0], parts[1], season_number)
        except (TypeError, ValueError):
            return None

    @property
    def item_id(self) -> str:
        suffix = f"-S{self.season_number}" if self.item_type == "Season" else ""
        return f"{self.tmdb_id}-{self.item_type}{suffix}"

    @property
    def scope_key(self) -> str:
        if self.item_type == "Season":
            return f"series:{self.tmdb_id}:season:{self.season_number}"
        return f"movie:{self.tmdb_id}"


@dataclass(frozen=True, slots=True)
class ResubscribeTaskIntent:
    task_key: str
    display_name: str
    processor_type: str = "media"


@dataclass(frozen=True, slots=True)
class ResubscribeBatchReservation:
    operation_ids: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class ResubscribePolicySnapshot:
    quota_date: date
    daily_cap: int
    delay_seconds: float
    language_keywords: Mapping[str, Sequence[str]]
    release_group_mapping: Mapping[str, Sequence[str]]


class ResubscribePolicyPort(Protocol):
    def snapshot(self) -> ResubscribePolicySnapshot: ...


class WashingAction(StrEnum):
    ACCEPT = "ACCEPT"
    REPLACE = "REPLACE"
    SKIP = "SKIP"
    REJECT = "REJECT"


@dataclass(frozen=True, slots=True)
class WashingCandidate:
    attributes: Mapping[str, Any]
    slot_id: str | None = "__single__"
    valid: bool = True


@dataclass(frozen=True, slots=True)
class WashingDecision:
    action: WashingAction
    reason: str
    new_level: int | None = None
    best_existing_level: int | None = None


class ResubscribeMediaProjectionPort(Protocol):
    def describe_items(
        self,
        uow: SqlAlchemyUnitOfWork,
        keys: Sequence[ResubscribeItemKey],
    ) -> Mapping[ResubscribeItemKey, Mapping[str, Any]]: ...

    def set_active_washing(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: ResubscribeItemKey,
        enabled: bool,
    ) -> int: ...


class ResubscribeOperationStagePort(Protocol):
    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation: ...


class ResubscribeOperationStorePort(Protocol):
    def read(self, operation_id: UUID) -> SubscriptionOperation | None: ...

    def reserve(self, intent: SubscriptionOperationIntent) -> SubscriptionOperationReservation: ...


class ResubscribeTaskPort(Protocol):
    def busy(self) -> bool: ...

    def submit(self, intent: ResubscribeTaskIntent) -> bool: ...


class ResubscribeLibraryPort(Protocol):
    def ready(self) -> bool: ...

    def list_libraries(self) -> Sequence[Mapping[str, Any]] | None: ...


class ResubscribeRepository(Protocol):
    def list_rules(self) -> list[dict[str, Any]]: ...

    def create_rule(self, data: Mapping[str, Any]) -> int: ...

    def update_rule(self, rule_id: int, data: Mapping[str, Any]) -> bool: ...

    def delete_rule(self, rule_id: int) -> bool: ...

    def order_rules(self, ordered_ids: Sequence[Any]) -> None: ...

    def list_library_status(self) -> list[dict[str, Any]]: ...

    def batch_update_status(self, keys: Sequence[ResubscribeItemKey], status: str) -> int: ...

    def stage_replacements(self, keys: Sequence[ResubscribeItemKey]) -> ResubscribeBatchReservation: ...

    def set_active_washing(self, key: ResubscribeItemKey, enabled: bool) -> int: ...


class PostgresResubscribeOperationStager:
    """Stages the existing P7 durable operation inside the caller's UoW."""

    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation:
        return PostgresSubscriptionOperationRepository.reserve_in_uow(uow, intent)


class SqlAlchemyResubscribeRepository:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media_projection: ResubscribeMediaProjectionPort,
        operation_stager: ResubscribeOperationStagePort,
        policy: ResubscribePolicyPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media_projection = media_projection
        self._operation_stager = operation_stager
        self._policy = policy

    def list_rules(self) -> list[dict[str, Any]]:
        try:
            with self._unit_of_work() as uow:
                rows = (
                    uow.session.execute(text("SELECT * FROM resubscribe_rules ORDER BY sort_order ASC, id ASC"))
                    .mappings()
                    .all()
                )
            return [dict(row) for row in rows]
        except Exception:
            logger.exception("Reading resubscribe rules failed; preserving the legacy empty list")
            return []

    def create_rule(self, data: Mapping[str, Any]) -> int:
        values = _rule_values(data)
        try:
            with self._unit_of_work() as uow:
                _lock_rule_order(uow)
                if "sort_order" not in values:
                    values["sort_order"] = int(
                        uow.session.scalar(text("SELECT COALESCE(MAX(sort_order), -1) + 1 FROM resubscribe_rules")) or 0
                    )
                columns = tuple(values)
                assignments = ", ".join(columns)
                placeholders = ", ".join(_rule_placeholder(column) for column in columns)
                raw_id = uow.session.scalar(
                    text(f"INSERT INTO resubscribe_rules ({assignments}) VALUES ({placeholders}) RETURNING id"),
                    values,
                )
                if raw_id is None:
                    raise RuntimeError("database did not return the new resubscribe rule id")
                rule_id = int(raw_id)
                uow.commit()
        except IntegrityError as error:
            if getattr(error.orig, "sqlstate", None) == "23505":
                raise DuplicateResubscribeRuleError from error
            raise
        return rule_id

    def update_rule(self, rule_id: int, data: Mapping[str, Any]) -> bool:
        values = _rule_values(data)
        with self._unit_of_work() as uow:
            _lock_rule_order(uow)
            assignments = ", ".join(f"{column} = {_rule_placeholder(column)}" for column in values)
            result = uow.session.execute(
                text(f"UPDATE resubscribe_rules SET {assignments} WHERE id = :rule_id"),
                {**values, "rule_id": rule_id},
            )
            updated = bool(getattr(result, "rowcount", 0))
            uow.commit()
        return updated

    def delete_rule(self, rule_id: int) -> bool:
        with self._unit_of_work() as uow:
            _lock_rule_order(uow)
            uow.session.execute(
                text("DELETE FROM resubscribe_index WHERE matched_rule_id = :rule_id"),
                {"rule_id": rule_id},
            )
            result = uow.session.execute(
                text("DELETE FROM resubscribe_rules WHERE id = :rule_id"),
                {"rule_id": rule_id},
            )
            deleted = bool(getattr(result, "rowcount", 0))
            uow.commit()
        return deleted

    def order_rules(self, ordered_ids: Sequence[Any]) -> None:
        with self._unit_of_work() as uow:
            _lock_rule_order(uow)
            existing = tuple(
                int(value)
                for value in uow.session.scalars(
                    text("SELECT id FROM resubscribe_rules ORDER BY sort_order ASC, id ASC")
                )
            )
            requested: list[int] = []
            for value in ordered_ids:
                try:
                    rule_id = int(value)
                except (TypeError, ValueError):
                    continue
                if isinstance(value, bool) or rule_id not in existing or rule_id in requested:
                    continue
                requested.append(rule_id)
            final_order = (*requested, *(rule_id for rule_id in existing if rule_id not in requested))
            for sort_order, rule_id in enumerate(final_order):
                uow.session.execute(
                    text("UPDATE resubscribe_rules SET sort_order = :sort_order WHERE id = :rule_id"),
                    {"sort_order": sort_order, "rule_id": rule_id},
                )
            uow.commit()

    def list_library_status(self) -> list[dict[str, Any]]:
        try:
            with self._unit_of_work() as uow:
                rows = (
                    uow.session.execute(
                        text(
                            """
                            SELECT
                                idx.tmdb_id,
                                idx.item_type,
                                idx.season_number,
                                idx.status,
                                idx.reason,
                                idx.matched_rule_id,
                                COALESCE(rule.rule_type, 'resubscribe') AS action,
                                COALESCE(rule.resubscribe_source, 'moviepilot') AS resubscribe_source
                            FROM resubscribe_index AS idx
                            LEFT JOIN resubscribe_rules AS rule ON rule.id = idx.matched_rule_id
                            ORDER BY idx.tmdb_id, idx.item_type, idx.season_number
                            """
                        )
                    )
                    .mappings()
                    .all()
                )
                keys = tuple(
                    ResubscribeItemKey(str(row["tmdb_id"]), str(row["item_type"]), int(row["season_number"]))
                    for row in rows
                )
                descriptions = self._media_projection.describe_items(uow, keys)
            items = []
            for row, key in zip(rows, keys, strict=True):
                media = dict(descriptions.get(key, {}))
                items.append(
                    {
                        "item_id": key.item_id,
                        "tmdb_id": key.tmdb_id,
                        "item_type": key.item_type,
                        "conceptual_type": "Series" if key.item_type == "Season" else "Movie",
                        "season_number": key.season_number if key.item_type == "Season" else None,
                        "status": row["status"],
                        "reason": row["reason"],
                        "matched_rule_id": row["matched_rule_id"],
                        "action": row["action"],
                        "resubscribe_source": row["resubscribe_source"],
                        **media,
                    }
                )
            items.sort(key=lambda item: (str(item.get("item_name") or "").casefold(), item["item_id"]))
            return items
        except Exception:
            logger.exception("Reading the resubscribe library failed; preserving the legacy empty list")
            return []

    def batch_update_status(self, keys: Sequence[ResubscribeItemKey], status: str) -> int:
        unique_keys = tuple(dict.fromkeys(keys))
        with self._unit_of_work() as uow:
            updated = 0
            for key in unique_keys:
                result = uow.session.execute(
                    text(
                        """
                        UPDATE resubscribe_index
                        SET status = :status
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = :item_type
                          AND season_number = :season_number
                        """
                    ),
                    {
                        "status": status,
                        "tmdb_id": key.tmdb_id,
                        "item_type": key.item_type,
                        "season_number": key.season_number,
                    },
                )
                updated += int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
        return updated

    def stage_replacements(self, keys: Sequence[ResubscribeItemKey]) -> ResubscribeBatchReservation:
        unique_keys = tuple(dict.fromkeys(keys))
        policy = self._policy.snapshot()
        with self._unit_of_work() as uow:
            rows: dict[ResubscribeItemKey, dict[str, Any]] = {}
            for key in unique_keys:
                row = (
                    uow.session.execute(
                        text(
                            """
                            SELECT
                                idx.last_checked_at AS index_last_checked_at,
                                COALESCE(rule.rule_type, 'resubscribe') AS action,
                                COALESCE(rule.resubscribe_source, 'moviepilot') AS resubscribe_source,
                                rule.*
                            FROM resubscribe_index AS idx
                            LEFT JOIN resubscribe_rules AS rule ON rule.id = idx.matched_rule_id
                            WHERE idx.tmdb_id = :tmdb_id
                              AND idx.item_type = :item_type
                              AND idx.season_number = :season_number
                            FOR UPDATE OF idx
                            """
                        ),
                        {
                            "tmdb_id": key.tmdb_id,
                            "item_type": key.item_type,
                            "season_number": key.season_number,
                        },
                    )
                    .mappings()
                    .first()
                )
                if row is not None:
                    values = dict(row)
                    values["last_checked_at"] = values.pop("index_last_checked_at")
                    rows[key] = values
            unsupported = tuple(
                key.item_id
                for key, rule in rows.items()
                if rule["action"] != "resubscribe" or rule["resubscribe_source"] != "moviepilot"
            )
            if unsupported:
                raise UnsupportedResubscribeBatchError(unsupported)
            descriptions = self._media_projection.describe_items(uow, tuple(rows))
            operation_ids: list[UUID] = []
            for key, rule in rows.items():
                intent = _replacement_delete_intent(
                    key,
                    rule,
                    descriptions.get(key, {}),
                    policy,
                )
                reservation = self._operation_stager.stage(uow, intent)
                operation_ids.append(reservation.operation.id)
            uow.commit()
        return ResubscribeBatchReservation(tuple(operation_ids))

    def set_active_washing(self, key: ResubscribeItemKey, enabled: bool) -> int:
        with self._unit_of_work() as uow:
            updated = self._media_projection.set_active_washing(uow, key, enabled)
            uow.commit()
        return updated


class ResubscribeWorkflowService:
    """Resumes the persisted create continuation only after delete confirmation."""

    def __init__(self, operations: ResubscribeOperationStorePort) -> None:
        self._operations = operations

    def resume(self, delete_operation_id: UUID) -> SubscriptionOperationReservation | None:
        operation = self._operations.read(delete_operation_id)
        if operation is None:
            return None
        descriptor = _continuation_descriptor(operation)
        if operation.status is not SubscriptionOperationStatus.CONFIRMED:
            return None
        intent = SubscriptionOperationIntent.create(
            kind=descriptor["kind"],
            scope_key=str(descriptor["scope_key"]),
            request=_mapping(descriptor["request"]),
        )
        return self._operations.reserve(intent)


class ResubscribeService:
    def __init__(
        self,
        repository: ResubscribeRepository,
        tasks: ResubscribeTaskPort,
        libraries: ResubscribeLibraryPort,
    ) -> None:
        self._repository = repository
        self._tasks = tasks
        self._libraries = libraries

    def list_rules(self) -> ResubscribeResult:
        try:
            return ResubscribeResult(self._repository.list_rules())
        except Exception:
            logger.exception("Listing resubscribe rules failed")
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def create_rule(self, data: Mapping[str, Any]) -> ResubscribeResult:
        name = data.get("name")
        if not name:
            return ResubscribeResult({"error": "规则名称不能为空"}, 400)
        try:
            rule_id = self._repository.create_rule(data)
            return ResubscribeResult({"message": "洗版规则已成功创建！", "id": rule_id}, 201)
        except DuplicateResubscribeRuleError:
            return ResubscribeResult({"error": f"创建失败：规则名称 '{name}' 已存在。"}, 409)
        except Exception:
            logger.exception("Creating a resubscribe rule failed")
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def update_rule(self, rule_id: int, data: Mapping[str, Any]) -> ResubscribeResult:
        if not data:
            return ResubscribeResult({"error": "请求体不能为空"}, 400)
        try:
            if self._repository.update_rule(rule_id, data):
                return ResubscribeResult({"message": "洗版规则已成功更新！"})
            return ResubscribeResult({"error": f"未找到ID为 {rule_id} 的规则"}, 404)
        except Exception:
            logger.exception("Updating resubscribe rule %s failed", rule_id)
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def delete_rule(self, rule_id: int) -> ResubscribeResult:
        try:
            if self._repository.delete_rule(rule_id):
                return ResubscribeResult({"message": "洗版规则已成功删除！"})
            return ResubscribeResult({"error": f"未找到ID为 {rule_id} 的规则"}, 404)
        except Exception:
            logger.exception("Deleting resubscribe rule %s failed", rule_id)
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def order_rules(self, ordered_ids: Sequence[Any]) -> ResubscribeResult:
        try:
            self._repository.order_rules(ordered_ids)
            return ResubscribeResult({"message": "规则顺序已更新！"})
        except Exception:
            logger.exception("Ordering resubscribe rules failed")
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def list_library_status(self) -> ResubscribeResult:
        try:
            return ResubscribeResult(self._repository.list_library_status())
        except Exception:
            logger.exception("Listing resubscribe library status failed")
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def refresh_status(self) -> ResubscribeResult:
        return self._submit_task(
            ResubscribeTaskIntent("update-resubscribe-cache", "刷新媒体整理"),
            "刷新媒体整理任务已提交！",
        )

    def resubscribe_all(self) -> ResubscribeResult:
        return self._submit_task(
            ResubscribeTaskIntent("resubscribe-library", "全库媒体洗版"),
            "一键洗版任务已提交！",
        )

    def libraries(self) -> ResubscribeResult:
        if not self._libraries.ready():
            return ResubscribeResult({"error": "Emby配置不完整或服务未就绪"}, 503)
        try:
            values = self._libraries.list_libraries()
            if values is None:
                return ResubscribeResult({"error": "无法获取Emby媒体库列表"}, 500)
            simplified = [
                {"label": item.get("Name"), "value": item.get("Id")}
                for item in values
                if item.get("Name") and item.get("Id") and item.get("CollectionType") in {"movies", "tvshows"}
            ]
            return ResubscribeResult(simplified)
        except Exception:
            logger.exception("Listing Emby libraries for resubscribe rules failed")
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def batch_action(self, item_ids: object, action: object) -> ResubscribeResult:
        normalized_ids = item_ids.split(",") if isinstance(item_ids, str) else item_ids
        if not isinstance(normalized_ids, list) or not normalized_ids:
            return ResubscribeResult({"message": "当前视图下没有可操作的项目。"})
        normalized_action = "ok" if action in {"reset", "unignore"} else action
        if normalized_action not in {"subscribe", "ignore", "ok", "delete"}:
            return ResubscribeResult({"error": f"无效的操作类型: {normalized_action}"}, 400)
        keys = tuple(key for value in normalized_ids if (key := ResubscribeItemKey.parse(value)) is not None)
        if not keys:
            return ResubscribeResult({"message": "没有有效的项目ID被处理。"})
        try:
            if normalized_action == "subscribe":
                self._repository.stage_replacements(keys)
                return ResubscribeResult({"message": "批量整理任务已提交到后台！"}, 202)
            if normalized_action == "ignore":
                count = self._repository.batch_update_status(keys, "ignored")
                return ResubscribeResult({"message": f"成功忽略了 {count} 个媒体项。"})
            if normalized_action == "ok":
                count = self._repository.batch_update_status(keys, "ok")
                return ResubscribeResult({"message": f"成功取消忽略了 {count} 个媒体项。"})
            return ResubscribeResult({"message": "批量删除操作暂未实现。"})
        except SubscriptionQuotaExhaustedError:
            return ResubscribeResult({"error": "每日订阅配额已用尽。"}, 409)
        except UnsupportedResubscribeBatchError as exc:
            return ResubscribeResult(
                {
                    "error": "所选项目包含尚未迁移的删除或 HDHive 处理，已整批拒绝。",
                    "unsupported_item_ids": list(exc.item_ids),
                },
                409,
            )
        except Exception:
            logger.exception("Applying resubscribe batch action %r failed", normalized_action)
            return ResubscribeResult({"error": "服务器内部错误"}, 500)

    def set_active_washing(self, item_id: str, enabled: bool) -> int:
        key = ResubscribeItemKey.parse(item_id)
        if key is None:
            raise ValueError("invalid resubscribe item id")
        return self._repository.set_active_washing(key, enabled)

    def _submit_task(self, intent: ResubscribeTaskIntent, message: str) -> ResubscribeResult:
        if self._tasks.busy():
            return ResubscribeResult({"error": "后台有任务正在运行，请稍后再试。"}, 409)
        try:
            self._tasks.submit(intent)
            return ResubscribeResult({"message": message}, 202)
        except Exception as error:
            return ResubscribeResult({"error": f"提交任务失败: {error}"}, 500)


class WashingPriorityPolicy:
    @classmethod
    def decide(
        cls,
        candidate: WashingCandidate,
        existing: Sequence[WashingCandidate],
        priorities: Sequence[Mapping[str, Any]],
        *,
        active_washing: bool = False,
    ) -> WashingDecision:
        if not candidate.valid:
            return WashingDecision(WashingAction.REJECT, "无法获取媒体流信息")
        if candidate.slot_id is None:
            return WashingDecision(WashingAction.REJECT, "未命中任何多版本槽位")
        if active_washing:
            return WashingDecision(WashingAction.REPLACE, "完结洗版特权：强制替换当前槽位旧版")
        if not priorities:
            return WashingDecision(WashingAction.ACCEPT, "未配置优先级规则，默认放行")

        new_level, new_reason = cls.level(candidate.attributes, priorities)
        if new_level <= 0:
            return WashingDecision(WashingAction.REJECT, new_reason, new_level=new_level)
        comparable = [item for item in existing if item.valid and item.slot_id == candidate.slot_id]
        if not comparable:
            return WashingDecision(WashingAction.ACCEPT, "当前槽位无旧版", new_level=new_level)
        old_levels = [cls.level(item.attributes, priorities)[0] for item in comparable]
        eligible_old_levels = [level for level in old_levels if level > 0]
        best_old = min(eligible_old_levels, default=999)
        if new_level < best_old:
            return WashingDecision(
                WashingAction.REPLACE,
                "新版优于当前槽位旧版",
                new_level,
                best_old,
            )
        return WashingDecision(
            WashingAction.SKIP,
            "新版未优于当前槽位旧版",
            new_level,
            best_old,
        )

    @classmethod
    def level(
        cls,
        attributes: Mapping[str, Any],
        priorities: Sequence[Mapping[str, Any]],
    ) -> tuple[int, str]:
        normal_index = 0
        failures: list[str] = []
        for rule in _stable_priorities(priorities):
            excluded = bool(rule.get("is_exclude"))
            if not excluded:
                normal_index += 1
            matched = _priority_matches(attributes, rule, excluded=excluded)
            if matched and excluded:
                return -1, "命中排除条件"
            if matched:
                return normal_index, f"优先级 {normal_index}"
            if not excluded:
                failures.append(f"优先级 {normal_index} 未命中")
        return 0, "\n".join(failures) if failures else "未配置任何有效的普通优先级规则"


def _rule_values(data: Mapping[str, Any]) -> dict[str, Any]:
    values = {key: value for key, value in data.items() if key != "id"}
    unknown = set(values) - _RULE_FIELDS
    if unknown:
        raise ValueError(f"unknown resubscribe rule fields: {sorted(unknown)}")
    for field in _JSON_RULE_FIELDS & values.keys():
        values[field] = None if values[field] is None else json.dumps(values[field], ensure_ascii=False)
    return values


def _rule_placeholder(column: str) -> str:
    return f"CAST(:{column} AS jsonb)" if column in _JSON_RULE_FIELDS else f":{column}"


def _lock_rule_order(uow: SqlAlchemyUnitOfWork) -> None:
    uow.session.execute(text("LOCK TABLE resubscribe_rules IN SHARE ROW EXCLUSIVE MODE"))


def _replacement_delete_intent(
    key: ResubscribeItemKey,
    rule: Mapping[str, Any],
    media: Mapping[str, Any],
    policy: ResubscribePolicySnapshot,
) -> SubscriptionOperationIntent:
    generation_value = rule.get("last_checked_at")
    generation = (
        generation_value.isoformat() if isinstance(generation_value, datetime) else str(generation_value or "initial")
    )
    create_request = {
        "action": "create",
        "tmdb_id": key.tmdb_id,
        "payload": _provider_payload(key, rule, media, policy),
        "expected": {},
        "local_continuation": {
            "type": "mark_resubscribe_subscribed",
            "tmdb_id": key.tmdb_id,
            "item_type": key.item_type,
            "season": key.season_number,
        },
    }
    if key.item_type == "Season":
        create_request["season"] = key.season_number
    descriptor = {
        "kind": SubscriptionOperationKind.CREATE.value,
        "scope_key": f"resubscribe:{key.scope_key}:{generation}:create",
        "request": create_request,
    }
    request: dict[str, Any] = {
        "action": "delete",
        "tmdb_id": key.tmdb_id,
        "expected": {},
        "provider": "moviepilot",
        "workflow": {
            "type": "delete_then_create",
            "version": 1,
            "generation": generation,
            "next_operation": descriptor,
            "delay_seconds": policy.delay_seconds,
        },
    }
    if key.item_type == "Season":
        request["season"] = key.season_number
    return SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.RESUBSCRIBE,
        scope_key=f"resubscribe:{key.scope_key}:{generation}:delete",
        request=request,
        available_at=datetime.now(UTC),
        quota=SubscriptionQuotaRequirement(
            quota_date=policy.quota_date,
            daily_cap=policy.daily_cap,
            ttl_seconds=86_400,
        ),
    )


def _provider_payload(
    key: ResubscribeItemKey,
    rule: Mapping[str, Any],
    media: Mapping[str, Any],
    policy: ResubscribePolicySnapshot,
) -> dict[str, Any]:
    tmdb_id: int | str = int(key.tmdb_id) if key.tmdb_id.isdigit() else key.tmdb_id
    name = str(media.get("item_name") or "未知项目").split(" - 第", maxsplit=1)[0]
    payload: dict[str, Any] = {
        "name": name,
        "tmdbid": tmdb_id,
        "type": "电视剧" if key.item_type == "Season" else "电影",
        "best_version": 1,
        "best_version_full": 1,
    }
    if key.item_type == "Season":
        payload["season"] = key.season_number
    else:
        release_groups = _string_values(media.get("release_group_raw"))
        excluded = [
            keyword
            for group in release_groups
            for keyword in policy.release_group_mapping.get(group, ())
            if str(keyword).strip()
        ]
        if excluded:
            payload["exclude"] = f"(?:{'|'.join(str(keyword) for keyword in excluded)})"
    if not rule.get("custom_resubscribe_enabled"):
        _apply_season_resubscribe_mode(payload, key, rule)
        return payload
    payload.pop("best_version", None)
    if rule.get("resubscribe_resolution_enabled"):
        threshold = rule.get("resubscribe_resolution_threshold")
        if threshold == 3840:
            payload["resolution"] = "4k"
        elif threshold == 1920:
            payload["resolution"] = "1080p"
    qualities = rule.get("resubscribe_quality_include")
    if rule.get("resubscribe_quality_enabled") and isinstance(qualities, list) and qualities:
        payload["quality"] = ",".join(str(value) for value in qualities)
    include: list[str] = []
    codecs = rule.get("resubscribe_codec_include")
    if rule.get("resubscribe_codec_enabled") and isinstance(codecs, list):
        codec_aliases = {
            "hevc": ("hevc", "h265", "x265"),
            "h264": ("h264", "avc", "x264"),
        }
        values = [alias for codec in codecs for alias in codec_aliases.get(str(codec).casefold(), ())]
        if values:
            include.append(f"(?=.*({'|'.join(values)}))")
    effects = rule.get("resubscribe_effect_include")
    if rule.get("resubscribe_effect_enabled") and isinstance(effects, list):
        hierarchy = ("dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr")
        aliases = {"杜比视界": "dovi_other", "HDR": "hdr"}
        choices = [aliases.get(str(value), str(value)) for value in effects]
        selected = next((value for value in hierarchy if value in choices), None)
        effect_map = {
            "dovi_p8": ("(?=.*(dovi|dolby|dv))(?=.*hdr)", "dovi"),
            "dovi_p7": ("(?=.*(dovi|dolby|dv))(?=.*(p7|profile.?7))", "dovi"),
            "dovi_p5": ("(?=.*(dovi|dolby|dv))", "dovi"),
            "dovi_other": ("(?=.*(dovi|dolby|dv))", "dovi"),
            "hdr10+": (r"(?=.*(hdr10\+|hdr10plus))", "hdr10+"),
            "hdr": ("(?=.*hdr)", "hdr"),
        }
        regex, simple = effect_map.get(selected or "", (None, None))
        if regex:
            include.append(regex)
        if simple:
            payload["effect"] = simple
    audio_languages = rule.get("resubscribe_audio_missing_languages")
    if rule.get("resubscribe_audio_enabled") and isinstance(audio_languages, list):
        keywords = _language_keywords(policy.language_keywords, audio_languages, subtitle=False)
        if keywords:
            include.append(f"(?=.*({'|'.join(keywords)}))")
    if rule.get("resubscribe_subtitle_effect_only"):
        include.append("(?=.*特效)")
    else:
        subtitle_languages = rule.get("resubscribe_subtitle_missing_languages")
        if rule.get("resubscribe_subtitle_enabled") and isinstance(subtitle_languages, list):
            keywords = _language_keywords(policy.language_keywords, subtitle_languages, subtitle=True)
            if keywords:
                include.append(f"(?=.*({'|'.join(keywords)}))")
    if include:
        payload["include"] = "".join(include)
    _apply_season_resubscribe_mode(payload, key, rule)
    return payload


def _apply_season_resubscribe_mode(
    payload: dict[str, Any],
    key: ResubscribeItemKey,
    rule: Mapping[str, Any],
) -> None:
    if key.item_type != "Season":
        return
    if rule.get("resubscribe_entire_season"):
        payload["best_version"] = 1
    else:
        payload.pop("best_version", None)


def _language_keywords(
    keyword_map: Mapping[str, Sequence[str]],
    languages: Sequence[Any],
    *,
    subtitle: bool,
) -> list[str]:
    values: set[str] = set()
    for language in languages:
        code = str(language)
        keys = ("sub_chi", "sub_yue") if subtitle and code == "zh" else ((f"sub_{code}",) if subtitle else (code,))
        for key in keys:
            values.update(str(value) for value in keyword_map.get(key, ()) if str(value))
    return sorted(values, key=lambda value: (-len(value), value))


def _string_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value and value != "无" else ()
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value if str(item))
    return ()


def _continuation_descriptor(operation: SubscriptionOperation) -> Mapping[str, Any]:
    if operation.kind is not SubscriptionOperationKind.RESUBSCRIBE:
        raise ValueError("operation is not a resubscribe workflow")
    workflow = _mapping(operation.request.get("workflow"))
    if workflow.get("type") != "delete_then_create" or workflow.get("version") != 1:
        raise ValueError("unsupported resubscribe workflow descriptor")
    descriptor = _mapping(workflow.get("next_operation"))
    if descriptor.get("kind") != SubscriptionOperationKind.CREATE.value:
        raise ValueError("resubscribe workflow continuation must be create")
    return descriptor


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("resubscribe workflow value must be an object")
    return value


def _stable_priorities(priorities: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    indexed = tuple(enumerate(priorities))

    def key(value: tuple[int, Mapping[str, Any]]) -> tuple[int, int, str, int]:
        index, rule = value
        group_order = _integer(rule.get("_group_sort_order", rule.get("sort_order")), index)
        priority_order = _integer(rule.get("_priority_order"), index)
        group_id = str(rule.get("_group_id") or rule.get("id") or "")
        return group_order, priority_order, group_id, index

    return tuple(rule for _index, rule in sorted(indexed, key=key))


def _priority_matches(
    attributes: Mapping[str, Any],
    rule: Mapping[str, Any],
    *,
    excluded: bool,
) -> bool:
    checks: list[bool] = []
    for field in ("resolution", "effect", "release_group"):
        required = _strings(rule.get(field))
        if required:
            checks.append(str(attributes.get(field) or "").casefold() in required)
    source = _strings(rule.get("source") or rule.get("quality"))
    if source:
        checks.append(bool(_strings(attributes.get("source")) & source))
    codec = _strings(rule.get("codec"))
    if codec:
        current_codec = str(attributes.get("codec") or "").casefold()
        checks.append(any(value in current_codec or current_codec in value for value in codec))

    original_language = str(attributes.get("original_lang") or "").casefold()
    for field, attribute_field in (("audio", "audio_langs"), ("subtitle", "sub_langs")):
        required = set(_strings(rule.get(field)))
        if rule.get("exempt_original_lang"):
            required.discard(original_language)
        if required:
            available = set(_strings(attributes.get(attribute_field)))
            matched = required <= available
            if field == "subtitle" and attributes.get("has_external_subtitle") and not excluded:
                matched = True
            checks.append(matched)
    if rule.get("subtitle_effect"):
        checks.append(bool(attributes.get("has_effect_subtitle")))
    if rule.get("clean_version"):
        checks.append(bool(attributes.get("is_clean_version")))
    size = _number(attributes.get("size_gb"))
    minimum = _optional_number(rule.get("min_size_gb"))
    maximum = _optional_number(rule.get("max_size_gb"))
    if minimum is not None:
        checks.append(size <= minimum if excluded else size >= minimum)
    if maximum is not None:
        checks.append(size >= maximum if excluded else size <= maximum)
    return any(checks) if excluded else all(checks)


def _strings(value: Any) -> frozenset[str]:
    if value is None:
        return frozenset()
    values = value if isinstance(value, Sequence) and not isinstance(value, str | bytes) else (value,)
    return frozenset(str(item).strip().casefold() for item in values if str(item).strip())


def _integer(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _optional_number(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return _number(value)


__all__ = [
    "DuplicateResubscribeRuleError",
    "PostgresResubscribeOperationStager",
    "RESUBSCRIBE_USE_CASE_PROVIDER",
    "ResubscribeBatchReservation",
    "ResubscribeItemKey",
    "ResubscribeLibraryPort",
    "ResubscribeMediaProjectionPort",
    "ResubscribeOperationStagePort",
    "ResubscribeOperationStorePort",
    "ResubscribeRepository",
    "ResubscribeResult",
    "ResubscribeService",
    "ResubscribeTaskIntent",
    "ResubscribeTaskPort",
    "ResubscribeWorkflowService",
    "SqlAlchemyResubscribeRepository",
    "WashingAction",
    "WashingCandidate",
    "WashingDecision",
    "WashingPriorityPolicy",
]
