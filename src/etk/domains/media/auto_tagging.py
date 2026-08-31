from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome
from etk.policy_defaults import DEFAULT_RATING_MAPPING

from .effects import MediaEffectProjection, MediaEffectStatus

AUTO_TAGGING_EFFECT_TYPE = "emby.tags.apply"


class AutoTaggingSettings(Protocol):
    def load(self, key: str) -> Any: ...


class AutoTaggingInventory(Protocol):
    def list_libraries(self) -> Sequence[Mapping[str, Any]]: ...

    def list_items(self, library_id: str) -> Sequence[Mapping[str, Any]]: ...


class AutoTaggingEffects(Protocol):
    def apply(
        self,
        *,
        effect_key: str,
        item_id: str,
        operation: str,
        tags: Sequence[str],
    ) -> MediaEffectProjection: ...


class AutoTaggingProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class AutoTaggingService:
    def __init__(
        self,
        settings: AutoTaggingSettings,
        inventory: AutoTaggingInventory,
        effects: AutoTaggingEffects,
    ) -> None:
        self._settings = settings
        self._inventory = inventory
        self._effects = effects

    def execute(
        self,
        progress: AutoTaggingProgress,
        *,
        operation_key: str,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        rules = _rules(parameters, self._settings.load("auto_tagging_rules"))
        if not rules:
            progress.update(100, "未配置自动打标规则，已跳过。")
            return TaskOutcome.skipped(
                "未配置自动打标规则，已跳过。",
                {"rules": 0, "items": 0, "confirmed": 0, "failed": 0, "indeterminate": 0},
            )
        rating_codes = _rating_codes(self._settings.load("rating_mapping"))
        libraries = tuple(self._inventory.list_libraries())
        library_ids = {
            str(library.get("Id") or "").strip()
            for library in libraries
            if str(library.get("Id") or "").strip()
            and str(library.get("CollectionType") or "").casefold() not in {"boxsets", "playlists", "music"}
        }
        scanned = 0
        confirmed = 0
        filtered = 0
        failed = 0
        indeterminate = 0
        effect_ids: list[str] = []

        for rule_index, rule in enumerate(rules):
            progress.checkpoint()
            selected = set(rule["library_ids"]) or library_ids
            selected &= library_ids
            for library_index, library_id in enumerate(sorted(selected), 1):
                progress.checkpoint()
                items = tuple(self._inventory.list_items(library_id))
                total_items = max(len(items), 1)
                for item_index, item in enumerate(items, 1):
                    progress.checkpoint()
                    item_id = str(item.get("Id") or "").strip()
                    if not item_id:
                        continue
                    scanned += 1
                    rating = str(item.get("CustomRating") or item.get("OfficialRating") or "").strip()
                    if not _rating_matches(rating, rule["rating_filters"], rating_codes):
                        filtered += 1
                        continue
                    progress.checkpoint()
                    effect_key = _effect_key(
                        operation_key,
                        rule_index,
                        item_id,
                        rule["operation"],
                        rule["tags"],
                    )
                    progress.record_started_effect(effect_key)
                    try:
                        effect = self._effects.apply(
                            effect_key=effect_key,
                            item_id=item_id,
                            operation=rule["operation"],
                            tags=rule["tags"],
                        )
                    except Exception:
                        progress.record_abandoned_effect(effect_key)
                        raise
                    effect_ids.append(str(effect.id))
                    if effect.status is MediaEffectStatus.CONFIRMED:
                        progress.record_committed_effect(effect_key)
                        confirmed += 1
                    elif effect.status in {
                        MediaEffectStatus.PENDING,
                        MediaEffectStatus.CLAIMED,
                        MediaEffectStatus.INDETERMINATE,
                    }:
                        indeterminate += 1
                    else:
                        if effect.status in {MediaEffectStatus.RETRYABLE, MediaEffectStatus.REJECTED}:
                            progress.record_abandoned_effect(effect_key)
                        failed += 1
                    progress.checkpoint()
                    completed = (library_index - 1) * total_items + item_index
                    denominator = max(len(selected) * total_items, 1)
                    progress.update(
                        min(99, max(1, int(completed / denominator * 100))),
                        f"正在处理标签规则 {rule_index + 1}/{len(rules)}",
                    )

        details = {
            "rules": len(rules),
            "items": scanned,
            "confirmed": confirmed,
            "filtered": filtered,
            "failed": failed,
            "indeterminate": indeterminate,
            "effect_ids": effect_ids,
        }
        progress.update(100, "自动打标规则执行完毕。")
        if failed or indeterminate:
            return TaskOutcome.partial("自动打标规则部分完成。", details)
        return TaskOutcome.succeeded("自动打标规则执行完毕。", details)


def _rules(parameters: Mapping[str, Any], configured: Any) -> tuple[dict[str, Any], ...]:
    if parameters:
        return (_rule(parameters, default_operation="add"),)
    if not isinstance(configured, list):
        return ()
    rules: list[dict[str, Any]] = []
    for value in configured:
        if not isinstance(value, Mapping):
            continue
        normalized = _rule(value, default_operation="add")
        if normalized["tags"]:
            rules.append(normalized)
    return tuple(rules)


def _rule(value: Mapping[str, Any], *, default_operation: str) -> dict[str, Any]:
    operation = str(value.get("operation") or default_operation).strip().casefold()
    if operation not in {"add", "remove"}:
        raise ValueError("auto-tagging operation must be add or remove")
    return {
        "operation": operation,
        "library_ids": _strings(value.get("library_ids")),
        "tags": _strings(value.get("tags")),
        "rating_filters": _strings(value.get("rating_filters")),
    }


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))


def _rating_codes(value: Any) -> Mapping[str, frozenset[str]]:
    mapping = value if isinstance(value, Mapping) else DEFAULT_RATING_MAPPING
    codes: dict[str, set[str]] = {}
    for rules in mapping.values():
        if not isinstance(rules, Sequence) or isinstance(rules, (str, bytes, bytearray)):
            continue
        for rule in rules:
            if not isinstance(rule, Mapping):
                continue
            label = str(rule.get("label") or "").strip()
            code = str(rule.get("code") or "").strip()
            if label and code:
                codes.setdefault(label, set()).add(code)
    return {label: frozenset(values) for label, values in codes.items()}


def _rating_matches(
    rating: str,
    filters: Sequence[str],
    codes: Mapping[str, frozenset[str]],
) -> bool:
    if not filters:
        return True
    if not rating:
        return False
    expected = {code for label in filters for code in codes.get(label, ())}
    normalized = rating.split(":")[-1].strip()
    return rating in expected or normalized in expected


def _effect_key(
    operation_key: str,
    rule_index: int,
    item_id: str,
    operation: str,
    tags: Sequence[str],
) -> str:
    digest = hashlib.sha256("\0".join((operation, *tags)).encode()).hexdigest()[:20]
    return f"{operation_key}:rule:{rule_index}:item:{item_id}:{digest}"


__all__ = [
    "AUTO_TAGGING_EFFECT_TYPE",
    "AutoTaggingEffects",
    "AutoTaggingInventory",
    "AutoTaggingProgress",
    "AutoTaggingService",
    "AutoTaggingSettings",
]
