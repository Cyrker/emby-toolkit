from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome
from etk.policy_defaults import DEFAULT_STUDIO_MAPPING

from .effects import MediaEffectProjection, MediaEffectStatus

STUDIO_IMAGE_EFFECT_TYPE = "emby.studio-image.reconcile"


class StudioImageSettings(Protocol):
    def load(self, key: str) -> Any: ...


class StudioImageProvider(Protocol):
    def list_studios(self) -> Sequence[Mapping[str, Any]]: ...

    def resolve_logo(self, mapping: Mapping[str, Any]) -> tuple[str, str] | None: ...


class StudioImageEffects(Protocol):
    def reconcile(
        self,
        *,
        effect_key: str,
        item_id: str,
        logo_path: str | None,
        logo_source: str | None,
    ) -> MediaEffectProjection: ...


class StudioImageProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class StudioImageService:
    def __init__(
        self,
        settings: StudioImageSettings,
        provider: StudioImageProvider,
        effects: StudioImageEffects,
    ) -> None:
        self._settings = settings
        self._provider = provider
        self._effects = effects

    def execute(self, progress: StudioImageProgress, *, operation_key: str) -> TaskOutcome:
        mapping = _mapping(self._settings.load("studio_mapping"))
        lookup = _lookup(mapping)
        if not lookup:
            progress.update(100, "未找到有效工作室映射。")
            return TaskOutcome.skipped(
                "未找到有效工作室映射。",
                {"studios": 0, "candidates": 0, "confirmed": 0, "failed": 0, "indeterminate": 0},
            )
        progress.checkpoint()
        progress.update(5, "正在获取 Emby 工作室列表。")
        studios = tuple(self._provider.list_studios())
        candidates: list[tuple[Mapping[str, Any], Mapping[str, Any], bool, bool]] = []
        unmapped = 0
        ready = 0
        for studio in studios:
            name = str(studio.get("Name") or "").strip()
            entry = lookup.get(_normalized_name(name))
            if not name or entry is None:
                unmapped += 1
                continue
            tags = studio.get("ImageTags")
            image_tags = tags if isinstance(tags, Mapping) else {}
            has_thumb = bool(image_tags.get("Thumb") or studio.get("ThumbImageTag"))
            has_primary = bool(image_tags.get("Primary") or studio.get("PrimaryImageTag"))
            if has_thumb and not has_primary:
                ready += 1
                continue
            candidates.append((studio, entry, has_thumb, has_primary))

        confirmed = 0
        failed = 0
        indeterminate = 0
        no_logo = 0
        effect_ids: list[str] = []
        total = len(candidates)
        for index, (studio, entry, has_thumb, _has_primary) in enumerate(candidates, 1):
            progress.checkpoint()
            item_id = str(studio.get("Id") or "").strip()
            if not item_id:
                failed += 1
                continue
            logo: tuple[str, str] | None = None
            if not has_thumb:
                try:
                    logo = self._provider.resolve_logo(entry)
                except Exception:
                    failed += 1
                    continue
                if logo is None:
                    no_logo += 1
                    continue
            logo_path, logo_source = logo or (None, None)
            progress.checkpoint()
            effect_key = _effect_key(operation_key, item_id, logo_path)
            progress.record_started_effect(effect_key)
            try:
                effect = self._effects.reconcile(
                    effect_key=effect_key,
                    item_id=item_id,
                    logo_path=logo_path,
                    logo_source=logo_source,
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
            progress.update(
                min(99, 10 + int(index / max(total, 1) * 85)),
                f"正在补图 ({index}/{total}): {studio.get('Name') or item_id}",
            )

        details = {
            "studios": len(studios),
            "candidates": total,
            "ready": ready,
            "unmapped": unmapped,
            "no_logo": no_logo,
            "confirmed": confirmed,
            "failed": failed,
            "indeterminate": indeterminate,
            "effect_ids": effect_ids,
        }
        progress.update(100, f"工作室图标补全完成：确认 {confirmed}/{total}。")
        if failed or indeterminate:
            return TaskOutcome.partial("工作室图标补全部分完成。", details)
        return TaskOutcome.succeeded("工作室图标补全完成。", details)


def _mapping(value: Any) -> tuple[dict[str, Any], ...]:
    raw = value or DEFAULT_STUDIO_MAPPING
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = DEFAULT_STUDIO_MAPPING
    if isinstance(raw, Mapping):
        raw = raw.get("items") or raw.get("mapping") or raw.get("studios") or raw.get("data") or list(raw.values())
    if not isinstance(raw, list):
        return ()
    return tuple(dict(item) for item in raw if isinstance(item, Mapping) and str(item.get("label") or "").strip())


def _lookup(mapping: Sequence[Mapping[str, Any]]) -> Mapping[str, Mapping[str, Any]]:
    values: dict[str, Mapping[str, Any]] = {}
    for entry in mapping:
        label = _normalized_name(entry.get("label"))
        if label:
            values[label] = entry
        aliases = entry.get("en")
        if isinstance(aliases, list):
            for alias in aliases:
                normalized = _normalized_name(alias)
                if normalized:
                    values.setdefault(normalized, entry)
    return values


def _normalized_name(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().casefold())


def _effect_key(operation_key: str, item_id: str, logo_path: str | None) -> str:
    digest = hashlib.sha256(str(logo_path or "thumb-only").encode()).hexdigest()[:20]
    return f"{operation_key}:studio:{item_id}:{digest}"


__all__ = [
    "STUDIO_IMAGE_EFFECT_TYPE",
    "StudioImageEffects",
    "StudioImageProgress",
    "StudioImageProvider",
    "StudioImageService",
    "StudioImageSettings",
]
