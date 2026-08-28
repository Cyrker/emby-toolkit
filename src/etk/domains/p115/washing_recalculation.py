from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from etk.platform import Settings

from .washing_evaluator import (
    WashingItemMutation,
    WashingMediaItem,
    WashingPolicySnapshot,
    build_washing_policy_snapshot,
    evaluate_washing_item,
    needs_remote_episode_runtime,
)
from .washing_priority import (
    P115WashingPriorityRepository,
    normalize_washing_priority_recalculate_scope,
)

logger = logging.getLogger(__name__)


class P115WashingRecalculationRepository(Protocol):
    def load_policy_source(self) -> tuple[dict[str, Any], list[dict[str, Any]]]: ...

    def list_candidates(
        self,
        *,
        item_types: Sequence[str],
        affected_scope: object,
        limit: int | None,
    ) -> list[tuple[str, str]]: ...

    def load_item(self, tmdb_id: str, item_type: str) -> WashingMediaItem | None: ...

    def persist_item(self, item: WashingMediaItem, mutation: WashingItemMutation) -> int: ...


class EpisodeRuntimeGateway(Protocol):
    def episode_runtime_minutes(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
    ) -> float | None: ...


class WashingTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


@dataclass(frozen=True, slots=True)
class P115WashingRecalculationResult:
    stats: Mapping[str, Any]

    @property
    def partial(self) -> bool:
        return int(self.stats.get("errors") or 0) > 0


class P115WashingPriorityRecalculationService:
    def __init__(
        self,
        *,
        repository: P115WashingRecalculationRepository,
        state: P115WashingPriorityRepository,
        settings: Callable[[], Settings],
        episode_runtime: EpisodeRuntimeGateway,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._state = state
        self._settings = settings
        self._episode_runtime = episode_runtime
        self._clock = clock or (lambda: datetime.now(UTC))

    def execute(
        self,
        progress: WashingTaskProgress,
        *,
        item_type: object = "all",
        limit: object = None,
        affected_scope: object = None,
        scope_revision: object = None,
    ) -> P115WashingRecalculationResult:
        item_types = normalize_washing_item_types(item_type)
        normalized_scope = normalize_washing_priority_recalculate_scope(affected_scope)
        processed_scope = _processed_scope(normalized_scope, item_types) if affected_scope is not None else {}
        effective_limit = normalize_washing_limit(limit)
        started_at = _timestamp(self._clock())
        stats: dict[str, Any] = {
            "scanned_items": 0,
            "updated_items": 0,
            "evaluated_versions": 0,
            "missing_raw": 0,
            "missing_identity": 0,
            "no_priority_rules": 0,
            "backfilled_target_cid": 0,
            "errors": 0,
            "affected_scope": normalized_scope,
            "scope_acknowledged": False,
            "started_at": started_at,
            "finished_at": None,
        }

        policy_settings, groups = self._repository.load_policy_source()
        runtime_settings = self._settings()
        policy = build_washing_policy_snapshot(
            settings=policy_settings,
            groups=groups,
            local_root=runtime_settings.local_strm_root,
            media_root_name=runtime_settings.p115_media_root_name,
        )
        candidates = self._repository.list_candidates(
            item_types=item_types,
            affected_scope=affected_scope,
            limit=effective_limit,
        )
        total = len(candidates)
        progress.update(1, f"  ➜ [洗版优先级重算] 开始执行，待处理 {total} 个受影响媒体项")

        for tmdb_id, current_item_type in candidates:
            progress.checkpoint()
            stats["scanned_items"] += 1
            try:
                item = self._repository.load_item(tmdb_id, current_item_type)
                if item is None:
                    raise RuntimeError("media item disappeared before washing recalculation")
                if affected_scope is not None and not washing_item_matches_scope(item, processed_scope):
                    raise RuntimeError("media item target scope changed during washing recalculation")
                remote_runtime = self._remote_runtime(item, policy)
                mutation = evaluate_washing_item(
                    item,
                    policy,
                    remote_episode_runtime_minutes=remote_runtime,
                    evaluated_at=self._clock(),
                )
                backfilled = self._repository.persist_item(item, mutation)
                stats["updated_items"] += 1
                for key in (
                    "evaluated_versions",
                    "missing_raw",
                    "missing_identity",
                    "no_priority_rules",
                ):
                    stats[key] += int(mutation.stats.get(key) or 0)
                stats["backfilled_target_cid"] += backfilled
            except Exception as exc:
                stats["errors"] += 1
                logger.warning(
                    "Washing priority recalculation failed for tmdb=%s type=%s: %s",
                    tmdb_id,
                    current_item_type,
                    exc,
                    exc_info=True,
                )
            if stats["scanned_items"] % 10 == 0:
                current_progress = 5 + int(stats["scanned_items"] / max(total, 1) * 90)
                progress.update(
                    min(current_progress, 95),
                    _progress_message(stats, total),
                )

        if scope_revision is not None and not bool(limit) and stats["errors"] == 0 and processed_scope:
            try:
                stats["scope_acknowledged"] = self._state.acknowledge_recalculate_scope(
                    int(cast(Any, scope_revision)),
                    processed_scope,
                )
            except Exception as exc:
                stats["errors"] += 1
                logger.warning("Unable to acknowledge washing priority scope: %s", exc, exc_info=True)
        stats["finished_at"] = _timestamp(self._clock())
        progress.update(
            100,
            (
                f"  ➜ [洗版优先级重算] 完成：总数 {stats['scanned_items']}，"
                f"已更新 {stats['updated_items']}，缺 RAW {stats['missing_raw']}，"
                f"未命中规则 {stats['no_priority_rules']}，回填CID {stats['backfilled_target_cid']}"
            ),
        )
        return P115WashingRecalculationResult(stats)

    def _remote_runtime(
        self,
        item: WashingMediaItem,
        policy: WashingPolicySnapshot,
    ) -> float | None:
        if not needs_remote_episode_runtime(item, policy):
            return None
        assert item.season_number is not None
        assert item.episode_number is not None
        try:
            return self._episode_runtime.episode_runtime_minutes(
                item.parent_series_tmdb_id,
                item.season_number,
                item.episode_number,
            )
        except Exception as exc:
            logger.debug(
                "TMDb runtime lookup failed for %s S%sE%s: %s",
                item.parent_series_tmdb_id,
                item.season_number,
                item.episode_number,
                exc,
            )
            return None


def normalize_washing_item_types(value: object) -> tuple[str, ...]:
    normalized = str(value or "all").strip().lower()
    if normalized in {"movie", "movies"}:
        return ("Movie",)
    if normalized in {"episode", "episodes", "series", "tv"}:
        return ("Episode",)
    return "Movie", "Episode"


def normalize_washing_limit(value: object) -> int | None:
    if not value:
        return None
    try:
        normalized = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def washing_item_matches_scope(
    item: WashingMediaItem,
    scope: Mapping[str, Any],
) -> bool:
    media_type = "Movie" if item.item_type == "Movie" else "Series"
    entry = scope.get(media_type)
    if not isinstance(entry, Mapping):
        return False
    if entry.get("all"):
        return True
    targets = {str(value) for value in entry.get("target_cids", [])}
    if not targets:
        return False
    observed = {str(item.washing_snapshot.get("target_cid") or "")}
    versions = item.washing_snapshot.get("versions")
    if isinstance(versions, list):
        observed.update(str(version.get("target_cid") or "") for version in versions if isinstance(version, Mapping))
    observed.update(version.organize_target_cid for version in item.versions)
    return bool(targets & observed)


def _processed_scope(
    scope: Mapping[str, Any],
    item_types: Sequence[str],
) -> dict[str, Any]:
    allowed_media_types = {"Movie" if item_type == "Movie" else "Series" for item_type in item_types}
    return {
        media_type: dict(entry)
        for media_type, entry in scope.items()
        if media_type in allowed_media_types and isinstance(entry, Mapping)
    }


def _progress_message(stats: Mapping[str, Any], total: int) -> str:
    return (
        f"  ➜ [洗版优先级重算] 进度: {stats['scanned_items']}/{total}，"
        f"已更新 {stats['updated_items']}，缺 RAW {stats['missing_raw']}，"
        f"未命中规则 {stats['no_priority_rules']}，回填CID {stats['backfilled_target_cid']}"
    )


def _timestamp(value: datetime) -> str:
    current = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return current.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "EpisodeRuntimeGateway",
    "P115WashingPriorityRecalculationService",
    "P115WashingRecalculationRepository",
    "P115WashingRecalculationResult",
    "WashingTaskProgress",
    "normalize_washing_item_types",
    "normalize_washing_limit",
    "washing_item_matches_scope",
]
