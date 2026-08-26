from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from etk.compatibility.background import spawn
from etk.domains.p115.directory_sync import P115TaskProgress
from etk.platform.tasks import TaskOutcome

from .intro_detection import (
    IntroDetectionIntent,
    IntroDetectionPolicySnapshot,
    IntroDetectionReservation,
    IntroEpisodeSnapshot,
    IntroJobTrigger,
    IntroTriggerMode,
)

logger = logging.getLogger(__name__)


class IntroDetectionSeasonRepository(Protocol):
    def list_season_snapshots(
        self,
        series_tmdb_id: str,
        season_number: int,
    ) -> tuple[IntroEpisodeSnapshot, ...]: ...


class IntroDetectionSubmitter(Protocol):
    def submit(self, intent: IntroDetectionIntent) -> IntroDetectionReservation: ...


class IntroDetectionRuntimeSource(Protocol):
    def snapshot(self) -> IntroDetectionPolicySnapshot: ...

    def season_is_favorite(self, series_tmdb_id: str, season_number: int) -> bool: ...

    def resolve_item(
        self,
        item: Mapping[str, Any],
        sha1: str = "",
    ) -> tuple[IntroEpisodeSnapshot, ...]: ...

    def resolve_episode_ids(
        self,
        episode_ids: Sequence[str],
        *,
        require_existing_detection: bool,
    ) -> tuple[tuple[tuple[IntroEpisodeSnapshot, ...], ...], int]: ...

    def resolve_favorite_item(
        self,
        item: Mapping[str, Any],
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]: ...

    def list_library_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]: ...

    def list_favorite_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]: ...


class LegacyP7IntroRuntimeSource:
    """Read P7-owned policy and candidates through the frozen legacy helpers."""

    def __init__(self, repository: IntroDetectionSeasonRepository) -> None:
        self._repository = repository

    def snapshot(self) -> IntroDetectionPolicySnapshot:
        legacy = self._legacy()
        return IntroDetectionPolicySnapshot.create(
            trigger_mode=legacy.get_trigger_mode(),
            credits_enabled=legacy._credits_detection_enabled(),
            library_ids=legacy._intro_detection_library_ids(),
        )

    def season_is_favorite(self, series_tmdb_id: str, season_number: int) -> bool:
        season = self._repository.list_season_snapshots(series_tmdb_id, season_number)
        if not season:
            return False
        legacy = self._legacy()
        target = legacy._load_episode_ref_by_item_id(season[0].item_id, sha1=season[0].sha1)
        return bool(target and legacy._is_favorited_season(target))

    def resolve_item(
        self,
        item: Mapping[str, Any],
        sha1: str = "",
    ) -> tuple[IntroEpisodeSnapshot, ...]:
        legacy = self._legacy()
        item_payload = dict(item)
        target = legacy._load_episode_ref_by_item_id(
            str(item_payload.get("Id") or "").strip(),
            sha1=str(sha1 or "").strip(),
            fallback_item=item_payload,
        )
        return self._season(target)

    def resolve_episode_ids(
        self,
        episode_ids: Sequence[str],
        *,
        require_existing_detection: bool,
    ) -> tuple[tuple[tuple[IntroEpisodeSnapshot, ...], ...], int]:
        legacy = self._legacy()
        seasons: list[tuple[IntroEpisodeSnapshot, ...]] = []
        seen: set[tuple[str, int]] = set()
        unresolved = 0
        for item_id in episode_ids:
            target = legacy._load_episode_ref_by_item_id(item_id)
            if target is None:
                unresolved += 1
                continue
            if require_existing_detection and not legacy._season_has_intro_detection_cache(target):
                continue
            key = (str(target.series_tmdb_id), int(target.season_number))
            if key in seen:
                continue
            seen.add(key)
            season = self._season(target)
            if season:
                seasons.append(season)
        return tuple(seasons), unresolved

    def resolve_favorite_item(
        self,
        item: Mapping[str, Any],
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        item_payload = dict(item)
        item_type = str(item_payload.get("Type") or "").strip().title()
        if item_type == "Episode":
            season = self.resolve_item(item_payload)
            return (season,) if season else ()
        legacy = self._legacy()
        refs = legacy._load_favorite_scope_episode_refs(
            str(item_payload.get("Id") or "").strip(),
            item_type,
        )
        return self._seasons(refs)

    def list_library_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        refs = self._legacy()._load_library_episode_refs(
            limit=limit,
            selected_library_ids=tuple(library_ids),
        )
        return self._seasons(refs)

    def list_favorite_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        refs = self._legacy()._load_active_watchlist_episode_refs(
            limit=limit,
            selected_library_ids=tuple(library_ids),
        )
        return self._seasons(refs)

    def _seasons(self, refs: Sequence[Any]) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        seasons: list[tuple[IntroEpisodeSnapshot, ...]] = []
        seen: set[tuple[str, int]] = set()
        for target in refs:
            key = (str(getattr(target, "series_tmdb_id", "") or ""), int(getattr(target, "season_number", 0)))
            if not key[0] or key in seen:
                continue
            seen.add(key)
            season = self._season(target)
            if season:
                seasons.append(season)
        return tuple(seasons)

    def _season(self, target: Any) -> tuple[IntroEpisodeSnapshot, ...]:
        if target is None:
            return ()
        series_tmdb_id = str(getattr(target, "series_tmdb_id", "") or "").strip()
        season_number = int(getattr(target, "season_number", 0))
        if not series_tmdb_id:
            return ()
        return self._repository.list_season_snapshots(series_tmdb_id, season_number)

    @staticmethod
    def _legacy() -> Any:
        from handler import intro_detection_service

        return intro_detection_service


class NativeP115IntroDetectionGateway:
    """Reserve native season operations for all still-legacy P7 trigger call sites."""

    def __init__(
        self,
        *,
        submission: IntroDetectionSubmitter,
        source: IntroDetectionRuntimeSource,
        account_id: Callable[[], object],
    ) -> None:
        self._submission = submission
        self._source = source
        self._account_id = account_id

    def enqueue_item(
        self,
        item: Mapping[str, Any],
        sha1: str = "",
        *,
        trigger: str = "import",
    ) -> Mapping[str, Any]:
        try:
            policy = self._source.snapshot()
            normalized_trigger = _job_trigger(trigger)
            skipped = _policy_skip(policy, normalized_trigger)
            if skipped is not None:
                return skipped
            item_payload = dict(item) if isinstance(item, Mapping) else {}
            if (
                str(item_payload.get("Type") or "").strip().title() != "Episode"
                or not str(item_payload.get("Id") or "").strip()
                or not str(item_payload.get("SeriesId") or "").strip()
            ):
                return {"ok": True, "queued": False, "skipped": True, "reason": "invalid_episode"}
            season = self._source.resolve_item(item_payload, sha1)
            summary = self._reserve((season,) if season else (), normalized_trigger, policy)
            if summary["candidates"] == 0:
                return {"ok": True, "queued": False, "skipped": True, "reason": "unresolved_episode"}
            return {
                "ok": True,
                "queued": summary["created"] > 0,
                "item_id": str(item_payload.get("Id") or "").strip(),
                **summary,
            }
        except Exception:
            logger.debug("Native intro item reservation failed", exc_info=True)
            return {"ok": False, "queued": False, "reason": "service_unavailable"}

    def enqueue_imported_episode_ids(
        self,
        episode_ids: Sequence[Any],
        *,
        allow_playback_followup: bool = False,
    ) -> Mapping[str, Any]:
        try:
            policy = self._source.snapshot()
            if policy.trigger_mode is IntroTriggerMode.OFF:
                return {"ok": True, "queued": 0, "skipped": True, "reason": "disabled"}
            playback_followup = policy.trigger_mode is IntroTriggerMode.PLAYBACK and allow_playback_followup
            trigger = IntroJobTrigger.PLAYBACK if playback_followup else IntroJobTrigger.IMPORT
            skipped = _policy_skip(policy, trigger)
            if skipped is not None:
                return {**skipped, "queued": 0}
            ids = tuple(dict.fromkeys(str(value or "").strip() for value in episode_ids if str(value or "").strip()))
            if not ids:
                return {"ok": True, "queued": 0, "skipped": True, "reason": "empty"}
            seasons, unresolved = self._source.resolve_episode_ids(
                ids,
                require_existing_detection=playback_followup,
            )
            summary = self._reserve(seasons, trigger, policy)
            return {
                "ok": True,
                "queued": summary["created"],
                "unresolved": unresolved,
                **summary,
            }
        except Exception:
            logger.debug("Native imported intro reservation failed", exc_info=True)
            return {"ok": False, "queued": 0, "reason": "service_unavailable"}

    def enqueue_favorite_item(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            policy = self._source.snapshot()
            if policy.trigger_mode is not IntroTriggerMode.FAVORITE:
                return {"ok": True, "queued": False, "skipped": True, "reason": "trigger_not_selected"}
            item_payload = dict(item) if isinstance(item, Mapping) else {}
            item_type = str(item_payload.get("Type") or "").strip().title()
            if not str(item_payload.get("Id") or "").strip() or item_type not in {"Series", "Season", "Episode"}:
                return {"ok": True, "queued": False, "skipped": True, "reason": "unsupported_favorite"}
            seasons = self._source.resolve_favorite_item(item_payload)
            summary = self._reserve(seasons, IntroJobTrigger.FAVORITE, policy)
            return {
                "ok": True,
                "queued": summary["created"] > 0,
                "count": summary["created"],
                **summary,
            }
        except Exception:
            logger.debug("Native favorite intro reservation failed", exc_info=True)
            return {"ok": False, "queued": False, "reason": "service_unavailable"}

    def enqueue_playback_halfway(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        return self.enqueue_item(item, trigger=IntroJobTrigger.PLAYBACK.value)

    def enqueue_active_backfill(self, limit: int = 50, *, force: bool = False) -> Mapping[str, Any]:
        try:
            policy = self._source.snapshot()
            if policy.trigger_mode is IntroTriggerMode.PLAYBACK:
                return {
                    "ok": True,
                    "queued": False,
                    "skipped": True,
                    "reason": "playback_realtime_only",
                }
            if policy.trigger_mode is IntroTriggerMode.OFF or (
                not force and policy.trigger_mode is not IntroTriggerMode.IMPORT
            ):
                return {"ok": True, "queued": False, "skipped": True, "reason": "disabled"}
            batch_limit = max(1, min(int(limit or 50), 200))
            if policy.trigger_mode is IntroTriggerMode.IMPORT:
                seasons = self._source.list_library_seasons(
                    limit=batch_limit,
                    library_ids=policy.library_ids,
                )
                scope = "library"
            else:
                seasons = self._source.list_favorite_seasons(
                    limit=batch_limit,
                    library_ids=policy.library_ids,
                )
                scope = "favorite"
            summary = self._reserve(seasons, IntroJobTrigger.BACKFILL, policy)
            return {
                "ok": True,
                "queued": summary["created"] > 0,
                "completed": True,
                "count": summary["created"],
                "scope": scope,
                **summary,
            }
        except Exception:
            logger.debug("Native intro backfill reservation failed", exc_info=True)
            return {"ok": False, "queued": False, "reason": "service_unavailable"}

    def _reserve(
        self,
        seasons: Sequence[Sequence[IntroEpisodeSnapshot]],
        trigger: IntroJobTrigger,
        policy: IntroDetectionPolicySnapshot,
    ) -> dict[str, Any]:
        account_id = str(self._account_id() or "").strip()
        if not account_id.isdigit():
            raise ValueError("stable 115 account identity is unavailable")
        created = 0
        accepted = 0
        skipped = 0
        operation_ids: list[str] = []
        seen: set[tuple[str, int]] = set()
        for raw_season in seasons:
            season = tuple(raw_season)
            if not season:
                continue
            key = (season[0].series_tmdb_id, season[0].season_number)
            if key in seen:
                continue
            seen.add(key)
            try:
                intent = IntroDetectionIntent.create(
                    account_id=account_id,
                    trigger=trigger,
                    policy=policy,
                    episodes=season,
                )
            except ValueError:
                skipped += 1
                continue
            reservation: IntroDetectionReservation = self._submission.submit(intent)
            accepted += 1
            created += int(reservation.created)
            operation_ids.append(str(reservation.operation.id))
        return {
            "created": created,
            "accepted": accepted,
            "duplicates": accepted - created,
            "skipped_seasons": skipped,
            "candidates": len(seen),
            "operation_ids": operation_ids,
        }


class P115IntroBackfillTaskService:
    def __init__(self, gateway: NativeP115IntroDetectionGateway, *, limit: int = 50) -> None:
        self._gateway = gateway
        self._limit = max(1, min(int(limit), 200))

    def execute(self, progress: P115TaskProgress) -> TaskOutcome:
        progress.checkpoint()
        progress.update(0, "=== 开始提交片头片尾提取任务 ===")
        result = dict(self._gateway.enqueue_active_backfill(limit=self._limit))
        details = {**result, "native_intro_detection": True}
        if not result.get("ok"):
            message = "片头片尾提取任务提交失败"
            progress.update(-1, message)
            return TaskOutcome.failed(message, details)
        if result.get("skipped"):
            message = "当前触发策略无需执行片头片尾查漏"
            progress.update(100, message)
            return TaskOutcome.skipped(message, details)
        count = int(result.get("count") or 0)
        message = f"片头片尾查漏完成，已创建 {count} 个持久化季任务"
        progress.update(100, message)
        return TaskOutcome.succeeded(message, details)


class LegacyP9IntroChaptersChangedHook:
    """Best-effort P9 seam; shared-center failure never changes P6 state."""

    def chapters_staged(
        self,
        operation_id: str,
        episode_sha1s: Sequence[str],
        effect_ids: Sequence[str],
    ) -> None:
        del effect_ids
        for sha1 in dict.fromkeys(str(value or "").strip().upper() for value in episode_sha1s):
            if not sha1:
                continue
            try:
                spawn(self._publish, sha1, str(operation_id or ""))
            except Exception:
                logger.debug("Unable to schedule P9 intro compatibility export", exc_info=True)

    @staticmethod
    def _publish(sha1: str, operation_id: str) -> None:
        try:
            from handler.p115_service import P115CacheManager
            from handler.shared_intro_service import upload_intro_for_cache_sha1

            cached_text = P115CacheManager.get_mediainfo_cache_text(sha1)
            if not cached_text:
                return
            upload_intro_for_cache_sha1(
                sha1,
                json.loads(cached_text),
                reason="etk_intro_detection",
            )
        except Exception:
            logger.debug(
                "P9 intro compatibility export failed after operation %s",
                operation_id,
                exc_info=True,
            )


def _job_trigger(value: object) -> IntroJobTrigger:
    try:
        return IntroJobTrigger(str(value or "").strip().lower())
    except ValueError:
        return IntroJobTrigger.IMPORT


def _policy_skip(
    policy: IntroDetectionPolicySnapshot,
    trigger: IntroJobTrigger,
) -> dict[str, Any] | None:
    if policy.trigger_mode is IntroTriggerMode.OFF:
        return {"ok": True, "queued": False, "skipped": True, "reason": "disabled"}
    if not policy.accepts(trigger):
        return {"ok": True, "queued": False, "skipped": True, "reason": "trigger_not_selected"}
    return None


__all__ = [
    "IntroDetectionRuntimeSource",
    "IntroDetectionSeasonRepository",
    "IntroDetectionSubmitter",
    "LegacyP7IntroRuntimeSource",
    "LegacyP9IntroChaptersChangedHook",
    "NativeP115IntroDetectionGateway",
    "P115IntroBackfillTaskService",
]
