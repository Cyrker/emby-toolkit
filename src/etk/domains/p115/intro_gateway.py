from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from .intro_detection import (
    IntroDetectionIntent,
    IntroDetectionPolicySnapshot,
    IntroDetectionReservation,
    IntroEpisodeSnapshot,
    IntroJobTrigger,
    IntroTriggerMode,
)

logger = logging.getLogger(__name__)


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
        credits_enabled: bool | None = None,
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]: ...

    def list_favorite_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
        credits_enabled: bool | None = None,
    ) -> Sequence[Sequence[IntroEpisodeSnapshot]]: ...


class NativeP115IntroDetectionGateway:
    """Reserve immutable season operations for native P6/P7 trigger call sites."""

    def __init__(
        self,
        *,
        submission: IntroDetectionSubmitter,
        source: IntroDetectionRuntimeSource,
        account_id: Callable[[], object],
        provider_backend: Callable[[], object] = lambda: "openapi",
        provider_identity: Callable[[], tuple[object, object]] | None = None,
    ) -> None:
        self._submission = submission
        self._source = source
        self._account_id = account_id
        self._provider_backend = provider_backend
        self._provider_identity = provider_identity

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
                    credits_enabled=policy.credits_enabled,
                )
                scope = "library"
            else:
                seasons = self._source.list_favorite_seasons(
                    limit=batch_limit,
                    library_ids=policy.library_ids,
                    credits_enabled=policy.credits_enabled,
                )
                scope = "active_watchlist"
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
        if self._provider_identity is None:
            raw_account_id, raw_backend = self._account_id(), self._provider_backend()
        else:
            raw_account_id, raw_backend = self._provider_identity()
        account_id = str(raw_account_id or "").strip()
        if not account_id.isdigit():
            raise ValueError("stable 115 account identity is unavailable")
        provider_backend = str(raw_backend or "").strip().lower()
        if provider_backend not in {"openapi", "cookie"}:
            raise ValueError("stable 115 provider backend is unavailable")
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
                    provider_backend=provider_backend,
                    trigger=trigger,
                    policy=policy,
                    episodes=season,
                )
            except ValueError:
                skipped += 1
                continue
            reservation = self._submission.submit(intent)
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


__all__ = ["IntroDetectionRuntimeSource", "IntroDetectionSubmitter", "NativeP115IntroDetectionGateway"]
