from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from etk.platform.tasks import TaskOutcomeStatus

from .daily_theme import DAILY_THEME_REPLENISHMENT_EFFECT_TYPE, DailyThemeService
from .effect_execution import MediaEffectDeliveryRejected, MediaEffectDeliveryRetryable
from .effects import MediaEffectProjection


class _EffectProgress:
    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        return


class DailyThemeReplenishmentEffectDelivery:
    effect_types = frozenset({DAILY_THEME_REPLENISHMENT_EFFECT_TYPE})

    def __init__(self, service: DailyThemeService) -> None:
        self._service = service

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        if effect.effect_type != DAILY_THEME_REPLENISHMENT_EFFECT_TYPE:
            raise ValueError("daily-theme replenishment effect type is invalid")
        revision = effect.payload.get("publication_revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("daily-theme replenishment publication revision is invalid")
        outcome = self._service.execute(
            _EffectProgress(),
            publication_id=str(effect.id),
            mode="replenish",
        )
        result = {
            "status": outcome.status.value,
            "message": outcome.message,
            "details": dict(outcome.details),
            "trigger_publication_revision": revision,
        }
        if outcome.status is TaskOutcomeStatus.FAILED:
            error = {
                "reason_code": str(outcome.details.get("reason_code") or "daily_theme_replenishment_failed"),
                "trigger_publication_revision": revision,
            }
            if outcome.details.get("retryable") is True:
                raise MediaEffectDeliveryRetryable(outcome.message, error=error)
            raise MediaEffectDeliveryRejected(outcome.message, error=error)
        if outcome.status not in {TaskOutcomeStatus.SUCCEEDED, TaskOutcomeStatus.SKIPPED}:
            raise MediaEffectDeliveryRejected(
                "daily-theme replenishment returned an unsupported outcome",
                error={"status": outcome.status.value},
            )
        return result


__all__ = ["DailyThemeReplenishmentEffectDelivery"]
