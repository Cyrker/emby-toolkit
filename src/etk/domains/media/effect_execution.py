from __future__ import annotations

import socket
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from etk.platform.jobs.json_payload import normalize_json_object
from etk.platform.observability.redaction import redact_sensitive

from .effects import (
    MediaEffectNotFoundError,
    MediaEffectProjection,
    PostgresMediaEffectLedger,
)


class MediaEffectDeliveryRejected(RuntimeError):
    def __init__(self, message: str, *, error: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.error = dict(error or {})


class MediaEffectDeliveryIndeterminate(RuntimeError):
    def __init__(self, message: str, *, error: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.error = dict(error or {})


class MediaEffectDeliveryRetryable(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 1,
    ) -> None:
        super().__init__(message)
        self.error = dict(error or {})
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))


class MediaEffectExecutionUnavailable(RuntimeError):
    pass


class MediaEffectDelivery(Protocol):
    @property
    def effect_types(self) -> frozenset[str]: ...

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]: ...


class RoutedMediaEffectDelivery:
    def __init__(self, deliveries: Sequence[MediaEffectDelivery]) -> None:
        routes: dict[str, MediaEffectDelivery] = {}
        for delivery in deliveries:
            for effect_type in delivery.effect_types:
                normalized = str(effect_type or "").strip()
                if not normalized:
                    raise ValueError("media effect delivery type must not be empty")
                if normalized in routes:
                    raise ValueError(f"duplicate media effect delivery type: {normalized}")
                routes[normalized] = delivery
        if not routes:
            raise ValueError("at least one media effect delivery is required")
        self._routes = routes

    @property
    def effect_types(self) -> frozenset[str]:
        return frozenset(self._routes)

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        delivery = self._routes.get(effect.effect_type)
        if delivery is None:
            raise MediaEffectDeliveryRejected(
                f"unsupported media effect type: {effect.effect_type}",
                error={"type": "unsupported_effect_type", "effect_type": effect.effect_type},
            )
        return delivery.deliver(effect)


class PersistentMediaEffectExecutor:
    def __init__(
        self,
        ledger: PostgresMediaEffectLedger,
        delivery: MediaEffectDelivery,
        *,
        worker_id: str | None = None,
        lease_seconds: float | Callable[[], float] = 90,
    ) -> None:
        self._ledger = ledger
        self._delivery = delivery
        self._worker_id = str(worker_id or f"media-effect:{socket.gethostname()}").strip()
        if not callable(lease_seconds) and float(lease_seconds) <= 0:
            raise ValueError("media effect lease must be positive")
        self._lease_seconds = lease_seconds if callable(lease_seconds) else lambda: float(lease_seconds)
        if not self._worker_id:
            raise ValueError("media effect worker id must not be empty")

    def execute(self, effect_id: UUID) -> MediaEffectProjection:
        current = self._ledger.read_by_id(effect_id)
        if current is None:
            raise MediaEffectNotFoundError(f"media effect {effect_id} was not found")
        if current.status.terminal:
            return current
        if current.effect_type not in self._delivery.effect_types:
            raise MediaEffectExecutionUnavailable(
                f"media effect type {current.effect_type!r} is unavailable in this Worker profile"
            )
        claimed = self._ledger.claim_by_id(
            effect_id,
            worker_id=self._worker_id,
            lease_seconds=self._claim_lease_seconds(),
        )
        if claimed is None:
            current = self._ledger.read_by_id(effect_id)
            if current is None:
                raise MediaEffectNotFoundError(f"media effect {effect_id} was not found")
            return current
        return self._deliver_claimed(claimed)

    def read(self, effect_key: str) -> MediaEffectProjection | None:
        return self._ledger.read(effect_key)

    def reserve_and_execute(
        self,
        *,
        effect_key: str,
        effect_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any],
    ) -> MediaEffectProjection:
        if effect_type not in self._delivery.effect_types:
            raise MediaEffectExecutionUnavailable(
                f"media effect type {effect_type!r} is unavailable in this Worker profile"
            )
        reservation = self._ledger.reserve_and_claim(
            effect_key=effect_key,
            effect_type=effect_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=payload,
            worker_id=self._worker_id,
            lease_seconds=self._claim_lease_seconds(),
        )
        if not reservation.claimed:
            return reservation.effect
        return self._deliver_claimed(reservation.effect)

    def _claim_lease_seconds(self) -> float:
        lease_seconds = float(self._lease_seconds())
        if lease_seconds <= 0:
            raise ValueError("media effect lease must be positive")
        return lease_seconds

    def _deliver_claimed(self, claimed: MediaEffectProjection) -> MediaEffectProjection:
        assert claimed.claim_token is not None
        try:
            result = self._delivery.deliver(claimed)
        except MediaEffectDeliveryRejected as exc:
            transition = self._ledger.mark_rejected(
                claimed.effect_key,
                claimed.claim_token,
                error=_error_payload("provider_rejected", exc, exc.error),
            )
        except MediaEffectDeliveryIndeterminate as exc:
            transition = self._ledger.mark_indeterminate(
                claimed.effect_key,
                claimed.claim_token,
                error=_error_payload("provider_outcome_unknown", exc, exc.error),
            )
        except MediaEffectDeliveryRetryable as exc:
            transition = self._ledger.mark_retryable(
                claimed.effect_key,
                claimed.claim_token,
                error=_error_payload("delivery_retryable", exc, exc.error),
                retry_after_seconds=exc.retry_after_seconds,
            )
        except ValueError as exc:
            transition = self._ledger.mark_rejected(
                claimed.effect_key,
                claimed.claim_token,
                error=_error_payload("invalid_effect_payload", exc),
            )
        except Exception as exc:
            transition = self._ledger.mark_indeterminate(
                claimed.effect_key,
                claimed.claim_token,
                error=_error_payload("unexpected_provider_failure", exc),
            )
        else:
            safe_result = redact_sensitive(dict(result))
            if not isinstance(safe_result, Mapping):
                raise TypeError("redacted media effect result must be an object")
            transition = self._ledger.mark_confirmed(
                claimed.effect_key,
                claimed.claim_token,
                result=normalize_json_object(dict(safe_result), field="media effect provider result"),
            )
        return transition.effect


def _error_payload(
    error_type: str,
    exc: BaseException,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    safe_details = redact_sensitive(dict(details or {}))
    if not isinstance(safe_details, Mapping):
        safe_details = {}
    safe_message = redact_sensitive(str(exc))
    return normalize_json_object(
        {
            "type": error_type,
            **dict(safe_details),
            "message": str(safe_message),
        },
        field="media effect provider error",
    )


__all__ = [
    "MediaEffectDelivery",
    "MediaEffectDeliveryIndeterminate",
    "MediaEffectDeliveryRejected",
    "MediaEffectDeliveryRetryable",
    "MediaEffectExecutionUnavailable",
    "PersistentMediaEffectExecutor",
    "RoutedMediaEffectDelivery",
]
