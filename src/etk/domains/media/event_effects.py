from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from etk.platform.jobs.types import UnitOfWorkFactory

from .effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    PersistentMediaEffectExecutor,
)
from .effects import MediaEffectProjection, MediaEffectStatus, PostgresMediaEffectLedger
from .event_dispatch import EmbyEventOwner, P5MediaEventGateway, classify_emby_event
from .models import ServiceResult

_EVENT_EFFECT_TYPES = {
    "metadata.update": "emby.event.metadata.update",
    "image.update": "emby.event.image.update",
}


class P5MediaEventEffectDelivery:
    effect_types = frozenset(_EVENT_EFFECT_TYPES.values())

    def __init__(self, compatibility: P5MediaEventGateway) -> None:
        self._compatibility = compatibility

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        payload = effect.payload
        event_payload = payload.get("event_payload")
        if not isinstance(event_payload, Mapping):
            raise ValueError("P5 media event effect payload is invalid")
        result = self._compatibility.handle(
            dict(event_payload),
            host_url=str(payload.get("host_url") or ""),
            remote_addr=str(payload.get("remote_addr") or ""),
        )
        response = {
            "status_code": result.status_code,
            "payload": dict(result.payload),
        }
        if result.headers is not None:
            response["headers"] = dict(result.headers)
        if result.status_code >= 500:
            raise MediaEffectDeliveryIndeterminate(
                "P5 media event processing returned a server failure",
                error=response,
            )
        if result.status_code >= 400:
            raise MediaEffectDeliveryRejected(
                "P5 media event was rejected",
                error=response,
            )
        return response


class PersistentP5MediaEventGateway:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        compatibility: P5MediaEventGateway,
        *,
        worker_id: str | None = None,
        lease_seconds: float | Callable[[], float] = 90,
    ) -> None:
        self._ledger = PostgresMediaEffectLedger(uow_factory)
        self._executor = PersistentMediaEffectExecutor(
            self._ledger,
            P5MediaEventEffectDelivery(compatibility),
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        event_name = str(payload.get("Event") or "").strip()
        if classify_emby_event(event_name) is not EmbyEventOwner.P5_MEDIA:
            raise ValueError(f"event {event_name!r} is not owned by P5 media")
        effect_payload = {
            "event_payload": dict(payload),
            "host_url": str(host_url or ""),
            "remote_addr": str(remote_addr or ""),
        }
        reservation = self._ledger.reserve(
            effect_key=_event_effect_key(payload),
            effect_type=_EVENT_EFFECT_TYPES[event_name],
            aggregate_type="emby_event",
            aggregate_id=_event_aggregate_id(payload),
            payload=effect_payload,
        )
        return self.execute(reservation.effect.id)

    def execute(self, effect_id: UUID) -> ServiceResult:
        effect = self._executor.execute(effect_id)
        if effect.status is MediaEffectStatus.CONFIRMED:
            return _service_result(effect.result or {})
        if effect.status is MediaEffectStatus.REJECTED:
            return _service_result(effect.error or {}, default_status=400)
        if effect.status is MediaEffectStatus.INDETERMINATE:
            return ServiceResult(
                {
                    "status": "event_processing_indeterminate",
                    "effect_id": str(effect.id),
                    "effect_status": effect.status.value,
                },
                503,
            )
        return ServiceResult(
            {
                "status": "event_effect_pending",
                "effect_id": str(effect.id),
                "effect_status": effect.status.value,
            },
            202,
        )


def _service_result(value: Mapping[str, Any], *, default_status: int = 200) -> ServiceResult:
    payload = value.get("payload")
    headers = value.get("headers")
    try:
        status_code = int(value.get("status_code") or default_status)
    except (TypeError, ValueError):
        status_code = default_status
    return ServiceResult(
        dict(payload) if isinstance(payload, Mapping) else {"status": "invalid_event_effect_result"},
        status_code,
        headers=dict(headers) if isinstance(headers, Mapping) else None,
    )


def _event_effect_key(payload: Mapping[str, Any]) -> str:
    explicit = str(
        payload.get("EventId")
        or payload.get("event_id")
        or payload.get("IdempotencyKey")
        or payload.get("idempotency_key")
        or ""
    ).strip()
    source = explicit or json.dumps(
        dict(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    return f"p5:emby-event:{digest}"


def _event_aggregate_id(payload: Mapping[str, Any]) -> str:
    item = payload.get("Item")
    item_id = str(item.get("Id") or "").strip() if isinstance(item, Mapping) else ""
    event_name = str(payload.get("Event") or "").strip()
    return f"{event_name}:{item_id or 'unknown'}"


__all__ = ["P5MediaEventEffectDelivery", "PersistentP5MediaEventGateway"]
