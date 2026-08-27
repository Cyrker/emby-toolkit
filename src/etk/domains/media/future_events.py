from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from etk.domains.collection.effects import CollectionDeletionSuppressionPort
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.operations import CollectionOperationIntent, CollectionOperationKind
from etk.domains.identity.user_activity import P8UserEventGateway
from etk.platform.jobs.types import UnitOfWorkFactory

from .effects import MediaEffectConflictError, SqlAlchemyMediaEffectRepository
from .models import ServiceResult


class P8CollectionRemovalEventGateway:
    def __init__(
        self,
        unit_of_work: UnitOfWorkFactory,
        suppression: CollectionDeletionSuppressionPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._operations = PostgresCollectionOperationRepository(unit_of_work)
        self._suppression = suppression

    def handle(self, payload: Mapping[str, Any]) -> ServiceResult:
        if str(payload.get("Event") or "").strip() != "collection.items.removed":
            return ServiceResult({"status": "event_ignored_not_in_trigger_list"})
        item = payload.get("Item")
        if not isinstance(item, Mapping):
            return ServiceResult({"status": "event_ignored_no_id_or_wrong_type"})
        collection_id = str(item.get("Id") or payload.get("ItemId") or "").strip()
        if not collection_id or str(item.get("Type") or "").strip() != "BoxSet":
            return ServiceResult({"status": "event_ignored_no_id_or_wrong_type"})
        event_id = str(payload.get("EventId") or payload.get("event_id") or "").strip()
        if not event_id:
            return ServiceResult({"status": "event_rejected_missing_event_id"}, 400)
        event_key = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        receipt_key = f"p8:collection-removal:{event_key}"
        receipt_payload = {
            "event_name": "collection.items.removed",
            "payload_sha256": _payload_digest(payload),
        }
        suppressed = self._suppression.is_suppressed(collection_id)
        result = ServiceResult(
            {"status": ("ignored_manual_deletion" if suppressed else "collection_removal_check_started")},
            200 if suppressed else 202,
        )
        intent = CollectionOperationIntent.create(
            kind=CollectionOperationKind.REMOVAL_SURVIVAL_CHECK,
            scope_key=f"emby-collection:{collection_id}",
            principal_id="collection-system:removal-event",
            request={
                "emby_collection_id": collection_id,
                "event_identity": event_id,
            },
            idempotency_key=event_id,
        )
        with self._unit_of_work() as uow:
            receipt = SqlAlchemyMediaEffectRepository(uow.session).record_confirmed(
                effect_key=receipt_key,
                effect_type="emby.event.collection.removal.applied",
                aggregate_type="emby_collection_event",
                aggregate_id=collection_id,
                payload=receipt_payload,
                result={"status_code": result.status_code, "payload": dict(result.payload)},
            )
            if receipt.created and not suppressed:
                self._operations.reserve_in_uow(uow, intent)
            uow.commit()
        if not receipt.created:
            return _receipt_result(receipt.effect.result)
        return result


class P8FutureDomainEventGateway:
    def __init__(
        self,
        users: P8UserEventGateway,
        collections: P8CollectionRemovalEventGateway,
    ) -> None:
        self._users = users
        self._collections = collections

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        if str(payload.get("Event") or "").strip() == "collection.items.removed":
            try:
                return self._collections.handle(payload)
            except MediaEffectConflictError:
                return ServiceResult({"status": "event_id_conflict"}, 409)
            except Exception:
                return ServiceResult({"status": "error_processing_collection_event"}, 500)
        return self._users.handle(
            payload,
            host_url=host_url,
            remote_addr=remote_addr,
        )


def _receipt_result(value: Mapping[str, Any] | None) -> ServiceResult:
    result = value if isinstance(value, Mapping) else {}
    payload = result.get("payload")
    try:
        status_code = int(result.get("status_code") or 200)
    except (TypeError, ValueError):
        status_code = 200
    return ServiceResult(
        dict(payload) if isinstance(payload, Mapping) else {"status": "collection_event_applied"},
        status_code,
    )


def _payload_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = ["P8CollectionRemovalEventGateway", "P8FutureDomainEventGateway"]
