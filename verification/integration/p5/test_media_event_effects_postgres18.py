from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from sqlalchemy import text

from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.media.event_effects import P5MediaEventEffectDelivery, PersistentP5MediaEventGateway
from etk.domains.media.models import ServiceResult
from etk.platform.db import DatabaseRuntime


class FakeEventCompatibility:
    def __init__(self) -> None:
        self.calls: list[tuple[Mapping[str, Any], str, str]] = []
        self.result = ServiceResult({"status": "metadata_update_task_debounced", "item_id": "movie-1"}, 202)

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        self.calls.append((dict(payload), host_url, remote_addr))
        return self.result


def _payload(event_id: str, event_name: str = "metadata.update") -> dict[str, Any]:
    return {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": event_name,
        "EventId": event_id,
        "Item": {"Id": "movie-1", "Type": "Movie", "Name": "Fixture Movie"},
    }


def test_pg18_p5_event_effect_preserves_response_and_replay_is_idempotent(
    p5_runtime: DatabaseRuntime,
) -> None:
    compatibility = FakeEventCompatibility()
    gateway = PersistentP5MediaEventGateway(
        p5_runtime.unit_of_work,
        compatibility,
        worker_id="fixture-api:event",
    )
    payload = _payload("fixture-event-confirmed")

    first = gateway.handle(
        payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    replay = gateway.handle(
        payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert first == ServiceResult(
        {"status": "metadata_update_task_debounced", "item_id": "movie-1"},
        202,
    )
    assert replay == first
    assert len(compatibility.calls) == 1
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert connection.scalar(text("SELECT status FROM media_effects")) == "confirmed"
        assert (
            connection.scalar(text("SELECT status FROM outbox_events WHERE topic = 'media.effect.requested'"))
            == "processed"
        )


def test_pg18_p5_event_rejection_and_unknown_outcome_are_terminal_without_replay(
    p5_runtime: DatabaseRuntime,
) -> None:
    compatibility = FakeEventCompatibility()
    gateway = PersistentP5MediaEventGateway(
        p5_runtime.unit_of_work,
        compatibility,
        worker_id="fixture-api:event",
    )
    compatibility.result = ServiceResult({"status": "invalid_event"}, 400)
    rejected_payload = _payload("fixture-event-rejected", "image.update")
    rejected = gateway.handle(
        rejected_payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    compatibility.result = ServiceResult({"status": "legacy_event_failed"}, 500)
    unknown_payload = _payload("fixture-event-unknown")
    first_unknown = gateway.handle(
        unknown_payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    second_unknown = gateway.handle(
        unknown_payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert rejected == ServiceResult({"status": "invalid_event"}, 400)
    assert first_unknown.status_code == 503
    assert first_unknown.payload["status"] == "event_processing_indeterminate"
    assert first_unknown.payload["effect_status"] == "indeterminate"
    assert second_unknown == first_unknown
    assert len(compatibility.calls) == 2
    with p5_runtime.engine.connect() as connection:
        statuses = set(connection.scalars(text("SELECT status FROM media_effects")))
    assert statuses == {"rejected", "indeterminate"}


def test_pg18_p5_event_api_defers_and_worker_delivery_confirms_once(
    p5_runtime: DatabaseRuntime,
) -> None:
    native = FakeEventCompatibility()
    api = PersistentP5MediaEventGateway(
        p5_runtime.unit_of_work,
        native,
        defer=True,
    )
    payload = _payload("fixture-event-worker")

    pending = api.handle(payload, host_url="http://fixture.invalid/", remote_addr="127.0.0.1")
    assert pending.status_code == 202
    assert pending.payload["effect_status"] == "pending"
    assert native.calls == []

    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        P5MediaEventEffectDelivery(native),
        worker_id="fixture-worker:event",
    )
    confirmed = executor.execute(UUID(str(pending.payload["effect_id"])))
    replay = executor.execute(confirmed.id)

    assert confirmed.status.value == "confirmed"
    assert replay == confirmed
    assert len(native.calls) == 1
