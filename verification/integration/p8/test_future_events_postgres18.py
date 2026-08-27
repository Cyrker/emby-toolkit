from __future__ import annotations

from typing import Any, cast

from sqlalchemy import text

from etk.domains.collection.effects import CollectionDeletionSuppressionPort
from etk.domains.identity.user_activity import P8UserEventGateway
from etk.domains.media.future_events import (
    P8CollectionRemovalEventGateway,
    P8FutureDomainEventGateway,
)
from etk.platform.db import DatabaseRuntime


class FixtureSuppression:
    def __init__(self, suppressed: bool = False) -> None:
        self.suppressed = suppressed
        self.calls: list[str] = []

    @staticmethod
    def suppress(collection_id: str, *, ttl_seconds: float) -> None:
        del collection_id, ttl_seconds

    def is_suppressed(self, collection_id: str) -> bool:
        self.calls.append(collection_id)
        return self.suppressed


def _gateway(
    runtime: DatabaseRuntime,
    suppression: FixtureSuppression,
) -> P8FutureDomainEventGateway:
    return P8FutureDomainEventGateway(
        cast(P8UserEventGateway, object()),
        P8CollectionRemovalEventGateway(
            runtime.unit_of_work,
            cast(CollectionDeletionSuppressionPort, suppression),
        ),
    )


def _event(event_id: str, *, name: str = "Fixture Collection") -> dict[str, Any]:
    return {
        "Event": "collection.items.removed",
        "EventId": event_id,
        "Item": {"Id": "box-1", "Type": "BoxSet", "Name": name},
    }


def _handle(gateway: P8FutureDomainEventGateway, payload: dict[str, Any]):
    return gateway.handle(payload, host_url="https://fixture.invalid", remote_addr="127.0.0.1")


def test_suppressed_collection_event_records_replay_receipt_without_operation(
    p8_runtime: DatabaseRuntime,
) -> None:
    suppression = FixtureSuppression(True)
    gateway = _gateway(p8_runtime, suppression)

    first = _handle(gateway, _event("suppressed-event"))
    suppression.suppressed = False
    replay = _handle(gateway, _event("suppressed-event"))

    assert first.status_code == replay.status_code == 200
    assert first.payload == replay.payload == {"status": "ignored_manual_deletion"}
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*) FROM media_effects
                WHERE effect_type = 'emby.event.collection.removal.applied'
                """
                )
            )
            == 1
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM collection_operations")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 0


def test_collection_event_reserves_survival_operation_and_outbox_atomically(
    p8_runtime: DatabaseRuntime,
) -> None:
    gateway = _gateway(p8_runtime, FixtureSuppression(False))
    payload = _event("survival-event")

    first = _handle(gateway, payload)
    replay = _handle(gateway, payload)

    assert first.status_code == replay.status_code == 202
    assert first.payload == replay.payload == {"status": "collection_removal_check_started"}
    with p8_runtime.engine.connect() as connection:
        operation = connection.execute(
            text(
                """
                SELECT kind, status, request_json
                FROM collection_operations
                """
            )
        ).one()
        assert operation.kind == "collection.removal.survival-check"
        assert operation.status == "ready"
        assert operation.request_json["event_identity"] == "survival-event"
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'collection.operation.requested'"))
            == 1
        )

    conflict = _handle(gateway, _event("survival-event", name="Changed Collection"))
    assert conflict.status_code == 409
    assert conflict.payload == {"status": "event_id_conflict"}


def test_collection_event_outbox_failure_rolls_back_receipt_and_operation(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_collection_event_outbox() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.topic = 'collection.operation.requested' THEN
                        RAISE EXCEPTION 'fixture collection outbox rejected';
                    END IF;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_collection_event_outbox
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION p8_reject_collection_event_outbox()
                """
            )
        )
    try:
        result = _handle(_gateway(p8_runtime, FixtureSuppression(False)), _event("rollback-event"))
        assert result.status_code == 500
        with p8_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM media_effects")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM collection_operations")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 0
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_collection_event_outbox ON outbox_events"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_collection_event_outbox()"))


def test_collection_event_requires_boxset_identity_and_stable_event_id(
    p8_runtime: DatabaseRuntime,
) -> None:
    gateway = _gateway(p8_runtime, FixtureSuppression(False))

    wrong_type = _handle(
        gateway,
        {"Event": "collection.items.removed", "EventId": "wrong", "Item": {"Id": "item", "Type": "Movie"}},
    )
    missing_event_id = _handle(
        gateway,
        {"Event": "collection.items.removed", "Item": {"Id": "box", "Type": "BoxSet"}},
    )

    assert wrong_type.payload == {"status": "event_ignored_no_id_or_wrong_type"}
    assert wrong_type.status_code == 200
    assert missing_event_id.status_code == 400
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM collection_operations")) == 0
