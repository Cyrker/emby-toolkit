from __future__ import annotations

from sqlalchemy import text

from etk.domains.subscription.actor import ActorSubscriptionCancellation
from etk.domains.subscription.actor_operations import ActorCancellationOperationStager
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.platform.db import DatabaseRuntime


def test_actor_cancellation_operation_and_outbox_commit_with_owning_uow(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    stager = ActorCancellationOperationStager(repository)
    cancellation = ActorSubscriptionCancellation(31, "p7-actor-operation-season", "Season", 2)

    with p7_runtime.unit_of_work() as uow:
        stager.stage(uow, cancellation)
        staged_count = uow.session.scalar(text("SELECT COUNT(*) FROM subscription_operations"))
        assert staged_count == 1
        uow.commit()

    with p7_runtime.engine.connect() as connection:
        operation = (
            connection.execute(
                text(
                    """
                SELECT kind, scope_key, request_json, status
                FROM subscription_operations
                """
                )
            )
            .mappings()
            .one()
        )
        outbox = (
            connection.execute(
                text(
                    """
                SELECT topic, aggregate_id, status
                FROM outbox_events
                WHERE topic = 'subscription.operation.requested'
                """
                )
            )
            .mappings()
            .one()
        )

    assert dict(operation) == {
        "kind": "delete",
        "scope_key": "actor-subscription:31:season:p7-actor-operation-season:2",
        "request_json": {
            "action": "delete",
            "actor_subscription_id": 31,
            "item_type": "Season",
            "season": 2,
            "tmdb_id": "p7-actor-operation-season",
        },
        "status": "ready",
    }
    assert outbox["topic"] == "subscription.operation.requested"
    assert outbox["aggregate_id"]
    assert outbox["status"] == "pending"


def test_actor_cancellation_operation_rolls_back_with_owning_uow(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    stager = ActorCancellationOperationStager(repository)

    with p7_runtime.unit_of_work() as uow:
        stager.stage(
            uow,
            ActorSubscriptionCancellation(32, "p7-actor-operation-movie", "Movie", None),
        )

    with p7_runtime.engine.connect() as connection:
        operation_count = connection.scalar(text("SELECT COUNT(*) FROM subscription_operations"))
        outbox_count = connection.scalar(
            text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'subscription.operation.requested'")
        )

    assert operation_count == 0
    assert outbox_count == 0
