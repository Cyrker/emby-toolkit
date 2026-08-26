from __future__ import annotations

from typing import cast

import pytest

from etk.domains.subscription.actor import ActorSubscriptionCancellation
from etk.domains.subscription.actor_operations import ActorCancellationOperationStager
from etk.domains.subscription.operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationReservation,
)
from etk.platform.db import SqlAlchemyUnitOfWork


class CapturingRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[SqlAlchemyUnitOfWork, SubscriptionOperationIntent]] = []

    def reserve_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation:
        self.calls.append((uow, intent))
        return cast(SubscriptionOperationReservation, object())


def test_actor_cancellation_stages_stable_season_delete_operation() -> None:
    repository = CapturingRepository()
    stager = ActorCancellationOperationStager(repository)
    uow = cast(SqlAlchemyUnitOfWork, object())
    cancellation = ActorSubscriptionCancellation(17, "1399", "Season", 4)

    stager.stage(uow, cancellation)
    stager.stage(uow, cancellation)

    assert len(repository.calls) == 2
    first = repository.calls[0][1]
    replay = repository.calls[1][1]
    assert first.id == replay.id
    assert first.kind is SubscriptionOperationKind.DELETE
    assert first.scope_key == "actor-subscription:17:season:1399:4"
    assert first.request == {
        "action": "delete",
        "tmdb_id": "1399",
        "season": 4,
        "actor_subscription_id": 17,
        "item_type": "Season",
    }


def test_actor_movie_cancellation_does_not_send_a_provider_season() -> None:
    repository = CapturingRepository()
    stager = ActorCancellationOperationStager(repository)

    stager.stage(
        cast(SqlAlchemyUnitOfWork, object()),
        ActorSubscriptionCancellation(18, "550", "Movie", None),
    )

    assert repository.calls[0][1].request["season"] is None


@pytest.mark.parametrize(
    "cancellation",
    [
        ActorSubscriptionCancellation(0, "550", "Movie", None),
        ActorSubscriptionCancellation(1, "", "Movie", None),
        ActorSubscriptionCancellation(1, "1399", "Season", None),
    ],
)
def test_actor_cancellation_rejects_incomplete_provider_identity(
    cancellation: ActorSubscriptionCancellation,
) -> None:
    stager = ActorCancellationOperationStager(CapturingRepository())

    with pytest.raises(ValueError):
        stager.stage(cast(SqlAlchemyUnitOfWork, object()), cancellation)
