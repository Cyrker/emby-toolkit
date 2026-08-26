from __future__ import annotations

from typing import Protocol

from etk.platform.db import SqlAlchemyUnitOfWork

from .actor import ActorSubscriptionCancellation
from .operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationReservation,
)


class ActorCancellationOperationRepository(Protocol):
    def reserve_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation: ...


class ActorCancellationOperationStager:
    """Stages a MoviePilot cancellation in the actor-delete transaction."""

    def __init__(self, repository: ActorCancellationOperationRepository) -> None:
        self._repository = repository

    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: ActorSubscriptionCancellation,
    ) -> None:
        item_type = str(intent.item_type or "").strip()
        tmdb_id = str(intent.tmdb_id or "").strip()
        if not item_type or not tmdb_id:
            raise ValueError("actor cancellation requires item_type and tmdb_id")
        if intent.actor_subscription_id < 1:
            raise ValueError("actor cancellation requires a positive actor_subscription_id")
        if item_type == "Season" and intent.season_number is None:
            raise ValueError("actor Season cancellation requires season_number")

        request = {
            "action": "delete",
            "tmdb_id": tmdb_id,
            "season": intent.season_number if item_type == "Season" else None,
            "actor_subscription_id": intent.actor_subscription_id,
            "item_type": item_type,
        }
        operation = SubscriptionOperationIntent.create(
            kind=SubscriptionOperationKind.DELETE,
            scope_key=(
                f"actor-subscription:{intent.actor_subscription_id}:"
                f"{item_type.casefold()}:{tmdb_id}:{intent.season_number}"
            ),
            request=request,
        )
        self._repository.reserve_in_uow(uow, operation)


__all__ = [
    "ActorCancellationOperationRepository",
    "ActorCancellationOperationStager",
]
