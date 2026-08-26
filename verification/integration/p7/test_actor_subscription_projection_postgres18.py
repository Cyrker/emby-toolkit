from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.actor_subscription_projection import SqlAlchemyActorSubscriptionProjection
from etk.domains.subscription.actor import (
    ActorCancellationIntentStagePort,
    ActorSubscriptionCancellation,
    SqlAlchemyActorSubscriptionRepository,
)
from etk.platform.db import DatabaseRuntime, SqlAlchemyUnitOfWork, UnitOfWorkState

_ACTOR_PERSON_IDS = (9_701_001, 9_701_002, 9_701_003, 9_701_004)
_COLLISION_PARENT_ID = "p7-actor-fixture-parent"
_COLLISION_SEASON_ID = "p7-actor-fixture-season"
_CONFIG_MEDIA_ID = "p7-actor-fixture-config"
_DELETE_MEDIA_ID = "p7-actor-fixture-delete"
_ROLLBACK_MEDIA_ID = "p7-actor-fixture-rollback"
_MEDIA_IDS = (
    _COLLISION_PARENT_ID,
    _COLLISION_SEASON_ID,
    _CONFIG_MEDIA_ID,
    _DELETE_MEDIA_ID,
    _ROLLBACK_MEDIA_ID,
)


class UnexpectedCancellationStager:
    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: ActorSubscriptionCancellation,
    ) -> None:
        raise AssertionError(f"unexpected cancellation intent in {uow.state}: {intent}")


class InspectingCancellationStager:
    def __init__(
        self,
        *,
        subscription_id: int,
        tmdb_id: str,
        item_type: str,
        fail: bool = False,
    ) -> None:
        self._subscription_id = subscription_id
        self._tmdb_id = tmdb_id
        self._item_type = item_type
        self._fail = fail
        self.observations: list[tuple[ActorSubscriptionCancellation, bool, dict[str, Any]]] = []

    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: ActorSubscriptionCancellation,
    ) -> None:
        assert uow.state is UnitOfWorkState.ACTIVE
        actor_exists = bool(
            uow.session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM actor_subscriptions WHERE id = :subscription_id)"),
                {"subscription_id": self._subscription_id},
            )
        )
        media = (
            uow.session.execute(
                text(
                    """
                    SELECT subscription_status, subscription_sources_json
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {"tmdb_id": self._tmdb_id, "item_type": self._item_type},
            )
            .mappings()
            .one()
        )
        self.observations.append((intent, actor_exists, dict(media)))
        if self._fail:
            raise RuntimeError("fixture cancellation staging failure")


@pytest.fixture()
def actor_projection_runtime(p7_runtime: DatabaseRuntime) -> Iterator[DatabaseRuntime]:
    _cleanup(p7_runtime)
    try:
        yield p7_runtime
    finally:
        _cleanup(p7_runtime)


def test_tracked_season_joins_only_the_series_side_of_composite_media_identity(
    actor_projection_runtime: DatabaseRuntime,
) -> None:
    subscription_id = _insert_actor(actor_projection_runtime, _ACTOR_PERSON_IDS[0])
    source = [{"type": "actor_subscription", "id": subscription_id}]
    _insert_media(
        actor_projection_runtime,
        tmdb_id=_COLLISION_PARENT_ID,
        item_type="Series",
        title="Fixture Series",
    )
    _insert_media(
        actor_projection_runtime,
        tmdb_id=_COLLISION_PARENT_ID,
        item_type="Movie",
        title="Colliding Fixture Movie",
    )
    _insert_media(
        actor_projection_runtime,
        tmdb_id=_COLLISION_SEASON_ID,
        item_type="Season",
        title="Fixture Season",
        parent_series_tmdb_id=_COLLISION_PARENT_ID,
        season_number=2,
        status="WANTED",
        sources=source,
    )
    repository = _repository(actor_projection_runtime, UnexpectedCancellationStager())

    subscription = repository.get_subscription(subscription_id)

    assert subscription is not None
    assert len(subscription["tracked_media"]) == 1
    tracked = subscription["tracked_media"][0]
    assert tracked["tmdb_media_id"] == _COLLISION_SEASON_ID
    assert tracked["media_type"] == "Season"
    assert tracked["parent_title"] == "Fixture Series"
    assert tracked["title"] == "Fixture Series 第 2 季"


def test_config_change_removes_actor_source_by_partial_identity(
    actor_projection_runtime: DatabaseRuntime,
) -> None:
    subscription_id = _insert_actor(
        actor_projection_runtime,
        _ACTOR_PERSON_IDS[1],
        start_year=1990,
        last_scanned=[501],
    )
    source = [
        {
            "type": "actor_subscription",
            "id": subscription_id,
            "name": "Fixture Actor",
            "person_id": 101,
        }
    ]
    _insert_media(
        actor_projection_runtime,
        tmdb_id=_CONFIG_MEDIA_ID,
        item_type="Movie",
        title="Ignored Fixture Movie",
        in_library=True,
        status="IGNORED",
        sources=source,
    )
    repository = _repository(actor_projection_runtime, UnexpectedCancellationStager())

    assert repository.update_subscription(subscription_id, {"config": {"start_year": 2000}}) is True

    with actor_projection_runtime.engine.connect() as connection:
        actor = (
            connection.execute(
                text(
                    """
                    SELECT config_start_year, last_scanned_tmdb_ids_json
                    FROM actor_subscriptions
                    WHERE id = :subscription_id
                    """
                ),
                {"subscription_id": subscription_id},
            )
            .mappings()
            .one()
        )
    assert dict(actor) == {"config_start_year": 2000, "last_scanned_tmdb_ids_json": None}
    assert _media_state(actor_projection_runtime, _CONFIG_MEDIA_ID, "Movie") == {
        "subscription_status": "NONE",
        "subscription_sources_json": [],
    }


def test_delete_stages_symbolic_cancellation_before_committing_actor_and_media_mutations(
    actor_projection_runtime: DatabaseRuntime,
) -> None:
    subscription_id = _insert_actor(actor_projection_runtime, _ACTOR_PERSON_IDS[2])
    source = [
        {
            "type": "actor_subscription",
            "id": subscription_id,
            "name": "Fixture Actor",
            "person_id": 102,
        }
    ]
    _insert_media(
        actor_projection_runtime,
        tmdb_id=_DELETE_MEDIA_ID,
        item_type="Season",
        title="Delete Fixture Season",
        season_number=4,
        in_library=True,
        status="WANTED",
        sources=source,
    )
    stager = InspectingCancellationStager(
        subscription_id=subscription_id,
        tmdb_id=_DELETE_MEDIA_ID,
        item_type="Season",
    )
    repository = _repository(actor_projection_runtime, stager)

    repository.delete_subscription(subscription_id)

    intent = ActorSubscriptionCancellation(subscription_id, _DELETE_MEDIA_ID, "Season", 4)
    assert stager.observations == [
        (
            intent,
            True,
            {"subscription_status": "NONE", "subscription_sources_json": []},
        )
    ]
    assert _actor_exists(actor_projection_runtime, subscription_id) is False
    assert _media_state(actor_projection_runtime, _DELETE_MEDIA_ID, "Season") == {
        "subscription_status": "NONE",
        "subscription_sources_json": [],
    }


def test_delete_rolls_back_actor_and_media_when_cancellation_staging_fails(
    actor_projection_runtime: DatabaseRuntime,
) -> None:
    subscription_id = _insert_actor(actor_projection_runtime, _ACTOR_PERSON_IDS[3])
    source = [
        {
            "type": "actor_subscription",
            "id": subscription_id,
            "name": "Fixture Actor",
            "person_id": 103,
        }
    ]
    _insert_media(
        actor_projection_runtime,
        tmdb_id=_ROLLBACK_MEDIA_ID,
        item_type="Movie",
        title="Rollback Fixture Movie",
        in_library=True,
        status="WANTED",
        sources=source,
    )
    stager = InspectingCancellationStager(
        subscription_id=subscription_id,
        tmdb_id=_ROLLBACK_MEDIA_ID,
        item_type="Movie",
        fail=True,
    )
    repository = _repository(actor_projection_runtime, stager)

    with pytest.raises(RuntimeError, match="fixture cancellation staging failure"):
        repository.delete_subscription(subscription_id)

    intent = ActorSubscriptionCancellation(subscription_id, _ROLLBACK_MEDIA_ID, "Movie", None)
    assert stager.observations == [
        (
            intent,
            True,
            {"subscription_status": "NONE", "subscription_sources_json": []},
        )
    ]
    assert _actor_exists(actor_projection_runtime, subscription_id) is True
    assert _media_state(actor_projection_runtime, _ROLLBACK_MEDIA_ID, "Movie") == {
        "subscription_status": "WANTED",
        "subscription_sources_json": source,
    }


def _repository(
    runtime: DatabaseRuntime,
    cancellation_intent_stager: ActorCancellationIntentStagePort,
) -> SqlAlchemyActorSubscriptionRepository:
    return SqlAlchemyActorSubscriptionRepository(
        runtime.unit_of_work,
        SqlAlchemyActorSubscriptionProjection(),
        cancellation_intent_stager,
    )


def _insert_actor(
    runtime: DatabaseRuntime,
    tmdb_person_id: int,
    *,
    start_year: int = 1900,
    last_scanned: list[int] | None = None,
) -> int:
    with runtime.engine.begin() as connection:
        subscription_id = connection.scalar(
            text(
                """
                INSERT INTO actor_subscriptions (
                    tmdb_person_id, actor_name, config_start_year, last_scanned_tmdb_ids_json
                )
                VALUES (
                    :tmdb_person_id, 'Fixture Actor', :start_year, CAST(:last_scanned AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "tmdb_person_id": tmdb_person_id,
                "start_year": start_year,
                "last_scanned": json.dumps(last_scanned or []),
            },
        )
    assert subscription_id is not None
    return int(subscription_id)


def _insert_media(
    runtime: DatabaseRuntime,
    *,
    tmdb_id: str,
    item_type: str,
    title: str,
    parent_series_tmdb_id: str | None = None,
    season_number: int | None = None,
    in_library: bool = False,
    status: str = "NONE",
    sources: list[dict[str, Any]] | None = None,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    in_library, subscription_status, subscription_sources_json
                )
                VALUES (
                    :tmdb_id, :item_type, :title, :parent_series_tmdb_id, :season_number,
                    :in_library, :status, CAST(:sources AS jsonb)
                )
                """
            ),
            {
                "tmdb_id": tmdb_id,
                "item_type": item_type,
                "title": title,
                "parent_series_tmdb_id": parent_series_tmdb_id,
                "season_number": season_number,
                "in_library": in_library,
                "status": status,
                "sources": json.dumps(sources or [], ensure_ascii=False),
            },
        )


def _actor_exists(runtime: DatabaseRuntime, subscription_id: int) -> bool:
    with runtime.engine.connect() as connection:
        return bool(
            connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM actor_subscriptions WHERE id = :subscription_id)"),
                {"subscription_id": subscription_id},
            )
        )


def _media_state(runtime: DatabaseRuntime, tmdb_id: str, item_type: str) -> dict[str, Any]:
    with runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT subscription_status, subscription_sources_json
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {"tmdb_id": tmdb_id, "item_type": item_type},
            )
            .mappings()
            .one()
        )
    return dict(row)


def _cleanup(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = ANY(CAST(:media_ids AS text[]))"),
            {"media_ids": list(_MEDIA_IDS)},
        )
        connection.execute(
            text(
                """
                DELETE FROM actor_subscriptions
                WHERE tmdb_person_id = ANY(CAST(:person_ids AS integer[]))
                """
            ),
            {"person_ids": list(_ACTOR_PERSON_IDS)},
        )
