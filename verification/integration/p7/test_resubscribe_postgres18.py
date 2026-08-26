from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.resubscribe_projection import SqlAlchemyResubscribeMediaProjection
from etk.domains.subscription.moviepilot import (
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.subscription.resubscribe import (
    PostgresResubscribeOperationStager,
    ResubscribeItemKey,
    ResubscribePolicySnapshot,
    SqlAlchemyResubscribeRepository,
    UnsupportedResubscribeBatchError,
)
from etk.domains.subscription.runtime_repository import SubscriptionQuotaExhaustedError
from etk.platform.db import DatabaseRuntime


class _Policy:
    def __init__(self, cap: int = 2) -> None:
        self.cap = cap

    def snapshot(self) -> ResubscribePolicySnapshot:
        return ResubscribePolicySnapshot(
            quota_date=date(2026, 8, 26),
            daily_cap=self.cap,
            delay_seconds=2,
            language_keywords={},
            release_group_mapping={},
        )


class _Gateway:
    def __init__(self) -> None:
        self.writes = 0
        self.queries = 0

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        del tmdb_id, season
        self.queries += 1
        return MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None)

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        del payload
        self.writes += 1
        return MoviePilotWriteResult("create-subscription", 201)

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        del payload
        self.writes += 1
        return MoviePilotWriteResult("update-subscription", 200)

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult:
        del tmdb_id, status, season, total_episodes
        self.writes += 1
        return MoviePilotWriteResult("update-status", 200)

    def delete_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotWriteResult:
        del tmdb_id, season
        self.writes += 1
        return MoviePilotWriteResult("delete-subscription", 200)


def _repository(runtime: DatabaseRuntime, policy: _Policy | None = None) -> SqlAlchemyResubscribeRepository:
    return SqlAlchemyResubscribeRepository(
        runtime.unit_of_work,
        SqlAlchemyResubscribeMediaProjection(),
        PostgresResubscribeOperationStager(),
        policy or _Policy(),
    )


def _insert_item(
    runtime: DatabaseRuntime,
    *,
    rule_id: int,
    tmdb_id: str,
    source: str = "moviepilot",
    rule_type: str = "resubscribe",
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_rules (
                    id, name, enabled, sort_order, rule_type, resubscribe_source,
                    custom_resubscribe_enabled, resubscribe_entire_season
                ) VALUES (
                    :rule_id, :name, TRUE, :rule_id, :rule_type, :source, FALSE, FALSE
                )
                """
            ),
            {
                "rule_id": rule_id,
                "name": f"rule-{rule_id}",
                "rule_type": rule_type,
                "source": source,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, subscription_status, asset_details_json
                ) VALUES (
                    :tmdb_id, 'Movie', :title, 'SUBSCRIBED',
                    '[{"release_group_raw":["fixture"]}]'::jsonb
                )
                """
            ),
            {"tmdb_id": tmdb_id, "title": f"Movie {tmdb_id}"},
        )
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (
                    tmdb_id, item_type, season_number, status, reason, matched_rule_id
                ) VALUES (
                    :tmdb_id, 'Movie', -1, 'needed', 'fixture', :rule_id
                )
                """
            ),
            {"tmdb_id": tmdb_id, "rule_id": rule_id},
        )


def test_resubscribe_stages_index_operation_quota_and_outbox_in_one_transaction(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_item(p7_runtime, rule_id=801, tmdb_id="801")
    repository = _repository(p7_runtime)

    reservation = repository.stage_replacements([ResubscribeItemKey("801", "Movie")])

    assert len(reservation.operation_ids) == 1
    operation_id = reservation.operation_ids[0]
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                SELECT
                    (SELECT status FROM resubscribe_index WHERE tmdb_id = '801') AS index_status,
                    (SELECT status FROM subscription_operations WHERE id = :id) AS operation_status,
                    (SELECT status FROM subscription_quota_reservations WHERE operation_id = :id) AS quota_status,
                    (SELECT status FROM outbox_events
                     WHERE aggregate_type = 'subscription-operation'
                       AND aggregate_id = CAST(:id AS text)) AS outbox_status
                """
                ),
                {"id": operation_id},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {
        "index_status": "subscribed",
        "operation_status": "ready",
        "quota_status": "reserved",
        "outbox_status": "pending",
    }


def test_quota_exhaustion_rolls_back_second_index_operation_and_outbox(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_item(p7_runtime, rule_id=802, tmdb_id="802")
    _insert_item(p7_runtime, rule_id=803, tmdb_id="803")
    repository = _repository(p7_runtime, _Policy(cap=1))
    repository.stage_replacements([ResubscribeItemKey("802", "Movie")])

    with pytest.raises(SubscriptionQuotaExhaustedError):
        repository.stage_replacements([ResubscribeItemKey("803", "Movie")])

    with p7_runtime.engine.connect() as connection:
        status = connection.scalar(text("SELECT status FROM resubscribe_index WHERE tmdb_id = '803'"))
        operations = int(
            connection.scalar(text("SELECT COUNT(*) FROM subscription_operations WHERE scope_key LIKE '%803%'")) or 0
        )
    assert status == "needed"
    assert operations == 0


def test_mixed_adapter_batch_is_rejected_without_partial_state_or_effects(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_item(p7_runtime, rule_id=804, tmdb_id="804")
    _insert_item(p7_runtime, rule_id=805, tmdb_id="805", source="hdhive")
    repository = _repository(p7_runtime)

    with pytest.raises(UnsupportedResubscribeBatchError) as raised:
        repository.stage_replacements([ResubscribeItemKey("804", "Movie"), ResubscribeItemKey("805", "Movie")])

    assert raised.value.item_ids == ("805-Movie",)
    with p7_runtime.engine.connect() as connection:
        statuses = tuple(
            connection.execute(
                text("SELECT status FROM resubscribe_index WHERE tmdb_id IN ('804','805') ORDER BY tmdb_id")
            ).scalars()
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0
    assert statuses == ("needed", "needed")


def test_delete_confirmation_atomically_commits_quota_and_stages_one_create_continuation(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_item(p7_runtime, rule_id=806, tmdb_id="806")
    reservation = _repository(p7_runtime, _Policy(cap=1)).stage_replacements([ResubscribeItemKey("806", "Movie")])
    delete_id = reservation.operation_ids[0]
    operations = PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work)
    gateway = _Gateway()
    executor = SubscriptionOperationExecutor(operations, gateway, readback_delay_seconds=0)
    now = datetime.now(UTC) + timedelta(seconds=1)

    result = executor.execute(
        delete_id,
        worker_id="fixture-resubscribe",
        lease_seconds=30,
        now=now,
    )
    replay = executor.execute(
        delete_id,
        worker_id="fixture-resubscribe",
        lease_seconds=30,
        now=now + timedelta(seconds=1),
    )

    assert result is not None and result.operation.status.value == "confirmed"
    assert replay is not None and replay.claimed is False
    assert gateway.writes == 1 and gateway.queries == 1
    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                SELECT kind, status, available_at, id
                FROM subscription_operations
                WHERE scope_key LIKE 'resubscribe:movie:806:%'
                ORDER BY created_at, kind
                """
                )
            )
            .mappings()
            .all()
        )
        quota_status = connection.scalar(
            text("SELECT status FROM subscription_quota_reservations WHERE operation_id = :id"),
            {"id": delete_id},
        )
        outbox = (
            connection.execute(
                text(
                    """
                SELECT aggregate_id, status
                FROM outbox_events
                WHERE aggregate_type = 'subscription-operation'
                  AND aggregate_id = ANY(CAST(:ids AS text[]))
                ORDER BY created_at
                """
                ),
                {"ids": [str(row["id"]) for row in rows]},
            )
            .mappings()
            .all()
        )

    assert [(row["kind"], row["status"]) for row in rows] == [
        ("resubscribe", "confirmed"),
        ("create", "ready"),
    ]
    assert rows[1]["available_at"] >= now + timedelta(seconds=2)
    assert quota_status == "committed"
    assert [row["status"] for row in outbox] == ["processed", "pending"]
