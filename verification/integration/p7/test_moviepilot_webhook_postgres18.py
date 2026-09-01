from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.effect_execution import MediaEffectDeliveryRetryable
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.subscription.moviepilot import MoviePilotActiveDownload
from etk.domains.subscription.moviepilot_webhook import (
    MOVIEPILOT_ASSISTANT_EVENT_TYPE,
    MoviePilotAssistantReadbackUnavailable,
    MoviePilotFullWashingOperations,
    MoviePilotWebhookEffectDelivery,
    MoviePilotWebhookService,
    SqlAlchemyMoviePilotAssistantEventRepository,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.operations import SubscriptionOperationStatus
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime


class _Settings:
    def load(self, key: str) -> Any:
        values = {
            "mp_config": {
                "moviepilot_url": "https://moviepilot.fixture",
                "subscribe_assistant": {"enabled": True},
            },
            "watchlist_config": {},
            "subscription_strategy_config": {"subscription_sources": ["mp"]},
        }
        return values.get(key)


class _Credentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(
            account_id="115001",
            access_token="fixture-openapi-token",
        )


def _reset_assistant_state(runtime: DatabaseRuntime, *, subscribe_ids: tuple[int, ...] = ()) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM subscribe_assistant_state
                WHERE state_key IN (
                    'subscribes', 'torrents', 'moviepilot_webhook_receipts',
                    'snapshot_restore_suppressions'
                )
                """
            )
        )
        if subscribe_ids:
            connection.execute(
                text(
                    """
                    DELETE FROM subscribe_assistant_snapshots
                    WHERE subscribe_id = ANY(CAST(:subscribe_ids AS integer[]))
                    """
                ),
                {"subscribe_ids": list(subscribe_ids)},
            )


def test_pg18_moviepilot_ingress_reserves_one_effect_and_one_outbox(
    p7_runtime: DatabaseRuntime,
) -> None:
    service = MoviePilotWebhookService(
        PostgresMediaEffectLedger(p7_runtime.unit_of_work),
        _Settings(),
        Settings,
        _Credentials(),
    )
    payload = {
        "type": "subscribe.added",
        "event_id": "pg18-subscribe-added",
        "data": {"subscribe_id": 7, "subscribe_info": {"id": 7, "tmdbid": 123}},
    }

    first = service.handle(payload)
    second = service.handle(payload)

    assert first.payload == {"status": "subscribe_assistant_processed"}
    assert second.payload == first.payload
    with p7_runtime.engine.connect() as connection:
        effects = connection.scalar(
            text("SELECT COUNT(*) FROM media_effects WHERE effect_type = 'moviepilot.webhook.assistant'")
        )
        outbox = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM outbox_events AS event
                JOIN media_effects AS effect ON effect.id::text = event.aggregate_id
                WHERE effect.effect_type = 'moviepilot.webhook.assistant'
                """
            )
        )
    assert effects == 1
    assert outbox == 1


def test_pg18_assistant_effect_stale_claim_is_requeued_for_local_replay(
    p7_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p7_runtime.unit_of_work)
    effect = ledger.reserve(
        effect_key="p11:moviepilot-webhook:stale-claim-fixture",
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        aggregate_type="moviepilot_webhook",
        aggregate_id="subscription:stale-claim",
        payload={
            "event_key": "stale-claim-fixture",
            "event_type": "subscribe.added",
            "payload_digest": "e" * 64,
            "payload": {"data": {"subscribe_id": 700}},
        },
    ).effect
    claimed = ledger.claim_by_id(
        effect.id,
        worker_id="fixture-moviepilot-worker",
        lease_seconds=60,
    )
    assert claimed is not None
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET claimed_at = NOW() - INTERVAL '2 seconds',
                    lease_expires_at = NOW() - INTERVAL '1 second'
                WHERE id = :effect_id
                """
            ),
            {"effect_id": effect.id},
        )

    recovered = ledger.requeue_replay_safe_stale_claims(
        effect_types=(MOVIEPILOT_ASSISTANT_EVENT_TYPE,),
        retry_delay_seconds=0,
    )

    assert recovered == 1
    current = ledger.read_by_id(effect.id)
    assert current is not None
    assert current.status.value == "retryable"


def test_pg18_assistant_event_replay_is_atomic_with_snapshot_and_torrent_cleanup(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(7,))
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)
    added = {
        "data": {
            "subscribe_id": 7,
            "subscribe_info": {
                "id": 7,
                "tmdbid": "123",
                "type": "电视剧",
                "season": 1,
                "total_episode": 12,
            },
        }
    }
    download = {
        "data": {
            "hash": "ABCDEF",
            "source": {"id": 7, "tmdbid": "123", "season": 1, "name": "Fixture"},
            "episodes": [1, 2],
            "context": {"torrent_info": {"title": "Fixture.S01"}},
        }
    }

    repository.apply(
        event_key="added",
        event_type="subscribe.added",
        payload_digest="a" * 64,
        payload=added,
    )
    repository.apply(
        event_key="download",
        event_type="download.added",
        payload_digest="b" * 64,
        payload=download,
    )

    def complete() -> dict[str, Any]:
        return dict(
            repository.apply(
                event_key="complete",
                event_type="subscribe.complete",
                payload_digest="c" * 64,
                payload=added,
            )
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _index: complete(), range(2)))

    assert results[0] == results[1]
    assert results[0]["snapshot_created"] is True
    assert results[0]["torrents_removed"] == 1
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
        snapshots = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM subscribe_assistant_snapshots
                WHERE tmdb_id = '123' AND subscribe_id = 7
                """
            )
        )
    assert subscribes["7"]["subscribe_info"]["tmdbid"] == "123"
    assert torrents == {}
    assert snapshots == 1


def test_pg18_download_source_prefix_preserves_monitor_baseline_and_deleted_tombstone(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(17,))
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)
    source = json.dumps(
        {"id": 17, "tmdbid": "1700", "season": 1, "name": "Fixture"},
        ensure_ascii=False,
    )

    repository.apply(
        event_key="download-prefixed",
        event_type="download.added",
        payload_digest="d" * 64,
        payload={
            "data": {
                "hash": "PREFIXEDHASH",
                "source": f"Subscribe|{source}",
                "progress": 12.5,
                "episodes": [1, 2],
                "context": {"torrent_info": {"title": "Fixture.S01"}},
            }
        },
    )

    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    torrent = torrents["prefixedhash"]
    assert torrent["hash"] == "prefixedhash"
    assert torrent["baseline_progress"] == 12.5
    assert isinstance(torrent["baseline_at"], (int, float))
    assert torrent["retry_count"] == 0
    assert isinstance(torrent["time"], (int, float))
    assert subscribes["17"]["active_sources"]["download_pending"]
    assert isinstance(subscribes["17"]["updated_at"], (int, float))

    deleted = repository.apply(
        event_key="delete-prefixed",
        event_type="subscribe.deleted",
        payload_digest="e" * 64,
        payload={"data": {"subscribe_id": 17}},
    )

    assert deleted["snapshot_repair_pending"] is True
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    assert subscribes["17"]["deleted"] is True
    assert subscribes["17"]["active_sources"] == {}
    assert subscribes["17"]["snapshot_repair_pending"] is True
    assert subscribes["17"]["subscribe_info"] == {
        "id": 17,
        "tmdbid": "1700",
        "season": 1,
        "name": "Fixture",
    }
    assert torrents == {}


def test_pg18_added_syncs_projection_clears_suppression_and_consumes_expected_update(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(18,))
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = 'season-mp-18' AND item_type = 'Season'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    in_library, subscription_status, subscription_sources_json, last_updated_at
                ) VALUES (
                    'season-mp-18', 'Season', 'Fixture Season', '1800', 1,
                    FALSE, 'WANTED', '[]'::jsonb, NOW()
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (
                    'snapshot_restore_suppressions',
                    CAST(:state_json AS jsonb),
                    NOW()
                )
                """
            ),
            {
                "state_json": json.dumps(
                    {
                        "tmdb:1800:S1": {"expires_at": 4102444800},
                        "sub:18": {"expires_at": 4102444800},
                    }
                )
            },
        )
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)
    info = {
        "id": 18,
        "tmdbid": "1800",
        "type": "电视剧",
        "season": 1,
        "state": "R",
        "name": "Fixture",
    }

    added = repository.apply(
        event_key="added-projection",
        event_type="subscribe.added",
        payload_digest="f" * 64,
        payload={"data": {"subscribe_id": 18, "subscribe_info": info}},
    )

    assert added["projection_changed"] == 1
    with p7_runtime.engine.begin() as connection:
        projection = (
            connection.execute(
                text(
                    """
                    SELECT subscription_status, subscription_sources_json
                    FROM media_metadata
                    WHERE tmdb_id = 'season-mp-18' AND item_type = 'Season'
                    """
                )
            )
            .mappings()
            .one()
        )
        suppressions = connection.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = 'snapshot_restore_suppressions'
                """
            )
        )
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        subscribes["18"]["expected_mp_update"] = {
            "fields": ["state"],
            "state": "R",
            "total_episode": None,
            "updated_at": subscribes["18"]["updated_at"],
        }
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_state
                SET state_json = CAST(:state_json AS jsonb), updated_at = NOW()
                WHERE state_key = 'subscribes'
                """
            ),
            {"state_json": json.dumps(subscribes)},
        )
    assert projection["subscription_status"] == "SUBSCRIBED"
    assert projection["subscription_sources_json"] == [{"type": "moviepilot", "subscribe_id": 18}]
    assert suppressions == {}

    modified = repository.apply(
        event_key="modified-expected",
        event_type="subscribe.modified",
        payload_digest="1" * 64,
        payload={
            "data": {
                "subscribe_id": 18,
                "subscribe_info": info,
                "old_subscribe_info": {**info, "state": "N"},
                "fields": ["state"],
            }
        },
    )

    assert modified["reason"] == "subscribe.modified.expected"
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
    assert "expected_mp_update" not in subscribes["18"]
    assert "last_manual_change" not in subscribes["18"]

    repository.apply(
        event_key="modified-manual",
        event_type="subscribe.modified",
        payload_digest="4" * 64,
        payload={
            "data": {
                "subscribe_id": 18,
                "subscribe_info": {**info, "name": "Fixture renamed"},
                "old_subscribe_info": info,
                "fields": ["name"],
            }
        },
    )
    with p7_runtime.engine.connect() as connection:
        sources = connection.scalar(
            text(
                """
                SELECT subscription_sources_json
                FROM media_metadata
                WHERE tmdb_id = 'season-mp-18' AND item_type = 'Season'
                """
            )
        )
    assert sources == [{"type": "moviepilot", "subscribe_id": 18}]


def test_pg18_empty_expected_fields_do_not_consume_manual_update_and_normalize_timestamps(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(20,))
    iso_value = datetime.now(UTC).isoformat()
    state = {
        "20": {
            "subscribe_id": 20,
            "subscribe_info": {
                "id": 20,
                "tmdbid": "2000",
                "type": "电影",
                "state": "N",
            },
            "created_at": iso_value,
            "download_started_at": iso_value,
            "full_washing_started_at": iso_value,
            "washing_timeout_started_at": iso_value,
            "last_manual_change": {"fields": ["name"], "updated_at": iso_value},
            "expected_mp_update": {"fields": [], "state": "R", "updated_at": iso_value},
        }
    }
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES ('subscribes', CAST(:state_json AS jsonb), NOW())
                """
            ),
            {"state_json": json.dumps(state)},
        )
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)

    result = repository.apply(
        event_key="modified-empty-expected-fields",
        event_type="subscribe.modified",
        payload_digest="5" * 64,
        payload={
            "data": {
                "subscribe_id": 20,
                "subscribe_info": {
                    "id": 20,
                    "tmdbid": "2000",
                    "type": "电影",
                    "state": "S",
                },
                "old_subscribe_info": {
                    "id": 20,
                    "tmdbid": "2000",
                    "type": "电影",
                    "state": "R",
                },
                "fields": ["state"],
            }
        },
    )

    assert result["reason"] == "subscribe.modified"
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
    current = subscribes["20"]
    assert current["expected_mp_update"]["fields"] == []
    assert current["last_manual_change"]["fields"] == ["state"]
    assert current["active_sources"]["manual_mp"] == "MP 手动修改：state"
    for field in (
        "created_at",
        "download_started_at",
        "full_washing_started_at",
        "washing_timeout_started_at",
    ):
        assert isinstance(current[field], (int, float))
    assert isinstance(current["last_manual_change"]["updated_at"], (int, float))
    assert isinstance(current["expected_mp_update"]["updated_at"], (int, float))


def test_pg18_full_washing_completion_stays_pending_until_native_verification(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(19,))
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = 'season-mp-19' AND item_type = 'Season'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    in_library, subscription_status, subscription_sources_json,
                    active_washing, last_updated_at
                ) VALUES (
                    'season-mp-19', 'Season', 'Fixture Washing', '1900', 1,
                    FALSE, 'SUBSCRIBED', '[]'::jsonb, FALSE, NOW()
                )
                """
            )
        )
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)
    info = {
        "id": 19,
        "tmdbid": "1900",
        "type": "电视剧",
        "season": 1,
        "state": "R",
        "best_version_full": 1,
        "total_episode": 12,
    }
    repository.apply(
        event_key="washing-download",
        event_type="download.added",
        payload_digest="2" * 64,
        payload={
            "data": {
                "hash": "WASHINGHASH",
                "source": info,
                "episodes": list(range(1, 13)),
            }
        },
    )
    with p7_runtime.engine.begin() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        subscribes["19"]["active_sources"] = {}
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_state
                SET state_json = CAST(:state_json AS jsonb), updated_at = NOW()
                WHERE state_key = 'subscribes'
                """
            ),
            {"state_json": json.dumps(subscribes)},
        )

    complete = repository.apply(
        event_key="washing-complete",
        event_type="subscribe.complete",
        payload_digest="3" * 64,
        payload={"data": {"subscribe_id": 19, "subscribe_info": info}},
    )

    assert complete["completion_deferred"] is True
    assert complete["snapshot_created"] is False
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
        snapshots = connection.scalar(
            text("SELECT COUNT(*) FROM subscribe_assistant_snapshots WHERE subscribe_id = 19")
        )
        active_washing = connection.scalar(
            text(
                """
                SELECT active_washing
                FROM media_metadata
                WHERE tmdb_id = 'season-mp-19' AND item_type = 'Season'
                """
            )
        )
    assert subscribes["19"]["full_washing"] is True
    assert subscribes["19"]["active_sources"] == {}
    assert subscribes["19"]["last_event"] == "full_washing_downloading"
    assert "washinghash" in torrents
    assert snapshots == 0
    assert active_washing is True


def test_pg18_full_washing_guard_disabled_or_missing_season_finishes_normally(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(21, 22))
    disabled = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        full_washing_guard=lambda: False,
    )
    enabled = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        full_washing_guard=lambda: True,
    )

    disabled_result = disabled.apply(
        event_key="washing-disabled",
        event_type="subscribe.complete",
        payload_digest="6" * 64,
        payload={
            "data": {
                "subscribe_id": 21,
                "subscribe_info": {
                    "id": 21,
                    "tmdbid": "2100",
                    "type": "电视剧",
                    "season": 1,
                    "best_version_full": 1,
                },
            }
        },
    )
    movie_result = enabled.apply(
        event_key="washing-movie",
        event_type="subscribe.complete",
        payload_digest="7" * 64,
        payload={
            "data": {
                "subscribe_id": 22,
                "subscribe_info": {
                    "id": 22,
                    "tmdbid": "2200",
                    "type": "电影",
                    "best_version_full": 1,
                },
            }
        },
    )

    assert disabled_result["snapshot_created"] is True
    assert disabled_result.get("completion_deferred") is None
    assert movie_result["snapshot_created"] is True
    assert movie_result.get("completion_deferred") is None
    with p7_runtime.engine.connect() as connection:
        snapshots = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM subscribe_assistant_snapshots
                WHERE subscribe_id IN (21, 22)
                """
            )
        )
    assert snapshots == 2


def test_pg18_full_washing_ignores_stale_pending_source_without_a_torrent(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(23,))
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES ('subscribes', CAST(:state_json AS jsonb), NOW())
                """
            ),
            {
                "state_json": json.dumps(
                    {
                        "23": {
                            "subscribe_id": 23,
                            "active_sources": {
                                "download_pending": "stale display projection",
                            },
                        }
                    }
                )
            },
        )
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)
    info = {
        "id": 23,
        "tmdbid": "2300",
        "type": "电视剧",
        "season": 1,
        "best_version_full": 1,
    }

    result = repository.apply(
        event_key="washing-stale-source",
        event_type="subscribe.complete",
        payload_digest="8" * 64,
        payload={"data": {"subscribe_id": 23, "subscribe_info": info}},
    )

    assert result["completion_deferred"] is True
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    assert subscribes["23"]["last_event"] == "consistency_failed_complete"
    assert torrents == {}


def test_pg18_assistant_receipt_failure_rolls_back_snapshot_state_and_torrent_cleanup(
    p7_runtime: DatabaseRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(24,))
    repository = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work)
    info = {
        "id": 24,
        "tmdbid": "2400",
        "type": "电视剧",
        "season": 1,
        "total_episode": 8,
    }
    repository.apply(
        event_key="rollback-download",
        event_type="download.added",
        payload_digest="9" * 64,
        payload={"data": {"hash": "ROLLBACKHASH", "source": info}},
    )
    original_write = repository._write_state

    def fail_receipt_write(uow: Any, key: str, value: Any) -> None:
        original_write(uow, key, value)
        if key == "moviepilot_webhook_receipts":
            raise RuntimeError("fixture receipt failure")

    monkeypatch.setattr(repository, "_write_state", fail_receipt_write)
    with pytest.raises(RuntimeError, match="fixture receipt failure"):
        repository.apply(
            event_key="rollback-complete",
            event_type="subscribe.complete",
            payload_digest="a" * 64,
            payload={"data": {"subscribe_id": 24, "subscribe_info": info}},
        )

    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
        receipts = connection.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = 'moviepilot_webhook_receipts'
                """
            )
        )
        snapshots = connection.scalar(
            text("SELECT COUNT(*) FROM subscribe_assistant_snapshots WHERE subscribe_id = 24")
        )
    assert subscribes["24"]["active_sources"]["download_pending"]
    assert "rollbackhash" in torrents
    assert "rollback-complete" not in receipts
    assert snapshots == 0

    monkeypatch.undo()
    retried = SqlAlchemyMoviePilotAssistantEventRepository(p7_runtime.unit_of_work).apply(
        event_key="rollback-complete",
        event_type="subscribe.complete",
        payload_digest="a" * 64,
        payload={"data": {"subscribe_id": 24, "subscribe_info": info}},
    )
    assert retried["snapshot_created"] is True
    assert retried["torrents_removed"] == 1
    with p7_runtime.engine.connect() as connection:
        snapshots = connection.scalar(
            text("SELECT COUNT(*) FROM subscribe_assistant_snapshots WHERE subscribe_id = 24")
        )
    assert snapshots == 1


def test_pg18_full_washing_retries_without_a_receipt_then_finishes_after_consistency(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(25,))
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM media_metadata
                WHERE parent_series_tmdb_id = '2500' OR tmdb_id = '2500'
                """
            )
        )
        connection.execute(
            text("DELETE FROM subscription_operations WHERE scope_key LIKE 'moviepilot-full-washing:2500:%'")
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    in_library, subscription_status, subscription_sources_json,
                    active_washing, last_updated_at
                ) VALUES (
                    'season-mp-25', 'Season', 'Fixture Washing', '2500', 1,
                    FALSE, 'SUBSCRIBED', '[]'::jsonb, FALSE, NOW()
                )
                """
            )
        )
    active_downloads = [
        MoviePilotActiveDownload("untrackedhash", 25, "2500", 1),
        MoviePilotActiveDownload("unknownseasonhash", None, "2500", None),
    ]
    repository = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        active_downloads=lambda: tuple(active_downloads),
    )
    info = {
        "id": 25,
        "tmdbid": "2500",
        "type": "电视剧",
        "season": 1,
        "state": "S",
        "best_version_full": 1,
        "total_episode": 2,
    }
    payload = {"data": {"subscribe_id": 25, "subscribe_info": info}}

    pending = repository.apply(
        event_key="washing-monitor",
        event_type="subscribe.complete",
        payload_digest="b" * 64,
        payload=payload,
    )

    assert pending["completion_deferred"] is True
    assert pending["reconciliation"] == "pending_download"
    with p7_runtime.engine.connect() as connection:
        receipts = connection.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = 'moviepilot_webhook_receipts'
                """
            )
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    assert "washing-monitor" not in receipts
    assert torrents["untrackedhash"]["source"] == "moviepilot_active_readback"
    assert "unknownseasonhash" not in torrents

    active_downloads.clear()
    deferred = repository.apply(
        event_key="washing-monitor",
        event_type="subscribe.complete",
        payload_digest="b" * 64,
        payload=payload,
    )
    assert deferred["completion_deferred"] is True
    assert deferred["reconciliation"] == "consistency_failed"
    with p7_runtime.engine.connect() as connection:
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    assert torrents == {}

    operations = MoviePilotFullWashingOperations(PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work))
    phase, update = operations.ensure_restart(
        payload=payload,
        cycle_started_at=float(deferred["full_washing_started_at"]),
    )
    assert phase == "resume"
    assert update.status is SubscriptionOperationStatus.READY
    assert update.request["payload"]["state"] == "R"
    assert update.request["payload"]["best_version_full"] == 1
    duplicate_phase, duplicate_update = operations.ensure_restart(
        payload=payload,
        cycle_started_at=float(deferred["full_washing_started_at"]),
    )
    assert duplicate_phase == phase
    assert duplicate_update.id == update.id

    asset = json.dumps(
        [
            {
                "resolution_display": "2160p",
                "codec_display": "HEVC",
                "effect_display": "DoVi P8",
                "release_group_raw": ["FixtureGroup"],
            }
        ]
    )
    with p7_runtime.engine.begin() as connection:
        for episode in (1, 2):
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, title, parent_series_tmdb_id,
                        season_number, episode_number, in_library,
                        asset_details_json, last_updated_at
                    ) VALUES (
                        :tmdb_id, 'Episode', :title, '2500', 1, :episode,
                        TRUE, CAST(:asset AS jsonb), NOW()
                    )
                    """
                ),
                {
                    "tmdb_id": f"2500-episode-{episode}",
                    "title": f"Episode {episode}",
                    "episode": episode,
                    "asset": asset,
                },
            )

    completed = repository.apply(
        event_key="washing-monitor",
        event_type="subscribe.complete",
        payload_digest="b" * 64,
        payload=payload,
    )

    assert completed["snapshot_created"] is True
    assert completed.get("completion_deferred") is None
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        receipts = connection.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = 'moviepilot_webhook_receipts'
                """
            )
        )
        active_washing = connection.scalar(
            text(
                """
                SELECT active_washing
                FROM media_metadata
                WHERE tmdb_id = 'season-mp-25' AND item_type = 'Season'
                """
            )
        )
    assert "full_washing" not in subscribes["25"]
    assert "washing-monitor" in receipts
    assert active_washing is False

    def unexpected_provider_read() -> tuple[MoviePilotActiveDownload, ...]:
        raise AssertionError("completed fact replay must not call MoviePilot")

    replay_repository = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        active_downloads=unexpected_provider_read,
    )
    duplicate_completed = replay_repository.apply(
        event_key="washing-monitor-duplicate-event",
        event_type="subscribe.complete",
        payload_digest="b" * 64,
        payload=payload,
    )
    assert duplicate_completed["snapshot_created"] is False
    with p7_runtime.engine.connect() as connection:
        snapshot_count = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM subscribe_assistant_snapshots
                WHERE subscribe_id = 25 AND tmdb_id = '2500' AND season_number = 1
                """
            )
        )
    assert snapshot_count == 1

    replayed = replay_repository.apply(
        event_key="washing-monitor",
        event_type="subscribe.complete",
        payload_digest="b" * 64,
        payload=payload,
    )
    assert replayed == completed


def test_pg18_active_download_readback_refuses_to_overwrite_newer_download_state(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(28,))
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES
                    ('subscribes', CAST(:subscribes AS jsonb), NOW()),
                    ('torrents', '{}'::jsonb, NOW())
                """
            ),
            {"subscribes": json.dumps({"28": {"subscribe_id": 28, "tmdb_id": "2800", "season": 1}})},
        )

    def concurrent_download() -> tuple[MoviePilotActiveDownload, ...]:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE subscribe_assistant_state
                    SET state_json = CAST(:torrents AS jsonb), updated_at = NOW()
                    WHERE state_key = 'torrents'
                    """
                ),
                {
                    "torrents": json.dumps(
                        {
                            "newerhash": {
                                "hash": "newerhash",
                                "subscribe_id": 28,
                                "tmdb_id": "2800",
                                "season": 1,
                            }
                        }
                    )
                },
            )
        return ()

    repository = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        active_downloads=concurrent_download,
    )
    payload = {
        "data": {
            "subscribe_id": 28,
            "subscribe_info": {
                "id": 28,
                "tmdbid": "2800",
                "type": "电视剧",
                "season": 1,
                "best_version_full": 1,
                "total_episode": 12,
            },
        }
    }

    with pytest.raises(MoviePilotAssistantReadbackUnavailable, match="state changed"):
        repository.apply(
            event_key="washing-concurrent-download",
            event_type="subscribe.complete",
            payload_digest="c" * 64,
            payload=payload,
        )
    with p7_runtime.engine.connect() as connection:
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
    assert "newerhash" in torrents


def test_pg18_terminal_full_washing_failure_closes_local_state_and_replays_without_provider(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(27,))
    cycle_started_at = datetime.now(UTC).timestamp() - 600
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = 'season-mp-27'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    in_library, subscription_status, subscription_sources_json,
                    active_washing, last_updated_at
                ) VALUES (
                    'season-mp-27', 'Season', 'Fixture Failure', '2700', 1,
                    FALSE, 'SUBSCRIBED', '[]'::jsonb, TRUE, NOW()
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES ('subscribes', CAST(:state AS jsonb), NOW())
                """
            ),
            {
                "state": json.dumps(
                    {
                        "27": {
                            "subscribe_id": 27,
                            "tmdb_id": "2700",
                            "season": 1,
                            "full_washing": True,
                            "full_washing_started_at": cycle_started_at,
                        }
                    }
                )
            },
        )
    payload = {
        "data": {
            "subscribe_id": 27,
            "subscribe_info": {
                "id": 27,
                "tmdbid": "2700",
                "type": "电视剧",
                "season": 1,
                "best_version_full": 1,
                "total_episode": 12,
            },
        }
    }

    def unexpected_provider_read() -> tuple[MoviePilotActiveDownload, ...]:
        raise AssertionError("terminal failure replay must not call MoviePilot")

    repository = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        active_downloads=unexpected_provider_read,
    )
    failed = repository.finalize_full_washing_failure(
        event_key="washing-terminal-failure",
        event_type="subscribe.complete",
        payload_digest="f" * 64,
        payload=payload,
        cycle_started_at=cycle_started_at,
        phase="resume",
        operation_id="operation-terminal-fixture",
        operation_status=SubscriptionOperationStatus.INDETERMINATE,
    )
    assert failed["completion_failed"] is True
    assert failed["operation_status"] == "indeterminate"

    duplicate = repository.apply(
        event_key="washing-terminal-failure-duplicate-event",
        event_type="subscribe.complete",
        payload_digest="f" * 64,
        payload=payload,
    )
    assert duplicate["completion_failed"] is True
    assert duplicate["operation_id"] == "operation-terminal-fixture"
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        active_washing = connection.scalar(
            text(
                """
                SELECT active_washing
                FROM media_metadata
                WHERE tmdb_id = 'season-mp-27' AND item_type = 'Season'
                """
            )
        )
    assert "full_washing" not in subscribes["27"]
    assert subscribes["27"]["full_washing_failure"]["operation_status"] == "indeterminate"
    assert active_washing is False

    new_cycle_started_at = datetime.now(UTC).timestamp()
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE subscribe_assistant_state
                SET state_json = CAST(:state AS jsonb), updated_at = NOW()
                WHERE state_key = 'subscribes'
                """
            ),
            {
                "state": json.dumps(
                    {
                        "27": {
                            "subscribe_id": 27,
                            "tmdb_id": "2700",
                            "season": 1,
                            "full_washing": True,
                            "full_washing_started_at": new_cycle_started_at,
                        }
                    }
                )
            },
        )
    superseded = repository.finalize_full_washing_failure(
        event_key="washing-terminal-failure-old-cycle",
        event_type="subscribe.complete",
        payload_digest="f" * 64,
        payload=payload,
        cycle_started_at=cycle_started_at,
        phase="resume",
        operation_id="operation-old-cycle",
        operation_status=SubscriptionOperationStatus.REJECTED,
    )
    assert superseded["completion_superseded"] is True
    with p7_runtime.engine.connect() as connection:
        current = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )["27"]
    assert current["full_washing"] is True
    assert current["full_washing_started_at"] == pytest.approx(new_cycle_started_at)


def test_pg18_full_washing_timeout_uses_stable_delete_then_atomically_closes_local_state(
    p7_runtime: DatabaseRuntime,
) -> None:
    _reset_assistant_state(p7_runtime, subscribe_ids=(26,))
    started_at = datetime.now(UTC).timestamp() - 7_200
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM media_metadata
                WHERE parent_series_tmdb_id = '2600' OR tmdb_id = 'season-mp-26'
                """
            )
        )
        connection.execute(
            text("DELETE FROM subscription_operations WHERE scope_key LIKE 'moviepilot-full-washing:2600:%'")
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id, season_number,
                    in_library, subscription_status, subscription_sources_json,
                    active_washing, last_updated_at
                ) VALUES (
                    'season-mp-26', 'Season', 'Fixture Timeout', '2600', 1,
                    FALSE, 'SUBSCRIBED', '[]'::jsonb, TRUE, NOW()
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES
                    ('subscribes', CAST(:subscribes AS jsonb), NOW()),
                    ('torrents', CAST(:torrents AS jsonb), NOW())
                """
            ),
            {
                "subscribes": json.dumps(
                    {
                        "26": {
                            "subscribe_id": 26,
                            "tmdb_id": "2600",
                            "season": 1,
                            "full_washing": True,
                            "full_washing_started_at": started_at,
                            "active_sources": {"download_pending": "pending"},
                        }
                    }
                ),
                "torrents": json.dumps(
                    {
                        "timeouthash": {
                            "hash": "timeouthash",
                            "subscribe_id": 26,
                            "tmdb_id": "2600",
                            "season": 1,
                        }
                    }
                ),
            },
        )
    repository = SqlAlchemyMoviePilotAssistantEventRepository(
        p7_runtime.unit_of_work,
        active_downloads=lambda: pytest.fail("expired full-washing state must not depend on MoviePilot readback"),
    )
    operations = MoviePilotFullWashingOperations(PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work))
    delivery = MoviePilotWebhookEffectDelivery(
        repository,
        SimpleNamespace(process=lambda **_kwargs: {}),  # type: ignore[arg-type]
        operations,
        full_washing_timeout_hours=lambda: 1,
        snapshot_retention_days=lambda: 30,
    )
    payload = {
        "data": {
            "subscribe_id": 26,
            "subscribe_info": {
                "id": 26,
                "tmdbid": "2600",
                "type": "电视剧",
                "season": 1,
                "best_version_full": 1,
                "total_episode": 12,
            },
        }
    }
    effect = SimpleNamespace(
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        payload={
            "event_key": "timeout-monitor",
            "event_type": "subscribe.complete",
            "payload_digest": "d" * 64,
            "payload": payload,
        },
        attempts=1,
    )

    with pytest.raises(MediaEffectDeliveryRetryable, match="timeout reconciliation is pending"):
        delivery.deliver(effect)  # type: ignore[arg-type]
    with p7_runtime.engine.begin() as connection:
        operation = (
            connection.execute(
                text(
                    """
                        SELECT id, status, request_json
                        FROM subscription_operations
                        WHERE scope_key LIKE :scope_pattern
                        """
                ),
                {"scope_pattern": "moviepilot-full-washing:2600:season:1:cycle:%:timeout-delete"},
            )
            .mappings()
            .one()
        )
        suppressions = connection.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = 'snapshot_restore_suppressions'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE subscription_operations
                SET status = 'confirmed', finished_at = NOW(), updated_at = NOW()
                WHERE id = :operation_id
                """
            ),
            {"operation_id": operation["id"]},
        )
    assert operation["status"] == "ready"
    assert operation["request_json"]["action"] == "delete"
    assert operation["request_json"]["expected"] == {"id": 26}
    assert suppressions["tmdb:2600:S1"]["reason"] == "full_washing_timeout_pending"

    completed = delivery.deliver(effect)  # type: ignore[arg-type]

    assert completed["completion_timed_out"] is True
    assert completed["torrents_removed"] == 1
    with p7_runtime.engine.connect() as connection:
        subscribes = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
        )
        torrents = connection.scalar(
            text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'torrents'")
        )
        receipts = connection.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = 'moviepilot_webhook_receipts'
                """
            )
        )
        active_washing = connection.scalar(
            text(
                """
                SELECT active_washing
                FROM media_metadata
                WHERE tmdb_id = 'season-mp-26' AND item_type = 'Season'
                """
            )
        )
    assert subscribes["26"]["deleted"] is True
    assert "full_washing" not in subscribes["26"]
    assert torrents == {}
    assert receipts["timeout-monitor"]["result"]["completion_timed_out"] is True
    assert active_washing is False
