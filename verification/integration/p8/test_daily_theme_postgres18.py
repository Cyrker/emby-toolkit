from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.daily_theme import DailyThemeService
from etk.domains.media.daily_theme_effects import DailyThemeReplenishmentEffectDelivery
from etk.domains.media.daily_theme_repository import (
    SqlAlchemyDailyThemeRepository,
    remove_recommendation_in_uow,
)
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import MediaEffectStatus, PostgresMediaEffectLedger
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime


def _seed_publication(runtime: DatabaseRuntime) -> SqlAlchemyDailyThemeRepository:
    SqlAlchemyAppSettingsRepository(runtime.unit_of_work).save_many(
        {
            "keyword_mapping": [{"label": "科幻", "ids": [11]}],
            "recommendation_pool": [{"id": 100}],
            "recommendation_theme_index": 0,
            "recommendation_pool_page": 2,
        }
    )
    return SqlAlchemyDailyThemeRepository(runtime.unit_of_work)


def _metadata(label: str, publication_id: str | None = None) -> dict[str, Any]:
    return {
        "publication_id": publication_id or f"fixture-{label}",
        "source_revision": 0,
        "source_fingerprint": label,
        "theme_index": 1,
        "theme_label": label,
        "pool_page": 3,
        "pool_size": 1,
        "published_at": "2026-08-30T00:00:00+00:00",
    }


class _ForbiddenDailyThemeProvider:
    def daily_theme_configured(self) -> bool:
        raise AssertionError("publication receipt replay must not call the provider")

    def discover_movie(self, payload: Any) -> Any:
        del payload
        raise AssertionError("publication receipt replay must not call the provider")

    def movie_details(self, movie_id: int) -> Any:
        del movie_id
        raise AssertionError("publication receipt replay must not call the provider")


class _DailyThemeDefaults:
    def keyword_mapping(self) -> list[dict[str, Any]]:
        return [{"label": "科幻", "ids": [11]}]


def _expire_media_effect_claim(runtime: DatabaseRuntime, effect_id: Any) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE id = :effect_id
                """
            ),
            {"effect_id": effect_id},
        )


def test_daily_theme_publication_is_atomic_and_stale_snapshot_cannot_overwrite(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _seed_publication(p8_runtime)
    settings = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    snapshot = repository.load_snapshot()

    revision = repository.publish(
        expected_fingerprint=snapshot.fingerprint,
        pool=[{"id": 200}],
        theme_index=1,
        pool_page=3,
        metadata=_metadata("科幻"),
    )
    stale = repository.publish(
        expected_fingerprint=snapshot.fingerprint,
        pool=[{"id": 300}],
        theme_index=2,
        pool_page=4,
        metadata=_metadata("旧计划"),
    )

    assert revision is not None
    assert revision.revision == 1
    assert revision.replayed is False
    assert stale is None
    assert settings.load_many(
        (
            "recommendation_pool",
            "recommendation_theme_index",
            "recommendation_pool_page",
            "recommendation_publication_state",
        )
    ) == {
        "recommendation_pool": [{"id": 200}],
        "recommendation_theme_index": 1,
        "recommendation_pool_page": 3,
        "recommendation_publication_state": {
            **_metadata("科幻"),
            "revision": 1,
        },
    }


def test_daily_theme_publication_rolls_back_all_settings_when_final_value_is_invalid(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _seed_publication(p8_runtime)
    settings = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    snapshot = repository.load_snapshot()

    with pytest.raises(TypeError):
        repository.publish(
            expected_fingerprint=snapshot.fingerprint,
            pool=[{"id": 200}],
            theme_index=1,
            pool_page=3,
            metadata={"publication_id": "fixture-invalid", "invalid": object()},
        )

    assert settings.load_many(
        (
            "recommendation_pool",
            "recommendation_theme_index",
            "recommendation_pool_page",
            "recommendation_publication_state",
        )
    ) == {
        "recommendation_pool": [{"id": 100}],
        "recommendation_theme_index": 0,
        "recommendation_pool_page": 2,
        "recommendation_publication_state": None,
    }
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                    SELECT count(*) FROM media_effects
                    WHERE effect_key = 'daily-theme-publication:fixture-invalid'
                    """
                )
            )
            == 0
        )


def test_concurrent_daily_theme_publications_have_exactly_one_winner(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _seed_publication(p8_runtime)
    snapshot = repository.load_snapshot()

    def publish(movie_id: int):
        return repository.publish(
            expected_fingerprint=snapshot.fingerprint,
            pool=[{"id": movie_id}],
            theme_index=1,
            pool_page=1,
            metadata=_metadata(str(movie_id), f"fixture-concurrent-{movie_id}"),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(publish, (201, 202)))

    assert sorted(result is None for result in results) == [False, True]
    published = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work).load_many(
        ("recommendation_pool", "recommendation_publication_state")
    )
    winner = int(published["recommendation_pool"][0]["id"])
    assert winner in {201, 202}
    assert published["recommendation_publication_state"]["theme_label"] == str(winner)
    assert published["recommendation_publication_state"]["revision"] == 1


def test_daily_theme_receipt_survives_a_later_publication_and_revision_zero_is_valid(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _seed_publication(p8_runtime)
    settings = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    settings.save("recommendation_publication_state", {"revision": 0})

    first_snapshot = repository.load_snapshot()
    assert first_snapshot.publication_revision == 0
    first = repository.publish(
        expected_fingerprint=first_snapshot.fingerprint,
        pool=[{"id": 201}],
        theme_index=1,
        pool_page=1,
        metadata=_metadata("first", "fixture-job-first"),
    )
    assert first is not None and first.revision == 1

    second_snapshot = repository.load_snapshot()
    second = repository.publish(
        expected_fingerprint=second_snapshot.fingerprint,
        pool=[{"id": 202}],
        theme_index=2,
        pool_page=2,
        metadata=_metadata("second", "fixture-job-second"),
    )
    assert second is not None and second.revision == 2

    first_receipt = repository.load_receipt("fixture-job-first")
    assert first_receipt is not None
    assert first_receipt["revision"] == 1
    assert first_receipt["theme_label"] == "first"


def test_replenishment_stale_claim_retries_before_commit_and_reconciles_after_commit(
    p8_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p8_runtime.unit_of_work)
    before_commit = ledger.reserve(
        effect_key="fixture-replenishment-before-commit",
        effect_type="daily-theme.replenishment",
        aggregate_type="daily_theme_publication",
        aggregate_id="1",
        payload={"publication_revision": 1},
    ).effect
    assert ledger.claim_by_id(before_commit.id, worker_id="fixture-crashed-worker", lease_seconds=60) is not None
    _expire_media_effect_claim(p8_runtime, before_commit.id)

    assert (
        ledger.requeue_replay_safe_stale_claims(
            effect_types=("daily-theme.replenishment",),
            limit=10,
        )
        == 1
    )
    retryable = ledger.read_by_id(before_commit.id)
    assert retryable is not None
    assert retryable.status is MediaEffectStatus.RETRYABLE
    assert retryable.error == {
        "type": "claim_lease_expired_retryable",
        "message": "执行器租约过期，本地幂等副作用将安全重放",
    }
    assert ledger.expire_stale_claims(limit=10) == 0

    repository = _seed_publication(p8_runtime)
    after_commit = ledger.reserve(
        effect_key="fixture-replenishment-after-commit",
        effect_type="daily-theme.replenishment",
        aggregate_type="daily_theme_publication",
        aggregate_id="1",
        payload={"publication_revision": 1},
    ).effect
    assert ledger.claim_by_id(after_commit.id, worker_id="fixture-crashed-worker", lease_seconds=60) is not None
    snapshot = repository.load_snapshot()
    publication = repository.publish(
        expected_fingerprint=snapshot.fingerprint,
        pool=[{"id": 100}],
        theme_index=0,
        pool_page=2,
        metadata={
            "publication_id": str(after_commit.id),
            "mode": "replenish",
            "source_revision": snapshot.publication_revision,
            "source_fingerprint": snapshot.fingerprint,
            "theme_index": 0,
            "theme_label": "科幻",
            "pool_page": 2,
            "pool_size": 1,
            "pages_scanned": 1,
            "added_count": 0,
            "published_at": "2026-08-30T00:00:00+00:00",
        },
    )
    assert publication is not None
    _expire_media_effect_claim(p8_runtime, after_commit.id)
    assert (
        ledger.requeue_replay_safe_stale_claims(
            effect_types=("daily-theme.replenishment",),
            limit=10,
        )
        == 1
    )

    reconciled = PersistentMediaEffectExecutor(
        ledger,
        DailyThemeReplenishmentEffectDelivery(
            DailyThemeService(
                _ForbiddenDailyThemeProvider(),
                repository,
                _DailyThemeDefaults(),
            )
        ),
        worker_id="fixture-replacement-worker",
    ).execute(after_commit.id)

    assert reconciled.status is MediaEffectStatus.CONFIRMED
    assert reconciled.result is not None
    assert reconciled.result["details"]["replayed"] is True
    assert repository.load_snapshot().publication_revision == publication.revision


def test_daily_publication_and_portal_removal_share_one_revision_fence(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _seed_publication(p8_runtime)
    snapshot = repository.load_snapshot()
    barrier = Barrier(2)

    def publish():
        barrier.wait()
        return repository.publish(
            expected_fingerprint=snapshot.fingerprint,
            pool=[{"id": 100}, {"id": 200}],
            theme_index=1,
            pool_page=3,
            metadata=_metadata("daily", "fixture-daily-race"),
        )

    def remove():
        barrier.wait()
        with p8_runtime.unit_of_work() as uow:
            result = remove_recommendation_in_uow(
                uow,
                tmdb_id="100",
                mutation_id="fixture-portal-race",
                occurred_at="2026-08-30T00:00:00+00:00",
            )
            uow.commit()
            return result

    with ThreadPoolExecutor(max_workers=2) as executor:
        published_future = executor.submit(publish)
        removed_future = executor.submit(remove)
        published = published_future.result()
        removed = removed_future.result()

    assert removed is not None
    final = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work).load_many(
        ("recommendation_pool", "recommendation_publication_state")
    )
    assert all(str(item.get("id")) != "100" for item in final["recommendation_pool"])
    assert final["recommendation_publication_state"]["revision"] in {1, 2}
    if published is None:
        assert final["recommendation_publication_state"]["mutation"] == "portal_removal"


def test_pool_or_mapping_drift_fences_an_old_daily_theme_plan(p8_runtime: DatabaseRuntime) -> None:
    repository = _seed_publication(p8_runtime)
    snapshot = repository.load_snapshot()
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = CAST(:value AS jsonb), last_updated_at = NOW()
                WHERE setting_key = 'recommendation_pool'
                """
            ),
            {"value": '[{"id":999}]'},
        )

    assert (
        repository.publish(
            expected_fingerprint=snapshot.fingerprint,
            pool=[{"id": 200}],
            theme_index=1,
            pool_page=3,
            metadata=_metadata("stale"),
        )
        is None
    )
    assert SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work).load("recommendation_pool") == [{"id": 999}]


def test_daily_theme_local_projection_excludes_library_and_active_subscription_and_reads_actor_names(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyDailyThemeRepository(p8_runtime.unit_of_work)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json, subscription_status
                ) VALUES
                    ('1', 'Movie', TRUE, '[]'::jsonb, 'NONE'),
                    ('2', 'Movie', FALSE, '[]'::jsonb, 'WANTED'),
                    ('3', 'Movie', FALSE, '["emby-3"]'::jsonb, 'NONE'),
                    ('4', 'Movie', FALSE, '[]'::jsonb, 'IGNORED')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO person_metadata (tmdb_person_id, primary_name)
                VALUES (700001, '张三'), (700002, 'Actor')
                """
            )
        )

    try:
        assert repository.excluded_movie_ids(("1", "2", "3", "4", "5")) == {"1", "2", "3"}
        assert repository.actor_names((700001, 700002)) == {700001: "张三", 700002: "Actor"}
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM person_metadata WHERE tmdb_person_id IN (700001, 700002)"))
