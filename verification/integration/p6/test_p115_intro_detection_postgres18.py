from __future__ import annotations

import json
import zlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import inspect, text

from etk.domains.p115.intro_detection import (
    INTRO_DETECTION_TOPIC,
    IntroChapterProjection,
    IntroDetectionCancelled,
    IntroDetectionIntent,
    IntroDetectionOperationStatus,
    IntroDetectionPhase,
    IntroDetectionPolicySnapshot,
    IntroDetectionStateError,
    IntroEpisodeSnapshot,
    IntroFingerprintKind,
    IntroJobTrigger,
    IntroTriggerMode,
)
from etk.domains.p115.intro_detection_repository import PostgresP115IntroDetectionRepository
from etk.platform.db import DatabaseRuntime


def _episode(number: int, *, cache_revision: int = 0) -> IntroEpisodeSnapshot:
    return IntroEpisodeSnapshot.create(
        item_id=f"intro-item-{number}",
        series_tmdb_id="intro-series",
        season_number=1,
        episode_number=number,
        sha1=f"{number:040X}",
        pick_code=f"intro-pick-{number}",
        source_library_id="intro-library",
        runtime_seconds=1200,
        cache_revision=cache_revision,
    )


def _intent(*, second_revision: int = 0) -> IntroDetectionIntent:
    return IntroDetectionIntent.create(
        account_id="115001",
        trigger=IntroJobTrigger.IMPORT,
        policy=IntroDetectionPolicySnapshot.create(
            trigger_mode=IntroTriggerMode.IMPORT,
            credits_enabled=True,
        ),
        episodes=(_episode(1), _episode(2, cache_revision=second_revision)),
        available_at=datetime.now(UTC),
    )


def _repository(runtime: DatabaseRuntime) -> PostgresP115IntroDetectionRepository:
    return PostgresP115IntroDetectionRepository(runtime.unit_of_work)


@pytest.fixture(autouse=True)
def clean_intro_state(p6_runtime: DatabaseRuntime):
    def clean() -> None:
        with p6_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM outbox_events WHERE topic IN (:intro_topic, 'media.effect.requested')"),
                {"intro_topic": INTRO_DETECTION_TOPIC},
            )
            connection.execute(text("DELETE FROM media_effects"))
            connection.execute(text("DELETE FROM p115_intro_detection_operations"))
            connection.execute(text("DELETE FROM p115_intro_detection_failures"))
            connection.execute(text("DELETE FROM p115_intro_fingerprint_failures"))
            connection.execute(text("DELETE FROM p115_intro_fingerprint_cache"))
            connection.execute(text("DELETE FROM p115_mediainfo_cache"))
            connection.execute(text("DELETE FROM media_metadata"))

    clean()
    yield
    clean()


def test_intro_operation_schema_has_hard_claim_and_active_season_constraints(
    p6_runtime: DatabaseRuntime,
) -> None:
    inspector = inspect(p6_runtime.engine)

    columns = {column["name"] for column in inspector.get_columns("p115_intro_detection_operations")}
    checks = {constraint["name"] for constraint in inspector.get_check_constraints("p115_intro_detection_operations")}
    indexes = {index["name"] for index in inspector.get_indexes("p115_intro_detection_operations")}

    assert {
        "operation_key",
        "request_hash",
        "source_set_revision",
        "policy_revision",
        "claim_token",
        "lease_expires_at",
        "cancel_requested_at",
        "phase",
        "progress_json",
    } <= columns
    assert "ck_p115_intro_claim" in checks
    assert "ck_p115_intro_finished" in checks
    assert "uq_p115_intro_active_season" in indexes


def test_reserve_is_idempotent_and_new_source_revision_supersedes_active_operation(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    first = repository.reserve(_intent())
    duplicate = repository.reserve(_intent())
    replacement = repository.reserve(_intent(second_revision=1))

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.operation.id == first.operation.id
    assert replacement.created is True
    assert replacement.operation.id != first.operation.id
    stale = repository.read(first.operation.id)
    assert stale is not None
    assert stale.status is IntroDetectionOperationStatus.SUPERSEDED
    with p6_runtime.engine.connect() as connection:
        active = connection.scalar(
            text(
                """
                SELECT count(*) FROM p115_intro_detection_operations
                WHERE status IN ('ready','claimed','retryable')
                """
            )
        )
        pending = connection.scalar(
            text("SELECT count(*) FROM outbox_events WHERE topic = :topic AND status = 'pending'"),
            {"topic": INTRO_DETECTION_TOPIC},
        )
    assert active == 1
    assert pending == 1


def test_claim_token_and_lease_are_hard_fences(p6_runtime: DatabaseRuntime) -> None:
    repository = _repository(p6_runtime)
    operation = repository.reserve(_intent()).operation
    claimed = repository.claim_by_id(operation.id, worker_id="intro-worker", lease_seconds=60)
    assert claimed is not None
    assert repository.claim_by_id(operation.id, worker_id="other-worker", lease_seconds=60) is None

    with pytest.raises(IntroDetectionStateError):
        repository.mark_retryable(
            operation.id,
            uuid4(),
            phase=IntroDetectionPhase.INTRO_EXTRACT,
            progress={},
            error={"type": "stale"},
            retry_after_seconds=1,
        )
    assert repository.heartbeat(operation.id, uuid4(), lease_seconds=60) is False


def test_cancel_request_fences_staging_and_wins_the_claimed_transition_race(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    operation = repository.reserve(_intent()).operation
    claimed = repository.claim_by_id(operation.id, worker_id="intro-worker", lease_seconds=60)
    assert claimed is not None and claimed.claim_token is not None
    repository.request_cancel(operation.id)

    projection = IntroChapterProjection(
        claimed.episodes[0],
        intro_start_ticks=0,
        intro_end_ticks=180_000_000,
    )
    with pytest.raises(IntroDetectionCancelled, match="cancellation was requested"):
        repository.stage_chapters(
            operation.id,
            claimed.claim_token,
            (projection,),
            authorization_revision="a" * 64,
            phase=IntroDetectionPhase.PROJECT,
            progress={"must_not_persist": True},
        )

    cancelled = repository.mark_retryable(
        operation.id,
        claimed.claim_token,
        phase=IntroDetectionPhase.PROJECT,
        progress={"must_not_persist": True},
        error={"type": "late_retry"},
        retry_after_seconds=1,
    ).operation

    assert cancelled.status is IntroDetectionOperationStatus.CANCELLED
    assert cancelled.progress == {}
    assert cancelled.error == {"type": "cancelled"}
    with p6_runtime.engine.connect() as connection:
        effect_count = connection.scalar(text("SELECT count(*) FROM media_effects"))
        invalid_retryable = connection.scalar(
            text(
                """
                SELECT count(*) FROM p115_intro_detection_operations
                WHERE status = 'retryable' AND cancel_requested_at IS NOT NULL
                """
            )
        )
        pending_dispatch = connection.scalar(
            text("SELECT count(*) FROM outbox_events WHERE topic = :topic AND status = 'pending'"),
            {"topic": INTRO_DETECTION_TOPIC},
        )
    assert effect_count == 0
    assert invalid_retryable == 0
    assert pending_dispatch == 0


def test_fingerprint_cache_coverage_never_downgrades(p6_runtime: DatabaseRuntime) -> None:
    repository = _repository(p6_runtime)
    sha1 = _episode(1).sha1
    long_values = tuple(range(600))
    short_values = tuple(range(180))

    assert repository.save_fingerprint(
        sha1,
        IntroFingerprintKind.INTRO,
        long_values,
        sample_seconds=600,
    )
    assert not repository.save_fingerprint(
        sha1,
        IntroFingerprintKind.INTRO,
        short_values,
        sample_seconds=180,
    )

    loaded = repository.load_fingerprint(
        sha1,
        IntroFingerprintKind.INTRO,
        minimum_sample_seconds=600,
    )
    assert loaded == (long_values, 600)


def test_legacy_credits_coverage_is_normalized_and_valid_300_second_data_self_heals(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    sha1 = _episode(1).sha1
    legacy_values = tuple(range(100))
    healed_values = tuple(range(100, 200))
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_intro_fingerprint_cache (
                    sha1, fingerprint_zlib, fingerprint_count, sample_seconds
                ) VALUES (
                    :cache_key, :fingerprint_zlib, :fingerprint_count, 600
                )
                """
            ),
            {
                "cache_key": f"{sha1}:credits",
                "fingerprint_zlib": zlib.compress(json.dumps(legacy_values).encode("utf-8")),
                "fingerprint_count": len(legacy_values),
            },
        )

    assert repository.load_fingerprint(
        sha1,
        IntroFingerprintKind.CREDITS,
        minimum_sample_seconds=300,
    ) == (legacy_values, 300)
    assert repository.save_fingerprint(
        sha1,
        IntroFingerprintKind.CREDITS,
        healed_values,
        sample_seconds=300,
    )
    assert not repository.save_fingerprint(
        sha1,
        IntroFingerprintKind.CREDITS,
        tuple(range(60)),
        sample_seconds=180,
    )
    assert repository.load_fingerprint(
        sha1,
        IntroFingerprintKind.CREDITS,
        minimum_sample_seconds=300,
    ) == (healed_values, 300)
    with p6_runtime.engine.connect() as connection:
        stored_sample = connection.scalar(
            text("SELECT sample_seconds FROM p115_intro_fingerprint_cache WHERE sha1 = :cache_key"),
            {"cache_key": f"{sha1}:credits"},
        )
    assert stored_sample == 300


def test_chapter_cache_effect_and_operation_progress_stage_in_one_uow(
    p6_runtime: DatabaseRuntime,
) -> None:
    episodes = (_episode(1), _episode(2))
    with p6_runtime.engine.begin() as connection:
        for episode in episodes:
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, in_library, emby_item_ids_json,
                        file_sha1_json, file_pickcode_json, asset_details_json,
                        parent_series_tmdb_id, season_number, episode_number, title
                    ) VALUES (
                        :tmdb_id, 'Episode', TRUE, CAST(:item_ids AS jsonb),
                        CAST(:sha1s AS jsonb), CAST(:pick_codes AS jsonb),
                        CAST(:assets AS jsonb), :series_tmdb_id, :season_number,
                        :episode_number, :title
                    )
                    """
                ),
                {
                    "tmdb_id": f"episode-{episode.episode_number}",
                    "item_ids": f'["{episode.item_id}"]',
                    "sha1s": f'["{episode.sha1}"]',
                    "pick_codes": f'["{episode.pick_code}"]',
                    "assets": '[{"source_library_id":"intro-library"}]',
                    "series_tmdb_id": episode.series_tmdb_id,
                    "season_number": episode.season_number,
                    "episode_number": episode.episode_number,
                    "title": f"Episode {episode.episode_number}",
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json, raw_ffprobe_json)
                    VALUES (
                        :sha1,
                        CAST(:mediainfo AS jsonb),
                        CAST(:raw AS jsonb)
                    )
                    """
                ),
                {
                    "sha1": episode.sha1,
                    "mediainfo": json.dumps(
                        {
                            "MediaSourceInfo": {
                                "RunTimeTicks": 12_000_000_000,
                                "MediaStreams": [],
                            },
                            "Chapters": [],
                        }
                    ),
                    "raw": "{}",
                },
            )

    repository = _repository(p6_runtime)
    intent = IntroDetectionIntent.create(
        account_id="115001",
        trigger=IntroJobTrigger.IMPORT,
        policy=IntroDetectionPolicySnapshot.create(
            trigger_mode=IntroTriggerMode.IMPORT,
            credits_enabled=True,
        ),
        episodes=episodes,
        available_at=datetime.now(UTC),
    )
    operation = repository.reserve(intent).operation
    claimed = repository.claim_by_id(operation.id, worker_id="intro-worker", lease_seconds=60)
    assert claimed is not None and claimed.claim_token is not None
    projection = IntroChapterProjection(
        claimed.episodes[0],
        intro_start_ticks=0,
        intro_end_ticks=180_000_000,
    )

    effect_ids = repository.stage_chapters(
        operation.id,
        claimed.claim_token,
        (projection,),
        authorization_revision="a" * 64,
        phase=IntroDetectionPhase.PROJECT,
        progress={"intro_projected": 1},
    )

    assert len(effect_ids) == 1
    with p6_runtime.engine.connect() as connection:
        chapters = connection.scalar(
            text("SELECT mediainfo_json -> 'Chapters' FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
            {"sha1": projection.episode.sha1},
        )
        effect = (
            connection.execute(
                text("SELECT effect_type, status FROM media_effects WHERE id = :effect_id"),
                {"effect_id": effect_ids[0]},
            )
            .mappings()
            .one()
        )
        progress = connection.scalar(
            text("SELECT progress_json FROM p115_intro_detection_operations WHERE id = :operation_id"),
            {"operation_id": operation.id},
        )
    assert [chapter["MarkerType"] for chapter in chapters] == ["IntroStart", "IntroEnd"]
    assert effect == {"effect_type": "emby.chapters.update", "status": "pending"}
    assert progress["intro_projected"] == 1
    assert progress["effect_ids"] == [str(effect_ids[0])]


def test_dead_outbox_rejects_unclaimed_operation(p6_runtime: DatabaseRuntime) -> None:
    repository = _repository(p6_runtime)
    operation = repository.reserve(_intent()).operation
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET status = 'dead' WHERE topic = :topic"),
            {"topic": INTRO_DETECTION_TOPIC},
        )

    assert repository.reject_dead_dispatches() == 1
    rejected = repository.read(operation.id)
    assert rejected is not None
    assert rejected.status is IntroDetectionOperationStatus.REJECTED
