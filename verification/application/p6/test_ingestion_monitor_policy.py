from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

from etk.domains.ingestion import (
    FileObservation,
    MonitorClassification,
    MonitorEventType,
    MonitorItemProjection,
    MonitorItemStatus,
    MonitorPolicy,
    MonitorSource,
    StabilityOutcome,
    evaluate_stability,
    normalize_media_path,
    valid_etk_strm_content,
)

NOW = datetime(2026, 8, 26, 1, 2, 3, tzinfo=UTC)


def _policy(*, enabled: bool = True, refresh_delay_seconds: int = 0) -> MonitorPolicy:
    return MonitorPolicy.create(
        enabled=enabled,
        extensions=("mkv", "*.strm", ".mp3"),
        exclude_paths=("/library/excluded",),
        refresh_delay_seconds=refresh_delay_seconds,
    )


def _item(path: str = "/library/Movie.mkv") -> MonitorItemProjection:
    return MonitorItemProjection(
        id=UUID("00000000-0000-0000-0000-000000000901"),
        event_sequence=1,
        producer_key="fixture-created",
        request_hash=b"x" * 32,
        source=MonitorSource.WATCHDOG,
        event_type=MonitorEventType.UPSERT,
        path=path,
        path_key=normalize_media_path(path),
        related_path=None,
        correlation_id=None,
        monitor_root="/library",
        config_revision="a" * 64,
        refresh_delay_seconds=0,
        classification=MonitorClassification.PHYSICAL,
        status=MonitorItemStatus.STABILITY_WAIT,
        attempts=0,
        available_at=NOW,
        stability_deadline=NOW + timedelta(seconds=60),
        last_size=None,
        last_mtime_ns=None,
        stable_matches=0,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


def test_created_modified_and_classification_preserve_legacy_boundaries() -> None:
    policy = _policy(refresh_delay_seconds=17)

    physical = policy.created(
        "/library/Movie.mkv",
        producer_key="watchdog-created-1",
        monitor_root="/library",
        observed_at=NOW,
    )[0]
    strm = policy.created(
        "/library/Movie.strm",
        producer_key="watchdog-created-2",
        monitor_root="/library",
        observed_at=NOW,
    )[0]
    audio = policy.created(
        "/library/Album/Track.mp3",
        producer_key="watchdog-created-3",
        monitor_root="/library",
        observed_at=NOW,
    )[0]
    excluded = policy.created(
        "/library/excluded/Movie.mkv",
        producer_key="watchdog-created-4",
        monitor_root="/library",
        observed_at=NOW,
    )[0]

    assert physical.classification is MonitorClassification.PHYSICAL
    assert strm.classification is MonitorClassification.STRM
    assert audio.classification is MonitorClassification.REFRESH_ONLY
    assert excluded.classification is MonitorClassification.REFRESH_ONLY
    assert audio.refresh_delay_seconds == excluded.refresh_delay_seconds == 17
    assert physical.refresh_delay_seconds == 17
    assert physical.available_at == NOW + timedelta(seconds=3)
    assert physical.stability_deadline == NOW + timedelta(seconds=63)
    assert policy.revision != _policy(refresh_delay_seconds=0).revision
    assert policy.modified("/library/Movie.mkv") == ()

    assert (
        policy.created(
            "/library/.hidden.mkv",
            producer_key="hidden",
            monitor_root="/library",
            observed_at=NOW,
        )
        == ()
    )
    assert (
        policy.created(
            "/library/Movie.mkv.part",
            producer_key="partial",
            monitor_root="/library",
            observed_at=NOW,
        )
        == ()
    )


def test_move_is_correlated_source_delete_then_destination_upsert() -> None:
    policy = _policy()
    correlation = UUID("00000000-0000-0000-0000-000000000902")

    source, destination = policy.moved(
        "/library/Old.mkv",
        "/library/New.mkv",
        producer_key="watchdog-move-1",
        monitor_root="/library",
        observed_at=NOW,
        correlation_id=correlation,
    )

    assert source.event_type is MonitorEventType.DELETED
    assert source.classification is MonitorClassification.DELETED
    assert source.producer_key == "watchdog-move-1:src"
    assert source.related_path == "/library/New.mkv"
    assert destination.event_type is MonitorEventType.UPSERT
    assert destination.classification is MonitorClassification.PHYSICAL
    assert destination.producer_key == "watchdog-move-1:dest"
    assert destination.related_path == "/library/Old.mkv"
    assert source.correlation_id == destination.correlation_id == correlation


def test_disabled_policy_records_suspended_work_for_later_resume() -> None:
    intent = _policy(enabled=False).created(
        "/library/Movie.mkv",
        producer_key="scan-created-1",
        monitor_root="/library",
        observed_at=NOW,
        source=MonitorSource.SCAN,
    )[0]

    assert intent.suspended is True
    assert intent.source is MonitorSource.SCAN


def test_strm_content_requires_a_nonempty_etk_play_marker() -> None:
    assert valid_etk_strm_content("https://etk.invalid/api/p115/play/pick-code") is True
    assert valid_etk_strm_content(b"https://etk.invalid/api/p115/virtual-play/sha1") is True
    assert valid_etk_strm_content("") is False
    assert valid_etk_strm_content("https://example.invalid/video.mkv") is False


def test_physical_stability_matches_legacy_three_equal_size_comparisons() -> None:
    item = _item()

    first = evaluate_stability(item, FileObservation(True, size=1024, mtime_ns=1), observed_at=NOW)
    second = evaluate_stability(
        replace(item, last_size=first.last_size, stable_matches=first.stable_matches),
        FileObservation(True, size=1024, mtime_ns=2),
        observed_at=NOW + timedelta(seconds=1),
    )
    third = evaluate_stability(
        replace(item, last_size=second.last_size, stable_matches=second.stable_matches),
        FileObservation(True, size=1024, mtime_ns=3),
        observed_at=NOW + timedelta(seconds=2),
    )
    fourth = evaluate_stability(
        replace(item, last_size=third.last_size, stable_matches=third.stable_matches),
        FileObservation(True, size=1024, mtime_ns=4),
        observed_at=NOW + timedelta(seconds=3),
    )

    assert [first.stable_matches, second.stable_matches, third.stable_matches, fourth.stable_matches] == [0, 1, 2, 3]
    assert first.outcome is second.outcome is third.outcome is StabilityOutcome.WAITING
    assert fourth.outcome is StabilityOutcome.READY


def test_strm_is_ready_when_nonempty_and_deadline_is_explicit() -> None:
    strm = replace(
        _item("/library/Movie.strm"),
        classification=MonitorClassification.STRM,
    )
    assert (
        evaluate_stability(strm, FileObservation(True, size=1, mtime_ns=1), observed_at=NOW).outcome
        is StabilityOutcome.READY
    )
    assert (
        evaluate_stability(
            _item(),
            FileObservation(True, size=0, mtime_ns=1),
            observed_at=NOW + timedelta(seconds=60),
        ).outcome
        is StabilityOutcome.TIMED_OUT
    )
