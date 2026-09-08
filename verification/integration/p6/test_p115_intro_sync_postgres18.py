from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.p115.intro_sync import P115IntroSyncService
from etk.domains.p115.intro_sync_adapters import PersistentP115IntroSnapshotExporter
from etk.domains.p115.media_info_repository import SqlAlchemyP115MediaInfoRepository
from etk.platform.db import DatabaseRuntime

SHA_A = "A" * 40
INTRO_CHAPTERS = [
    {
        "StartPositionTicks": 10,
        "Name": "片头",
        "MarkerType": "IntroStart",
        "ChapterIndex": 0,
    },
    {
        "StartPositionTicks": 20,
        "Name": "片头结束",
        "MarkerType": "IntroEnd",
        "ChapterIndex": 1,
    },
]


class EnabledFeature:
    def enabled(self) -> bool:
        return True


class VerifiedItem:
    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        assert (sha1, item_id, expected_pick_code) == (SHA_A, "1001", "")
        return {"Name": "Fixture.mkv", "Chapters": INTRO_CHAPTERS}


class NoopExporter:
    def publish_if_changed(
        self,
        *,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None:
        raise AssertionError("unchanged intro snapshot must not be exported")


class RecordingExporter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def publish_if_changed(
        self,
        *,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None:
        self.calls.append(
            {
                "sha1": sha1,
                "chapters": [dict(chapter) for chapter in chapters],
                "file_name": file_name,
                "reason": reason,
                "cache_revision": cache_revision,
            }
        )


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyP115MediaInfoRepository:
    return SqlAlchemyP115MediaInfoRepository(runtime.unit_of_work)


def _formatted(chapters: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "MediaSourceInfo": {"MediaStreams": [], "Container": "mkv"},
            "Chapters": [dict(chapter) for chapter in chapters],
        }
    ]


def _insert_cache(runtime: DatabaseRuntime, *, chapters: Sequence[Mapping[str, Any]]) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (
                    sha1, mediainfo_json, raw_ffprobe_json,
                    formatter_revision, probe_revision
                ) VALUES (
                    :sha1, CAST(:formatted AS jsonb), CAST(:raw AS jsonb),
                    'p6-v1', 'ffprobe-v1'
                )
                """
            ),
            {
                "sha1": SHA_A,
                "formatted": json.dumps(_formatted(chapters), ensure_ascii=False),
                "raw": json.dumps({"fixture": True}),
            },
        )


def test_postgres18_formatted_cache_cas_advances_once_and_stale_revision_cannot_overwrite(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(p6_runtime, chapters=[])
    repository = _repository(p6_runtime)
    replacement = _formatted(INTRO_CHAPTERS)

    updated = repository.replace_formatted_if_revision(
        SHA_A,
        expected_cache_revision=0,
        formatted=replacement,
    )
    stale = repository.replace_formatted_if_revision(
        SHA_A,
        expected_cache_revision=0,
        formatted={"stale": True},
    )

    assert updated is not None
    assert updated.cache_revision == 1
    assert updated.formatted == replacement
    assert updated.raw == {"fixture": True}
    assert stale is None
    persisted = repository.find_by_sha1(SHA_A)
    assert persisted is not None
    assert persisted.cache_revision == 1
    assert persisted.formatted == replacement


def test_postgres18_intro_sync_unchanged_snapshot_does_not_advance_revision(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(p6_runtime, chapters=INTRO_CHAPTERS)
    repository = _repository(p6_runtime)
    service = P115IntroSyncService(
        repository,
        EnabledFeature(),
        VerifiedItem(),
        NoopExporter(),
        lambda: "115001",
    )

    result = service.sync_by_sha1(SHA_A, "1001")

    assert result == {"ok": True, "updated": False, "chapter_count": 2}
    persisted = repository.find_by_sha1(SHA_A)
    assert persisted is not None and persisted.cache_revision == 0


def test_postgres18_intro_sync_updates_cache_without_staging_bridge_effect_or_outbox(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(p6_runtime, chapters=[])
    exporter = RecordingExporter()
    service = P115IntroSyncService(
        repository=_repository(p6_runtime),
        feature=EnabledFeature(),
        verifier=VerifiedItem(),
        exporter=exporter,
        account_id=lambda: "115001",
    )

    result = service.sync_by_sha1(SHA_A, "1001")

    assert result == {"ok": True, "updated": True, "chapter_count": 2}
    assert len(exporter.calls) == 1
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0
        assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 0
        row = (
            connection.execute(
                text("SELECT cache_revision, mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": SHA_A},
            )
            .mappings()
            .one()
        )
    assert int(row["cache_revision"]) == 1
    assert row["mediainfo_json"][0]["Chapters"] == INTRO_CHAPTERS


def test_postgres18_native_intro_exporter_stages_one_durable_p9_effect_and_outbox(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(p6_runtime, chapters=[])
    service = P115IntroSyncService(
        repository=_repository(p6_runtime),
        feature=EnabledFeature(),
        verifier=VerifiedItem(),
        exporter=PersistentP115IntroSnapshotExporter(
            PostgresMediaEffectLedger(p6_runtime.unit_of_work),
        ),
        account_id=lambda: "115001",
    )

    result = service.sync_by_sha1(SHA_A, "1001")

    assert result == {"ok": True, "updated": True, "chapter_count": 2}
    with p6_runtime.engine.connect() as connection:
        effect = (
            connection.execute(
                text(
                    """
                    SELECT effect_type, aggregate_type, aggregate_id, payload_json, status
                    FROM media_effects
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
                    SELECT topic, aggregate_type, aggregate_id, status
                    FROM outbox_events
                    """
                )
            )
            .mappings()
            .one()
        )
    assert dict(effect) == {
        "effect_type": "sharing.intro.export",
        "aggregate_type": "media_asset",
        "aggregate_id": SHA_A,
        "payload_json": {
            "sha1": SHA_A,
            "chapters": INTRO_CHAPTERS,
            "file_name": "Fixture.mkv",
            "reason": "emby_item_updated",
        },
        "status": "pending",
    }
    assert outbox["topic"] == "media.effect.requested"
    assert outbox["aggregate_type"] == "media_effect"
    assert outbox["aggregate_id"]
    assert outbox["status"] == "pending"


def test_postgres18_formatted_cache_cas_returns_none_for_missing_row(
    p6_runtime: DatabaseRuntime,
) -> None:
    assert (
        _repository(p6_runtime).replace_formatted_if_revision(
            SHA_A,
            expected_cache_revision=0,
            formatted=_formatted(INTRO_CHAPTERS),
        )
        is None
    )
