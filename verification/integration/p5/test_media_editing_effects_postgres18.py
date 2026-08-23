from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from etk.domains.media.bridge_client import (
    BridgeWriteIndeterminate,
    BridgeWriteResult,
)
from etk.domains.media.editing import (
    MediaCommandStatus,
    prepare_chapter_update,
)
from etk.domains.media.editing_effects import PersistentMediaInfoCommandGateway
from etk.domains.media.repository import (
    MediaIdentityConflictError,
    SqlAlchemyMediaRepository,
)
from etk.platform.db import DatabaseRuntime

SHA_A = "A" * 40


class FakeBridge:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.indeterminate = False

    def authorization_revision(self) -> str:
        return "a" * 64

    def apply_mediainfo(
        self,
        item_id: str,
        raw: Any,
        *,
        drop_conflicting_external_streams: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult:
        assert expected_authorization_revision == self.authorization_revision()
        self.calls.append(("mediainfo", item_id, raw))
        if self.indeterminate:
            raise BridgeWriteIndeterminate("apply-mediainfo", "fixture response lost")
        return BridgeWriteResult(
            "apply-mediainfo",
            200,
            {"Applied": True, "Drop": drop_conflicting_external_streams},
        )

    def update_chapters(
        self,
        item_id: str,
        *,
        intro_start_ticks: int | None = None,
        intro_end_ticks: int | None = None,
        credits_start_ticks: int | None = None,
        clear_intro: bool = False,
        clear_credits: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult:
        assert expected_authorization_revision == self.authorization_revision()
        payload = {
            "IntroStartTicks": intro_start_ticks,
            "IntroEndTicks": intro_end_ticks,
            "CreditsStartTicks": credits_start_ticks,
            "ClearIntro": clear_intro,
            "ClearCredits": clear_credits,
        }
        self.calls.append(("chapters", item_id, payload))
        return BridgeWriteResult("update-chapters", 200, payload)


def _seed(runtime: DatabaseRuntime) -> None:
    mediainfo = {
        "MediaSourceInfo": {
            "RunTimeTicks": 600_000_000,
            "MediaStreams": [{"Type": "Video", "Codec": "h264"}],
        },
        "Chapters": [],
    }
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, in_library,
                    emby_item_ids_json, asset_details_json, file_sha1_json,
                    file_pickcode_json
                ) VALUES (
                    '900', 'Movie', 'Effect Fixture', 'Effect Fixture', TRUE,
                    '["item-900"]'::jsonb,
                    '[{"path":"/library/effect.mkv","emby_item_id":"item-900"}]'::jsonb,
                    CAST(:sha1s AS jsonb), '["pc-900"]'::jsonb
                )
                """
            ),
            {"sha1s": json.dumps([SHA_A])},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                VALUES (:sha1, CAST(:mediainfo AS jsonb))
                """
            ),
            {"sha1": SHA_A, "mediainfo": json.dumps(mediainfo)},
        )


def test_pg18_mediainfo_write_effect_is_atomic_confirmed_and_idempotent(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed(p5_runtime)
    repository = SqlAlchemyMediaRepository(p5_runtime.unit_of_work)
    context = repository.resolve_media_info_context("item-900")
    bridge = FakeBridge()
    gateway = PersistentMediaInfoCommandGateway(
        p5_runtime.unit_of_work,
        bridge,
        worker_id="fixture-api",
    )
    desired = {
        "MediaSourceInfo": {
            "RunTimeTicks": 700_000_000,
            "MediaStreams": [{"Type": "Video", "Codec": "hevc"}],
        },
        "Chapters": [],
    }

    first = gateway.apply_mediainfo(context, desired)
    second = gateway.apply_mediainfo(context, desired)

    assert first.status is MediaCommandStatus.CONFIRMED
    assert second.status is MediaCommandStatus.CONFIRMED
    assert first.effect_id == second.effect_id
    assert len(bridge.calls) == 1
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert connection.scalar(text("SELECT status FROM media_effects")) == "confirmed"
        assert (
            connection.scalar(text("SELECT status FROM outbox_events WHERE topic = 'media.effect.requested'"))
            == "processed"
        )
        assert (
            connection.scalar(
                text("SELECT mediainfo_json->'MediaSourceInfo'->>'RunTimeTicks' FROM p115_mediainfo_cache")
            )
            == "700000000"
        )


def test_pg18_unknown_bridge_outcome_is_persisted_and_never_blindly_replayed(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed(p5_runtime)
    context = SqlAlchemyMediaRepository(p5_runtime.unit_of_work).resolve_media_info_context("item-900")
    bridge = FakeBridge()
    bridge.indeterminate = True
    gateway = PersistentMediaInfoCommandGateway(
        p5_runtime.unit_of_work,
        bridge,
        worker_id="fixture-api",
    )
    desired = {
        "MediaSourceInfo": {"MediaStreams": [{"Type": "Video"}]},
        "Chapters": [],
    }

    first = gateway.apply_mediainfo(context, desired)
    second = gateway.apply_mediainfo(context, desired)

    assert first.status is MediaCommandStatus.INDETERMINATE
    assert second.status is MediaCommandStatus.INDETERMINATE
    assert first.effect_id == second.effect_id
    assert len(bridge.calls) == 1
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT status FROM media_effects")) == "indeterminate"
        assert (
            connection.scalar(text("SELECT status FROM outbox_events WHERE topic = 'media.effect.requested'"))
            == "processed"
        )


def test_pg18_chapter_write_uses_same_atomic_effect_boundary(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed(p5_runtime)
    context = SqlAlchemyMediaRepository(p5_runtime.unit_of_work).resolve_media_info_context("item-900")
    update = prepare_chapter_update(
        context,
        {
            "sha1": SHA_A,
            "intro_enabled": True,
            "intro_start_seconds": 1,
            "intro_end_seconds": 5,
            "credits_enabled": True,
            "credits_start_seconds": 55,
        },
    )
    bridge = FakeBridge()
    outcome = PersistentMediaInfoCommandGateway(
        p5_runtime.unit_of_work,
        bridge,
        worker_id="fixture-api",
    ).apply_chapters(context, update)

    assert outcome.status is MediaCommandStatus.CONFIRMED
    assert bridge.calls[0][0] == "chapters"
    with p5_runtime.engine.connect() as connection:
        marker_types = connection.scalar(
            text(
                """
                SELECT jsonb_path_query_array(mediainfo_json, '$.Chapters[*].MarkerType')
                FROM p115_mediainfo_cache
                """
            )
        )
    assert marker_types == ["IntroStart", "IntroEnd", "CreditsStart"]


def test_pg18_stale_item_binding_rolls_back_local_write_and_effect(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed(p5_runtime)
    repository = SqlAlchemyMediaRepository(p5_runtime.unit_of_work)
    context = repository.resolve_media_info_context("item-900")
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET file_sha1_json = CAST(:sha1s AS jsonb)
                WHERE tmdb_id = '900' AND item_type = 'Movie'
                """
            ),
            {"sha1s": json.dumps(["B" * 40])},
        )
    desired = {
        "MediaSourceInfo": {"MediaStreams": [{"Type": "Video", "Codec": "hevc"}]},
        "Chapters": [],
    }

    gateway = PersistentMediaInfoCommandGateway(
        p5_runtime.unit_of_work,
        FakeBridge(),
        worker_id="fixture-api",
    )
    try:
        gateway.apply_mediainfo(context, desired)
    except MediaIdentityConflictError:
        pass
    else:
        raise AssertionError("stale binding must fail closed")

    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0
        assert connection.scalar(text("SELECT count(*) FROM outbox_events WHERE topic = 'media.effect.requested'")) == 0
        assert (
            connection.scalar(
                text("SELECT mediainfo_json->'MediaSourceInfo'->'MediaStreams'->0->>'Codec' FROM p115_mediainfo_cache")
            )
            == "h264"
        )
