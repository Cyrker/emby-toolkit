from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

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
from etk.domains.media.models import MediaInfoEditContext
from etk.domains.media.repository import (
    MediaIdentityConflictError,
    SqlAlchemyMediaRepository,
)
from etk.platform.db import DatabaseRuntime

SHA_A = "A" * 40
PROVENANCE_A = "a" * 64


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


def _seed_generated_probe(
    runtime: DatabaseRuntime,
    *,
    expected_cache_revision: int | None,
) -> tuple[str, str]:
    item_id = str(uuid4())
    claim_token = str(uuid4())
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json)
                VALUES ('p115_auth_tokens', '{"account_id":"115001"}'::jsonb)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_media_probe_items (
                    id, operation_key, provenance_key, source, account_id, sha1, pick_code,
                    file_name, status, attempts, claim_token, lease_expires_at,
                    expected_cache_revision
                ) VALUES (
                    CAST(:id AS uuid), 'fixture-generated-operation', :provenance_key, 'restore_mediainfo',
                    '115001', :sha1, 'pc-900', 'effect.mkv', 'probing', 1,
                    CAST(:claim_token AS uuid), NOW() + INTERVAL '2 minutes',
                    :expected_cache_revision
                )
                """
            ),
            {
                "id": item_id,
                "provenance_key": PROVENANCE_A,
                "sha1": SHA_A,
                "claim_token": claim_token,
                "expected_cache_revision": expected_cache_revision,
            },
        )
    return item_id, claim_token


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


def test_pg18_generated_mediainfo_atomically_inserts_cache_confirms_work_and_reserves_effect(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed(p5_runtime)
    with p5_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM p115_mediainfo_cache WHERE sha1 = :sha1"), {"sha1": SHA_A})
    probe_item_id, claim_token = _seed_generated_probe(
        p5_runtime,
        expected_cache_revision=None,
    )
    desired = {
        "MediaSourceInfo": {
            "RunTimeTicks": 800_000_000,
            "MediaStreams": [{"Type": "Video", "Codec": "av1"}],
        },
        "Chapters": [],
    }
    raw = {
        "streams": [{"codec_type": "video", "codec_name": "av1", "width": 3840, "height": 2160}],
        "format": {"duration": "80.0"},
        "_etk": {"tmdb_id": "900", "type": "Movie", "sha1": SHA_A},
    }
    context = MediaInfoEditContext(
        item_id="item-900",
        sha1=SHA_A,
        mediainfo=desired,
        title="Effect Fixture",
        item_type="Movie",
    )
    bridge = FakeBridge()
    gateway = PersistentMediaInfoCommandGateway(
        p5_runtime.unit_of_work,
        bridge,
        worker_id="fixture-generated",
    )

    outcome = gateway.apply_generated_mediainfo(
        context,
        desired,
        raw,
        probe_item_id=probe_item_id,
        probe_claim_token=claim_token,
        account_id="115001",
        provider_backend="openapi",
        formatter_revision="formatter-v1",
        probe_revision="ffprobe-v1",
        probe_result={"probe_method": "ffprobe"},
    )

    assert outcome.status is MediaCommandStatus.CONFIRMED
    assert len(bridge.calls) == 1
    with p5_runtime.engine.connect() as connection:
        cache = (
            connection.execute(
                text(
                    """
                    SELECT mediainfo_json, raw_ffprobe_json, cache_revision,
                           raw_schema_version, formatter_revision, probe_revision
                    FROM p115_mediainfo_cache
                    WHERE sha1 = :sha1
                    """
                ),
                {"sha1": SHA_A},
            )
            .mappings()
            .one()
        )
        work = (
            connection.execute(
                text(
                    """
                    SELECT status, claim_token, lease_expires_at, provider_backend,
                           result_json, finished_at
                    FROM p115_media_probe_items
                    WHERE id = CAST(:id AS uuid)
                    """
                ),
                {"id": probe_item_id},
            )
            .mappings()
            .one()
        )
        effect_count = connection.scalar(text("SELECT count(*) FROM media_effects"))
        outbox_status = connection.scalar(
            text("SELECT status FROM outbox_events WHERE topic = 'media.effect.requested'")
        )
    assert cache["mediainfo_json"] == desired
    assert cache["raw_ffprobe_json"] == raw
    assert cache["cache_revision"] == 1
    assert cache["raw_schema_version"] == 1
    assert cache["formatter_revision"] == "formatter-v1"
    assert cache["probe_revision"] == "ffprobe-v1"
    assert work["status"] == "confirmed"
    assert work["claim_token"] is None
    assert work["lease_expires_at"] is None
    assert work["provider_backend"] == "openapi"
    assert work["result_json"] == {
        "probe_method": "ffprobe",
        "effect_id": outcome.effect_id,
        "cache_revision": 1,
        "source": "restore_mediainfo",
    }
    assert work["finished_at"] is not None
    assert effect_count == 1
    assert outbox_status == "processed"


def test_pg18_generated_mediainfo_cache_revision_conflict_rolls_back_work_and_effect(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed(p5_runtime)
    probe_item_id, claim_token = _seed_generated_probe(
        p5_runtime,
        expected_cache_revision=0,
    )
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE p115_mediainfo_cache
                SET cache_revision = 1,
                    mediainfo_json = CAST(:mediainfo AS jsonb)
                WHERE sha1 = :sha1
                """
            ),
            {"sha1": SHA_A, "mediainfo": json.dumps({"preserved": True})},
        )
    desired = {"MediaSourceInfo": {"MediaStreams": [{"Type": "Video"}]}, "Chapters": []}
    context = MediaInfoEditContext(
        item_id="item-900",
        sha1=SHA_A,
        mediainfo=desired,
        title="Effect Fixture",
        item_type="Movie",
    )
    gateway = PersistentMediaInfoCommandGateway(
        p5_runtime.unit_of_work,
        FakeBridge(),
        worker_id="fixture-generated",
    )

    try:
        gateway.apply_generated_mediainfo(
            context,
            desired,
            {"streams": [{"codec_type": "video"}]},
            probe_item_id=probe_item_id,
            probe_claim_token=claim_token,
            account_id="115001",
            provider_backend="openapi",
            formatter_revision="formatter-v1",
            probe_revision="ffprobe-v1",
        )
    except MediaIdentityConflictError:
        pass
    else:
        raise AssertionError("generated MediaInfo cache revision drift must fail closed")

    with p5_runtime.engine.connect() as connection:
        cache = (
            connection.execute(
                text("SELECT mediainfo_json, cache_revision FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": SHA_A},
            )
            .mappings()
            .one()
        )
        work = (
            connection.execute(
                text(
                    "SELECT status, claim_token, finished_at FROM p115_media_probe_items WHERE id = CAST(:id AS uuid)"
                ),
                {"id": probe_item_id},
            )
            .mappings()
            .one()
        )
        effect_count = connection.scalar(text("SELECT count(*) FROM media_effects"))
        outbox_count = connection.scalar(
            text("SELECT count(*) FROM outbox_events WHERE topic = 'media.effect.requested'")
        )
    assert dict(cache) == {"mediainfo_json": {"preserved": True}, "cache_revision": 1}
    assert work["status"] == "probing"
    assert str(work["claim_token"]) == claim_token
    assert work["finished_at"] is None
    assert effect_count == 0
    assert outbox_count == 0
