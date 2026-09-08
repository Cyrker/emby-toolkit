from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping

import pytest
from sqlalchemy import text

from etk.domains.collection.cover_task_execution import (
    COVER_IMAGE_EFFECT_TYPE,
    PersistentCoverTaskEffects,
    SqlAlchemyCoverTargetRepository,
)
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    PersistentMediaEffectExecutor,
)
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus, PostgresMediaEffectLedger
from etk.platform.db import DatabaseRuntime


class _Delivery:
    effect_types = frozenset({COVER_IMAGE_EFFECT_TYPE})

    def __init__(self, *, indeterminate: bool = False) -> None:
        self.calls = 0
        self.indeterminate = indeterminate

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, object]:
        del effect
        self.calls += 1
        if self.indeterminate:
            raise MediaEffectDeliveryIndeterminate("fixture cover outcome is unknown")
        return {"readback": "confirmed"}


def _effects(runtime: DatabaseRuntime, delivery: _Delivery) -> PersistentCoverTaskEffects:
    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(runtime.unit_of_work),
        delivery,
        worker_id="cover-pg18",
        lease_seconds=30,
    )
    return PersistentCoverTaskEffects(executor)


def test_cover_effect_payload_is_durable_and_terminal_replay_does_not_redeliver(
    p8_runtime: DatabaseRuntime,
) -> None:
    delivery = _Delivery()
    effects = _effects(p8_runtime, delivery)
    first = effects.apply(
        effect_key="cover-pg18:once",
        target_id="emby-cover",
        target_name="First title",
        content=b"fixture-cover",
        cache_key="cache-1",
        output_root=None,
    )
    second = effects.apply(
        effect_key="cover-pg18:once",
        target_id="emby-cover",
        target_name="Changed title",
        content=b"different-cover",
        cache_key="cache-2",
        output_root=None,
    )

    assert first.status is MediaEffectStatus.CONFIRMED
    assert second.status is MediaEffectStatus.CONFIRMED
    assert delivery.calls == 1
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT effect_type, payload_json FROM media_effects WHERE effect_key = :key"),
                {"key": "cover-pg18:once"},
            )
            .mappings()
            .one()
        )
    assert row["effect_type"] == COVER_IMAGE_EFFECT_TYPE
    payload = row["payload_json"]
    assert payload["target_id"] == "emby-cover"
    assert payload["target_name"] == "First title"
    assert payload["cache_key"] == "cache-1"
    assert payload["byte_size"] == len(b"fixture-cover")
    assert payload["content_sha256"] == hashlib.sha256(b"fixture-cover").hexdigest()
    assert base64.b64decode(payload["content_b64"], validate=True) == b"fixture-cover"


def test_cover_effect_keeps_indeterminate_terminal_and_does_not_resend(
    p8_runtime: DatabaseRuntime,
) -> None:
    delivery = _Delivery(indeterminate=True)
    effects = _effects(p8_runtime, delivery)
    first = effects.apply(
        effect_key="cover-pg18:indeterminate",
        target_id="emby-cover",
        target_name="Cover",
        content=b"fixture-cover",
        cache_key="cache-1",
        output_root=None,
    )
    second = effects.apply(
        effect_key="cover-pg18:indeterminate",
        target_id="emby-cover",
        target_name="Changed cover",
        content=b"different-cover",
        cache_key="cache-2",
        output_root=None,
    )

    assert first.status is MediaEffectStatus.INDETERMINATE
    assert second.status is MediaEffectStatus.INDETERMINATE
    assert delivery.calls == 1


def test_cover_effect_rejects_oversized_payload_before_ledger_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    effects = _effects(p8_runtime, _Delivery())
    with pytest.raises(ValueError, match="too large"):
        effects.apply(
            effect_key="cover-pg18:too-large",
            target_id="emby-cover",
            target_name="Cover",
            content=b"x" * (8 * 1024 * 1024 + 1),
            cache_key="cache-1",
            output_root=None,
        )
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM media_effects WHERE effect_key = :key"),
                {"key": "cover-pg18:too-large"},
            )
            == 0
        )


def test_cover_target_repository_preserves_custom_emby_identity_and_poster_fallback(
    p8_runtime: DatabaseRuntime,
) -> None:
    definition = {"url": "https://www.douban.com/doulist/804", "item_type": ["Movie"]}
    generated = [{"tmdb_id": "cover-pg18-804", "emby_id": "emby-source-804"}]
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO app_settings (setting_key, value_json) "
                "VALUES ('cover_generator_config', CAST(:config AS jsonb))"
            ),
            {"config": json.dumps({"enabled": True, "cover_style": "multi_1"})},
        )
        connection.execute(
            text(
                """
                INSERT INTO custom_collections (
                    name, type, definition_json, status, emby_collection_id,
                    item_type, in_library_count, generated_media_info_json
                ) VALUES (
                    'Cover Fixture', 'list', CAST(:definition AS jsonb), 'active',
                    'emby-collection-804', 'Movie', 7, CAST(:generated AS jsonb)
                )
                """
            ),
            {"definition": json.dumps(definition), "generated": json.dumps(generated)},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (tmdb_id, item_type, title, poster_path)
                VALUES ('cover-pg18-804', 'Movie', 'Cover Source', '/poster-804.jpg')
                """
            )
        )

    repository = SqlAlchemyCoverTargetRepository(p8_runtime.unit_of_work, lambda: object())

    assert repository.config() == {"enabled": True, "cover_style": "multi_1"}
    assert repository.list_custom_targets() == (
        {
            "id": 1,
            "name": "Cover Fixture",
            "type": "list",
            "item_type": "Movie",
            "in_library_count": 7,
            "emby_collection_id": "emby-collection-804",
            "definition_json": definition,
            "generated_media_info_json": generated,
            "tmdb_poster_paths": ["/poster-804.jpg"],
        },
    )
