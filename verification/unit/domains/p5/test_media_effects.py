from __future__ import annotations

import ast
import hashlib
import math
from pathlib import Path
from typing import Any

import pytest

from etk.domains.media.effects import (
    MEDIA_EFFECT_TOPIC,
    MediaEffectStatus,
    PostgresMediaEffectLedger,
    media_effect_event_key,
    media_effect_request_hash,
)
from etk.platform.jobs import InvalidJsonPayloadError

REPO_ROOT = Path(__file__).resolve().parents[4]
MIGRATION = REPO_ROOT / "migrations" / "versions" / "0003_p5_media_effects.py"


def test_p5_media_effect_migration_extends_p4_and_owns_its_outbox_cleanup() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignments = {
        node.target.id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None
    }

    assert assignments["revision"] == "0003_p5_media_effects"
    assert assignments["down_revision"] == "0002_p4_persistent_jobs"
    assert 'op.create_table(\n        "media_effects"' in source
    assert "uq_media_effects_effect_key" in source
    assert "ck_media_effects_claim" in source
    assert "ix_media_effects_claimable" in source
    assert "ix_media_effects_claim_recovery" in source
    assert "DELETE FROM outbox_events WHERE topic = 'media.effect.requested'" in source


def test_media_effect_event_key_is_stable_and_does_not_expose_the_business_key() -> None:
    effect_key = "emby.metadata:Movie:42:revision-7"
    expected_digest = hashlib.sha256(effect_key.encode("utf-8")).hexdigest()

    assert media_effect_event_key(effect_key) == f"{MEDIA_EFFECT_TOPIC}:{expected_digest}"
    assert media_effect_event_key(f"  {effect_key}  ") == f"{MEDIA_EFFECT_TOPIC}:{expected_digest}"
    assert effect_key not in media_effect_event_key(effect_key)


def test_media_effect_request_hash_is_canonical_and_covers_effect_semantics() -> None:
    request = {
        "effect_type": "emby.metadata.write",
        "aggregate_type": "media",
        "aggregate_id": "Movie:42",
        "payload": {"z": [3, 2, 1], "nested": {"name": "测试", "enabled": True}},
    }
    reordered = {
        **request,
        "payload": {"nested": {"enabled": True, "name": "测试"}, "z": [3, 2, 1]},
    }

    digest = media_effect_request_hash(**request)

    assert len(digest) == 32
    assert digest == media_effect_request_hash(**reordered)
    assert digest != media_effect_request_hash(**{**request, "effect_type": "emby.image.write"})
    assert digest != media_effect_request_hash(**{**request, "aggregate_id": "Movie:43"})
    assert digest != media_effect_request_hash(**{**request, "payload": {"z": [1, 2, 3]}})


@pytest.mark.parametrize(
    "payload",
    [
        {"callable": lambda: None},
        {"tuple": (1, 2)},
        {"nan": math.nan},
        {"inf": math.inf},
        {1: "non-string-key"},
    ],
)
def test_media_effect_request_hash_rejects_non_json_values(payload: Any) -> None:
    with pytest.raises(InvalidJsonPayloadError):
        media_effect_request_hash(
            effect_type="emby.metadata.write",
            aggregate_type="media",
            aggregate_id="Movie:42",
            payload=payload,
        )


def test_ledger_rejects_invalid_reservations_before_opening_a_uow() -> None:
    def forbidden_uow() -> Any:
        raise AssertionError("database must not be touched for an invalid reservation")

    ledger = PostgresMediaEffectLedger(forbidden_uow)

    with pytest.raises(InvalidJsonPayloadError):
        ledger.reserve(
            effect_key="fixture",
            effect_type="emby.metadata.write",
            aggregate_type="media",
            aggregate_id="Movie:42",
            payload={"nan": math.nan},
        )
    with pytest.raises(ValueError, match="effect_key"):
        ledger.reserve(
            effect_key=" ",
            effect_type="emby.metadata.write",
            aggregate_type="media",
            aggregate_id="Movie:42",
            payload={},
        )
    with pytest.raises(ValueError, match="max_publish_attempts"):
        ledger.reserve(
            effect_key="fixture",
            effect_type="emby.metadata.write",
            aggregate_type="media",
            aggregate_id="Movie:42",
            payload={},
            max_publish_attempts=0,
        )


def test_media_effect_statuses_keep_indeterminate_terminal() -> None:
    assert MediaEffectStatus.CONFIRMED.terminal
    assert MediaEffectStatus.REJECTED.terminal
    assert MediaEffectStatus.INDETERMINATE.terminal
    assert MediaEffectStatus.RETRYABLE.terminal is False
