from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.identity.user_activity_effects import (
    USER_EVENT_EFFECT_TYPES,
    LegacyUserEventEffectsAdapter,
    P8UserEventEffectDelivery,
    UserPolicyEffectService,
)
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus

EFFECT_ID = UUID("018f24aa-5be2-7f3a-b909-3f8d5e2f8f10")


class FakePolicy:
    def __init__(self) -> None:
        self.refresh_calls: list[str] = []
        self.sync_calls: list[tuple[str, str]] = []
        self.error: BaseException | None = None

    def refresh_user_policy(self, user_id: str) -> Mapping[str, Any]:
        self.refresh_calls.append(user_id)
        if self.error is not None:
            raise self.error
        return {"updated": True}

    def sync_template_policy(self, user_id: str, *, source_event_key: str) -> Mapping[str, Any]:
        self.sync_calls.append((user_id, source_event_key))
        return {"operations": ["operation-1"]}


class FakeCompatibility:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, kind: str, snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
        self.calls.append((kind, dict(snapshot)))
        return {"completed": True, "kind": kind}


def test_native_event_retry_budget_does_not_repeat_forever():
    from unittest.mock import Mock

    compatibility = Mock()
    compatibility.execute.side_effect = MediaEffectDeliveryRetryable("temporary failure")
    delivery = P8UserEventEffectDelivery(cast(Any, FakePolicy()), compatibility)
    with pytest.raises(MediaEffectDeliveryRetryable):
        delivery.deliver(_effect("emby.event.user.halfway-intro-detection", {"event_snapshot": {}}, attempts=1))
    with pytest.raises(MediaEffectDeliveryIndeterminate, match="重试次数已耗尽"):
        delivery.deliver(_effect("emby.event.user.halfway-intro-detection", {"event_snapshot": {}}, attempts=12))


def _effect(
    effect_type: str,
    payload: Mapping[str, Any],
    *,
    attempts: int = 1,
) -> MediaEffectProjection:
    now = datetime.now(UTC)
    return MediaEffectProjection(
        id=EFFECT_ID,
        effect_key=f"fixture:{effect_type}",
        effect_type=effect_type,
        aggregate_type="emby_user_event_effect",
        aggregate_id="fixture",
        payload_version=1,
        payload=dict(payload),
        request_hash=b"a" * 32,
        status=MediaEffectStatus.CLAIMED,
        attempts=attempts,
        available_at=now,
        claimed_by="fixture",
        claim_token=EFFECT_ID,
        claimed_at=now,
        lease_expires_at=now,
        last_attempt_at=now,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


def test_delivery_registers_and_routes_all_eight_user_event_effects() -> None:
    policy = FakePolicy()
    compatibility = FakeCompatibility()
    delivery = P8UserEventEffectDelivery(
        cast(UserPolicyEffectService, policy),
        cast(LegacyUserEventEffectsAdapter, compatibility),
    )

    refresh = delivery.deliver(_effect("emby.event.user.refresh-user-policy", {"user_id": "user-1"}))
    sync = delivery.deliver(
        _effect(
            "emby.event.user.sync-template-policy",
            {"updated_user_id": "user-1", "source_event_key": "event-key"},
        )
    )
    compatibility_types = sorted(
        USER_EVENT_EFFECT_TYPES
        - {
            "emby.event.user.refresh-user-policy",
            "emby.event.user.sync-template-policy",
        }
    )
    for effect_type in compatibility_types:
        result = delivery.deliver(
            _effect(
                effect_type,
                {
                    "event_snapshot": {
                        "Event": "playback.stop",
                        "UserId": "user-1",
                        "ItemId": "item-1",
                    }
                },
            )
        )
        assert result["completed"] is True

    assert delivery.effect_types == USER_EVENT_EFFECT_TYPES
    assert refresh == {"updated": True}
    assert sync == {"operations": ["operation-1"]}
    assert policy.refresh_calls == ["user-1"]
    assert policy.sync_calls == [("user-1", "event-key")]
    assert [kind for kind, _snapshot in compatibility.calls] == [
        effect_type.removeprefix("emby.event.user.") for effect_type in compatibility_types
    ]


def test_delivery_rejects_missing_snapshot_before_compatibility_call() -> None:
    compatibility = FakeCompatibility()
    delivery = P8UserEventEffectDelivery(
        cast(UserPolicyEffectService, FakePolicy()),
        cast(LegacyUserEventEffectsAdapter, compatibility),
    )

    with pytest.raises(ValueError, match="snapshot"):
        delivery.deliver(_effect("emby.event.user.playback-cleanup", {}))

    assert compatibility.calls == []


def test_policy_delivery_retries_prewrite_failure_then_becomes_indeterminate_at_ceiling() -> None:
    policy = FakePolicy()
    policy.error = RuntimeError("fixture provider unavailable")
    delivery = P8UserEventEffectDelivery(
        cast(UserPolicyEffectService, policy),
        cast(LegacyUserEventEffectsAdapter, FakeCompatibility()),
    )
    payload = {"user_id": "user-1"}

    with pytest.raises(MediaEffectDeliveryRetryable):
        delivery.deliver(_effect("emby.event.user.refresh-user-policy", payload, attempts=1))
    with pytest.raises(MediaEffectDeliveryIndeterminate):
        delivery.deliver(_effect("emby.event.user.refresh-user-policy", payload, attempts=12))
