from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from etk.domains.subscription.operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionQuotaRequirement,
    safe_durable_mapping,
)


def test_operation_intent_is_deterministic_and_canonicalizes_request_order() -> None:
    now = datetime(2026, 8, 26, tzinfo=UTC)
    first = SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.CREATE,
        scope_key="series:101:season:2",
        request={"season": 2, "tmdbid": 101, "name": "示例剧集"},
        available_at=now,
    )
    second = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:101:season:2",
        request={"name": "示例剧集", "tmdbid": 101, "season": 2},
        available_at=now,
    )

    assert first == second
    assert len(first.operation_key) == 64
    assert len(first.request_hash) == 32
    assert first.id == second.id


def test_explicit_idempotency_key_keeps_identity_stable_when_snapshot_request_drifts() -> None:
    first = SubscriptionOperationIntent.create(
        kind="reconcile",
        scope_key="episode-residue:101:2:history:7",
        request={"record": {"id": 7, "name": "first"}},
        idempotency_key="episode-residue:101:2:history:7",
    )
    second = SubscriptionOperationIntent.create(
        kind="reconcile",
        scope_key="episode-residue:101:2:history:7",
        request={"record": {"id": 7, "name": "changed"}},
        idempotency_key="episode-residue:101:2:history:7",
    )

    assert first.id == second.id
    assert first.operation_key == second.operation_key
    assert first.request_hash != second.request_hash


@pytest.mark.parametrize(
    "payload",
    [
        {"password": "fixture"},
        {"nested": {"access_token": "fixture"}},
        {"items": [{"cookie": "fixture"}]},
    ],
)
def test_operation_intent_rejects_secret_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="secret field"):
        SubscriptionOperationIntent.create(
            kind="create",
            scope_key="fixture",
            request=payload,
        )


def test_operation_intent_rejects_non_json_or_unstable_inputs() -> None:
    with pytest.raises(ValueError, match="finite"):
        SubscriptionOperationIntent.create(
            kind="update",
            scope_key="fixture",
            request={"rating": float("nan")},
        )
    with pytest.raises(ValueError, match="unsupported"):
        SubscriptionOperationIntent.create(
            kind="update",
            scope_key="fixture",
            request={"value": object()},
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        SubscriptionOperationIntent.create(
            kind="update",
            scope_key="fixture",
            request={},
            available_at=datetime(2026, 8, 26),
        )


def test_quota_requirement_validates_cap_units_and_ttl() -> None:
    requirement = SubscriptionQuotaRequirement(
        date(2026, 8, 26),
        daily_cap=200,
        units=1,
        ttl_seconds=300,
    )

    assert requirement.daily_cap == 200
    with pytest.raises(ValueError, match="daily_cap"):
        SubscriptionQuotaRequirement(date(2026, 8, 26), daily_cap=-1)
    with pytest.raises(ValueError, match="positive"):
        SubscriptionQuotaRequirement(date(2026, 8, 26), daily_cap=1, units=0)
    with pytest.raises(ValueError, match="finite"):
        SubscriptionQuotaRequirement(date(2026, 8, 26), daily_cap=1, ttl_seconds=float("inf"))


@pytest.mark.parametrize(
    "payload",
    [
        {"Authorization": "fixture"},
        {"headers": {"Set-Cookie": "fixture"}},
        {"provider": {"refresh_token": "fixture"}},
        {"items": [{"moviepilot_api_key": "fixture"}]},
    ],
)
def test_all_durable_provider_payloads_reject_secret_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="cannot persist secret field"):
        safe_durable_mapping(payload, field="fixture provider payload")


def test_durable_provider_payload_is_detached_and_json_normalized() -> None:
    payload = {"provider_id": 42, "nested": {"status": "ok"}}

    normalized = safe_durable_mapping(payload, field="fixture provider payload")
    payload["nested"] = {"status": "changed"}

    assert normalized == {"provider_id": 42, "nested": {"status": "ok"}}
