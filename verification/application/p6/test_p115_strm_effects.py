from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import SecretStr

from etk.domains.media.bridge_client import emby_authorization_revision
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
)
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus
from etk.domains.p115.emby_path_gateway import EmbyPathChangeRejected
from etk.domains.p115.strm_effects import (
    EMBY_PATHS_CHANGED_EFFECT,
    EmbyPathChangeEffectDelivery,
)
from etk.platform import Settings


class FakeGateway:
    def __init__(self, *, result: bool = True, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def notify(
        self,
        paths: Sequence[str],
        *,
        base_url: str,
        api_key: str,
        update_type: str,
    ) -> bool:
        self.calls.append(
            {
                "paths": tuple(paths),
                "base_url": base_url,
                "api_key": api_key,
                "update_type": update_type,
            }
        )
        if self.error is not None:
            raise self.error
        return self.result


def _settings(*, api_key: str = "fixture-token") -> Settings:
    return Settings(
        emby_server_url="http://emby.fixture.invalid/",
        emby_api_key=SecretStr(api_key),
    )


def _effect(settings: Settings, **payload_values: Any) -> MediaEffectProjection:
    now = datetime.now(UTC)
    payload: dict[str, Any] = {
        "paths": ["/library/Movie.strm"],
        "update_type": "Modified",
        "emby_authorization_revision": emby_authorization_revision(settings),
        "operation_revision": "a" * 64,
        **payload_values,
    }
    return MediaEffectProjection(
        id=uuid4(),
        effect_key="p6:fixture-path-change",
        effect_type=EMBY_PATHS_CHANGED_EFFECT,
        aggregate_type="p115_strm_sync",
        aggregate_id="115001",
        payload_version=1,
        payload=payload,
        request_hash=b"x" * 32,
        status=MediaEffectStatus.CLAIMED,
        attempts=1,
        available_at=now,
        claimed_by="fixture-worker",
        claim_token=uuid4(),
        claimed_at=now,
        lease_expires_at=now,
        last_attempt_at=now,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


def test_path_change_delivery_confirms_only_an_explicit_gateway_success() -> None:
    settings = _settings()
    gateway = FakeGateway()
    delivery = EmbyPathChangeEffectDelivery(lambda: settings, gateway)

    result = delivery.deliver(_effect(settings))

    assert result == {
        "operation": "library-paths-changed",
        "path_count": 1,
        "update_type": "Modified",
    }
    assert gateway.calls == [
        {
            "paths": ("/library/Movie.strm",),
            "base_url": "http://emby.fixture.invalid",
            "api_key": "fixture-token",
            "update_type": "Modified",
        }
    ]


@pytest.mark.parametrize(
    "gateway",
    [
        FakeGateway(result=False),
        FakeGateway(error=RuntimeError("fixture transport failure")),
    ],
)
def test_path_change_delivery_maps_unconfirmed_or_transport_outcome_to_indeterminate(
    gateway: FakeGateway,
) -> None:
    settings = _settings()
    delivery = EmbyPathChangeEffectDelivery(lambda: settings, gateway)

    with pytest.raises(MediaEffectDeliveryIndeterminate):
        delivery.deliver(_effect(settings))


def test_path_change_delivery_maps_deterministic_http_rejection_to_rejected() -> None:
    settings = _settings()
    gateway = FakeGateway(error=EmbyPathChangeRejected("fixture HTTP 403"))
    delivery = EmbyPathChangeEffectDelivery(lambda: settings, gateway)

    with pytest.raises(MediaEffectDeliveryRejected, match="was rejected"):
        delivery.deliver(_effect(settings))


def test_path_change_delivery_rejects_authorization_drift_before_gateway_call() -> None:
    staged_settings = _settings(api_key="staged-token")
    current_settings = _settings(api_key="changed-token")
    gateway = FakeGateway()
    delivery = EmbyPathChangeEffectDelivery(lambda: current_settings, gateway)

    with pytest.raises(MediaEffectDeliveryRejected, match="authorization changed"):
        delivery.deliver(_effect(staged_settings))

    assert gateway.calls == []
