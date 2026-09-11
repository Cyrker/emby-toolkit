from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus
from etk.domains.sharing.intro_effects import (
    SHARED_INTRO_EXPORT_EFFECT,
    SharedIntroExportEffectDelivery,
)
from etk.integrations import ProviderDisposition, ProviderResponse


class _Settings:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled

    def load(self, key: str) -> dict[str, bool]:
        assert key == "shared_resource_config"
        return {
            "p115_shared_resource_enabled": self.enabled,
            "p115_shared_intro_enabled": self.enabled,
        }


class _Center:
    def __init__(self, response: ProviderResponse[dict[str, Any]]) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def upload_intro_chapters(
        self,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        *,
        file_name: str = "",
        reason: str = "",
    ) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(
            {
                "sha1": sha1,
                "chapters": chapters,
                "file_name": file_name,
                "reason": reason,
            }
        )
        return self.response


def _effect(*, start_ticks: int = 0, end_ticks: int = 180_000_000) -> MediaEffectProjection:
    now = datetime.now(UTC)
    return MediaEffectProjection(
        id=uuid4(),
        effect_key="p9:sharing.intro.export:fixture",
        effect_type=SHARED_INTRO_EXPORT_EFFECT,
        aggregate_type="media_asset",
        aggregate_id="A" * 40,
        payload_version=1,
        payload={
            "sha1": "A" * 40,
            "chapters": [
                {
                    "StartPositionTicks": start_ticks,
                    "Name": "片头",
                    "MarkerType": "IntroStart",
                    "ChapterIndex": 0,
                },
                {
                    "StartPositionTicks": end_ticks,
                    "Name": "片头结束",
                    "MarkerType": "IntroEnd",
                    "ChapterIndex": 1,
                },
            ],
            "file_name": "",
            "reason": "etk_intro_detection",
        },
        request_hash=b"a" * 32,
        status=MediaEffectStatus.PENDING,
        attempts=0,
        available_at=now,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        last_attempt_at=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


def test_disabled_shared_intro_confirms_a_local_skip_without_provider_write() -> None:
    center = _Center(ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True}))
    delivery = SharedIntroExportEffectDelivery(_Settings(enabled=False), center)

    result = delivery.deliver(_effect())

    assert result["skipped"] is True
    assert result["reason"] == "feature_retired"
    assert center.calls == []


def test_enabled_historical_config_cannot_reactivate_intro_export() -> None:
    center = _Center(ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True}))
    delivery = SharedIntroExportEffectDelivery(_Settings(), center)

    result = delivery.deliver(_effect())

    assert result == {
        "operation": "shared-intro-export",
        "sha1": "A" * 40,
        "skipped": True,
        "reason": "feature_retired",
    }
    assert center.calls == []


def test_provider_response_is_never_observed_after_retirement() -> None:
    retryable_center = _Center(ProviderResponse(ProviderDisposition.RETRYABLE, 503, None, 7))
    malformed_center = _Center(ProviderResponse(ProviderDisposition.CONFIRMED, 200, None))
    retryable = SharedIntroExportEffectDelivery(
        _Settings(),
        retryable_center,
    )
    malformed = SharedIntroExportEffectDelivery(
        _Settings(),
        malformed_center,
    )

    assert retryable.deliver(_effect())["reason"] == "feature_retired"
    assert malformed.deliver(_effect())["reason"] == "feature_retired"
    assert retryable_center.calls == []
    assert malformed_center.calls == []


@pytest.mark.parametrize(("start_ticks", "end_ticks"), [(0, 0), (20, 10)])
def test_historical_intro_payload_is_skipped_without_validation_or_provider_write(
    start_ticks: int,
    end_ticks: int,
) -> None:
    center = _Center(ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True}))

    result = SharedIntroExportEffectDelivery(_Settings(), center).deliver(
        _effect(start_ticks=start_ticks, end_ticks=end_ticks)
    )

    assert result["reason"] == "feature_retired"
    assert center.calls == []
