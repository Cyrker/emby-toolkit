from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.identity.post_authorization import identity_authorization_revision
from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate, MediaEffectDeliveryRejected
from etk.domains.media.effects import MediaEffectProjection
from etk.domains.media.final_refresh_effects import (
    FINAL_METADATA_REFRESH_EFFECT,
    EmbyMetadataRefreshEffectDelivery,
)
from etk.platform import Settings


def _effect(**payload: Any) -> MediaEffectProjection:
    return cast(
        MediaEffectProjection,
        SimpleNamespace(
            effect_type=FINAL_METADATA_REFRESH_EFFECT,
            payload={
                "item_id": "movie/1",
                "item_type": "Movie",
                "replace_all_metadata": True,
                "replace_all_images": False,
                "metadata_only": False,
                "emby_authorization_revision": identity_authorization_revision(_settings()),
                **payload,
            },
        ),
    )


def _settings() -> Settings:
    return Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-user",
    )


def test_final_refresh_delivery_sends_one_legacy_compatible_request_without_persisting_secrets() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204, request=request)

    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    effect = _effect()

    result = delivery.deliver(effect)

    assert result == {
        "operation": "metadata-final-refresh",
        "status_code": 204,
        "item_id": "movie/1",
    }
    assert len(requests) == 1
    request = requests[0]
    assert request.url.raw_path.split(b"?", 1)[0] == b"/Items/movie%2F1/Refresh"
    assert dict(request.url.params) == {
        "api_key": "fixture-token",
        "Recursive": "false",
        "MetadataRefreshMode": "FullRefresh",
        "ImageRefreshMode": "FullRefresh",
        "ReplaceAllMetadata": "true",
        "ReplaceAllImages": "false",
    }
    assert "fixture-token" not in str(effect.payload)


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (401, MediaEffectDeliveryRejected),
        (503, MediaEffectDeliveryIndeterminate),
    ],
)
def test_final_refresh_delivery_classifies_provider_failures(
    status_code: int,
    error_type: type[Exception],
) -> None:
    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings,
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(status_code, request=request))
        ),
    )

    with pytest.raises(error_type):
        delivery.deliver(_effect())


def test_final_refresh_delivery_treats_transport_loss_as_indeterminate() -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("fixture response lost", request=request)

    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(timeout)),
    )

    with pytest.raises(MediaEffectDeliveryIndeterminate):
        delivery.deliver(_effect())


def test_final_refresh_delivery_rejects_missing_configuration_before_http() -> None:
    delivery = EmbyMetadataRefreshEffectDelivery(lambda: Settings())

    with pytest.raises(MediaEffectDeliveryRejected, match="configuration is incomplete"):
        delivery.deliver(_effect(emby_authorization_revision=identity_authorization_revision(Settings())))
