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
    PersistentReprocessMetadataRefresh,
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


@pytest.mark.parametrize("kind", ["Movie", "Series", "Season", "Episode"])
def test_reprocess_fills_missing_images_without_replacing_existing_or_recursing(kind):
    from unittest.mock import Mock

    from etk.domains.media.effects import MediaEffectStatus

    executor = Mock()
    executor.reserve_and_execute.return_value = SimpleNamespace(id="effect", status=MediaEffectStatus.CONFIRMED)
    refresh = PersistentReprocessMetadataRefresh(executor, _settings)
    refresh({"Id": "item", "Type": kind, "ImageTags": {}, "_missing_image_types": ["Primary"]}, "op")
    payload = executor.reserve_and_execute.call_args.kwargs["payload"]
    assert payload["metadata_only"] is False
    assert payload["replace_all_images"] is False
    assert payload["recursive"] is False


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


def test_images_only_refresh_preserves_metadata_and_all_existing_images():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204, request=request)

    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    delivery.deliver(_effect(images_only=True, replace_all_metadata=False))
    params = dict(requests[0].url.params)
    assert params["MetadataRefreshMode"] == "ValidationOnly"
    assert params["ImageRefreshMode"] == "FullRefresh"
    assert params["ReplaceAllMetadata"] == params["ReplaceAllImages"] == params["Recursive"] == "false"


def test_missing_image_compensation_has_distinct_durable_key_and_preserves_metadata():
    from unittest.mock import Mock

    from etk.domains.media.effects import MediaEffectStatus

    executor = Mock()
    executor.reserve_and_execute.return_value = SimpleNamespace(id="effect", status=MediaEffectStatus.CONFIRMED)
    refresh = PersistentReprocessMetadataRefresh(executor, _settings)
    item = {"Id": "item", "Type": "Movie", "_missing_image_types": ["Logo"]}
    refresh(item, "op")
    refresh({**item, "_images_only": True}, "op:missing-images")
    initial, repair = [call.kwargs for call in executor.reserve_and_execute.call_args_list]
    assert initial["effect_key"] != repair["effect_key"]
    assert repair["payload"]["images_only"] is True
    assert repair["payload"]["replace_all_metadata"] is False
    assert repair["payload"]["replace_all_images"] is False
    assert repair["payload"]["metadata_only"] is False
    assert repair["payload"]["recursive"] is False


def test_final_refresh_delivery_rejects_missing_configuration_before_http() -> None:
    delivery = EmbyMetadataRefreshEffectDelivery(lambda: Settings())

    with pytest.raises(MediaEffectDeliveryRejected, match="configuration is incomplete"):
        delivery.deliver(_effect(emby_authorization_revision=identity_authorization_revision(Settings())))


def test_final_refresh_updates_existing_person_entity_and_verifies_name():
    import json

    state = {
        "Id": "person-1", "Type": "Person", "Name": "Mike Listo",
        "Overview": "保留简介", "ProviderIds": {"Tmdb": "1"},
    }
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith('/Refresh'):
            return httpx.Response(204)
        if request.url.path.endswith('/Items/movie/1'):
            return httpx.Response(200, json={"People": [{"Id": "person-1", "Name": state["Name"], "Type": "Director"}]})
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["Overview"] == "保留简介"
            assert body["ProviderIds"] == {"Tmdb": "1"}
            state.update(body)
            return httpx.Response(204)
        return httpx.Response(200, json=state)

    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    effect = _effect(people_name_translations={"Mike Listo": "迈克·利斯托"})
    delivery.deliver(effect)
    assert state["Name"] == "迈克·利斯托"
    assert sum(r.method == "POST" and not r.url.path.endswith('/Refresh') for r in requests) == 1
    delivery.deliver(effect)
    assert sum(r.method == "POST" and not r.url.path.endswith('/Refresh') for r in requests) == 1


def test_final_refresh_keeps_manually_changed_person_name():
    posts = []

    def handler(request):
        if request.url.path.endswith('/Refresh'):
            return httpx.Response(204)
        if request.method == "POST":
            posts.append(request)
        if request.url.path.endswith('/Items/movie/1'):
            return httpx.Response(200, json={"People": [{"Id": "1", "Name": "Original", "Type": "Actor"}]})
        return httpx.Response(200, json={"Id": "1", "Type": "Person", "Name": "Manual Name"})

    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    delivery.deliver(_effect(people_name_translations={"Original": "译名"}))
    assert not posts


def test_final_refresh_does_not_confirm_ignored_person_update():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(204)
        if request.url.path.endswith('/Items/movie/1'):
            return httpx.Response(200, json={"People": [{"Id": "1", "Name": "Original", "Type": "Actor"}]})
        return httpx.Response(200, json={"Id": "1", "Type": "Person", "Name": "Original"})

    delivery = EmbyMetadataRefreshEffectDelivery(
        _settings, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(MediaEffectDeliveryIndeterminate, match="回读不一致"):
        delivery.deliver(_effect(people_name_translations={"Original": "译名"}))


def test_media_readback_does_not_accept_old_names_just_because_people_count_matches():
    from etk.domains.media.reprocess import _metadata_present

    item = {"People": [{"Id": "1", "Name": "Mike Listo", "Type": "Director"}]}
    expected = {"_director_count": 1, "_people_name_translations": {"Mike Listo": "迈克·利斯托"}}
    assert not _metadata_present(item, expected)
    item["People"][0]["Name"] = "迈克·利斯托"
    assert _metadata_present(item, expected)
