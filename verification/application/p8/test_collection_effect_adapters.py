from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.collection.effect_adapters import HttpxCollectionRemoteEffectsAdapter
from etk.domains.collection.effects import CollectionRemoteIndeterminate, CollectionRemoteRejected
from etk.platform import Settings


def _settings() -> Settings:
    return Settings(
        emby_server_url="https://emby.example",
        emby_user_id="user-fixture",
        emby_api_key=SecretStr("token-fixture"),
        emby_api_timeout=5,
    )


def _adapter(handler: Callable[[httpx.Request], httpx.Response]) -> HttpxCollectionRemoteEffectsAdapter:
    transport = httpx.MockTransport(handler)
    return HttpxCollectionRemoteEffectsAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=transport),
    )


@pytest.mark.parametrize("status_code", [302, 408, 425, 429, 500])
def test_collection_read_maps_redirect_and_transient_statuses_to_indeterminate(status_code: int) -> None:
    adapter = _adapter(lambda _request: httpx.Response(status_code, json={}))

    with pytest.raises(CollectionRemoteIndeterminate):
        adapter.read_collection("box-1")


def test_collection_read_distinguishes_absence_rejection_and_timeout() -> None:
    absent = _adapter(lambda _request: httpx.Response(404, json={}))
    rejected = _adapter(lambda _request: httpx.Response(401, json={}))

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("fixture timeout", request=request)

    assert absent.read_collection("box-1") is None
    with pytest.raises(CollectionRemoteRejected):
        rejected.read_collection("box-1")
    with pytest.raises(CollectionRemoteIndeterminate):
        _adapter(timeout).read_collection("box-1")


@pytest.mark.parametrize("status_code", [302, 408, 425, 429, 500])
def test_collection_write_maps_redirect_and_transient_statuses_to_indeterminate(status_code: int) -> None:
    adapter = _adapter(lambda _request: httpx.Response(status_code, json={}))

    with pytest.raises(CollectionRemoteIndeterminate):
        adapter.delete_collection("box-1")


def test_collection_write_accepts_absence_but_rejects_authorization_failure() -> None:
    absent = _adapter(lambda _request: httpx.Response(404, json={}))
    rejected = _adapter(lambda _request: httpx.Response(403, json={}))

    absent.delete_collection("box-1")
    with pytest.raises(CollectionRemoteRejected):
        rejected.delete_collection("box-1")
