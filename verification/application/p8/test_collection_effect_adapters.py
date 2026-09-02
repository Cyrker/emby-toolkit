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


def test_custom_collection_create_and_member_mutations_use_emby_query_contract() -> None:
    observed: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append((request.method, request.url.path, dict(request.url.params)))
        if request.method == "POST" and request.url.path == "/Collections":
            assert request.content == b""
            return httpx.Response(200, json={"Id": "box-new"})
        if request.method == "GET" and request.url.path == "/Users/user-fixture/Items/box-new":
            return httpx.Response(200, json={"Id": "box-new", "Name": "Fixture", "Type": "BoxSet"})
        if request.method == "GET" and request.url.path == "/Users/user-fixture/Items":
            return httpx.Response(200, json={"Items": [{"Id": "item-a"}, {"Id": "item-b"}]})
        if request.method in {"POST", "DELETE"} and request.url.path == "/Collections/box-new/Items":
            return httpx.Response(204)
        raise AssertionError(request.url)

    adapter = _adapter(handler)

    created = adapter.create_collection("Fixture", ("item-a", "item-b"))
    adapter.add_members("box-new", ("item-c",))
    adapter.remove_members("box-new", ("item-a",))

    assert created.collection_id == "box-new"
    assert observed[0] == (
        "POST",
        "/Collections",
        {"Name": "Fixture", "UserId": "user-fixture", "Ids": "item-a,item-b"},
    )
    assert observed[-2] == ("POST", "/Collections/box-new/Items", {"Ids": "item-c"})
    assert observed[-1] == ("DELETE", "/Collections/box-new/Items", {"Ids": "item-a"})
