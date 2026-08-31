from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.collection.activation import (
    CollectionActivationNotFoundError,
    CollectionActivationProviderRejectedError,
    CollectionActivationProviderUnavailableError,
)
from etk.domains.collection.refresh_adapter import HttpxNativeCollectionRefreshAdapter
from etk.domains.media.auto_tagging_adapter import HttpxAutoTaggingAdapter
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import MediaEffectProjection
from etk.platform import Settings


def _settings() -> Settings:
    return Settings(
        emby_server_url="https://emby.example",
        emby_user_id="user-fixture",
        emby_api_key=SecretStr("emby-token"),
        emby_api_timeout=5,
        tmdb_api_key=SecretStr("tmdb-token"),
        tmdb_api_base_url="https://tmdb.example/3",
    )


def _client_factory(handler: Callable[[httpx.Request], httpx.Response]) -> Callable[[], httpx.Client]:
    transport = httpx.MockTransport(handler)
    return lambda: httpx.Client(transport=transport)


def _tag_effect(operation: str = "add") -> MediaEffectProjection:
    return cast(
        MediaEffectProjection,
        SimpleNamespace(payload={"item_id": "item-1", "operation": operation, "tags": ["Fixture"]}),
    )


def test_auto_tagging_adapter_confirms_lost_response_from_authoritative_readback() -> None:
    tags: list[str] = []
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"Id": "item-1", "Tags": list(tags), "TagItems": [], "LockedFields": ["Tags"]},
            )
        if request.method == "POST":
            posts += 1
            payload = request.read().decode()
            assert '"Fixture"' in payload
            assert '"LockedFields":[]' in payload
            tags.append("Fixture")
            return httpx.Response(503, json={})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    adapter = HttpxAutoTaggingAdapter(_settings, client_factory=_client_factory(handler))

    result = adapter.deliver(_tag_effect())

    assert result == {"item_id": "item-1", "operation": "add", "changed": True, "readback": "confirmed"}
    assert posts == 1


def test_auto_tagging_adapter_uses_one_full_prewrite_snapshot() -> None:
    tags = ["Existing", "Concurrent"]
    reads = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal reads, tags
        if request.method == "GET":
            reads += 1
            if reads == 1 and request.url.params.get("Fields") == "Tags,TagItems,LockedFields":
                return httpx.Response(200, json={"Id": "item-1", "Tags": ["Existing"]})
            return httpx.Response(200, json={"Id": "item-1", "Tags": list(tags), "LockedFields": []})
        if request.method == "POST":
            payload = request.read().decode()
            assert '"Concurrent"' in payload
            assert '"Fixture"' in payload
            tags = ["Concurrent", "Existing", "Fixture"]
            return httpx.Response(204)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = HttpxAutoTaggingAdapter(_settings, client_factory=_client_factory(handler)).deliver(_tag_effect())

    assert result["changed"] is True
    assert reads == 2


def test_auto_tagging_adapter_separates_rejection_from_unknown_write() -> None:
    def rejected(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"Id": "item-1", "Tags": []})
        return httpx.Response(400, json={})

    with pytest.raises(MediaEffectDeliveryRejected, match="rejected"):
        HttpxAutoTaggingAdapter(_settings, client_factory=_client_factory(rejected)).deliver(_tag_effect())

    def unknown(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"Id": "item-1", "Tags": []})
        return httpx.Response(503, json={})

    with pytest.raises(MediaEffectDeliveryIndeterminate, match="outcome is unknown"):
        HttpxAutoTaggingAdapter(_settings, client_factory=_client_factory(unknown)).deliver(_tag_effect())


@pytest.mark.parametrize("status_code", [401, 403, 404])
def test_auto_tagging_adapter_rejects_authoritative_initial_read_failure(status_code: int) -> None:
    adapter = HttpxAutoTaggingAdapter(
        _settings,
        client_factory=_client_factory(lambda _request: httpx.Response(status_code, json={})),
    )

    with pytest.raises(MediaEffectDeliveryRejected):
        adapter.deliver(_tag_effect())


@pytest.mark.parametrize("status_code", [302, 408, 425, 429, 500])
def test_auto_tagging_adapter_retries_transient_initial_read_failure(status_code: int) -> None:
    adapter = HttpxAutoTaggingAdapter(
        _settings,
        client_factory=_client_factory(lambda _request: httpx.Response(status_code, json={})),
    )

    with pytest.raises(MediaEffectDeliveryRetryable):
        adapter.deliver(_tag_effect())


def test_auto_tagging_adapter_reads_supported_inventory_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/Library/VirtualFolders":
            return httpx.Response(200, json=[{"Id": "library-1", "CollectionType": "movies"}])
        return httpx.Response(200, json={"Items": [{"Id": "item-1", "OfficialRating": "PG-13"}]})

    adapter = HttpxAutoTaggingAdapter(_settings, client_factory=_client_factory(handler))

    assert adapter.list_libraries() == ({"Id": "library-1", "CollectionType": "movies"},)
    assert adapter.list_items("library-1") == ({"Id": "item-1", "OfficialRating": "PG-13"},)
    assert requests[1].url.params["ParentId"] == "library-1"
    assert requests[1].url.params["Recursive"] == "true"


def test_collection_refresh_adapter_requires_complete_library_scan() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/Library/VirtualFolders":
            return httpx.Response(
                200,
                json=[
                    {"Id": "library-1", "CollectionType": "movies"},
                    {"Id": "library-2", "CollectionType": "tvshows"},
                    {"Id": "boxsets", "CollectionType": "boxsets"},
                ],
            )
        if request.url.params.get("ParentId") == "library-1":
            return httpx.Response(
                200,
                json={
                    "Items": [
                        {"Id": "box-1", "Name": "Fixture", "ProviderIds": {"Tmdb": "999"}},
                        {"Id": "ignored", "Name": "No provider", "ProviderIds": {}},
                    ]
                },
            )
        return httpx.Response(500, json={})

    adapter = HttpxNativeCollectionRefreshAdapter(_settings, client_factory=_client_factory(handler))

    with pytest.raises(RuntimeError, match="query is unavailable"):
        adapter.list_collections()
    assert calls == [
        "/Library/VirtualFolders",
        "/Users/user-fixture/Items",
        "/Users/user-fixture/Items",
    ]


def test_collection_refresh_adapter_projects_identity_and_tmdb_details() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Library/VirtualFolders":
            return httpx.Response(200, json=[{"Id": "library-1", "CollectionType": "movies"}])
        if request.url.host == "emby.example":
            return httpx.Response(
                200,
                json={"Items": [{"Id": "box-1", "Name": "Fixture", "ProviderIds": {"Tmdb": "999"}}]},
            )
        assert request.url.path == "/3/collection/999"
        assert request.url.params["api_key"] == "tmdb-token"
        return httpx.Response(200, json={"id": 999, "parts": []})

    adapter = HttpxNativeCollectionRefreshAdapter(_settings, client_factory=_client_factory(handler))

    references = adapter.list_collections()
    details = adapter.collection_details("999")

    assert len(references) == 1
    assert references[0].emby_collection_id == "box-1"
    assert references[0].tmdb_collection_id == "999"
    assert references[0].library_id == "library-1"
    assert details == {"id": 999, "parts": []}


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (404, CollectionActivationNotFoundError),
        (401, CollectionActivationProviderRejectedError),
        (429, CollectionActivationProviderUnavailableError),
        (503, CollectionActivationProviderUnavailableError),
    ],
)
def test_collection_refresh_adapter_classifies_targeted_activation_failures(
    status_code: int,
    error_type: type[BaseException],
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={})

    adapter = HttpxNativeCollectionRefreshAdapter(_settings, client_factory=_client_factory(handler))

    with pytest.raises(error_type):
        adapter.collection_details("999")


def test_collection_refresh_adapter_classifies_missing_tmdb_configuration_as_unavailable() -> None:
    settings = Settings(tmdb_api_base_url="", tmdb_api_key=SecretStr(""))
    adapter = HttpxNativeCollectionRefreshAdapter(
        lambda: settings,
        client_factory=lambda: pytest.fail("missing configuration must not construct an HTTP client"),
    )

    with pytest.raises(CollectionActivationProviderUnavailableError, match="TMDb is not configured"):
        adapter.collection_details("999")


def test_collection_refresh_adapter_rejects_ambiguous_tmdb_identity_before_reconcile() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Library/VirtualFolders":
            return httpx.Response(
                200,
                json=[
                    {"Id": "library-1", "CollectionType": "movies"},
                    {"Id": "library-2", "CollectionType": "tvshows"},
                ],
            )
        library_id = request.url.params["ParentId"]
        return httpx.Response(
            200,
            json={
                "Items": [
                    {
                        "Id": f"box-{library_id}",
                        "Name": "Duplicate",
                        "ProviderIds": {"Tmdb": "999"},
                    }
                ]
            },
        )

    adapter = HttpxNativeCollectionRefreshAdapter(_settings, client_factory=_client_factory(handler))

    with pytest.raises(RuntimeError, match="TMDb identity is ambiguous"):
        adapter.list_collections()
