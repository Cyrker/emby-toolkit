from __future__ import annotations

from typing import Any

import pytest
import requests

from handler import emby


class FakeResponse:
    def raise_for_status(self) -> None:
        return None


class FakeEmbyClient:
    def __init__(self, on_post=None) -> None:
        self.on_post = on_post
        self.posts: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs):
        call = {"url": url, **kwargs}
        self.posts.append(call)
        if self.on_post is not None:
            self.on_post(call)
        return FakeResponse()


def _folder(
    *,
    name: str = "ETK Movies",
    path: str = "/media/Movies",
    collection_type: str = "movies",
    folder_id: str = "library-1",
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "Name": name,
        "Locations": [path],
        "CollectionType": collection_type,
        "ItemId": folder_id,
        "LibraryOptions": options if options is not None else emby._library_options_with_etk(collection_type),
    }


def test_exact_existing_library_keeps_unrelated_options_and_ensures_etk(monkeypatch) -> None:
    existing = _folder(
        collection_type="MOVIES",
        options={
            "EnableRealtimeMonitor": True,
            "CustomUserSetting": {"keep": True},
            "DisabledLocalMetadataReaders": ["Xml"],
            "LocalMetadataReaderOrder": ["Xml", "Nfo"],
            "TypeOptions": [
                {
                    "Type": "Movie",
                    "MetadataFetchers": ["TheMovieDb"],
                    "ImageOptions": [{"Type": "Primary", "Limit": 2}],
                },
                {"Type": "Audio", "MetadataFetchers": ["AudioDb"]},
            ],
        },
    )
    client = FakeEmbyClient()
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: [existing])

    result = emby.create_library(
        "http://emby.invalid/",
        "fixture-token",
        "ETK Movies",
        "movies",
        "/media/Movies/",
    )

    assert result == {
        "name": "ETK Movies",
        "path": "/media/Movies",
        "id": "library-1",
        "created": False,
    }
    assert len(client.posts) == 1
    assert client.posts[0]["url"].endswith("/Library/VirtualFolders/LibraryOptions")
    assert client.posts[0]["json"]["Id"] == "library-1"
    updated = client.posts[0]["json"]["LibraryOptions"]
    assert updated["CustomUserSetting"] == {"keep": True}
    assert updated["DisabledLocalMetadataReaders"] == ["Xml", "Nfo"]
    assert updated["LocalMetadataReaderOrder"] == ["Xml"]
    by_type = {value["Type"]: value for value in updated["TypeOptions"]}
    assert by_type["Movie"]["MetadataFetchers"] == ["ETK Metadata"]
    assert by_type["Movie"]["ImageOptions"] == [{"Type": "Primary", "Limit": 2}]
    assert by_type["BoxSet"]["ImageFetchers"] == ["ETK Images"]
    assert by_type["Audio"] == {"Type": "Audio", "MetadataFetchers": ["AudioDb"]}


@pytest.mark.parametrize(
    "existing",
    [
        _folder(path="/media/Other"),
        _folder(name="Other Movies", collection_type="tvshows"),
    ],
    ids=["same-name-different-path", "same-path-incompatible-type"],
)
def test_overlapping_incompatible_library_is_a_conflict(monkeypatch, existing) -> None:
    client = FakeEmbyClient()
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: [existing])

    with pytest.raises(emby.EmbyLibraryConflictError, match="Emby 媒体库冲突"):
        emby.create_library(
            "http://emby.invalid",
            "fixture-token",
            "ETK Movies",
            "movies",
            "/media/Movies",
        )

    assert client.posts == []


def test_created_library_is_polled_until_exact_target_becomes_visible(monkeypatch) -> None:
    responses = iter([[], [], [_folder(folder_id="library-eventual")]])
    client = FakeEmbyClient()
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: next(responses))
    monkeypatch.setattr(emby.time, "sleep", lambda _seconds: None)

    result = emby.create_library(
        "http://emby.invalid",
        "fixture-token",
        "ETK Movies",
        "movies",
        "/media/Movies",
    )

    assert result["id"] == "library-eventual"
    assert result["created"] is True
    create_posts = [call for call in client.posts if call["url"].endswith("/Library/VirtualFolders")]
    assert len(create_posts) == 1
    assert create_posts[0]["json"]["LibraryOptions"]["TypeOptions"]


def test_created_library_with_empty_options_is_configured_before_success(monkeypatch) -> None:
    responses = iter([[], [_folder(folder_id="library-needs-options", options={})]])
    client = FakeEmbyClient()
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: next(responses))

    result = emby.create_library(
        "http://emby.invalid",
        "fixture-token",
        "ETK Movies",
        "movies",
        "/media/Movies",
    )

    assert result["id"] == "library-needs-options"
    assert result["created"] is True
    assert [call["url"].rsplit("/", 1)[-1] for call in client.posts] == ["VirtualFolders", "LibraryOptions"]
    assert client.posts[1]["json"]["Id"] == "library-needs-options"
    configured_types = {value["Type"] for value in client.posts[1]["json"]["LibraryOptions"]["TypeOptions"]}
    assert configured_types == {"Movie", "BoxSet"}


def test_ambiguous_create_timeout_reconciles_the_created_library(monkeypatch) -> None:
    responses = iter([[], [_folder(folder_id="library-after-timeout")]])

    def timeout_after_create(call: dict[str, Any]) -> None:
        if call["url"].endswith("/Library/VirtualFolders"):
            raise requests.exceptions.Timeout("create response timed out")

    client = FakeEmbyClient(on_post=timeout_after_create)
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: next(responses))

    result = emby.create_library(
        "http://emby.invalid",
        "fixture-token",
        "ETK Movies",
        "movies",
        "/media/Movies",
    )

    assert result == {
        "name": "ETK Movies",
        "path": "/media/Movies",
        "id": "library-after-timeout",
        "created": True,
    }
    assert len(client.posts) == 1


def test_ambiguous_create_timeout_surfaces_an_incompatible_resource(monkeypatch) -> None:
    responses = iter([[], [_folder(collection_type="tvshows")]])

    def timeout_after_create(call: dict[str, Any]) -> None:
        if call["url"].endswith("/Library/VirtualFolders"):
            raise requests.exceptions.Timeout("create response timed out")

    client = FakeEmbyClient(on_post=timeout_after_create)
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: next(responses))

    with pytest.raises(emby.EmbyLibraryConflictError, match="Emby 媒体库冲突"):
        emby.create_library(
            "http://emby.invalid",
            "fixture-token",
            "ETK Movies",
            "movies",
            "/media/Movies",
        )

    assert len(client.posts) == 1


def test_ambiguous_create_timeout_is_preserved_when_target_never_appears(monkeypatch) -> None:
    def timeout_after_create(call: dict[str, Any]) -> None:
        if call["url"].endswith("/Library/VirtualFolders"):
            raise requests.exceptions.Timeout("fixture create timeout after commit")

    client = FakeEmbyClient(on_post=timeout_after_create)
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: [])
    monkeypatch.setattr(emby, "_LIBRARY_RECONCILE_TIMEOUT_SECONDS", 0)

    with pytest.raises(RuntimeError, match="fixture create timeout after commit") as exc_info:
        emby.create_library(
            "http://emby.invalid",
            "fixture-token",
            "ETK Movies",
            "movies",
            "/media/Movies",
        )

    assert isinstance(exc_info.value.__cause__, requests.exceptions.Timeout)
    assert len(client.posts) == 1


def test_exact_library_without_stable_id_fails(monkeypatch) -> None:
    client = FakeEmbyClient()
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: [_folder(folder_id="")])

    with pytest.raises(RuntimeError, match="未返回稳定 ID"):
        emby.create_library(
            "http://emby.invalid",
            "fixture-token",
            "ETK Movies",
            "movies",
            "/media/Movies",
        )

    assert client.posts == []


def test_created_library_visibility_timeout_fails(monkeypatch) -> None:
    client = FakeEmbyClient()
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: [])
    monkeypatch.setattr(emby, "_LIBRARY_RECONCILE_TIMEOUT_SECONDS", 0)

    with pytest.raises(RuntimeError, match="创建媒体库超时"):
        emby.create_library(
            "http://emby.invalid",
            "fixture-token",
            "ETK Movies",
            "movies",
            "/media/Movies",
        )

    assert len(client.posts) == 1


def test_repeated_reconciliation_posts_the_library_only_once(monkeypatch) -> None:
    state: list[dict[str, Any]] = []

    def make_visible(call: dict[str, Any]) -> None:
        if not call["url"].endswith("/Library/VirtualFolders"):
            return
        payload = call["json"]
        state.append(
            {
                "Name": payload["Name"],
                "Locations": payload["Paths"],
                "CollectionType": payload["CollectionType"],
                "ItemId": "stable-library-id",
                "LibraryOptions": payload["LibraryOptions"],
            }
        )

    client = FakeEmbyClient(on_post=make_visible)
    monkeypatch.setattr(emby, "emby_client", client)
    monkeypatch.setattr(emby, "get_virtual_folders", lambda *_args: list(state))

    first = emby.create_library(
        "http://emby.invalid",
        "fixture-token",
        "ETK Movies",
        "movies",
        "/media/Movies",
    )
    second = emby.create_library(
        "http://emby.invalid",
        "fixture-token",
        "ETK Movies",
        "movies",
        "/media/Movies/",
    )

    assert first == {
        "name": "ETK Movies",
        "path": "/media/Movies",
        "id": "stable-library-id",
        "created": True,
    }
    assert second == {**first, "created": False}
    assert len(client.posts) == 1
