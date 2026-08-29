from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.p115.intro_runtime import (
    HttpxEmbyIntroFavoriteGateway,
    IntroFavoriteIdentity,
    select_intro_provider_identity,
)


class _Token:
    def get_secret_value(self) -> str:
        return "fixture-token"


class _Client:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __enter__(self) -> _Client:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def _response(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(status, json=payload, request=httpx.Request("GET", "https://emby.fixture"))


def _settings() -> Any:
    return SimpleNamespace(
        emby_server_url="https://emby.fixture",
        emby_api_key=_Token(),
        emby_api_timeout=20,
    )


def test_authoritative_favorite_fallback_checks_direct_ids_then_season_episode_filter() -> None:
    client = _Client(
        [
            _response(200, [{"Id": "user-1"}]),
            _response(200, {"Id": "series-1", "UserData": {"IsFavorite": False}}),
            _response(404, {}),
            _response(200, {"Items": [{"Id": "episode-9", "UserData": {"IsFavorite": True}}]}),
        ]
    )
    gateway = HttpxEmbyIntroFavoriteGateway(
        cast(Any, _settings),
        client_factory=lambda: cast(Any, client),
    )

    favorite = gateway.find_favorite(
        direct_item_ids=("series-1", "season-1"),
        season_ids=("season-1",),
    )

    assert favorite == IntroFavoriteIdentity("user-1", "episode-9")
    assert [call[0] for call in client.calls] == [
        "https://emby.fixture/Users",
        "https://emby.fixture/Users/user-1/Items/series-1",
        "https://emby.fixture/Users/user-1/Items/season-1",
        "https://emby.fixture/Users/user-1/Items",
    ]
    assert client.calls[-1][1]["params"] == {
        "ParentId": "season-1",
        "IncludeItemTypes": "Episode",
        "Filters": "IsFavorite",
        "Limit": "1",
        "Fields": "UserData",
    }


def test_authoritative_favorite_fallback_fails_closed_on_transport_contract_failure() -> None:
    client = _Client([_response(503, {"error": "unavailable"})])
    gateway = HttpxEmbyIntroFavoriteGateway(
        cast(Any, _settings),
        client_factory=lambda: cast(Any, client),
    )

    with pytest.raises(RuntimeError, match="HTTP 503"):
        gateway.find_favorite(direct_item_ids=("series-1",), season_ids=("season-1",))


def test_provider_identity_selects_one_available_backend_before_operation_reservation() -> None:
    credentials = SimpleNamespace(
        load=lambda: P115CredentialSnapshot(
            access_token="",
            cookie="fixture-cookie",
            app_type="web",
            account_id="115001",
        )
    )

    assert select_intro_provider_identity(credentials, lambda: "openapi") == ("115001", "cookie")
