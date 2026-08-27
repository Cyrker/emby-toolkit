from __future__ import annotations

from typing import Any

import httpx
import pytest

from etk.domains.identity.user_activity_adapters import HttpxEmbyUserActivityAdapter
from etk.platform import Settings


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "emby_server_url": "https://emby.fixture.invalid",
            "emby_api_key": "fixture-token",
            "emby_api_timeout": 5,
        }
    )


def _adapter(handler: Any) -> HttpxEmbyUserActivityAdapter:
    transport = httpx.MockTransport(handler)
    return HttpxEmbyUserActivityAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=transport),
    )


def test_episode_resolver_distinguishes_absence_from_authoritative_series_identity() -> None:
    responses = [
        httpx.Response(404),
        httpx.Response(200, json={"Id": "episode-1", "Type": "Episode"}),
        httpx.Response(200, json={"Id": "episode-1", "Type": "Episode", "SeriesId": "series-1"}),
    ]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return responses.pop(0)

    adapter = _adapter(handler)

    assert adapter.series_id_for_episode("user-1", "episode-1") is None
    assert adapter.series_id_for_episode("user-1", "episode-1") is None
    assert adapter.series_id_for_episode("user-1", "episode-1") == "series-1"
    assert all(request.headers["x-emby-token"] == "fixture-token" for request in requests)


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500),
        httpx.Response(200, json=[]),
        httpx.Response(200, content=b"not-json"),
    ],
)
def test_episode_resolver_fails_closed_for_provider_or_payload_errors(response: httpx.Response) -> None:
    adapter = _adapter(lambda _request: response)

    with pytest.raises(RuntimeError):
        adapter.series_id_for_episode("user-1", "episode-1")


def test_episode_resolver_propagates_transport_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("fixture timeout", request=request)

    with pytest.raises(RuntimeError, match="unavailable"):
        _adapter(handler).series_id_for_episode("user-1", "episode-1")
