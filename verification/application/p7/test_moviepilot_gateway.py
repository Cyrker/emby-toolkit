from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from etk.domains.subscription.moviepilot import (
    HttpxMoviePilotGateway,
    MoviePilotProviderError,
    MoviePilotProviderErrorKind,
    MoviePilotQueryDisposition,
    MoviePilotWriteOutcome,
)

_BASE_URL = "http://moviepilot.fixture.invalid"


def _gateway(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    timeout_seconds: float = 7.5,
) -> HttpxMoviePilotGateway:
    transport = httpx.MockTransport(handler)
    return HttpxMoviePilotGateway(
        _BASE_URL,
        "fixture-user",
        "fixture-password",
        timeout_seconds=timeout_seconds,
        client_factory=lambda: httpx.Client(transport=transport),
    )


def _login(request: httpx.Request, token: str = "fixture-token") -> httpx.Response:
    assert request.method == "POST"
    assert request.url.path == "/api/v1/login/access-token"
    return httpx.Response(200, json={"access_token": token})


def test_login_form_bearer_token_cache_and_unified_timeout() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        assert request.method == "GET"
        assert request.url.path == "/api/v1/subscribe/"
        return httpx.Response(200, json=[])

    gateway = _gateway(handler)
    first = gateway.list_subscriptions()
    second = gateway.list_subscriptions()

    assert first.empty is True
    assert first.disposition is MoviePilotQueryDisposition.EMPTY
    assert second.empty is True
    assert [request.url.path for request in requests] == [
        "/api/v1/login/access-token",
        "/api/v1/subscribe/",
        "/api/v1/subscribe/",
    ]
    assert parse_qs(requests[0].content.decode()) == {
        "username": ["fixture-user"],
        "password": ["fixture-password"],
    }
    assert requests[0].headers["Content-Type"].startswith("application/x-www-form-urlencoded")
    assert requests[1].headers["Authorization"] == "Bearer fixture-token"
    assert requests[2].headers["Authorization"] == "Bearer fixture-token"
    for request in requests:
        timeout = request.extensions["timeout"]
        assert timeout == {"connect": 7.5, "read": 7.5, "write": 7.5, "pool": 7.5}


def test_401_invalidates_token_reauthenticates_and_replays_write_once() -> None:
    login_tokens = iter(("expired-token", "fresh-token"))
    requests: list[httpx.Request] = []
    payload = {"name": "Fixture Series", "tmdbid": 101, "type": "电视剧", "season": 2}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/login/access-token":
            return _login(request, next(login_tokens))
        assert request.method == "POST"
        assert request.url.path == "/api/v1/subscribe/"
        assert json.loads(request.content) == payload
        if request.headers["Authorization"] == "Bearer expired-token":
            return httpx.Response(401)
        assert request.headers["Authorization"] == "Bearer fresh-token"
        return httpx.Response(201, content=b"accepted")

    result = _gateway(handler).create_subscription(payload)

    assert result.confirmed is True
    assert result.outcome is MoviePilotWriteOutcome.CONFIRMED
    assert result.status_code == 201
    assert [request.url.path for request in requests] == [
        "/api/v1/login/access-token",
        "/api/v1/subscribe/",
        "/api/v1/login/access-token",
        "/api/v1/subscribe/",
    ]
    assert all("Idempotency-Key" not in request.headers for request in requests)


def test_second_401_is_authentication_error_without_a_third_attempt() -> None:
    login_tokens = iter(("first-token", "second-token"))
    login_count = 0
    write_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal login_count, write_count
        if request.url.path == "/api/v1/login/access-token":
            login_count += 1
            return _login(request, next(login_tokens))
        write_count += 1
        return httpx.Response(401)

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).create_subscription({"tmdbid": 101})

    assert raised.value.kind is MoviePilotProviderErrorKind.AUTHENTICATION
    assert raised.value.status_code == 401
    assert raised.value.outcome is MoviePilotWriteOutcome.REJECTED
    assert login_count == 2
    assert write_count == 2


def test_query_found_and_404_are_distinct_typed_results() -> None:
    provider_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/api/v1/subscribe/media/tmdb:101"
        if request.url.params["season"] == "2":
            return httpx.Response(200, json={"id": 42, "tmdbid": 101, "season": 2})
        return httpx.Response(404)

    gateway = _gateway(handler)
    found = gateway.query_subscription(101, season=2)
    empty = gateway.query_subscription("101", season=3)

    assert found.disposition is MoviePilotQueryDisposition.FOUND
    assert found.found is True
    assert found.subscription == {"id": 42, "tmdbid": 101, "season": 2}
    assert empty.disposition is MoviePilotQueryDisposition.EMPTY
    assert empty.empty is True
    assert empty.subscription is None
    assert all(request.headers["Authorization"] == "Bearer fixture-token" for request in provider_requests)


def test_query_empty_object_is_empty_but_nonempty_object_without_id_is_invalid() -> None:
    responses = iter((httpx.Response(200, json={}), httpx.Response(200, json={"tmdbid": 101})))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        return next(responses)

    gateway = _gateway(handler)
    assert gateway.query_subscription(101).empty is True
    with pytest.raises(MoviePilotProviderError) as raised:
        gateway.query_subscription(101)
    assert raised.value.kind is MoviePilotProviderErrorKind.INVALID_RESPONSE


def test_list_preserves_provider_objects_and_distinguishes_empty_from_failure() -> None:
    response_status = 200
    response_payload: Any = [{"id": 1}, {"id": 2, "season": 3}]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        if response_status == 200:
            return httpx.Response(200, json=response_payload)
        return httpx.Response(response_status)

    gateway = _gateway(handler)
    populated = gateway.list_subscriptions()
    assert populated.disposition is MoviePilotQueryDisposition.FOUND
    assert populated.subscriptions == ({"id": 1}, {"id": 2, "season": 3})

    response_payload = []
    empty = gateway.list_subscriptions()
    assert empty.empty is True

    response_status = 503
    with pytest.raises(MoviePilotProviderError) as raised:
        gateway.list_subscriptions()
    assert raised.value.kind is MoviePilotProviderErrorKind.SERVER
    assert raised.value.status_code == 503
    assert raised.value.outcome is None


def test_active_downloads_normalize_supported_response_shapes() -> None:
    responses: list[Any] = [
        {
            "data": {
                "items": [
                    {"hashString": "ABCDEF", "media": {"tmdbid": "2500", "season": 1}},
                    {"info_hash": "123456", "source": {"id": 25}},
                ]
            }
        },
        [],
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        assert request.method == "GET"
        assert request.url.path == "/api/v1/download/"
        return httpx.Response(200, json=responses.pop(0))

    gateway = _gateway(handler)

    active = gateway.active_downloads()
    assert [value.task_hash for value in active] == ["abcdef", "123456"]
    assert (active[0].tmdb_id, active[0].season) == ("2500", 1)
    assert active[1].subscribe_id == 25
    assert gateway.active_downloads() == ()


def test_active_downloads_reject_items_without_an_authoritative_hash() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        return httpx.Response(200, json=[{"id": 42, "media": {"tmdbid": "2500"}}])

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).active_downloads()

    assert raised.value.kind is MoviePilotProviderErrorKind.INVALID_RESPONSE


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"data": {}},
        [{"hash": {"unexpected": "object"}, "media": {"tmdbid": "2500"}}],
        [{"hash": "abcdef", "media": {"tmdbid": {"unexpected": "object"}}}],
    ],
)
def test_active_downloads_reject_ambiguous_empty_or_invalid_contract_shapes(payload: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        return httpx.Response(200, json=payload)

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).active_downloads()

    assert raised.value.kind is MoviePilotProviderErrorKind.INVALID_RESPONSE


def test_create_and_update_preserve_the_legacy_method_path_and_json_payload() -> None:
    provider_requests: list[httpx.Request] = []
    payload = {
        "name": "Fixture Series",
        "tmdbid": 101,
        "type": "电视剧",
        "season": 2,
        "start_episode": 3,
        "best_version": 1,
        "best_version_full": 1,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        assert request.url.path == "/api/v1/subscribe/"
        assert json.loads(request.content) == payload
        return httpx.Response(201 if request.method == "POST" else 204)

    gateway = _gateway(handler)
    created = gateway.create_subscription(payload)
    updated = gateway.update_subscription(payload)

    assert (created.operation, created.status_code, created.confirmed) == ("create-subscription", 201, True)
    assert (updated.operation, updated.status_code, updated.confirmed) == ("update-subscription", 204, True)
    assert [request.method for request in provider_requests] == ["POST", "PUT"]
    assert all(request.headers["Authorization"] == "Bearer fixture-token" for request in provider_requests)
    assert all("Idempotency-Key" not in request.headers for request in provider_requests)


def test_status_update_uses_identity_lookup_then_status_query_contract() -> None:
    provider_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"id": 42, "tmdbid": 101, "season": 2})
        return httpx.Response(204)

    result = _gateway(handler).update_subscription_status(101, "S", season=2)

    assert result.confirmed is True
    assert result.status_code == 204
    assert [(request.method, request.url.path, dict(request.url.params)) for request in provider_requests] == [
        ("GET", "/api/v1/subscribe/media/tmdb:101", {"season": "2"}),
        ("PUT", "/api/v1/subscribe/status/42", {"state": "S"}),
    ]
    assert all("Idempotency-Key" not in request.headers for request in provider_requests)


def test_status_update_adjusts_total_and_lack_episode_with_full_detail_payload() -> None:
    provider_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        if request.method == "GET" and request.url.path.endswith("/media/tmdb:101"):
            return httpx.Response(200, json={"id": 42})
        if request.method == "PUT" and request.url.path.endswith("/status/42"):
            return httpx.Response(200)
        if request.method == "GET" and request.url.path.endswith("/subscribe/42"):
            return httpx.Response(
                200,
                json={"id": 42, "name": "Fixture Series", "total_episode": 12, "lack_episode": 5},
            )
        assert request.method == "PUT"
        assert request.url.path == "/api/v1/subscribe/"
        assert json.loads(request.content) == {
            "id": 42,
            "name": "Fixture Series",
            "total_episode": 10,
            "lack_episode": 3,
        }
        return httpx.Response(204)

    result = _gateway(handler).update_subscription_status(101, "R", total_episodes=10)

    assert result.confirmed is True
    assert result.status_code == 204
    assert [(request.method, request.url.path) for request in provider_requests] == [
        ("GET", "/api/v1/subscribe/media/tmdb:101"),
        ("PUT", "/api/v1/subscribe/status/42"),
        ("GET", "/api/v1/subscribe/42"),
        ("PUT", "/api/v1/subscribe/"),
    ]


def test_status_update_does_not_put_detail_when_total_is_unchanged() -> None:
    provider_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        if request.url.path.endswith("/media/tmdb:101"):
            return httpx.Response(200, json={"id": 42})
        if request.url.path.endswith("/status/42"):
            return httpx.Response(200)
        return httpx.Response(200, json={"id": 42, "total_episode": 10, "lack_episode": 1})

    result = _gateway(handler).update_subscription_status(101, "P", total_episodes=10)

    assert result.status_code == 200
    assert [(request.method, request.url.path) for request in provider_requests] == [
        ("GET", "/api/v1/subscribe/media/tmdb:101"),
        ("PUT", "/api/v1/subscribe/status/42"),
        ("GET", "/api/v1/subscribe/42"),
    ]


def test_status_update_provider_empty_is_typed_not_found_without_a_write() -> None:
    provider_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        return httpx.Response(404)

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).update_subscription_status(101, "S", season=2)

    assert raised.value.kind is MoviePilotProviderErrorKind.NOT_FOUND
    assert raised.value.outcome is MoviePilotWriteOutcome.REJECTED
    assert [(request.method, request.url.path) for request in provider_requests] == [
        ("GET", "/api/v1/subscribe/media/tmdb:101")
    ]


def test_delete_404_is_confirmed_idempotent_for_identity_and_numeric_id() -> None:
    provider_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        return httpx.Response(404)

    gateway = _gateway(handler)
    by_identity = gateway.delete_subscription(101, season=2)
    by_id = gateway.delete_subscription_by_id(42)

    assert by_identity.confirmed is True
    assert by_identity.idempotent is True
    assert by_identity.status_code == 404
    assert by_id.confirmed is True
    assert by_id.idempotent is True
    assert by_id.status_code == 404
    assert [(request.method, request.url.path, dict(request.url.params)) for request in provider_requests] == [
        ("DELETE", "/api/v1/subscribe/media/tmdb:101", {"season": "2"}),
        ("DELETE", "/api/v1/subscribe/42", {}),
    ]
    assert all("Idempotency-Key" not in request.headers for request in provider_requests)


def test_transfer_history_delete_preserves_query_and_json_contract() -> None:
    provider_requests: list[httpx.Request] = []
    responses = iter((httpx.Response(204), httpx.Response(404)))
    record = {"id": 7, "tmdbid": 101, "seasons": "S02", "episodes": "E01"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        provider_requests.append(request)
        return next(responses)

    gateway = _gateway(handler)
    deleted = gateway.delete_transfer_history(record)
    replay = gateway.delete_transfer_history(record)

    assert deleted.confirmed is True and deleted.idempotent is False
    assert replay.confirmed is True and replay.idempotent is True
    assert [(request.method, request.url.path, dict(request.url.params)) for request in provider_requests] == [
        ("DELETE", "/api/v1/history/transfer", {"deletesrc": "false", "deletedest": "false"}),
        ("DELETE", "/api/v1/history/transfer", {"deletesrc": "false", "deletedest": "false"}),
    ]
    assert [json.loads(request.content) for request in provider_requests] == [record, record]


@pytest.mark.parametrize(
    ("status_code", "expected_kind", "expected_outcome"),
    [
        (404, MoviePilotProviderErrorKind.NOT_FOUND, MoviePilotWriteOutcome.REJECTED),
        (409, MoviePilotProviderErrorKind.CONFLICT, MoviePilotWriteOutcome.REJECTED),
        (429, MoviePilotProviderErrorKind.RATE_LIMITED, MoviePilotWriteOutcome.REJECTED),
        (503, MoviePilotProviderErrorKind.SERVER, MoviePilotWriteOutcome.INDETERMINATE),
    ],
)
def test_write_status_errors_are_programmatically_typed(
    status_code: int,
    expected_kind: MoviePilotProviderErrorKind,
    expected_outcome: MoviePilotWriteOutcome,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        return httpx.Response(status_code)

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).create_subscription({"tmdbid": 101})

    assert raised.value.kind is expected_kind
    assert raised.value.status_code == status_code
    assert raised.value.outcome is expected_outcome


@pytest.mark.parametrize(
    ("failure", "expected_kind"),
    [
        ("timeout", MoviePilotProviderErrorKind.TIMEOUT),
        ("transport", MoviePilotProviderErrorKind.TRANSPORT),
    ],
)
def test_timeout_and_transport_write_failures_are_indeterminate(
    failure: str,
    expected_kind: MoviePilotProviderErrorKind,
) -> None:
    write_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal write_count
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        write_count += 1
        if failure == "timeout":
            raise httpx.ReadTimeout("fixture timeout", request=request)
        raise httpx.ConnectError("fixture transport failure", request=request)

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).create_subscription({"tmdbid": 101})

    assert raised.value.kind is expected_kind
    assert raised.value.status_code is None
    assert raised.value.outcome is MoviePilotWriteOutcome.INDETERMINATE
    assert write_count == 1


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, content=b"not-json", headers={"Content-Type": "application/json"}),
        httpx.Response(200, json={"not": "an array"}),
        httpx.Response(200, json=[{"id": 1}, "not-an-object"]),
    ],
)
def test_list_invalid_json_or_shape_is_typed_invalid_response(response: httpx.Response) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        return response

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).list_subscriptions()

    assert raised.value.kind is MoviePilotProviderErrorKind.INVALID_RESPONSE
    assert raised.value.status_code == 200


@pytest.mark.parametrize(
    "login_response",
    [
        httpx.Response(200, content=b"not-json"),
        httpx.Response(200, json=[]),
        httpx.Response(200, json={}),
        httpx.Response(200, json={"access_token": ""}),
    ],
)
def test_login_invalid_json_or_token_shape_is_typed_invalid_response(login_response: httpx.Response) -> None:
    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(lambda _request: login_response).list_subscriptions()

    assert raised.value.kind is MoviePilotProviderErrorKind.INVALID_RESPONSE
    assert raised.value.operation == "authenticate"


def test_configuration_and_status_values_fail_before_provider_io() -> None:
    with pytest.raises(ValueError, match="required"):
        HttpxMoviePilotGateway("", "user", "password")
    with pytest.raises(ValueError, match="positive"):
        HttpxMoviePilotGateway(_BASE_URL, "user", "password", timeout_seconds=0)

    provider_calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal provider_calls
        provider_calls += 1
        return httpx.Response(500)

    with pytest.raises(ValueError, match="P, R, or S"):
        _gateway(handler).update_subscription_status(101, "invalid")
    assert provider_calls == 0


def test_system_setting_write_uses_scalar_json_and_confirms_readback() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        assert request.url.path == "/api/v1/system/setting/MOVIE_RENAME_FORMAT"
        if request.method == "POST":
            assert json.loads(request.content) == "{{ title }}/{{ name }}{{ fileExt }}"
            return httpx.Response(200)
        assert request.method == "GET"
        return httpx.Response(200, json={"data": {"value": "{{ title }}/{{ name }}{{ fileExt }}"}})

    result = _gateway(handler).set_system_setting(
        "MOVIE_RENAME_FORMAT",
        "{{ title }}/{{ name }}{{ fileExt }}",
    )

    assert result.confirmed is True
    assert [request.method for request in requests] == ["POST", "POST", "GET"]


def test_system_setting_readback_mismatch_is_indeterminate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        if request.method == "POST":
            return httpx.Response(204)
        return httpx.Response(200, json={"data": "old-template"})

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).set_system_setting("TV_RENAME_FORMAT", "new-template")

    assert raised.value.kind is MoviePilotProviderErrorKind.INVALID_RESPONSE
    assert raised.value.outcome is MoviePilotWriteOutcome.INDETERMINATE


def test_system_setting_timeout_is_indeterminate_without_write_replay() -> None:
    write_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal write_count
        if request.url.path == "/api/v1/login/access-token":
            return _login(request)
        write_count += 1
        raise httpx.ReadTimeout("fixture timeout", request=request)

    with pytest.raises(MoviePilotProviderError) as raised:
        _gateway(handler).set_system_setting("TV_RENAME_FORMAT", "new-template")

    assert raised.value.kind is MoviePilotProviderErrorKind.TIMEOUT
    assert raised.value.outcome is MoviePilotWriteOutcome.INDETERMINATE
    assert write_count == 1
