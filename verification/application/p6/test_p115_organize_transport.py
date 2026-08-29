from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from etk.domains.p115 import P115CredentialSnapshot, P115ProviderNoEffectError
from etk.domains.p115.organize_transport import (
    HttpxP115OrganizeMutationGateway,
    P115OrganizeTransportError,
)


class MutableCredentials:
    def __init__(self, snapshot: P115CredentialSnapshot) -> None:
        self.current = snapshot
        self.load_calls = 0

    def load(self) -> P115CredentialSnapshot:
        self.load_calls += 1
        return self.current

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("organize transport must not refresh credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("organize transport must not save credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


def _snapshot(
    *,
    account_id: str = "115001",
    access_token: str = "fixture-openapi-token",
    cookie: str = "UID=115001_fixture; CID=fixture; KID=fixture; SEID=fixture",
    app_type: str = "alipaymini",
) -> P115CredentialSnapshot:
    return P115CredentialSnapshot(
        access_token=access_token,
        cookie=cookie,
        app_type=app_type,
        account_id=account_id,
    )


def _client_factory(transport: httpx.BaseTransport) -> Callable[..., httpx.Client]:
    return lambda **kwargs: httpx.Client(transport=transport, **kwargs)


@pytest.mark.parametrize(
    ("backend", "expected"),
    [
        (
            "openapi",
            [
                ("https://proapi.115.com/open/folder/add", {"pid": ["parent"], "file_name": ["Movies"]}),
                ("https://proapi.115.com/open/ufile/move", {"file_ids": ["file-1"], "to_cid": ["target"]}),
                (
                    "https://proapi.115.com/open/ufile/update",
                    {"file_id": ["file-1"], "file_name": ["Renamed.mkv"]},
                ),
            ],
        ),
        (
            "cookie",
            [
                ("https://webapi.115.com/files/add", {"cname": ["Movies"], "pid": ["parent"]}),
                ("https://webapi.115.com/files/move", {"pid": ["target"], "fid": ["file-1"]}),
                (
                    "https://webapi.115.com/files/batch_rename",
                    {"files_new_name[file-1]": ["Renamed.mkv"]},
                ),
            ],
        ),
    ],
)
def test_mutations_use_exact_backend_url_headers_and_form_without_fallback(
    backend: str,
    expected: list[tuple[str, dict[str, list[str]]]],
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"state": True, "data": {"ignored": "receipt-is-limited"}})

    credentials = MutableCredentials(_snapshot())
    gateway = HttpxP115OrganizeMutationGateway(
        credentials=credentials,
        priority=lambda: "cookie" if backend == "openapi" else "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001", expected_backend=backend)
    assert client is not None

    receipts = [
        client.ensure_directory("parent", "Movies"),
        client.move("file-1", "target"),
        client.rename("file-1", "Renamed.mkv"),
    ]

    assert client.backend == backend
    assert receipts == [
        {"state": True, "http_status": 200, "provider_code": None, "message": ""},
        {"state": True, "http_status": 200, "provider_code": None, "message": ""},
        {"state": True, "http_status": 200, "provider_code": None, "message": ""},
    ]
    assert [(str(request.url), parse_qs(request.content.decode())) for request in requests] == expected
    assert all(request.method == "POST" for request in requests)
    assert all(request.headers["content-type"].startswith("application/x-www-form-urlencoded") for request in requests)
    if backend == "openapi":
        assert all(request.headers["authorization"] == "Bearer fixture-openapi-token" for request in requests)
        assert all("cookie" not in request.headers for request in requests)
    else:
        assert all(request.headers["cookie"] == credentials.current.cookie for request in requests)
        assert all("AlipayClient" in request.headers["user-agent"] for request in requests)
        assert all("authorization" not in request.headers for request in requests)


@pytest.mark.parametrize("failure", ["timeout", "http-503", "invalid-json"])
def test_post_failure_is_never_retried_refreshed_or_sent_to_another_backend(failure: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("fixture timeout", request=request)
        if failure == "http-503":
            return httpx.Response(503, json={"state": False})
        return httpx.Response(200, content=b"not-json")

    gateway = HttpxP115OrganizeMutationGateway(
        credentials=MutableCredentials(_snapshot()),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(P115OrganizeTransportError):
        client.rename("file-1", "Renamed.mkv")

    assert len(requests) == 1
    assert requests[0].url.host == "proapi.115.com"


def test_explicit_provider_rejection_returns_a_limited_receipt_for_authoritative_readback() -> None:
    responses = iter(
        (
            httpx.Response(409, json={"state": False, "message": "conflict"}),
            httpx.Response(200, json={"state": False, "error_msg": "provider rejected", "secret": "not-persisted"}),
        )
    )
    gateway = HttpxP115OrganizeMutationGateway(
        credentials=MutableCredentials(_snapshot()),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(lambda _request: next(responses))),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    assert client.ensure_directory("parent", "Movies") == {
        "state": False,
        "http_status": 409,
        "message": "115 提供方拒绝整理请求",
    }
    assert client.rename("file-1", "Renamed.mkv") == {
        "state": False,
        "http_status": 200,
        "provider_code": None,
        "message": "provider rejected",
    }


def test_credential_and_app_type_fences_distinguish_before_and_after_request_drift() -> None:
    requests: list[httpx.Request] = []
    credentials = MutableCredentials(_snapshot())

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        credentials.current = _snapshot(cookie="replacement-cookie", app_type="web")
        return httpx.Response(200, json={"state": True})

    gateway = HttpxP115OrganizeMutationGateway(
        credentials=credentials,
        priority=lambda: "cookie",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001", expected_backend="cookie")
    assert client is not None

    credentials.current = _snapshot(cookie="rotated-before-request", app_type="web")
    with pytest.raises(P115ProviderNoEffectError, match="发生变化"):
        client.ensure_directory("parent", "Movies")
    assert requests == []

    credentials.current = _snapshot()
    with pytest.raises(P115OrganizeTransportError, match="发生变化"):
        client.ensure_directory("parent", "Movies")
    assert len(requests) == 1


def test_move_to_root_is_rejected_before_any_provider_request() -> None:
    requests: list[httpx.Request] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("move-to-root rejection must happen before HTTP")

    gateway = HttpxP115OrganizeMutationGateway(
        credentials=MutableCredentials(_snapshot()),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(forbidden)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(P115ProviderNoEffectError, match="CID 0"):
        client.move("file-1", "0")
    assert requests == []


def test_open_selects_first_configured_backend_once_and_never_refreshes() -> None:
    requests: list[httpx.Request] = []
    credentials = MutableCredentials(_snapshot(access_token=""))

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"state": True})

    gateway = HttpxP115OrganizeMutationGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )

    client = gateway.open(expected_account_id="115001")
    assert client is not None
    assert client.backend == "cookie"
    client.rename("file-1", "Renamed.mkv")
    assert [request.url.host for request in requests] == ["webapi.115.com"]
