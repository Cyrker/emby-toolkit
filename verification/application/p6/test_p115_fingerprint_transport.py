from __future__ import annotations

from dataclasses import replace
from typing import Any

import httpx
import pytest
from p115pickcode import to_pickcode

from etk.domains.p115 import (
    HttpxP115FingerprintRemoteGateway,
    P115CredentialSnapshot,
    P115FingerprintAccountChanged,
    P115FingerprintConflict,
    P115FingerprintTransportError,
)

FILE_ID = "9001"
PARENT_ID = "8001"
PICK_CODE = str(to_pickcode(int(FILE_ID)))
SHA1 = "A" * 40


class MutableCredentials:
    def __init__(self, snapshot: P115CredentialSnapshot) -> None:
        self.snapshot = snapshot
        self.loads = 0

    def load(self) -> P115CredentialSnapshot:
        self.loads += 1
        return self.snapshot

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("fingerprint readback must not save OpenAPI credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("fingerprint readback must not save Cookie credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


def _snapshot() -> P115CredentialSnapshot:
    return P115CredentialSnapshot(
        access_token="fixture-openapi-token",
        refresh_token="fixture-refresh-token",
        cookie="UID=115001_A1_1; CID=fixture; SEID=fixture",
        app_type="mac",
        account_id="115001",
    )


def _gateway(
    credentials: MutableCredentials,
    handler: httpx.MockTransport,
    *,
    priority: str,
) -> HttpxP115FingerprintRemoteGateway:
    return HttpxP115FingerprintRemoteGateway(
        credentials=credentials,
        priority=lambda: priority,
        client_factory=lambda **kwargs: httpx.Client(transport=handler, **kwargs),
    )


@pytest.mark.parametrize("backend", ["openapi", "cookie"])
def test_http_gateway_normalizes_file_info_and_pins_one_backend(backend: str) -> None:
    credentials = MutableCredentials(_snapshot())
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if backend == "openapi":
            payload = {
                "state": True,
                "data": {
                    "file_id": FILE_ID,
                    "parent_id": PARENT_ID,
                    "file_name": "Fixture.mkv",
                    "sha1": SHA1.lower(),
                    "pick_code": PICK_CODE,
                    "size_byte": "123",
                },
            }
        else:
            payload = {
                "errno": 0,
                "file_info": [
                    {
                        "fid": FILE_ID,
                        "cid": PARENT_ID,
                        "n": "Fixture.mkv",
                        "sha": SHA1,
                        "pc": PICK_CODE,
                        "fs": 123,
                    }
                ],
            }
        return httpx.Response(200, json=payload)

    client = _gateway(
        credentials,
        httpx.MockTransport(respond),
        priority=backend,
    ).open(expected_account_id="115001")

    assert client is not None and client.backend == backend
    value = client.lookup(PICK_CODE)

    assert value is not None
    assert value.file_id == FILE_ID
    assert value.parent_id == PARENT_ID
    assert value.name == "Fixture.mkv"
    assert value.sha1 == SHA1
    assert value.pick_code == PICK_CODE
    assert value.size == 123
    assert value.account_id == "115001"
    assert len(requests) == 1
    assert requests[0].url.params["file_id"] == FILE_ID
    if backend == "openapi":
        assert requests[0].url.path == "/open/folder/get_info"
        assert requests[0].headers["Authorization"] == "Bearer fixture-openapi-token"
        assert "Cookie" not in requests[0].headers
    else:
        assert requests[0].url.path == "/files/file"
        assert requests[0].headers["Cookie"].startswith("UID=115001_A1_1")
        assert "Authorization" not in requests[0].headers


def test_cookie_uses_top_level_breadcrumb_to_resolve_parent_without_openapi() -> None:
    credentials = MutableCredentials(replace(_snapshot(), access_token="", refresh_token=""))

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "errno": 0,
                "file_info": [
                    {
                        "fid": FILE_ID,
                        "n": "Fixture.mkv",
                        "sha": SHA1,
                        "pc": PICK_CODE,
                        "fs": 123,
                    }
                ],
                "breadcrumb": [
                    {"file_id": "0", "file_category": "0"},
                    {"file_id": PARENT_ID, "file_category": "0"},
                    {"file_id": FILE_ID, "file_category": "1"},
                ],
            },
        )

    client = _gateway(
        credentials,
        httpx.MockTransport(respond),
        priority="cookie",
    ).open(expected_account_id="115001")
    assert client is not None and client.backend == "cookie"

    value = client.lookup(PICK_CODE)

    assert value is not None and value.parent_id == PARENT_ID


def test_cookie_missing_parent_uses_same_account_openapi_readback() -> None:
    credentials = MutableCredentials(_snapshot())
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        data = {
            "file_id": FILE_ID,
            "file_name": "Fixture.mkv",
            "sha1": SHA1,
            "pick_code": PICK_CODE,
            "size": 123,
        }
        if request.url.path == "/open/folder/get_info":
            data["parent_id"] = PARENT_ID
        return httpx.Response(200, json={"state": True, "data": data})

    client = _gateway(
        credentials,
        httpx.MockTransport(respond),
        priority="cookie",
    ).open(expected_account_id="115001")
    assert client is not None and client.backend == "cookie"

    value = client.lookup(PICK_CODE)

    assert value is not None and value.parent_id == PARENT_ID
    assert [request.url.path for request in requests] == [
        "/files/file",
        "/open/folder/get_info",
    ]
    assert requests[0].headers["Cookie"].startswith("UID=115001_A1_1")
    assert requests[1].headers["Authorization"] == "Bearer fixture-openapi-token"


def test_cookie_only_missing_parent_fails_current_asset_without_guessing() -> None:
    credentials = MutableCredentials(replace(_snapshot(), access_token="", refresh_token=""))
    client = _gateway(
        credentials,
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "state": True,
                    "data": {
                        "file_id": FILE_ID,
                        "file_name": "Fixture.mkv",
                        "sha1": SHA1,
                        "pick_code": PICK_CODE,
                        "size": 123,
                    },
                },
            )
        ),
        priority="cookie",
    ).open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(P115FingerprintConflict, match="parent"):
        client.lookup(PICK_CODE)


def test_gateway_selects_available_primary_when_preferred_credential_is_missing() -> None:
    credentials = MutableCredentials(replace(_snapshot(), cookie=""))
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            json={
                "state": True,
                "data": {
                    "file_id": FILE_ID,
                    "parent_id": PARENT_ID,
                    "file_name": "Fixture.mkv",
                    "sha1": SHA1,
                    "pick_code": PICK_CODE,
                    "size": 1,
                },
            },
        )
    )

    client = _gateway(credentials, transport, priority="cookie").open(expected_account_id="115001")

    assert client is not None and client.backend == "openapi"
    assert client.lookup(PICK_CODE) is not None


def test_lookup_rejects_account_credential_and_cookie_app_drift() -> None:
    for changed in (
        replace(_snapshot(), account_id="115002"),
        replace(_snapshot(), access_token="replacement-token"),
        replace(_snapshot(), app_type="tv"),
    ):
        credentials = MutableCredentials(_snapshot())
        backend = "cookie" if changed.app_type == "tv" else "openapi"

        def respond(
            _request: httpx.Request,
            target: P115CredentialSnapshot = changed,
            store: MutableCredentials = credentials,
        ) -> httpx.Response:
            store.snapshot = target
            return httpx.Response(
                200,
                json={
                    "state": True,
                    "data": {
                        "file_id": FILE_ID,
                        "parent_id": PARENT_ID,
                        "file_name": "Fixture.mkv",
                        "sha1": SHA1,
                        "pick_code": PICK_CODE,
                        "size": 1,
                    },
                },
            )

        client = _gateway(
            credentials,
            httpx.MockTransport(respond),
            priority=backend,
        ).open(expected_account_id="115001")
        assert client is not None

        with pytest.raises(P115FingerprintAccountChanged):
            client.lookup(PICK_CODE)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(503, json={"state": False}), "HTTP 503"),
        (httpx.Response(200, content=b"not-json"), "invalid JSON"),
        (httpx.Response(200, json=[{"state": True}]), "invalid response object"),
        (httpx.Response(200, json={"state": True, "data": "bad"}), "invalid object"),
    ],
)
def test_lookup_rejects_http_and_payload_failures(
    response: httpx.Response,
    message: str,
) -> None:
    credentials = MutableCredentials(_snapshot())
    client = _gateway(
        credentials,
        httpx.MockTransport(lambda _request: response),
        priority="openapi",
    ).open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(P115FingerprintTransportError, match=message):
        client.lookup(PICK_CODE)


@pytest.mark.parametrize(
    "code",
    [404, 20018, 31001, 31003, 31004, 50003, 50015, 70005, 70008, 90008, 430004, 800001],
)
def test_lookup_maps_confirmed_not_found_codes_to_none(code: int) -> None:
    credentials = MutableCredentials(_snapshot())
    client = _gateway(
        credentials,
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"state": False, "errno": code, "message": "fixture not found"},
            )
        ),
        priority="openapi",
    ).open(expected_account_id="115001")
    assert client is not None

    assert client.lookup(PICK_CODE) is None


def test_lookup_maps_http_404_to_none() -> None:
    credentials = MutableCredentials(_snapshot())
    client = _gateway(
        credentials,
        httpx.MockTransport(lambda _request: httpx.Response(404)),
        priority="openapi",
    ).open(expected_account_id="115001")
    assert client is not None

    assert client.lookup(PICK_CODE) is None


def test_primary_backend_failure_does_not_trigger_cross_backend_replay() -> None:
    credentials = MutableCredentials(_snapshot())
    requests: list[httpx.Request] = []

    def fail(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503, json={"state": False})

    client = _gateway(
        credentials,
        httpx.MockTransport(fail),
        priority="cookie",
    ).open(expected_account_id="115001")
    assert client is not None and client.backend == "cookie"

    with pytest.raises(P115FingerprintTransportError, match="HTTP 503"):
        client.lookup(PICK_CODE)
    assert [request.url.path for request in requests] == ["/files/file"]


def test_lookup_retries_transport_errors_without_switching_backend() -> None:
    credentials = MutableCredentials(_snapshot())
    calls = 0

    def fail(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("fixture unavailable", request=request)

    client = _gateway(
        credentials,
        httpx.MockTransport(fail),
        priority="openapi",
    ).open(expected_account_id="115001")
    assert client is not None and client.backend == "openapi"

    with pytest.raises(P115FingerprintTransportError, match="transport failed"):
        client.lookup(PICK_CODE)
    assert calls == 3


@pytest.mark.parametrize(
    "changes",
    [
        {"file_id": "9002"},
        {"pick_code": "a1h000"},
        {"sha": "B" * 40},
        {"paths": [{"file_id": "8002"}]},
        {"fc": "0"},
        {"fc": "0", "file_category": "1"},
        {"fn": "Other.mkv"},
        {"parent_id": FILE_ID},
        {"size_byte": -1},
        {"size": None},
        {"size": "1.5"},
    ],
)
def test_lookup_rejects_identity_and_field_conflicts(changes: dict[str, object]) -> None:
    credentials = MutableCredentials(_snapshot())
    data: dict[str, object] = {
        "file_id": FILE_ID,
        "parent_id": PARENT_ID,
        "file_name": "Fixture.mkv",
        "sha1": SHA1,
        "pick_code": PICK_CODE,
        "size": 1,
    }
    data.update(changes)
    client = _gateway(
        credentials,
        httpx.MockTransport(lambda _request: httpx.Response(200, json={"state": True, "data": data})),
        priority="openapi",
    ).open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(P115FingerprintConflict):
        client.lookup(PICK_CODE)
