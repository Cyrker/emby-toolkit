from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
import pytest

from etk.domains.p115 import (
    HttpxP115DirectoryGateway,
    HttpxP115DirectoryTreeGateway,
    HttpxP115SortingRuleDirectoryGateway,
    HttpxP115StrmSyncGateway,
    P115CredentialSnapshot,
    P115DirectoryTransportError,
    P115DirectUrl,
)


class FakeCredentials:
    def __init__(self, snapshots: Sequence[P115CredentialSnapshot]) -> None:
        self._snapshots = list(snapshots)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self._snapshots) - 1)
        self.calls += 1
        return self._snapshots[index]

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("directory transport must not refresh or save credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("directory transport must not save credentials")

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


def test_openapi_transport_pins_backend_and_sends_exact_authoritative_page_request() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "state": True,
                "count": 1,
                "data": [{"file_id": "dir-open", "file_name": "Open", "type": 0}],
            },
        )

    credentials = FakeCredentials([_snapshot()])
    gateway = HttpxP115DirectoryTreeGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )

    client = gateway.open(expected_account_id="115001")
    assert client is not None
    page = client.list_page(cid="root-a", limit=1000, offset=2000)

    assert page["state"] is True
    assert page["count"] == 1
    assert page["_etk_authoritative_list_payload"] is True
    assert page["_etk_api_label_lower"] == "openapi"
    assert page["data"] == [
        {
            "file_id": "dir-open",
            "file_name": "Open",
            "type": 0,
            "fid": "dir-open",
            "fn": "Open",
            "n": "Open",
            "name": "Open",
            "fc": "0",
            "file_category": "0",
        }
    ]
    assert len(requests) == 1
    request = requests[0]
    assert str(request.url.copy_with(query=None)) == "https://proapi.115.com/open/ufile/files"
    assert dict(request.url.params) == {
        "cid": "root-a",
        "limit": "1000",
        "offset": "2000",
        "record_open_time": "0",
        "count_folders": "0",
        "show_dir": "1",
    }
    assert request.headers["authorization"] == "Bearer fixture-openapi-token"
    assert request.headers["user-agent"] == "Emby-toolkit/1.0 (OpenAPI)"
    assert "cookie" not in request.headers


def test_native_directory_picker_uses_bounded_list_and_search_endpoints() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"state": True, "data": []})

    tree = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(cookie="")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    gateway = HttpxP115DirectoryGateway(tree)

    assert gateway.list_entries(cid=7, search="", expected_account_id="115001") is not None
    assert gateway.list_entries(cid=8, search="fixture", expected_account_id="115001") is not None

    assert [str(request.url.copy_with(query=None)) for request in requests] == [
        "https://proapi.115.com/open/ufile/files",
        "https://proapi.115.com/open/ufile/search",
    ]
    assert dict(requests[0].url.params)["cid"] == "7"
    assert dict(requests[0].url.params).get("search_value") is None
    assert dict(requests[1].url.params)["cid"] == "8"
    assert dict(requests[1].url.params)["search_value"] == "fixture"


def test_native_sorting_rule_directory_resolves_account_bound_path() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "state": True,
                "data": {
                    "path": [
                        {"cid": "0", "name": "root"},
                        {"cid": "media", "name": "Media"},
                    ],
                    "list": [],
                },
            },
        )

    tree = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(cookie="")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    gateway = HttpxP115SortingRuleDirectoryGateway(tree)

    assert gateway.is_available(expected_account_id="115001") is True
    assert gateway.resolve_path(cid="media", expected_account_id="115001") == (
        {"cid": "0", "name": "root"},
        {"cid": "media", "name": "Media"},
    )


def test_cookie_transport_uses_priority_once_and_normalizes_cookie_directory_aliases() -> None:
    priority = ["cookie"]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "errno": 0,
                "count": "1",
                "data": {"list": [{"cid": "dir-cookie", "pid": "root-a", "n": "Cookie"}]},
            },
        )

    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot()]),
        priority=lambda: priority[0],
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None
    priority[0] = "openapi"

    page = client.list_page(cid="root-a", limit=25, offset=0)

    assert page["_etk_api_label_lower"] == "cookie"
    assert page["data"][0]["fid"] == "dir-cookie"
    assert page["data"][0]["fn"] == "Cookie"
    assert page["data"][0]["fc"] == "0"
    request = requests[0]
    assert str(request.url.copy_with(query=None)) == "https://webapi.115.com/files"
    assert dict(request.url.params) == {
        "cid": "root-a",
        "limit": "25",
        "offset": "0",
        "record_open_time": "0",
        "count_folders": "0",
        "aid": "1",
        "show_dir": "1",
    }
    assert request.headers["cookie"].startswith("UID=115001_fixture")
    assert "AlipayClient" in request.headers["user-agent"]
    assert "authorization" not in request.headers


def test_missing_preferred_credential_falls_back_before_backend_is_pinned() -> None:
    requests: list[httpx.Request] = []
    transport = httpx.MockTransport(
        lambda request: requests.append(request) or httpx.Response(200, json={"state": True, "data": []})
    )
    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(access_token="")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(transport),
    )

    client = gateway.open(expected_account_id="115001")
    assert client is not None
    page = client.list_page(cid="root-a", limit=1000, offset=0)

    assert page["_etk_api_label_lower"] == "cookie"
    assert requests[0].url.host == "webapi.115.com"


def test_preferred_backend_failure_falls_back_once_then_pins_the_successful_backend() -> None:
    hosts: list[str] = []
    openapi_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal openapi_calls
        hosts.append(str(request.url.host))
        if request.url.host == "proapi.115.com":
            openapi_calls += 1
            return httpx.Response(
                200,
                json={"state": False, "errno": 40140125, "error": "fixture expired token"},
            )
        offset = int(request.url.params["offset"])
        data = [{"cid": "dir-cookie", "n": "Cookie"}] if offset == 0 else []
        return httpx.Response(200, json={"state": True, "count": 1, "data": data})

    credentials = FakeCredentials([_snapshot()])
    gateway = HttpxP115DirectoryTreeGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    first = client.list_page(cid="root-a", limit=1000, offset=0)
    second = client.list_page(cid="root-a", limit=1000, offset=1)

    assert first["_etk_api_label_lower"] == "cookie"
    assert second["_etk_api_label_lower"] == "cookie"
    assert hosts == ["proapi.115.com", "webapi.115.com", "webapi.115.com"]
    assert openapi_calls == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"state": True, "data": []},
        {"state": True, "data": {"list": []}},
        {"state": True, "data": {"items": []}},
        {"state": True, "data": {"files": []}},
        {"state": True, "data": {"data": []}},
        {"state": True, "list": []},
        {"state": True, "items": []},
        {"state": True, "files": []},
    ],
)
def test_all_supported_list_carriers_preserve_authoritative_empty(payload: Mapping[str, object]) -> None:
    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(cookie="")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    page = client.list_page(cid="root-a", limit=1000, offset=0)

    assert page["state"] is True
    assert page["data"] == []
    assert page["_etk_authoritative_list_payload"] is True


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (httpx.Response(200, json={"state": True}), "缺少权威列表载荷"),
        (
            httpx.Response(
                200,
                json={
                    "state": True,
                    "data": [],
                    "_etk_authoritative_list_payload": False,
                },
            ),
            "缺少权威列表载荷",
        ),
        (httpx.Response(200, json=[{"fid": "not-an-object"}]), "无效响应结构"),
        (httpx.Response(200, text="not-json"), "无效 JSON"),
    ],
)
def test_invalid_or_compatibility_generated_payloads_fail_closed(
    response: httpx.Response,
    error: str,
) -> None:
    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot()]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(lambda _request: response)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(P115DirectoryTransportError, match=error):
        client.list_page(cid="root-a", limit=1000, offset=0)


def test_transport_error_has_a_finite_retry_budget() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("fixture timeout", request=request)

    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(cookie="")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(httpx.ReadTimeout, match="fixture timeout"):
        client.list_page(cid="root-a", limit=1000, offset=0)

    assert attempts == 3


def test_account_mismatch_or_drift_fails_before_any_provider_request() -> None:
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"state": True, "data": []})

    mismatch = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(account_id="115002")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    with pytest.raises(RuntimeError, match="客户端创建前发生变化"):
        mismatch.open(expected_account_id="115001")

    drift = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(), _snapshot(account_id="115002")]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = drift.open(expected_account_id="115001")
    assert client is not None
    with pytest.raises(RuntimeError, match="目录同步期间发生变化"):
        client.list_page(cid="root-a", limit=1000, offset=0)

    credential_drift = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials(
            [
                _snapshot(),
                _snapshot(access_token="replacement-openapi-token"),
            ]
        ),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = credential_drift.open(expected_account_id="115001")
    assert client is not None
    with pytest.raises(RuntimeError, match="凭据在目录同步期间发生变化"):
        client.list_page(cid="root-a", limit=1000, offset=0)

    assert requests == 0


def test_credential_change_after_failed_request_blocks_backend_fallback() -> None:
    hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(str(request.url.host))
        if request.url.host == "proapi.115.com":
            return httpx.Response(503, json={"state": False, "error": "fixture unavailable"})
        return httpx.Response(200, json={"state": True, "data": []})

    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials(
            [
                _snapshot(),
                _snapshot(),
                _snapshot(access_token="replacement-openapi-token"),
            ]
        ),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None

    with pytest.raises(RuntimeError, match="凭据在目录同步期间发生变化"):
        client.list_page(cid="root-a", limit=1000, offset=0)

    assert hosts == ["proapi.115.com"]


def test_missing_credentials_returns_no_client_without_network_access() -> None:
    gateway = HttpxP115DirectoryTreeGateway(
        credentials=FakeCredentials([_snapshot(access_token="", cookie="")]),
        priority=lambda: "openapi",
        client_factory=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("network client constructed")),
    )

    assert gateway.open(expected_account_id="115001") is None


def test_strm_transport_reuses_pinned_backend_for_type_path_and_bounded_subtitle() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "fixture.invalid":
            return httpx.Response(200, content=b"fixture-subtitle")
        return httpx.Response(
            200,
            json={
                "state": True,
                "data": {
                    "path": [{"cid": "0", "name": "root"}, {"cid": "media", "name": "Media"}],
                    "list": [],
                },
            },
        )

    class DirectUrls:
        def resolve_backend(self, *_args: Any, **_kwargs: Any) -> P115DirectUrl:
            return P115DirectUrl(
                url="https://fixture.invalid/subtitle.srt",
                user_agent="fixture-agent",
                backend="openapi",
                method="fixture",
            )

    gateway = HttpxP115StrmSyncGateway(
        credentials=FakeCredentials([_snapshot(cookie="")]),
        priority=lambda: "openapi",
        direct_urls=DirectUrls(),  # type: ignore[arg-type]
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001")
    assert client is not None
    assert client.backend == ""

    typed = client.list_page(cid="media", file_type=4, limit=10, offset=0)
    assert client.backend == "openapi"
    path = client.directory_path("media")
    content = client.download_subtitle("fixture-pick-code", max_bytes=64)

    assert typed["_etk_api_label_lower"] == "openapi"
    assert path == ({"cid": "0", "name": "root"}, {"cid": "media", "name": "Media"})
    assert content == b"fixture-subtitle"
    assert dict(requests[0].url.params)["type"] == "4"
    assert requests[-1].headers["user-agent"] == "fixture-agent"
