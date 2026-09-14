from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from etk.compatibility.p115_credentials import (
    clear_persistent_p115_credential_gateway,
    install_persistent_p115_credential_gateway,
    invalidate_legacy_p115_credential_clients,
)
from etk.domains.p115 import (
    HttpxP115AuthorizationGateway,
    LegacyP115CredentialStore,
    LegacyP115DirectoryGateway,
    LegacyP115DirectoryTreeGateway,
)
from handler import p115_service


def _factory(handler: Callable[[httpx.Request], httpx.Response]):
    transport = httpx.MockTransport(handler)

    def create(**kwargs: Any) -> httpx.Client:
        return httpx.Client(transport=transport, **kwargs)

    return create


def test_refresh_is_direct_and_does_not_retry_ambiguous_transport_failure():
    calls = []
    options = []

    def respond(request):
        calls.append(request)
        assert request.url.host == "passportapi.115.com" and request.url.path == "/open/refreshToken"
        assert parse_qs(request.content.decode()) == {"refresh_token": ["fixture-refresh"]}
        raise httpx.ReadTimeout("fixture timeout")

    def client_factory(**kwargs):
        options.append(kwargs)
        return httpx.Client(transport=httpx.MockTransport(respond), **kwargs)

    gateway = HttpxP115AuthorizationGateway(client_factory=client_factory)
    with pytest.raises(httpx.ReadTimeout):
        gateway.refresh_openapi("fixture-refresh")
    assert len(calls) == 1 and options[0]["trust_env"] is False


def test_openapi_qrcode_exchange_uses_pkce_and_never_exposes_tokens_in_repr() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/open/authDeviceCode":
            form = parse_qs(request.content.decode())
            assert form["client_id"] == ["fixture-app"]
            assert form["code_challenge_method"] == ["sha256"]
            assert len(form["code_challenge"][0]) == 43
            return httpx.Response(
                200,
                json={
                    "state": True,
                    "data": {"qrcode": "fixture-qr", "uid": "fixture-uid", "time": 1, "sign": "sign"},
                },
            )
        if request.url.path == "/get/status/":
            assert dict(request.url.params) == {"uid": "fixture-uid", "time": "1", "sign": "sign"}
            return httpx.Response(200, json={"state": 1, "data": {"status": 2}})
        if request.url.path == "/open/deviceCodeToToken":
            form = parse_qs(request.content.decode())
            assert form["uid"] == ["fixture-uid"]
            assert 43 <= len(form["code_verifier"][0]) <= 128
            return httpx.Response(
                200,
                json={
                    "state": True,
                    "data": {"access_token": "fixture-access", "refresh_token": "fixture-refresh"},
                },
            )
        if request.url.path == "/open/user/info":
            assert request.headers["authorization"] == "Bearer fixture-access"
            return httpx.Response(200, json={"state": True, "data": {"user_id": "115001"}})
        raise AssertionError(request.url)

    gateway = HttpxP115AuthorizationGateway(client_factory=_factory(handler), clock=lambda: 10)
    data, session = gateway.create_openapi_qrcode("fixture-app")
    outcome = gateway.poll_openapi_qrcode(session)

    assert data == {"qrcode": "fixture-qr", "uid": "fixture-uid", "time": 1, "sign": "sign"}
    assert outcome.status == "success"
    assert outcome.user_info == {"user_id": "115001"}
    assert outcome.access_token == "fixture-access"
    assert outcome.refresh_token == "fixture-refresh"
    assert "fixture-access" not in repr(outcome)
    assert "code_verifier" not in repr(session)
    assert [request.url.path for request in requests] == [
        "/open/authDeviceCode",
        "/get/status/",
        "/open/deviceCodeToToken",
        "/open/user/info",
    ]


def test_cookie_qrcode_exchange_preserves_app_user_agent_and_response_cookies() -> None:
    seen_user_agents: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_user_agents.append(request.headers.get("user-agent", ""))
        if request.url.path.endswith("/token/"):
            assert request.url.params["app"] == "alipaymini"
            return httpx.Response(
                200,
                json={"state": 1, "data": {"qrcode": "cookie-qr", "uid": "cookie-uid", "time": 2, "sign": "s"}},
            )
        if request.url.path == "/get/status/":
            return httpx.Response(200, json={"state": 1, "data": {"status": 2}})
        if request.url.path.endswith("/login/qrcode"):
            return httpx.Response(
                200,
                json={"state": 1, "data": {}},
                headers=[("set-cookie", "UID=115001_fixture; Path=/"), ("set-cookie", "CID=fixture; Path=/")],
            )
        raise AssertionError(request.url)

    gateway = HttpxP115AuthorizationGateway(client_factory=_factory(handler), clock=lambda: 20)
    _data, session = gateway.create_cookie_qrcode("alipaymini")
    outcome = gateway.poll_cookie_qrcode(session)

    assert outcome.status == "success"
    assert outcome.cookie == "UID=115001_fixture; CID=fixture"
    assert outcome.app_type == "alipaymini"
    assert "Alipay" in seen_user_agents[0]
    assert "Alipay" in seen_user_agents[1]
    assert "Alipay" in seen_user_agents[2]


def test_read_only_status_probe_retries_transport_errors_but_non_idempotent_create_does_not() -> None:
    status_attempts = 0

    def status_handler(request: httpx.Request) -> httpx.Response:
        nonlocal status_attempts
        status_attempts += 1
        if status_attempts < 3:
            raise httpx.ConnectError("fixture unavailable", request=request)
        return httpx.Response(200, json={"state": True, "data": {"user_id": "115001"}})

    gateway = HttpxP115AuthorizationGateway(client_factory=_factory(status_handler))
    assert gateway.check_openapi("fixture-token")["state"] is True
    assert status_attempts == 3

    create_attempts = 0

    def create_handler(request: httpx.Request) -> httpx.Response:
        nonlocal create_attempts
        create_attempts += 1
        raise httpx.ConnectError("fixture unavailable", request=request)

    gateway = HttpxP115AuthorizationGateway(client_factory=_factory(create_handler))
    with pytest.raises(httpx.ConnectError):
        gateway.create_openapi_qrcode("fixture-app")
    assert create_attempts == 1


def test_legacy_credential_adapter_delegates_to_atomic_account_bound_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saves: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    monkeypatch.setattr(
        p115_service,
        "get_115_tokens",
        lambda: ("fixture-access", "fixture-refresh", "UID=115001_fixture", "web"),
    )
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_service,
        "save_115_tokens",
        lambda *args, **kwargs: saves.append((args, kwargs)),
    )
    store = LegacyP115CredentialStore()

    snapshot = store.load()
    store.save_openapi(None, None, account_id="115001", expected_access_token="fixture-access")
    store.save_cookie("UID=115001_fixture", "web")

    assert snapshot.account_id == "115001"
    assert snapshot.has_openapi and snapshot.has_cookie
    assert "fixture-access" not in repr(snapshot)
    assert saves == [
        (
            (None, None),
            {
                "account_id": "115001",
                "credential_type": "openapi",
                "expected_access_token": "fixture-access",
            },
        ),
        ((None, None, "UID=115001_fixture", "web"), {"credential_type": "cookie"}),
    ]


def test_active_legacy_credential_calls_use_installed_native_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Gateway:
        def __init__(self) -> None:
            self.record = {
                "access_token": "fixture-access",
                "refresh_token": "fixture-refresh",
                "cookie": "UID=115001_fixture",
                "app_type": "web",
                "account_id": "115001",
                "openapi_account_id": "115001",
                "cookie_account_id": "115001",
            }
            self.saves: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def load_record(self) -> Mapping[str, Any]:
            return dict(self.record)

        def save_bundle(self, *args: Any, **kwargs: Any) -> bool:
            self.saves.append((args, kwargs))
            return True

    gateway = Gateway()
    monkeypatch.setattr(
        p115_service.settings_db,
        "get_setting",
        lambda _key: (_ for _ in ()).throw(AssertionError("legacy settings reader used")),
    )
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda _key, _value: (_ for _ in ()).throw(AssertionError("legacy settings writer used")),
    )
    clear_persistent_p115_credential_gateway()
    install_persistent_p115_credential_gateway(gateway)
    try:
        assert p115_service.get_115_tokens() == (
            "fixture-access",
            "fixture-refresh",
            "UID=115001_fixture",
            "web",
        )
        assert p115_service.get_115_account_id() == "115001"
        p115_service.save_115_tokens(
            "fixture-access-2",
            "fixture-refresh-2",
            account_id="115001",
            credential_type="openapi",
            expected_access_token="fixture-access",
            expected_refresh_token="fixture-refresh",
        )
    finally:
        clear_persistent_p115_credential_gateway(gateway)

    assert gateway.saves == [
        (
            ("fixture-access-2", "fixture-refresh-2"),
            {
                "cookie": None,
                "app_type": None,
                "account_id": "115001",
                "credential_type": "openapi",
                "expected_access_token": "fixture-access",
                "expected_refresh_token": "fixture-refresh",
            },
        )
    ]


def test_native_credential_change_invalidates_token_cookie_and_app_type_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resets: list[str] = []
    monkeypatch.setattr(p115_service.P115Service, "_openapi_client", object())
    monkeypatch.setattr(p115_service.P115Service, "_token_cache", ("fixture-token", "115001"))
    monkeypatch.setattr(
        p115_service.P115Service,
        "reset_cookie_client",
        staticmethod(lambda: resets.append("cookie")),
    )

    invalidate_legacy_p115_credential_clients()

    assert p115_service.P115Service._openapi_client is None
    assert p115_service.P115Service._token_cache is None
    assert resets == ["cookie"]


def test_legacy_directory_adapter_pins_the_expected_account_and_selects_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def fs_files(self, payload: dict[str, Any]):
            self.calls.append(("files", payload))
            return {"state": True, "data": []}

        def fs_search(self, payload: dict[str, Any]):
            self.calls.append(("search", payload))
            return {"state": True, "data": []}

    client = Client()
    expected_accounts: list[str | None] = []

    def get_client(*, expected_account_id: str | None = None):
        expected_accounts.append(expected_account_id)
        return client

    monkeypatch.setattr(p115_service.P115Service, "get_client", get_client)
    gateway = LegacyP115DirectoryGateway()

    gateway.list_entries(cid=7, search="", expected_account_id="115001")
    gateway.list_entries(cid=8, search="fixture", expected_account_id="115001")

    assert expected_accounts == ["115001", "115001"]
    assert client.calls == [
        ("files", {"cid": 7, "limit": 1000}),
        ("search", {"cid": 8, "limit": 1000, "search_value": "fixture"}),
    ]


def test_directory_tree_adapter_pins_each_page_to_the_opened_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def fs_files(self, payload: dict[str, Any]):
            self.calls.append(payload)
            return {
                "state": True,
                "data": [],
                "_etk_authoritative_list_payload": True,
                "_etk_api_label": "OpenAPI",
            }

    client = Client()
    current_account = ["115001"]
    expected_accounts: list[str | None] = []
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: current_account[0])

    def get_client(*, expected_account_id: str | None = None):
        expected_accounts.append(expected_account_id)
        return client

    monkeypatch.setattr(p115_service.P115Service, "get_client", get_client)
    opened = LegacyP115DirectoryTreeGateway().open(expected_account_id="115001")
    assert opened is not None

    assert opened.list_page(cid="root-a", limit=1000, offset=2000) == {
        "state": True,
        "data": [],
        "_etk_authoritative_list_payload": True,
        "_etk_api_label": "OpenAPI",
    }
    current_account[0] = "115002"
    with pytest.raises(RuntimeError, match="账号在目录同步期间发生变化"):
        opened.list_page(cid="root-a", limit=1000, offset=3000)

    assert expected_accounts == ["115001"]
    assert client.calls == [
        {
            "cid": "root-a",
            "limit": 1000,
            "offset": 2000,
            "record_open_time": 0,
            "count_folders": 0,
        }
    ]


@pytest.mark.parametrize("raw", [{"state": True}, {"errno": 0}])
def test_directory_tree_adapter_rejects_compatibility_generated_empty_pages(
    monkeypatch: pytest.MonkeyPatch,
    raw: dict[str, Any],
) -> None:
    normalized = p115_service._p115_normalize_list_response(p115_service._p115_normalize_list_response(raw))

    class Client:
        def fs_files(self, _payload: dict[str, Any]):
            return normalized

    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        lambda *, expected_account_id=None: Client(),
    )
    opened = LegacyP115DirectoryTreeGateway().open(expected_account_id="115001")
    assert opened is not None

    with pytest.raises(RuntimeError, match="缺少权威列表载荷"):
        opened.list_page(cid="root-a", limit=1000, offset=0)

    assert normalized["state"] is True
    assert normalized["data"] == []
    assert normalized["_etk_authoritative_list_payload"] is False


def test_directory_tree_adapter_accepts_explicit_authoritative_empty_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = p115_service._p115_normalize_list_response({"state": True, "data": []})
    normalized["_etk_api_label"] = "OpenAPI"

    class Client:
        def fs_files(self, _payload: dict[str, Any]):
            return normalized

    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        lambda *, expected_account_id=None: Client(),
    )
    opened = LegacyP115DirectoryTreeGateway().open(expected_account_id="115001")
    assert opened is not None

    assert opened.list_page(cid="root-a", limit=1000, offset=0) == normalized
    assert normalized["_etk_authoritative_list_payload"] is True


@pytest.mark.parametrize(
    "raw",
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
def test_list_normalizer_preserves_all_supported_authoritative_list_carriers(raw: dict[str, Any]) -> None:
    normalized = p115_service._p115_normalize_list_response(p115_service._p115_normalize_list_response(raw))

    assert normalized["state"] is True
    assert normalized["data"] == []
    assert normalized["_etk_authoritative_list_payload"] is True


def test_directory_tree_adapter_rejects_backend_switch_between_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        (
            {
                "state": True,
                "data": [{"fid": "one", "fn": "One", "fc": 0}],
                "_etk_authoritative_list_payload": True,
                "_etk_api_label": "OpenAPI",
            },
            {
                "state": True,
                "data": [],
                "_etk_authoritative_list_payload": True,
                "_etk_api_label": "Cookie",
            },
        )
    )

    class Client:
        def fs_files(self, _payload: dict[str, Any]):
            return next(responses)

    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        lambda *, expected_account_id=None: Client(),
    )
    opened = LegacyP115DirectoryTreeGateway().open(expected_account_id="115001")
    assert opened is not None

    opened.list_page(cid="root-a", limit=1000, offset=0)
    with pytest.raises(RuntimeError, match="API 后端发生变化"):
        opened.list_page(cid="root-a", limit=1000, offset=1)
