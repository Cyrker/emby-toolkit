from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest
from p115client import P115Client, P115OpenClient

from etk.domains.p115 import P115CredentialSnapshot, P115SdkDirectUrlGateway, direct_url_transport

PICK_CODE = "a1b2c3"
ACCOUNT_ID = "115001"
USER_AGENT = "fixture-user-agent"


class MutableCredentials:
    def __init__(self, snapshot: P115CredentialSnapshot) -> None:
        self.snapshot = snapshot
        self.loads = 0

    def load(self) -> P115CredentialSnapshot:
        self.loads += 1
        return self.snapshot

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("direct URL resolution must not save OpenAPI credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("direct URL resolution must not save Cookie credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeOpenApiClient:
    def __init__(self, result: object, *, on_call: Any = None) -> None:
        self.result = result
        self.on_call = on_call
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def download_url_info_open(self, pick_code: str, **kwargs: Any) -> object:
        self.calls.append((pick_code, kwargs))
        if self.on_call is not None:
            self.on_call()
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class FakeCookieClient:
    def __init__(self, result: object, *, on_call: Any = None) -> None:
        self.result = result
        self.on_call = on_call
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def download_url_app(self, pick_code: str, **kwargs: Any) -> object:
        self.calls.append((pick_code, kwargs))
        if self.on_call is not None:
            self.on_call()
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def _snapshot() -> P115CredentialSnapshot:
    return P115CredentialSnapshot(
        access_token="fixture-openapi-token",
        refresh_token="fixture-refresh-token",
        cookie="UID=115001_A1_1; CID=fixture; SEID=fixture",
        app_type="mac",
        account_id=ACCOUNT_ID,
    )


def test_openapi_priority_uses_access_token_only_and_returns_sdk_url() -> None:
    credentials = MutableCredentials(_snapshot())
    client = FakeOpenApiClient("https://download.invalid/openapi.mkv")
    factory_calls: list[str] = []

    def openapi_factory(access_token: str) -> FakeOpenApiClient:
        factory_calls.append(access_token)
        return client

    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=openapi_factory,
        cookie_client_factory=lambda *_args: pytest.fail("Cookie fallback must not run"),
    )

    resolved = gateway.resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)

    assert resolved is not None
    assert resolved.url == "https://download.invalid/openapi.mkv"
    assert resolved.user_agent == USER_AGENT
    assert resolved.backend == "openapi"
    assert resolved.method == "p115client.download_url_info_open"
    assert factory_calls == ["fixture-openapi-token"]
    assert client.calls == [
        (
            PICK_CODE,
            {
                "user_agent": USER_AGENT,
                "base_url": "https://proapi.115.com",
                "timeout": 15.0,
            },
        )
    ]


def test_account_bound_direct_url_accepts_an_opaque_local_pool_identity() -> None:
    local_account_id = "local-pool-account-uuid"
    credentials = MutableCredentials(replace(_snapshot(), account_id=local_account_id, cookie=""))
    client = FakeOpenApiClient("https://download.invalid/pool-openapi.mkv")
    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=lambda _token: client,
    )

    resolved = gateway.resolve_backend(
        PICK_CODE,
        expected_account_id=local_account_id,
        expected_backend="openapi",
        user_agent=USER_AGENT,
    )

    assert resolved is not None
    assert resolved.backend == "openapi"
    assert resolved.url == "https://download.invalid/pool-openapi.mkv"


def test_default_sdk_factories_disable_openapi_refresh_and_preserve_cookie_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials = MutableCredentials(_snapshot())
    constructor_calls: list[tuple[str, dict[str, Any]]] = []

    def openapi_constructor(**kwargs: Any) -> FakeOpenApiClient:
        constructor_calls.append(("openapi", kwargs))
        return FakeOpenApiClient("https://download.invalid/default-openapi.mkv")

    def cookie_constructor(**kwargs: Any) -> FakeCookieClient:
        constructor_calls.append(("cookie", kwargs))
        return FakeCookieClient("https://download.invalid/default-cookie.mkv")

    monkeypatch.setattr(direct_url_transport, "P115OpenClient", openapi_constructor)
    monkeypatch.setattr(direct_url_transport, "P115Client", cookie_constructor)

    openapi = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
    ).resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)
    cookie = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "cookie",
    ).resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)

    assert openapi is not None and openapi.backend == "openapi"
    assert cookie is not None and cookie.backend == "cookie"
    assert constructor_calls == [
        (
            "openapi",
            {
                "access_token": "fixture-openapi-token",
                "refresh_token": "",
                "console_qrcode": False,
            },
        ),
        (
            "cookie",
            {
                "cookies": _snapshot().cookie,
                "app": "mac",
                "console_qrcode": False,
            },
        ),
    ]


def test_locked_sdk_low_level_calls_do_not_run_pickcode_stable_point_requests() -> None:
    credentials = MutableCredentials(_snapshot())
    openapi_client = P115OpenClient(
        access_token="fixture-openapi-token",
        refresh_token="",
        console_qrcode=False,
    )
    cookie_client = P115Client(
        cookies=_snapshot().cookie,
        app="mac",
        console_qrcode=False,
    )
    requests: list[dict[str, Any]] = []

    def fake_request(**kwargs: Any) -> dict[str, Any]:
        requests.append(kwargs)
        return {
            "state": True,
            "data": {
                "fixture": {
                    "url": {
                        "url": f"https://download.invalid/{len(requests)}.mkv",
                    }
                }
            },
        }

    cast(Any, openapi_client).request = fake_request
    cast(Any, cookie_client).request = fake_request

    openapi = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=cast(Any, lambda _token: openapi_client),
    ).resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)
    cookie = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "cookie",
        cookie_client_factory=cast(Any, lambda _cookie, _app: cookie_client),
    ).resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)

    assert openapi is not None and openapi.url == "https://download.invalid/1.mkv"
    assert cookie is not None and cookie.url == "https://download.invalid/2.mkv"
    assert len(requests) == 2
    assert str(requests[0]["url"]).endswith("/open/ufile/downurl")
    assert "/2.0/ufile/downurl" in str(requests[1]["url"])


def test_cookie_priority_uses_encrypted_windows_endpoint_contract() -> None:
    credentials = MutableCredentials(_snapshot())
    client = FakeCookieClient({"data": {"fixture": {"url": {"url": "https://download.invalid/cookie.mkv"}}}})
    factory_calls: list[tuple[str, str]] = []

    def cookie_factory(cookie: str, app_type: str) -> FakeCookieClient:
        factory_calls.append((cookie, app_type))
        return client

    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "cookie",
        openapi_client_factory=lambda *_args: pytest.fail("OpenAPI fallback must not run"),
        cookie_client_factory=cookie_factory,
    )

    resolved = gateway.resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)

    assert resolved is not None
    assert resolved.url == "https://download.invalid/cookie.mkv"
    assert resolved.backend == "cookie"
    assert resolved.method == "p115client.download_url_app"
    assert factory_calls == [(_snapshot().cookie, "mac")]
    assert client.calls == [
        (
            PICK_CODE,
            {
                "user_agent": USER_AGENT,
                "app": "os_windows",
                "base_url": "https://proapi.115.com",
                "timeout": 15.0,
            },
        )
    ]


def test_backend_failure_falls_back_in_configured_order() -> None:
    credentials = MutableCredentials(_snapshot())
    openapi = FakeOpenApiClient(RuntimeError("fixture provider failure"))
    cookie = FakeCookieClient("https://download.invalid/fallback.mkv")
    opened: list[str] = []

    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=lambda _token: (opened.append("openapi"), openapi)[1],
        cookie_client_factory=lambda _cookie, _app: (opened.append("cookie"), cookie)[1],
    )

    resolved = gateway.resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)

    assert resolved is not None and resolved.backend == "cookie"
    assert opened == ["openapi", "cookie"]


def test_pinned_backend_never_falls_back_or_follows_later_priority_changes() -> None:
    credentials = MutableCredentials(_snapshot())
    openapi = FakeOpenApiClient(RuntimeError("fixture provider failure"))
    opened: list[str] = []
    priority = ["cookie"]
    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: priority[0],
        openapi_client_factory=lambda _token: (opened.append("openapi"), openapi)[1],
        cookie_client_factory=lambda _cookie, _app: pytest.fail("pinned backend must not fall back"),
    )

    resolved = gateway.resolve_backend(
        PICK_CODE,
        expected_account_id=ACCOUNT_ID,
        expected_backend="openapi",
        user_agent=USER_AGENT,
    )

    assert resolved is None
    assert opened == ["openapi"]


def test_missing_preferred_credential_uses_available_backend() -> None:
    credentials = MutableCredentials(replace(_snapshot(), access_token="", refresh_token=""))
    cookie = FakeCookieClient("https://download.invalid/cookie-only.mkv")

    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=lambda *_args: pytest.fail("missing OpenAPI credentials must be skipped"),
        cookie_client_factory=lambda _cookie, _app: cookie,
    )

    resolved = gateway.resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT)

    assert resolved is not None and resolved.backend == "cookie"


def test_account_mismatch_rejects_before_opening_a_provider_client() -> None:
    credentials = MutableCredentials(_snapshot())
    calls: list[str] = []
    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=lambda _token: (calls.append("openapi"), FakeOpenApiClient(""))[1],
        cookie_client_factory=lambda _cookie, _app: (calls.append("cookie"), FakeCookieClient(""))[1],
    )

    assert gateway.resolve(PICK_CODE, expected_account_id="115002", user_agent=USER_AGENT) is None
    assert calls == []


@pytest.mark.parametrize("backend", ["openapi", "cookie"])
def test_post_request_credential_or_app_drift_rejects_without_fallback(backend: str) -> None:
    credentials = MutableCredentials(_snapshot())
    openapi_calls: list[str] = []
    cookie_calls: list[str] = []

    def drift() -> None:
        credentials.snapshot = (
            replace(_snapshot(), access_token="replacement-token")
            if backend == "openapi"
            else replace(_snapshot(), app_type="tv")
        )

    openapi = FakeOpenApiClient("https://download.invalid/drift.mkv", on_call=drift)
    cookie = FakeCookieClient("https://download.invalid/drift.mkv", on_call=drift)
    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: backend,
        openapi_client_factory=lambda _token: (openapi_calls.append("openapi"), openapi)[1],
        cookie_client_factory=lambda _cookie, _app: (cookie_calls.append("cookie"), cookie)[1],
    )

    assert gateway.resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT) is None
    if backend == "openapi":
        assert openapi_calls == ["openapi"]
        assert cookie_calls == []
    else:
        assert cookie_calls == ["cookie"]
        assert openapi_calls == []


@pytest.mark.parametrize("value", ["", "ftp://download.invalid/file.mkv", {"data": {"url": "bad"}}])
def test_invalid_or_non_http_provider_url_is_unavailable(value: object) -> None:
    credentials = MutableCredentials(replace(_snapshot(), cookie=""))
    gateway = P115SdkDirectUrlGateway(
        credentials=credentials,
        priority=lambda: "openapi",
        openapi_client_factory=lambda _token: FakeOpenApiClient(value),
    )

    assert gateway.resolve(PICK_CODE, expected_account_id=ACCOUNT_ID, user_agent=USER_AGENT) is None
