from __future__ import annotations

import gzip
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest
from p115client import P115Client, P115OpenClient

from etk.domains.p115 import P115CredentialSnapshot, P115SdkDirectUrlGateway, direct_url_transport
from etk.domains.p115.playback import (
    P115PlaybackStateError,
    PlaybackArtifact,
    PlaybackArtifactStatus,
    PlaybackMode,
    pool_account_revision,
    pool_credential_fingerprint,
)
from etk.domains.p115.playback_native import NativeP115DirectLinkGateway

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


class FakePlaybackConfig:
    def __init__(self, account: dict[str, Any] | None = None) -> None:
        self.value = account

    def account(self, account_id: str) -> dict[str, Any] | None:
        if self.value is None or str(self.value.get("id") or "") != str(account_id):
            return None
        return dict(self.value)


def _artifact(
    *,
    mode: PlaybackMode,
    account_id: str,
    receipt: dict[str, Any],
) -> PlaybackArtifact:
    now = datetime.now(UTC)
    return PlaybackArtifact(
        id=uuid4(),
        artifact_key="a" * 64,
        generation=1,
        operation_id=uuid4(),
        mode=mode,
        account_id=account_id,
        provider_backend="openapi",
        target_cid="target-cid",
        source_sha1="A" * 40,
        source_size=1024,
        source_pick_code="source-pick",
        source_fid="source-fid",
        file_name="Fixture.mkv",
        remote_fid="remote-fid",
        remote_pick_code=PICK_CODE,
        status=PlaybackArtifactStatus.AVAILABLE,
        state_revision=1,
        remote_receipt=receipt,
        reclaim_not_before=now,
        last_verified_at=None,
        reclaimed_at=None,
        created_at=now,
        updated_at=now,
    )


def test_native_direct_link_resolves_primary_artifact_only_with_frozen_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials = MutableCredentials(_snapshot())
    constructor_calls: list[dict[str, Any]] = []

    def constructor(**kwargs: Any) -> FakeOpenApiClient:
        constructor_calls.append(kwargs)
        return FakeOpenApiClient("https://download.invalid/native-primary.mkv")

    monkeypatch.setattr(direct_url_transport, "P115OpenClient", constructor)
    fingerprint = pool_credential_fingerprint(
        {
            "access_token": _snapshot().access_token,
            "cookie": _snapshot().cookie,
            "app_type": _snapshot().app_type,
        },
        "openapi",
    )
    artifact = _artifact(
        mode=PlaybackMode.COPY,
        account_id=ACCOUNT_ID,
        receipt={"account_kind": "primary", "credential_hash": fingerprint},
    )
    gateway = NativeP115DirectLinkGateway(
        cast(Any, FakePlaybackConfig()),
        cast(Any, credentials),
        priority=lambda: "openapi",
    )

    assert gateway.resolve_artifact(artifact, USER_AGENT) == "https://download.invalid/native-primary.mkv"
    credentials.snapshot = replace(_snapshot(), access_token="rotated-token")
    assert gateway.resolve_artifact(artifact, USER_AGENT) == ""
    assert len(constructor_calls) == 1


def test_native_direct_link_resolves_pool_artifact_with_account_revision_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = {
        "id": "local-pool-account",
        "access_token": "fixture-pool-token",
        "refresh_token": "fixture-pool-refresh",
        "cookie": "",
        "app_type": "mac",
        "alias": "Pool",
        "enabled": True,
    }
    monkeypatch.setattr(
        direct_url_transport,
        "P115OpenClient",
        lambda **_kwargs: FakeOpenApiClient("https://download.invalid/native-pool.mkv"),
    )
    artifact = _artifact(
        mode=PlaybackMode.POOL,
        account_id=str(account["id"]),
        receipt={
            "account_kind": "pool",
            "credential_hash": pool_credential_fingerprint(account, "openapi"),
            "account_revision": pool_account_revision(account, "openapi"),
        },
    )
    config = FakePlaybackConfig(account)
    gateway = NativeP115DirectLinkGateway(
        cast(Any, config),
        cast(Any, MutableCredentials(_snapshot())),
        priority=lambda: "openapi",
    )

    assert gateway.resolve_artifact(artifact, USER_AGENT) == "https://download.invalid/native-pool.mkv"
    config.value = {**account, "temp_cid": "rotated-temp-cid"}
    assert gateway.resolve_artifact(artifact, USER_AGENT) == ""


def test_native_direct_stream_forwards_valid_range_and_allowlists_response_headers() -> None:
    decoded = b"native-stream-payload" * 8
    encoded = gzip.compress(decoded)
    request_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        request_headers.update(request.headers)
        return httpx.Response(
            206,
            headers={
                "content-encoding": "gzip",
                "content-length": str(len(encoded)),
                "content-range": "bytes 0-23/24",
                "set-cookie": "provider-secret=must-not-leak",
            },
            content=encoded,
            request=request,
        )

    gateway = NativeP115DirectLinkGateway(
        cast(Any, FakePlaybackConfig()),
        cast(Any, MutableCredentials(_snapshot())),
        priority=lambda: "openapi",
        client_factory=lambda **options: httpx.Client(transport=httpx.MockTransport(handler), **options),
    )
    stream = gateway.stream(
        "https://media.fixture.invalid/signed?token=secret",
        user_agent=USER_AGENT,
        range_header="bytes=0-23",
    )
    try:
        body = b"".join(stream.chunks)
    finally:
        stream.close()

    assert request_headers["accept-encoding"] == "identity"
    assert request_headers["range"] == "bytes=0-23"
    assert body == decoded
    assert stream.headers == {"content-range": "bytes 0-23/24"}


class FailingNativeStreamClient:
    def __init__(self) -> None:
        self.close_calls = 0

    def build_request(self, *_args: Any, **_kwargs: Any) -> object:
        return object()

    def send(self, _request: object, *, stream: bool) -> Any:
        assert stream is True
        raise RuntimeError("https://media.fixture.invalid/signed?token=secret")

    def close(self) -> None:
        self.close_calls += 1


def test_native_direct_stream_closes_and_redacts_upstream_connect_failure() -> None:
    client = FailingNativeStreamClient()
    gateway = NativeP115DirectLinkGateway(
        cast(Any, FakePlaybackConfig()),
        cast(Any, MutableCredentials(_snapshot())),
        priority=lambda: "openapi",
        client_factory=lambda **_options: cast(Any, client),
    )

    with pytest.raises(P115PlaybackStateError, match="upstream request failed") as captured:
        gateway.stream("https://media.fixture.invalid/signed?token=secret", user_agent=USER_AGENT)

    assert "token=secret" not in str(captured.value)
    assert client.close_calls == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/private",
        "http://[::1]/private",
        "http://169.254.169.254/latest/meta-data",
        "http://localhost/private",
    ],
)
def test_native_direct_stream_rejects_private_upstream_before_opening_client(url: str) -> None:
    opened: list[str] = []
    gateway = NativeP115DirectLinkGateway(
        cast(Any, FakePlaybackConfig()),
        cast(Any, MutableCredentials(_snapshot())),
        priority=lambda: "openapi",
        client_factory=lambda **_options: (opened.append("client"), pytest.fail("client must not open"))[1],
    )

    with pytest.raises(ValueError, match="upstream URL"):
        gateway.stream(url, user_agent=USER_AGENT)

    assert opened == []


def test_native_direct_stream_revalidates_redirect_and_drops_invalid_range() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"location": "http://127.0.0.1/private"},
            request=request,
        )

    gateway = NativeP115DirectLinkGateway(
        cast(Any, FakePlaybackConfig()),
        cast(Any, MutableCredentials(_snapshot())),
        priority=lambda: "openapi",
        client_factory=lambda **options: httpx.Client(transport=httpx.MockTransport(handler), **options),
    )

    with pytest.raises(P115PlaybackStateError, match="upstream request failed"):
        gateway.stream(
            "https://media.fixture.invalid/signed",
            user_agent=USER_AGENT,
            range_header="bytes=invalid",
        )

    assert len(requests) == 1
    assert "range" not in requests[0].headers
