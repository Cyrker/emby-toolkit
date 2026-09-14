from __future__ import annotations

import json
from collections.abc import Callable, Collection, Mapping
from typing import Any

import httpx
import pytest

from etk.domains.sharing.configuration import SharingConfigurationService, SharingServerIdentity
from etk.integrations import (
    CircuitBreaker,
    HDHiveUnlockIndeterminateError,
    HttpTransportPolicy,
    HttpxHDHiveAdapter,
    HttpxSharedCenterAdapter,
    HttpxTelegramBotAdapter,
    ProviderDisposition,
    RetryPolicy,
)
from etk.integrations.shared_center import SharedCenterError
from etk.integrations.telegram import (
    DurableTelegramUserBotAdapter,
    TelegramAdapterError,
)


class FakeClient:
    def __init__(self, events: list[httpx.Response | BaseException]) -> None:
        self.events = list(events)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def __enter__(self) -> FakeClient:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        self.calls.append((method, url, kwargs))
        if not self.events:
            raise AssertionError("fake client received more requests than expected")
        event = self.events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event


def _response(status: int, payload: Any, *, text: str | None = None) -> httpx.Response:
    request = httpx.Request("GET", "https://fixture.invalid")
    if text is not None:
        return httpx.Response(status, text=text, request=request)
    return httpx.Response(status, json=payload, request=request)


def _factory(client: FakeClient) -> Callable[[], Any]:
    return lambda: client


def _policy(*, attempts: int = 3, threshold: int = 10) -> HttpTransportPolicy:
    return HttpTransportPolicy(
        system="fixture",
        retry_policy=RetryPolicy(max_attempts=attempts, base_delay_seconds=0, max_delay_seconds=0),
        breaker=CircuitBreaker(failure_threshold=threshold, reset_timeout_seconds=5),
        sleep=lambda _seconds: None,
    )


class FakeAppSettings:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = dict(values or {})

    def load(self, key: str) -> Any:
        return self.values.get(key)

    def save(self, key: str, value: Any) -> None:
        self.values[key] = value


def test_shared_center_maps_success_and_rejection_without_leaking_payload() -> None:
    success_client = FakeClient([_response(200, {"ok": True, "items": [1]})])
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture", "app_version": "10.9.0"},
        client_factory=_factory(success_client),
        policy=_policy(),
    )

    success = adapter.list_sources(limit=2)

    assert success.disposition is ProviderDisposition.CONFIRMED
    assert success.payload == {"ok": True, "items": [1]}
    assert success_client.calls[0][0:2] == ("GET", "https://center.fixture/api/v1/sources/list")
    assert success_client.calls[0][2]["params"]["limit"] == 2
    assert success_client.calls[0][2]["headers"]["X-ETK-Version"] == "10.9.0"

    rejected_client = FakeClient([_response(403, {"detail": "denied"})])
    rejected_adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(rejected_client),
        policy=_policy(),
    )
    rejected = rejected_adapter.register_source("movie", {"tmdb_id": "1"})

    assert rejected.disposition is ProviderDisposition.REJECTED
    assert rejected.status_code == 403
    assert rejected.payload is None


def test_shared_center_raw_upload_and_readback_use_typed_endpoints() -> None:
    sha1 = "A" * 40
    client = FakeClient(
        [
            _response(200, {"ok": True, "sha1": sha1}),
            _response(200, {"item": {"sha1": sha1}}),
        ]
    )
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(),
    )

    uploaded = adapter.upload_raw_ffprobe(
        {
            "sha1": sha1,
            "size": 1024,
            "raw_ffprobe_json": {"format": {"size": "1024"}, "streams": []},
            "summary_json": {"resolution": "4K"},
        }
    )
    readback = adapter.get_raw_ffprobe(sha1)

    assert uploaded.disposition is ProviderDisposition.CONFIRMED
    assert readback.disposition is ProviderDisposition.CONFIRMED
    assert client.calls[0][0:2] == ("POST", "https://center.fixture/api/v1/rawffprobe/upload")
    assert client.calls[0][2]["json"]["sha1"] == sha1
    assert client.calls[1][0:2] == ("GET", f"https://center.fixture/api/v1/rawffprobe/{sha1}")


def test_shared_center_display_metadata_uses_the_typed_upsert_endpoint() -> None:
    client = FakeClient([_response(200, {"ok": True, "updated": 1})])
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(),
    )
    payload = {"skip_logical_share_dispatch": True, "items": [{"display_meta_items_json": []}]}

    result = adapter.upsert_display_metadata(payload)

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert client.calls[0][0:2] == ("POST", "https://center.fixture/api/v1/metadata/display/upsert")
    assert client.calls[0][2]["json"] == payload


def test_shared_center_reads_standalone_runtime_configuration() -> None:
    client = FakeClient([_response(200, {"ok": True, "items": []})])
    adapter = HttpxSharedCenterAdapter(
        {},
        configuration=lambda: {"p115_shared_center_url": "https://standalone-center.fixture"},
        client_factory=_factory(client),
        policy=_policy(),
    )

    result = adapter.list_sources(limit=1)

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert client.calls[0][1] == "https://standalone-center.fixture/api/v1/sources/list"


def test_shared_center_uses_native_cached_server_identity_for_headers() -> None:
    emby_client = FakeClient([_response(200, {"Id": "native-server-id"})])
    app_settings = FakeAppSettings()
    identity = SharingServerIdentity(
        {"emby_server_url": "https://emby.fixture", "emby_api_key": "fixture-token", "emby_api_timeout": 5},
        app_settings,
        client_factory=_factory(emby_client),
        policy=_policy(),
    )

    server_hash = identity.server_id_hash()

    assert server_hash == "d2a886b90ca2f3e0a700cf9ff7762def156fdd234a7ca664533152b011e21891"
    assert app_settings.values["emby_server_id_cache"] == "native-server-id"
    assert emby_client.calls[0][0:2] == ("GET", "https://emby.fixture/System/Info")
    assert emby_client.calls[0][2]["headers"]["X-Emby-Token"] == "fixture-token"
    assert identity.server_id_hash() == server_hash
    assert len(emby_client.calls) == 1

    center_client = FakeClient([_response(200, {"items": []})])
    adapter = HttpxSharedCenterAdapter(
        {},
        configuration=lambda: {"p115_shared_center_url": "https://center.fixture"},
        server_id_hash=identity.server_id_hash,
        client_factory=_factory(center_client),
        policy=_policy(),
    )

    assert adapter.list_display_sources(limit=1).disposition is ProviderDisposition.CONFIRMED
    assert center_client.calls[0][2]["headers"]["X-Server-ID-Hash"] == server_hash


def test_shared_center_display_browse_uses_exact_endpoints_and_query_shapes() -> None:
    client = FakeClient(
        [
            _response(200, {"items": []}),
            _response(200, {"items": []}),
            _response(200, {"sections": []}),
            _response(200, {"children": []}),
            _response(200, {"resources": []}),
        ]
    )
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(),
    )
    default_sections = SharingConfigurationService(FakeAppSettings()).load()["p115_shared_center_home_sections"]

    adapter.list_display_sources(q="fixture", limit=2, force_refresh=False)
    adapter.list_display_tags()
    adapter.list_display_home(limit_per_section=3, force_refresh=True, sections=default_sections)
    adapter.list_display_children(source_id="source-1", source_ids=["source-1", "source-2"], limit=20)
    adapter.display_detail(source_kind="logical_season", source_id="source-1", include_people=True)

    assert [call[1] for call in client.calls] == [
        "https://center.fixture/api/v1/sources/display-list",
        "https://center.fixture/api/v1/sources/display-tags",
        "https://center.fixture/api/v1/sources/display-home",
        "https://center.fixture/api/v1/sources/display-children",
        "https://center.fixture/api/v1/sources/display-detail",
    ]
    assert client.calls[2][2]["params"]["limit_per_section"] == 3
    assert client.calls[2][2]["params"]["force_refresh"] == 1
    assert json.loads(client.calls[2][2]["params"]["sections_json"]) == default_sections
    assert client.calls[3][2]["params"]["source_ids"] == "source-1,source-2"
    assert client.calls[4][2]["params"]["include_people"] == 1


def test_shared_center_logical_season_share_uses_typed_manifest_channel_and_status_endpoints() -> None:
    client = FakeClient(
        [
            _response(200, {"item": {"files": []}}),
            _response(200, {"item": {"channel_id": "channel-1", "status": "valid"}}),
            _response(200, {"ok": True}),
        ]
    )
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(),
    )

    adapter.logical_season_manifest("logical-season-1")
    adapter.get_logical_season_share_channel("logical-season-1")
    adapter.update_logical_season_share_status(
        "channel-1",
        {"status": "expired", "review_status": "expired"},
    )

    assert [call[0:2] for call in client.calls] == [
        ("GET", "https://center.fixture/api/v1/logical-seasons/logical-season-1/manifest"),
        ("GET", "https://center.fixture/api/v1/logical-seasons/logical-season-1/share-channel"),
        ("POST", "https://center.fixture/api/v1/logical-season-share-channels/channel-1/status"),
    ]
    assert client.calls[2][2]["json"] == {"status": "expired", "review_status": "expired"}


def test_shared_center_retries_idempotent_reads_but_invalid_json_is_safe() -> None:
    client = FakeClient([_response(429, {"detail": "busy"}), _response(500, {}), _response(200, {"ok": True})])
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=3, threshold=10),
    )

    result = adapter.health()

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert len(client.calls) == 3

    invalid_client = FakeClient([_response(200, {}, text="not-json")])
    invalid_adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(invalid_client),
        policy=_policy(),
    )
    with pytest.raises(SharedCenterError) as raised:
        invalid_adapter.health()
    assert raised.value.disposition is ProviderDisposition.RETRYABLE
    assert "not-json" not in str(raised.value)

    mutation_client = FakeClient([_response(200, {}, text="not-json")])
    mutation_adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(mutation_client),
        policy=_policy(),
    )
    with pytest.raises(SharedCenterError) as mutation_raised:
        mutation_adapter.register_source("movie", {"tmdb_id": "1"})
    assert mutation_raised.value.disposition is ProviderDisposition.INDETERMINATE


def test_shared_center_quote_is_a_retryable_read_only_post() -> None:
    client = FakeClient([_response(503, {"detail": "busy"}), _response(200, {"cost": 2})])
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=2),
    )

    result = adapter.quote_share_request({"tmdb_id": "101"})

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert result.payload == {"cost": 2}
    assert len(client.calls) == 2
    assert all(call[0] == "POST" for call in client.calls)

    invalid_client = FakeClient([_response(200, {}, text="not-json")])
    invalid_adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(invalid_client),
        policy=_policy(),
    )
    with pytest.raises(SharedCenterError) as raised:
        invalid_adapter.quote_share_request({"tmdb_id": "101"})
    assert raised.value.disposition is ProviderDisposition.RETRYABLE


def test_shared_center_disable_source_uses_exact_single_shot_contract() -> None:
    client = FakeClient([_response(200, {"ok": True, "source_id": "source-3", "status": "disabled"})])
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=4),
    )

    result = adapter.disable_source("episode", "source-3", message="local delete")

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert len(client.calls) == 1
    assert client.calls[0][0:2] == (
        "POST",
        "https://center.fixture/api/v1/sources/episode/source-3/disable",
    )
    assert client.calls[0][2]["json"] == {"message": "local delete"}


def test_shared_center_share_request_participant_mutations_use_exact_single_shot_contracts() -> None:
    client = FakeClient(
        [
            _response(200, {"ok": True, "group_id": "group-1", "joined_by_me": True}),
            _response(200, {"ok": True, "group_id": "group-1", "joined_by_me": False}),
        ]
    )
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=4),
    )

    joined = adapter.co_request_share_request("group-1", {"bounty": 2})
    cancelled = adapter.cancel_share_request("group-1", {"reason": "fixture"})

    assert joined.disposition is ProviderDisposition.CONFIRMED
    assert cancelled.disposition is ProviderDisposition.CONFIRMED
    assert [call[0:2] for call in client.calls] == [
        ("POST", "https://center.fixture/api/v1/share-requests/group-1/co-request"),
        ("POST", "https://center.fixture/api/v1/share-requests/group-1/cancel"),
    ]
    assert client.calls[0][2]["json"] == {"bounty": 2}
    assert client.calls[1][2]["json"] == {"reason": "fixture"}


def test_shared_center_telegram_login_code_is_single_shot_and_transport_safe() -> None:
    session = "fixture-telegram-session"
    client = FakeClient(
        [
            _response(
                200,
                {
                    "success": True,
                    "data": {
                        "telegram_session": "fixture-challenge-session",
                        "phone_code_hash": "fixture-code-hash",
                    },
                },
            )
        ]
    )
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=4),
    )

    result = adapter.send_telegram_login_code("+8613800000000", session)

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert len(client.calls) == 1
    assert client.calls[0][0:2] == (
        "POST",
        "https://center.fixture/api/v1/telegram/send-code",
    )
    assert client.calls[0][2]["json"] == {
        "phone": "+8613800000000",
        "telegram_session": session,
    }

    rejected_client = FakeClient([_response(503, {"detail": "busy"})])
    rejected_adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(rejected_client),
        policy=_policy(attempts=4),
    )
    rejected = rejected_adapter.send_telegram_login_code("+8613800000000", session)
    assert rejected.disposition is ProviderDisposition.INDETERMINATE
    assert len(rejected_client.calls) == 1

    transport_client = FakeClient(
        [httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://fixture.invalid"))]
    )
    transport_adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(transport_client),
        policy=_policy(attempts=4),
    )
    with pytest.raises(SharedCenterError) as raised:
        transport_adapter.send_telegram_login_code("+8613800000000", session)
    assert raised.value.disposition is ProviderDisposition.INDETERMINATE
    assert session not in str(raised.value)
    assert len(transport_client.calls) == 1


def test_shared_intro_upload_uses_exact_single_shot_contract() -> None:
    client = FakeClient([_response(200, {"ok": True})])
    adapter = HttpxSharedCenterAdapter(
        {"p115_shared_center_url": "https://center.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=4),
    )

    result = adapter.upload_intro_chapters(
        "a" * 40,
        [
            {"StartPositionTicks": 0, "MarkerType": "IntroStart", "ChapterIndex": 0},
            {"StartPositionTicks": 180_000_000, "MarkerType": "IntroEnd", "ChapterIndex": 1},
        ],
        reason="etk_intro_detection",
    )

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert len(client.calls) == 1
    method, url, options = client.calls[0]
    assert (method, url) == ("POST", "https://center.fixture/api/v1/intro-chapters/upload")
    assert options["json"] == {
        "sha1": "A" * 40,
        "chapters": [
            {"StartPositionTicks": 0, "MarkerType": "IntroStart", "ChapterIndex": 0},
            {"StartPositionTicks": 180_000_000, "MarkerType": "IntroEnd", "ChapterIndex": 1},
        ],
        "file_name": "",
        "reason": "etk_intro_detection",
    }


def test_telegram_mutations_are_not_retried_and_token_is_redacted() -> None:
    token = "123456:fixture-secret-token"
    client = FakeClient([_response(429, {"ok": False, "description": "rate limited"})])
    adapter = HttpxTelegramBotAdapter(
        {"telegram_bot_token": token, "telegram_api_base_url": "https://telegram.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=4),
    )

    rate_limited = adapter.send_message("chat", "hello")

    assert rate_limited.disposition is ProviderDisposition.INDETERMINATE
    assert len(client.calls) == 1
    assert token in client.calls[0][1]

    transport_client = FakeClient(
        [httpx.ConnectError("connection failed", request=httpx.Request("POST", "https://fixture.invalid"))]
    )
    transport_adapter = HttpxTelegramBotAdapter(
        {"telegram_bot_token": token, "telegram_api_base_url": "https://telegram.fixture"},
        client_factory=_factory(transport_client),
        policy=_policy(attempts=2),
    )
    with pytest.raises(TelegramAdapterError) as raised:
        transport_adapter.send_photo("chat", "photo", "caption")
    assert token not in str(raised.value)
    assert len(transport_client.calls) == 1

    invalid_client = FakeClient([_response(200, {}, text="not-json")])
    invalid_adapter = HttpxTelegramBotAdapter(
        {"telegram_bot_token": token, "telegram_api_base_url": "https://telegram.fixture"},
        client_factory=_factory(invalid_client),
        policy=_policy(),
    )
    with pytest.raises(TelegramAdapterError) as invalid_raised:
        invalid_adapter.send_message("chat", "hello")
    assert invalid_raised.value.disposition is ProviderDisposition.INDETERMINATE
    assert len(invalid_client.calls) == 1


def test_telegram_default_client_uses_only_the_configured_proxy(monkeypatch) -> None:
    captured: list[dict[str, Any]] = []

    def client_factory(**kwargs: Any) -> FakeClient:
        captured.append(kwargs)
        return FakeClient([])

    monkeypatch.setattr("etk.integrations.telegram.httpx.Client", client_factory)

    proxied = HttpxTelegramBotAdapter(
        {
            "telegram_bot_token": "fixture-token",
            "network_proxy_enabled": True,
            "network_http_proxy_url": "http://proxy.fixture.invalid:8080",
        }
    )
    direct = HttpxTelegramBotAdapter(
        {
            "telegram_bot_token": "fixture-token",
            "network_proxy_enabled": False,
            "network_http_proxy_url": "http://ignored.fixture.invalid:8080",
        }
    )

    proxied._default_client()
    direct._default_client()

    assert captured == [
        {
            "proxy": "http://proxy.fixture.invalid:8080",
            "timeout": 20.0,
            "follow_redirects": False,
            "trust_env": False,
        },
        {
            "proxy": None,
            "timeout": 20.0,
            "follow_redirects": False,
            "trust_env": False,
        },
    ]


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_hdhive_unlock_indeterminate_status_is_single_shot(status: int) -> None:
    client = FakeClient([_response(status, {"success": False})])
    adapter = HttpxHDHiveAdapter(
        {"hdhive_relay_base_url": "https://hdhive.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=5),
    )

    with pytest.raises(HDHiveUnlockIndeterminateError) as raised:
        adapter.unlock("fixture-slug")

    assert raised.value.disposition is ProviderDisposition.INDETERMINATE
    assert len(client.calls) == 1


def test_hdhive_unlock_transport_is_indeterminate_without_replay() -> None:
    client = FakeClient([httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://fixture.invalid"))])
    adapter = HttpxHDHiveAdapter(
        {"hdhive_relay_base_url": "https://hdhive.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=3),
    )

    with pytest.raises(HDHiveUnlockIndeterminateError):
        adapter.unlock("fixture-slug")
    assert len(client.calls) == 1


def test_hdhive_mutation_invalid_json_is_indeterminate() -> None:
    client = FakeClient([_response(200, {}, text="not-json")])
    adapter = HttpxHDHiveAdapter(
        {"hdhive_relay_base_url": "https://hdhive.fixture"},
        client_factory=_factory(client),
        policy=_policy(),
    )
    with pytest.raises(HDHiveUnlockIndeterminateError):
        adapter.unlock("fixture-slug")
    assert len(client.calls) == 1


def test_retryable_hdhive_reads_are_bounded() -> None:
    client = FakeClient([_response(503, {}), _response(503, {}), _response(200, {"success": True})])
    adapter = HttpxHDHiveAdapter(
        {"hdhive_relay_base_url": "https://hdhive.fixture"},
        client_factory=_factory(client),
        policy=_policy(attempts=3, threshold=10),
    )

    result = adapter.ping()

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert len(client.calls) == 3


def test_hdhive_reads_standalone_runtime_configuration() -> None:
    client = FakeClient([_response(200, {"success": True})])
    adapter = HttpxHDHiveAdapter(
        {},
        configuration=lambda: {
            "relay_base_url": "https://standalone-hdhive.fixture",
            "relay_instance_id": "fixture-instance",
            "relay_instance_secret": "fixture-secret",
        },
        client_factory=_factory(client),
        policy=_policy(),
    )

    result = adapter.ping()

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert client.calls[0][1] == "https://standalone-hdhive.fixture/api/hdhive/status"
    assert client.calls[0][2]["headers"]["X-ETK-Instance-ID"] == "fixture-instance"
    assert client.calls[0][2]["headers"]["X-ETK-Instance-Secret"] == "fixture-secret"


def test_hdhive_registration_uses_greenfield_relay_default_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HDHIVE_RELAY_BASE_URL", raising=False)
    monkeypatch.delenv("HDHIVE_RELAY_REGISTER_TOKEN", raising=False)
    client = FakeClient(
        [
            _response(
                200,
                {
                    "success": True,
                    "data": {
                        "instance_id": "fixture-instance",
                        "instance_secret": "fixture-secret",
                    },
                },
            )
        ]
    )
    adapter = HttpxHDHiveAdapter({}, client_factory=_factory(client), policy=_policy())

    result = adapter.register()

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert client.calls[0][0:2] == (
        "POST",
        "https://hdhive.55565576.xyz/api/etk/register",
    )
    assert "X-ETK-Instance-ID" not in client.calls[0][2]["headers"]
    assert "X-ETK-Instance-Secret" not in client.calls[0][2]["headers"]


def test_circuit_breaker_transitions_closed_open_half_open() -> None:
    now = [0.0]
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout_seconds=10, clock=lambda: now[0])

    assert breaker.allow() is True
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == "open"
    assert breaker.allow() is False

    now[0] = 10.0
    assert breaker.allow() is True
    assert breaker.state == "half_open"
    assert breaker.allow() is False
    breaker.record_success()
    assert breaker.state == "closed"
    assert breaker.failures == 0


class FakeUserBotBridge:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def enqueue(self, command_type: str, payload: Mapping[str, Any] | None = None) -> str:
        self.calls.append(("enqueue", command_type, dict(payload or {})))
        return "command-1"

    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str:
        self.calls.append(("enqueue_once", command_type, dict(payload or {}), idempotency_key, result_owner_key))
        return "command-once"

    def claim_next(
        self,
        owner_id: str,
        *,
        lease_seconds: int = 60,
        command_types: Collection[str] | None = None,
    ) -> Mapping[str, Any] | None:
        assert command_types is None
        self.calls.append(("claim", owner_id, lease_seconds))
        return {"id": "command-1", "status": "claimed"}

    def complete(
        self,
        command_id: str,
        owner_id: str,
        result: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        self.calls.append(("complete", command_id, owner_id, dict(result or {})))
        return {"id": command_id, "status": "succeeded"}

    def fail(self, command_id: str, owner_id: str, error: str) -> Mapping[str, Any]:
        self.calls.append(("fail", command_id, owner_id, error))
        return {"id": command_id, "status": "failed"}

    def get_command(self, command_id: str) -> Mapping[str, Any] | None:
        self.calls.append(("get_command", command_id))
        return {"id": command_id, "status": "succeeded"}

    def consume_resource_secret(self, reference: str, owner_key: str) -> Mapping[str, Any]:
        self.calls.append(("consume_resource_secret", reference, owner_key))
        return {"ok": True, "results": []}

    def wait(
        self,
        command_id: str,
        *,
        timeout_seconds: float = 45,
        poll_seconds: float = 0.1,
    ) -> Mapping[str, Any]:
        self.calls.append(("wait", command_id, timeout_seconds, poll_seconds))
        return {"id": command_id, "status": "succeeded"}


def test_userbot_facade_uses_durable_claim_complete_fail_bridge() -> None:
    bridge = FakeUserBotBridge()
    adapter = DurableTelegramUserBotAdapter(bridge)  # type: ignore[arg-type]

    assert adapter.enqueue("search", {"query": "fixture"}) == "command-1"
    assert adapter.claim_next("worker-1") == {"id": "command-1", "status": "claimed"}
    assert adapter.complete("command-1", "worker-1", {"items": []})["status"] == "succeeded"
    assert adapter.fail("command-1", "worker-1", "failed")["status"] == "failed"
    assert bridge.calls == [
        ("enqueue", "search", {"query": "fixture"}),
        ("claim", "worker-1", 60),
        ("complete", "command-1", "worker-1", {"items": []}),
        ("fail", "command-1", "worker-1", "failed"),
    ]


def test_userbot_facade_exposes_replay_safe_search_and_readback() -> None:
    bridge = FakeUserBotBridge()
    adapter = DurableTelegramUserBotAdapter(bridge)  # type: ignore[arg-type]

    command_id = adapter.enqueue_once(
        "search",
        {"query": "fixture"},
        idempotency_key="auto-subscribe:fixture:search",
        result_owner_key="a" * 64,
    )

    assert command_id == "command-once"
    assert adapter.get_command(command_id) == {"id": command_id, "status": "succeeded"}
    assert adapter.wait(command_id, timeout_seconds=5, poll_seconds=0.5)["status"] == "succeeded"
    assert adapter.consume_resource_secret("b" * 64, "a" * 64) == {"ok": True, "results": []}
    assert bridge.calls == [
        (
            "enqueue_once",
            "search",
            {"query": "fixture"},
            "auto-subscribe:fixture:search",
            "a" * 64,
        ),
        ("get_command", "command-once"),
        ("wait", "command-once", 5, 0.5),
        ("consume_resource_secret", "b" * 64, "a" * 64),
    ]
