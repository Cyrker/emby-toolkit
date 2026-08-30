from __future__ import annotations

import json
from collections.abc import Callable, Mapping
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

    def claim_next(self, owner_id: str, *, lease_seconds: int = 60) -> Mapping[str, Any] | None:
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


def test_userbot_facade_uses_durable_claim_complete_fail_bridge() -> None:
    bridge = FakeUserBotBridge()
    adapter = DurableTelegramUserBotAdapter(bridge)

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
