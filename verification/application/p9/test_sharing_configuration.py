from __future__ import annotations

import hashlib
from collections.abc import Mapping
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.sharing import SharingConfigurationService, SharingServerIdentity, SharingService
from etk.domains.sharing.operations import SharingOperationStatus
from etk.domains.sharing.router import router
from etk.integrations import ProviderDisposition
from etk.integrations.http_policy import ExternalAdapterError, HttpTransportPolicy, RetryPolicy
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SESSION_KEY = b"p9-sharing-configuration-fixture-key"


class MemorySettings:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = deepcopy(dict(values or {}))

    def load(self, key: str) -> Any:
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        self.values[key] = deepcopy(value)


def test_sharing_configuration_normalizes_and_merges_partial_updates() -> None:
    settings = MemorySettings(
        {
            "shared_resource_config": {
                "p115_shared_resource_enabled": False,
                "p115_shared_intro_enabled": True,
                "p115_shared_center_url": "https://old-center.fixture/",
                "p115_shared_max_active_shares": 4,
                "future_option": "preserved",
            }
        }
    )
    service = SharingConfigurationService(settings)

    saved = service.save(
        {
            "p115_shared_resource_enabled": "true",
            "p115_shared_virtual_auto_promote_movie_percent": 120,
        }
    )

    assert saved["p115_shared_resource_enabled"] is True
    assert saved["p115_shared_intro_enabled"] is True
    assert saved["p115_shared_center_url"] == "https://old-center.fixture"
    assert saved["p115_shared_resource_mode"] == "rapid"
    assert saved["p115_shared_virtual_auto_promote_movie_percent"] == 100
    assert "p115_shared_max_active_shares" not in saved
    assert saved["future_option"] == "preserved"
    assert settings.load("shared_resource_config") == saved


def test_sharing_configuration_preserves_legacy_home_section_defaults_and_bounds() -> None:
    service = SharingConfigurationService(MemorySettings())

    defaults = service.load()["p115_shared_center_home_sections"]
    saved = service.save(
        {
            "p115_shared_center_home_sections": [
                {
                    "key": " latest ",
                    "title": "A" * 50,
                    "display_type": "invalid",
                    "order_by": "latest",
                    "status": "short_drama,invalid tag,short_drama",
                    "genre_id": "x" * 50,
                    "limit": "30.9",
                    "enabled": "off",
                },
                "not-an-object",
            ]
        }
    )

    assert [section["key"] for section in defaults] == ["latest", "popular", "movies", "series"]
    assert saved["p115_shared_center_home_sections"] == [
        {
            "key": "latest",
            "title": "A" * 40,
            "display_type": "all",
            "order_by": "pool_time",
            "status": "alive,available,short_drama",
            "genre_id": "x" * 40,
            "tags": ["short_drama"],
            "limit": 20,
            "enabled": False,
        }
    ]


class FakeIdentityClient:
    def __init__(self, events: list[httpx.Response | BaseException]) -> None:
        self.events = list(events)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def __enter__(self) -> FakeIdentityClient:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        self.calls.append((method, url, kwargs))
        event = self.events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event


def test_server_identity_is_cached_and_only_hash_is_derived() -> None:
    request = httpx.Request("GET", "http://emby.fixture/System/Info")
    client = FakeIdentityClient([httpx.Response(200, json={"Id": "server-fixture"}, request=request)])
    settings = MemorySettings()
    identity = SharingServerIdentity(
        {"emby_server_url": "http://emby.fixture", "emby_api_key": "fixture-token"},
        settings,
        client_factory=cast(Any, lambda: client),
        policy=HttpTransportPolicy(
            system="fixture",
            retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0, max_delay_seconds=0),
        ),
    )

    expected = hashlib.sha256(b"server-fixture").hexdigest()
    assert identity.server_id_hash() == expected
    assert identity.server_id_hash() == expected
    assert len(client.calls) == 1
    assert client.calls[0][0:2] == ("GET", "http://emby.fixture/System/Info")
    assert client.calls[0][2]["headers"]["X-Emby-Token"] == "fixture-token"
    assert settings.load("emby_server_id_cache") == "server-fixture"


def test_server_identity_rejects_missing_authorization_without_leaking_secret() -> None:
    with pytest.raises(ExternalAdapterError) as raised:
        SharingServerIdentity(
            {"emby_server_url": "http://emby.fixture", "emby_api_key": ""},
            MemorySettings(),
        ).server_id()

    assert raised.value.disposition is ProviderDisposition.REJECTED
    assert raised.value.status_code == 400
    assert "fixture-token" not in str(raised.value)


@pytest.mark.parametrize("status_code", [408, 425, 429])
def test_server_identity_classifies_transient_client_status_as_retryable(status_code: int) -> None:
    request = httpx.Request("GET", "http://emby.fixture/System/Info")
    client = FakeIdentityClient([httpx.Response(status_code, request=request)])
    identity = SharingServerIdentity(
        {"emby_server_url": "http://emby.fixture", "emby_api_key": "fixture-token"},
        MemorySettings(),
        client_factory=cast(Any, lambda: client),
        policy=HttpTransportPolicy(
            system="fixture",
            retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0, max_delay_seconds=0),
        ),
    )

    with pytest.raises(ExternalAdapterError) as raised:
        identity.server_id()

    assert raised.value.disposition is ProviderDisposition.RETRYABLE


class Permissions:
    @staticmethod
    def is_admin(user_id: str) -> bool:
        return user_id == "admin"


class MemoryOperations:
    def __init__(self) -> None:
        self.intents: list[Any] = []
        self.by_key: dict[str, Any] = {}

    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]:
        existing = self.by_key.get(intent.operation_key)
        if existing is not None:
            return existing, True
        operation = SimpleNamespace(
            id=intent.id,
            operation_key=intent.operation_key,
            kind=intent.kind,
            scope_key=intent.scope_key,
            status=SharingOperationStatus.READY,
            attempts=0,
            state_revision=0,
            available_at=intent.available_at,
            finished_at=None,
            result=None,
            error=None,
        )
        self.intents.append(intent)
        self.by_key[intent.operation_key] = operation
        return operation, False


class StaticIdentity:
    def server_id_hash(self) -> str:
        return "a" * 64


def test_register_device_uses_backend_identity_and_accepts_empty_body() -> None:
    settings = MemorySettings({"shared_resource_config": {"p115_shared_resource_enabled": False}})
    app = FastAPI()
    app.state.container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: Permissions()},
    )
    operations = MemoryOperations()
    app.state.sharing_service = SharingService(
        object(),  # type: ignore[arg-type]
        object(),
        operations,
        configuration=SharingConfigurationService(settings),
        identity=StaticIdentity(),  # type: ignore[arg-type]
    )
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: LegacySessionCodec.from_secret_key(SESSION_KEY))
    app.include_router(router)

    codec = LegacySessionCodec.from_secret_key(SESSION_KEY)
    with TestClient(app) as client:
        client.cookies.set("session", codec.dumps({"emby_user_id": "admin"}))
        response = client.post(
            "/api/shared/resources/center/device/register",
            json={"name": "fixture-device", "server_id_hash": "attacker-controlled"},
        )
        replay = client.post(
            "/api/shared/resources/center/device/register",
            json={"name": "fixture-device", "server_id_hash": "different-attacker-value"},
        )

    assert response.status_code == 202
    assert replay.status_code == 202
    assert response.json()["operation_id"] == replay.json()["operation_id"]
    assert response.json()["replayed"] is False
    assert replay.json()["replayed"] is True
    assert len(operations.intents) == 1
    request = operations.intents[0].request
    assert request["payload"] == {"name": "fixture-device", "server_id_hash": "a" * 64}
    assert request["readback"]["match_value"] == "a" * 64
    assert request["local_continuation"] == {"type": "enable_shared_resource"}
    assert "attacker" not in repr(request)
    assert settings.load("shared_resource_config")["p115_shared_resource_enabled"] is False
