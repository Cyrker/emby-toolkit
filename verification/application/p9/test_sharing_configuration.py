from __future__ import annotations

import hashlib
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.sharing import SharingConfigurationService, SharingServerIdentity, SharingService
from etk.domains.sharing.router import router
from etk.integrations import ProviderDisposition, ProviderResponse
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


class RecordingCenter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[dict[str, Any]]:
        self.calls.append((name, server_id_hash))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"device": {"id": "device-1"}})


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
    center = RecordingCenter()
    app.state.sharing_service = SharingService(
        object(),  # type: ignore[arg-type]
        center,
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

    assert response.status_code == 200
    assert center.calls == [("fixture-device", "a" * 64)]
    assert response.json()["configuration_saved"] is True
    assert settings.load("shared_resource_config")["p115_shared_resource_enabled"] is True


class FailingEnableSettings(MemorySettings):
    def save(self, key: str, value: Any) -> None:
        if key == "shared_resource_config":
            raise RuntimeError("fixture local projection failure")
        super().save(key, value)


def test_register_device_does_not_invite_provider_replay_when_local_enable_save_fails() -> None:
    center = RecordingCenter()
    result = SharingService(
        object(),  # type: ignore[arg-type]
        center,
        configuration=SharingConfigurationService(FailingEnableSettings()),
        identity=StaticIdentity(),  # type: ignore[arg-type]
    ).register_device(name="fixture-device")

    assert result.status_code == 200
    assert result.payload["success"] is True
    assert result.payload["configuration_saved"] is False
    assert result.payload["data"] is None
    assert center.calls == [("fixture-device", "a" * 64)]


class IndeterminateCenter(RecordingCenter):
    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[dict[str, Any]]:
        self.calls.append((name, server_id_hash))
        return ProviderResponse(ProviderDisposition.INDETERMINATE, 200)


def test_register_device_surfaces_indeterminate_provider_result_as_202() -> None:
    center = IndeterminateCenter()
    settings = MemorySettings()
    result = SharingService(
        object(),  # type: ignore[arg-type]
        center,
        configuration=SharingConfigurationService(settings),
        identity=StaticIdentity(),  # type: ignore[arg-type]
    ).register_device(name="fixture-device")

    assert result.status_code == 202
    assert result.payload["status"] == "indeterminate"
    assert settings.load("shared_resource_config") is None
    assert center.calls == [("fixture-device", "a" * 64)]
