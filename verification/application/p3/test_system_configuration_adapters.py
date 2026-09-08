from __future__ import annotations

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.media.bridge_client import BridgeWriteResult
from etk.domains.system.configuration_adapters import HttpxConfigurationGateway
from etk.platform import Settings


class Repository:
    @staticmethod
    def load(key: str):
        return "fixture-server" if key == "emby_server_id_cache" else None


class Bridge:
    def __init__(self) -> None:
        self.origins: list[str] = []
        self.discovery_urls: list[str] = []

    def configure_origin(self, etk_url: str) -> BridgeWriteResult:
        self.origins.append(etk_url)
        return BridgeWriteResult("configure-origin", 204, {})

    def configure_discovery_url(self, discovery_url: str) -> BridgeWriteResult:
        self.discovery_urls.append(discovery_url)
        return BridgeWriteResult("configure-discovery-url", 204, {})


def test_configuration_gateway_uses_canonical_state_and_native_http_clients() -> None:
    def transport(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/emby/Users/fixture-admin/Views"
        return httpx.Response(200, json={"Items": [{"Id": "library-1"}, {"Id": "library-2"}]})

    settings = Settings(
        emby_server_url="http://192.168.1.20:8096",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-admin",
        emby_api_timeout=1,
    )
    bridge = Bridge()
    gateway = HttpxConfigurationGateway(
        lambda: settings,
        Repository(),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(transport)),
        bridge=bridge,
    )
    config = {
        **settings.to_runtime_dict(),
        "etk_server_url": "invalid-default",
        "proxy_enabled": True,
        "proxy_port": 8097,
    }

    assert gateway.preview_etk_url(config) == "http://192.168.1.20:5257"
    assert gateway.preview_proxy_url(config) == "http://192.168.1.20:8097"
    assert gateway.valid_library_ids(config) == {"library-1", "library-2"}
    assert gateway.server_id() == "fixture-server"
    assert gateway.sync_plugin_origin(config, "http://192.168.1.20:5257") is True
    assert gateway.sync_plugin_discovery(config, "http://192.168.1.20:8097") is True
    assert bridge.origins == ["http://192.168.1.20:5257"]
    assert bridge.discovery_urls == ["http://192.168.1.20:8097"]


def test_configuration_gateway_rejects_public_or_named_internal_addresses() -> None:
    gateway = HttpxConfigurationGateway(lambda: Settings(), Repository(), bridge=Bridge())

    with pytest.raises(ValueError, match="内网 IP"):
        gateway.validate_internal_url("https://example.invalid")
    with pytest.raises(ValueError, match="可用的内网 IP"):
        gateway.validate_internal_url("http://127.0.0.1:5257")
