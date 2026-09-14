from __future__ import annotations

import json
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.identity import adapters as identity_adapters_module
from etk.domains.identity.adapters import HttpxEmbyIdentityGateway, RuntimeConfigurationWriter
from etk.platform import Settings


def test_identity_default_client_bypasses_configured_and_environment_proxies() -> None:
    settings = Settings(
        emby_api_timeout=5,
        network_proxy_enabled=True,
        network_http_proxy_url="http://proxy.fixture.invalid:8080",
    )
    gateway = HttpxEmbyIdentityGateway(lambda: settings)

    with gateway._default_client() as client:
        assert client._trust_env is False
        assert client._mounts == {}


def test_httpx_identity_gateway_owns_authorization_plugin_and_library_readback() -> None:
    posted_options: dict[str, object] = {}

    def transport(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Users/AuthenticateByName":
            return httpx.Response(
                200,
                json={
                    "AccessToken": "fixture-token",
                    "User": {"Id": "fixture-admin", "Policy": {"IsAdministrator": True}},
                },
            )
        if request.url.path == "/Users/fixture-admin":
            return httpx.Response(200, json={"Id": "fixture-admin", "Policy": {"IsAdministrator": True}})
        if request.url.path == "/emby/System/Info":
            return httpx.Response(200, json={"ServerId": "fixture-server"})
        if request.url.path == "/Library/VirtualFolders" and request.method == "GET":
            return httpx.Response(
                200,
                json=[
                    {
                        "Id": "library-1",
                        "Name": "Movies",
                        "CollectionType": "movies",
                        "LibraryOptions": posted_options,
                    }
                ],
            )
        if request.url.path == "/Library/VirtualFolders/LibraryOptions":
            payload = json.loads(request.content)
            posted_options.update(payload["LibraryOptions"])
            return httpx.Response(204)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    control_plane_calls: list[dict[str, str]] = []

    class ControlPlane:
        def install_emby_plugin(self, **values):
            control_plane_calls.append(values)
            return {"plugin_install": {"ok": True, "message": "fixture ready"}}

    settings = Settings(
        emby_server_url="http://192.168.1.20:8096",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-admin",
        emby_auth_mode="user_token",
        etk_server_url="invalid-default",
    )
    gateway = HttpxEmbyIdentityGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(transport)),
        control_plane_factory=ControlPlane,
        reconcile_timeout_seconds=0,
    )

    authorization = gateway.authenticate(
        settings.emby_server_url,
        "fixture-admin",
        "fixture-password",
        device_id="fixture-device",
    )
    assert authorization is not None
    assert gateway.get_user(settings.emby_server_url, "fixture-token", "fixture-admin") == {
        "Id": "fixture-admin",
        "Policy": {"IsAdministrator": True},
    }
    assert gateway.test_connection(settings.emby_server_url, "fixture-token") == {"success": True}

    plugin = gateway.install_plugin(
        {**authorization, "_normalized_url": settings.emby_server_url},
        settings,
        "fixture-job",
    )
    assert plugin["ok"] is True
    assert control_plane_calls == [
        {
            "emby_url": settings.emby_server_url,
            "emby_token": "fixture-token",
            "etk_origin": "http://192.168.1.20:5257",
            "request_id": "fixture-job",
        }
    ]

    result = gateway.configure_libraries(
        {**authorization, "_normalized_url": settings.emby_server_url},
        plugin,
    )
    assert result["ok"] is True
    assert result["updated"] == 1
    assert result["certainty"] == "observed-complete"
    assert posted_options["EnableRealtimeMonitor"] is False
    assert posted_options["MetadataSavers"] == []
    type_options = posted_options["TypeOptions"]
    assert isinstance(type_options, list)
    assert {item["Type"] for item in type_options if isinstance(item, dict)} == {"Movie", "BoxSet"}


def test_library_configuration_fails_closed_before_writes_when_readback_is_unavailable() -> None:
    settings = Settings(emby_api_timeout=1)
    gateway = HttpxEmbyIdentityGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(503))),
    )

    result = gateway.configure_libraries(
        {"_normalized_url": "http://192.168.1.20:8096", "AccessToken": "fixture-token"},
        {"ok": True},
    )

    assert result["ok"] is False
    assert result["updated"] == 0
    assert result["certainty"] == "confirmed-no-effect"


def _gateway(
    handler: Any,
    *,
    settings: Settings | None = None,
    control_plane: Any | None = None,
    **values: Any,
) -> HttpxEmbyIdentityGateway:
    return HttpxEmbyIdentityGateway(
        lambda: settings or Settings(emby_api_timeout=1),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        control_plane_factory=control_plane,
        reconcile_timeout_seconds=values.pop("reconcile_timeout_seconds", 0),
        **values,
    )


def test_identity_authenticate_and_user_read_fail_closed_on_provider_boundaries() -> None:
    mode = "rejected"

    def transport(request: httpx.Request) -> httpx.Response:
        if mode == "rejected":
            return httpx.Response(401)
        if mode == "invalid-json":
            return httpx.Response(200, content=b"not-json")
        if mode == "incomplete":
            return httpx.Response(200, json={"AccessToken": "token"})
        if mode == "not-object":
            return httpx.Response(200, json=[])
        if mode == "error":
            raise httpx.ReadError("fixture read error", request=request)
        return httpx.Response(200, json={"AccessToken": "token", "User": {"Id": "admin"}})

    gateway = _gateway(transport)
    assert gateway.authenticate("http://emby.fixture", "admin", "password", device_id="device") is None
    mode = "invalid-json"
    assert gateway.authenticate("http://emby.fixture", "admin", "password", device_id="device") is None
    mode = "incomplete"
    assert gateway.authenticate("http://emby.fixture", "admin", "password", device_id="device") is None
    assert gateway.authenticate("invalid-url", "admin", "password", device_id="device") is None
    mode = "ok"
    assert gateway.authenticate("http://emby.fixture", "admin", "password", device_id="device") == {
        "AccessToken": "token",
        "User": {"Id": "admin"},
    }

    mode = "rejected"
    assert gateway.get_user("http://emby.fixture", "token", "admin") is None
    mode = "not-object"
    assert gateway.get_user("http://emby.fixture", "token", "admin") is None
    mode = "error"
    assert gateway.get_user("http://emby.fixture", "token", "admin") is None


def test_identity_connection_classifies_input_transport_and_http_failures() -> None:
    mode = "timeout"

    def transport(request: httpx.Request) -> httpx.Response:
        if mode == "timeout":
            raise httpx.ConnectTimeout("fixture timeout", request=request)
        if mode == "connect":
            raise httpx.ConnectError("fixture connect failure", request=request)
        if mode == "error":
            raise httpx.ReadError("fixture read failure", request=request)
        return httpx.Response(int(mode))

    gateway = _gateway(transport)
    assert gateway.test_connection("", "token")["error"] == "URL 或 API Key 为空"
    assert gateway.test_connection("http://emby.fixture", "token")["error"] == "连接超时"
    mode = "connect"
    assert "无法连接" in gateway.test_connection("http://emby.fixture", "token")["error"]
    mode = "error"
    assert "fixture read failure" in gateway.test_connection("http://emby.fixture", "token")["error"]
    mode = "401"
    assert "API Key 无效" in gateway.test_connection("http://emby.fixture", "token")["error"]
    mode = "404"
    assert "找不到 Emby" in gateway.test_connection("http://emby.fixture", "token")["error"]
    mode = "503"
    assert gateway.test_connection("http://emby.fixture", "token")["error"] == "连接失败 (HTTP 503)"


def test_identity_plugin_install_honors_manual_origin_and_validates_control_plane_response() -> None:
    calls: list[dict[str, Any]] = []

    class ControlPlane:
        def install_emby_plugin(self, **values: Any) -> Mapping[str, Any]:
            calls.append(values)
            return {"plugin_install": {"ok": True}}

    settings = Settings(etk_server_url_manual="http://192.168.1.50:5257")
    gateway = _gateway(lambda _request: httpx.Response(500), control_plane=ControlPlane)
    result = gateway.install_plugin(
        {"_normalized_url": "http://emby.fixture:8096", "AccessToken": "token"},
        settings,
        "request-1",
    )
    assert result == {"ok": True}
    assert calls[0]["etk_origin"] == "http://192.168.1.50:5257"

    class InvalidControlPlane:
        def install_emby_plugin(self, **_values: Any) -> Mapping[str, Any]:
            return {"plugin_install": "invalid"}

    invalid = _gateway(lambda _request: httpx.Response(500), control_plane=InvalidControlPlane)
    with pytest.raises(RuntimeError, match="missing plugin_install"):
        invalid.install_plugin(
            {"_normalized_url": "http://emby.fixture:8096", "AccessToken": "token"},
            settings,
            "request-2",
        )


def test_identity_library_configuration_skips_unready_plugin_and_missing_token() -> None:
    gateway = _gateway(lambda _request: httpx.Response(500))
    skipped = gateway.configure_libraries(
        {"_normalized_url": "http://emby.fixture"},
        {"ok": False},
    )
    assert skipped == {
        "ok": False,
        "status": "skipped",
        "certainty": "confirmed-no-effect",
        "message": "ETK 插件未就绪，已跳过媒体库加速配置",
    }
    missing_token = gateway.configure_libraries(
        {"_normalized_url": "http://emby.fixture"},
        {"ok": True},
    )
    assert missing_token["updated"] == 0
    assert missing_token["certainty"] == "confirmed-no-effect"


def test_identity_library_configuration_reports_rejected_and_unknown_writes() -> None:
    def transport(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        payload = json.loads(request.content)
        return httpx.Response(400 if payload["Id"] == "rejected" else 503)

    gateway = _gateway(transport)
    folders = [
        {"Name": "Ignored", "CollectionType": "music"},
        {"Name": "Missing ID", "CollectionType": "movies"},
        {"Id": "rejected", "Name": "Rejected", "CollectionType": "movies"},
        {"Id": "unknown", "Name": "Unknown", "CollectionType": "tvshows"},
    ]
    with httpx.Client(transport=httpx.MockTransport(transport)) as client:
        result = gateway._configure_libraries(client, "http://emby.fixture", "token", folders)

    assert result["ok"] is False
    assert result["updated"] == 0
    assert result["failed"] == 3
    assert result["certainty"] == "unknown"
    assert any("缺少媒体库 ID" in error for error in result["errors"])
    assert any("Emby 拒绝" in error for error in result["errors"])
    assert any("无法确认" in error for error in result["errors"])


def test_identity_library_configuration_counts_confirmed_existing_defaults() -> None:
    desired = identity_adapters_module._apply_library_defaults({}, {"Movie", "BoxSet"})
    gateway = _gateway(lambda _request: httpx.Response(500))
    with httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500))) as client:
        result = gateway._configure_libraries(
            client,
            "http://emby.fixture",
            "token",
            [{"Id": "library-1", "CollectionType": "movies", "LibraryOptions": desired}],
        )
    assert result == {
        "ok": True,
        "updated": 0,
        "configured": 1,
        "failed": 0,
        "certainty": "observed-complete",
        "message": "已为 1 个媒体库应用 ETK 加速配置",
    }


def test_identity_library_poll_handles_ambiguous_readback_and_waits_until_deadline() -> None:
    clocks = iter((0.0, 0.5, 2.0))
    sleeps: list[float] = []

    def transport(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"Id": "library-1", "CollectionType": "movies"},
                {"Id": "library-1", "CollectionType": "movies"},
            ],
        )

    gateway = _gateway(
        transport,
        clock=lambda: next(clocks),
        sleeper=sleeps.append,
        reconcile_timeout_seconds=1,
    )
    with httpx.Client(transport=httpx.MockTransport(transport)) as client:
        assert (
            gateway._poll_library_defaults(
                client,
                "http://emby.fixture",
                "token",
                "library-1",
                {"Movie", "BoxSet"},
            )
            is False
        )
    assert sleeps == [0.2]


def test_identity_library_helpers_validate_payloads_and_every_required_default() -> None:
    assert identity_adapters_module._library_item_types({"CollectionType": "tvshows"}) == {
        "Series",
        "Season",
        "Episode",
    }
    assert identity_adapters_module._library_item_types({"CollectionType": "music"}) == set()
    with pytest.raises(ValueError, match="http://"):
        identity_adapters_module._http_base_url("ftp://emby.fixture")

    current = {
        "DisabledLocalMetadataReaders": ["Nfo"],
        "LocalMetadataReaderOrder": ["Nfo", "Other"],
        "TypeOptions": [
            "invalid",
            {"Type": "Movie", "MetadataFetchers": ["Old"]},
        ],
    }
    desired = identity_adapters_module._apply_library_defaults(current, {"Movie", "BoxSet"})
    assert desired["DisabledLocalMetadataReaders"] == ["Nfo"]
    assert desired["LocalMetadataReaderOrder"] == ["Other"]
    assert {option["Type"] for option in desired["TypeOptions"] if option.get("Type")} == {"Movie", "BoxSet"}
    assert identity_adapters_module._library_defaults_confirmed(desired, {"Movie", "BoxSet"}) is True

    mutations = [
        "not-a-mapping",
        {**desired, "EnableRealtimeMonitor": True},
        {**desired, "SaveLocalMetadata": True},
        {**desired, "DisabledLocalMetadataReaders": []},
        {**desired, "LocalMetadataReaderOrder": ["Nfo"]},
        {**desired, "TypeOptions": "invalid"},
        {**desired, "TypeOptions": []},
        {
            **desired,
            "TypeOptions": [
                {**option, "ImageFetchers": ["Wrong"]} if option.get("Type") == "Movie" else option
                for option in desired["TypeOptions"]
            ],
        },
    ]
    assert all(not identity_adapters_module._library_defaults_confirmed(value, {"Movie"}) for value in mutations)

    with httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={}))) as client:
        with pytest.raises(ValueError, match="无效数据"):
            HttpxEmbyIdentityGateway._list_libraries(client, "http://emby.fixture", "token")


def test_runtime_configuration_writer_validates_persists_and_refreshes_snapshot() -> None:
    container = SimpleNamespace(settings=Settings(emby_api_timeout=30))

    class Supervisor:
        def __init__(self) -> None:
            self.config = {"emby_api_timeout": 45}
            self.saved: list[dict[str, Any]] = []

        def save_config_and_reload(self, updates: dict[str, Any]) -> None:
            self.saved.append(updates)

    supervisor = Supervisor()
    writer = RuntimeConfigurationWriter(cast(Any, container), cast(Any, supervisor))
    refreshed = writer.save({"emby_api_timeout": 40})
    assert supervisor.saved == [{"emby_api_timeout": 40}]
    assert refreshed.emby_api_timeout == 45
    assert container.settings is refreshed

    supervisor.config = {}
    assert writer.save({"emby_api_timeout": 50}).emby_api_timeout == 50


def test_runtime_configuration_writer_persists_native_updates_before_refresh() -> None:
    container = SimpleNamespace(settings=Settings(emby_api_timeout=30))

    class Persistence:
        def __init__(self) -> None:
            self.saved: list[tuple[Settings, dict[str, Any]]] = []

        def merge_and_save(self, settings: Settings, updates: Mapping[str, Any]) -> Settings:
            self.saved.append((settings, dict(updates)))
            return settings.with_updates({"persisted_revision": "fixture-revision"})

    class Supervisor:
        def __init__(self) -> None:
            self.config: dict[str, Any] = {}
            self.saved: list[dict[str, Any]] = []

        def save_config_and_reload(self, updates: dict[str, Any]) -> None:
            self.saved.append(updates)
            self.config = dict(updates)

    persistence = Persistence()
    supervisor = Supervisor()
    writer = RuntimeConfigurationWriter(
        cast(Any, container),
        cast(Any, supervisor),
        persistence=cast(Any, persistence),
    )

    refreshed = writer.save({"emby_api_timeout": 40})

    persisted_base, persisted_updates = persistence.saved[0]
    assert persisted_base.emby_api_timeout == 40
    assert persisted_updates == {"emby_api_timeout": 40}
    assert supervisor.saved[0]["emby_api_timeout"] == 40
    assert supervisor.saved[0]["persisted_revision"] == "fixture-revision"
    assert refreshed.emby_api_timeout == 40
    assert refreshed.get("persisted_revision") == "fixture-revision"
    assert container.settings is refreshed


def test_runtime_configuration_writer_keeps_committed_snapshot_when_reload_fails() -> None:
    container = SimpleNamespace(settings=Settings(emby_api_timeout=30))

    class Persistence:
        def merge_and_save(self, settings: Settings, updates: Mapping[str, Any]) -> Settings:
            return settings.with_updates({**dict(updates), "persisted_revision": "fixture-revision"})

    class Supervisor:
        config: dict[str, Any] = {}

        def save_config_and_reload(self, _updates: dict[str, Any]) -> None:
            raise RuntimeError("fixture reload failed")

    writer = RuntimeConfigurationWriter(
        cast(Any, container),
        cast(Any, Supervisor()),
        persistence=cast(Any, Persistence()),
    )

    refreshed = writer.save({"emby_api_timeout": 40})

    assert refreshed.emby_api_timeout == 40
    assert refreshed.get("persisted_revision") == "fixture-revision"
    assert container.settings is refreshed


def test_runtime_configuration_writer_rejects_nonpersistent_bootstrap_updates() -> None:
    container = SimpleNamespace(settings=Settings())

    class Supervisor:
        config: dict[str, Any] = {}

        def save_config_and_reload(self, _updates: dict[str, Any]) -> None:
            raise AssertionError("bootstrap updates must fail before runtime reload")

    writer = RuntimeConfigurationWriter(cast(Any, container), cast(Any, Supervisor()))

    with pytest.raises(ValueError, match="db_host"):
        writer.save({"db_host": "fixture-db"})


def test_identity_default_client_uses_runtime_timeout() -> None:
    gateway = HttpxEmbyIdentityGateway(lambda: Settings(emby_api_timeout=2))
    with gateway._default_client() as client:
        assert client.timeout.connect == 2
