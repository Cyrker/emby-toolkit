from __future__ import annotations

import ast
import json
import logging
import re
import subprocess
import typing
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.media.bridge_client import (
    BridgeWriteIndeterminate,
    BridgeWriteRejected,
    HttpxEmbyBridgeClient,
    HttpxPluginUpdateGateway,
    PluginUpdateError,
)
from etk.platform import Settings

_CONTRACT = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/p5-media-bridge.json").read_text(
        encoding="utf-8"
    )
)
_BRIDGE_MANIFEST = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/contracts/bridge-contract-ownership.yaml").read_text(
        encoding="utf-8"
    )
)


def _settings() -> Settings:
    return Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
    )


@pytest.mark.parametrize("status_code", [200, 404])
def test_plugin_download_uses_independent_source_without_upstream_fallback(status_code: int) -> None:
    requested: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(status_code, content=b"fixture-dll")

    gateway = HttpxPluginUpdateGateway(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handle)),
    )
    if status_code == 200:
        assert b"".join(gateway.download().chunks) == b"fixture-dll"
    else:
        with pytest.raises(PluginUpdateError, match="plugin_update_download_failed"):
            gateway.download()
    assert requested == [
        "https://github.com/Cyrker/embytool-bridge/releases/latest/download/ETKMediaInfoBridge.dll"
    ]


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> HttpxEmbyBridgeClient:
    transport = httpx.MockTransport(handler)
    return HttpxEmbyBridgeClient(
        _settings,
        client_factory=lambda: httpx.Client(transport=transport),
    )


class _LegacyResponse:
    status_code = 200
    ok = True
    text = ""
    content = b"{}"

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return dict(self._payload)

    def raise_for_status(self) -> None:
        return None


class _LegacyRequestRecorder:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def post(self, url: str, **kwargs: Any) -> _LegacyResponse:
        self.requests.append((url, kwargs))
        payload = {"Configured": True} if url.endswith(("/Origin", "/DiscoveryUrl")) else {}
        return _LegacyResponse(payload)


def _p4_bridge_functions() -> tuple[dict[str, Any], _LegacyRequestRecorder]:
    baseline = _CONTRACT["provenance"]["legacy_baseline_commit"]
    source = subprocess.run(
        ["git", "show", f"{baseline}:handler/emby.py"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    selected_names = {
        "configure_etk_plugin_origin",
        "configure_etk_plugin_discovery_url",
        "normalize_etk_mediainfo_payload",
        "apply_etk_mediainfo",
        "apply_etk_intro_chapters",
        "apply_etk_credits_chapter",
        "update_etk_chapters",
    }
    source_tree = ast.parse(source, filename=f"{baseline}:handler/emby.py")
    selected: list[ast.stmt] = [
        node for node in source_tree.body if isinstance(node, ast.FunctionDef) and node.name in selected_names
    ]
    assert {node.name for node in selected if isinstance(node, ast.FunctionDef)} == selected_names

    recorder = _LegacyRequestRecorder()
    namespace: dict[str, Any] = {
        "Any": Any,
        "Dict": dict,
        "Optional": typing.Optional,
        "emby_client": recorder,
        "json": json,
        "logger": logging.getLogger("p5-legacy-outbound-golden"),
    }
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, f"{baseline}:handler/emby.py", "exec"), namespace)
    return namespace, recorder


def _exercise_p4_legacy_bridge(namespace: dict[str, Any]) -> None:
    assert namespace["configure_etk_plugin_origin"](
        "http://emby.fixture.invalid",
        "fixture-token",
        "http://etk.fixture.invalid",
    )
    assert namespace["configure_etk_plugin_discovery_url"](
        "http://emby.fixture.invalid",
        "fixture-token",
        "http://discovery.fixture.invalid",
    )
    assert (
        namespace["apply_etk_mediainfo"](
            "item-1",
            {
                "MediaSourceInfo": {"MediaStreams": [{"Type": "Video"}]},
                "Chapters": [{"Name": "Chapter"}],
            },
            "http://emby.fixture.invalid",
            "fixture-token",
            drop_conflicting_external_streams=True,
        )
        == {}
    )
    assert (
        namespace["apply_etk_intro_chapters"](
            "item-2",
            10,
            20,
            "http://emby.fixture.invalid",
            "fixture-token",
        )
        == {}
    )
    assert (
        namespace["apply_etk_credits_chapter"](
            "item-3",
            30,
            "http://emby.fixture.invalid",
            "fixture-token",
        )
        == {}
    )
    assert (
        namespace["update_etk_chapters"](
            "item-4",
            "http://emby.fixture.invalid",
            "fixture-token",
            intro_start_ticks=40,
            intro_end_ticks=50,
            credits_start_ticks=60,
            clear_credits=True,
        )
        == {}
    )


def _canonical_request(
    url: str,
    *,
    params: dict[str, Any] | None,
    headers: dict[str, Any] | None,
    body: dict[str, Any],
) -> dict[str, Any]:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query))
    query.update({str(key): str(value) for key, value in (params or {}).items()})
    if "api_key" in query:
        query["api_key"] = "<redacted>"
    normalized_headers = {key.lower(): value for key, value in (headers or {}).items()}
    selected_headers: dict[str, Any] = {}
    if normalized_headers.get("accept") == "application/json":
        selected_headers["Accept"] = "application/json"
    if "x-emby-token" in normalized_headers:
        selected_headers["X-Emby-Token"] = "<redacted>"
    request: dict[str, Any] = {
        "method": "POST",
        "path": parsed.path,
        "body": body,
    }
    if query:
        request["query"] = query
    if selected_headers:
        request["headers"] = selected_headers
    return request


def _legacy_request_snapshots(recorder: _LegacyRequestRecorder) -> list[dict[str, Any]]:
    return [
        _canonical_request(
            url,
            params=kwargs.get("params"),
            headers=kwargs.get("headers"),
            body=dict(kwargs.get("json") or {}),
        )
        for url, kwargs in recorder.requests
    ]


def _native_request_snapshots(requests: list[httpx.Request]) -> list[dict[str, Any]]:
    return [
        _canonical_request(
            str(request.url),
            params=None,
            headers=dict(request.headers),
            body=json.loads(request.content or b"{}"),
        )
        for request in requests
    ]


def test_all_six_bridge_write_contracts_have_typed_requests() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = json.loads(request.content or b"{}")
        if request.url.path.endswith(("/Origin", "/DiscoveryUrl")):
            return httpx.Response(200, json={"Configured": True})
        return httpx.Response(200, json={"received": body})

    bridge = _client(handler)
    origin = bridge.configure_origin("http://etk.fixture.invalid")
    discovery = bridge.configure_discovery_url("http://discovery.fixture.invalid")
    mediainfo = bridge.apply_mediainfo(
        "item / 1",
        {
            "MediaSourceInfo": {"MediaStreams": [{"Type": "Video"}]},
            "Chapters": [{"Name": "Chapter"}],
        },
        drop_conflicting_external_streams=True,
    )
    intro = bridge.apply_intro("item-2", 10, 20)
    credits = bridge.apply_credits("item-3", 30)
    chapters = bridge.update_chapters(
        "item-4",
        intro_start_ticks=40,
        intro_end_ticks=50,
        credits_start_ticks=60,
        clear_credits=True,
    )

    assert origin.confirmed is True
    assert discovery.confirmed is True
    actual_contract_paths = [
        re.sub(
            r"^/Items/[^/]+/ETKMediaInfo",
            "/Items/{item_id}/ETKMediaInfo",
            request.url.raw_path.decode().split("?", 1)[0],
        )
        for request in requests
    ]
    assert actual_contract_paths == [value["path"] for value in _CONTRACT["outbound"]]
    assert requests[0].url.params.get("api_key") == "fixture-token"
    assert requests[1].url.params.get("api_key") == "fixture-token"
    for request in requests[2:]:
        assert request.headers["X-Emby-Token"] == "fixture-token"
    assert mediainfo.payload["received"]["DropConflictingExternalStreams"] is True
    assert intro.payload["received"] == {"IntroStartTicks": 10, "IntroEndTicks": 20}
    assert credits.payload["received"] == {"CreditsStartTicks": 30}
    assert chapters.payload["received"] == {
        "ClearIntro": False,
        "ClearCredits": True,
        "IntroStartTicks": 40,
        "IntroEndTicks": 50,
        "CreditsStartTicks": 60,
    }


def test_all_six_outbound_requests_match_the_ast_extracted_p4_legacy_golden() -> None:
    legacy_namespace, legacy_recorder = _p4_bridge_functions()
    _exercise_p4_legacy_bridge(legacy_namespace)

    native_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        native_requests.append(request)
        payload = {"Configured": True} if request.url.path.endswith(("/Origin", "/DiscoveryUrl")) else {}
        return httpx.Response(200, json=payload, request=request)

    bridge = _client(handler)
    bridge.configure_origin("http://etk.fixture.invalid")
    bridge.configure_discovery_url("http://discovery.fixture.invalid")
    bridge.apply_mediainfo(
        "item-1",
        {
            "MediaSourceInfo": {"MediaStreams": [{"Type": "Video"}]},
            "Chapters": [{"Name": "Chapter"}],
        },
        drop_conflicting_external_streams=True,
    )
    bridge.apply_intro("item-2", 10, 20)
    bridge.apply_credits("item-3", 30)
    bridge.update_chapters(
        "item-4",
        intro_start_ticks=40,
        intro_end_ticks=50,
        credits_start_ticks=60,
        clear_credits=True,
    )

    expected = [case["request"] for case in _CONTRACT["golden_master"]["outbound_cases"]]
    assert _legacy_request_snapshots(legacy_recorder) == expected
    assert _native_request_snapshots(native_requests) == expected


def test_outbound_manifest_and_client_accept_the_full_2xx_success_family() -> None:
    outbound = [contract for contract in _BRIDGE_MANIFEST["contracts"] if contract["direction"] == "outbound"]
    assert len(outbound) == 6
    assert all(contract["response_statuses"] == ["2XX"] for contract in outbound)

    bridge = _client(lambda request: httpx.Response(204, request=request))
    results = [
        bridge.configure_origin("http://etk.fixture.invalid"),
        bridge.configure_discovery_url("http://discovery.fixture.invalid"),
        bridge.apply_mediainfo("item-1", {"MediaSourceInfo": {"MediaStreams": []}}),
        bridge.apply_intro("item-2", 10, 20),
        bridge.apply_credits("item-3", 30),
        bridge.update_chapters("item-4", clear_intro=True),
    ]
    assert [result.status_code for result in results] == [204] * 6


def test_bridge_rejection_is_distinct_from_unknown_write_outcome() -> None:
    rejected = _client(lambda request: httpx.Response(409, text="conflict", request=request))
    with pytest.raises(BridgeWriteRejected) as rejection:
        rejected.apply_credits("item", 10)
    assert rejection.value.status_code == 409

    calls = 0

    def timeout(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("unknown outcome", request=request)

    indeterminate = _client(timeout)
    with pytest.raises(BridgeWriteIndeterminate):
        indeterminate.apply_intro("item", 10, 20)
    assert calls == 1


def test_mediainfo_contract_rejects_payload_without_streams_before_network() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={}, request=request)

    with pytest.raises(ValueError, match="MediaSourceInfo.MediaStreams"):
        _client(handler).apply_mediainfo("item", {"MediaSourceInfo": {}})
    assert calls == 0


def test_success_with_invalid_json_is_indeterminate_and_not_replayed() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"not-json", request=request)

    with pytest.raises(BridgeWriteIndeterminate):
        _client(handler).apply_credits("item", 10)
    assert calls == 1
