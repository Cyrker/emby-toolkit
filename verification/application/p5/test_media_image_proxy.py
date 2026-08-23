from __future__ import annotations

import ast
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from requests.exceptions import RequestException

import handler
from etk.domains.media.image_proxy import (
    TRANSPARENT_PIXEL_PNG,
    CachedImage,
    ImageProxyUpstreamError,
    MediaImageProxyService,
    UpstreamImage,
    is_supported_external_image_url,
)
from etk.domains.media.image_proxy_compatibility import LegacyMediaImageProxyGateway
from etk.domains.media.image_proxy_router import router

REPO_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = REPO_ROOT / "verification/fixtures/http/p5-media-image-proxy.json"
COMPATIBILITY_PATH = REPO_ROOT / "src/etk/domains/media/image_proxy_compatibility.py"
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
CASES = tuple(CONTRACT["cases"])


class FakeGateway:
    def __init__(self, case: Mapping[str, Any], cache_path: Path) -> None:
        gateway = case["gateway"]
        self.gateway = gateway
        self.cache_path = cache_path
        self.ready = bool(gateway.get("ready", True))
        self.cache_calls: list[str] = []
        self.external_calls: list[str] = []
        self.emby_calls: list[dict[str, str]] = []

    def processor_ready(self) -> bool:
        return self.ready

    def cached_image(self, cache_key: str) -> CachedImage | None:
        self.cache_calls.append(cache_key)
        if self.gateway.get("cache_result") != "hit":
            return None
        if not self.cache_path.exists():
            self.cache_path.write_bytes(str(self.gateway["cache_body"]).encode())
        return CachedImage(
            str(self.cache_path),
            str(self.gateway.get("cache_content_type") or "application/octet-stream"),
        )

    def fetch_external(self, external_url: str) -> UpstreamImage:
        self.external_calls.append(external_url)
        if self.gateway.get("external_error"):
            raise ImageProxyUpstreamError("fixture external failure")
        return UpstreamImage(
            tuple(str(chunk).encode() for chunk in self.gateway.get("external_chunks", [])),
            int(self.gateway.get("external_status", 200)),
            self.gateway.get("external_content_type"),
        )

    def fetch_emby(self, image_path: str, raw_query: bytes) -> UpstreamImage:
        self.emby_calls.append(
            {
                "image_path": image_path,
                "raw_query": raw_query.decode("utf-8"),
            }
        )
        if self.gateway.get("emby_error"):
            raise RuntimeError("fixture Emby failure")
        return UpstreamImage(
            tuple(str(chunk).encode() for chunk in self.gateway.get("emby_chunks", [])),
            int(self.gateway.get("emby_status", 200)),
            self.gateway.get("emby_content_type"),
        )


def _build_app(case: Mapping[str, Any], tmp_path: Path) -> tuple[FastAPI, FakeGateway]:
    gateway = FakeGateway(case, tmp_path / "cached-image.bin")
    app = FastAPI()
    app.state.media_image_proxy_service = MediaImageProxyService(gateway)
    app.include_router(router)
    return app, gateway


@pytest.mark.parametrize("case", CASES, ids=[str(case["case_id"]) for case in CASES])
def test_image_proxy_fixture_contract(case: Mapping[str, Any], tmp_path: Path) -> None:
    app, gateway = _build_app(case, tmp_path)
    request = case["request"]
    with TestClient(app) as client:
        response = client.request(
            str(request["method"]),
            str(request["path"]),
            headers=dict(request.get("headers") or {}),
        )

    expected = case["response"]
    assert response.status_code == expected["status"]
    for name, value in expected["headers"].items():
        assert response.headers[name] == value
    for name in expected.get("absent_headers", []):
        assert name not in response.headers

    body_kind = expected["body_kind"]
    if body_kind == "json":
        assert response.json() == expected["body"]
    elif body_kind == "transparent-pixel":
        assert response.content == TRANSPARENT_PIXEL_PNG
    else:
        assert body_kind == "text-bytes"
        assert response.content == str(expected["body"]).encode()

    expected_gateway = case["expected_gateway"]
    assert gateway.cache_calls == expected_gateway["cache_calls"]
    assert gateway.external_calls == expected_gateway["external_calls"]
    assert gateway.emby_calls == expected_gateway["emby_calls"]


def test_image_proxy_openapi_has_exact_paths_manual_parameters_and_no_422(tmp_path: Path) -> None:
    app, _gateway = _build_app(CASES[0], tmp_path)
    schema = app.openapi()

    assert set(schema["paths"]) == {
        "/api/image_proxy",
        "/image_proxy/{image_path}",
    }
    external = schema["paths"]["/api/image_proxy"]["get"]
    emby = schema["paths"]["/image_proxy/{image_path}"]["get"]
    assert [parameter["name"] for parameter in external["parameters"]] == ["cache", "url"]
    assert emby["parameters"] == [
        {
            "name": "image_path",
            "in": "path",
            "required": True,
            "schema": {"type": "string"},
        }
    ]
    assert "422" not in external["responses"]
    assert "422" not in emby["responses"]
    assert {value for value in external["responses"] if value.isdigit()} == {
        "200",
        "206",
        "304",
        "400",
        "404",
        "416",
    }
    assert {value for value in emby["responses"] if value.isdigit()} == {"200", "404", "503"}
    assert {"2XX", "3XX"} <= set(external["responses"])
    assert {"2XX", "3XX"} <= set(emby["responses"])


def test_empty_emby_image_path_is_route_not_found_instead_of_422(tmp_path: Path) -> None:
    app, gateway = _build_app(CASES[0], tmp_path)
    with TestClient(app) as client:
        response = client.get("/image_proxy/")

    assert response.status_code == 404
    assert gateway.emby_calls == []


def test_cached_image_honors_etag_and_last_modified_conditionals(tmp_path: Path) -> None:
    app, gateway = _build_app(CASES[0], tmp_path)
    with TestClient(app) as client:
        initial = client.get("/api/image_proxy?cache=fixture-cache")
        by_etag = client.get(
            "/api/image_proxy?cache=fixture-cache",
            headers={"If-None-Match": initial.headers["etag"]},
        )
        by_date = client.get(
            "/api/image_proxy?cache=fixture-cache",
            headers={"If-Modified-Since": initial.headers["last-modified"]},
        )

    for response in (by_etag, by_date):
        assert response.status_code == 304
        assert response.content == b""
        assert response.headers["cache-control"] == "public, max-age=31536000, immutable"
        assert response.headers["etag"] == initial.headers["etag"]
        assert response.headers["content-disposition"] == 'inline; filename="cached-image.bin"'
        assert response.headers["accept-ranges"] == "bytes"
        assert "content-type" not in response.headers
        assert "content-length" not in response.headers
    assert gateway.cache_calls == ["fixture-cache"] * 3


def test_cached_image_rejects_an_unsatisfiable_range(tmp_path: Path) -> None:
    app, gateway = _build_app(CASES[0], tmp_path)
    with TestClient(app) as client:
        response = client.get(
            "/api/image_proxy?cache=fixture-cache",
            headers={"Range": "bytes=999999-"},
        )

    assert response.status_code == 416
    assert response.headers["content-range"].startswith("bytes */")
    assert gateway.cache_calls == ["fixture-cache"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://images.example.invalid/poster.jpg", True),
        ("HTTP://images.example.invalid/poster.jpg", True),
        ("file:///fixture/poster.jpg", False),
        ("/fixture/poster.jpg", False),
        ("https:///poster.jpg", False),
    ],
)
def test_external_url_compatibility_validation(value: str, expected: bool) -> None:
    assert is_supported_external_image_url(value) is expected


def test_compatibility_adapter_has_no_eager_legacy_or_http_imports() -> None:
    tree = ast.parse(COMPATIBILITY_PATH.read_text(encoding="utf-8"))
    top_level_imports = [node for node in tree.body if isinstance(node, ast.Import | ast.ImportFrom)]
    imported = {alias.name for node in top_level_imports for alias in node.names if isinstance(node, ast.Import)}
    imported.update(node.module or "" for node in top_level_imports if isinstance(node, ast.ImportFrom))
    assert "config_manager" not in imported
    assert "extensions" not in imported
    assert "requests" not in imported
    assert not any(name.startswith("handler") for name in imported)


def test_compatibility_adapter_uses_sanitized_local_stubs_only_when_called(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, Mapping[str, Any]]] = []
    cache_path = tmp_path / "fixture-cache.bin"
    cache_path.write_bytes(b"fixture-cache")

    class FakeResponse:
        def __init__(self, body: bytes, content_type: str) -> None:
            self.status_code = 200
            self.headers = {"Content-Type": content_type}
            self.body = body
            self.closed = False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, *, chunk_size: int):
            assert chunk_size == 8192
            yield self.body[:8]
            yield self.body[8:]

        def close(self) -> None:
            self.closed = True

    responses: list[FakeResponse] = []

    def request_get(url: str, **kwargs: Any) -> FakeResponse:
        calls.append((url, kwargs))
        response = FakeResponse(
            b"fixture-external" if url.startswith("https://") else b"fixture-emby",
            "image/jpeg" if url.startswith("https://") else "image/png",
        )
        responses.append(response)
        return response

    processor = SimpleNamespace(
        emby_url="http://emby.example.invalid/",
        emby_api_key="fixture-token",
    )
    fake_extensions = SimpleNamespace(media_processor_instance=processor)
    fake_config = SimpleNamespace(
        APP_CONFIG={"user_agent": "fixture-agent"},
        get_proxies_for_requests=lambda: {"https": "http://proxy.example.invalid:8080"},
    )
    fake_cache = SimpleNamespace(
        get_cached_image=lambda cache_key: (
            {"path": str(cache_path), "mime_type": "image/webp"} if cache_key == "fixture-cache" else None
        )
    )
    monkeypatch.setitem(sys.modules, "extensions", fake_extensions)
    monkeypatch.setitem(sys.modules, "config_manager", fake_config)
    monkeypatch.setitem(sys.modules, "handler.media_image_cache", fake_cache)
    monkeypatch.setattr(handler, "media_image_cache", fake_cache, raising=False)

    gateway = LegacyMediaImageProxyGateway(request_get)
    cached = gateway.cached_image("fixture-cache")
    external = gateway.fetch_external("https://images.example.invalid/poster.jpg?size=w500")
    emby = gateway.fetch_emby(
        "Items/movie-1/Images/Primary",
        b"tag=cover%2Fone&quality=90&api_key=caller-value",
    )

    assert gateway.processor_ready() is True
    assert cached == CachedImage(str(cache_path), "image/webp")
    assert b"".join(external.chunks) == b"fixture-external"
    assert b"".join(emby.chunks) == b"fixture-emby"
    assert responses and all(response.closed for response in responses)
    assert calls == [
        (
            "https://images.example.invalid/poster.jpg?size=w500",
            {
                "stream": True,
                "timeout": 15,
                "headers": {
                    "User-Agent": "fixture-agent",
                    "Referer": "https://images.example.invalid/",
                },
                "proxies": {"https": "http://proxy.example.invalid:8080"},
            },
        ),
        (
            "http://emby.example.invalid/Items/movie-1/Images/Primary"
            "?tag=cover%2Fone&quality=90&api_key=caller-value&api_key=fixture-token",
            {"stream": True, "timeout": 20},
        ),
    ]


def test_compatibility_adapter_maps_requests_failure_to_external_failure() -> None:
    def request_get(_url: str, **_kwargs: Any) -> None:
        raise RequestException("fixture upstream failure")

    gateway = LegacyMediaImageProxyGateway(request_get)
    with pytest.raises(ImageProxyUpstreamError, match="external_image_fetch_failed"):
        gateway.fetch_external("https://images.example.invalid/missing.jpg")
