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

import handler
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.media.image_editing import (
    EmbyMediaDetailsNotFoundError,
    ImageUpdateCommand,
    ImageUpdateOutcome,
    MediaImageEditingService,
    TmdbIdentityMissingError,
    TmdbImageCandidate,
    TmdbImageCatalog,
    TmdbImagesNotFoundError,
    UnsupportedImageMediaTypeError,
)
from etk.domains.media.image_editing_compatibility import LegacyMediaImageEditingGateway
from etk.domains.media.image_editing_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = REPO_ROOT / "verification/fixtures/http/p5-media-image-editing.json"
COMPATIBILITY_PATH = REPO_ROOT / "src/etk/domains/media/image_editing_compatibility.py"
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
CASES = tuple(CONTRACT["cases"])
SECRET_KEY = b"p5-media-image-editing-session"


class FakePermissions:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def is_admin(self, user_id: str) -> bool:
        self.calls.append(user_id)
        return user_id == "fixture-admin"


class FakeGateway:
    def __init__(self, case: Mapping[str, Any]) -> None:
        gateway = case["gateway"]
        self.ready = bool(gateway.get("ready", True))
        self.readiness_calls = 0
        self.tmdb_calls: list[str] = []
        self.update_calls: list[ImageUpdateCommand] = []
        self.catalog = _catalog(str(gateway.get("catalog") or "movie"))
        self.error = str(gateway.get("error") or "")
        self.update_outcome = ImageUpdateOutcome(
            bool(gateway.get("update_success")),
            str(gateway.get("message") or ""),
        )

    def processor_ready(self) -> bool:
        self.readiness_calls += 1
        return self.ready

    def tmdb_images(self, item_id: str) -> TmdbImageCatalog:
        self.tmdb_calls.append(item_id)
        _raise_gateway_error(self.error)
        return self.catalog

    def update_image(self, command: ImageUpdateCommand) -> ImageUpdateOutcome:
        self.update_calls.append(command)
        _raise_gateway_error(self.error)
        return self.update_outcome


def _catalog(name: str) -> TmdbImageCatalog:
    payload = CONTRACT["catalogs"][name]

    def candidates(values: list[Mapping[str, Any]]) -> tuple[TmdbImageCandidate, ...]:
        return tuple(
            TmdbImageCandidate(
                preview=str(value["preview"]),
                original=str(value["original"]),
                aspect_ratio=value["aspect_ratio"],
                width=value.get("width"),
                height=value.get("height"),
            )
            for value in values
        )

    return TmdbImageCatalog(
        posters=candidates(payload["posters"]),
        backdrops=candidates(payload["backdrops"]),
        logos=candidates(payload["logos"]),
    )


def _raise_gateway_error(error: str) -> None:
    errors: dict[str, Exception] = {
        "emby-not-found": EmbyMediaDetailsNotFoundError(),
        "tmdb-id-missing": TmdbIdentityMissingError(),
        "unsupported-type": UnsupportedImageMediaTypeError(),
        "images-not-found": TmdbImagesNotFoundError(),
        "unexpected": RuntimeError("fixture gateway failure"),
    }
    if error:
        raise errors[error]


def _build_app(case: Mapping[str, Any]) -> tuple[FastAPI, FakeGateway, FakePermissions, LegacySessionCodec]:
    request_fixture = case["request"]
    gateway = FakeGateway(case)
    permissions = FakePermissions()
    auth_enabled = bool(request_fixture.get("auth_enabled", True))
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app = FastAPI()
    app.state.container = container
    app.state.media_image_editing_service = MediaImageEditingService(gateway)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, gateway, permissions, codec


def _set_user(client: TestClient, codec: LegacySessionCodec, user_id: str) -> None:
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def _request_case(client: TestClient, case: Mapping[str, Any]):
    request_fixture = case["request"]
    method = str(request_fixture["method"])
    path = str(request_fixture["path"])
    if "json" in request_fixture:
        return client.request(method, path, json=request_fixture["json"])
    if "raw_json" in request_fixture:
        return client.request(
            method,
            path,
            content=str(request_fixture["raw_json"]),
            headers={"Content-Type": "application/json"},
        )
    multipart = request_fixture.get("multipart")
    if isinstance(multipart, Mapping):
        files: dict[str, tuple[Any, ...]] = {
            "image_type": (None, str(multipart.get("image_type") or "")),
        }
        if multipart.get("image_url") is not None:
            files["image_url"] = (None, str(multipart["image_url"]))
        file_fixture = multipart.get("file")
        if isinstance(file_fixture, Mapping):
            files["file"] = (
                str(file_fixture["filename"]),
                str(file_fixture["content"]).encode(),
                str(file_fixture["content_type"]),
            )
        return client.request(method, path, files=files)
    return client.request(method, path)


def _expected_response(case: Mapping[str, Any]) -> tuple[int, Mapping[str, Any]]:
    response = case["response"]
    catalog = str(response.get("body_from_catalog") or "")
    body = CONTRACT["catalogs"][catalog] if catalog else response["body"]
    return int(response["status"]), body


@pytest.mark.parametrize("case", CASES, ids=[str(case["case_id"]) for case in CASES])
def test_image_editing_fixture_contract(case: Mapping[str, Any]) -> None:
    app, gateway, permissions, codec = _build_app(case)
    with TestClient(app) as client:
        user_id = str(case["request"].get("user") or "")
        if user_id:
            _set_user(client, codec, user_id)
        response = _request_case(client, case)

    expected_status, expected_body = _expected_response(case)
    assert response.status_code == expected_status
    assert response.headers["content-type"] == "application/json"
    assert response.json() == expected_body

    case_id = str(case["case_id"])
    if case_id == "post-permission-denied":
        assert permissions.calls == ["fixture-user"]
        assert gateway.readiness_calls == 0
        assert gateway.update_calls == []
    if case_id in {"get-processor-not-ready", "post-processor-not-ready"}:
        assert gateway.tmdb_calls == []
        assert gateway.update_calls == []
    if case_id == "post-auth-disabled-success":
        assert permissions.calls == []
    if case_id == "post-json-url-success":
        assert gateway.update_calls == [
            ImageUpdateCommand(
                item_id="movie-1",
                image_type="poster",
                image_url="https://fixture.invalid/poster.jpg",
                image_bytes=None,
                content_type=None,
            )
        ]
    if case_id == "post-multipart-file-success":
        assert gateway.update_calls == [
            ImageUpdateCommand(
                item_id="movie-1",
                image_type="clearlogo",
                image_url="",
                image_bytes=b"fixture-logo-bytes",
                content_type="image/png",
            )
        ]
    if case_id == "post-multipart-url-success":
        assert gateway.update_calls == [
            ImageUpdateCommand(
                item_id="movie-1",
                image_type="fanart",
                image_url="https://fixture.invalid/fanart.jpg",
                image_bytes=None,
                content_type=None,
            )
        ]


def test_image_editing_openapi_has_exact_paths_manual_bodies_and_no_422() -> None:
    app, _gateway, _permissions, _codec = _build_app(CASES[0])
    schema = app.openapi()

    assert set(schema["paths"]) == {
        "/api/tmdb_images/{item_id}",
        "/api/update_media_image/{item_id}",
    }
    get_operation = schema["paths"]["/api/tmdb_images/{item_id}"]["get"]
    post_operation = schema["paths"]["/api/update_media_image/{item_id}"]["post"]
    assert "422" not in get_operation["responses"]
    assert "422" not in post_operation["responses"]
    assert {int(value) for value in get_operation["responses"]} == {200, 400, 404, 500, 503}
    assert {int(value) for value in post_operation["responses"]} == {200, 202, 400, 403, 500, 503}
    content = post_operation["requestBody"]["content"]
    assert set(content) == {"application/json", "multipart/form-data"}
    assert content["application/json"]["schema"]["oneOf"] == [{"required": ["image_url"]}]
    assert content["multipart/form-data"]["schema"]["oneOf"] == [
        {"required": ["image_url"]},
        {"required": ["file"]},
    ]
    effect_schema = post_operation["responses"]["202"]["content"]["application/json"]["schema"]
    assert effect_schema == {"$ref": "#/components/schemas/ImageUpdateResponse"}


def test_image_editing_service_exposes_durable_unknown_and_pending_effect_states() -> None:
    gateway = FakeGateway(CASES[0])
    service = MediaImageEditingService(gateway)
    gateway.update_outcome = ImageUpdateOutcome(
        False,
        "图片写入结果未知，禁止自动重放",
        effect_id="fixture-effect-unknown",
        effect_status="indeterminate",
    )
    unknown = service.update_image(
        "movie-1",
        image_type="poster",
        image_url="https://fixture.invalid/poster.jpg",
    )
    gateway.update_outcome = ImageUpdateOutcome(
        False,
        "图片副作用正在执行",
        effect_id="fixture-effect-pending",
        effect_status="pending",
    )
    pending = service.update_image(
        "movie-1",
        image_type="poster",
        image_url="https://fixture.invalid/poster.jpg",
    )

    assert unknown.status_code == 503
    assert unknown.payload == {
        "error": "图片写入结果未知，禁止自动重放",
        "effect_id": "fixture-effect-unknown",
        "effect_status": "indeterminate",
    }
    assert pending.status_code == 202
    assert pending.payload == {
        "error": "图片副作用正在执行",
        "effect_id": "fixture-effect-pending",
        "effect_status": "pending",
    }


def test_compatibility_adapter_has_no_eager_legacy_imports() -> None:
    tree = ast.parse(COMPATIBILITY_PATH.read_text(encoding="utf-8"))
    top_level_imports = [node for node in tree.body if isinstance(node, ast.Import | ast.ImportFrom)]
    imported = {alias.name for node in top_level_imports for alias in node.names if isinstance(node, ast.Import)}
    imported.update(node.module or "" for node in top_level_imports if isinstance(node, ast.ImportFrom))
    assert "extensions" not in imported
    assert not any(name.startswith("handler") for name in imported)


def test_compatibility_adapter_uses_stubbed_legacy_clients_only_when_called(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tmdb_calls: list[tuple[str, int, tuple[Any, ...], Mapping[str, Any]]] = []
    update_calls: list[Mapping[str, Any]] = []

    class Processor:
        emby_url = "http://emby.fixture.invalid"
        emby_api_key = "fixture-token"
        emby_user_id = "fixture-user"
        tmdb_api_key = "fixture-tmdb-key"

        def update_media_image_manually(self, **kwargs: Any) -> tuple[bool, str]:
            update_calls.append(kwargs)
            return True, "poster 替换成功"

    def get_item(item_id: str, *_args: Any) -> Mapping[str, Any]:
        return {
            "ProviderIds": {"Tmdb": "100"},
            "Type": "Series" if item_id == "series-1" else "Movie",
        }

    def movie(tmdb_id: int, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        tmdb_calls.append(("movie", tmdb_id, args, kwargs))
        return {"images": {"posters": [{"file_path": "/movie.jpg", "width": 1000, "height": 1500}]}}

    def series(tmdb_id: int, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        tmdb_calls.append(("series", tmdb_id, args, kwargs))
        return {"images": {"backdrops": [{"file_path": "/series.jpg", "aspect_ratio": 1.778}]}}

    fake_extensions = SimpleNamespace(media_processor_instance=Processor())
    fake_emby = SimpleNamespace(get_emby_item_details=get_item)
    fake_tmdb = SimpleNamespace(get_movie_details=movie, get_tv_details=series)
    monkeypatch.setitem(sys.modules, "extensions", fake_extensions)
    monkeypatch.setitem(sys.modules, "handler.emby", fake_emby)
    monkeypatch.setitem(sys.modules, "handler.tmdb", fake_tmdb)
    monkeypatch.setattr(handler, "emby", fake_emby, raising=False)
    monkeypatch.setattr(handler, "tmdb", fake_tmdb, raising=False)

    gateway = LegacyMediaImageEditingGateway()
    movie_catalog = gateway.tmdb_images("movie-1")
    series_catalog = gateway.tmdb_images("series-1")
    outcome = gateway.update_image(
        ImageUpdateCommand(
            item_id="movie-1",
            image_type="poster",
            image_url="https://fixture.invalid/poster.jpg",
            image_bytes=None,
            content_type=None,
        )
    )

    expected_tmdb_kwargs = {
        "append_to_response": "images",
        "include_image_language": "zh-CN,zh-TW,zh,en,null,ja,ko",
    }
    assert tmdb_calls == [
        ("movie", 100, ("fixture-tmdb-key",), expected_tmdb_kwargs),
        ("series", 100, ("fixture-tmdb-key",), expected_tmdb_kwargs),
    ]
    assert movie_catalog.posters[0].preview.endswith("/movie.jpg")
    assert series_catalog.backdrops[0].original.endswith("/series.jpg")
    assert outcome == ImageUpdateOutcome(True, "poster 替换成功")
    assert update_calls == [
        {
            "item_id": "movie-1",
            "image_type": "poster",
            "image_url": "https://fixture.invalid/poster.jpg",
            "image_bytes": None,
            "content_type": None,
        }
    ]
