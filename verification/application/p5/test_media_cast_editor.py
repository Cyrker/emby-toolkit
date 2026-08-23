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
from etk.domains.media.cast_editor import (
    CastTranslationCommand,
    ManualCastUpdateCommand,
    MediaCastEditorService,
    ProcessedCastActor,
    legacy_http_error_result,
)
from etk.domains.media.cast_editor_compatibility import LegacyMediaCastEditorGateway
from etk.domains.media.cast_editor_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = REPO_ROOT / "verification/fixtures/http/p5-media-cast-editor.json"
COMPATIBILITY_PATH = REPO_ROOT / "src/etk/domains/media/cast_editor_compatibility.py"
LEGACY_ROUTES_PATH = REPO_ROOT / "verification/contracts/http/legacy-runtime-routes.json"
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
CASES = tuple(CONTRACT["cases"])
SECRET_KEY = b"p5-media-cast-editor-session"


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
        self.editing_calls: list[str] = []
        self.submit_calls: list[ManualCastUpdateCommand] = []
        self.translation_calls: list[CastTranslationCommand] = []
        self.details_calls: list[str] = []
        self.preview_calls: list[dict[str, Any]] = []
        self.gateway_fixture = gateway

    def processor_ready(self) -> bool:
        self.readiness_calls += 1
        return self.ready

    def media_for_editing(self, item_id: str) -> Any:
        self.editing_calls.append(item_id)
        if self.gateway_fixture.get("editing_error"):
            raise RuntimeError("fixture editing provider failure")
        if self.gateway_fixture.get("editing") == "fixture":
            return CONTRACT["editing_payload"]
        return None

    def submit_manual_update(self, command: ManualCastUpdateCommand) -> bool:
        self.submit_calls.append(command)
        if self.gateway_fixture.get("task_error"):
            raise RuntimeError("fixture task provider failure")
        return bool(self.gateway_fixture.get("task_accepted"))

    def translate_cast(self, command: CastTranslationCommand) -> Any:
        self.translation_calls.append(command)
        if self.gateway_fixture.get("translation_error"):
            raise RuntimeError("fixture translation provider failure")
        return CONTRACT["translated_cast"]

    def preview_item_details(self, item_id: str) -> dict[str, Any] | None:
        self.details_calls.append(item_id)
        if self.gateway_fixture.get("details_error"):
            raise RuntimeError("fixture Emby failure")
        if not self.gateway_fixture.get("item_details"):
            return None
        return {"Id": item_id, "Name": "Fixture Movie", "People": []}

    def process_preview_cast(
        self,
        item_details: dict[str, Any],
    ) -> tuple[ProcessedCastActor, ...]:
        self.preview_calls.append(item_details)
        if self.gateway_fixture.get("preview_error"):
            raise RuntimeError("fixture cast processing failure")
        return tuple(
            ProcessedCastActor(
                emby_person_id=actor["embyPersonId"],
                name=actor["name"],
                role=actor["role"],
                imdb_id=actor["imdbId"],
                douban_id=actor["doubanId"],
                tmdb_id=actor["tmdbId"],
            )
            for actor in CONTRACT["preview_cast"]
        )


def _build_app(
    case: Mapping[str, Any],
) -> tuple[FastAPI, FakeGateway, FakePermissions, LegacySessionCodec]:
    request_fixture = case["request"]
    gateway = FakeGateway(case)
    permissions = FakePermissions()
    container = AppContainer(
        Settings(auth_enabled=bool(request_fixture.get("auth_enabled", True))),
        repository_providers={"identity": lambda _: permissions},
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app = FastAPI()
    app.state.container = container
    app.state.media_cast_editor_service = MediaCastEditorService(gateway)
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
    return client.request(method, path)


@pytest.mark.parametrize("case", CASES, ids=[str(case["case_id"]) for case in CASES])
def test_media_cast_editor_fixture_contract(case: Mapping[str, Any]) -> None:
    app, gateway, permissions, codec = _build_app(case)
    with TestClient(app) as client:
        user_id = str(case["request"].get("user") or "")
        if user_id:
            _set_user(client, codec, user_id)
        response = _request_case(client, case)

    expected = case["response"]
    assert response.status_code == expected["status"]
    if "html_status" in expected:
        html_result = legacy_http_error_result(int(expected["html_status"]))
        assert response.headers["content-type"] == "text/html; charset=utf-8"
        assert response.text == html_result.payload
    else:
        source = str(expected.get("body_from") or "")
        expected_body = CONTRACT[source] if source else expected["body"]
        assert response.headers["content-type"] == "application/json"
        assert response.json() == expected_body

    case_id = str(case["case_id"])
    if case_id in {"get-permission-denied-before-readiness", "update-denial-precedes-readiness-and-body"}:
        assert permissions.calls == ["fixture-user"]
        assert gateway.readiness_calls == 0
    if case_id == "get-auth-disabled-success":
        assert permissions.calls == []
    if case_id in {"get-processor-not-ready", "update-readiness-precedes-body"}:
        assert gateway.readiness_calls == 1
        assert gateway.editing_calls == []
        assert gateway.submit_calls == []
    if case_id in {"update-invalid-cast", "update-malformed-json", "update-unsupported-content-type"}:
        assert gateway.submit_calls == []
    if case_id == "update-success":
        assert gateway.submit_calls == [
            ManualCastUpdateCommand(
                item_id="fixture-item-1",
                cast=({"tmdbId": 1001, "name": "Fixture Actor", "role": "Fixture Role"},),
                item_name="Fixture Movie",
            )
        ]
    if case_id == "update-task-rejected-still-accepted":
        assert gateway.submit_calls == [
            ManualCastUpdateCommand(
                item_id="fixture-item-1",
                cast=(),
                item_name="Fixture Movie",
            )
        ]
    if case_id == "translate-success":
        assert gateway.translation_calls == [
            CastTranslationCommand(
                cast=({"tmdbId": 1001, "name": "Fixture Actor", "role": "Fixture Role"},),
                title="Fixture Movie",
                year=2026,
            )
        ]
    if case_id == "preview-success-malformed-body-is-ignored":
        assert gateway.details_calls == ["fixture-item-1"]
        assert gateway.preview_calls == [{"Id": "fixture-item-1", "Name": "Fixture Movie", "People": []}]
    if case_id == "preview-processor-not-ready-precedes-body":
        assert gateway.details_calls == []
        assert gateway.preview_calls == []


def test_media_cast_editor_openapi_has_exact_routes_manual_bodies_and_no_422() -> None:
    app, _gateway, _permissions, _codec = _build_app(CASES[0])
    schema = app.openapi()

    assert set(schema["paths"]) == {
        "/api/media_for_editing/{item_id}",
        "/api/update_media_cast_sa/{item_id}",
        "/api/actions/translate_cast_sa",
        "/api/preview_processed_cast/{item_id}",
    }
    for operations in schema["paths"].values():
        for operation in operations.values():
            assert "422" not in operation["responses"]

    expected_statuses = {
        ("get", "/api/media_for_editing/{item_id}"): {200, 403, 404, 500, 503},
        ("post", "/api/update_media_cast_sa/{item_id}"): {202, 400, 403, 415, 500, 503},
        ("post", "/api/actions/translate_cast_sa"): {200, 400, 403, 415, 500, 503},
        ("post", "/api/preview_processed_cast/{item_id}"): {200, 404, 500, 503},
    }
    for (method, path), statuses in expected_statuses.items():
        assert {int(value) for value in schema["paths"][path][method]["responses"]} == statuses

    update = schema["paths"]["/api/update_media_cast_sa/{item_id}"]["post"]
    translate = schema["paths"]["/api/actions/translate_cast_sa"]["post"]
    preview = schema["paths"]["/api/preview_processed_cast/{item_id}"]["post"]
    assert update["requestBody"]["content"]["application/json"]["schema"]["required"] == ["cast"]
    assert translate["requestBody"]["content"]["application/json"]["schema"]["required"] == ["cast"]
    assert "requestBody" not in preview
    assert set(update["responses"]["400"]["content"]) == {"application/json", "text/html"}
    assert set(translate["responses"]["500"]["content"]) == {"application/json", "text/html"}
    assert set(update["responses"]["415"]["content"]) == {"text/html"}


def test_fixture_routes_match_the_owned_legacy_http_surface() -> None:
    legacy = json.loads(LEGACY_ROUTES_PATH.read_text(encoding="utf-8"))
    expected = {(route["method"], route["path"].replace("{item_id}", "<item_id>")) for route in CONTRACT["routes"]}
    actual = {
        (method, route["path"])
        for route in legacy["rules"]
        for method in route["methods"]
        if (method, route["path"]) in expected
    }
    assert actual == expected


def test_compatibility_adapter_has_no_eager_legacy_imports() -> None:
    tree = ast.parse(COMPATIBILITY_PATH.read_text(encoding="utf-8"))
    top_level_imports = [node for node in tree.body if isinstance(node, ast.Import | ast.ImportFrom)]
    imported = {alias.name for node in top_level_imports for alias in node.names if isinstance(node, ast.Import)}
    imported.update(node.module or "" for node in top_level_imports if isinstance(node, ast.ImportFrom))
    assert "extensions" not in imported
    assert "task_manager" not in imported
    assert "tasks.media" not in imported
    assert not any(name.startswith("handler") for name in imported)


def test_compatibility_adapter_uses_stubbed_legacy_dependencies_only_when_called(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, list[Any]] = {
        "editing": [],
        "submit": [],
        "translate": [],
        "details": [],
        "preview": [],
    }

    class Processor:
        emby_url = "https://emby.fixture.invalid"
        emby_api_key = "fixture-emby-token"
        emby_user_id = "fixture-user"

        def get_cast_for_editing(self, item_id: str) -> Mapping[str, Any]:
            calls["editing"].append(item_id)
            return CONTRACT["editing_payload"]

        def translate_cast_list_for_editing(self, **kwargs: Any) -> Any:
            calls["translate"].append(kwargs)
            return CONTRACT["translated_cast"]

        def _process_cast_list(self, **kwargs: Any) -> list[dict[str, Any]]:
            calls["preview"].append(kwargs)
            return [
                {
                    "EmbyPersonId": "fixture-person-1",
                    "Name": "示例演员",
                    "Role": "示例角色",
                    "ImdbId": "fixture-imdb-person",
                    "DoubanCelebrityId": "fixture-douban-person",
                    "TmdbPersonId": 1001,
                }
            ]

        def _reconstruct_full_data_from_db(
            self,
            tmdb_id: str,
            item_type: str,
        ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            calls["preview"].append({"tmdb_id": tmdb_id, "item_type": item_type})
            return {}, [
                {
                    "tmdb_id": 1001,
                    "name": "示例演员",
                    "character": "示例角色",
                    "imdb_id": "fixture-imdb-person",
                    "douban_id": "fixture-douban-person",
                }
            ]

    def get_item(*args: Any) -> dict[str, Any]:
        calls["details"].append(args)
        return {
            "Id": "fixture-item-1",
            "Type": "Movie",
            "ProviderIds": {"Tmdb": "100"},
            "People": [
                {
                    "Id": "fixture-person-1",
                    "Name": "Fixture Actor",
                    "ProviderIds": {"Tmdb": "1001"},
                }
            ],
        }

    def task_manual_update(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("fixture task function must not execute during submission")

    def submit(*args: Any, **kwargs: Any) -> bool:
        calls["submit"].append((args, kwargs))
        return False

    processor = Processor()
    fake_extensions = SimpleNamespace(media_processor_instance=processor)
    fake_emby = SimpleNamespace(get_emby_item_details=get_item)
    fake_task_manager = SimpleNamespace(submit_task=submit)
    fake_tasks_media = SimpleNamespace(task_manual_update=task_manual_update)
    monkeypatch.setitem(sys.modules, "extensions", fake_extensions)
    monkeypatch.setitem(sys.modules, "handler.emby", fake_emby)
    monkeypatch.setitem(sys.modules, "task_manager", fake_task_manager)
    monkeypatch.setitem(sys.modules, "tasks.media", fake_tasks_media)
    monkeypatch.setattr(handler, "emby", fake_emby, raising=False)

    gateway = LegacyMediaCastEditorGateway()
    assert gateway.processor_ready() is True
    assert gateway.media_for_editing("fixture-item-1") == CONTRACT["editing_payload"]
    assert (
        gateway.submit_manual_update(
            ManualCastUpdateCommand(
                item_id="fixture-item-1",
                cast=({"name": "Fixture Actor"},),
                item_name="Fixture Movie",
            )
        )
        is False
    )
    assert (
        gateway.translate_cast(
            CastTranslationCommand(
                cast=({"name": "Fixture Actor"},),
                title="Fixture Movie",
                year=2026,
            )
        )
        == CONTRACT["translated_cast"]
    )
    item_details = gateway.preview_item_details("fixture-item-1")
    assert item_details is not None
    processed = gateway.process_preview_cast(item_details)

    assert calls["editing"] == ["fixture-item-1"]
    submit_args, submit_kwargs = calls["submit"][0]
    assert submit_args == (task_manual_update, "手动更新: Fixture Movie")
    assert submit_kwargs == {
        "processor_type": "media",
        "item_id": "fixture-item-1",
        "manual_cast_list": [{"name": "Fixture Actor"}],
        "item_name": "Fixture Movie",
    }
    assert calls["translate"] == [
        {
            "cast_list": [{"name": "Fixture Actor"}],
            "title": "Fixture Movie",
            "year": 2026,
        }
    ]
    assert calls["details"] == [
        (
            "fixture-item-1",
            "https://emby.fixture.invalid",
            "fixture-emby-token",
            "fixture-user",
        )
    ]
    assert calls["preview"] == [{"tmdb_id": "100", "item_type": "Movie"}]
    assert [actor.to_payload() for actor in processed] == CONTRACT["preview_cast"]
