from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.media.editing import (
    ChapterUpdate,
    MediaCommandOutcome,
    MediaCommandStatus,
    MediaEditingService,
    prepare_chapter_update,
    validate_mediainfo_payload,
)
from etk.domains.media.editing_router import router
from etk.domains.media.models import MediaInfoEditContext, MediaInfoEpisode
from etk.domains.media.repository import (
    MediaAssetAlignmentError,
    MediaIdentityNotFoundError,
)
from etk.platform.web import CookieSessionMiddleware, PermissionPolicy

_CONTRACT = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/p5-media-editing.json").read_text(
        encoding="utf-8"
    )
)
_SESSION_CODEC = LegacySessionCodec.from_secret_key(b"p5-media-editing-session")


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeRepository:
    def __init__(self) -> None:
        fixture = _CONTRACT["fixture"]
        self.context = MediaInfoEditContext(
            item_id=fixture["item_id"],
            sha1=fixture["sha1"],
            mediainfo=tuple(fixture["mediainfo"]),
            title="Fixture Movie",
            item_type="Movie",
        )
        self.episodes = (
            MediaInfoEpisode(
                emby_id="fixture-episode-1",
                season_number=1,
                episode_number=1,
                title="Fixture Episode",
            ),
        )
        self.context_error: Exception | None = None
        self.series_error: Exception | None = None
        self.calls = 0

    def resolve_media_info_context(self, item_id: str) -> MediaInfoEditContext:
        self.calls += 1
        if self.context_error is not None:
            raise self.context_error
        assert item_id == _CONTRACT["fixture"]["item_id"]
        return self.context

    def list_series_media_info_episodes(self, series_id: str) -> tuple[MediaInfoEpisode, ...]:
        self.calls += 1
        if self.series_error is not None:
            raise self.series_error
        assert series_id == _CONTRACT["fixture"]["series_id"]
        return self.episodes


class FakeCommands:
    def __init__(self) -> None:
        self.chapter_updates: list[tuple[MediaInfoEditContext, ChapterUpdate]] = []
        self.mediainfo_updates: list[tuple[MediaInfoEditContext, object]] = []
        self.outcome = MediaCommandOutcome(
            MediaCommandStatus.CONFIRMED,
            "fixture-effect-1",
            provider_result={"Updated": True},
        )

    def apply_chapters(
        self,
        context: MediaInfoEditContext,
        update: ChapterUpdate,
    ) -> MediaCommandOutcome:
        self.chapter_updates.append((context, update))
        return self.outcome

    def apply_mediainfo(
        self,
        context: MediaInfoEditContext,
        mediainfo: object,
    ) -> MediaCommandOutcome:
        self.mediainfo_updates.append((context, mediainfo))
        return self.outcome


def _service(
    repository: FakeRepository,
    *,
    ready: bool = True,
    commands: FakeCommands | None = None,
) -> MediaEditingService:
    return MediaEditingService(
        cast(Any, repository),
        processor_ready=lambda: ready,
        commands=cast(Any, commands),
    )


def _client(
    service: MediaEditingService,
    *,
    auth_enabled: bool = False,
) -> tuple[TestClient, FastAPI]:
    app = FastAPI()
    app.state.media_editing_service = service
    app.state.permission_policy = PermissionPolicy(
        FakePermissions(),
        auth_enabled=auth_enabled,
    )
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: _SESSION_CODEC)
    app.include_router(router)
    return TestClient(app), app


def _set_user(client: TestClient, user_id: str) -> None:
    client.cookies.set("session", _SESSION_CODEC.dumps({"emby_user_id": user_id}))


def test_media_info_read_routes_match_the_sanitized_contract() -> None:
    repository = FakeRepository()
    client, _app = _client(_service(repository))

    with client:
        media_info = client.get(f"/api/media_info/edit/{_CONTRACT['fixture']['item_id']}")
        chapters = client.get(f"/api/media_info/chapters/{_CONTRACT['fixture']['item_id']}")
        episodes = client.get(f"/api/media_info/series/{_CONTRACT['fixture']['series_id']}/episodes")

    assert media_info.status_code == _CONTRACT["responses"]["media_info"]["status"]
    assert media_info.json() == _CONTRACT["responses"]["media_info"]["body"]
    assert chapters.status_code == _CONTRACT["responses"]["chapters"]["status"]
    assert chapters.json() == _CONTRACT["responses"]["chapters"]["body"]
    assert episodes.status_code == _CONTRACT["responses"]["episodes"]["status"]
    assert episodes.json() == _CONTRACT["responses"]["episodes"]["body"]
    assert repository.calls == 3


def test_media_info_read_routes_preserve_ready_and_fail_closed_guards() -> None:
    unavailable_repository = FakeRepository()
    unavailable, _app = _client(_service(unavailable_repository, ready=False))
    with unavailable:
        response = unavailable.get(f"/api/media_info/edit/{_CONTRACT['fixture']['item_id']}")
    assert response.status_code == _CONTRACT["responses"]["processor_unavailable"]["status"]
    assert response.json() == _CONTRACT["responses"]["processor_unavailable"]["body"]
    assert unavailable_repository.calls == 0

    conflict_repository = FakeRepository()
    conflict_repository.context_error = MediaAssetAlignmentError("fixture conflict")
    conflict, _app = _client(_service(conflict_repository))
    with conflict:
        response = conflict.get(f"/api/media_info/edit/{_CONTRACT['fixture']['item_id']}")
    assert response.status_code == _CONTRACT["responses"]["unaligned_asset"]["status"]
    assert response.json() == _CONTRACT["responses"]["unaligned_asset"]["body"]

    missing_repository = FakeRepository()
    missing_repository.series_error = MediaIdentityNotFoundError("missing")
    missing, _app = _client(_service(missing_repository))
    with missing:
        response = missing.get(f"/api/media_info/series/{_CONTRACT['fixture']['series_id']}/episodes")
    assert response.status_code == 404
    assert response.json() == {"error": "未在数据库中找到该剧集信息"}


def test_media_info_openapi_has_five_exact_operations_manual_bodies_and_no_422() -> None:
    client, app = _client(_service(FakeRepository()))
    with client:
        schema = app.openapi()

    assert set(schema["paths"]) == {
        "/api/media_info/edit/{item_id}",
        "/api/media_info/chapters/{item_id}",
        "/api/media_info/series/{series_id}/episodes",
    }
    operations = [operation for methods in schema["paths"].values() for operation in methods.values()]
    assert len(operations) == 5
    for operation in operations:
        assert "200" in operation["responses"]
        assert "422" not in operation["responses"]
    read_statuses = {200, 404, 409, 500, 503}
    write_statuses = {200, 202, 400, 403, 404, 409, 415, 500, 502, 503}
    for path, method in (
        ("/api/media_info/edit/{item_id}", "get"),
        ("/api/media_info/chapters/{item_id}", "get"),
        ("/api/media_info/series/{series_id}/episodes", "get"),
    ):
        assert {int(value) for value in schema["paths"][path][method]["responses"]} == read_statuses
    for path in (
        "/api/media_info/edit/{item_id}",
        "/api/media_info/chapters/{item_id}",
    ):
        operation = schema["paths"][path]["post"]
        assert {int(value) for value in operation["responses"]} == write_statuses
        content = operation["requestBody"]["content"]
        assert set(content) == {"application/json"}
        assert set(operation["responses"]["400"]["content"]) == {"application/json", "text/html"}
        assert set(operation["responses"]["500"]["content"]) == {"application/json", "text/html"}
        assert set(operation["responses"]["415"]["content"]) == {"text/html"}


def test_chapter_update_validates_stale_binding_and_runtime_before_mutation() -> None:
    context = FakeRepository().context
    update = prepare_chapter_update(
        context,
        {
            "sha1": context.sha1.lower(),
            "intro_enabled": True,
            "intro_start_seconds": 2,
            "intro_end_seconds": 6,
            "credits_enabled": False,
        },
    )

    assert update.intro_start_ticks == 20_000_000
    assert update.intro_end_ticks == 60_000_000
    assert update.credits_start_ticks is None
    assert update.clear_intro is False
    assert update.clear_credits is True
    assert isinstance(update.mediainfo, tuple)
    root = update.mediainfo[0]
    markers = [value.get("MarkerType") for value in root["Chapters"]]
    assert markers == ["IntroStart", "IntroEnd"]
    assert isinstance(context.mediainfo, tuple)
    assert context.mediainfo[0]["Chapters"][2]["MarkerType"] == "CreditsStart"

    with pytest.raises(ValueError, match="当前媒体信息已变化"):
        prepare_chapter_update(context, {"sha1": "B" * 40})
    with pytest.raises(ValueError, match="片头结束必须大于"):
        prepare_chapter_update(
            context,
            {
                "sha1": context.sha1,
                "intro_enabled": True,
                "intro_start_seconds": 6,
                "intro_end_seconds": 2,
            },
        )
    with pytest.raises(ValueError, match="片尾开始不能超过"):
        prepare_chapter_update(
            context,
            {
                "sha1": context.sha1,
                "credits_enabled": True,
                "credits_start_seconds": 60,
            },
        )


def test_generic_mediainfo_validation_requires_a_stream_array() -> None:
    valid = validate_mediainfo_payload(_CONTRACT["fixture"]["mediainfo"])
    assert isinstance(valid, tuple)

    with pytest.raises(ValueError, match="MediaSourceInfo.MediaStreams"):
        validate_mediainfo_payload([{"MediaSourceInfo": {}}])
    with pytest.raises(ValueError, match="媒体信息格式无效"):
        validate_mediainfo_payload([])


def test_media_info_commands_validate_binding_and_map_persistent_effect_outcomes() -> None:
    repository = FakeRepository()
    commands = FakeCommands()
    service = _service(repository, commands=commands)
    fixture = _CONTRACT["fixture"]

    chapters = service.update_chapters(
        fixture["item_id"],
        {
            "sha1": fixture["sha1"],
            "intro_enabled": True,
            "intro_start_seconds": 1,
            "intro_end_seconds": 5,
            "credits_enabled": True,
            "credits_start_seconds": 55,
        },
    )
    mediainfo = service.update_media_info(
        fixture["item_id"],
        {"sha1": fixture["sha1"], "mediainfo": fixture["mediainfo"]},
    )

    assert chapters.status_code == 200
    assert chapters.payload["message"] == "片头片尾已更新并写入 Emby"
    assert chapters.payload["result"] == {"Updated": True}
    assert mediainfo.status_code == 200
    assert mediainfo.payload["message"] == "媒体信息已更新并写入 Emby"
    assert len(commands.chapter_updates) == 1
    assert len(commands.mediainfo_updates) == 1

    stale = service.update_media_info(
        fixture["item_id"],
        {"sha1": "B" * 40, "mediainfo": fixture["mediainfo"]},
    )
    assert stale.status_code == 409
    assert len(commands.mediainfo_updates) == 1

    commands.outcome = MediaCommandOutcome(
        MediaCommandStatus.INDETERMINATE,
        "fixture-effect-unknown",
        error="Bridge 返回结果未知，禁止自动重放",
    )
    unknown = service.update_media_info(
        fixture["item_id"],
        {"sha1": fixture["sha1"], "mediainfo": fixture["mediainfo"]},
    )
    assert unknown.status_code == 503
    assert unknown.payload == {
        "error": "Bridge 返回结果未知，禁止自动重放",
        "effect_id": "fixture-effect-unknown",
        "effect_status": "indeterminate",
    }


def test_media_info_write_routes_match_sanitized_contract() -> None:
    repository = FakeRepository()
    commands = FakeCommands()
    client, _app = _client(_service(repository, commands=commands))
    fixture = _CONTRACT["fixture"]

    with client:
        chapters = client.post(
            f"/api/media_info/chapters/{fixture['item_id']}",
            json={
                "sha1": fixture["sha1"],
                "intro_enabled": True,
                "intro_start_seconds": 2,
                "intro_end_seconds": 6,
                "credits_enabled": False,
            },
        )
        media_info = client.post(
            f"/api/media_info/edit/{fixture['item_id']}",
            json={"sha1": fixture["sha1"], "mediainfo": fixture["mediainfo"]},
        )

    expected_chapters = _CONTRACT["responses"]["chapter_update"]
    expected_media_info = _CONTRACT["responses"]["media_info_update"]
    assert chapters.status_code == expected_chapters["status"]
    assert chapters.json() == expected_chapters["body"]
    assert media_info.status_code == expected_media_info["status"]
    assert media_info.json() == expected_media_info["body"]
    assert len(commands.chapter_updates) == 1
    assert len(commands.mediainfo_updates) == 1


def test_media_info_write_routes_preserve_guard_order_and_legacy_http_body_failures() -> None:
    fixture = _CONTRACT["fixture"]
    denied_client, _app = _client(
        _service(FakeRepository(), commands=FakeCommands()),
        auth_enabled=True,
    )
    with denied_client:
        _set_user(denied_client, "fixture-user")
        denied = denied_client.post(f"/api/media_info/edit/{fixture['item_id']}")
    assert denied.status_code == 403
    assert denied.json() == {
        "status": "error",
        "message": "需要 Emby 管理员权限才能执行此操作",
    }

    unavailable_client, _app = _client(
        _service(FakeRepository(), ready=False, commands=FakeCommands()),
    )
    with unavailable_client:
        unavailable = unavailable_client.post(f"/api/media_info/edit/{fixture['item_id']}")
    assert unavailable.status_code == 503
    assert unavailable.json() == {"error": "核心处理器未就绪。"}

    client, _app = _client(_service(FakeRepository(), commands=FakeCommands()))
    with client:
        unsupported = client.post(f"/api/media_info/edit/{fixture['item_id']}")
        malformed = client.post(
            f"/api/media_info/chapters/{fixture['item_id']}",
            content="{",
            headers={"content-type": "application/json"},
        )
        wrong_shape = client.post(
            f"/api/media_info/edit/{fixture['item_id']}",
            json=[],
        )
    assert unsupported.status_code == 415
    assert malformed.status_code == 400
    assert wrong_shape.status_code == 500
    for response in (unsupported, malformed, wrong_shape):
        assert response.headers["content-type"] == "text/html; charset=utf-8"
        assert response.text.startswith("<!doctype html>")
