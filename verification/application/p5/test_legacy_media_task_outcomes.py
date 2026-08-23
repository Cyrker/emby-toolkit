from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace
from typing import Any

import constants

import_module("config_manager")
actors = import_module("tasks.actors")
media = import_module("tasks.media")


class FakeTranslator:
    def batch_translate(self, **kwargs: Any) -> dict[str, str]:
        del kwargs
        return {"Alice": "艾丽丝"}


class FakeProcessor:
    def __init__(self) -> None:
        self.config = {constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE: True}
        self.emby_url = "http://emby.fixture.invalid"
        self.emby_api_key = "fixture-token"
        self.emby_user_id = "fixture-user"
        self.ai_translator = FakeTranslator()

    def get_stop_event(self) -> Any:
        return SimpleNamespace()

    def is_stop_requested(self) -> bool:
        return False

    def _extract_thumb_via_ffmpeg(self, video_path: str, output_path: str) -> bool:
        del video_path, output_path
        return False


def test_actor_translation_reports_all_provider_writes_failed(monkeypatch) -> None:
    processor = FakeProcessor()
    monkeypatch.setattr(actors.logger, "trace", lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(
        actors.emby,
        "get_all_persons_from_emby",
        lambda **kwargs: iter([[{"Id": "person-1", "Name": "Alice", "ProviderIds": {}}]]),
    )
    monkeypatch.setattr(actors.emby, "update_person_details", lambda **kwargs: False)
    monkeypatch.setattr(actors.task_manager, "update_status_from_thread", lambda *args, **kwargs: None)

    result = actors.task_persons_translation(processor)

    assert result == {
        "status": "failed",
        "message": "  ➜ 任务完成！共成功翻译并更新了 0 个人物名。",
        "updated": 0,
        "failed_updates": 1,
        "failed_batches": 0,
        "stopped": False,
    }


def test_restore_mediainfo_reports_invalid_asset_as_failed(monkeypatch) -> None:
    processor = FakeProcessor()
    monkeypatch.setattr(media.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        media.media_db,
        "get_all_in_library_physical_paths",
        lambda: [{"path": "", "emby_item_id": "item-1"}],
    )
    from handler.p115_service import P115Service

    monkeypatch.setattr(P115Service, "get_client", lambda: None)
    monkeypatch.setattr(media.task_manager, "update_status_from_thread", lambda *args, **kwargs: None)

    result = media.task_restore_mediainfo(processor, force_full_update=True)

    assert result == {
        "status": "failed",
        "message": "任务完成！成功注入: 0 个，失败: 1 个。",
        "restored": 0,
        "failed": 1,
        "stopped": False,
    }


def test_missing_screenshot_task_reports_capture_failure(monkeypatch) -> None:
    processor = FakeProcessor()
    monkeypatch.setattr(media.logger, "trace", lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(
        media.emby,
        "get_emby_libraries",
        lambda *args, **kwargs: [{"Id": "library-1"}],
    )
    monkeypatch.setattr(
        media.emby,
        "get_emby_library_items",
        lambda **kwargs: [
            {
                "Id": "item-1",
                "Type": "Movie",
                "Path": "http://fixture.invalid/video.strm",
                "ImageTags": {},
            }
        ],
    )
    monkeypatch.setattr(media.task_manager, "update_status_from_thread", lambda *args, **kwargs: None)

    result = media.task_fill_missing_video_screenshots(processor)

    assert result == {
        "status": "failed",
        "message": "补图完成：上传 0 个，失败 1 个，已有图片 0 个，缺图但非 STRM 0 个。",
        "uploaded": 0,
        "failed": 1,
        "stopped": False,
    }
