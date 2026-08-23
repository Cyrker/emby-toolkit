from __future__ import annotations

from handler import media_image_cache


def test_formal_episode_image_schedules_persistent_screenshot_cleanup(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(media_image_cache, "image_archive_enabled", lambda: True)
    monkeypatch.setattr(
        media_image_cache,
        "spawn_later",
        lambda delay, function, *args: calls.append((delay, function, args)),
    )
    payload = {
        "_screenshot_hash": "fixture-screenshot-hash",
        "_emby_item_ids": [],
        "item_type": "Episode",
        "series_tmdb_id": 100,
        "season_number": 2,
        "episode_number": 3,
        "images": {"primary": "https://fixture.invalid/formal.jpg"},
    }

    media_image_cache.archive_metadata_images(payload, "http://fixture.invalid", archive_sources=False)

    assert calls == [
        (
            30,
            media_image_cache.discard_episode_screenshot,
            (100, 2, 3),
        )
    ]
