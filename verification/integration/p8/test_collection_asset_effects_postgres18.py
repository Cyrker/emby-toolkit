from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text

from etk.domains.collection.effect_adapters import SqlAlchemyCollectionAssetEffectsAdapter
from etk.platform import Settings


def test_native_collection_asset_owner_archives_reads_and_discards_with_policy_fence(
    p8_runtime: Any,
    tmp_path: Path,
) -> None:
    image = b"fixture-collection-image"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("https://images.example/fixture.jpg")
        return httpx.Response(200, content=image, headers={"Content-Type": "image/jpeg"})

    settings = Settings(media_image_archive_enabled=True)
    adapter = SqlAlchemyCollectionAssetEffectsAdapter(
        p8_runtime.unit_of_work,
        lambda: settings,
        data_root=tmp_path,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    archived = adapter.archive_image("https://images.example/fixture.jpg")
    assert archived is not None
    assert archived["byte_size"] == len(image)
    assert adapter.read_archived_image("https://images.example/fixture.jpg") == archived
    cached_path = tmp_path / "media_images" / str(archived["relative_path"])
    assert cached_path.read_bytes() == image

    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_image_policy_cache (
                    item_type, tmdb_id, season_number, episode_number,
                    policy_json, images_json, updated_at
                ) VALUES (
                    'BoxSet', '25', -1, -1, '[]'::jsonb, CAST(:images AS jsonb), NOW()
                )
                """
            ),
            {
                "images": json.dumps(
                    [{"source_url": "https://images.example/fixture.jpg"}],
                    separators=(",", ":"),
                )
            },
        )
    assert adapter.discard_unreferenced_sources(["https://images.example/fixture.jpg"]) == 0
    assert cached_path.is_file()

    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM media_image_policy_cache
                WHERE item_type = 'BoxSet' AND tmdb_id = '25'
                """
            )
        )
    assert adapter.discard_unreferenced_sources(["https://images.example/fixture.jpg"]) == 1
    assert not cached_path.exists()
    assert adapter.read_archived_image("https://images.example/fixture.jpg") is None


def test_native_collection_asset_owner_cleans_only_inactive_placeholder_files(
    p8_runtime: Any,
    tmp_path: Path,
) -> None:
    directory = tmp_path / "cache" / "missing_posters"
    directory.mkdir(parents=True)
    placeholder = directory / "2501_WANTED.jpg"
    placeholder.write_bytes(b"fixture-placeholder")
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, subscription_status
                ) VALUES ('2501', 'Movie', 'Fixture', FALSE, 'WANTED')
                """
            )
        )
    adapter = SqlAlchemyCollectionAssetEffectsAdapter(
        p8_runtime.unit_of_work,
        lambda: Settings(),
        data_root=tmp_path,
    )

    assert adapter.cleanup_placeholder("2501") is True
    assert placeholder.is_file()
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET subscription_status = 'NONE'
                WHERE tmdb_id = '2501' AND item_type = 'Movie'
                """
            )
        )
    assert adapter.cleanup_placeholder("2501") is True
    assert not placeholder.exists()
