from __future__ import annotations

import pytest

from etk.domains.media.asset_versions import (
    AlignedAssetVersionArrays,
    AssetVersionSlot,
    align_asset_version_slots,
)


def _assert_equal_lengths(result: AlignedAssetVersionArrays) -> None:
    lengths = {
        len(result.asset_details),
        len(result.emby_item_ids),
        len(result.file_sha1s),
        len(result.file_pickcodes),
    }
    assert lengths == {len(result.asset_details)}


@pytest.mark.parametrize(
    ("item_type", "path_prefix"),
    [("Movie", "/library/movies"), ("Episode", "/library/series/s01")],
)
def test_missing_fingerprints_keep_empty_placeholders(item_type: str, path_prefix: str) -> None:
    result = align_asset_version_slots(
        [
            AssetVersionSlot(
                asset_details={"path": f"{path_prefix}/first.mkv", "item_type": item_type},
                emby_item_id="item-first",
                path=f"{path_prefix}/first.mkv",
                file_sha1="",
                file_pickcode="pc-first",
            ),
            AssetVersionSlot(
                asset_details={"path": f"{path_prefix}/second.mkv", "item_type": item_type},
                emby_item_id="item-second",
                path=f"{path_prefix}/second.mkv",
                file_sha1="sha-second",
                file_pickcode="",
            ),
        ]
    )

    _assert_equal_lengths(result)
    assert result.emby_item_ids == ("item-first", "item-second")
    assert result.file_sha1s == ("", "sha-second")
    assert result.file_pickcodes == ("pc-first", "")


def test_movie_duplicate_slot_uses_item_id_and_normalized_path() -> None:
    result = align_asset_version_slots(
        [
            AssetVersionSlot(
                asset_details={"path": "C:\\Media\\Movie.mkv", "label": "first"},
                emby_item_id="movie-version",
                path="C:\\Media\\Movie.mkv",
                file_sha1="",
                file_pickcode="",
            ),
            AssetVersionSlot(
                asset_details={"path": "c:/media/movie.mkv/", "label": "duplicate"},
                emby_item_id="movie-version",
                path="c:/media/movie.mkv/",
                file_sha1="shared-sha1",
                file_pickcode="shared-pc",
            ),
        ]
    )

    _assert_equal_lengths(result)
    assert result.asset_details == ({"path": "C:\\Media\\Movie.mkv", "label": "first"},)
    assert result.file_sha1s == ("shared-sha1",)
    assert result.file_pickcodes == ("shared-pc",)


def test_episode_duplicate_fingerprints_are_kept_for_distinct_slots() -> None:
    result = align_asset_version_slots(
        [
            AssetVersionSlot(
                asset_details={"path": "/series/s01/e01-version-a.strm"},
                emby_item_id="episode-version-a",
                path="/series/s01/e01-version-a.strm",
                file_sha1="shared-sha1",
                file_pickcode="shared-pc",
            ),
            AssetVersionSlot(
                asset_details={"path": "/series/s01/e01-version-b.strm"},
                emby_item_id="episode-version-b",
                path="/series/s01/e01-version-b.strm",
                file_sha1="shared-sha1",
                file_pickcode="shared-pc",
            ),
        ]
    )

    _assert_equal_lengths(result)
    assert result.emby_item_ids == ("episode-version-a", "episode-version-b")
    assert result.file_sha1s == ("shared-sha1", "shared-sha1")
    assert result.file_pickcodes == ("shared-pc", "shared-pc")
