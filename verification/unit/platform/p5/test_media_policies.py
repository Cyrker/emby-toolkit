from __future__ import annotations

import pytest

from etk.domains.media import (
    ImagePolicyKey,
    MediaIdentity,
    MediaItemType,
    MediaPresentationPolicy,
    MetadataAggregate,
    build_image_language_parameter,
    build_image_language_priority,
    format_metadata_payload,
    normalize_image_rules,
    normalize_media_path,
    select_image_path,
    select_policy_candidates,
)


def test_media_identity_and_path_normalization_are_strict() -> None:
    assert normalize_media_path("C:\\Media\\\\Movie\\") == "C:/Media/Movie"
    assert MediaItemType.parse("episode") is MediaItemType.EPISODE
    assert MediaItemType.parse("BoxSet") is None
    assert MediaItemType.parse("BoxSet", allow_boxset=True) is MediaItemType.BOXSET
    assert ImagePolicyKey.create("Episode", 100, season_number=2, episode_number=3) == ImagePolicyKey(
        item_type=MediaItemType.EPISODE,
        tmdb_id="100",
        season_number=2,
        episode_number=3,
    )
    with pytest.raises(ValueError, match="television identities"):
        MediaIdentity("100", "movie", MediaItemType.EPISODE, 1, 1)


def test_image_language_policy_matches_the_legacy_ordering_contract() -> None:
    priorities = build_image_language_priority("ja", "original")
    assert priorities == [
        ("ja",),
        ("en", "null"),
        ("zh-cn", "zh-tw", "zh-hk", "zh-sg", "zh", "cn"),
    ]
    assert build_image_language_parameter(priorities) == "ja,en,null,zh-CN,zh-TW,zh-HK,zh-SG,zh"
    images = [
        {"file_path": "/zh.jpg", "iso_639_1": "zh-cn", "vote_average": 10, "vote_count": 100},
        {"file_path": "/ja-low.jpg", "iso_639_1": "ja", "vote_average": 5, "vote_count": 10},
        {"file_path": "/ja-high.jpg", "iso_639_1": "ja", "vote_average": 8, "vote_count": 1},
    ]
    assert select_image_path(images, priorities) == "/ja-high.jpg"


def test_image_rules_clamp_limits_and_preserve_manual_selection() -> None:
    rules = normalize_image_rules(
        [
            {"Type": "Backdrop", "Limit": 99, "MinWidth": -10},
            {"type": "Primary", "limit": 4, "min_width": 1200},
            {"type": "Unknown", "limit": 1},
            {"type": "Logo", "limit": 0},
        ]
    )
    assert rules == [
        {"type": "Primary", "limit": 1, "min_width": 1200},
        {"type": "Backdrop", "limit": 20, "min_width": 0},
    ]
    candidates = [
        {"type": "Primary", "source_url": "new", "width": 1600},
        {"type": "Backdrop", "source_url": "bg", "width": 1920},
    ]
    selected = select_policy_candidates(
        candidates,
        rules,
        previous_images=[{"type": "Primary", "source_url": "manual", "manual": True, "index": 0}],
    )
    assert selected == [
        {"type": "Primary", "source_url": "manual", "manual": True, "index": 0},
        {"type": "Backdrop", "source_url": "bg", "width": 1920, "index": 0},
    ]


def test_metadata_payload_preserves_movie_series_season_episode_hierarchy() -> None:
    policy = MediaPresentationPolicy(
        keyword_to_tags=True,
        studio_to_chinese=True,
        actor_role_add_prefix=True,
        max_actors=2,
        keyword_mapping=({"label": "漫改", "ids": [9717]},),
        studio_mapping=({"label": "示例台", "network_ids": [10]},),
    )
    base_row = {
        "tmdb_id": "2001-S01",
        "title": "Season 1",
        "original_title": "Season 1",
        "overview": "overview",
        "release_date": "2026-01-01",
        "release_year": 2026,
        "rating": 8.2,
        "official_rating_json": {"US": "TV-14", "zh": "辅导级"},
        "genres_json": [{"name": "Animation"}],
        "keywords_json": [{"id": 9717, "name": "based on comic"}, {"name": "unmapped"}],
        "production_companies_json": [{"id": 20, "name": "Unmapped Studio"}],
        "networks_json": [{"id": 10, "name": "Fixture Network"}],
        "season_number": 1,
        "episode_number": 99,
        "actors_ready": True,
        "emby_item_ids_json": ["season-1"],
        "poster_path": "/season.jpg",
    }
    identity = MediaIdentity("2001", "tv", MediaItemType.SEASON, season_number=1)
    aggregate = MetadataAggregate(
        identity=identity,
        requested_type=MediaItemType.SEASON,
        row=base_row,
        actors=(
            {"tmdb_id": 1, "name": "Actor", "character": "饰 角色", "order": 1},
            {"tmdb_id": 2, "name": "Actor 2", "character": "配 角色 2", "order": 2},
            {"tmdb_id": 3, "name": "Actor 3", "character": "角色 3", "order": 3},
        ),
    )

    season = format_metadata_payload(aggregate, policy)

    assert season["item_type"] == "Season"
    assert season["series_tmdb_id"] == "2001"
    assert season["season_number"] == 1
    assert season["episode_number"] is None
    assert season["official_rating"] == "TV-14"
    assert season["tags"] == ["漫改"]
    assert season["studios"] == ["示例台"]
    assert [person["role"] for person in season["people"]] == ["配 角色", "配 角色 2"]

    episode_row = {
        **base_row,
        "tmdb_id": "2001-S01E02",
        "title": "Episode 2",
        "season_number": 1,
        "episode_number": 2,
        "poster_path": None,
        "backdrop_path": None,
        "thumb_path": None,
        "screenshot_hash": "a" * 64,
        "emby_item_ids_json": ["episode-2"],
    }
    episode = format_metadata_payload(
        MetadataAggregate(
            identity=MediaIdentity("2001", "tv", MediaItemType.EPISODE, 1, 2),
            requested_type=MediaItemType.EPISODE,
            row=episode_row,
        ),
        MediaPresentationPolicy(),
        screenshot_token=lambda value: f"etk-cache://{value}",
    )
    assert episode["season_number"] == 1
    assert episode["episode_number"] == 2
    assert episode["images"] == {
        "primary": f"etk-cache://{'a' * 64}",
        "backdrop": f"etk-cache://{'a' * 64}",
        "logo": None,
        "thumb": f"etk-cache://{'a' * 64}",
    }
