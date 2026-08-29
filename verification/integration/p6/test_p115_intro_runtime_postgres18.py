from __future__ import annotations

import json
from collections.abc import Sequence

from sqlalchemy import text

from etk.domains.p115 import (
    IntroFavoriteIdentity,
    PostgresP115IntroDetectionRepository,
    PostgresP115IntroRuntimeSource,
)
from etk.domains.system import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime


class _FavoriteFallback:
    def __init__(self, result: IntroFavoriteIdentity | None = None) -> None:
        self.result = result
        self.calls: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

    def find_favorite(
        self,
        *,
        direct_item_ids: Sequence[str],
        season_ids: Sequence[str],
    ) -> IntroFavoriteIdentity | None:
        self.calls.append((tuple(direct_item_ids), tuple(season_ids)))
        return self.result


def _source(
    runtime: DatabaseRuntime,
    fallback: _FavoriteFallback | None = None,
) -> PostgresP115IntroRuntimeSource:
    repository = PostgresP115IntroDetectionRepository(runtime.unit_of_work)
    return PostgresP115IntroRuntimeSource(
        runtime.unit_of_work,
        repository,
        SqlAlchemyAppSettingsRepository(runtime.unit_of_work),
        fallback,
    )


def _seed_season(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    title, watching_status
                ) VALUES (
                    'series-42', 'Series', TRUE, '["series-emby"]'::jsonb,
                    'Series 42', 'Watching'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    parent_series_tmdb_id, season_number, title
                ) VALUES (
                    'series-42-S1', 'Season', TRUE, '["season-emby"]'::jsonb,
                    'series-42', 1, 'Season 1'
                )
                """
            )
        )
        for number in (1, 2):
            sha1 = f"{number:040X}"
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata(
                        tmdb_id, item_type, in_library, emby_item_ids_json,
                        file_sha1_json, file_pickcode_json, asset_details_json,
                        parent_series_tmdb_id, season_number, episode_number,
                        title, last_updated_at
                    ) VALUES (
                        :tmdb_id, 'Episode', TRUE, CAST(:item_ids AS jsonb),
                        CAST(:sha1s AS jsonb), CAST(:pick_codes AS jsonb),
                        CAST(:assets AS jsonb), 'series-42', 1, :episode_number,
                        :title, NOW() - make_interval(secs => :episode_number)
                    )
                    """
                ),
                {
                    "tmdb_id": f"series-42-S1E{number}",
                    "item_ids": json.dumps([f"episode-{number}"]),
                    "sha1s": json.dumps([sha1]),
                    "pick_codes": json.dumps([f"pick-{number}"]),
                    "assets": json.dumps([{"source_library_id": "library-a"}]),
                    "episode_number": number,
                    "title": f"Episode {number}",
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_mediainfo_cache(sha1, mediainfo_json, raw_ffprobe_json)
                    VALUES (:sha1, CAST(:mediainfo AS jsonb), '{}'::jsonb)
                    """
                ),
                {
                    "sha1": sha1,
                    "mediainfo": json.dumps(
                        {
                            "MediaSourceInfo": {"RunTimeTicks": 12_000_000_000, "MediaStreams": []},
                            "Chapters": [],
                        }
                    ),
                },
            )


def test_native_runtime_reads_frozen_policy_and_library_or_watchlist_candidates(
    p6_runtime: DatabaseRuntime,
) -> None:
    _seed_season(p6_runtime)
    settings = SqlAlchemyAppSettingsRepository(p6_runtime.unit_of_work)
    settings.save(
        "watchlist_config",
        {
            "intro_detection_trigger_mode": "import",
            "intro_detection_credits_enabled": False,
            "intro_detection_library_ids": ["library-a"],
        },
    )
    source = _source(p6_runtime)

    policy = source.snapshot()
    library = source.list_library_seasons(
        limit=50,
        library_ids=policy.library_ids,
        credits_enabled=policy.credits_enabled,
    )
    resolved = source.resolve_item(
        {
            "Id": "episode-1",
            "Type": "Episode",
            "SeriesId": "series-emby",
            "ParentIndexNumber": 1,
            "IndexNumber": 1,
        }
    )

    assert policy.trigger_mode.value == "import"
    assert len(library) == 1 and [episode.episode_number for episode in library[0]] == [1, 2]
    assert resolved == library[0]

    settings.save(
        "watchlist_config",
        {
            "intro_detection_trigger_mode": "favorite",
            "intro_detection_credits_enabled": False,
            "intro_detection_library_ids": ["library-a"],
        },
    )
    active = source.list_favorite_seasons(
        limit=50,
        library_ids=("library-a",),
        credits_enabled=False,
    )
    assert active == library


def test_exact_favorite_precedes_authoritative_fallback_and_fallback_is_projected(
    p6_runtime: DatabaseRuntime,
) -> None:
    _seed_season(p6_runtime)
    fallback = _FavoriteFallback(IntroFavoriteIdentity("user-2", "episode-2"))
    source = _source(p6_runtime, fallback)
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_favorite_items(user_id, item_id, updated_at)
                VALUES ('user-1', 'season-emby', NOW())
                """
            )
        )

    assert source.season_is_favorite("series-42", 1) is True
    assert fallback.calls == []

    with p6_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM emby_favorite_items"))
    assert source.season_is_favorite("series-42", 1) is True
    assert fallback.calls == [(("series-emby", "season-emby"), ("season-emby",))]
    with p6_runtime.engine.connect() as connection:
        projected = connection.execute(text("SELECT user_id, item_id FROM emby_favorite_items")).one()
    assert projected == ("user-2", "episode-2")
