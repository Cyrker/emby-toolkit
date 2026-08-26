from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.domains.subscription.resubscribe import ResubscribeItemKey
from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyResubscribeMediaProjection:
    """P5-owned read/command projection for P7 resubscribe use cases."""

    def describe_items(
        self,
        uow: SqlAlchemyUnitOfWork,
        keys: Sequence[ResubscribeItemKey],
    ) -> Mapping[ResubscribeItemKey, Mapping[str, Any]]:
        descriptions: dict[ResubscribeItemKey, Mapping[str, Any]] = {}
        for key in dict.fromkeys(keys):
            row = self._movie(uow, key) if key.item_type == "Movie" else self._season(uow, key)
            descriptions[key] = _description(row)
        return descriptions

    def set_active_washing(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: ResubscribeItemKey,
        enabled: bool,
    ) -> int:
        if key.item_type == "Movie":
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET active_washing = :enabled
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": key.tmdb_id, "enabled": enabled},
            )
        elif key.item_type == "Season":
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET active_washing = :enabled
                    WHERE parent_series_tmdb_id = :tmdb_id
                      AND season_number = :season_number
                      AND item_type = 'Season'
                    """
                ),
                {
                    "tmdb_id": key.tmdb_id,
                    "season_number": key.season_number,
                    "enabled": enabled,
                },
            )
        else:
            return 0
        return int(getattr(result, "rowcount", 0) or 0)

    @staticmethod
    def _movie(uow: SqlAlchemyUnitOfWork, key: ResubscribeItemKey) -> Mapping[str, Any] | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT
                        title AS item_name,
                        poster_path,
                        emby_item_ids_json ->> 0 AS emby_item_id,
                        NULL::text AS series_emby_id,
                        '{}'::jsonb AS missing_info,
                        asset_details_json -> 0 AS asset_details
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": key.tmdb_id},
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    @staticmethod
    def _season(uow: SqlAlchemyUnitOfWork, key: ResubscribeItemKey) -> Mapping[str, Any] | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT
                        COALESCE(parent.title || ' - 第 ' || season.season_number || ' 季', season.title)
                            AS item_name,
                        COALESCE(season.poster_path, parent.poster_path) AS poster_path,
                        season.emby_item_ids_json ->> 0 AS emby_item_id,
                        parent.emby_item_ids_json ->> 0 AS series_emby_id,
                        COALESCE(season.watchlist_missing_info_json, '{}'::jsonb) AS missing_info,
                        (
                            SELECT episode.asset_details_json -> 0
                            FROM media_metadata AS episode
                            WHERE episode.parent_series_tmdb_id = season.parent_series_tmdb_id
                              AND episode.season_number = season.season_number
                              AND episode.item_type = 'Episode'
                            ORDER BY episode.episode_number ASC, episode.tmdb_id ASC
                            LIMIT 1
                        ) AS asset_details
                    FROM media_metadata AS season
                    LEFT JOIN media_metadata AS parent
                      ON season.parent_series_tmdb_id = parent.tmdb_id
                     AND parent.item_type = 'Series'
                    WHERE season.parent_series_tmdb_id = :tmdb_id
                      AND season.season_number = :season_number
                      AND season.item_type = 'Season'
                    ORDER BY season.tmdb_id ASC
                    LIMIT 1
                    """
                ),
                {"tmdb_id": key.tmdb_id, "season_number": key.season_number},
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None


def _description(row: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(row or {})
    asset: Mapping[str, Any] = {}
    raw_asset = raw.get("asset_details")
    if isinstance(raw_asset, Mapping):
        asset = dict(raw_asset)
    missing_info: Mapping[str, Any] = {}
    raw_missing_info = raw.get("missing_info")
    if isinstance(raw_missing_info, Mapping):
        missing_info = dict(raw_missing_info)
    missing_episodes = missing_info.get("missing_episodes")
    if not isinstance(missing_episodes, list):
        missing_episodes = []
    path = str(asset.get("path") or "")
    return {
        "item_name": raw.get("item_name") or "未知项目",
        "poster_path": raw.get("poster_path"),
        "emby_item_id": raw.get("emby_item_id"),
        "series_emby_id": raw.get("series_emby_id"),
        "missing_episodes": missing_episodes,
        "resolution_display": asset.get("resolution_display", "Unknown"),
        "quality_display": asset.get("quality_display", "Unknown"),
        "release_group_raw": asset.get("release_group_raw", "无"),
        "codec_display": asset.get("codec_display", "unknown"),
        "effect_display": asset.get("effect_display", ["SDR"]),
        "audio_display": asset.get("audio_display", "无"),
        "subtitle_display": asset.get("subtitle_display", "无"),
        "filename": os.path.basename(path) if path else None,
    }


__all__ = ["SqlAlchemyResubscribeMediaProjection"]
