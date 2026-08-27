from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyDiscoverProjection:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def enrich(
        self,
        payload: Mapping[str, Any],
        *,
        item_type: str,
        hide_in_library: bool,
    ) -> dict[str, Any]:
        result = dict(payload)
        raw_items = result.get("results")
        items = [dict(item) for item in raw_items if isinstance(item, Mapping)] if isinstance(raw_items, list) else []
        items = [item for item in items if item.get("poster_path")]
        identifiers = tuple(dict.fromkeys(str(item.get("id")) for item in items if item.get("id") is not None))
        local: dict[str, Mapping[str, Any]] = {}
        if identifiers:
            with self._unit_of_work() as uow:
                rows = uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, in_library, emby_item_ids_json, subscription_status
                        FROM media_metadata
                        WHERE item_type = :item_type AND tmdb_id = ANY(CAST(:tmdb_ids AS text[]))
                        """
                    ),
                    {"item_type": item_type, "tmdb_ids": list(identifiers)},
                ).mappings()
                local = {str(row["tmdb_id"]): dict(row) for row in rows}
        media_type = "movie" if item_type == "Movie" else "tv"
        enriched: list[dict[str, Any]] = []
        for item in items:
            identifier = str(item.get("id"))
            row = local.get(identifier)
            emby_ids = row.get("emby_item_ids_json") if row is not None else None
            item["in_library"] = bool(row.get("in_library")) if row is not None else False
            item["emby_item_id"] = emby_ids
            item["subscription_status"] = row.get("subscription_status") if row is not None else None
            item["media_type"] = media_type
            if not hide_in_library or not item["in_library"]:
                enriched.append(item)
        result["results"] = enriched
        result["total_results"] = len(enriched)
        if not enriched and not result.get("total_pages"):
            result["total_pages"] = 0
        return result

    def enrich_tv_seasons(self, tv_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(payload)
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT season_number, in_library, subscription_status
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = :tv_id AND item_type = 'Season'
                    """
                ),
                {"tv_id": tv_id},
            ).mappings()
            local = {int(row["season_number"]): row for row in rows if row["season_number"] is not None}
        raw_seasons = result.get("seasons")
        seasons: list[dict[str, Any]] = []
        if isinstance(raw_seasons, list):
            for value in raw_seasons:
                if not isinstance(value, Mapping):
                    continue
                season = dict(value)
                raw_season_number = season.get("season_number")
                if raw_season_number is None:
                    seasons.append(season)
                    continue
                try:
                    season_number = int(raw_season_number)
                except (TypeError, ValueError):
                    seasons.append(season)
                    continue
                row = local.get(season_number)
                season["in_library"] = bool(row.get("in_library")) if row is not None else False
                season["subscription_status"] = (
                    str(row.get("subscription_status") or "NONE") if row is not None else "NONE"
                )
                seasons.append(season)
        result["seasons"] = seasons
        return result


__all__ = ["SqlAlchemyDiscoverProjection"]
