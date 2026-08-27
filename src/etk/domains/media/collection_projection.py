from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

from sqlalchemy import bindparam, text

from etk.platform.db import SqlAlchemyUnitOfWork

logger = logging.getLogger(__name__)


class SqlAlchemyCollectionMediaProjection:
    """Media-owned read projection used by P8 collection contracts."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def custom_status(self, raw_collection: Mapping[str, Any]) -> dict[str, Any]:
        collection = dict(raw_collection)
        if collection.get("type") == "filter":
            collection.update(media_items=[], missing_count=0, health_status="dynamic")
            return collection
        definitions = _json_list(collection.get("generated_media_info_json"))
        if not definitions:
            collection["media_items"] = []
            return collection

        normal_ids = tuple(
            dict.fromkeys(
                str(item.get("tmdb_id"))
                for item in definitions
                if item.get("tmdb_id") and not (item.get("media_type") == "Series" and item.get("season") is not None)
            )
        )
        media: dict[str, dict[str, Any]] = {}
        try:
            with self._unit_of_work() as uow:
                if normal_ids:
                    rows = uow.session.execute(
                        text("SELECT * FROM media_metadata WHERE tmdb_id IN :tmdb_ids").bindparams(
                            bindparam("tmdb_ids", expanding=True)
                        ),
                        {"tmdb_ids": normal_ids},
                    ).mappings()
                    for row in rows:
                        media[f"{row['tmdb_id']}_{row['item_type']}"] = dict(row)
                for parent_id, season_number in dict.fromkeys(
                    (str(item.get("tmdb_id")), int(item["season"]))
                    for item in definitions
                    if item.get("tmdb_id") and item.get("media_type") == "Series" and item.get("season") is not None
                ):
                    row = (
                        uow.session.execute(
                            text(
                                """
                                SELECT * FROM media_metadata
                                WHERE item_type = 'Season'
                                  AND parent_series_tmdb_id = :parent_id
                                  AND season_number = :season_number
                                """
                            ),
                            {"parent_id": parent_id, "season_number": season_number},
                        )
                        .mappings()
                        .first()
                    )
                    if row is not None:
                        media[f"{parent_id}_S_{season_number}"] = dict(row)
        except Exception:
            logger.exception("Unable to project custom collection media status")
            media.clear()

        items: list[dict[str, Any]] = []
        missing = 0
        for definition in definitions:
            tmdb_id = str(definition.get("tmdb_id") or "").strip()
            if not tmdb_id:
                continue
            media_type = str(definition.get("media_type") or "Movie")
            season_number = definition.get("season")
            key = (
                f"{tmdb_id}_S_{season_number}"
                if media_type == "Series" and season_number is not None
                else f"{tmdb_id}_{media_type}"
            )
            record = media.get(key)
            if record is None:
                missing += 1
                items.append(
                    {
                        "tmdb_id": tmdb_id,
                        "emby_id": None,
                        "title": definition.get("title"),
                        "release_date": definition.get("release_date", ""),
                        "poster_path": definition.get("poster_path"),
                        "status": "missing",
                        "media_type": media_type,
                        "season": season_number,
                    }
                )
                continue
            status = _media_status(record)
            if status not in {"in_library", "ignored"}:
                missing += 1
            items.append(
                {
                    "tmdb_id": tmdb_id,
                    "emby_id": None,
                    "title": definition.get("title"),
                    "original_title": record.get("original_title"),
                    "release_date": _date_text(record.get("release_date")),
                    "poster_path": record.get("poster_path"),
                    "status": status,
                    "media_type": media_type,
                    "season": season_number,
                }
            )
        collection.pop("generated_media_info_json", None)
        collection.update(
            media_items=items,
            missing_count=missing,
            health_status="has_missing" if missing else "ok",
        )
        return collection

    def collection_status(
        self,
        collections: Sequence[Mapping[str, Any]],
        *,
        today: date | None = None,
    ) -> list[dict[str, Any]]:
        all_ids = tuple(
            dict.fromkeys(
                value for collection in collections for value in _json_strings(collection.get("all_tmdb_ids_json"))
            )
        )
        media: dict[str, dict[str, Any]] = {}
        if all_ids:
            with self._unit_of_work() as uow:
                rows = uow.session.execute(
                    text("SELECT * FROM media_metadata WHERE tmdb_id IN :tmdb_ids").bindparams(
                        bindparam("tmdb_ids", expanding=True)
                    ),
                    {"tmdb_ids": all_ids},
                ).mappings()
                media = {str(row["tmdb_id"]): dict(row) for row in rows}
        current_day = (today or datetime.now().date()).isoformat()
        projected: list[dict[str, Any]] = []
        for raw_collection in collections:
            collection = dict(raw_collection)
            statistics = {"missing": 0, "in_library": 0, "subscribed": 0, "unreleased": 0}
            movies: list[dict[str, Any]] = []
            for tmdb_id in _json_strings(collection.get("all_tmdb_ids_json")):
                item = media.get(tmdb_id)
                if item is None:
                    continue
                release_date = str(item.get("release_date")) if item.get("release_date") else None
                emby_id = None
                if item.get("in_library"):
                    ids = _json_strings(item.get("emby_item_ids_json"))
                    emby_id = ids[0] if ids else None
                    status = "in_library"
                    statistics["in_library"] += 1
                elif item.get("subscription_status") in {"SUBSCRIBED", "PAUSED"}:
                    status = "subscribed" if item.get("subscription_status") == "SUBSCRIBED" else "paused"
                    statistics["subscribed"] += 1
                elif release_date and release_date > current_day:
                    status = "unreleased"
                    statistics["unreleased"] += 1
                else:
                    status = "missing"
                    statistics["missing"] += 1
                movies.append(
                    {
                        "tmdb_id": tmdb_id,
                        "emby_id": emby_id,
                        "title": item.get("title"),
                        "poster_path": item.get("poster_path"),
                        "release_date": release_date,
                        "status": status,
                    }
                )
            collection.pop("all_tmdb_ids_json", None)
            collection["statistics"] = statistics
            collection["movies"] = sorted(movies, key=lambda item: item.get("release_date") or "9999")
            projected.append(collection)
        return projected

    def local_filter_values(self, kind: str, query: str = "") -> list[str]:
        if kind == "tags":
            columns = ("tags_json",)
            predicate = "in_library IS TRUE"
        elif kind == "movie_genres":
            columns = ("genres_json",)
            predicate = "item_type = 'Movie' AND in_library IS TRUE"
        elif kind == "tv_genres":
            columns = ("genres_json",)
            predicate = "item_type = 'Series' AND in_library IS TRUE"
        elif kind == "studios":
            columns = ("production_companies_json", "networks_json")
            predicate = "in_library IS TRUE"
        else:
            raise LookupError(kind)
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(f"SELECT {', '.join(columns)} FROM media_metadata WHERE {predicate}")
            ).mappings()
            values: set[str] = set()
            for row in rows:
                for column in columns:
                    for item in _json_sequence(row.get(column)):
                        raw = item.get("name") if isinstance(item, Mapping) else item
                        normalized = str(raw or "").strip()
                        if normalized:
                            values.add(normalized)
        ordered = sorted(values)
        normalized_query = str(query or "").casefold()
        if kind != "studios" or not normalized_query:
            return ordered
        starts = [value for value in ordered if value.casefold().startswith(normalized_query)]
        contains = [
            value
            for value in ordered
            if normalized_query in value.casefold() and not value.casefold().startswith(normalized_query)
        ]
        return (starts + contains)[:20]


def _json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_sequence(value: Any) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return list(value) if isinstance(value, list) else []


def _json_strings(value: Any) -> list[str]:
    return [str(item).strip() for item in _json_sequence(value) if str(item or "").strip()]


def _media_status(record: Mapping[str, Any]) -> str:
    if record.get("in_library"):
        return "in_library"
    return {
        "SUBSCRIBED": "subscribed",
        "PAUSED": "paused",
        "IGNORED": "ignored",
        "PENDING_RELEASE": "unreleased",
    }.get(str(record.get("subscription_status") or ""), "missing")


def _date_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value) if value else ""


__all__ = ["SqlAlchemyCollectionMediaProjection"]
