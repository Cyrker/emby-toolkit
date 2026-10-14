from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

import httpx
from sqlalchemy import text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

from .policies import build_image_language_priority, normalize_media_path, select_image_path
from .task_execution import HttpxTmdbMetadataAdapter

IMAGE_CHECKS = {
    "_primary_image": "Primary",
    "_backdrop_image": "Backdrop",
    "_logo_image": "Logo",
    "_thumb_image": "Thumb",
}
_COLUMNS = {"Primary": "poster_path", "Backdrop": "backdrop_path", "Logo": "logo_path", "Thumb": "thumb_path"}
_TYPES = {
    "Movie": ("Primary", "Backdrop", "Logo"),
    "Series": ("Primary", "Backdrop", "Logo"),
    "Season": ("Primary",),
    "Episode": ("Primary", "Thumb"),
}


def image_present(item: Mapping[str, Any], image_type: str) -> bool:
    return (
        bool(item.get("BackdropImageTags"))
        if image_type == "Backdrop"
        else bool((item.get("ImageTags") or {}).get(image_type))
    )


class NativeReprocessImagePreparation:
    """只准备所选项的缺图来源；Emby 下载仍由现有持久刷新执行。"""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        tmdb: HttpxTmdbMetadataAdapter,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._unit_of_work = unit_of_work
        self._tmdb = tmdb
        self._client_factory = client_factory or (
            lambda: httpx.Client(timeout=float(self._settings().emby_api_timeout), trust_env=False)
        )

    def prepare(self, item: Mapping[str, Any], root: Mapping[str, Any], cache: dict[str, Any]) -> dict[str, bool]:
        kind = str(item["Type"])
        rules = self._rules(item, cache)
        allowed = [t for t in _TYPES[kind] if t in rules]
        if not allowed:
            return {}
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text("""
                SELECT tmdb_id,item_type,parent_series_tmdb_id,season_number,episode_number,
                       poster_path,backdrop_path,logo_path,thumb_path,
                       COALESCE(original_language, (
                           SELECT parent.original_language FROM media_metadata parent
                           WHERE parent.tmdb_id=media_metadata.parent_series_tmdb_id AND parent.item_type='Series'
                       )) AS original_language
                FROM media_metadata WHERE item_type=:kind AND emby_item_ids_json @> CAST(:ids AS jsonb)
            """),
                    {"kind": kind, "ids": json.dumps([str(item["Id"])])},
                )
                .mappings()
                .all()
            )
        if len(rows) != 1:
            raise ValueError("无法唯一确认补图条目的缓存身份")
        row = dict(rows[0])
        root_id = str((root.get("ProviderIds") or {}).get("Tmdb") or "")
        if str(row["tmdb_id"] if kind in {"Movie", "Series"} else row["parent_series_tmdb_id"]) != root_id:
            raise ValueError("补图条目的缓存与作品身份不一致")
        missing_sources = [t for t in allowed if not image_present(item, t) and not row.get(_COLUMNS[t])]
        if missing_sources:
            raw = self._tmdb.images(root_id, kind, season=row["season_number"], episode=row["episode_number"])
            settings = self._settings()
            priorities = build_image_language_priority(
                row.get("original_language"), settings.tmdb_image_language_preference
            )
            for image_type in missing_sources:
                source_key = (
                    "stills"
                    if kind == "Episode"
                    else {"Primary": "posters", "Backdrop": "backdrops", "Logo": "logos", "Thumb": "backdrops"}[
                        image_type
                    ]
                )
                candidates = [
                    v
                    for v in raw.get(source_key) or []
                    if isinstance(v, Mapping) and int(v.get("width") or 0) >= rules[image_type]
                ]
                path = select_image_path(candidates, priorities)
                if path:
                    row[_COLUMNS[image_type]] = path
            with self._unit_of_work() as uow:
                for image_type in missing_sources:
                    column = _COLUMNS[image_type]
                    if not row.get(column):
                        continue
                    # 仅填空缓存，保留用户或先前流程已经选择的图片。
                    uow.session.execute(
                        text(f"""
                        UPDATE media_metadata SET {column}=COALESCE(NULLIF({column},''),:path)
                        WHERE tmdb_id=:id AND item_type=:kind
                    """),
                        {"path": row[column], "id": row["tmdb_id"], "kind": kind},
                    )
                uow.commit()
        return {
            check: True
            for check, t in IMAGE_CHECKS.items()
            if t in allowed and (image_present(item, t) or row.get(_COLUMNS[t]))
        }

    def _rules(self, item: Mapping[str, Any], cache: dict[str, Any]) -> dict[str, int]:
        if "libraries" not in cache:
            settings = self._settings()
            with self._client_factory() as client:
                response = client.get(
                    settings.emby_server_url.rstrip("/") + "/Library/VirtualFolders",
                    headers={"X-Emby-Token": settings.emby_api_key.get_secret_value()},
                )
                response.raise_for_status()
                libraries = response.json()
            if not isinstance(libraries, list):
                raise ValueError("Emby 媒体库图片策略响应无效")
            cache["libraries"] = libraries
        path = normalize_media_path(item.get("Path"))
        matches = [
            (len(location), library)
            for library in cache["libraries"]
            for raw in library.get("Locations") or []
            if (location := normalize_media_path(raw)) and (path == location or path.startswith(location + "/"))
        ]
        if not matches:
            raise ValueError("无法确认补图条目所属媒体库，不改变图片策略")
        library = max(matches, key=lambda pair: pair[0])[1]
        options = next(
            (
                v
                for v in (library.get("LibraryOptions") or {}).get("TypeOptions") or []
                if v.get("Type") == item["Type"]
            ),
            {},
        )
        if "ImageFetchers" in options and not options["ImageFetchers"]:
            return {}
        rules = {t: 0 for t in _TYPES[str(item["Type"])]}
        for option in options.get("ImageOptions") or []:
            image_type = option.get("Type")
            if image_type not in rules:
                continue
            if "Limit" in option and int(option["Limit"]) <= 0:
                rules.pop(image_type)
            else:
                rules[image_type] = max(0, int(option.get("MinWidth") or 0))
        return rules
