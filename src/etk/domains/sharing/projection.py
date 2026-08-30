from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import PurePosixPath
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

_VIDEO_SUFFIXES = {
    ".3gp",
    ".asf",
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".mts",
    ".rm",
    ".rmvb",
    ".ts",
    ".vob",
    ".webm",
    ".wmv",
}


class SqlAlchemySharingProjectionRepository:
    """Read-only projections over the existing P9 local sharing tables."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        account_id: Callable[[], object] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._account_id = account_id or (lambda: "")

    def summary(self) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            shares = (
                uow.session.execute(
                    text(
                        """
                        SELECT
                            COUNT(*) AS total,
                            COUNT(*) FILTER (WHERE status IN ('active','available')) AS alive,
                            COUNT(*) FILTER (WHERE status IN ('updating','pending')) AS pending,
                            COUNT(*) FILTER (WHERE center_status = 'reported') AS reported,
                            COUNT(*) FILTER (WHERE status IN ('inconsistent','incomplete','error')) AS failed,
                            COUNT(*) FILTER (WHERE source_kind = 'movie') AS movies,
                            COUNT(*) FILTER (WHERE source_kind = 'episode') AS episodes,
                            COUNT(*) FILTER (
                                WHERE status IN ('active','available') AND source_kind = 'movie'
                            ) AS alive_movies,
                            COUNT(*) FILTER (
                                WHERE status IN ('active','available') AND source_kind = 'episode'
                            ) AS alive_episodes,
                            COUNT(DISTINCT COALESCE(NULLIF(parent_series_tmdb_id, ''), NULLIF(tmdb_id, '')))
                                FILTER (
                                    WHERE status IN ('active','available')
                                      AND source_kind IN ('episode','logical_season')
                                      AND COALESCE(
                                          NULLIF(parent_series_tmdb_id, ''), NULLIF(tmdb_id, '')
                                      ) IS NOT NULL
                                ) AS alive_series_count,
                            COUNT(DISTINCT CONCAT(
                                COALESCE(NULLIF(parent_series_tmdb_id, ''), NULLIF(tmdb_id, '')),
                                ':', season_number::text
                            )) FILTER (
                                WHERE status IN ('active','available')
                                  AND source_kind IN ('episode','logical_season')
                                  AND COALESCE(
                                      NULLIF(parent_series_tmdb_id, ''), NULLIF(tmdb_id, '')
                                  ) IS NOT NULL
                                  AND season_number IS NOT NULL
                            ) AS alive_season_count
                        FROM shared_rapid_sources
                        """
                    )
                )
                .mappings()
                .one()
            )
            credit = (
                uow.session.execute(text("SELECT * FROM shared_credit_snapshot WHERE id = 1")).mappings().one_or_none()
            )
        normalized_shares = _row(shares)
        normalized_shares["alive_videos"] = _integer(normalized_shares.get("alive_movies")) + _integer(
            normalized_shares.get("alive_episodes")
        )
        return {"shares": normalized_shares, "credit": _row(credit)}

    def list_local_sources(
        self,
        *,
        status: str = "usable",
        keyword: str = "",
        page: int = 1,
        page_size: int = 30,
        raw_limit: int = 200_000,
    ) -> dict[str, Any]:
        bounded_page = max(1, int(page))
        bounded_page_size = max(1, min(int(page_size), 200))
        bounded_raw_limit = max(1_000, min(int(raw_limit), 200_000))
        normalized_keyword = str(keyword or "").strip()
        where = ""
        params: dict[str, Any] = {"limit": bounded_raw_limit}
        if normalized_keyword:
            where = """
                WHERE title ILIKE :keyword OR file_name ILIKE :keyword
                   OR tmdb_id ILIKE :keyword OR parent_series_tmdb_id ILIKE :keyword
                   OR sha1 ILIKE :keyword OR center_source_id ILIKE :keyword
                   OR source_key ILIKE :keyword
            """
            params["keyword"] = f"%{normalized_keyword}%"
        with self._unit_of_work() as uow:
            raw_total = int(uow.session.scalar(text(f"SELECT COUNT(*) FROM shared_rapid_sources {where}"), params) or 0)
            light_rows = [
                _row(value)
                for value in (
                    uow.session.execute(
                        text(
                            f"""
                            SELECT id, source_key, source_kind, item_type, tmdb_id,
                                   parent_series_tmdb_id, season_number, episode_number,
                                   status, center_status, center_source_id, created_at, updated_at,
                                   file_count, total_size, title, file_name, source_provider,
                                   is_clean_version, last_error,
                                   COALESCE(
                                       raw_json->'center_response'->'item'->>'hub_id',
                                       raw_json->'event'->>'hub_id',
                                       raw_json->'payload'->>'hub_id',
                                       raw_json->>'hub_id'
                                   ) AS hub_id
                            FROM shared_rapid_sources
                            {where}
                            ORDER BY updated_at DESC NULLS LAST, id DESC
                            LIMIT :limit
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
            ]
            aggregated = _aggregate_local_sources(light_rows)
            _attach_completed_share_channels(uow, aggregated)
            filtered = [value for value in aggregated if _matches_source_status(value, status)]
            start = (bounded_page - 1) * bounded_page_size
            page_items = filtered[start : start + bounded_page_size]
            needed_ids: set[int] = set()
            for item in page_items:
                for source_id in item.get("source_ids") or [item.get("id")]:
                    normalized_id = _positive_integer(source_id)
                    if normalized_id is not None:
                        needed_ids.add(normalized_id)
            raw_by_id: dict[int, Mapping[str, Any]] = {}
            if needed_ids:
                raw_rows = (
                    uow.session.execute(
                        text(
                            """
                            SELECT id, raw_json
                            FROM shared_rapid_sources
                            WHERE id = ANY(CAST(:ids AS integer[]))
                            """
                        ),
                        {"ids": sorted(needed_ids)},
                    )
                    .mappings()
                    .all()
                )
                raw_by_id = {
                    int(value["id"]): dict(value.get("raw_json") or {})
                    for value in raw_rows
                    if isinstance(value.get("raw_json"), Mapping)
                }
        items: list[dict[str, Any]] = []
        for item in page_items:
            source_ids = item.get("source_ids")
            representative = source_ids[0] if isinstance(source_ids, list) and source_ids else item.get("id")
            normalized = dict(item)
            normalized["raw_json"] = raw_by_id.get(_integer(representative), {})
            items.append(_decorate_local_source(normalized))
        return {
            "items": items,
            "total": len(filtered),
            "raw_total": raw_total,
            "scanned_raw": len(light_rows),
            "page": bounded_page,
            "page_size": bounded_page_size,
        }

    def get_local_source(self, source_id: int) -> dict[str, Any] | None:
        with self._unit_of_work() as uow:
            value = (
                uow.session.execute(
                    text("SELECT * FROM shared_rapid_sources WHERE id = :id"),
                    {"id": int(source_id)},
                )
                .mappings()
                .one_or_none()
            )
        return _decorate_local_source(_row(value)) if value is not None else None

    def list_virtual_imports(
        self,
        *,
        status: str = "virtual",
        keyword: str = "",
        item_type: str = "all",
        page: int = 1,
        page_size: int = 30,
    ) -> dict[str, Any]:
        clauses: list[str] = []
        params: dict[str, Any] = {}
        normalized_status = str(status or "").strip().lower()
        normalized_item_type = str(item_type or "").strip().lower()
        normalized_keyword = str(keyword or "").strip()
        if normalized_status and normalized_status != "all":
            clauses.append("LOWER(status) = :status")
            params["status"] = normalized_status
        if normalized_item_type and normalized_item_type != "all":
            if normalized_item_type in {"movie", "film"}:
                clauses.append("LOWER(item_type) = 'movie'")
            elif normalized_item_type in {"tv", "series", "season", "episode"}:
                clauses.append("LOWER(item_type) IN ('series','season','episode','tv')")
        if normalized_keyword:
            clauses.append("(title ILIKE :keyword OR tmdb_id ILIKE :keyword OR source_id ILIKE :keyword)")
            params["keyword"] = f"%{normalized_keyword}%"
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        bounded_page = max(1, int(page))
        bounded_page_size = max(1, min(int(page_size), 200))
        params.update(limit=bounded_page_size, offset=(bounded_page - 1) * bounded_page_size)
        with self._unit_of_work() as uow:
            total = int(uow.session.scalar(text(f"SELECT COUNT(*) FROM shared_virtual_imports {where}"), params) or 0)
            rows = (
                uow.session.execute(
                    text(
                        f"""
                        SELECT * FROM shared_virtual_imports
                        {where}
                        ORDER BY updated_at DESC NULLS LAST, id DESC
                        LIMIT :limit OFFSET :offset
                        """
                    ),
                    params,
                )
                .mappings()
                .all()
            )
        return {
            "items": [_row(value) for value in rows],
            "total": total,
            "page": bounded_page,
            "page_size": bounded_page_size,
        }

    def get_virtual_import(self, virtual_id: int) -> dict[str, Any] | None:
        with self._unit_of_work() as uow:
            value = (
                uow.session.execute(
                    text("SELECT * FROM shared_virtual_imports WHERE id = :id"),
                    {"id": int(virtual_id)},
                )
                .mappings()
                .one_or_none()
            )
        return _row(value) if value is not None else None

    def search_shareable_media(self, keyword: str, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 500))
        with self._unit_of_work() as uow:
            rows = self._media_rows(uow, str(keyword or "").strip(), min(max(bounded * 6, 200), 2_000))
            result: list[dict[str, Any]] = []
            seen: set[tuple[Any, ...]] = set()
            for row in rows:
                item_type = str(row.get("item_type") or "")
                candidates: list[dict[str, Any]] = []
                if item_type == "Series":
                    parent_id = str(row.get("tmdb_id") or "").strip()
                    seasons = sorted(
                        {
                            _integer(value.get("season_number"))
                            for value in self._episode_rows(uow, parent_id, None)
                            if value.get("season_number") is not None
                        }
                    )
                    for season_number in seasons:
                        season_row = self._season_row(uow, parent_id, season_number)
                        candidates.append(
                            self._candidate(
                                uow,
                                {
                                    **season_row,
                                    "item_type": "Season",
                                    "season_number": season_number,
                                    "parent_series_tmdb_id": parent_id,
                                    "tmdb_id": season_row.get("tmdb_id") or parent_id,
                                    "series_title": row.get("title") or row.get("original_title"),
                                    "series_original_title": row.get("original_title"),
                                    "series_release_year": row.get("release_year"),
                                },
                            )
                        )
                else:
                    candidates.append(self._candidate(uow, row))
                for candidate in candidates:
                    if not candidate.get("resolvable"):
                        continue
                    key = (
                        candidate.get("share_tmdb_id"),
                        candidate.get("share_item_type"),
                        candidate.get("season_number"),
                        candidate.get("episode_number"),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    result.append(candidate)
                    if len(result) >= bounded:
                        return result
            return result

    def validate_manual_source(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        data = _normalize_candidate_identity(payload)
        with self._unit_of_work() as uow:
            files = self._files_for_identity(uow, data)
            missing_raw: list[dict[str, Any]] = []
            for item in files:
                sha1 = str(item.get("sha1") or "").strip().upper()
                if sha1 and not self._raw_available(uow, sha1):
                    missing_raw.append(
                        {
                            "sha1": sha1,
                            "file_name": item.get("file_name") or "",
                            "reason": "RAW 或 summary_json 缺失",
                        }
                    )
            root = self._candidate_root(uow, files)
        if not files:
            message = "没有找到可登记视频文件"
            valid = False
        elif missing_raw:
            message = f"找到 {len(files)} 个视频文件，但有 {len(missing_raw)} 个缺少 RAW 媒体信息，暂不登记中心"
            valid = False
        else:
            message = f"找到 {len(files)} 个可登记视频文件，可登记为 Rapid v2 逻辑季资产"
            valid = True
        return {
            "valid": valid,
            "message": message,
            "file_count": len(files),
            "missing_raw": missing_raw,
            "files": files,
            "root": root,
            "root_fid": root.get("root_fid") or "",
            "reason": "raw_missing" if missing_raw else "",
            "center_managed_logical_season": True,
        }

    @staticmethod
    def _media_rows(uow: SqlAlchemyUnitOfWork, keyword: str, limit: int) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "item_types": ["Movie", "Series", "Season", "Episode"],
            "limit": max(1, min(int(limit), 2_000)),
        }
        keyword_clause = ""
        if keyword:
            params["keyword"] = f"%{keyword}%"
            keyword_clause = """
                AND (
                    m.title ILIKE :keyword OR m.original_title ILIKE :keyword
                    OR m.tmdb_id ILIKE :keyword OR m.parent_series_tmdb_id ILIKE :keyword
                    OR parent.title ILIKE :keyword OR parent.original_title ILIKE :keyword
                )
            """
        rows = (
            uow.session.execute(
                text(
                    f"""
                    SELECT m.*,
                           parent.title AS series_title,
                           parent.original_title AS series_original_title,
                           parent.release_year AS series_release_year,
                           season.watching_status AS season_watching_status,
                           season.total_episodes AS season_total_episodes
                    FROM media_metadata AS m
                    LEFT JOIN media_metadata AS parent
                      ON parent.item_type = 'Series'
                     AND parent.tmdb_id = COALESCE(
                         NULLIF(m.parent_series_tmdb_id, ''),
                         CASE WHEN m.item_type = 'Series' THEN m.tmdb_id ELSE NULL END
                     )
                    LEFT JOIN media_metadata AS season
                      ON season.item_type = 'Season'
                     AND season.season_number = m.season_number
                     AND COALESCE(NULLIF(season.parent_series_tmdb_id, ''), season.tmdb_id)
                         = COALESCE(
                             NULLIF(m.parent_series_tmdb_id, ''),
                             CASE WHEN m.item_type = 'Season' THEN m.tmdb_id ELSE NULL END
                         )
                    WHERE m.item_type = ANY(CAST(:item_types AS text[]))
                      {keyword_clause}
                      AND (
                          COALESCE(m.in_library, FALSE) = TRUE
                          OR (
                              m.item_type = 'Series' AND EXISTS (
                                  SELECT 1 FROM media_metadata AS episode
                                  WHERE episode.item_type = 'Episode'
                                    AND COALESCE(episode.in_library, FALSE) = TRUE
                                    AND episode.parent_series_tmdb_id = m.tmdb_id
                              )
                          )
                          OR (
                              m.item_type = 'Season' AND EXISTS (
                                  SELECT 1 FROM media_metadata AS episode
                                  WHERE episode.item_type = 'Episode'
                                    AND COALESCE(episode.in_library, FALSE) = TRUE
                                    AND episode.parent_series_tmdb_id = COALESCE(
                                        NULLIF(m.parent_series_tmdb_id, ''), m.tmdb_id
                                    )
                                    AND (m.season_number IS NULL OR episode.season_number = m.season_number)
                              )
                          )
                      )
                    ORDER BY CASE m.item_type
                        WHEN 'Movie' THEN 0 WHEN 'Series' THEN 1 WHEN 'Season' THEN 2 ELSE 3 END,
                        COALESCE(m.date_added, m.created_at, m.last_updated_at) DESC NULLS LAST
                    LIMIT :limit
                    """
                ),
                params,
            )
            .mappings()
            .all()
        )
        return [_row(value) for value in rows]

    @staticmethod
    def _episode_rows(
        uow: SqlAlchemyUnitOfWork,
        parent_tmdb_id: str,
        season_number: int | None,
    ) -> list[dict[str, Any]]:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT * FROM media_metadata
                    WHERE item_type = 'Episode' AND COALESCE(in_library, FALSE) = TRUE
                      AND parent_series_tmdb_id = :parent_tmdb_id
                      AND (:season_number IS NULL OR season_number = :season_number)
                    ORDER BY season_number ASC, episode_number ASC
                    """
                ),
                {"parent_tmdb_id": parent_tmdb_id, "season_number": season_number},
            )
            .mappings()
            .all()
        )
        return [_row(value) for value in rows]

    @staticmethod
    def _season_row(uow: SqlAlchemyUnitOfWork, parent_tmdb_id: str, season_number: int) -> dict[str, Any]:
        value = (
            uow.session.execute(
                text(
                    """
                    SELECT * FROM media_metadata
                    WHERE item_type = 'Season' AND season_number = :season_number
                      AND COALESCE(NULLIF(parent_series_tmdb_id, ''), tmdb_id) = :parent_tmdb_id
                    ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """
                ),
                {"parent_tmdb_id": parent_tmdb_id, "season_number": season_number},
            )
            .mappings()
            .one_or_none()
        )
        return _row(value)

    def _candidate(self, uow: SqlAlchemyUnitOfWork, row: Mapping[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item_type = str(item.get("item_type") or "")
        season_number = _optional_integer(item.get("season_number"))
        episode_number = _optional_integer(item.get("episode_number"))
        tmdb_id = str(
            item.get("parent_series_tmdb_id") or item.get("tmdb_id")
            if item_type in {"Season", "Episode"}
            else item.get("tmdb_id") or ""
        ).strip()
        if item_type == "Season":
            files = [
                file
                for episode in self._episode_rows(uow, tmdb_id, season_number)
                for file in self._media_files(uow, episode)
            ]
            share_type = "season_pack"
        else:
            files = self._media_files(uow, item)
            share_type = {
                "Movie": "movie_file" if len(files) <= 1 else "movie_folder",
                "Episode": "episode_file",
            }.get(item_type, "series_pack")
        root = self._candidate_root(uow, files)
        base_title = str(
            item.get("series_title")
            if item_type in {"Season", "Episode"} and item.get("series_title")
            else item.get("title") or item.get("original_title") or tmdb_id
        )
        title = base_title
        if item_type == "Season" and season_number is not None:
            title = f"{base_title} S{season_number:02d}"
        elif item_type == "Episode" and season_number is not None and episode_number is not None:
            title = f"{base_title} S{season_number:02d}E{episode_number:02d}"
        resolvable = bool(files)
        raw_json = {
            key: _json_value(item.get(key))
            for key in (
                "tmdb_id",
                "item_type",
                "parent_series_tmdb_id",
                "season_number",
                "episode_number",
                "title",
                "original_title",
                "release_year",
                "watching_status",
                "total_episodes",
                "in_library",
            )
            if item.get(key) not in (None, "", [], {})
        }
        return {
            "tmdb_id": tmdb_id,
            "share_tmdb_id": tmdb_id,
            "item_type": item_type,
            "share_item_type": item_type,
            "parent_series_tmdb_id": item.get("parent_series_tmdb_id")
            or (tmdb_id if item_type in {"Season", "Episode"} else ""),
            "season_number": season_number,
            "episode_number": episode_number,
            "title": title,
            "standard_title": title,
            "display_title": title,
            "release_year": item.get("release_year") or item.get("series_release_year"),
            "official_rating_json": item.get("official_rating_json") or item.get("series_official_rating_json"),
            "custom_rating": item.get("custom_rating") or item.get("series_custom_rating"),
            "watching_status": item.get("watching_status") or item.get("season_watching_status") or "",
            "season_status": item.get("watching_status") or item.get("season_watching_status") or "",
            "total_episodes": _optional_integer(item.get("total_episodes") or item.get("season_total_episodes")),
            "expected_episode_count": _optional_integer(
                item.get("total_episodes") or item.get("season_total_episodes")
            ),
            "in_library": bool(resolvable or item.get("in_library")),
            "source_in_library": bool(item.get("in_library")),
            "share_type": share_type,
            "root_fid": root.get("root_fid") or "",
            "root_name": root.get("root_name") or title,
            "root_is_dir": root.get("root_is_dir") is not False,
            "file_count": len(files),
            "resolvable": resolvable,
            "message": (
                f"已定位 {len(files)} 个可登记视频文件"
                if resolvable
                else "未定位到已入库视频文件；需要媒体身份和 115 文件缓存"
            ),
            "consistency": {},
            "source_provider": "manual_rapid",
            "raw_json": raw_json,
        }

    def _files_for_identity(
        self,
        uow: SqlAlchemyUnitOfWork,
        payload: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        item_type = str(payload.get("item_type") or payload.get("share_item_type") or "").strip().title()
        tmdb_id = str(
            payload.get("parent_series_tmdb_id") or payload.get("tmdb_id")
            if item_type in {"Season", "Episode"}
            else payload.get("tmdb_id") or ""
        ).strip()
        season_number = _optional_integer(payload.get("season_number"))
        episode_number = _optional_integer(payload.get("episode_number"))
        rows: list[dict[str, Any]] = []
        if item_type == "Movie":
            value = (
                uow.session.execute(
                    text("SELECT * FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Movie' LIMIT 1"),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            if value is not None:
                rows = [_row(value)]
        elif item_type == "Season":
            rows = self._episode_rows(uow, tmdb_id, season_number)
        elif item_type == "Episode":
            values = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM media_metadata
                        WHERE item_type = 'Episode' AND COALESCE(in_library, FALSE) = TRUE
                          AND (tmdb_id = :episode_tmdb_id OR parent_series_tmdb_id = :parent_tmdb_id)
                          AND (:season_number IS NULL OR season_number = :season_number)
                          AND (:episode_number IS NULL OR episode_number = :episode_number)
                        ORDER BY date_added DESC NULLS LAST
                        LIMIT 5
                        """
                    ),
                    {
                        "episode_tmdb_id": str(payload.get("tmdb_id") or ""),
                        "parent_tmdb_id": tmdb_id,
                        "season_number": season_number,
                        "episode_number": episode_number,
                    },
                )
                .mappings()
                .all()
            )
            rows = [_row(value) for value in values]
        files = [file for row in rows for file in self._media_files(uow, row)]
        deduplicated: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for file in files:
            key = (file.get("fid"), file.get("sha1"), file.get("episode_number"))
            if key not in seen:
                seen.add(key)
                deduplicated.append(file)
        return deduplicated

    def _media_files(self, uow: SqlAlchemyUnitOfWork, row: Mapping[str, Any]) -> list[dict[str, Any]]:
        account_id = str(self._account_id() or "").strip()
        if not account_id.isdigit():
            return []
        sha1_slots = [_sha1(value) for value in _slot_array(row.get("file_sha1_json"))]
        pick_code_slots = [str(value).strip() for value in _slot_array(row.get("file_pickcode_json"))]
        sha1s = list(dict.fromkeys(value for value in sha1_slots if value))
        pick_codes = list(dict.fromkeys(value for value in pick_code_slots if value))
        pick_code_sha1s: dict[str, str] = {}
        for index, pick_code in enumerate(pick_code_slots):
            if pick_code and index < len(sha1_slots) and sha1_slots[index]:
                pick_code_sha1s.setdefault(pick_code, sha1_slots[index])
        if not sha1s and not pick_codes:
            return []
        values = (
            uow.session.execute(
                text(
                    """
                    SELECT id, parent_id, pick_code, sha1, size, name, local_path
                    FROM p115_filesystem_cache
                    WHERE account_id = :account_id
                      AND (
                          pick_code = ANY(CAST(:pick_codes AS text[]))
                          OR UPPER(sha1) = ANY(CAST(:sha1s AS text[]))
                      )
                    ORDER BY updated_at DESC NULLS LAST
                    """
                ),
                {"account_id": account_id, "pick_codes": pick_codes, "sha1s": sha1s},
            )
            .mappings()
            .all()
        )
        result: list[dict[str, Any]] = []
        seen_fids: set[str] = set()
        sole_fallback_sha1 = sha1_slots[0] if len(sha1_slots) == 1 and len(pick_code_slots) <= 1 else ""
        for value in values:
            name = str(value.get("name") or "").strip()
            if PurePosixPath(name.lower()).suffix not in _VIDEO_SUFFIXES:
                continue
            fid = str(value.get("id") or "").strip()
            if not fid or fid in seen_fids:
                continue
            pick_code = str(value.get("pick_code") or "").strip()
            sha1 = _sha1(value.get("sha1")) or pick_code_sha1s.get(pick_code, "") or sole_fallback_sha1
            if not sha1:
                continue
            seen_fids.add(fid)
            result.append(
                {
                    "fid": fid,
                    "parent_id": str(value.get("parent_id") or ""),
                    "pick_code": pick_code,
                    "sha1": sha1,
                    "preid": "",
                    "size": _integer(value.get("size")),
                    "file_name": name,
                    "relative_path": str(value.get("local_path") or name),
                    "tmdb_id": row.get("parent_series_tmdb_id")
                    if row.get("item_type") == "Episode"
                    else row.get("tmdb_id"),
                    "item_type": row.get("item_type"),
                    "season_number": _optional_integer(row.get("season_number")),
                    "episode_number": _optional_integer(row.get("episode_number")),
                }
            )
        return result

    def _candidate_root(self, uow: SqlAlchemyUnitOfWork, files: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        values = [dict(value) for value in files if isinstance(value, Mapping)]
        if not values:
            return {"root_fid": "", "root_name": "", "root_is_dir": True}
        if len(values) == 1:
            item = values[0]
            return {
                "root_fid": str(item.get("fid") or ""),
                "root_name": str(item.get("file_name") or item.get("fid") or ""),
                "root_is_dir": False,
                "parent_fid": str(item.get("parent_id") or ""),
            }
        parent_ids = [str(value.get("parent_id") or "").strip() for value in values]
        parent_ids = [value for value in parent_ids if value]
        if not parent_ids:
            return {"root_fid": "", "root_name": f"{len(values)} 个已定位文件", "root_is_dir": True}
        account_id = str(self._account_id() or "").strip()
        if not account_id.isdigit():
            return {"root_fid": "", "root_name": f"{len(values)} 个已定位文件", "root_is_dir": True}
        chains = [_ancestor_chain(uow, value, account_id=account_id) for value in parent_ids]
        if any(not chain for chain in chains):
            return {"root_fid": "", "root_name": f"{len(values)} 个已定位文件", "root_is_dir": True}
        common: dict[str, Any] | None = None
        for nodes in zip(*(reversed(chain) for chain in chains), strict=False):
            if len({str(node.get("id") or "") for node in nodes}) != 1:
                break
            common = nodes[0]
        return {
            "root_fid": str((common or {}).get("id") or ""),
            "root_name": str((common or {}).get("name") or f"{len(values)} 个文件公共目录"),
            "root_is_dir": True,
        }

    @staticmethod
    def _raw_available(uow: SqlAlchemyUnitOfWork, sha1: str) -> bool:
        value = (
            uow.session.execute(
                text(
                    """
                SELECT raw_ffprobe_json, mediainfo_json
                FROM p115_mediainfo_cache
                WHERE UPPER(sha1) = :sha1
                LIMIT 1
                """
                ),
                {"sha1": sha1},
            )
            .mappings()
            .one_or_none()
        )
        return _raw_summary_usable(_row(value))


def _attach_completed_share_channels(
    uow: SqlAlchemyUnitOfWork,
    rows: Sequence[dict[str, Any]],
) -> None:
    local_ids: set[int] = set()
    center_ids: set[str] = set()
    hub_ids: set[str] = set()
    tmdb_ids: set[str] = set()
    row_pairs: dict[int, tuple[str, int]] = {}
    eligible: list[dict[str, Any]] = []
    for row in rows:
        raw_source_ids = row.get("source_ids")
        source_ids = list(raw_source_ids) if isinstance(raw_source_ids, list) else []
        source_kind = str(row.get("source_kind") or "").strip().lower()
        if not _channel_is_logical(row) and not source_ids and source_kind not in {"episode_group", "completed_season"}:
            continue
        eligible.append(row)
        for value in [row.get("id"), *source_ids]:
            source_id = _positive_integer(value)
            if source_id is not None:
                local_ids.add(source_id)
        center_values = [row.get("center_source_id")]
        if isinstance(row.get("center_source_ids"), list):
            center_values.extend(row.get("center_source_ids") or [])
        center_ids.update(str(value or "").strip() for value in center_values if str(value or "").strip())
        hub_id = _channel_hub_id(row)
        if hub_id:
            hub_ids.add(hub_id)
        pair = _channel_pair(row)
        if pair is not None:
            row_pairs[id(row)] = pair
            tmdb_ids.add(pair[0])
    if not eligible or not (local_ids or center_ids or hub_ids or tmdb_ids):
        return
    channels = [
        _row(value)
        for value in (
            uow.session.execute(
                text(
                    """
                    SELECT *
                    FROM shared_completed_season_share_channels
                    WHERE local_source_id = ANY(CAST(:local_ids AS integer[]))
                       OR center_source_id = ANY(CAST(:center_ids AS text[]))
                       OR hub_id = ANY(CAST(:hub_ids AS text[]))
                       OR raw_json->'event'->>'tmdb_id' = ANY(CAST(:tmdb_ids AS text[]))
                       OR raw_json->'payload'->>'tmdb_id' = ANY(CAST(:tmdb_ids AS text[]))
                       OR raw_json->>'tmdb_id' = ANY(CAST(:tmdb_ids AS text[]))
                    ORDER BY CASE status
                        WHEN 'valid' THEN 0 WHEN 'pending_review' THEN 1 WHEN 'creating' THEN 2 ELSE 9
                    END,
                    updated_at DESC NULLS LAST, id DESC
                    """
                ),
                {
                    "local_ids": sorted(local_ids),
                    "center_ids": sorted(center_ids),
                    "hub_ids": sorted(hub_ids),
                    "tmdb_ids": sorted(tmdb_ids),
                },
            )
            .mappings()
            .all()
        )
    ]
    channels = [value for value in channels if _channel_is_logical(value)]
    by_local: dict[int, dict[str, Any]] = {}
    by_center: dict[str, dict[str, Any]] = {}
    by_hub: dict[str, dict[str, Any]] = {}
    by_pair: dict[tuple[str, int], dict[str, Any]] = {}
    for channel in channels:
        local_id = _positive_integer(channel.get("local_source_id"))
        if local_id is not None:
            by_local.setdefault(local_id, channel)
        center_id = str(channel.get("center_source_id") or "").strip()
        if center_id:
            by_center.setdefault(center_id, channel)
        hub_id = _channel_hub_id(channel)
        if hub_id:
            by_hub.setdefault(hub_id, channel)
        pair = _channel_pair(channel)
        if pair is not None:
            by_pair.setdefault(pair, channel)
    for row in eligible:
        channel = by_local.get(_integer(row.get("id")))
        if channel is None and isinstance(row.get("source_ids"), list):
            channel = next(
                (
                    by_local[source_id]
                    for value in row.get("source_ids") or []
                    if (source_id := _positive_integer(value)) is not None and source_id in by_local
                ),
                None,
            )
        if channel is None:
            channel = by_center.get(str(row.get("center_source_id") or "").strip())
        if channel is None and isinstance(row.get("center_source_ids"), list):
            channel = next(
                (
                    by_center[center_id]
                    for value in row.get("center_source_ids") or []
                    if (center_id := str(value or "").strip()) and center_id in by_center
                ),
                None,
            )
        if channel is None:
            channel = by_hub.get(_channel_hub_id(row))
        if channel is None:
            pair = row_pairs.get(id(row)) or _channel_pair(row)
            if pair is not None:
                channel = by_pair.get(pair)
        if channel is None:
            row["has_share_channel"] = False
            row["share_channel_status"] = "none"
            continue
        status = str(channel.get("status") or "").strip().lower()
        row.update(
            {
                "completed_share_channel": _channel_public(channel),
                "has_share_channel": True,
                "share_channel_status": status,
                "share_review_status": channel.get("review_status") or "",
                "share_status_message": channel.get("status_message") or "",
                "has_valid_share_channel": status == "valid",
                "share_transfer_available": status == "valid",
            }
        )


def _channel_is_logical(row: Mapping[str, Any]) -> bool:
    raw = _json_mapping(row.get("raw_json"))
    nested_event = _json_mapping(raw.get("event"))
    identity = str(row.get("center_source_id") or row.get("source_id") or row.get("group_id") or "").strip()
    event_text = " ".join(
        str(value or "")
        for value in (
            raw.get("event"),
            raw.get("event_type"),
            raw.get("command"),
            raw.get("share_kind"),
            raw.get("source_kind"),
            nested_event.get("event_type"),
            nested_event.get("command"),
            nested_event.get("share_kind"),
            nested_event.get("source_kind"),
        )
    )
    return bool(
        identity.startswith("svg_")
        or str(row.get("source_kind") or "").strip().lower() == "logical_season"
        or str(raw.get("share_kind") or "").strip() == "logical_season"
        or "create_logical_season_filelist_share" in event_text
        or "logical_season" in event_text
    )


def _channel_pair(row: Mapping[str, Any]) -> tuple[str, int] | None:
    raw = _json_mapping(row.get("raw_json"))
    event = _json_mapping(raw.get("event"))
    payload = _json_mapping(raw.get("payload"))
    tmdb_id = _first_text(row.get("tmdb_id"), event.get("tmdb_id"), payload.get("tmdb_id"), raw.get("tmdb_id"))
    season = next(
        (
            value
            for value in (
                row.get("season_number"),
                event.get("season_number"),
                payload.get("season_number"),
                raw.get("season_number"),
            )
            if value not in (None, "")
        ),
        None,
    )
    season_number = _optional_integer(season)
    return (tmdb_id, season_number) if tmdb_id and season_number is not None else None


def _channel_hub_id(row: Mapping[str, Any]) -> str:
    raw = _json_mapping(row.get("raw_json"))
    event = _json_mapping(raw.get("event"))
    payload = _json_mapping(raw.get("payload"))
    center_item = _json_mapping(_json_mapping(raw.get("center_response")).get("item"))
    return _first_text(
        row.get("hub_id"),
        center_item.get("hub_id"),
        event.get("hub_id"),
        payload.get("hub_id"),
        raw.get("hub_id"),
    )


def _channel_public(channel: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "channel_id",
        "center_source_id",
        "hub_id",
        "manifest_hash",
        "share_code",
        "receive_code",
        "share_url",
        "share_title",
        "root_fid",
        "root_cid",
        "root_name",
        "file_count",
        "total_size",
        "status",
        "review_status",
        "status_message",
        "fail_count",
        "last_checked_at",
        "last_reported_at",
        "created_at",
        "updated_at",
    )
    return {key: channel[key] for key in keys if key in channel and channel[key] not in (None, "", [], {})}


def _ancestor_chain(
    uow: SqlAlchemyUnitOfWork,
    fid: str,
    *,
    account_id: str,
    max_depth: int = 30,
) -> list[dict[str, Any]]:
    chain: list[dict[str, Any]] = []
    current = str(fid or "").strip()
    seen: set[str] = set()
    for _ in range(max_depth):
        if not current or current == "0" or current in seen:
            break
        seen.add(current)
        value = (
            uow.session.execute(
                text(
                    """
                    SELECT id, parent_id, name, local_path
                    FROM p115_filesystem_cache
                    WHERE id = :id AND account_id = :account_id
                    LIMIT 1
                    """
                ),
                {"id": current, "account_id": account_id},
            )
            .mappings()
            .one_or_none()
        )
        if value is None:
            return []
        node = _row(value)
        chain.append(node)
        parent = str(node.get("parent_id") or "").strip()
        if not parent or parent == current:
            break
        current = parent
    return chain


def _aggregate_local_sources(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    singles: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        source_kind = str(row.get("source_kind") or "").strip().lower()
        item_type = str(row.get("item_type") or "").strip().lower()
        season_number = row.get("season_number")
        tmdb_id = str(row.get("tmdb_id") or "").strip()
        if (
            (source_kind == "episode" or item_type == "episode")
            and tmdb_id
            and season_number not in (None, "")
            and _source_usable(row)
        ):
            key = f"episode-season:{tmdb_id}:{season_number}"
            group = groups.setdefault(
                key,
                {
                    **row,
                    "source_ids": [],
                    "center_source_ids": [],
                    "source_kind": "episode_group",
                    "share_type": "season_pack",
                    "item_type": "Season",
                    "episode_number": None,
                    "center_source_id": "",
                    "source_key": "",
                    "is_aggregated_season": True,
                    "aggregated_source_count": 0,
                    "episode_numbers": [],
                    "item_count": 0,
                    "reported_count": 0,
                    "center_reported_count": 0,
                    "raw_uploaded_count": 0,
                    "status_values": [],
                    "center_status_values": [],
                },
            )
            source_id = _positive_integer(row.get("id"))
            if source_id is not None and source_id not in group["source_ids"]:
                group["source_ids"].append(source_id)
            center_id = str(row.get("center_source_id") or "").strip()
            if center_id and center_id not in group["center_source_ids"]:
                group["center_source_ids"].append(center_id)
            episode_number = _optional_integer(row.get("episode_number"))
            if episode_number is not None:
                group["episode_numbers"].append(episode_number)
            count = max(1, _integer(row.get("file_count") or 1))
            group["item_count"] += count
            group["reported_count"] += count if str(row.get("center_status") or "") == "reported" else 0
            group["center_reported_count"] = group["reported_count"]
            group["raw_uploaded_count"] += count
            group["aggregated_source_count"] += 1
            group["status_values"].append(str(row.get("status") or ""))
            group["center_status_values"].append(str(row.get("center_status") or ""))
            continue
        singles.append(row)
    aggregated: list[dict[str, Any]] = []
    for group in groups.values():
        statuses = [value for value in group.pop("status_values") if value]
        center_statuses = [value for value in group.pop("center_status_values") if value]
        if statuses and all(value == "disabled" for value in statuses):
            group["status"] = "disabled"
        elif "active" in statuses:
            group["status"] = "active"
        if center_statuses and all(value == "reported" for value in center_statuses):
            group["center_status"] = "reported"
        elif "reported" in center_statuses:
            group["center_status"] = "partial"
        elif center_statuses and all(value == "disabled" for value in center_statuses):
            group["center_status"] = "disabled"
        group["episode_numbers"] = sorted(set(group["episode_numbers"]))
        group["file_count"] = group["item_count"]
        group["share_remark"] = f"聚合显示：{group['aggregated_source_count']} 个本机分集源"
        aggregated.append(group)
    aggregated.extend(singles)
    aggregated.sort(key=lambda value: str(value.get("updated_at") or value.get("created_at") or ""), reverse=True)
    return aggregated


def _decorate_local_source(source: Mapping[str, Any]) -> dict[str, Any]:
    row = _row(source)
    row["share_code"] = row.get("center_source_id") or row.get("source_key") or ""
    row.setdefault("receive_code", "")
    row.setdefault("share_url", "")
    row["share_type"] = row.get("share_type") or row.get("source_kind")
    row["review_status"] = row.get("status")
    row["root_is_dir"] = bool(row.get("source_kind") == "logical_season" or row.get("is_aggregated_season"))
    count = _integer(row.get("item_count") or row.get("file_count"))
    if count <= 0 and row.get("center_status") == "reported":
        count = 1
    row["item_count"] = count
    row["raw_uploaded_count"] = (
        _integer(row.get("raw_uploaded_count")) if row.get("raw_uploaded_count") is not None else count
    )
    row["center_reported_count"] = (
        _integer(row.get("center_reported_count"))
        if row.get("center_reported_count") is not None
        else (count if row.get("center_status") == "reported" else 0)
    )
    row["reported_count"] = _integer(row.get("reported_count") or row.get("center_reported_count"))
    provider = str(row.get("source_provider") or "").strip()
    raw_value = row.get("raw_json")
    raw = raw_value if isinstance(raw_value, Mapping) else {}
    raw_text = " ".join(str(raw.get(key) or "") for key in ("source_provider", "task_source", "message", "reason"))
    row["is_auto_share"] = bool(
        provider in {"rapid_auto_library", "rapid_all_library"}
        or any(value in raw_text for value in ("入库自动", "自动登记", "自动共享", "一键全库"))
    )
    row["source_provider_label"] = {
        "manual_rapid": "手动登记",
        "rapid_auto_library": "入库自动登记",
        "rapid_all_library": "一键全库登记",
    }.get(provider, provider or "本地秒传源")
    return row


def _matches_source_status(source: Mapping[str, Any], status_filter: str) -> bool:
    tokens = [value.strip().lower() for value in str(status_filter or "usable").split(",") if value.strip()]
    if not tokens or any(value in {"all", "全部", "全部状态"} for value in tokens):
        return True
    status = str(source.get("status") or source.get("review_status") or "").strip().lower()
    center = str(source.get("center_status") or "").strip().lower()
    source_kind = str(source.get("source_kind") or "").strip().lower()
    has_center = bool(str(source.get("center_source_id") or "").strip())
    share_channel_status = str(source.get("share_channel_status") or "").strip().lower()
    has_share_channel = bool(source.get("has_share_channel"))
    disabled = status in {"disabled", "cancelled", "canceled", "deleted"} or center in {
        "disabled",
        "cancelled",
        "canceled",
    }
    failed = status in {
        "failed",
        "error",
        "dead",
        "expired",
        "rejected",
        "inconsistent",
        "incomplete",
        "raw_missing",
        "dirty_raw",
        "dirty_summary",
        "dirty_meta",
    } or center in {
        "failed",
        "error",
        "dead",
        "expired",
        "rejected",
        "raw_missing",
        "dirty_raw",
        "dirty_summary",
        "dirty_meta",
    }
    live = status in {"active", "available"}
    for token in tokens:
        if (
            token in {"usable", "active", "alive", "valid", "valid_share", "有效", "有效共享"}
            and live
            and not disabled
            and not failed
        ):
            return True
        if token in {"with_share", "has_share", "share", "已创建分享", "有分享"} and has_share_channel:
            return True
        if token in {"share_valid", "valid_channel", "可转存", "分享可用"} and share_channel_status == "valid":
            return True
        if token in {"share_pending", "pending_review", "分享审核中", "待审核分享"} and share_channel_status in {
            "creating",
            "pending_review",
        }:
            return True
        if token in {"share_abnormal", "share_failed", "分享异常"} and share_channel_status in {
            "review_failed",
            "expired",
            "import_failed",
            "disabled",
            "source_unavailable",
            "failed",
        }:
            return True
        if token in {"without_share", "no_share", "none_share", "无分享"} and not has_share_channel:
            return True
        if token in {"reported", "registered", "已登记", "已登记中心", "已上报"} and (
            center in {"reported", "partial"} or has_center
        ):
            return True
        if token in {"partial", "部分登记"} and center == "partial":
            return True
        if token in {"local", "local_only", "unreported", "not_reported", "本地", "未登记"} and (
            not has_center and center in {"", "local", "pending", "not_reported"}
        ):
            return True
        if token in {"failed", "error", "abnormal", "invalid", "失败", "异常"} and failed:
            return True
        if token in {"disabled", "cancelled", "canceled", "deleted", "停用", "已停用", "已取消"} and disabled:
            return True
        if token in {status, center, source_kind}:
            return True
    return False


def _source_usable(source: Mapping[str, Any]) -> bool:
    return _matches_source_status(source, "usable")


def _row(value: Mapping[Any, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    return value


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _first_text(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""


def _normalize_candidate_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(payload)
    raw_type = str(data.get("item_type") or data.get("share_item_type") or "").strip().lower()
    item_type = {
        "movie": "Movie",
        "film": "Movie",
        "series": "Series",
        "tv": "Series",
        "season": "Season",
        "episode": "Episode",
    }.get(raw_type, str(data.get("item_type") or data.get("share_item_type") or "").strip().title())
    if item_type:
        data["item_type"] = item_type
    if item_type in {"Season", "Episode"}:
        parent = _first_text(
            data.get("parent_series_tmdb_id"),
            data.get("series_tmdb_id"),
            data.get("parent_tmdb_id"),
            data.get("tmdb_id"),
        )
        if parent:
            data["parent_series_tmdb_id"] = parent
    return data


def _raw_summary_usable(cache_row: Mapping[str, Any]) -> bool:
    raw = _json_mapping(cache_row.get("raw_ffprobe_json"))
    if not raw:
        return False
    media_source = _json_mapping(raw.get("MediaSourceInfo"))
    streams = media_source.get("MediaStreams")
    if streams is None:
        streams = raw.get("MediaStreams")
    if streams is None:
        streams = raw.get("streams")
    if not isinstance(streams, list) or any(not isinstance(stream, Mapping) for stream in streams):
        return False
    typed_streams = [dict(stream) for stream in streams]
    videos = [stream for stream in typed_streams if _stream_type(stream) == "video"]
    audios = [stream for stream in typed_streams if _stream_type(stream) == "audio"]
    subtitles = [stream for stream in typed_streams if _stream_type(stream) == "subtitle"]
    has_video_display = bool(
        videos
        and any(
            _integer(stream.get("width") or stream.get("Width")) > 0
            or _integer(stream.get("height") or stream.get("Height")) > 0
            or _first_text(stream.get("codec_name"), stream.get("Codec"))
            for stream in videos
        )
    )
    if not has_video_display:
        return False

    def has_track_title(values: Sequence[Mapping[str, Any]]) -> bool:
        return any(
            _first_text(
                value.get("title"),
                value.get("Title"),
                value.get("DisplayTitle"),
                _json_mapping(value.get("tags")).get("title"),
                _json_mapping(value.get("tags")).get("TITLE"),
            )
            for value in values
        )

    if audios and not has_track_title(audios):
        return False
    return not subtitles or has_track_title(subtitles)


def _stream_type(stream: Mapping[str, Any]) -> str:
    return _first_text(stream.get("codec_type"), stream.get("Type")).lower()


def _array(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, str) and value.strip():
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = [value]
    if isinstance(value, Mapping):
        value = list(value.values())
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    result: list[Any] = []
    for item in value:
        if isinstance(item, Mapping):
            candidate = next(
                (item.get(key) for key in ("sha1", "pick_code", "pickcode", "pc", "value") if item.get(key)),
                None,
            )
        else:
            candidate = item
        normalized = str(candidate or "").strip()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _slot_array(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        if not value.strip():
            return []
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = [value]
    if isinstance(value, Mapping):
        value = list(value.values())
    if not isinstance(value, (list, tuple)):
        value = [value]
    result: list[Any] = []
    for item in value:
        if isinstance(item, Mapping):
            item = next(
                (item.get(key) for key in ("sha1", "pick_code", "pickcode", "pc", "value") if item.get(key)),
                "",
            )
        result.append(item)
    return result


def _sha1(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    return (
        normalized if len(normalized) == 40 and all(character in "0123456789ABCDEF" for character in normalized) else ""
    )


def _integer(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _optional_integer(value: Any) -> int | None:
    normalized = _integer(value, -1)
    return normalized if normalized >= 0 else None


def _positive_integer(value: Any) -> int | None:
    normalized = _integer(value)
    return normalized if normalized > 0 else None


__all__ = ["SqlAlchemySharingProjectionRepository"]
