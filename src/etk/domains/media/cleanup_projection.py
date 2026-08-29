from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyCleanupMediaProjection:
    """P5-owned read projection for pending cleanup rows and media metadata."""

    def lock_cleanup_scan_source(self, uow: SqlAlchemyUnitOfWork) -> None:
        uow.session.execute(text("LOCK TABLE emby_users, media_metadata IN SHARE MODE"))

    def list_pending_tasks(self, uow: SqlAlchemyUnitOfWork) -> tuple[Mapping[str, Any], ...]:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT
                        t.id,
                        t.tmdb_id,
                        t.item_type,
                        t.versions_info_json,
                        t.best_version_json,
                        m.title AS item_name,
                        m.season_number,
                        m.episode_number,
                        m.parent_series_tmdb_id,
                        parent.title AS parent_series_name
                    FROM cleanup_index AS t
                    JOIN media_metadata AS m
                      ON t.tmdb_id = m.tmdb_id
                     AND t.item_type = m.item_type
                    LEFT JOIN media_metadata AS parent
                      ON m.parent_series_tmdb_id = parent.tmdb_id
                     AND parent.item_type = 'Series'
                    WHERE t.status = 'pending'
                    ORDER BY
                        parent.title,
                        m.season_number,
                        m.episode_number,
                        m.title
                    """
                )
            )
            .mappings()
            .all()
        )
        return tuple(dict(row) for row in rows)

    def cleanup_scan_source(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        user_id: str | None,
        library_ids: Sequence[str],
        rating_mapping: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> Mapping[str, Any]:
        user_policy: Mapping[str, Any] | None = None
        if user_id:
            value = uow.session.scalar(
                text("SELECT policy_json FROM emby_users WHERE id = :user_id"),
                {"user_id": user_id},
            )
            if value is None:
                return {
                    "user_policy": None,
                    "user_exists": False,
                    "visible_movie_ids": (),
                    "visible_series_ids": (),
                    "items": (),
                }
            if not isinstance(value, Mapping):
                raise TypeError("cleanup scan Emby user policy must be an object")
            user_policy = dict(value)

        root_rows = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, asset_details_json, tags_json,
                           custom_rating, official_rating_json
                    FROM media_metadata
                    WHERE in_library IS TRUE
                      AND item_type IN ('Movie', 'Series')
                    ORDER BY item_type, tmdb_id
                    """
                )
            )
            .mappings()
            .all()
        )
        visible_movie_ids: list[str] = []
        visible_series_ids: list[str] = []
        for row in root_rows:
            if not _cleanup_scan_row_visible(
                dict(row),
                user_policy=user_policy,
                library_ids=library_ids,
                rating_mapping=rating_mapping,
            ):
                continue
            tmdb_id = str(row["tmdb_id"] or "").strip()
            if not tmdb_id:
                continue
            if str(row["item_type"]) == "Movie":
                visible_movie_ids.append(tmdb_id)
            else:
                visible_series_ids.append(tmdb_id)

        if not visible_movie_ids and not visible_series_ids:
            return {
                "user_policy": dict(user_policy) if user_policy is not None else None,
                "user_exists": True,
                "visible_movie_ids": (),
                "visible_series_ids": (),
                "items": (),
            }
        items = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, title, original_language,
                           countries_json, asset_details_json
                    FROM media_metadata
                    WHERE in_library IS TRUE
                      AND jsonb_typeof(asset_details_json) = 'array'
                      AND jsonb_array_length(asset_details_json) > 1
                      AND (
                            (item_type = 'Movie' AND tmdb_id = ANY(CAST(:movie_ids AS text[])))
                         OR (item_type = 'Episode'
                             AND parent_series_tmdb_id = ANY(CAST(:series_ids AS text[])))
                      )
                    ORDER BY item_type, COALESCE(title, ''), tmdb_id
                    """
                ),
                {
                    "movie_ids": visible_movie_ids,
                    "series_ids": visible_series_ids,
                },
            )
            .mappings()
            .all()
        )
        return {
            "user_policy": dict(user_policy) if user_policy is not None else None,
            "user_exists": True,
            "visible_movie_ids": tuple(visible_movie_ids),
            "visible_series_ids": tuple(visible_series_ids),
            "items": tuple(dict(row) for row in items),
        }

    def version_deletion_snapshot(
        self,
        uow: SqlAlchemyUnitOfWork,
        emby_id: str,
    ) -> Mapping[str, Any] | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, title, asset_details_json, file_pickcode_json
                    FROM media_metadata
                    WHERE jsonb_typeof(asset_details_json) = 'array'
                      AND EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(asset_details_json) AS asset(value)
                          WHERE COALESCE(asset.value->>'emby_item_id', asset.value->>'id') = :emby_id
                      )
                    ORDER BY tmdb_id, item_type
                    LIMIT 1
                    FOR UPDATE
                    """
                ),
                {"emby_id": emby_id},
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return {
            "tmdb_id": str(row["tmdb_id"]),
            "item_type": str(row["item_type"]),
            "title": row["title"],
            "asset_details": row["asset_details_json"],
            "file_pickcodes": row["file_pickcode_json"],
        }

    def execution_identity(
        self,
        uow: SqlAlchemyUnitOfWork,
        emby_id: str,
        *,
        fallback_path: str | None = None,
        fallback_tmdb_id: str | None = None,
        fallback_item_type: str | None = None,
    ) -> Mapping[str, Any] | None:
        normalized_id = str(emby_id or "").strip()
        if not normalized_id:
            raise ValueError("cleanup execution identity requires emby_id")
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, title, parent_series_tmdb_id,
                           emby_item_ids_json, asset_details_json, file_pickcode_json
                    FROM media_metadata
                    WHERE (
                            jsonb_typeof(asset_details_json) = 'array'
                        AND EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(asset_details_json) AS asset(value)
                            WHERE COALESCE(asset.value->>'emby_item_id', asset.value->>'id') = :emby_id
                        )
                    ) OR (
                            jsonb_typeof(emby_item_ids_json) = 'array'
                        AND emby_item_ids_json ? :emby_id
                    )
                    ORDER BY tmdb_id, item_type
                    LIMIT 1
                    FOR UPDATE
                    """
                ),
                {"emby_id": normalized_id},
            )
            .mappings()
            .first()
        )
        if row is None and fallback_tmdb_id and fallback_item_type:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, parent_series_tmdb_id,
                               emby_item_ids_json, asset_details_json, file_pickcode_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = :item_type
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_id": fallback_tmdb_id, "item_type": fallback_item_type},
                )
                .mappings()
                .first()
            )
        if row is None and not fallback_path:
            return None

        assets = _mapping_values(row.get("asset_details_json") if row is not None else None)
        emby_ids = _string_values(row.get("emby_item_ids_json") if row is not None else None)
        pickcodes = _string_values(row.get("file_pickcode_json") if row is not None else None)
        matched_indexes = {
            index
            for index, asset in enumerate(assets)
            if str(asset.get("emby_item_id") or asset.get("id") or "").strip() == normalized_id
        }
        matched_indexes.update(index for index, value in enumerate(emby_ids) if value == normalized_id)
        if not matched_indexes and fallback_path:
            matched_indexes.update(
                index for index, asset in enumerate(assets) if str(asset.get("path") or "").strip() == fallback_path
            )

        files: list[dict[str, Any]] = []
        for index in sorted(matched_indexes):
            asset = assets[index] if index < len(assets) else {}
            path = str(asset.get("path") or fallback_path or "").strip()
            pick_code = str(asset.get("pick_code") or (pickcodes[index] if index < len(pickcodes) else "")).strip()
            files.append({"path": path, "pick_code": pick_code})
        if not files and fallback_path:
            files.append({"path": fallback_path, "pick_code": pickcodes[0] if len(pickcodes) == 1 else ""})
        if not files and len(pickcodes) == 1:
            files.append({"path": "", "pick_code": pickcodes[0]})

        unique_files: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for file in files:
            identity = (str(file["path"]), str(file["pick_code"]))
            if identity in seen:
                continue
            seen.add(identity)
            unique_files.append(file)

        p115_by_pickcode: dict[str, Mapping[str, Any]] = {}
        requested_pickcodes = [str(file["pick_code"]) for file in unique_files if file["pick_code"]]
        if requested_pickcodes:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, parent_id, account_id, pick_code, local_path, sha1, size, updated_at
                        FROM p115_filesystem_cache
                        WHERE pick_code = ANY(CAST(:pickcodes AS text[]))
                        ORDER BY pick_code, updated_at DESC NULLS LAST, id
                        FOR SHARE
                        """
                    ),
                    {"pickcodes": requested_pickcodes},
                )
                .mappings()
                .all()
            )
            for cache in rows:
                pick_code = str(cache["pick_code"] or "").strip()
                if not pick_code:
                    continue
                if pick_code in p115_by_pickcode:
                    raise ValueError(f"cleanup target PickCode {pick_code!r} has ambiguous 115 cache identities")
                p115_by_pickcode[pick_code] = dict(cache)

        for file in unique_files:
            cache = p115_by_pickcode.get(str(file["pick_code"]))
            file["p115"] = (
                {
                    "fid": str(cache["id"] or ""),
                    "parent_id": str(cache["parent_id"] or ""),
                    "account_id": str(cache["account_id"] or ""),
                    "pick_code": str(cache["pick_code"] or ""),
                    "local_path": str(cache["local_path"] or ""),
                    "sha1": str(cache["sha1"] or ""),
                    "size": int(cache["size"] or 0),
                }
                if cache is not None
                else None
            )

        title = str(row["title"] or "") if row is not None else ""
        series_id = str(row["parent_series_tmdb_id"] or "") if row is not None else ""
        return {
            "emby_id": normalized_id,
            "tmdb_id": str(row["tmdb_id"] if row is not None else fallback_tmdb_id or ""),
            "item_type": str(row["item_type"] if row is not None else fallback_item_type or ""),
            "title": title,
            "series_id": series_id,
            "files": unique_files,
        }


def _mapping_values(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


def _string_values(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    return tuple(str(item or "").strip() for item in value)


def _cleanup_scan_row_visible(
    row: Mapping[str, Any],
    *,
    user_policy: Mapping[str, Any] | None,
    library_ids: Sequence[str],
    rating_mapping: Mapping[str, Sequence[Mapping[str, Any]]],
) -> bool:
    assets = _mapping_values(row.get("asset_details_json"))
    selected_libraries = {str(value) for value in library_ids}
    if selected_libraries and not any(
        str(asset.get("source_library_id") or "") in selected_libraries for asset in assets
    ):
        return False
    if user_policy is None:
        return True

    raw_limit = user_policy.get("MaxParentalRating")
    if raw_limit is not None:
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError) as exc:
            raise ValueError("cleanup scan user MaxParentalRating must be an integer") from exc
        if _cleanup_rating_value(row, rating_mapping) > limit:
            return False

    enabled_folders = _policy_json_string_set(user_policy, "EnabledFolders")
    excluded_folders = _policy_json_string_set(user_policy, "ExcludedSubFolders")
    all_folders = user_policy.get("EnableAllFolders") is True
    folder_visible = False
    for asset in assets:
        ancestors = _json_string_set(asset.get("ancestor_ids"))
        allowed = (
            all_folders
            or bool(ancestors & enabled_folders)
            or str(asset.get("source_library_id") or "") in enabled_folders
        )
        if allowed and not ancestors & excluded_folders:
            folder_visible = True
            break
    if not folder_visible:
        return False

    blocked_tags = _policy_json_string_set(user_policy, "BlockedTags")
    if blocked_tags & _json_string_set(row.get("tags_json")):
        return False

    blocked_unrated_types = _policy_json_string_set(user_policy, "BlockUnratedItems")
    if str(row.get("item_type") or "") in blocked_unrated_types and _cleanup_unrated(_cleanup_rating_text(row)):
        return False
    return True


def _cleanup_rating_value(
    row: Mapping[str, Any],
    rating_mapping: Mapping[str, Sequence[Mapping[str, Any]]],
) -> int:
    rating = _cleanup_rating_text(row)
    for rules in rating_mapping.values():
        for rule in rules:
            if str(rule.get("code") or "") != rating:
                continue
            value = rule.get("emby_value")
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError("cleanup rating mapping emby_value must be an integer") from exc
    digits = re.sub(r"[^0-9]", "", rating)
    return int(digits) if digits else 0


def _cleanup_rating_text(row: Mapping[str, Any]) -> str:
    custom = row.get("custom_rating")
    if custom is not None and str(custom) != "":
        return str(custom)
    official = row.get("official_rating_json")
    return str(official.get("US") or "") if isinstance(official, Mapping) else ""


def _cleanup_unrated(value: object) -> bool:
    text_value = str(value or "")
    if not text_value or text_value in {"NR", "UR", "Unrated", "Not Rated"}:
        return True
    known = {
        "G",
        "PG",
        "PG-13",
        "R",
        "NC-17",
        "X",
        "XXX",
        "AO",
        "TV-Y",
        "TV-Y7",
        "TV-G",
        "TV-PG",
        "TV-14",
        "TV-MA",
    }
    return text_value not in known and not re.sub(r"[^0-9]", "", text_value)


def _json_string_set(value: object) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value if isinstance(item, str)}


def _policy_json_string_set(policy: Mapping[str, Any], field: str) -> set[str]:
    value = policy.get(field)
    if value is None:
        return set()
    if not isinstance(value, list):
        raise TypeError(f"cleanup scan user {field} must be an array")
    result: set[str] = set()
    for item in value:
        if item is None:
            continue
        if isinstance(item, Mapping | list):
            raise TypeError(f"cleanup scan user {field} entries must be scalar values")
        if isinstance(item, str):
            result.add(item)
            continue
        if isinstance(item, bool | int | float):
            result.add(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            continue
        raise TypeError(f"cleanup scan user {field} entries must be JSON scalar values")
    return result


__all__ = ["SqlAlchemyCleanupMediaProjection"]
