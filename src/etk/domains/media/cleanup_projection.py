from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyCleanupMediaProjection:
    """P5-owned read projection for pending cleanup rows and media metadata."""

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


__all__ = ["SqlAlchemyCleanupMediaProjection"]
