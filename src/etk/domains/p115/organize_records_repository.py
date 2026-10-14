from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .organize_records import P115OrganizeRecordsPage, normalize_record_rows


class SqlAlchemyP115OrganizeRecordsRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def add_missing_record(self, record: Mapping[str, Any]) -> bool:
        """直出只补缺失记录；重复或晚到的事件不覆盖人工纠错及原处理时间。"""
        with self._unit_of_work() as uow:
            inserted = uow.session.scalar(
                text(
                    """
                    INSERT INTO p115_organize_records (
                        file_id, pick_code, original_name, renamed_name, status,
                        tmdb_id, media_type, season_number, target_cid, category_name, processed_at
                    ) VALUES (
                        :file_id, :pick_code, :original_name, :original_name, 'success',
                        :tmdb_id, :media_type, :season_number, :target_cid, :category_name, NOW()
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """
                ),
                dict(record),
            )
            uow.commit()
        return inserted is not None

    def list_records(
        self,
        *,
        page: int,
        per_page: int,
        search: str,
        status: str,
        cid: str,
        fail_reason: str,
    ) -> P115OrganizeRecordsPage:
        clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if search:
            clauses.append("(original_name ILIKE :search OR renamed_name ILIKE :search)")
            parameters["search"] = f"%{search}%"
        if status != "all":
            clauses.append("status = :status")
            parameters["status"] = status
        if cid:
            clauses.append("target_cid = :cid")
            parameters["cid"] = cid
        if fail_reason:
            clauses.append("fail_reason = :fail_reason")
            parameters["fail_reason"] = fail_reason
        where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
        parameters.update({"limit": per_page, "offset": (page - 1) * per_page})
        with self._unit_of_work() as uow:
            total = int(
                uow.session.scalar(
                    text(f"SELECT COUNT(*) FROM p115_organize_records{where_sql}"),
                    parameters,
                )
                or 0
            )
            items = (
                uow.session.execute(
                    text(
                        f"""
                    SELECT *
                    FROM p115_organize_records
                    {where_sql}
                    ORDER BY processed_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                    ),
                    parameters,
                )
                .mappings()
                .all()
            )
            stats_row = (
                uow.session.execute(
                    text(
                        """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status = 'success') AS success,
                        COUNT(*) FILTER (WHERE status = 'unrecognized') AS unrecognized,
                        COUNT(*) FILTER (WHERE status = 'unqualified') AS unqualified,
                        COUNT(*) FILTER (WHERE processed_at >= NOW() - INTERVAL '7 days') AS this_week
                    FROM p115_organize_records
                    """
                    )
                )
                .mappings()
                .one()
            )
            fail_reasons = (
                uow.session.execute(
                    text(
                        """
                    SELECT fail_reason, COUNT(*) AS count
                    FROM p115_organize_records
                    WHERE fail_reason IS NOT NULL AND BTRIM(fail_reason) <> ''
                    GROUP BY fail_reason
                    ORDER BY count DESC, fail_reason
                    """
                    )
                )
                .mappings()
                .all()
            )
            return P115OrganizeRecordsPage(
                items=normalize_record_rows(items),
                total=total,
                fail_reasons=normalize_record_rows(fail_reasons),
                stats={
                    "total": int(stats_row["total"] or 0),
                    "success": int(stats_row["success"] or 0),
                    "unrecognized": int(stats_row["unrecognized"] or 0),
                    "unqualified": int(stats_row["unqualified"] or 0),
                    "thisWeek": int(stats_row["this_week"] or 0),
                },
            )
        raise AssertionError("unreachable")

    def delete_record(self, record_id: int) -> None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                    SELECT file_id, target_cid
                    FROM p115_organize_records
                    WHERE id = :record_id
                    FOR UPDATE
                    """
                    ),
                    {"record_id": record_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                uow.session.execute(
                    text("DELETE FROM p115_organize_records WHERE id = :record_id"),
                    {"record_id": record_id},
                )
                uow.session.execute(
                    text("DELETE FROM p115_filesystem_cache WHERE id = :file_id"),
                    {"file_id": row["file_id"]},
                )
                target_cid = str(row["target_cid"] or "").strip()
                if target_cid:
                    uow.session.execute(
                        text(
                            """
                            DELETE FROM p115_filesystem_cache
                            WHERE id = :target_cid OR parent_id = :target_cid
                            """
                        ),
                        {"target_cid": target_cid},
                    )
            uow.commit()

    def backfill_missing_tmdb_ids(self) -> int:
        """Resolve missing identities from the authoritative media metadata index."""
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    WITH matched AS (
                        SELECT DISTINCT ON (record.id)
                            record.id,
                            CASE
                                WHEN LOWER(metadata.item_type) = 'movie' THEN metadata.tmdb_id
                                WHEN LOWER(metadata.item_type) IN ('series', 'season', 'episode')
                                    THEN COALESCE(metadata.parent_series_tmdb_id, metadata.tmdb_id)
                                ELSE NULL
                            END AS tmdb_id
                        FROM p115_organize_records record
                        LEFT JOIN p115_filesystem_cache cache ON cache.id = record.file_id
                        JOIN media_metadata metadata ON (
                            NULLIF(record.pick_code, '') IS NOT NULL
                            AND metadata.file_pickcode_json ? record.pick_code
                        ) OR (
                            NULLIF(cache.sha1, '') IS NOT NULL
                            AND (
                                metadata.file_sha1_json ? UPPER(cache.sha1)
                                OR metadata.file_sha1_json ? LOWER(cache.sha1)
                            )
                        )
                        WHERE LOWER(BTRIM(COALESCE(record.tmdb_id, '')))
                            IN ('', 'null', 'none', 'undefined')
                        ORDER BY
                            record.id,
                            CASE WHEN NULLIF(record.pick_code, '') IS NOT NULL
                                AND metadata.file_pickcode_json ? record.pick_code THEN 0 ELSE 1 END,
                            CASE metadata.item_type
                                WHEN 'Movie' THEN 1
                                WHEN 'Episode' THEN 2
                                WHEN 'Season' THEN 3
                                WHEN 'Series' THEN 4
                                ELSE 9
                            END,
                            metadata.in_library DESC,
                            metadata.last_updated_at DESC NULLS LAST
                    )
                    UPDATE p115_organize_records record
                    SET tmdb_id = matched.tmdb_id
                    FROM matched
                    WHERE record.id = matched.id
                      AND LOWER(BTRIM(COALESCE(matched.tmdb_id, '')))
                          NOT IN ('', 'null', 'none', 'undefined')
                    RETURNING record.id
                    """
                )
            )
            updated_count = len(result.fetchall())
            uow.commit()
            return updated_count


__all__ = ["SqlAlchemyP115OrganizeRecordsRepository"]
