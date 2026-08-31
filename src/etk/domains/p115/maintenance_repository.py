from __future__ import annotations

import random
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

_VIDEO_EXTENSIONS = (
    "mp4",
    "mkv",
    "avi",
    "ts",
    "iso",
    "rmvb",
    "wmv",
    "mov",
    "m2ts",
    "flv",
    "mpg",
)


class SqlAlchemyP115MaintenanceRepository:
    """Local facts used by bounded P115 maintenance operations."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        choice: Callable[[Sequence[int]], int] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._choice = choice or random.choice

    def speedtest_sample(self, *, account_id: str) -> dict[str, Any] | None:
        normalized_account_id = _account_id(account_id)
        with self._unit_of_work() as uow:
            rows = list(
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, parent_series_tmdb_id,
                               season_number, episode_number, file_pickcode_json,
                               file_sha1_json, asset_details_json
                        FROM media_metadata
                        WHERE in_library = TRUE
                          AND item_type IN ('Movie', 'Episode')
                          AND file_pickcode_json IS NOT NULL
                          AND jsonb_typeof(file_pickcode_json) = 'array'
                          AND jsonb_array_length(file_pickcode_json) > 0
                        ORDER BY RANDOM()
                        LIMIT 20
                        """
                    )
                ).mappings()
            )
            for row in rows:
                pick_codes = _json_list(row.get("file_pickcode_json"))
                sha1s = _json_list(row.get("file_sha1_json"))
                indexes = [index for index, value in enumerate(pick_codes) if str(value or "").strip()]
                if not indexes:
                    continue
                index = self._choice(indexes)
                pick_code = str(pick_codes[index] or "").strip()
                sha1 = str(sha1s[index] or "").strip().upper() if index < len(sha1s) else ""
                cache = self._matching_cache_row(
                    uow,
                    account_id=normalized_account_id,
                    pick_code=pick_code,
                    sha1=sha1,
                )
                file_name = _asset_name(row.get("asset_details_json"), index)
                return _sample(dict(row), cache, pick_code=pick_code, sha1=sha1, file_name=file_name)

            cache = (
                uow.session.execute(
                    text(
                        """
                    SELECT id, name, pick_code, sha1, size, local_path
                    FROM p115_filesystem_cache
                    WHERE account_id = :account_id
                      AND entry_kind = 'file'
                      AND pick_code IS NOT NULL
                      AND pick_code <> ''
                      AND LOWER(split_part(name, '.', array_length(string_to_array(name, '.'), 1))) = ANY(:extensions)
                    ORDER BY RANDOM()
                    LIMIT 1
                    """
                    ),
                    {"account_id": normalized_account_id, "extensions": list(_VIDEO_EXTENSIONS)},
                )
                .mappings()
                .first()
            )
        if cache is None:
            return None
        value = dict(cache)
        return {
            "pick_code": str(value.get("pick_code") or "").strip(),
            "sha1": str(value.get("sha1") or "").strip().upper(),
            "file_name": str(value.get("name") or value.get("pick_code") or ""),
            "size": int(value.get("size") or 0),
            "local_path": str(value.get("local_path") or ""),
            "tmdb_id": "",
            "item_type": "",
            "title": str(value.get("name") or ""),
            "parent_series_tmdb_id": "",
            "season_number": None,
            "episode_number": None,
        }

    def cached_unrecognized_file_ids(
        self,
        *,
        account_id: str,
        parent_id: str,
    ) -> frozenset[str]:
        normalized_account_id = _account_id(account_id)
        normalized_parent_id = _directory_id(parent_id)
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT record.file_id
                    FROM p115_organize_records AS record
                    JOIN p115_filesystem_cache AS cache ON cache.id = record.file_id
                    WHERE record.status = 'unrecognized'
                      AND cache.account_id = :account_id
                      AND cache.parent_id = :parent_id
                      AND cache.entry_kind = 'file'
                    """
                ),
                {"account_id": normalized_account_id, "parent_id": normalized_parent_id},
            ).mappings()
            return frozenset(str(row["file_id"]) for row in rows if str(row.get("file_id") or "").strip())

    def delete_confirmed_unrecognized_facts(
        self,
        file_ids: Sequence[str],
        *,
        account_id: str,
        parent_id: str,
    ) -> int:
        normalized = sorted({str(value or "").strip() for value in file_ids if str(value or "").strip()})
        if not normalized:
            return 0
        normalized_account_id = _account_id(account_id)
        normalized_parent_id = _directory_id(parent_id)
        with self._unit_of_work() as uow:
            locked = [
                str(row["id"])
                for row in uow.session.execute(
                    text(
                        """
                        SELECT id
                        FROM p115_filesystem_cache
                        WHERE id = ANY(:file_ids)
                          AND account_id = :account_id
                          AND parent_id = :parent_id
                          AND entry_kind = 'file'
                        FOR UPDATE
                        """
                    ),
                    {
                        "file_ids": normalized,
                        "account_id": normalized_account_id,
                        "parent_id": normalized_parent_id,
                    },
                ).mappings()
            ]
            if not locked:
                return 0
            deleted = uow.session.execute(
                text(
                    """
                    DELETE FROM p115_organize_records
                    WHERE status = 'unrecognized'
                      AND file_id = ANY(:file_ids)
                    """
                ),
                {"file_ids": locked},
            )
            uow.session.execute(
                text(
                    """
                    DELETE FROM p115_filesystem_cache
                    WHERE id = ANY(:file_ids)
                      AND account_id = :account_id
                      AND parent_id = :parent_id
                      AND entry_kind = 'file'
                    """
                ),
                {
                    "file_ids": locked,
                    "account_id": normalized_account_id,
                    "parent_id": normalized_parent_id,
                },
            )
            uow.commit()
        return int(getattr(deleted, "rowcount", 0) or 0)

    @staticmethod
    def _matching_cache_row(
        uow: SqlAlchemyUnitOfWork,
        *,
        account_id: str,
        pick_code: str,
        sha1: str,
    ) -> Mapping[str, Any]:
        clauses: list[str] = []
        parameters: dict[str, Any] = {"account_id": account_id}
        if pick_code:
            clauses.append("pick_code = :pick_code")
            parameters["pick_code"] = pick_code
        if sha1:
            clauses.append("UPPER(sha1) = :sha1")
            parameters["sha1"] = sha1
        if not clauses:
            return {}
        row = (
            uow.session.execute(
                text(
                    f"""
                SELECT id, name, pick_code, sha1, size, local_path
                FROM p115_filesystem_cache
                WHERE account_id = :account_id
                  AND entry_kind = 'file'
                  AND ({" OR ".join(clauses)})
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
                """
                ),
                parameters,
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else {}


def _sample(
    metadata: Mapping[str, Any],
    cache: Mapping[str, Any],
    *,
    pick_code: str,
    sha1: str,
    file_name: str,
) -> dict[str, Any]:
    return {
        "pick_code": pick_code or str(cache.get("pick_code") or "").strip(),
        "sha1": sha1 or str(cache.get("sha1") or "").strip().upper(),
        "file_name": file_name or str(cache.get("name") or metadata.get("title") or pick_code),
        "size": int(cache.get("size") or 0),
        "local_path": str(cache.get("local_path") or ""),
        "tmdb_id": metadata.get("tmdb_id"),
        "item_type": metadata.get("item_type"),
        "title": str(metadata.get("title") or ""),
        "parent_series_tmdb_id": str(metadata.get("parent_series_tmdb_id") or ""),
        "season_number": metadata.get("season_number"),
        "episode_number": metadata.get("episode_number"),
    }


def _json_list(value: object) -> list[Any]:
    return list(deepcopy(value)) if isinstance(value, list) else []


def _asset_name(value: object, index: int) -> str:
    assets = _json_list(value)
    candidates = ([assets[index]] if 0 <= index < len(assets) else []) + assets
    for asset in candidates:
        if not isinstance(asset, Mapping):
            continue
        name = asset.get("file_name") or asset.get("FileName") or asset.get("name") or asset.get("Name")
        if name:
            return str(name)
        path = asset.get("path") or asset.get("Path") or asset.get("file_path") or asset.get("FilePath")
        if path:
            return str(path).replace("\\", "/").rsplit("/", 1)[-1]
    return ""


def _account_id(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized.isdigit():
        raise ValueError("stable 115 account identity is required")
    return normalized


def _directory_id(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized or not normalized.isdigit():
        raise ValueError("115 directory ID must be numeric")
    return normalized


__all__ = ["SqlAlchemyP115MaintenanceRepository"]
