from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Protocol

from pypinyin import Style, pinyin
from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .models import ServiceResult

logger = logging.getLogger(__name__)

_COMPACT_RE = re.compile(r"[^0-9a-z\u3400-\u9fff]+", re.IGNORECASE)
_SEARCH_PART_RE = re.compile(r"[0-9a-z\u3400-\u9fff]+", re.IGNORECASE)
_PINYIN_INITIAL_OVERRIDES = {"甄嬛传": "zhz"}


class EmbySearchRepository(Protocol):
    def start_rebuild(self, token: str) -> None: ...

    def upsert_items(self, rows: Sequence[Mapping[str, Any]]) -> int: ...

    def complete_rebuild(self, token: str) -> int: ...

    def abort_rebuild(self, token: str) -> None: ...

    def delete_items(self, item_ids: Iterable[object]) -> int: ...

    def search(self, query: str, item_types: Sequence[str], limit: int) -> dict[str, Any]: ...


class SqlAlchemyEmbySearchRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def start_rebuild(self, token: str) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("DELETE FROM emby_search_rebuild_state WHERE started_at < NOW() - INTERVAL '30 minutes'")
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_search_rebuild_state(token, started_at)
                    VALUES (:token, NOW())
                    ON CONFLICT (token) DO UPDATE SET started_at = NOW()
                    """
                ),
                {"token": token},
            )
            uow.commit()

    def upsert_items(self, rows: Sequence[Mapping[str, Any]]) -> int:
        values = [dict(row) for row in rows]
        if not values:
            return 0
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_search_index (
                        emby_item_id, item_type, title, original_title, series_name,
                        search_compact, pinyin_full, pinyin_initials,
                        search_ngrams, pinyin_ngrams
                    ) VALUES (
                        :emby_item_id, :item_type, :title, :original_title, :series_name,
                        :search_compact, :pinyin_full, :pinyin_initials,
                        :search_ngrams, :pinyin_ngrams
                    )
                    ON CONFLICT (emby_item_id) DO UPDATE SET
                        item_type = EXCLUDED.item_type,
                        title = EXCLUDED.title,
                        original_title = EXCLUDED.original_title,
                        series_name = EXCLUDED.series_name,
                        search_compact = EXCLUDED.search_compact,
                        pinyin_full = EXCLUDED.pinyin_full,
                        pinyin_initials = EXCLUDED.pinyin_initials,
                        search_ngrams = EXCLUDED.search_ngrams,
                        pinyin_ngrams = EXCLUDED.pinyin_ngrams,
                        updated_at = NOW()
                    """
                ),
                values,
            )
            uow.commit()
        return len(values)

    def complete_rebuild(self, token: str) -> int:
        with self._unit_of_work() as uow:
            state = (
                uow.session.execute(
                    text("SELECT started_at FROM emby_search_rebuild_state WHERE token = :token FOR UPDATE"),
                    {"token": token},
                )
                .mappings()
                .first()
            )
            if state is None:
                return 0
            result = uow.session.execute(
                text("DELETE FROM emby_search_index WHERE updated_at < :started_at"),
                {"started_at": state["started_at"]},
            )
            uow.session.execute(
                text("DELETE FROM emby_search_rebuild_state WHERE token = :token"),
                {"token": token},
            )
            uow.commit()
        return max(int(getattr(result, "rowcount", 0) or 0), 0)

    def abort_rebuild(self, token: str) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("DELETE FROM emby_search_rebuild_state WHERE token = :token"),
                {"token": token},
            )
            uow.commit()

    def delete_items(self, item_ids: Iterable[object]) -> int:
        normalized: list[int] = []
        for value in item_ids:
            try:
                item_id = int(value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if item_id > 0 and item_id not in normalized:
                normalized.append(item_id)
        if not normalized:
            return 0
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text("DELETE FROM emby_search_index WHERE emby_item_id = ANY(:item_ids)"),
                {"item_ids": normalized},
            )
            uow.commit()
        return max(int(getattr(result, "rowcount", 0) or 0), 0)

    def search(self, query: str, item_types: Sequence[str], limit: int) -> dict[str, Any]:
        compact = _compact(query)
        if not compact:
            return {"ready": True, "items": []}
        full = _pinyin(compact, Style.NORMAL) or compact
        pinyin_enabled = re.search(r"[\u3400-\u9fff]", compact) is None
        normalized_types = [str(value).strip() for value in item_types if str(value).strip()]
        bounded_limit = max(1, min(int(limit or 300), 500))
        with self._unit_of_work() as uow:
            state = (
                uow.session.execute(
                    text(
                        """
                        SELECT EXISTS(SELECT 1 FROM emby_search_index LIMIT 1) AS populated,
                               EXISTS(
                                   SELECT 1 FROM emby_search_rebuild_state
                                   WHERE started_at >= NOW() - INTERVAL '30 minutes'
                               ) AS rebuilding,
                               EXISTS(
                                   SELECT 1 FROM emby_search_index
                                   WHERE updated_at < (
                                       SELECT MIN(started_at)
                                       FROM emby_search_rebuild_state
                                       WHERE started_at >= NOW() - INTERVAL '30 minutes'
                                   )
                               ) AS has_previous_index
                        """
                    )
                )
                .mappings()
                .one()
            )
            if not state["populated"] or (state["rebuilding"] and not state["has_previous_index"]):
                return {"ready": False, "items": []}
            rows = [
                dict(row)
                for row in uow.session.execute(
                    text(
                        """
                        SELECT emby_item_id, item_type, title, series_name,
                               CASE
                                   WHEN item_type = 'Movie' THEN 0
                                   WHEN item_type = 'Series' THEN 1
                                   WHEN item_type = 'BoxSet' THEN 2
                                   WHEN item_type IN ('MusicAlbum', 'Audio', 'MusicVideo') THEN 3
                                   WHEN item_type = 'Person' THEN 4
                                   WHEN item_type = 'MusicArtist' THEN 5
                                   ELSE 6
                               END AS type_priority,
                               CASE
                                   WHEN search_compact = :compact THEN 1000
                                   WHEN search_compact LIKE :compact || '%' THEN 900
                                   WHEN POSITION(:compact IN search_compact) > 0 THEN 800
                                   WHEN :pinyin_enabled AND pinyin_full = :full THEN 700
                                   WHEN :pinyin_enabled AND pinyin_full LIKE :full || '%' THEN 600
                                   WHEN :pinyin_enabled AND POSITION(:full IN pinyin_full) > 0 THEN 500
                                   WHEN :pinyin_enabled AND :initials = ANY(
                                       string_to_array(pinyin_initials, '|')
                                   ) THEN 450
                                   WHEN :pinyin_enabled AND EXISTS (
                                       SELECT 1
                                       FROM unnest(string_to_array(pinyin_initials, '|')) AS variant
                                       WHERE POSITION(:initials IN variant) > 0
                                   ) THEN 400
                                   ELSE 300
                               END - CHAR_LENGTH(search_compact) AS rank
                        FROM emby_search_index
                        WHERE (CAST(:item_types AS text[]) IS NULL OR item_type = ANY(CAST(:item_types AS text[])))
                          AND (
                              search_ngrams @> CAST(:search_grams AS text[])
                              OR (
                                  :pinyin_enabled
                                  AND (
                                      pinyin_ngrams @> CAST(:full_grams AS text[])
                                      OR pinyin_ngrams @> CAST(:initial_grams AS text[])
                                  )
                              )
                          )
                          AND (
                              POSITION(:compact IN search_compact) > 0
                              OR (
                                  :pinyin_enabled
                                  AND (
                                      POSITION(:full IN pinyin_full) > 0
                                      OR EXISTS (
                                          SELECT 1
                                          FROM unnest(string_to_array(pinyin_initials, '|')) AS variant
                                          WHERE POSITION(:initials IN variant) > 0
                                      )
                                  )
                              )
                          )
                        ORDER BY type_priority, rank DESC, title, emby_item_id
                        LIMIT :query_limit
                        """
                    ),
                    {
                        "compact": compact,
                        "pinyin_enabled": pinyin_enabled,
                        "full": full,
                        "initials": compact,
                        "item_types": normalized_types or None,
                        "search_grams": _ngrams(compact),
                        "full_grams": _ngrams(full),
                        "initial_grams": _ngrams(compact),
                        "query_limit": min(bounded_limit * 2, 500),
                    },
                ).mappings()
            ]
        visible = _hide_series_children(rows)[:bounded_limit]
        return {
            "ready": True,
            "items": [
                {
                    "id": int(row["emby_item_id"]),
                    "type": str(row["item_type"]),
                    "title": str(row["title"]),
                    "rank": int(row["rank"]),
                }
                for row in visible
            ],
        }


class EmbySearchIndexService:
    def __init__(self, repository: EmbySearchRepository) -> None:
        self._repository = repository

    def search(self, query: str, item_types: Sequence[str], limit: int) -> ServiceResult:
        normalized_query = str(query or "").strip()
        if not normalized_query:
            return ServiceResult({"ready": True, "items": []})
        try:
            result = self._repository.search(normalized_query, item_types, limit)
        except Exception as exc:
            logger.warning("ETK Emby search failed: %s", exc)
            return ServiceResult({"ready": False, "items": []}, 503)
        return ServiceResult(result, headers={"Cache-Control": "no-store"})

    def update(self, payload: Mapping[str, Any]) -> ServiceResult:
        mode = str(payload.get("mode") or "incremental").strip().lower()
        token = str(payload.get("token") or "").strip()
        if token and len(token) > 100:
            return ServiceResult({"ok": False, "error": "invalid token"}, 400)
        try:
            if mode == "start":
                if not token:
                    return ServiceResult({"ok": False, "error": "token is required"}, 400)
                self._repository.start_rebuild(token)
                return ServiceResult({"ok": True})
            if mode == "complete":
                if not token:
                    return ServiceResult({"ok": False, "error": "token is required"}, 400)
                return ServiceResult({"ok": True, "deleted": self._repository.complete_rebuild(token)})
            if mode == "abort":
                if token:
                    self._repository.abort_rebuild(token)
                return ServiceResult({"ok": True})
            if mode not in {"batch", "incremental", "delete"}:
                return ServiceResult({"ok": False, "error": "invalid mode"}, 400)
            items = payload.get("items") or []
            if not isinstance(items, list) or len(items) > 1000:
                return ServiceResult({"ok": False, "error": "invalid items"}, 400)
            if mode == "delete":
                deleted = self._repository.delete_items(item.get("id") for item in items if isinstance(item, Mapping))
                return ServiceResult({"ok": True, "deleted": deleted})
            rows = [row for row in (_prepare_item(item) for item in items if isinstance(item, Mapping)) if row]
            return ServiceResult({"ok": True, "indexed": self._repository.upsert_items(rows)})
        except Exception as exc:
            logger.warning("ETK Emby search index update failed: %s", exc)
            return ServiceResult({"ok": False, "error": "search index update failed"}, 503)


def _normalize(value: object) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).casefold().strip()


def _compact(value: object) -> str:
    return _COMPACT_RE.sub("", _normalize(value))[:512]


def _pinyin(value: str, style: Style) -> str:
    values: list[str] = []
    for part in pinyin(value, style=style, strict=False, errors="default"):
        if not part:
            continue
        item = str(part[0] or "")
        values.append(item[:1] if style == Style.FIRST_LETTER else item)
    return "".join(values).casefold()[:512]


def _pinyin_variants(value: str, style: Style) -> list[str]:
    variants = [_pinyin(value, style)]
    if style == Style.FIRST_LETTER:
        override = _PINYIN_INITIAL_OVERRIDES.get(value)
        if override and override not in variants:
            variants.append(override)
    return variants


def _ngrams(value: str) -> list[str]:
    compact = _compact(value)
    if not compact:
        return []
    grams: set[str] = set()
    for size in range(1, min(3, len(compact)) + 1):
        grams.update(compact[index : index + size] for index in range(len(compact) - size + 1))
    return sorted(grams)


def _prepare_item(item: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        item_id = int(item.get("id"))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    item_type = str(item.get("type") or "").strip()
    title = str(item.get("title") or "").strip()
    if item_id <= 0 or not item_type or not title:
        return None
    original_title = str(item.get("original_title") or "").strip()
    series_name = str(item.get("series_name") or "").strip()
    search_compact = _compact(" ".join((title, original_title, series_name)))
    pinyin_full = _pinyin(search_compact, Style.NORMAL)
    initial_variants: list[str] = []
    for source in dict.fromkeys(value for value in (title, original_title, series_name) if value):
        initial_sources = [_compact(source)]
        initial_sources.extend(_compact(part) for part in _SEARCH_PART_RE.findall(_normalize(source)))
        for initial_source in dict.fromkeys(value for value in initial_sources if value):
            for variant in _pinyin_variants(initial_source, Style.FIRST_LETTER):
                if variant not in initial_variants:
                    initial_variants.append(variant)
    return {
        "emby_item_id": item_id,
        "item_type": item_type,
        "title": title,
        "original_title": original_title,
        "series_name": series_name,
        "search_compact": search_compact,
        "pinyin_full": pinyin_full,
        "pinyin_initials": "|".join(initial_variants),
        "search_ngrams": _ngrams(search_compact),
        "pinyin_ngrams": sorted(
            set(_ngrams(pinyin_full) + [gram for variant in initial_variants for gram in _ngrams(variant)])
        ),
    }


def _hide_series_children(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    if not any(row.get("item_type") == "Series" for row in rows):
        return list(rows)
    return [row for row in rows if row.get("item_type") not in {"Season", "Episode"}]


__all__ = ["EmbySearchIndexService", "SqlAlchemyEmbySearchRepository"]
