from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

import httpx
from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .reprocess_metadata import ReprocessIdentificationError, identify_root, prepare_metadata


class IdentificationRepository(Protocol):
    def source(self, item_id: str, job_id: str | None) -> Mapping[str, Any] | None: ...
    def bindings(self, item: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]: ...


class SqlAlchemyIdentificationRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def source(self, item_id: str, job_id: str | None) -> Mapping[str, Any] | None:
        if job_id:
            try:
                job_id = str(UUID(job_id))
            except ValueError as exc:
                raise ReprocessIdentificationError("INVALID_JOB", "任务 ID 无效。") from exc
        with self._unit_of_work() as uow:
            row = uow.session.execute(text("""
                SELECT j.id, j.status, j.request_payload_json->'parameters' AS parameters,
                       COALESCE(j.result_json, a.error_details_json->'task_outcome', '{}'::jsonb) AS outcome,
                       j.error_summary_json->>'message' AS error_message
                FROM job_runs j
                LEFT JOIN LATERAL (
                    SELECT error_details_json FROM job_attempts WHERE job_id=j.id
                    ORDER BY attempt_no DESC LIMIT 1
                ) a ON TRUE
                WHERE j.task_key IN ('native-operation:media.reprocess-item', 'p8-review-reprocess-one')
                  AND COALESCE(j.request_payload_json->'parameters'->>'item_id',
                               j.request_payload_json->'parameters'->>'emby_item_id',
                               j.result_json->'details'->>'item_id',
                               a.error_details_json->'task_outcome'->'details'->>'item_id') = :item_id
                  AND (CAST(:job_id AS text) IS NULL OR j.id::text = :job_id)
                ORDER BY j.created_at DESC LIMIT 1
            """), {"item_id": item_id, "job_id": job_id}).mappings().first()
        return dict(row) if row else None

    def bindings(self, item: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            return tuple(dict(row) for row in uow.session.execute(text("""
                SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number
                FROM media_metadata
                WHERE emby_item_ids_json @> CAST(:ids AS jsonb)
                   OR (:path <> '' AND EXISTS (
                       SELECT 1 FROM jsonb_array_elements(COALESCE(asset_details_json, '[]'::jsonb)) a
                       WHERE a->>'path' = :path
                   ))
            """), {"ids": json.dumps([str(item.get("Id"))]), "path": str(item.get("Path") or "")}).mappings())


def tmdb_id(item: Mapping[str, Any]) -> str:
    ids = item.get("ProviderIds") or {}
    return str(ids.get("Tmdb") or ids.get("tmdb") or "").strip()


def identity_snapshot(item: Mapping[str, Any], root: Mapping[str, Any]) -> str:
    fields = ("Id", "Type", "Name", "ProductionYear", "SeriesId", "SeasonId", "IndexNumber", "ParentIndexNumber",
              "ProviderIds", "Path")
    value = [{key: source.get(key) for key in fields} for source in (item, root)]
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def selection_value(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"tmdb_id", "media_type", "snapshot"}:
        raise ReprocessIdentificationError("INVALID_SELECTION", "选片参数不完整，请重新打开选片窗口。")
    result = dict(value)
    if (
        not all(isinstance(v, str) for v in result.values())
        or not result["tmdb_id"].isascii() or not result["tmdb_id"].isdigit()
        or not 0 < int(result["tmdb_id"]) < 10**12
        or result["media_type"] not in {"movie", "tv"}
        or len(result["snapshot"]) != 64
        or any(c not in "0123456789abcdef" for c in result["snapshot"])
    ):
        raise ReprocessIdentificationError("INVALID_SELECTION", "TMDb 身份或确认快照无效。")
    return result


class ReprocessIdentificationService:
    """只读确认上下文；写入仍由原有重处理任务负责。"""

    def __init__(self, catalog: Any, provider: Any, repository: IdentificationRepository) -> None:
        self.catalog = catalog
        self.provider = provider
        self.repository = repository

    def context(self, item_id: str, job_id: str | None) -> dict[str, Any] | None:
        source = self.repository.source(item_id, job_id)
        if source is None:
            if job_id:
                raise ReprocessIdentificationError("SOURCE_NOT_FOUND", "未找到对应媒体的来源任务。", 404)
            return None
        if source["status"] not in {"succeeded", "failed", "skipped", "cancelled"}:
            raise ReprocessIdentificationError("SOURCE_RUNNING", "来源任务尚未结束，请稍后重试。", 409)
        outcome = source.get("outcome") or {}
        details = outcome.get("details") or {}
        previous = (source.get("parameters") or {}).get("tmdb_selection")
        code = details.get("error_code") or (details.get("metadata") or {}).get("error_code")
        legacy = not code and (
            details.get("needs_identification")
            or any(s in str(outcome.get("message") or source.get("error_message") or "") for s in (
                "TMDb 候选过多", "TMDb 没有唯一的片名及年份匹配",
            ))
        )
        if not previous and code != "TMDB_AMBIGUOUS" and not legacy:
            return None
        item = self.catalog.item(item_id)
        if not item or str(item.get("Id")) != item_id:
            raise ReprocessIdentificationError("ITEM_NOT_FOUND", "Emby 目标不存在。", 404)
        root = item
        if item.get("Type") in {"Season", "Episode"}:
            root = self.catalog.item(str(item.get("SeriesId") or "")) if item.get("SeriesId") else None
        if not isinstance(root, Mapping):
            raise ReprocessIdentificationError("INVALID_TARGET", "无法定位所属根剧集。")
        kind = {"Movie": "movie", "Series": "tv"}.get(str(root.get("Type")))
        if kind is None or (item.get("Type") in {"Season", "Episode"} and root.get("Id") != item.get("SeriesId")):
            raise ReprocessIdentificationError("INVALID_TARGET", "无法确定目标类型或所属根剧集。")
        current = tmdb_id(root)
        if previous and current and current != previous["tmdb_id"]:
            raise ReprocessIdentificationError("IDENTITY_CONFLICT", "目标已绑定其他身份；本功能不执行身份纠错。", 409)
        if current:
            return None
        if legacy and not previous:
            try:
                identify_root(root, self.provider)
            except ReprocessIdentificationError as exc:
                if exc.code != "TMDB_AMBIGUOUS":
                    return None
            except ValueError:
                return None
            else:
                return None
        return {"item": item, "root": root, "media_type": kind, "source_job_id": str(source["id"])}

    def preview(
        self, item_id: str, *, job_id: str | None = None, query: str | None = None,
        year: int | None = None, page: int = 1, manual_id: str | None = None, context_only: bool = False,
    ) -> dict[str, Any]:
        context = self.context(item_id, job_id)
        if context is None:
            return {"requires_selection": False}
        item, root, kind = context["item"], context["root"], context["media_type"]
        name = str(root.get("Name") or "").strip()
        if query is None:
            query = re.sub(r"\s*[（(]\d{4}[）)]\s*$", "", name)
            year = root.get("ProductionYear")
        else:
            query = query.strip()
        result = {
            "requires_selection": True, "source_job_id": context["source_job_id"],
            "snapshot": identity_snapshot(item, root),
            "target": {"item_id": item_id, "root_item_id": str(root["Id"]), "name": str(item.get("Name") or ""),
                       "root_name": name, "media_type": kind, "year": root.get("ProductionYear"),
                       "path": str(item.get("Path") or "")},
            "candidates": [], "page": page, "total_pages": 0, "query": query,
        }
        if context_only:
            return result
        if not query and not manual_id:
            raise ReprocessIdentificationError("EMPTY_QUERY", "请输入搜索关键词或 TMDb ID。")
        if manual_id:
            rows, total_pages = [self.details(manual_id, kind)], 1
        else:
            response = self.provider.search(query, kind, year=year, page=page)
            rows, total_pages = response.get("results") or [], int(response.get("total_pages") or 0)
        return {**result, "total_pages": min(total_pages, 500),
                "candidates": [self.candidate(row, kind) for row in rows if isinstance(row, Mapping) and row.get("id")]}

    def details(self, identifier: str, kind: str) -> Mapping[str, Any]:
        if not identifier.isascii() or not identifier.isdigit() or not 0 < int(identifier) < 10**12:
            raise ReprocessIdentificationError("INVALID_TMDB_ID", "TMDb ID 必须为有效正整数。")
        try:
            data = self.provider.details(identifier, kind)
        except RuntimeError as exc:
            cause = exc.__cause__
            if isinstance(cause, httpx.HTTPStatusError) and cause.response.status_code == 404:
                raise ReprocessIdentificationError("INVALID_TMDB_ID", "该媒体类型下不存在此 TMDb ID。") from exc
            raise
        if str(data.get("id")) != identifier or not data.get("title" if kind == "movie" else "name"):
            raise ReprocessIdentificationError("INVALID_TMDB_ID", "TMDb 详情身份或媒体类型不匹配。")
        return data

    def validate(self, item_id: str, job_id: str, selection: Any) -> dict[str, Any]:
        choice = selection_value(selection)
        context = self.context(item_id, job_id)
        if context is None:
            raise ReprocessIdentificationError("IDENTITY_CHANGED", "目标身份或识别状态已变化，请重新确认。", 409)
        if choice["snapshot"] != identity_snapshot(context["item"], context["root"]):
            raise ReprocessIdentificationError("IDENTITY_CHANGED", "目标身份已变化，请重新打开选片窗口确认。", 409)
        if choice["media_type"] != context["media_type"]:
            raise ReprocessIdentificationError("TYPE_MISMATCH", "所选作品类型与 Emby 目标不匹配。")
        self.details(choice["tmdb_id"], context["media_type"])
        self.check_bindings([context["item"], context["root"]], choice["tmdb_id"])
        initial_snapshot = identity_snapshot(context["root"], context["root"])
        context["root"] = {**context["root"], "ProviderIds": {
            **(context["root"].get("ProviderIds") or {}), "Tmdb": choice["tmdb_id"],
        }, "_newly_identified": True, "_confirmed_initial_snapshot": initial_snapshot}
        return context

    def check_bindings(
        self, items: Sequence[Mapping[str, Any]], root_id: str,
    ) -> dict[str, Sequence[Mapping[str, Any]]]:
        bindings = {}
        for item in {str(v.get("Id")): v for v in items}.values():
            rows = self.repository.bindings(item)
            bindings[str(item["Id"])] = rows
            for row in rows:
                bound = str(row.get("parent_series_tmdb_id") if row["item_type"] in {"Season", "Episode"}
                            else row["tmdb_id"])
                if bound != root_id or row["item_type"] != item.get("Type"):
                    raise ReprocessIdentificationError(
                        "CACHE_IDENTITY_CONFLICT", "已有缓存属于其他身份，未重新归属。", 409,
                    )
        return bindings

    def validate_scope(self, targets: Sequence[Mapping[str, Any]], root: Mapping[str, Any], cache: dict) -> None:
        bindings = self.check_bindings(targets, tmdb_id(root))
        for item in targets:
            rows = bindings[str(item["Id"])]
            if tmdb_id(item) or rows:
                _, data = prepare_metadata(item, root, self.provider, cache)
                if (tmdb_id(item) and tmdb_id(item) != str(data["id"])) or any(
                    str(row["tmdb_id"]) != str(data["id"]) for row in rows
                ):
                    raise ReprocessIdentificationError(
                        "IDENTITY_CONFLICT", "所选范围已有不同 TMDb 身份，未执行纠错。", 409,
                    )

    @staticmethod
    def candidate(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
        return {"tmdb_id": str(row["id"]), "media_type": kind,
                "title": row.get("title") or row.get("name") or "",
                "original_title": row.get("original_title") or row.get("original_name") or "",
                "year": str(row.get("release_date") or row.get("first_air_date") or "")[:4],
                "overview": str(row.get("overview") or "")[:1200], "poster_path": row.get("poster_path")}
