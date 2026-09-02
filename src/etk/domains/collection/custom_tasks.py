from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from uuid import UUID

from etk.platform.jobs.json_payload import canonical_json
from etk.platform.tasks import TaskOutcome

MediaType = Literal["Movie", "Series"]

CUSTOM_COLLECTION_BATCH_TASK_KEYS = frozenset({"custom-collections", "process_all_custom_collections"})
CUSTOM_COLLECTION_SOURCE_TYPES = frozenset({"list", "ai_recommendation_global"})


@dataclass(frozen=True, slots=True)
class CustomCollectionCandidate:
    tmdb_id: str | None
    media_type: MediaType
    title: str | None
    season: int | None = None
    year: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedCustomCollectionItem:
    tmdb_id: str | None
    media_type: MediaType
    title: str | None
    season: int | None
    emby_id: str | None
    season_tmdb_id: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "tmdb_id": self.tmdb_id,
            "media_type": self.media_type,
            "emby_id": self.emby_id,
            "title": self.title,
        }
        if self.season is not None:
            value["season"] = self.season
        if self.season_tmdb_id is not None:
            value["season_tmdb_id"] = self.season_tmdb_id
        return value


@dataclass(frozen=True, slots=True)
class CustomCollectionLibrarySnapshot:
    emby_ids: tuple[str, ...]
    total_count: int


@dataclass(frozen=True, slots=True)
class CustomCollectionRefreshPlan:
    collection_id: int
    collection_name: str
    collection_type: str
    definition_digest: str
    source_snapshot: Mapping[str, Any]
    desired_member_ids: tuple[str, ...]
    generated_items: tuple[Mapping[str, Any], ...]
    item_type: tuple[str, ...]
    in_library_count: int
    expected_emby_collection_id: str | None
    allow_empty: bool
    delete_empty_source: bool

    def to_request(self) -> dict[str, Any]:
        return {
            "collection_id": self.collection_id,
            "collection_name": self.collection_name,
            "collection_type": self.collection_type,
            "definition_digest": self.definition_digest,
            "source_snapshot": dict(self.source_snapshot),
            "desired_member_ids": list(self.desired_member_ids),
            "generated_items": [dict(item) for item in self.generated_items],
            "item_type": list(self.item_type),
            "in_library_count": self.in_library_count,
            "expected_emby_collection_id": self.expected_emby_collection_id,
            "allow_empty": self.allow_empty,
            "delete_empty_source": self.delete_empty_source,
        }


@dataclass(frozen=True, slots=True)
class CustomCollectionRefreshResult:
    status: Literal["confirmed", "pending", "rejected", "indeterminate"]
    message: str = ""
    details: Mapping[str, Any] = field(default_factory=dict)


class CustomCollectionTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class CustomCollectionSourcePort(Protocol):
    def fetch_list(self, definition: Mapping[str, Any]) -> Sequence[CustomCollectionCandidate]: ...

    def fetch_global_recommendation(self, definition: Mapping[str, Any]) -> Sequence[CustomCollectionCandidate]: ...


class CustomCollectionTmdbPort(Protocol):
    def series_details(self, tmdb_id: str) -> Mapping[str, Any]: ...


class CustomCollectionMediaIndexPort(Protocol):
    def lookup(
        self,
        *,
        tmdb_id: str,
        media_type: MediaType,
        season: int | None = None,
    ) -> str | None: ...


class CustomCollectionLibraryQueryPort(Protocol):
    def query_filter(
        self,
        definition: Mapping[str, Any],
        *,
        user_id: str,
        execution_key: str = "",
    ) -> CustomCollectionLibrarySnapshot: ...

    def query_personal_recommendation(
        self,
        definition: Mapping[str, Any],
        *,
        user_id: str,
        execution_key: str = "",
    ) -> CustomCollectionLibrarySnapshot: ...

    def query_placeholders(
        self,
        definition: Mapping[str, Any],
        *,
        user_id: str,
        execution_key: str,
        limit: int = 9,
    ) -> tuple[str, ...]: ...


class CustomCollectionRefreshOperationPort(Protocol):
    def apply(
        self,
        plan: CustomCollectionRefreshPlan,
        *,
        idempotency_key: str,
    ) -> CustomCollectionRefreshResult: ...


class CustomCollectionTaskRepository(Protocol):
    def list_active_for_execution(self) -> Sequence[Mapping[str, Any]]: ...

    def get_for_execution(self, collection_id: int) -> Mapping[str, Any] | None: ...


class CustomCollectionSourceRejected(RuntimeError):
    """The source explicitly rejected a request; never interpret this as an empty list."""


class CustomCollectionSourceIndeterminate(RuntimeError):
    """The source outcome is unknown; never delete an existing remote collection."""


class CustomCollectionTaskService:
    """Processor-free owner for the three custom-collection task aliases."""

    def __init__(
        self,
        repository: CustomCollectionTaskRepository,
        source: CustomCollectionSourcePort,
        tmdb: CustomCollectionTmdbPort,
        media_index: CustomCollectionMediaIndexPort,
        library_query: CustomCollectionLibraryQueryPort,
        operations: CustomCollectionRefreshOperationPort,
        *,
        default_emby_user_id: Callable[[], str] | None = None,
    ) -> None:
        self._repository = repository
        self._source = source
        self._tmdb = tmdb
        self._media_index = media_index
        self._library_query = library_query
        self._operations = operations
        self._default_emby_user_id = default_emby_user_id

    def execute_all(
        self,
        progress: CustomCollectionTaskProgress,
        *,
        job_id: UUID,
        emby_user_id: str = "",
    ) -> TaskOutcome:
        effective_user_id = self._effective_emby_user_id(emby_user_id)
        collections = [
            dict(value)
            for value in self._repository.list_active_for_execution()
            if isinstance(value, Mapping) and str(value.get("type") or "").strip() in CUSTOM_COLLECTION_SOURCE_TYPES
        ]
        if not collections:
            progress.update(100, "没有需要刷新的榜单或全局推荐合集。")
            return TaskOutcome.succeeded("没有需要刷新的榜单或全局推荐合集。", {"collections": 0})

        failures: list[dict[str, Any]] = []
        pending_details: list[dict[str, Any]] = []
        confirmed = 0
        for index, collection in enumerate(collections, 1):
            progress.checkpoint()
            progress.update(
                min(95, 5 + int((index - 1) / len(collections) * 90)),
                f"正在处理自建合集 ({index}/{len(collections)}): {collection.get('name', collection.get('id'))}",
            )
            try:
                result = self._execute_collection(collection, progress, job_id=job_id, emby_user_id=effective_user_id)
                if result.status == "confirmed":
                    confirmed += 1
                elif result.status == "pending":
                    pending_details.append(
                        {
                            "collection_id": collection.get("id"),
                            "status": result.status,
                            **dict(result.details),
                        }
                    )
                else:
                    failures.append({"collection_id": collection.get("id"), "status": result.status})
            except Exception as exc:
                failures.append(
                    {
                        "collection_id": collection.get("id"),
                        "error_type": type(exc).__name__,
                        "message": str(exc)[:300],
                    }
                )
        progress.update(
            100,
            "自建合集刷新完成。" if not failures and not pending_details else "自建合集刷新部分完成。",
        )
        details = {
            "collections": len(collections),
            "confirmed": confirmed,
            "pending": pending_details,
            "failed": failures,
        }
        return (
            TaskOutcome.partial("自建合集刷新部分完成。", details)
            if failures or pending_details
            else TaskOutcome.succeeded("自建合集刷新完成。", details)
        )

    def execute_one(
        self,
        collection_id: int,
        progress: CustomCollectionTaskProgress,
        *,
        job_id: UUID,
        emby_user_id: str = "",
    ) -> TaskOutcome:
        normalized_id = _positive_int(collection_id, "custom_collection_id")
        effective_user_id = self._effective_emby_user_id(emby_user_id)
        collection = self._repository.get_for_execution(normalized_id)
        if collection is None:
            raise ValueError(f"未找到ID为 {normalized_id} 的自定义合集。")
        progress.checkpoint()
        progress.update(10, "正在读取合集定义...")
        result = self._execute_collection(
            dict(collection),
            progress,
            job_id=job_id,
            emby_user_id=effective_user_id,
        )
        details = {"collection_id": normalized_id, "operation_status": result.status, **dict(result.details)}
        if result.status == "confirmed":
            return TaskOutcome.succeeded(result.message or "自建合集刷新完成。", details)
        if result.status == "pending":
            return TaskOutcome.partial(result.message or "自建合集刷新已提交，等待远端确认。", details)
        return TaskOutcome.failed(result.message or "自建合集刷新失败。", details)

    def _execute_collection(
        self,
        collection: Mapping[str, Any],
        progress: CustomCollectionTaskProgress,
        *,
        job_id: UUID,
        emby_user_id: str,
    ) -> CustomCollectionRefreshResult:
        collection_id = _positive_int(collection.get("id"), "collection_id")
        name = str(collection.get("name") or "").strip()
        collection_type = str(collection.get("type") or "").strip()
        definition = _mapping(collection.get("definition_json"))
        if not name or collection_type not in {"filter", "list", "ai_recommendation", "ai_recommendation_global"}:
            raise ValueError("custom collection definition is invalid")

        if collection_type in CUSTOM_COLLECTION_SOURCE_TYPES:
            progress.update(25, f"正在读取《{name}》的数据源...")
            candidates = self._fetch_source(collection_type, definition)
            source_snapshot = _source_snapshot(collection_type, candidates)
            resolved = self._resolve_source_items(candidates, collection_type, definition)
            generated = tuple(item.to_mapping() for item in resolved)
            desired = tuple(dict.fromkeys(item.emby_id for item in resolved if item.emby_id))
            if candidates and not desired:
                desired = self._library_query.query_placeholders(
                    definition,
                    user_id=emby_user_id,
                    execution_key=str(job_id),
                )
                source_snapshot = {
                    **dict(source_snapshot),
                    "placeholder_member_ids": list(desired),
                }
            in_library_count = len(desired)
            allow_empty = True
            delete_empty_source = not candidates
        elif collection_type == "filter":
            progress.update(25, f"正在查询《{name}》的筛选结果...")
            snapshot = self._library_query.query_filter(
                definition,
                user_id=emby_user_id,
                execution_key=str(job_id),
            )
            desired = tuple(dict.fromkeys(snapshot.emby_ids))
            generated = tuple({"emby_id": value} for value in desired)
            source_snapshot = _source_snapshot("filter", generated)
            in_library_count = max(0, int(snapshot.total_count))
            allow_empty = False
            delete_empty_source = False
        else:
            progress.update(25, f"正在查询《{name}》的推荐素材...")
            snapshot = self._library_query.query_personal_recommendation(
                definition,
                user_id=emby_user_id,
                execution_key=str(job_id),
            )
            desired = tuple(dict.fromkeys(snapshot.emby_ids))
            generated = tuple({"emby_id": value} for value in desired)
            source_snapshot = _source_snapshot("ai_recommendation", generated)
            in_library_count = 0
            allow_empty = True
            delete_empty_source = False

        plan = CustomCollectionRefreshPlan(
            collection_id=collection_id,
            collection_name=name,
            collection_type=collection_type,
            definition_digest=_definition_digest(definition),
            source_snapshot=source_snapshot,
            desired_member_ids=desired,
            generated_items=generated,
            item_type=_item_types(definition, generated),
            in_library_count=in_library_count,
            expected_emby_collection_id=(str(collection.get("emby_collection_id") or "").strip() or None),
            allow_empty=allow_empty,
            delete_empty_source=delete_empty_source,
        )
        progress.checkpoint()
        return self._operations.apply(plan, idempotency_key=f"{job_id}:custom-collection:{collection_id}")

    def _effective_emby_user_id(self, value: str) -> str:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
        if self._default_emby_user_id is not None:
            normalized = str(self._default_emby_user_id() or "").strip()
        if not normalized:
            raise ValueError("Emby user identity is required for custom collection execution")
        return normalized

    def _fetch_source(
        self,
        collection_type: str,
        definition: Mapping[str, Any],
    ) -> tuple[CustomCollectionCandidate, ...]:
        try:
            values = (
                self._source.fetch_list(definition)
                if collection_type == "list"
                else self._source.fetch_global_recommendation(definition)
            )
        except (CustomCollectionSourceRejected, CustomCollectionSourceIndeterminate):
            raise
        candidates: list[CustomCollectionCandidate] = []
        for value in values:
            candidate = _candidate(value)
            if candidate is not None:
                candidates.append(candidate)
        return tuple(candidates)

    def _resolve_source_items(
        self,
        candidates: Sequence[CustomCollectionCandidate],
        collection_type: str,
        definition: Mapping[str, Any],
    ) -> tuple[ResolvedCustomCollectionItem, ...]:
        corrections = definition.get("corrections")
        corrected = tuple(_apply_correction(candidate, corrections) for candidate in candidates)
        resolved: list[ResolvedCustomCollectionItem] = []
        for candidate in corrected:
            if not candidate.tmdb_id:
                continue
            if candidate.media_type == "Series" and candidate.season is None and collection_type == "list":
                try:
                    details = self._tmdb.series_details(candidate.tmdb_id)
                    seasons = details.get("seasons", [])
                except Exception:
                    details = {}
                    seasons = ()
                expanded = False
                for season in seasons if isinstance(seasons, list) else ():
                    if not isinstance(season, Mapping):
                        continue
                    number = _positive_int_or_none(season.get("season_number"))
                    if number is None:
                        continue
                    expanded = True
                    resolved.append(self._resolve(candidate, season=number, details=details))
                if expanded:
                    continue
            season = candidate.season
            if collection_type == "ai_recommendation_global" and candidate.media_type == "Series" and season is None:
                season = 1
            details = None
            if candidate.media_type == "Series" and season is not None:
                details = self._tmdb.series_details(candidate.tmdb_id)
            resolved.append(self._resolve(candidate, season=season, details=details))
        unique: list[ResolvedCustomCollectionItem] = []
        seen: set[tuple[str | None, str, int | None, str | None, str | None]] = set()
        for item in resolved:
            key = (item.tmdb_id, item.media_type, item.season, item.emby_id, item.season_tmdb_id)
            if key not in seen:
                seen.add(key)
                unique.append(item)
        return tuple(unique)

    def _resolve(
        self,
        candidate: CustomCollectionCandidate,
        *,
        season: int | None,
        details: Mapping[str, Any] | None = None,
    ) -> ResolvedCustomCollectionItem:
        emby_id = self._media_index.lookup(
            tmdb_id=str(candidate.tmdb_id),
            media_type=candidate.media_type,
            season=season,
        )
        season_tmdb_id = _season_tmdb_id(details, season) if candidate.media_type == "Series" else None
        return ResolvedCustomCollectionItem(
            candidate.tmdb_id,
            candidate.media_type,
            candidate.title,
            season,
            emby_id,
            season_tmdb_id,
        )


def custom_collection_refresh_plan_from_request(request: Mapping[str, Any]) -> CustomCollectionRefreshPlan:
    return CustomCollectionRefreshPlan(
        collection_id=_positive_int(request.get("collection_id"), "collection_id"),
        collection_name=str(request.get("collection_name") or "").strip(),
        collection_type=str(request.get("collection_type") or "").strip(),
        definition_digest=str(request.get("definition_digest") or "").strip(),
        source_snapshot=_mapping(request.get("source_snapshot")),
        desired_member_ids=tuple(_strings(request.get("desired_member_ids"))),
        generated_items=tuple(
            _mapping(value) for value in request.get("generated_items", []) if isinstance(value, Mapping)
        ),
        item_type=tuple(_strings(request.get("item_type"))),
        in_library_count=max(0, int(request.get("in_library_count") or 0)),
        expected_emby_collection_id=str(request.get("expected_emby_collection_id") or "").strip() or None,
        allow_empty=bool(request.get("allow_empty")),
        delete_empty_source=bool(request.get("delete_empty_source")),
    )


def _candidate(value: Any) -> CustomCollectionCandidate | None:
    if isinstance(value, CustomCollectionCandidate):
        return value
    if not isinstance(value, Mapping):
        return None
    raw_type = str(value.get("media_type") or value.get("type") or "").strip()
    media_type: MediaType = "Series" if raw_type.casefold() in {"series", "tv"} else "Movie"
    tmdb_id = str(value.get("tmdb_id") or value.get("id") or "").strip() or None
    season = _positive_int_or_none(value.get("season")) or _season_from_title(value.get("title") or value.get("name"))
    return CustomCollectionCandidate(
        tmdb_id,
        media_type,
        str(value.get("title") or value.get("name") or "").strip() or None,
        season,
        str(value.get("year") or "").strip() or None,
    )


def _apply_correction(candidate: CustomCollectionCandidate, corrections: Any) -> CustomCollectionCandidate:
    if not isinstance(corrections, Mapping):
        return candidate
    value = corrections.get(candidate.tmdb_id) if candidate.tmdb_id else None
    if value is None and candidate.title:
        value = corrections.get(f"title:{candidate.title}")
    if value is None:
        return candidate
    if isinstance(value, Mapping):
        tmdb_id = str(value.get("tmdb_id") or candidate.tmdb_id or "").strip() or None
        season = _positive_int_or_none(value.get("season")) if "season" in value else candidate.season
    else:
        tmdb_id = str(value).strip() or candidate.tmdb_id
        season = candidate.season
    return CustomCollectionCandidate(tmdb_id, candidate.media_type, candidate.title, season, candidate.year)


def _source_snapshot(kind: str, values: Sequence[Any]) -> dict[str, Any]:
    items = []
    for value in values:
        if isinstance(value, CustomCollectionCandidate):
            items.append(
                {
                    "tmdb_id": value.tmdb_id,
                    "media_type": value.media_type,
                    "title": value.title,
                    "season": value.season,
                    "year": value.year,
                }
            )
        elif isinstance(value, ResolvedCustomCollectionItem):
            items.append(value.to_mapping())
        elif isinstance(value, Mapping):
            items.append(dict(value))
    status = "empty" if not items else "ready"
    digest = hashlib.sha256(canonical_json({"kind": kind, "status": status, "items": items}).encode()).hexdigest()
    return {"kind": kind, "status": status, "digest": digest, "items": items}


def _definition_digest(definition: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(definition)).encode()).hexdigest()


def _item_types(definition: Mapping[str, Any], items: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    configured = definition.get("item_type")
    if isinstance(configured, list):
        values = tuple(dict.fromkeys(str(value).strip() for value in configured if str(value).strip()))
        if values:
            return values
    return tuple(dict.fromkeys(str(item.get("media_type") or "Movie") for item in items)) or ("Movie",)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    return [str(item).strip() for item in value if str(item).strip()] if isinstance(value, (list, tuple)) else []


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _positive_int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _season_from_title(value: Any) -> int | None:
    import re

    match = re.search(r"(?:第\s*)?(\d{1,3})\s*季\b|\bS(\d{1,3})\b", str(value or ""), re.IGNORECASE)
    if not match:
        return None
    return _positive_int_or_none(match.group(1) or match.group(2))


def _season_tmdb_id(details: Mapping[str, Any] | None, season: int | None) -> str | None:
    if details is None or season is None:
        return None
    seasons = details.get("seasons")
    if not isinstance(seasons, list):
        return None
    for value in seasons:
        if not isinstance(value, Mapping):
            continue
        if _positive_int_or_none(value.get("season_number")) != season:
            continue
        tmdb_id = str(value.get("id") or "").strip()
        return tmdb_id or None
    return None


__all__ = [
    "CUSTOM_COLLECTION_BATCH_TASK_KEYS",
    "CUSTOM_COLLECTION_SOURCE_TYPES",
    "CustomCollectionCandidate",
    "CustomCollectionLibraryQueryPort",
    "CustomCollectionLibrarySnapshot",
    "CustomCollectionMediaIndexPort",
    "CustomCollectionRefreshOperationPort",
    "CustomCollectionRefreshPlan",
    "CustomCollectionRefreshResult",
    "CustomCollectionSourceIndeterminate",
    "CustomCollectionSourcePort",
    "CustomCollectionSourceRejected",
    "CustomCollectionTaskProgress",
    "CustomCollectionTaskRepository",
    "CustomCollectionTaskService",
    "CustomCollectionTmdbPort",
    "ResolvedCustomCollectionItem",
    "custom_collection_refresh_plan_from_request",
]
