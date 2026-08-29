from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from etk.domains.operations.jobs import TaskBusyError


@dataclass(frozen=True, slots=True)
class DiscoverEffectsHttpResult:
    payload: Any
    status_code: int = 200


class DiscoverProviderPort(Protocol):
    def discover_movie(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None: ...

    def discover_tv(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None: ...

    def person_tv_credits(self, person_id: int) -> Mapping[str, Any] | None: ...

    def genres(self, media_type: str) -> Sequence[Mapping[str, Any]] | None: ...

    def search(
        self,
        *,
        query: str,
        media_type: str,
        page: int,
    ) -> Mapping[str, Any] | None: ...

    def tv_details(self, tv_id: int) -> Mapping[str, Any] | None: ...


class DiscoverProjection(Protocol):
    def enrich(
        self,
        payload: Mapping[str, Any],
        *,
        item_type: str,
        hide_in_library: bool,
    ) -> dict[str, Any]: ...

    def enrich_tv_seasons(self, tv_id: str, payload: Mapping[str, Any]) -> dict[str, Any]: ...


class DiscoverEffectsSettings(Protocol):
    def load_many(self, keys: Sequence[str]) -> Mapping[str, Any]: ...


class DiscoverEffectsDefaults(Protocol):
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]: ...

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]: ...

    def rating_mapping(self) -> Mapping[str, Sequence[Mapping[str, Any]]]: ...


class DiscoverJobPort(Protocol):
    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any: ...


class DiscoverEffectsService:
    _DISCOVER_FIELDS = frozenset(
        {
            "sort_by",
            "page",
            "vote_average.gte",
            "with_genres",
            "with_keywords",
            "with_companies",
            "without_genres",
            "primary_release_date.gte",
            "primary_release_date.lte",
            "first_air_date.gte",
            "first_air_date.lte",
            "with_original_language",
            "with_origin_country",
            "with_cast",
            "with_crew",
            "with_rating_label",
            "hide_in_library",
        }
    )
    _SEARCH_FIELDS = frozenset({"query", "media_type", "page", "hide_in_library"})
    _POLICY_KEYS = ("keyword_mapping", "studio_mapping", "rating_mapping")
    _TARGET_COUNT = 20
    _SCAN_TIMEOUT_SECONDS = 20.0

    def __init__(
        self,
        provider: DiscoverProviderPort,
        projection: DiscoverProjection,
        settings: DiscoverEffectsSettings,
        defaults: DiscoverEffectsDefaults,
        jobs: DiscoverJobPort,
        *,
        include_adult: Callable[[], bool],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._provider = provider
        self._projection = projection
        self._settings = settings
        self._defaults = defaults
        self._jobs = jobs
        self._include_adult = include_adult
        self._clock = clock

    def discover(self, media_type: str, payload: Any) -> DiscoverEffectsHttpResult:
        if media_type not in {"movie", "tv"}:
            return DiscoverEffectsHttpResult({"status": "error", "message": "无效的媒体类型"}, 400)
        try:
            values = _object(payload, allowed=self._DISCOVER_FIELDS)
            frontend_page = _positive_page(values.get("page", 1))
            hide_in_library = _bool(values.get("hide_in_library"), default=False)
            policy = self._policy_snapshot()
            parameters, adult_search = self._discover_parameters(media_type, values, policy)
            person_items = self._tv_person_items(values) if media_type == "tv" else None
        except ProviderPayloadUnavailableError as exc:
            return DiscoverEffectsHttpResult({"status": "error", "message": str(exc)}, 502)
        except (TypeError, ValueError) as exc:
            return DiscoverEffectsHttpResult({"status": "error", "message": str(exc)}, 400)

        if person_items is not None and not person_items:
            return DiscoverEffectsHttpResult(
                {"results": [], "page": frontend_page, "total_pages": 0, "total_results": 0}
            )

        try:
            has_person_filter = person_items is not None
            needs_provider_intersection = bool(
                parameters.get("with_keywords")
                or parameters.get("with_companies")
                or parameters.get("with_networks")
                or values.get("with_rating_label")
            )
            if media_type == "tv" and has_person_filter and not needs_provider_intersection:
                return DiscoverEffectsHttpResult(
                    self._person_only_tv_result(
                        values,
                        person_items or {},
                        frontend_page=frontend_page,
                        hide_in_library=hide_in_library,
                        include_adult=adult_search,
                    )
                )
            return self._scan_provider(
                media_type,
                parameters,
                frontend_page=frontend_page,
                hide_in_library=hide_in_library,
                adult_search=adult_search,
                person_ids=set(person_items or {}) if has_person_filter else None,
            )
        except Exception:
            message = (
                "从 TMDb 获取电影数据失败，请检查网络或配置。" if media_type == "movie" else "从 TMDb 获取数据失败"
            )
            return DiscoverEffectsHttpResult({"status": "error", "message": message}, 500)

    def genres(self, media_type: str) -> DiscoverEffectsHttpResult:
        if media_type not in {"movie", "tv"}:
            return DiscoverEffectsHttpResult({"status": "error", "message": "无效的媒体类型"}, 400)
        try:
            genres = self._provider.genres(media_type)
            return DiscoverEffectsHttpResult(list(genres or ()))
        except Exception:
            return DiscoverEffectsHttpResult({"status": "error", "message": "获取类型列表失败"}, 500)

    def search(self, payload: Any) -> DiscoverEffectsHttpResult:
        try:
            values = _object(payload, allowed=self._SEARCH_FIELDS)
            query = str(values.get("query") or "").strip()
            if not query:
                raise ValueError("搜索词不能为空")
            media_type = str(values.get("media_type") or "movie").strip().casefold()
            if media_type not in {"movie", "tv"}:
                raise ValueError("无效的媒体类型")
            page = _positive_page(values.get("page", 1))
            hide_in_library = _bool(values.get("hide_in_library"), default=False)
        except (TypeError, ValueError) as exc:
            return DiscoverEffectsHttpResult({"status": "error", "message": str(exc)}, 400)
        try:
            raw = self._provider.search(query=query, media_type=media_type, page=page)
            if raw is None:
                raise RuntimeError("TMDb returned no search payload")
            return DiscoverEffectsHttpResult(
                self._projection.enrich(
                    raw,
                    item_type="Movie" if media_type == "movie" else "Series",
                    hide_in_library=hide_in_library,
                )
            )
        except Exception:
            return DiscoverEffectsHttpResult(
                {"status": "error", "message": "从 TMDb 搜索数据失败"},
                500,
            )

    def trigger_recommendation_update(
        self,
        *,
        requested_by_user_id: str | None,
        trace_id: str | None,
    ) -> DiscoverEffectsHttpResult:
        try:
            submitted = self._jobs.submit_task(
                "update-daily-theme",
                trigger_type="manual",
                requested_by_user_id=requested_by_user_id,
                trace_id=trace_id,
                max_attempts=2,
                reject_when_busy=True,
            )
        except TaskBusyError:
            return DiscoverEffectsHttpResult({"error": "后台有任务正在运行，请稍后再试。"}, 409)
        except Exception:
            return DiscoverEffectsHttpResult({"error": "启动任务失败"}, 500)
        projection = getattr(submitted, "projection", None)
        job = getattr(projection, "job", None)
        job_id = str(getattr(job, "id", "") or "").strip() or None
        if job_id is None:
            return DiscoverEffectsHttpResult({"error": "启动任务失败"}, 500)
        return DiscoverEffectsHttpResult(
            {
                "status": "ok",
                "task_key": "update-daily-theme",
                "job_id": job_id,
                "message": "更新任务已在后台启动。",
            },
            202,
        )

    def tv_details(self, tv_id: int) -> DiscoverEffectsHttpResult:
        if isinstance(tv_id, bool) or int(tv_id) < 1:
            return DiscoverEffectsHttpResult({"error": "无效的 TMDb ID"}, 400)
        try:
            raw = self._provider.tv_details(int(tv_id))
            if raw is None:
                raise RuntimeError("TMDb returned no TV details")
            return DiscoverEffectsHttpResult(self._projection.enrich_tv_seasons(str(tv_id), raw))
        except Exception:
            return DiscoverEffectsHttpResult({"error": "获取 TMDb TV 详情失败"}, 500)

    def _policy_snapshot(self) -> dict[str, Any]:
        values = dict(self._settings.load_many(self._POLICY_KEYS))
        return {
            "keyword_mapping": values.get("keyword_mapping") or self._defaults.keyword_mapping(),
            "studio_mapping": values.get("studio_mapping") or self._defaults.studio_mapping(),
            "rating_mapping": values.get("rating_mapping") or self._defaults.rating_mapping(),
        }

    def _discover_parameters(
        self,
        media_type: str,
        values: Mapping[str, Any],
        policy: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        item_type = "Movie" if media_type == "movie" else "Series"
        keywords = _expand_keyword_labels(values.get("with_keywords"), policy["keyword_mapping"])
        companies = _expand_studio_labels(
            values.get("with_companies"),
            policy["studio_mapping"],
            media_type=media_type,
        )
        parameters: dict[str, Any] = {
            "sort_by": values.get("sort_by", "popularity.desc"),
            "vote_average.gte": values.get("vote_average.gte", 0),
            "with_genres": values.get("with_genres", ""),
            "with_keywords": keywords,
            "without_genres": values.get("without_genres", ""),
            "with_original_language": values.get("with_original_language", ""),
            "with_origin_country": values.get("with_origin_country", ""),
        }
        if media_type == "movie":
            parameters.update(
                {
                    "with_companies": companies,
                    "primary_release_date.gte": values.get("primary_release_date.gte", ""),
                    "primary_release_date.lte": values.get("primary_release_date.lte", ""),
                    "with_cast": values.get("with_cast", ""),
                    "with_crew": values.get("with_crew", ""),
                }
            )
        else:
            parameters.update(
                {
                    "with_networks": companies,
                    "first_air_date.gte": values.get("first_air_date.gte", ""),
                    "first_air_date.lte": values.get("first_air_date.lte", ""),
                }
            )

        rating_label = str(values.get("with_rating_label") or "").strip()
        adult_labels = _adult_rating_labels(policy["rating_mapping"])
        adult_search = bool(self._include_adult() and rating_label in adult_labels)
        parameters["include_adult"] = "true" if adult_search else "false"
        if adult_search:
            if media_type == "movie":
                parameters.update({"certification_country": "US", "certification": "NC-17"})
            else:
                parameters.update({"watch_region": "US", "with_content_ratings": "TV-MA"})
        elif rating_label:
            parameters.update(_rating_parameters(rating_label, item_type, policy["rating_mapping"]))
        return (
            {key: value for key, value in parameters.items() if value is not None and value != ""},
            adult_search,
        )

    def _tv_person_items(self, values: Mapping[str, Any]) -> dict[str, dict[str, Any]] | None:
        credit_maps: list[dict[str, dict[str, Any]]] = []
        for raw, credit_type in ((values.get("with_cast"), "cast"), (values.get("with_crew"), "crew")):
            for person_id in _person_ids(raw):
                credits = self._provider.person_tv_credits(person_id)
                if credits is None:
                    raise ProviderPayloadUnavailableError(f"读取 TMDb 人物 {person_id} 的剧集作品失败")
                raw_items = credits.get(credit_type)
                items = raw_items if isinstance(raw_items, list) else []
                credit_maps.append(
                    {
                        str(item["id"]): dict(item)
                        for item in items
                        if isinstance(item, Mapping) and item.get("id") is not None
                    }
                )
        if not credit_maps:
            return None
        matched = set.intersection(*(set(items) for items in credit_maps))
        return {
            identifier: next(items[identifier] for items in credit_maps if identifier in items)
            for identifier in matched
        }

    def _scan_provider(
        self,
        media_type: str,
        parameters: Mapping[str, Any],
        *,
        frontend_page: int,
        hide_in_library: bool,
        adult_search: bool,
        person_ids: set[str] | None,
    ) -> DiscoverEffectsHttpResult:
        max_pages = 10 if person_ids is not None else (3 if adult_search or hide_in_library else 1)
        start_page = (frontend_page - 1) * max_pages + 1
        raw_target = self._TARGET_COUNT * max_pages if hide_in_library else self._TARGET_COUNT
        results: list[dict[str, Any]] = []
        total_pages = 0
        started = self._clock()
        for offset in range(max_pages):
            if self._clock() - started > self._SCAN_TIMEOUT_SECONDS:
                break
            page = start_page + offset
            request_values = {**parameters, "page": page}
            raw = (
                self._provider.discover_movie(request_values)
                if media_type == "movie"
                else self._provider.discover_tv(request_values)
            )
            if raw is None:
                label = "电影" if media_type == "movie" else "电视剧"
                return DiscoverEffectsHttpResult(
                    {
                        "status": "error",
                        "message": f"从 TMDb 获取{label}数据失败，请检查网络、代理或 TMDb API Key。",
                    },
                    502,
                )
            total_pages = _nonnegative_int(raw.get("total_pages"))
            raw_results = raw.get("results")
            if isinstance(raw_results, list):
                results.extend(
                    dict(item)
                    for item in raw_results
                    if isinstance(item, Mapping)
                    and item.get("poster_path")
                    and (person_ids is None or str(item.get("id")) in person_ids)
                )
            if len(results) >= raw_target or page >= total_pages:
                break
        results = results[:raw_target]
        adjusted_pages = total_pages
        if max_pages > 1:
            adjusted_pages = total_pages // max_pages
            if adjusted_pages == 0 and total_pages > 0:
                adjusted_pages = 1
        enriched = self._projection.enrich(
            {
                "results": results,
                "page": frontend_page,
                "total_pages": adjusted_pages,
                "total_results": len(results),
            },
            item_type="Movie" if media_type == "movie" else "Series",
            hide_in_library=hide_in_library,
        )
        if hide_in_library:
            enriched["results"] = list(enriched.get("results") or [])[: self._TARGET_COUNT]
            enriched["total_results"] = len(enriched["results"])
        return DiscoverEffectsHttpResult(enriched)

    def _person_only_tv_result(
        self,
        values: Mapping[str, Any],
        person_items: Mapping[str, Mapping[str, Any]],
        *,
        frontend_page: int,
        hide_in_library: bool,
        include_adult: bool,
    ) -> dict[str, Any]:
        items = _filter_tv_credit_items(list(person_items.values()), values, include_adult=include_adult)
        page_size = self._TARGET_COUNT * (3 if hide_in_library else 1)
        start = (frontend_page - 1) * page_size
        page_items = items[start : start + page_size]
        total_pages = (len(items) + page_size - 1) // page_size
        enriched = self._projection.enrich(
            {
                "results": page_items,
                "page": frontend_page,
                "total_pages": total_pages,
                "total_results": len(items),
            },
            item_type="Series",
            hide_in_library=hide_in_library,
        )
        if hide_in_library:
            enriched["results"] = list(enriched.get("results") or [])[: self._TARGET_COUNT]
            enriched["total_results"] = len(enriched["results"])
        return enriched


class ProviderPayloadUnavailableError(RuntimeError):
    pass


def _object(payload: Any, *, allowed: frozenset[str]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise TypeError("请求参数必须是对象")
    return {str(key): value for key, value in payload.items() if str(key) in allowed}


def _positive_page(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("页码无效")
    try:
        page = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("页码无效") from exc
    if page < 1 or page > 500:
        raise ValueError("页码无效")
    return page


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    normalized = str(value).strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("布尔参数无效")


def _string_values(value: Any) -> list[str]:
    raw = value if isinstance(value, list | tuple | set) else str(value or "").split(",")
    return [str(item).strip() for item in raw if str(item).strip()]


def _mapping_by_label(value: Any) -> dict[str, Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return {str(label): item for label, item in value.items() if isinstance(item, Mapping)}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return {str(item["label"]): item for item in value if isinstance(item, Mapping) and item.get("label")}
    return {}


def _expand_keyword_labels(value: Any, mapping: Any) -> str:
    by_label = _mapping_by_label(mapping)
    groups: list[str] = []
    for label in _string_values(value):
        item = by_label.get(label)
        identifiers = item.get("ids") if item is not None else None
        if isinstance(identifiers, list | tuple) and identifiers:
            groups.append("|".join(str(identifier) for identifier in identifiers))
        elif label.isdigit():
            groups.append(label)
    return ",".join(groups)


def _expand_studio_labels(value: Any, mapping: Any, *, media_type: str) -> str:
    by_label = _mapping_by_label(mapping)
    identifiers: list[str] = []
    for label in _string_values(value):
        item = by_label.get(label)
        if item is None:
            values = [label] if label.isdigit() else []
        else:
            key = "company_ids" if media_type == "movie" else "network_ids"
            raw = item.get(key) or item.get("ids") or []
            values = list(raw) if isinstance(raw, list | tuple) else []
        identifiers.extend(str(identifier) for identifier in values)
    return "|".join(dict.fromkeys(identifiers))


def _adult_rating_labels(mapping: Any) -> set[str]:
    labels: set[str] = set()
    if isinstance(mapping, Mapping):
        for rules in mapping.values():
            if not isinstance(rules, list | tuple):
                continue
            labels.update(
                str(rule["label"])
                for rule in rules
                if isinstance(rule, Mapping) and rule.get("label") and rule.get("emby_value") == 15
            )
    return labels or {"成人"}


def _rating_parameters(label: str, item_type: str, mapping: Any) -> dict[str, str]:
    rules = mapping.get("US") if isinstance(mapping, Mapping) else None
    codes = [
        str(rule["code"])
        for rule in rules or ()
        if isinstance(rule, Mapping) and rule.get("label") == label and rule.get("code")
    ]
    if not codes:
        return {}
    joined = "|".join(codes)
    if item_type == "Movie":
        return {"certification_country": "US", "certification": joined}
    return {"watch_region": "US", "with_content_ratings": joined}


def _person_ids(value: Any) -> list[int]:
    raw = value if isinstance(value, list | tuple | set) else str(value or "").replace("|", ",").split(",")
    return [int(item) for item in raw if str(item).strip().isdigit()]


def _tokens(value: Any) -> set[str]:
    return {token.strip() for token in str(value or "").replace("|", ",").split(",") if token.strip()}


def _filter_tv_credit_items(
    items: Sequence[Mapping[str, Any]],
    values: Mapping[str, Any],
    *,
    include_adult: bool,
) -> list[dict[str, Any]]:
    required_genres = _tokens(values.get("with_genres"))
    excluded_genres = _tokens(values.get("without_genres"))
    required_countries = _tokens(values.get("with_origin_country"))
    required_language = str(values.get("with_original_language") or "").strip()
    date_gte = str(values.get("first_air_date.gte") or "").strip()
    date_lte = str(values.get("first_air_date.lte") or "").strip()
    try:
        vote_gte = float(values.get("vote_average.gte") or 0)
    except (TypeError, ValueError):
        vote_gte = 0
    filtered: list[dict[str, Any]] = []
    for value in items:
        item = dict(value)
        genres = {str(identifier) for identifier in item.get("genre_ids", [])}
        countries = {str(country) for country in item.get("origin_country", [])}
        first_air_date = str(item.get("first_air_date") or "")
        if not item.get("poster_path") or (not include_adult and item.get("adult")):
            continue
        if required_genres and not required_genres.issubset(genres):
            continue
        if excluded_genres.intersection(genres):
            continue
        if required_countries and not required_countries.intersection(countries):
            continue
        if required_language and item.get("original_language") != required_language:
            continue
        if date_gte and (not first_air_date or first_air_date < date_gte):
            continue
        if date_lte and (not first_air_date or first_air_date > date_lte):
            continue
        if float(item.get("vote_average") or 0) < vote_gte:
            continue
        filtered.append(item)
    sort_by = str(values.get("sort_by") or "popularity.desc")
    sort_field, _, direction = sort_by.partition(".")
    if sort_field not in {"popularity", "vote_average", "first_air_date"}:
        sort_field = "popularity"
    filtered.sort(
        key=lambda item: item.get(sort_field) or ("" if sort_field == "first_air_date" else 0),
        reverse=direction != "asc",
    )
    return filtered


def _nonnegative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "DiscoverEffectsDefaults",
    "DiscoverEffectsHttpResult",
    "DiscoverEffectsService",
    "DiscoverEffectsSettings",
    "DiscoverJobPort",
    "DiscoverProjection",
    "DiscoverProviderPort",
]
