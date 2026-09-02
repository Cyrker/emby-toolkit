from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any, cast
from urllib.parse import parse_qs, quote, unquote, urlparse

import httpx
from sqlalchemy import bindparam, text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.policy_defaults import DEFAULT_KEYWORD_MAPPING, DEFAULT_RATING_MAPPING, DEFAULT_STUDIO_MAPPING

from .custom_recommendation import CustomCollectionRecommendationPort
from .custom_tasks import (
    CustomCollectionCandidate,
    CustomCollectionLibrarySnapshot,
    CustomCollectionRefreshOperationPort,
    CustomCollectionRefreshPlan,
    CustomCollectionRefreshResult,
    CustomCollectionSourceIndeterminate,
    CustomCollectionSourceRejected,
    MediaType,
)
from .operation_repository import PostgresCollectionOperationRepository
from .operations import CollectionOperationIntent, CollectionOperationKind, CollectionOperationStatus

_MAX_SOURCE_ITEMS = 10_000
_MAX_TMDB_LIST_PAGES = 500


class HttpxCustomCollectionSourceAdapter:
    """Native source adapter for the URL and local-library recommendation inputs."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        recommendations: CustomCollectionRecommendationPort | None = None,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._recommendations = recommendations
        self._client_factory = client_factory or self._default_client
        self._clock = clock

    def fetch_list(self, definition: Mapping[str, object]) -> Sequence[CustomCollectionCandidate]:
        urls = definition.get("url")
        values = urls if isinstance(urls, list) else [urls]
        limit = _positive_limit(definition.get("limit"))
        fetch_limit = limit or _MAX_SOURCE_ITEMS
        collected: list[list[CustomCollectionCandidate]] = []
        for value in values:
            url = str(value or "").strip()
            if not url:
                continue
            parsed = urlparse(url)
            if parsed.scheme == "maoyan":
                collected.append(self._maoyan(url, definition, limit=fetch_limit))
            elif "themoviedb.org/list/" in url:
                collected.append(self._tmdb_list(url, limit=fetch_limit))
            elif "themoviedb.org/discover/" in url:
                collected.append(self._tmdb_discover(url, limit=fetch_limit))
            elif "douban.com/doulist" in url:
                collected.append(self._douban(url, definition, limit=fetch_limit))
            else:
                collected.append(self._rss(url, definition, limit=fetch_limit))
        result = _unique_candidates(_round_robin(collected))
        return result[:limit] if limit is not None else result

    def _maoyan(
        self,
        url: str,
        definition: Mapping[str, object],
        *,
        limit: int,
    ) -> list[CustomCollectionCandidate]:
        if limit <= 0:
            return []
        content = url.split("://", 1)[1]
        platforms = {"all", "tencent", "iqiyi", "youku", "mango"}
        type_part, separator, suffix = content.rpartition("-")
        platform = suffix.casefold() if separator and suffix.casefold() in platforms else "all"
        if platform == "all":
            type_part = content
        types = {value.strip().casefold() for value in type_part.split(",") if value.strip()}
        if not types:
            raise CustomCollectionSourceRejected("maoyan source has no ranking type")
        api_base = "https://piaofang.maoyan.com"
        titles: list[tuple[str, str]] = []
        for rank_type in types:
            if rank_type == "movie":
                payload = self._json_get(f"{api_base}/dashboard-ajax/movie")
                movie_list = payload.get("movieList") if isinstance(payload, Mapping) else None
                values = movie_list.get("list", []) if isinstance(movie_list, Mapping) else []
                titles.extend(
                    (str(item.get("movieInfo", {}).get("movieName") or "").strip(), "Movie")
                    for item in values
                    if isinstance(item, Mapping) and str(item.get("movieInfo", {}).get("movieName") or "").strip()
                )
            elif rank_type in {"web-heat", "web-tv", "zongyi"}:
                code = {"web-heat": "0", "web-tv": "1", "zongyi": "2"}[rank_type]
                platform_code = {"all": "", "tencent": "3", "iqiyi": "2", "youku": "1", "mango": "7"}[platform]
                payload = self._json_get(
                    f"{api_base}/dashboard/webHeatData",
                    params={"seriesType": code, "platformType": platform_code, "showDate": "2"},
                )
                data_list = payload.get("dataList") if isinstance(payload, Mapping) else None
                values = data_list.get("list", []) if isinstance(data_list, Mapping) else []
                titles.extend(
                    (str(item.get("seriesInfo", {}).get("name") or "").strip(), "Series")
                    for item in values
                    if isinstance(item, Mapping) and str(item.get("seriesInfo", {}).get("name") or "").strip()
                )
        allowed = _item_types(definition)
        result: list[CustomCollectionCandidate] = []
        for title, source_type in titles:
            if source_type not in allowed:
                continue
            candidate = self._search_title(title, source_type)
            if candidate is not None:
                result.append(candidate)
            if len(result) >= limit:
                break
        return result[:limit]

    def _douban(
        self,
        url: str,
        definition: Mapping[str, object],
        *,
        limit: int,
    ) -> list[CustomCollectionCandidate]:
        if limit <= 0:
            return []
        allowed = _item_types(definition)
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        result: list[CustomCollectionCandidate] = []
        page_limit = min(1250, max(25, ((limit + 24) // 25) * 25))
        for start in range(0, page_limit, 25):
            response = self._get(base_url, params={"start": str(start), "sort": "seq", "playable": "0", "sub_type": ""})
            parser = _DoubanListParser()
            parser.feed(response.text)
            if not parser.items:
                break
            for title, year, subject_url in parser.items:
                for media_type in allowed:
                    imdb_id = self._douban_imdb_id(subject_url) if subject_url else None
                    candidate = self._candidate_from_imdb(imdb_id, media_type, title, year)
                    if candidate is None:
                        candidate = self._search_title(title, media_type, year=year)
                    if candidate is not None:
                        result.append(candidate)
                        break
                if len(result) >= limit:
                    return result[:limit]
        return result[:limit]

    def _douban_imdb_id(self, url: str) -> str | None:
        response = self._get(url, headers={"Accept": "text/html,application/xhtml+xml"})
        return _imdb_id_from_text(response.text)

    def fetch_global_recommendation(self, definition: Mapping[str, object]) -> Sequence[CustomCollectionCandidate]:
        if self._recommendations is None:
            raise RuntimeError("native custom collection recommendation service is unavailable")
        return self._recommendations.generate(definition, resolve_title=self._search_title)

    def _tmdb_list(self, url: str, *, limit: int) -> list[CustomCollectionCandidate]:
        if limit <= 0:
            return []
        path = urlparse(url).path.rstrip("/").split("/")
        list_id = path[-1] if path else ""
        result: list[CustomCollectionCandidate] = []
        page = 1
        while len(result) < limit and page <= _MAX_TMDB_LIST_PAGES:
            payload = self._tmdb(f"/list/{quote(list_id, safe='')}", {"page": page})
            items = payload.get("items") if isinstance(payload, Mapping) else None
            if not isinstance(items, list):
                raise CustomCollectionSourceRejected("TMDb list response is invalid")
            if not items:
                break
            for item in items:
                candidate = _tmdb_candidate(item)
                if candidate is not None:
                    result.append(candidate)
            page += 1
            if page > _positive_int(payload.get("total_pages"), default=page - 1):
                break
        return result[:limit]

    def _tmdb_discover(self, url: str, *, limit: int) -> list[CustomCollectionCandidate]:
        url = _resolve_dynamic_dates(unquote(url), today=self._clock().date())
        parsed = urlparse(url)
        media_type = "Series" if "/discover/tv" in parsed.path else "Movie"
        params = {key: values[0] for key, values in parse_qs(parsed.query).items() if values}
        person_items: dict[str, Mapping[str, object]] | None = None
        if media_type == "Series":
            person_items = self._tv_person_items(
                params.pop("with_cast", None),
                params.pop("with_crew", None),
            )
            if person_items == {}:
                return []
            local_filters = {
                "sort_by",
                "page",
                "first_air_date.gte",
                "first_air_date.lte",
                "with_genres",
                "without_genres",
                "with_origin_country",
                "with_original_language",
                "vote_average.gte",
                "vote_count.gte",
                "include_adult",
                "language",
            }
            has_remote_only_filter = any(
                value not in (None, "") and key not in local_filters for key, value in params.items()
            )
            if person_items is not None and not has_remote_only_filter:
                return [
                    candidate
                    for item in _filter_tv_credit_items(tuple(person_items.values()), params)
                    if (candidate := _tmdb_candidate(item, media_type="Series")) is not None
                ][:limit]
        result: list[CustomCollectionCandidate] = []
        for page in range(1, 11):
            payload = self._tmdb(
                "/discover/tv" if media_type == "Series" else "/discover/movie", {**params, "page": page}
            )
            items = payload.get("results") if isinstance(payload, Mapping) else None
            if not isinstance(items, list):
                raise CustomCollectionSourceRejected("TMDb discover response is invalid")
            if not items:
                break
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                if person_items is not None and str(item.get("id") or "") not in person_items:
                    continue
                if not item.get("poster_path") or not item.get("overview"):
                    continue
                candidate = _tmdb_candidate(item, media_type=media_type)
                if candidate is not None:
                    result.append(candidate)
                    if len(result) >= limit:
                        return result[:limit]
            if page >= _positive_int(payload.get("total_pages"), default=1):
                break
        return result[:limit]

    def _tv_person_items(
        self,
        cast_value: object,
        crew_value: object,
    ) -> dict[str, Mapping[str, object]] | None:
        credit_maps: list[dict[str, Mapping[str, object]]] = []
        for raw_value, credit_type in ((cast_value, "cast"), (crew_value, "crew")):
            for person_id in _person_ids(raw_value):
                payload = self._tmdb(f"/person/{person_id}/tv_credits", {})
                values = payload.get(credit_type)
                if not isinstance(values, list):
                    raise CustomCollectionSourceRejected("TMDb person credits response is invalid")
                credit_maps.append(
                    {
                        str(item.get("id")): item
                        for item in values
                        if isinstance(item, Mapping) and item.get("id") is not None
                    }
                )
        if not credit_maps:
            return None
        matched_ids = set.intersection(*(set(values) for values in credit_maps))
        return {
            tmdb_id: next(values[tmdb_id] for values in credit_maps if tmdb_id in values) for tmdb_id in matched_ids
        }

    def _rss(
        self,
        url: str,
        definition: Mapping[str, object],
        *,
        limit: int,
    ) -> list[CustomCollectionCandidate]:
        response = self._get(url, headers={"Accept": "application/xml,text/xml"})
        content = response.content
        import xml.etree.ElementTree as element_tree

        try:
            root = element_tree.fromstring(content)
        except element_tree.ParseError as exc:
            raise CustomCollectionSourceRejected("RSS source returned invalid XML") from exc
        allowed = _item_types(definition)
        result: list[CustomCollectionCandidate] = []
        items = [item for item in root.iter() if _local_name(item.tag) == "item"]
        for item in items[:limit]:
            title = _element_text(item, "title")
            guid = _element_text(item, "guid")
            link = _element_text(item, "link")
            description = _element_text(item, "description")
            tmdb_id = _tmdb_id_from_text(guid) or _tmdb_id_from_text(link)
            imdb_id = _imdb_id_from_text(guid) or _imdb_id_from_text(link)
            year = _year(title) or _year(description)
            for media_type in allowed:
                candidate = (
                    self._candidate_from_id(tmdb_id, media_type, title, year)
                    if tmdb_id
                    else self._candidate_from_imdb(imdb_id, media_type, title, year)
                )
                if candidate is None and title:
                    candidate = self._search_title(title, media_type, year=year)
                if candidate is not None:
                    result.append(candidate)
                    break
        return result

    def _candidate_from_id(
        self,
        tmdb_id: str,
        media_type: str,
        title: str | None,
        year: str | None,
    ) -> CustomCollectionCandidate:
        return CustomCollectionCandidate(
            str(tmdb_id),
            "Series" if media_type.casefold() in {"series", "tv"} else "Movie",
            title,
            season=_season_from_title(title),
            year=year,
        )

    def _candidate_from_imdb(
        self,
        imdb_id: str | None,
        media_type: str,
        title: str | None,
        year: str | None,
    ) -> CustomCollectionCandidate | None:
        if not imdb_id:
            return None
        payload = self._tmdb(f"/find/{quote(imdb_id, safe='')}", {"external_source": "imdb_id"})
        key = "tv_results" if media_type.casefold() in {"series", "tv"} else "movie_results"
        values = payload.get(key) if isinstance(payload, Mapping) else None
        if not isinstance(values, list) or not values or not isinstance(values[0], Mapping):
            return None
        candidate = _tmdb_candidate(values[0], media_type="Series" if key == "tv_results" else "Movie")
        if candidate is None or candidate.media_type != "Series":
            return candidate
        _base_title, season = _series_title_and_season(title)
        return CustomCollectionCandidate(
            candidate.tmdb_id,
            candidate.media_type,
            title or candidate.title,
            season=season,
            year=candidate.year,
        )

    def _search_title(
        self,
        title: str,
        media_type: str,
        *,
        year: str | None = None,
    ) -> CustomCollectionCandidate | None:
        source_title = re.sub(r"^\s*\d+[.)、]\s*", "", str(title or "")).strip()
        source_title = re.sub(r"\s*[（(]\s*(?:19|20)\d{2}\s*[)）]\s*$", "", source_title).strip()
        is_series = media_type.casefold() in {"series", "tv"}
        base_title, season = _series_title_and_season(source_title) if is_series else (source_title, None)
        cleaned = base_title or source_title
        path = "/search/tv" if is_series else "/search/movie"
        params: dict[str, object] = {"query": cleaned}
        if year and media_type.casefold() not in {"series", "tv"}:
            params["year"] = year
        payload = self._tmdb(path, params)
        values = payload.get("results") if isinstance(payload, Mapping) else None
        if not isinstance(values, list):
            return None
        normalized = _normalize_title(cleaned)
        selected: Mapping[str, object] | None = None
        for value in values:
            if not isinstance(value, Mapping) or not value.get("id"):
                continue
            candidate_title = value.get("title") if path.endswith("movie") else value.get("name")
            if _normalize_title(candidate_title) == normalized:
                selected = value
                break
            if selected is None:
                selected = value
        if selected is None:
            return None
        if year:
            date_field = "first_air_date" if is_series else "release_date"
            matching_year = [
                value
                for value in values
                if isinstance(value, Mapping) and str(value.get(date_field) or "")[:4] == str(year) and value.get("id")
            ]
            if matching_year:
                selected = matching_year[0]
            elif not is_series:
                return None
        candidate = _tmdb_candidate(selected, media_type="Series" if path.endswith("tv") else "Movie")
        if candidate is not None and candidate.media_type == "Series":
            return CustomCollectionCandidate(
                candidate.tmdb_id,
                candidate.media_type,
                title,
                season=season,
                year=candidate.year,
            )
        return candidate

    def _get(self, url: str, **kwargs: Any) -> httpx.Response:
        try:
            with self._client_factory() as client:
                response = client.get(url, **kwargs)
                response.raise_for_status()
                return response
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise CustomCollectionSourceIndeterminate("custom collection source request failed") from exc
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            error_type = (
                CustomCollectionSourceIndeterminate
                if status >= 500 or status in {408, 425, 429}
                else CustomCollectionSourceRejected
            )
            raise error_type(f"custom collection source returned HTTP {status}") from exc

    def _json_get(self, url: str, *, params: Mapping[str, object] | None = None) -> Mapping[str, object]:
        response = self._get(url, params=dict(params or {}), headers={"Accept": "application/json"})
        try:
            payload = response.json()
        except ValueError as exc:
            raise CustomCollectionSourceRejected("custom collection source returned invalid JSON") from exc
        if not isinstance(payload, Mapping):
            raise CustomCollectionSourceRejected("custom collection source JSON must be an object")
        return payload

    def _tmdb(self, path: str, params: Mapping[str, object]) -> Mapping[str, object]:
        settings = self._settings()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise CustomCollectionSourceRejected("TMDb is not configured")
        response = self._get(
            f"{base_url}{path}",
            params={"api_key": api_key, "language": "zh-CN", **dict(params)},
            headers={"Accept": "application/json"},
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise CustomCollectionSourceRejected("TMDb response is not valid JSON") from exc
        if not isinstance(payload, Mapping):
            raise CustomCollectionSourceRejected("TMDb response is invalid")
        return payload

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=httpx.Timeout(30.0, connect=5.0), trust_env=False)


class HttpxCustomCollectionTmdbAdapter:
    def __init__(
        self, settings: Callable[[], Settings], *, client_factory: Callable[[], httpx.Client] | None = None
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or HttpxCustomCollectionSourceAdapter(settings)._default_client

    def series_details(self, tmdb_id: str) -> Mapping[str, object]:
        settings = self._settings()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise RuntimeError("TMDb is not configured")
        with self._client_factory() as client:
            response = client.get(
                f"{base_url}/tv/{quote(str(tmdb_id), safe='')}",
                params={"api_key": api_key, "language": "zh-CN"},
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb series response is invalid")
        return payload


class SqlAlchemyCustomCollectionMediaIndexAdapter:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def lookup(self, *, tmdb_id: str, media_type: str, season: int | None = None) -> str | None:
        with self._unit_of_work() as uow:
            if media_type == "Series" and season is not None:
                row = (
                    uow.session.execute(
                        text(
                            """
                            SELECT emby_item_ids_json
                            FROM media_metadata
                            WHERE item_type = 'Season'
                              AND parent_series_tmdb_id = :tmdb_id
                              AND season_number = :season
                              AND in_library IS TRUE
                              AND jsonb_typeof(emby_item_ids_json) = 'array'
                              AND jsonb_array_length(emby_item_ids_json) > 0
                            ORDER BY last_updated_at DESC NULLS LAST, tmdb_id
                            LIMIT 1
                            """
                        ),
                        {"tmdb_id": tmdb_id, "season": int(season)},
                    )
                    .mappings()
                    .first()
                )
            else:
                row = (
                    uow.session.execute(
                        text(
                            """
                            SELECT emby_item_ids_json
                            FROM media_metadata
                            WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                              AND in_library IS TRUE
                              AND jsonb_typeof(emby_item_ids_json) = 'array'
                              AND jsonb_array_length(emby_item_ids_json) > 0
                            """
                        ),
                        {"tmdb_id": tmdb_id, "item_type": media_type},
                    )
                    .mappings()
                    .first()
                )
        return _first_json_string(row.get("emby_item_ids_json")) if row else None


class SqlAlchemyCustomCollectionLibraryQueryAdapter:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: Callable[[], Settings] | None = None,
        mapping_settings: object | None = None,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._mapping_settings = mapping_settings
        self._client_factory = client_factory
        self._clock = clock

    def query_filter(
        self,
        definition: Mapping[str, object],
        *,
        user_id: str,
        execution_key: str = "",
    ) -> CustomCollectionLibrarySnapshot:
        visible_ids = self._visible_emby_ids(user_id)
        item_types = _item_types(definition)
        target_libraries = _strings(definition.get("target_library_ids"))
        rows = self._rows(
            item_types=item_types,
            target_libraries=target_libraries,
            visible_ids=visible_ids,
        )
        rows = _apply_rules(
            rows,
            definition.get("rules"),
            str(definition.get("logic") or "AND"),
            keyword_mapping=self._mapping("keyword_mapping", DEFAULT_KEYWORD_MAPPING),
            studio_mapping=self._mapping("studio_mapping", DEFAULT_STUDIO_MAPPING),
            rating_mapping=self._mapping("rating_mapping", DEFAULT_RATING_MAPPING),
            today=self._clock().date(),
        )
        rows = _sort_rows(rows, definition, execution_key=execution_key)
        total = len(rows)
        return CustomCollectionLibrarySnapshot(
            tuple(
                emby_id for row in rows[:10000] if (emby_id := _row_emby_id(row, visible_ids=visible_ids)) is not None
            ),
            total,
        )

    def query_personal_recommendation(
        self,
        definition: Mapping[str, object],
        *,
        user_id: str,
        execution_key: str = "",
    ) -> CustomCollectionLibrarySnapshot:
        snapshot = self.query_filter(
            {
                **dict(definition),
                "rules": [{"field": "rating", "operator": "gte", "value": 7}],
                "sort_by": "Random",
            },
            user_id=user_id,
            execution_key=execution_key,
        )
        if len(snapshot.emby_ids) >= 9:
            return CustomCollectionLibrarySnapshot(snapshot.emby_ids[:20], 0)
        fallback = self.query_filter(
            {**dict(definition), "rules": [], "sort_by": "Random"},
            user_id=user_id,
            execution_key=execution_key,
        )
        return CustomCollectionLibrarySnapshot(fallback.emby_ids[:20], 0)

    def query_placeholders(
        self,
        definition: Mapping[str, object],
        *,
        user_id: str,
        execution_key: str,
        limit: int = 9,
    ) -> tuple[str, ...]:
        snapshot = self.query_filter(
            {
                "item_type": ["Movie", "Series"],
                "target_library_ids": definition.get("target_library_ids", []),
                "rules": [],
                "sort_by": "Random",
            },
            user_id=user_id,
            execution_key=execution_key,
        )
        return snapshot.emby_ids[: max(0, int(limit))]

    def _rows(
        self,
        *,
        item_types: Sequence[str],
        target_libraries: Sequence[str],
        visible_ids: set[str] | None,
    ) -> list[dict[str, object]]:
        clauses = ["in_library IS TRUE"]
        params: dict[str, object] = {}
        if item_types:
            clauses.append("item_type IN :item_types")
            params["item_types"] = tuple(item_types)
        if target_libraries:
            clauses.append(
                "EXISTS ("
                "SELECT 1 FROM jsonb_array_elements(COALESCE(asset_details_json, '[]'::jsonb)) AS asset "
                "WHERE asset->>'source_library_id' IN :libraries"
                ")"
            )
            params["libraries"] = tuple(target_libraries)
        if visible_ids is not None:
            if not visible_ids:
                return []
            clauses.append("emby_item_ids_json ?| CAST(:visible_ids AS text[])")
            params["visible_ids"] = list(visible_ids)
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    SELECT
                        tmdb_id, item_type, emby_item_ids_json, title,
                        original_language, custom_rating, official_rating_json,
                        tags_json, countries_json, genres_json, keywords_json,
                        production_companies_json, networks_json,
                        actors_json, directors_json, asset_details_json,
                        release_year, rating, runtime_minutes,
                        date_added, release_date, last_updated_at,
                        watching_status,
                        CASE WHEN item_type = 'Series' THEN (
                            SELECT AVG(COALESCE(episode.runtime_minutes, 0))
                            FROM media_metadata AS episode
                            WHERE episode.item_type = 'Episode'
                              AND episode.parent_series_tmdb_id = media_metadata.tmdb_id
                              AND COALESCE(episode.runtime_minutes, 0) > 0
                        ) ELSE runtime_minutes END AS effective_runtime_minutes,
                        CASE WHEN item_type = 'Series' THEN EXISTS (
                            SELECT 1 FROM media_metadata AS season
                            WHERE season.item_type = 'Season'
                              AND season.parent_series_tmdb_id = media_metadata.tmdb_id
                              AND COALESCE(season.season_number, 0) > 0
                              AND lower(btrim(COALESCE(season.watching_status, '')))
                                  IN ('watching','paused')
                              AND EXISTS (
                                  SELECT 1 FROM media_metadata AS episode
                                  WHERE episode.item_type = 'Episode'
                                    AND episode.parent_series_tmdb_id = season.parent_series_tmdb_id
                                    AND episode.season_number = season.season_number
                                    AND episode.in_library IS TRUE
                              )
                        ) ELSE FALSE END AS is_in_progress
                    FROM media_metadata
                    WHERE """
                    + " AND ".join(clauses)
                ).bindparams(
                    *([bindparam("item_types", expanding=True)] if item_types else []),
                    *([bindparam("libraries", expanding=True)] if target_libraries else []),
                ),
                params,
            ).mappings()
            return [dict(row) for row in result]

    def _mapping(self, key: str, fallback: object) -> object:
        loader = getattr(self._mapping_settings, "load", None)
        if callable(loader):
            value = loader(key)
            if value:
                return value
        return fallback

    def _visible_emby_ids(self, user_id: str) -> set[str] | None:
        if self._settings is None:
            return None
        normalized_user = str(user_id or "").strip()
        if not normalized_user:
            raise ValueError("custom collection library query requires an Emby user identity")
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise RuntimeError("Emby is not configured for custom collection library query")
        client_factory = self._client_factory or (
            lambda: httpx.Client(timeout=float(settings.emby_api_timeout), trust_env=False)
        )
        try:
            visible_ids: set[str] = set()
            with client_factory() as client:
                start_index = 0
                page_size = 500
                while start_index < 100_000:
                    response = client.get(
                        f"{base_url}/Users/{quote(normalized_user, safe='')}/Items",
                        headers={"Accept": "application/json", "X-Emby-Token": token},
                        params={
                            "Recursive": "true",
                            "IncludeItemTypes": "Movie,Series",
                            "Fields": "Id",
                            "StartIndex": start_index,
                            "Limit": page_size,
                        },
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                        raise RuntimeError("Emby user library response is invalid")
                    page = [
                        str(item.get("Id") or "").strip()
                        for item in payload["Items"]
                        if isinstance(item, Mapping) and str(item.get("Id") or "").strip()
                    ]
                    visible_ids.update(page)
                    start_index += len(payload["Items"])
                    total = payload.get("TotalRecordCount")
                    if not payload["Items"] or (isinstance(total, int) and start_index >= total):
                        break
                    if total is None and len(payload["Items"]) < page_size:
                        break
        except (httpx.HTTPError, httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
            raise RuntimeError("Emby user library query failed") from exc
        return visible_ids


class NativeCustomCollectionRefreshOperationAdapter(CustomCollectionRefreshOperationPort):
    def __init__(
        self,
        operations: PostgresCollectionOperationRepository,
        executor: Any,
        *,
        principal_id: str = "collection-system:custom-refresh",
    ) -> None:
        self._operations = operations
        self._executor = executor
        self._principal_id = principal_id

    def apply(self, plan: CustomCollectionRefreshPlan, *, idempotency_key: str) -> CustomCollectionRefreshResult:
        scope = (
            f"emby-collection:{plan.expected_emby_collection_id}"
            if plan.expected_emby_collection_id
            else f"custom-collection:{plan.collection_id}"
        )
        intent = CollectionOperationIntent.create(
            kind=CollectionOperationKind.CUSTOM_REFRESH,
            scope_key=scope,
            principal_id=self._principal_id,
            request=plan.to_request(),
            idempotency_key=idempotency_key,
        )
        reservation = self._operations.reserve(intent)
        operation = self._executor.execute(reservation.operation.id)
        if operation is None:
            return CustomCollectionRefreshResult("indeterminate", "合集刷新操作不存在。")
        status = operation.status
        if status is CollectionOperationStatus.CONFIRMED:
            return CustomCollectionRefreshResult("confirmed", "自建合集刷新完成。", operation.result or {})
        if status in {
            CollectionOperationStatus.READY,
            CollectionOperationStatus.CLAIMED,
            CollectionOperationStatus.REQUESTING,
            CollectionOperationStatus.RETRYABLE,
            CollectionOperationStatus.AWAITING_READBACK,
        }:
            return CustomCollectionRefreshResult(
                "pending",
                "自建合集刷新已提交，等待远端确认。",
                {"operation_id": str(operation.id), "status": status.value},
            )
        if status is CollectionOperationStatus.REJECTED:
            return CustomCollectionRefreshResult("rejected", "自建合集刷新被拒绝。", operation.error or {})
        return CustomCollectionRefreshResult(
            "indeterminate", "自建合集刷新结果不确定，请人工核对。", operation.error or {}
        )


def _tmdb_candidate(value: object, *, media_type: str | None = None) -> CustomCollectionCandidate | None:
    if not isinstance(value, Mapping):
        return None
    raw_type: MediaType = (
        "Series" if str(media_type or value.get("media_type") or "").casefold() in {"series", "tv"} else "Movie"
    )
    tmdb_id = str(value.get("id") or "").strip()
    if not tmdb_id:
        return None
    title = value.get("title") if raw_type == "Movie" else value.get("name")
    date = value.get("release_date") if raw_type == "Movie" else value.get("first_air_date")
    return CustomCollectionCandidate(
        tmdb_id, raw_type, str(title or "").strip() or None, year=str(date)[:4] if date else None
    )


def _unique_candidates(values: Sequence[CustomCollectionCandidate]) -> tuple[CustomCollectionCandidate, ...]:
    result: list[CustomCollectionCandidate] = []
    seen: set[tuple[str | None, str, int | None, str | None]] = set()
    for value in values:
        key = (
            value.tmdb_id,
            value.media_type,
            value.season,
            None if value.tmdb_id else _normalize_title(value.title),
        )
        if key not in seen:
            seen.add(key)
            result.append(value)
    return tuple(result)


def _round_robin(values: Sequence[Sequence[CustomCollectionCandidate]]) -> list[CustomCollectionCandidate]:
    result: list[CustomCollectionCandidate] = []
    width = max((len(items) for items in values), default=0)
    for index in range(width):
        result.extend(items[index] for items in values if index < len(items))
    return result


def _tmdb_id_from_text(value: str) -> str | None:
    match = re.search(r"tmdb://(\d+)|/(?:movie|tv)/(\d+)(?:\D|$)", value, re.IGNORECASE)
    if not match:
        return None
    return match.group(1) or match.group(2)


def _imdb_id_from_text(value: str) -> str | None:
    match = re.search(r"\btt\d{7,}\b", value or "", re.IGNORECASE)
    return match.group(0) if match else None


def _normalize_title(value: object) -> str:
    return re.sub(r"[\s:：·\-_*'!,?.。()（）]+", "", str(value or "")).casefold()


def _series_title_and_season(value: object) -> tuple[str, int | None]:
    title = str(value or "").strip()
    chinese = {
        "一": 1,
        "二": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
        "十": 10,
        "十一": 11,
        "十二": 12,
        "十三": 13,
        "十四": 14,
        "十五": 15,
        "十六": 16,
        "十七": 17,
        "十八": 18,
        "十九": 19,
        "二十": 20,
    }
    roman = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}
    patterns = (
        re.compile(
            r"^(.*?)\s*(?:第\s*([一二三四五六七八九十\d]{1,3})\s*季|Season\s*(\d{1,3})|S(\d{1,3}))\b",
            re.IGNORECASE,
        ),
        re.compile(r"^(.*?)\s*(?:[第部]\s*([一二三四五六七八九十])|([IVX\d]+))(?:[:\s-]|$)"),
    )
    for pattern in patterns:
        match = pattern.match(title)
        if match is None:
            continue
        raw = next((item for item in match.groups()[1:] if item), "")
        if not raw:
            continue
        if raw.isdigit():
            season = int(raw)
        else:
            season = chinese.get(raw) or roman.get(raw.upper()) or 0
        base = str(match.group(1) or "").strip()
        if season > 0 and len(base) > 1:
            return base, season
    return title, None


def _season_from_title(value: object) -> int | None:
    return _series_title_and_season(value)[1]


def _resolve_dynamic_dates(value: str, *, today: date) -> str:
    def replace(match: re.Match[str]) -> str:
        base = today + timedelta(days=1 if match.group(1) == "tomorrow" else 0)
        offset = int(match.group(2) or 0)
        return (base + timedelta(days=offset)).isoformat()

    return re.sub(r"\{(today|tomorrow)(?:\+(\d+))?\}", replace, value)


def _person_ids(value: object) -> tuple[int, ...]:
    raw_values = value if isinstance(value, list | tuple | set) else str(value or "").replace("|", ",").split(",")
    return tuple(int(item) for item in raw_values if str(item).strip().isdigit())


def _filter_tv_credit_items(
    values: Sequence[Mapping[str, object]],
    params: Mapping[str, object],
) -> list[Mapping[str, object]]:
    def token_groups(value: object) -> list[set[str]]:
        return [
            {token.strip() for token in group.split("|") if token.strip()}
            for group in str(value or "").split(",")
            if group.strip()
        ]

    required_genres = token_groups(params.get("with_genres"))
    excluded_genres = (
        set().union(*token_groups(params.get("without_genres"))) if params.get("without_genres") else set()
    )
    required_countries = (
        set().union(*token_groups(params.get("with_origin_country"))) if params.get("with_origin_country") else set()
    )
    required_language = str(params.get("with_original_language") or "").strip()
    date_gte = str(params.get("first_air_date.gte") or "").strip()
    date_lte = str(params.get("first_air_date.lte") or "").strip()
    include_adult = str(params.get("include_adult") or "").strip().casefold() in {"1", "true", "yes", "on"}
    try:
        vote_gte = float(cast(Any, params.get("vote_average.gte")) or 0)
    except (TypeError, ValueError):
        vote_gte = 0.0
    try:
        vote_count_gte = int(cast(Any, params.get("vote_count.gte")) or 0)
    except (TypeError, ValueError):
        vote_count_gte = 0

    result: list[Mapping[str, object]] = []
    for item in values:
        genre_ids = item.get("genre_ids")
        genres = {str(value) for value in genre_ids} if isinstance(genre_ids, list) else set()
        origin_country = item.get("origin_country")
        countries = {str(value) for value in origin_country} if isinstance(origin_country, list) else set()
        first_air_date = str(item.get("first_air_date") or "")
        if not item.get("poster_path") or not item.get("overview"):
            continue
        if not include_adult and bool(item.get("adult")):
            continue
        if required_genres and not all(group.intersection(genres) for group in required_genres):
            continue
        if excluded_genres.intersection(genres):
            continue
        if required_countries and not required_countries.intersection(countries):
            continue
        if required_language and str(item.get("original_language") or "") != required_language:
            continue
        if date_gte and (not first_air_date or first_air_date < date_gte):
            continue
        if date_lte and (not first_air_date or first_air_date > date_lte):
            continue
        if float(cast(Any, item.get("vote_average")) or 0) < vote_gte:
            continue
        if int(cast(Any, item.get("vote_count")) or 0) < vote_count_gte:
            continue
        result.append(item)

    sort_by = str(params.get("sort_by") or "popularity.desc")
    sort_field, _, direction = sort_by.partition(".")
    if sort_field not in {"popularity", "vote_average", "vote_count", "first_air_date"}:
        sort_field = "popularity"
    result.sort(
        key=lambda item: cast(Any, item.get(sort_field) or ("" if sort_field == "first_air_date" else 0)),
        reverse=direction != "asc",
    )
    return result


def _local_name(value: object) -> str:
    return str(value or "").rsplit("}", 1)[-1].casefold()


def _element_text(item: object, name: str) -> str:
    iterator = getattr(item, "iter", None)
    if not callable(iterator):
        return ""
    for child in cast(Callable[[], Any], iterator)():
        if _local_name(getattr(child, "tag", "")) == name.casefold():
            return str(getattr(child, "text", "") or "").strip()
    return ""


class _DoubanListParser(HTMLParser):
    """Extract the title and year fields from a Douban doulist page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: list[tuple[str, str | None, str | None]] = []
        self._item_depth = 0
        self._title_depth = 0
        self._abstract_depth = 0
        self._title: list[str] = []
        self._abstract: list[str] = []
        self._subject_url: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        classes = set(str(attributes.get("class") or "").split())
        if tag == "div" and "doulist-item" in classes and self._item_depth == 0:
            self._item_depth = 1
            self._title = []
            self._abstract = []
            self._subject_url = None
        elif tag == "div" and self._item_depth:
            self._item_depth += 1
        if self._item_depth and tag == "div" and "title" in classes:
            self._title_depth = 1
        elif self._title_depth:
            self._title_depth += 1
        if self._item_depth and tag == "div" and "abstract" in classes:
            self._abstract_depth = 1
        elif self._abstract_depth:
            self._abstract_depth += 1
        if self._item_depth and self._title_depth and tag == "a":
            self._subject_url = str(attributes.get("href") or "").strip() or None

    def handle_endtag(self, tag: str) -> None:
        if self._title_depth:
            self._title_depth -= 1
        if self._abstract_depth:
            self._abstract_depth -= 1
        if tag == "div" and self._item_depth:
            self._item_depth -= 1
            if self._item_depth == 0:
                title = "".join(self._title).strip()
                if title:
                    self.items.append((title, _year("".join(self._abstract)), self._subject_url))

    def handle_data(self, data: str) -> None:
        if self._title_depth:
            self._title.append(data)
        if self._abstract_depth:
            self._abstract.append(data)


def _year(value: str | None) -> str | None:
    import re

    match = re.search(r"\b(?:19|20)\d{2}\b", value or "")
    return match.group(0) if match else None


def _positive_int(value: object, *, default: int) -> int:
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _positive_limit(value: object) -> int | None:
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _first_json_string(value: object) -> str | None:
    return next(iter(_json_strings(value)), None)


def _json_strings(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return (value.strip(),) if value.strip() else ()
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item or "").strip())
    return ()


def _item_types(definition: Mapping[str, object]) -> tuple[str, ...]:
    value = definition.get("item_type")
    if not isinstance(value, list):
        return ("Movie", "Series")
    return tuple(dict.fromkeys(str(item).strip() for item in value if str(item).strip())) or ("Movie", "Series")


def _strings(value: object) -> tuple[str, ...]:
    return tuple(str(item).strip() for item in value if str(item).strip()) if isinstance(value, list) else ()


def _row_emby_id(
    row: Mapping[str, object],
    *,
    visible_ids: set[str] | None = None,
) -> str | None:
    values = _json_strings(row.get("emby_item_ids_json"))
    if visible_ids is None:
        return values[0] if values else None
    return next((value for value in values if value in visible_ids), None)


_RULE_FIELDS = frozenset(
    {
        "actors",
        "audio_lang",
        "codec",
        "countries",
        "date_added",
        "directors",
        "effect",
        "genres",
        "is_in_progress",
        "keywords",
        "library",
        "original_language",
        "quality",
        "rating",
        "release_date",
        "release_year",
        "resolution",
        "runtime",
        "studios",
        "tags",
        "title",
        "unified_rating",
        "year",
    }
)


def _apply_rules(
    rows: Sequence[Mapping[str, object]],
    rules: object,
    logic: str,
    *,
    keyword_mapping: object,
    studio_mapping: object,
    rating_mapping: object,
    today: date,
) -> list[Mapping[str, object]]:
    normalized = (
        [
            dict(rule)
            for rule in rules
            if isinstance(rule, Mapping)
            and str(rule.get("field") or "").strip() in _RULE_FIELDS
            and rule.get("value") not in (None, "", [])
        ]
        if isinstance(rules, list)
        else []
    )
    if not normalized:
        return list(rows)
    match_all = str(logic).casefold() != "or"
    result: list[Mapping[str, object]] = []
    for row in rows:
        values = [
            _matches_rule(
                row,
                str(rule.get("field") or "").strip(),
                str(rule.get("operator") or "eq").strip().casefold(),
                rule.get("value"),
                keyword_mapping=keyword_mapping,
                studio_mapping=studio_mapping,
                rating_mapping=rating_mapping,
                today=today,
            )
            for rule in normalized
        ]
        if all(values) if match_all else any(values):
            result.append(row)
    return result


def _matches_rule(
    row: Mapping[str, object],
    field: str,
    operator: str,
    expected: object,
    *,
    keyword_mapping: object,
    studio_mapping: object,
    rating_mapping: object,
    today: date,
) -> bool:
    if field in {"tags", "countries"}:
        return _membership(
            _string_set(row.get(f"{field}_json")),
            expected,
            operator,
            primary=_first_string(row.get(f"{field}_json")),
        )
    if field == "genres":
        values = _object_names(row.get("genres_json"))
        return _membership(values, expected, operator, primary=_first_object_name(row.get("genres_json")))
    if field == "keywords":
        ids, names = _mapped_targets(keyword_mapping, expected, ("ids",))
        items = _objects(row.get("keywords_json"))
        values = {str(item.get("id") or "") for item in items if str(item.get("id") or "")} | {
            str(item.get("name") or "").casefold() for item in items if str(item.get("name") or "")
        }
        return _membership(values, ids | {value.casefold() for value in names}, operator)
    if field == "studios":
        company_ids, names = _mapped_targets(studio_mapping, expected, ("company_ids",))
        network_ids, network_names = _mapped_targets(studio_mapping, expected, ("network_ids",))
        companies = _objects(row.get("production_companies_json"))
        networks = _objects(row.get("networks_json"))
        values = {str(item.get("id") or "") for item in (*companies, *networks) if str(item.get("id") or "")} | {
            str(item.get("name") or "").casefold() for item in (*companies, *networks) if str(item.get("name") or "")
        }
        targets = company_ids | network_ids | {value.casefold() for value in names | network_names}
        primary = None
        first = companies[0] if companies else (networks[0] if networks else None)
        if first is not None:
            primary = str(first.get("id") or first.get("name") or "")
        return _membership(values, targets, operator, primary=primary)
    if field in {"actors", "directors"}:
        key = "tmdb_id" if field == "actors" else "id"
        items = _objects(row.get(f"{field}_json"))
        values = {str(item.get(key) or "") for item in items if str(item.get(key) or "")}
        primary_values = {str(item.get(key) or "") for item in items[:3] if str(item.get(key) or "")}
        targets = _expected_strings(expected)
        if operator == "is_primary":
            return bool(primary_values & targets)
        return _membership(values, targets, operator)
    if field == "unified_rating":
        codes = _rating_codes(rating_mapping, expected)
        value = _rating_text(row)
        return _membership({value}, codes, operator)
    if field in {"runtime", "release_year", "year", "rating"}:
        key = {
            "runtime": "effective_runtime_minutes",
            "year": "release_year",
        }.get(field, field)
        return _numeric_compare(row.get(key), expected, operator)
    if field in {"date_added", "release_date"}:
        value = _as_date(row.get(field))
        try:
            boundary = today - timedelta(days=int(cast(Any, expected)))
        except (TypeError, ValueError):
            return False
        if value is None:
            return False
        if operator == "in_last_days":
            return value >= boundary
        if operator == "not_in_last_days":
            return value < boundary
        return False
    if field == "title":
        value = str(row.get("title") or "")
        target = str(expected or "")
        if operator == "contains":
            return target.casefold() in value.casefold()
        if operator == "does_not_contain":
            return target.casefold() not in value.casefold()
        if operator == "starts_with":
            return value.casefold().startswith(target.casefold())
        if operator == "ends_with":
            return value.casefold().endswith(target.casefold())
        return value.casefold() == target.casefold()
    if field == "original_language":
        return _membership({str(row.get(field) or "")}, expected, operator)
    if field == "is_in_progress":
        return bool(row.get("is_in_progress")) is bool(expected) if operator == "is" else False
    if field in {"resolution", "quality", "effect", "codec", "audio_lang", "library"}:
        key = {
            "resolution": "resolution_display",
            "quality": "quality_display",
            "effect": "effect_display",
            "codec": "codec_display",
            "audio_lang": "audio_display",
            "library": "source_library_id",
        }[field]
        values = {str(asset.get(key) or "") for asset in _objects(row.get("asset_details_json"))}
        if field == "audio_lang" and operator in {"contains", "eq"}:
            return any(str(expected).casefold() in value.casefold() for value in values)
        return _membership(values, expected, operator)
    return True


def _sort_rows(
    rows: Sequence[Mapping[str, object]],
    definition: Mapping[str, object],
    *,
    execution_key: str = "",
) -> list[Mapping[str, object]]:
    field = str(definition.get("sort_by") or "DateCreated").strip()
    normalized = field.casefold()
    if normalized == "random":
        seed = str(execution_key or "custom-collection")
        return sorted(
            rows,
            key=lambda row: hashlib.sha256(f"{seed}\0{_row_emby_id(row) or ''}".encode()).digest(),
        )
    key = {
        "datecreated": "date_added",
        "dateplayed": "date_added",
        "sortname": "title",
        "productionyear": "release_year",
        "communityrating": "rating",
        "premieredate": "release_date",
    }.get(normalized, field)
    reverse = str(definition.get("sort_order") or "Descending").casefold() != "ascending"
    return sorted(rows, key=lambda row: (row.get(key) is not None, str(row.get(key) or "")), reverse=reverse)


def _objects(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _string_set(value: object) -> set[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {value}
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value if not isinstance(item, Mapping)}


def _object_names(value: object) -> set[str]:
    return {str(item.get("name") or "") for item in _objects(value) if str(item.get("name") or "")}


def _first_object_name(value: object) -> str | None:
    items = _objects(value)
    return str(items[0].get("name") or "") or None if items else None


def _first_string(value: object) -> str | None:
    if isinstance(value, list) and value:
        return str(value[0])
    return None


def _expected_strings(value: object) -> set[str]:
    values = value if isinstance(value, list) else [value]
    return {str(item) for item in values if str(item or "")}


def _membership(
    values: set[str],
    expected: object,
    operator: str,
    *,
    primary: str | None = None,
) -> bool:
    targets = expected if isinstance(expected, set) else _expected_strings(expected)
    folded_values = {value.casefold() for value in values}
    folded_targets = {value.casefold() for value in targets}
    matched = bool(folded_values & folded_targets)
    if operator == "is_primary":
        return primary is not None and primary.casefold() in folded_targets
    if operator in {"is_none_of", "not_in", "does_not_contain"}:
        return not matched
    return matched


def _mapped_targets(
    mapping: object,
    expected: object,
    id_fields: Sequence[str],
) -> tuple[set[str], set[str]]:
    labels = _expected_strings(expected)
    values = mapping if isinstance(mapping, list) else []
    ids: set[str] = set()
    names: set[str] = set()
    for item in values:
        if not isinstance(item, Mapping) or str(item.get("label") or "") not in labels:
            continue
        for field in id_fields:
            ids.update(_expected_strings(item.get(field) if isinstance(item.get(field), list) else []))
        names.update(_expected_strings(item.get("en") if isinstance(item.get("en"), list) else []))
    names.update(labels)
    return ids, names


def _rating_codes(mapping: object, expected: object) -> set[str]:
    labels = _expected_strings(expected)
    result: set[str] = set()
    if not isinstance(mapping, Mapping):
        return result
    for entries in mapping.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, Mapping) and str(entry.get("label") or "") in labels:
                code = str(entry.get("code") or "")
                if code:
                    result.add(code)
    return result


def _rating_text(row: Mapping[str, object]) -> str:
    custom = str(row.get("custom_rating") or "")
    if custom:
        return custom
    official = row.get("official_rating_json")
    return str(official.get("US") or "") if isinstance(official, Mapping) else ""


def _numeric_compare(left: object, right: object, operator: str) -> bool:
    try:
        first, second = float(cast(Any, left) or 0), float(cast(Any, right) or 0)
    except (TypeError, ValueError):
        return False
    return {
        "gte": first >= second,
        "gt": first > second,
        "lte": first <= second,
        "lt": first < second,
        "eq": first == second,
    }.get(operator, False)


def _as_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


__all__ = [
    "HttpxCustomCollectionSourceAdapter",
    "HttpxCustomCollectionTmdbAdapter",
    "NativeCustomCollectionRefreshOperationAdapter",
    "SqlAlchemyCustomCollectionLibraryQueryAdapter",
    "SqlAlchemyCustomCollectionMediaIndexAdapter",
]
