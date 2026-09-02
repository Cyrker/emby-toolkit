from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from threading import Lock
from typing import Any, Protocol, cast

from google import genai
from google.genai import types
from openai import OpenAI
from sqlalchemy import text
from zhipuai import ZhipuAI

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

from .custom_tasks import CustomCollectionCandidate


class CustomCollectionTitleResolver(Protocol):
    def __call__(
        self,
        title: str,
        media_type: str,
        *,
        year: str | None = None,
    ) -> CustomCollectionCandidate | None: ...


class CustomCollectionRecommendationPort(Protocol):
    def generate(
        self,
        definition: Mapping[str, object],
        *,
        resolve_title: CustomCollectionTitleResolver,
    ) -> Sequence[CustomCollectionCandidate]: ...


class CustomCollectionDiscoveryPort(Protocol):
    def recommend(
        self,
        history: Sequence[Mapping[str, object]],
        *,
        allowed_types: Sequence[str],
        limit: int,
        instruction: str,
    ) -> Sequence[Mapping[str, object] | str]: ...


class SqlAlchemyCustomCollectionRecommendationService:
    """Generate the global custom collection from frozen DB reads and bounded AI discovery."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        discovery: CustomCollectionDiscoveryPort | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._discovery = discovery

    def generate(
        self,
        definition: Mapping[str, object],
        *,
        resolve_title: CustomCollectionTitleResolver,
    ) -> Sequence[CustomCollectionCandidate]:
        limit = _positive_int(definition.get("limit"), default=20)
        allowed_types = _allowed_types(definition.get("item_type"))
        history = self._popular(limit=20)
        if not history:
            return ()
        excluded = {str(item.get("tmdb_id") or "") for item in history if str(item.get("tmdb_id") or "")}
        result: list[CustomCollectionCandidate] = []

        ratio = _bounded_ratio(definition.get("ai_discovery_ratio"), default=0.2)
        discovery_limit = min(limit, max(0, int(limit * ratio)))
        if discovery_limit and self._discovery is not None:
            try:
                recommendations = self._discovery.recommend(
                    history,
                    allowed_types=allowed_types,
                    limit=max(2, discovery_limit),
                    instruction=str(definition.get("ai_prompt") or "").strip(),
                )
            except Exception:
                recommendations = ()
            for value in recommendations:
                title, original_title, year, preferred_type = _discovery_value(value)
                if not title:
                    continue
                search_types = _search_types(preferred_type, allowed_types)
                candidate = None
                for media_type in search_types:
                    query = original_title or title
                    candidate = resolve_title(query, media_type, year=year)
                    if candidate is None and query != title:
                        candidate = resolve_title(title, media_type, year=year)
                    if candidate is not None:
                        break
                if candidate is None or candidate.media_type not in allowed_types:
                    continue
                if not candidate.tmdb_id or candidate.tmdb_id in excluded:
                    continue
                if candidate.media_type == "Series" and candidate.season is None:
                    candidate = CustomCollectionCandidate(
                        candidate.tmdb_id,
                        candidate.media_type,
                        candidate.title,
                        season=1,
                        year=candidate.year,
                    )
                result.append(candidate)
                if len(result) >= discovery_limit:
                    break

        needed = max(0, limit - len(result))
        if needed:
            result.extend(
                self._vector_recommendations(
                    history,
                    excluded=excluded | {str(item.tmdb_id) for item in result if item.tmdb_id},
                    allowed_types=allowed_types,
                    limit=needed,
                )
            )
        return _unique(result)[:limit]

    def _popular(self, *, limit: int) -> tuple[Mapping[str, object], ...]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT m.tmdb_id, m.item_type, m.title, m.release_year,
                               COUNT(DISTINCT user_data.user_id) AS play_count
                        FROM user_media_data AS user_data
                        JOIN media_metadata AS m
                          ON user_data.item_id = m.tmdb_id
                          OR m.emby_item_ids_json ? user_data.item_id
                        WHERE user_data.played IS TRUE
                          AND m.item_type IN ('Movie','Series')
                        GROUP BY m.tmdb_id, m.item_type, m.title, m.release_year
                        ORDER BY play_count DESC, m.item_type, m.tmdb_id
                        LIMIT :limit
                        """
                    ),
                    {"limit": max(1, int(limit))},
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def _vector_recommendations(
        self,
        history: Sequence[Mapping[str, object]],
        *,
        excluded: set[str],
        allowed_types: Sequence[str],
        limit: int,
    ) -> list[CustomCollectionCandidate]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, release_year, overview_embedding
                        FROM media_metadata
                        WHERE in_library IS TRUE
                          AND item_type IN ('Movie','Series')
                          AND overview_embedding IS NOT NULL
                        ORDER BY item_type, tmdb_id
                        """
                    )
                )
                .mappings()
                .all()
            )
        values = [dict(row) for row in rows]
        seed_ids = {str(item.get("tmdb_id") or "") for item in history}
        seed_vectors = [_vector(row.get("overview_embedding")) for row in values if str(row.get("tmdb_id")) in seed_ids]
        seed_vectors = [value for value in seed_vectors if value]
        if not seed_vectors:
            return []
        dimensions = {len(value) for value in seed_vectors}
        if len(dimensions) != 1:
            return []
        width = next(iter(dimensions))
        profile = _normalize(
            tuple(sum(vector[index] for vector in seed_vectors) / len(seed_vectors) for index in range(width))
        )
        if not profile:
            return []
        scored: list[tuple[float, Mapping[str, object]]] = []
        for row in values:
            tmdb_id = str(row.get("tmdb_id") or "")
            item_type = str(row.get("item_type") or "")
            if not tmdb_id or tmdb_id in excluded or item_type not in allowed_types:
                continue
            raw_vector = _vector(row.get("overview_embedding"))
            if len(raw_vector) != len(profile):
                continue
            vector = _normalize(raw_vector)
            if not vector:
                continue
            score = sum(left * right for left, right in zip(profile, vector, strict=True))
            if 0.45 <= score < 0.999:
                scored.append((score, row))
        scored.sort(key=lambda item: (-item[0], str(item[1].get("item_type")), str(item[1].get("tmdb_id"))))
        return [
            CustomCollectionCandidate(
                str(row.get("tmdb_id")),
                "Series" if str(row.get("item_type")) == "Series" else "Movie",
                str(row.get("title") or "").strip() or None,
                season=1 if str(row.get("item_type")) == "Series" else None,
                year=str(row.get("release_year") or "").strip() or None,
            )
            for _score, row in scored[: max(0, int(limit))]
        ]


class ConfiguredCustomCollectionDiscoveryProvider:
    """Bounded JSON recommendation adapter for the configured OpenAI, ZhipuAI, or Gemini provider."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings
        self._lock = Lock()
        self._identity: tuple[str, str, str] | None = None
        self._client: Any = None

    def recommend(
        self,
        history: Sequence[Mapping[str, object]],
        *,
        allowed_types: Sequence[str],
        limit: int,
        instruction: str,
    ) -> Sequence[Mapping[str, object] | str]:
        if limit <= 0:
            return ()
        settings = self._settings()
        api_key = settings.ai_api_key.get_secret_value().strip()
        if not api_key:
            raise RuntimeError("AI recommendation provider is not configured")
        prompt = _recommendation_prompt(history, allowed_types, limit, instruction)
        provider = str(settings.ai_provider or "openai").strip().casefold()
        if provider == "openai":
            client = self._cached(
                (provider, api_key, settings.ai_base_url.strip()),
                lambda: OpenAI(
                    api_key=api_key,
                    base_url=settings.ai_base_url.strip() or None,
                    timeout=60.0,
                    max_retries=0,
                ),
            )
            response = client.chat.completions.create(
                model=settings.ai_model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
        elif provider == "zhipuai":
            client = self._cached(
                (provider, api_key, ""),
                lambda: ZhipuAI(api_key=api_key, timeout=60.0, max_retries=0),
            )
            response = client.chat.completions.create(
                model=settings.ai_model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
        elif provider == "gemini":
            client = self._cached(
                (provider, api_key, ""),
                lambda: genai.Client(api_key=api_key, http_options=types.HttpOptions(timeout=60_000)),
            )
            response = client.models.generate_content(
                model=settings.ai_model_name,
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )
            content = response.text
        else:
            raise RuntimeError("AI recommendation provider is unsupported")
        return _recommendation_values(content, limit=limit)

    def _cached(self, identity: tuple[str, str, str], factory: Callable[[], Any]) -> Any:
        with self._lock:
            if self._client is None or self._identity != identity:
                self._client = factory()
                self._identity = identity
            return self._client


def _recommendation_prompt(
    history: Sequence[Mapping[str, object]],
    allowed_types: Sequence[str],
    limit: int,
    instruction: str,
) -> str:
    titles = [
        f"{item.get('title')} ({item.get('release_year')})"
        if item.get("release_year")
        else str(item.get("title") or "")
        for item in history
        if str(item.get("title") or "").strip()
    ]
    extra = f"\n额外要求：{instruction}" if instruction else ""
    return (
        "你是资深选片人。根据历史偏好推荐未出现在历史中的同类高分作品。"
        f"只允许类型 {list(allowed_types)}，最多 {limit} 项。"
        "仅返回 JSON 对象，格式为 "
        '{"recommendations":[{"title":"...","original_title":"...","year":2024,"type":"Movie"}]}。'
        f"\n历史偏好：{titles}{extra}"
    )


def _recommendation_values(content: object, *, limit: int) -> tuple[Mapping[str, object] | str, ...]:
    raw = str(content or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        raw = raw.removeprefix("json").strip()
    payload = json.loads(raw)
    values = payload.get("recommendations") if isinstance(payload, Mapping) else payload
    if not isinstance(values, list):
        raise ValueError("AI recommendation response must contain a recommendations array")
    return tuple(value for value in values[:limit] if isinstance(value, Mapping | str))


def _discovery_value(value: Mapping[str, object] | str) -> tuple[str, str | None, str | None, str]:
    if isinstance(value, str):
        return value.strip(), None, None, "Movie"
    title = str(value.get("title") or "").strip()
    original_title = str(value.get("original_title") or "").strip() or None
    year = str(value.get("year") or "").strip() or None
    preferred = "Series" if str(value.get("type") or "").casefold() in {"series", "tv"} else "Movie"
    return title, original_title, year, preferred


def _search_types(preferred: str, allowed: Sequence[str]) -> tuple[str, ...]:
    result = [preferred] if preferred in allowed else []
    result.extend(value for value in allowed if value not in result)
    return tuple(result)


def _allowed_types(value: object) -> tuple[str, ...]:
    values = value if isinstance(value, list) else ["Movie", "Series"]
    result = tuple(dict.fromkeys("Series" if str(item).casefold() in {"series", "tv"} else "Movie" for item in values))
    return result or ("Movie", "Series")


def _positive_int(value: object, *, default: int) -> int:
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _bounded_ratio(value: object, *, default: float) -> float:
    try:
        parsed = float(cast(Any, value))
    except (TypeError, ValueError):
        return default
    return min(1.0, max(0.0, parsed))


def _vector(value: object) -> tuple[float, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, list):
        return ()
    try:
        result = tuple(float(item) for item in value)
    except (TypeError, ValueError):
        return ()
    return result if result and all(math.isfinite(item) for item in result) else ()


def _normalize(value: Sequence[float]) -> tuple[float, ...]:
    norm = math.sqrt(sum(item * item for item in value))
    if not math.isfinite(norm) or norm <= 0:
        return ()
    return tuple(item / norm for item in value)


def _unique(values: Sequence[CustomCollectionCandidate]) -> list[CustomCollectionCandidate]:
    result: list[CustomCollectionCandidate] = []
    seen: set[tuple[str | None, str, int | None]] = set()
    for value in values:
        key = (value.tmdb_id, value.media_type, value.season)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


__all__ = [
    "ConfiguredCustomCollectionDiscoveryProvider",
    "CustomCollectionDiscoveryPort",
    "CustomCollectionRecommendationPort",
    "SqlAlchemyCustomCollectionRecommendationService",
]
