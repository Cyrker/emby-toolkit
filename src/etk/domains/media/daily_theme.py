from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome

DAILY_THEME_REPLENISHMENT_EFFECT_TYPE = "daily-theme.replenishment"


@dataclass(frozen=True, slots=True)
class DailyThemeSnapshot:
    values: Mapping[str, Any]
    fingerprint: str
    publication_revision: int


@dataclass(frozen=True, slots=True)
class DailyThemePublishResult:
    revision: int
    receipt: Mapping[str, Any]
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class DailyThemePoolMutationResult:
    revision: int
    pool_size: int


class DailyThemeProvider(Protocol):
    def daily_theme_configured(self) -> bool: ...

    def discover_movie(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None: ...

    def movie_details(self, movie_id: int) -> Mapping[str, Any] | None: ...


class DailyThemeRepository(Protocol):
    def load_snapshot(self) -> DailyThemeSnapshot: ...

    def load_receipt(self, publication_id: str) -> Mapping[str, Any] | None: ...

    def excluded_movie_ids(self, tmdb_ids: Sequence[str]) -> set[str]: ...

    def actor_names(self, actor_ids: Sequence[int]) -> Mapping[int, str]: ...

    def publish(
        self,
        *,
        expected_fingerprint: str,
        pool: Sequence[Mapping[str, Any]],
        theme_index: int,
        pool_page: int,
        metadata: Mapping[str, Any],
    ) -> DailyThemePublishResult | None: ...


class DailyThemeDefaults(Protocol):
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]: ...


class DailyThemeProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _Theme:
    label: str
    keyword_ids: tuple[str, ...]


class _DailyThemeFailure(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class DailyThemeService:
    _TARGET_POOL_SIZE = 10
    _MAX_PAGES = 5
    _REPLENISH_THRESHOLD = 5

    def __init__(
        self,
        provider: DailyThemeProvider,
        repository: DailyThemeRepository,
        defaults: DailyThemeDefaults,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._provider = provider
        self._repository = repository
        self._defaults = defaults
        self._clock = clock

    def execute(
        self,
        progress: DailyThemeProgress,
        *,
        publication_id: str,
        mode: str = "daily",
    ) -> TaskOutcome:
        normalized_publication_id = str(publication_id).strip()
        if not normalized_publication_id:
            raise ValueError("daily-theme publication_id must not be empty")
        normalized_mode = str(mode).strip().lower()
        if normalized_mode not in {"daily", "replenish"}:
            raise ValueError("daily-theme mode must be daily or replenish")
        progress.update(2, "正在读取每日主题发布快照。")
        snapshot: DailyThemeSnapshot | None = None
        try:
            snapshot = self._load_snapshot()
            replay = _publication_replay(
                self._load_receipt(normalized_publication_id),
                normalized_publication_id,
            )
            if replay is not None:
                progress.update(100, "本 Job 的每日主题发布已提交，已直接恢复结果。")
                return replay
            themes = _themes(snapshot.values.get("keyword_mapping"), self._defaults)
            if normalized_mode == "daily":
                theme_index = _next_theme_index(snapshot.values.get("recommendation_theme_index"), len(themes))
                theme = themes[theme_index]
                progress.checkpoint()
                detailed_movies, last_page = self._collect_movies(theme, progress)
                actor_names = self._actor_names(detailed_movies)
                pool = [_recommendation(movie, actor_names) for movie in detailed_movies]
                added_count = len(pool)
                pages_scanned = last_page
            else:
                replenishment = self._replenishment_plan(snapshot, themes, progress)
                if isinstance(replenishment, TaskOutcome):
                    return replenishment
                theme_index, theme, pool, last_page, added_count = replenishment
                pages_scanned = 1
            metadata = {
                "publication_id": normalized_publication_id,
                "mode": normalized_mode,
                "source_revision": snapshot.publication_revision,
                "source_fingerprint": snapshot.fingerprint,
                "theme_index": theme_index,
                "theme_label": theme.label,
                "pool_page": last_page,
                "pool_size": len(pool),
                "pages_scanned": pages_scanned,
                "added_count": added_count,
                "published_at": self._clock().astimezone(UTC).isoformat(),
            }
            progress.update(95, "正在原子发布每日主题推荐池。")
            publication = self._publish(
                snapshot=snapshot,
                pool=pool,
                theme_index=theme_index,
                pool_page=last_page,
                metadata=metadata,
            )
        except _DailyThemeFailure as exc:
            values = snapshot.values if snapshot is not None else {}
            previous_pool = values.get("recommendation_pool")
            return TaskOutcome.failed(
                str(exc),
                {
                    "failure_kind": exc.code,
                    "reason_code": exc.code,
                    "publication": "retained",
                    "published": False,
                    "retained_previous_pool": True,
                    "preserved_existing_pool": True,
                    "pool_size": len(previous_pool) if isinstance(previous_pool, list) else 0,
                    "theme_index": (
                        values.get("recommendation_theme_index")
                        if isinstance(values.get("recommendation_theme_index"), int)
                        else None
                    ),
                    "expected_revision": snapshot.publication_revision if snapshot is not None else None,
                    "publication_id": normalized_publication_id,
                    "retryable": exc.retryable,
                },
            )

        if publication is None:
            progress.update(100, "已有更新的每日主题发布，本次旧计划未覆盖它。")
            return TaskOutcome.skipped(
                "每日主题发布快照已变化，本次旧计划未覆盖更新结果。",
                {
                    "reason_code": "publication_conflict",
                    "publication": "retained",
                    "published": False,
                    "publication_conflict": True,
                    "source_revision": snapshot.publication_revision,
                    "publication_id": normalized_publication_id,
                    "retained_previous_pool": True,
                    "preserved_existing_pool": True,
                    "retryable": False,
                },
            )
        if publication.replayed:
            replay = _publication_replay(publication.receipt, normalized_publication_id)
            if replay is None:
                raise RuntimeError("daily-theme repository returned an empty replay receipt")
            progress.update(100, "本 Job 的每日主题发布已提交，已直接恢复结果。")
            return replay
        progress.update(100, f"每日主题【{theme.label}】已发布。")
        message = (
            f"每日主题【{theme.label}】已更新，共发布 {len(pool)} 部电影。"
            if normalized_mode == "daily"
            else f"主题【{theme.label}】推荐池已补充，新增 {added_count} 部电影。"
        )
        return TaskOutcome.succeeded(
            message,
            {
                "publication": "committed",
                "published": True,
                "publication_id": normalized_publication_id,
                "mode": normalized_mode,
                "theme_index": theme_index,
                "theme_label": theme.label,
                "pool_page": last_page,
                "pages_scanned": pages_scanned,
                "pool_size": len(pool),
                "added_count": added_count,
                "empty_result": not pool,
                "retained_previous_pool": False,
                "publication_revision": publication.revision,
                "publication_conflict": False,
                "detail_failures": 0,
            },
        )

    def _load_snapshot(self) -> DailyThemeSnapshot:
        try:
            return self._repository.load_snapshot()
        except Exception as exc:
            raise _DailyThemeFailure("repository_unavailable", "读取每日主题发布快照失败。") from exc

    def _load_receipt(self, publication_id: str) -> Mapping[str, Any] | None:
        try:
            return self._repository.load_receipt(publication_id)
        except Exception as exc:
            raise _DailyThemeFailure("repository_unavailable", "读取每日主题发布回执失败。") from exc

    def _collect_movies(
        self,
        theme: _Theme,
        progress: DailyThemeProgress,
    ) -> tuple[list[dict[str, Any]], int]:
        detailed: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        last_page = 0
        page = 1
        while len(detailed) < self._TARGET_POOL_SIZE and page <= self._MAX_PAGES:
            progress.update(5 + (page - 1) * 15, f"正在扫描主题【{theme.label}】第 {page} 页。")
            progress.checkpoint()
            payload = self._discover_page(theme, page)
            raw_results = payload.get("results")
            if not isinstance(raw_results, list):
                raise _DailyThemeFailure("invalid_provider_payload", "TMDb 每日主题返回了无效的电影列表。")
            if not raw_results:
                break
            movies = _movie_results(raw_results)
            last_page = page
            excluded = self._excluded_movie_ids(movies)
            for movie in movies:
                movie_id = _movie_id(movie)
                if movie_id in seen_ids:
                    continue
                seen_ids.add(movie_id)
                if not _eligible(movie, excluded):
                    continue
                details = self._movie_details(movie_id)
                if str(details.get("id")) != str(movie_id):
                    raise _DailyThemeFailure("invalid_provider_payload", "TMDb 电影详情身份与请求不一致。")
                _cast(details)
                detailed.append(details)
            page += 1
        return detailed, last_page

    def _replenishment_plan(
        self,
        snapshot: DailyThemeSnapshot,
        themes: Sequence[_Theme],
        progress: DailyThemeProgress,
    ) -> tuple[int, _Theme, list[dict[str, Any]], int, int] | TaskOutcome:
        raw_pool = snapshot.values.get("recommendation_pool")
        if raw_pool is None:
            current_pool: list[dict[str, Any]] = []
        elif isinstance(raw_pool, list) and all(isinstance(item, Mapping) for item in raw_pool):
            current_pool = [dict(item) for item in raw_pool]
        else:
            raise _DailyThemeFailure(
                "invalid_publication",
                "当前推荐池格式无效，无法执行补充。",
                retryable=False,
            )
        if len(current_pool) >= self._REPLENISH_THRESHOLD:
            progress.update(100, "推荐池库存充足，无需补充。")
            return TaskOutcome.skipped(
                "推荐池库存充足，无需补充。",
                {
                    "reason_code": "pool_sufficient",
                    "mode": "replenish",
                    "publication": "retained",
                    "published": False,
                    "pool_size": len(current_pool),
                    "retryable": False,
                },
            )
        raw_theme_index = snapshot.values.get("recommendation_theme_index")
        if (
            isinstance(raw_theme_index, bool)
            or not isinstance(raw_theme_index, int)
            or raw_theme_index < 0
            or raw_theme_index >= len(themes)
        ):
            progress.update(100, "尚无可补充的当前推荐主题。")
            return TaskOutcome.skipped(
                "尚无可补充的当前推荐主题，请先完成每日主题更新。",
                {
                    "reason_code": "current_theme_unavailable",
                    "mode": "replenish",
                    "publication": "retained",
                    "published": False,
                    "pool_size": len(current_pool),
                    "retryable": False,
                },
            )
        raw_page = snapshot.values.get("recommendation_pool_page")
        if raw_page is None:
            current_page = 1
        elif isinstance(raw_page, bool) or not isinstance(raw_page, int) or raw_page < 0:
            raise _DailyThemeFailure(
                "invalid_publication",
                "当前推荐池页码格式无效，无法执行补充。",
                retryable=False,
            )
        else:
            current_page = raw_page
        theme = themes[raw_theme_index]
        next_page = current_page + 1
        progress.update(20, f"正在扫描主题【{theme.label}】第 {next_page} 页以补充推荐池。")
        progress.checkpoint()
        payload = self._discover_page(theme, next_page)
        raw_results = payload.get("results")
        if not isinstance(raw_results, list):
            raise _DailyThemeFailure("invalid_provider_payload", "TMDb 推荐池补充返回了无效的电影列表。")
        if not raw_results:
            return raw_theme_index, theme, current_pool, 0, 0

        movies = _movie_results(raw_results)
        excluded = self._excluded_movie_ids(movies)
        current_ids = {str(item.get("id")) for item in current_pool if item.get("id") is not None}
        detailed: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        for movie in movies:
            movie_id = _movie_id(movie)
            if movie_id in seen_ids or str(movie_id) in current_ids:
                continue
            seen_ids.add(movie_id)
            if not _eligible(movie, excluded):
                continue
            details = self._movie_details(movie_id)
            if str(details.get("id")) != str(movie_id):
                raise _DailyThemeFailure("invalid_provider_payload", "TMDb 电影详情身份与请求不一致。")
            _cast(details)
            detailed.append(details)
        actor_names = self._actor_names(detailed)
        additions = [_recommendation(movie, actor_names) for movie in detailed]
        return raw_theme_index, theme, [*current_pool, *additions], next_page, len(additions)

    def _discover_page(self, theme: _Theme, page: int) -> Mapping[str, Any]:
        try:
            configured = self._provider.daily_theme_configured()
        except Exception as exc:
            raise _DailyThemeFailure("provider_configuration_unavailable", "读取 TMDb 每日主题配置失败。") from exc
        if not configured:
            raise _DailyThemeFailure(
                "provider_not_configured",
                "TMDb 每日主题配置缺失，已保留现有推荐池。",
                retryable=False,
            )
        try:
            payload = self._provider.discover_movie(
                {
                    "with_keywords": "|".join(theme.keyword_ids),
                    "sort_by": "popularity.desc",
                    "page": page,
                    "include_adult": False,
                }
            )
        except Exception as exc:
            raise _DailyThemeFailure("provider_unavailable", "TMDb 每日主题扫描失败，已保留现有推荐池。") from exc
        if payload is None or not isinstance(payload, Mapping):
            raise _DailyThemeFailure("provider_unavailable", "TMDb 每日主题扫描失败，已保留现有推荐池。")
        return payload

    def _excluded_movie_ids(self, movies: Sequence[Mapping[str, Any]]) -> set[str]:
        identifiers = tuple(dict.fromkeys(str(_movie_id(movie)) for movie in movies))
        try:
            return self._repository.excluded_movie_ids(identifiers)
        except Exception as exc:
            raise _DailyThemeFailure("repository_unavailable", "读取本地媒体和订阅状态失败。") from exc

    def _movie_details(self, movie_id: int) -> dict[str, Any]:
        try:
            details = self._provider.movie_details(movie_id)
        except Exception as exc:
            raise _DailyThemeFailure("provider_unavailable", "TMDb 电影详情读取失败，已保留现有推荐池。") from exc
        if details is None or not isinstance(details, Mapping):
            raise _DailyThemeFailure("provider_unavailable", "TMDb 电影详情读取失败，已保留现有推荐池。")
        return dict(details)

    def _actor_names(self, movies: Sequence[Mapping[str, Any]]) -> Mapping[int, str]:
        identifiers = tuple(
            dict.fromkeys(
                actor_id
                for movie in movies
                for actor in _cast(movie)
                if (actor_id := _optional_positive_int(actor.get("id"))) is not None
            )
        )
        try:
            return self._repository.actor_names(identifiers)
        except Exception as exc:
            raise _DailyThemeFailure("repository_unavailable", "读取演员中文名失败。") from exc

    def _publish(
        self,
        *,
        snapshot: DailyThemeSnapshot,
        pool: Sequence[Mapping[str, Any]],
        theme_index: int,
        pool_page: int,
        metadata: Mapping[str, Any],
    ) -> DailyThemePublishResult | None:
        progress_safe_pool = tuple(dict(item) for item in pool)
        try:
            return self._repository.publish(
                expected_fingerprint=snapshot.fingerprint,
                pool=progress_safe_pool,
                theme_index=theme_index,
                pool_page=pool_page,
                metadata=metadata,
            )
        except Exception as exc:
            raise _DailyThemeFailure("publication_failed", "原子发布每日主题推荐池失败。") from exc


def _themes(raw: Any, defaults: DailyThemeDefaults) -> tuple[_Theme, ...]:
    mapping = raw or defaults.keyword_mapping()
    values: list[tuple[str, Any]] = []
    if isinstance(mapping, Mapping):
        for label, info in mapping.items():
            if not isinstance(info, Mapping):
                raise _DailyThemeFailure(
                    "invalid_mapping",
                    "关键词映射表格式无效。",
                    retryable=False,
                )
            values.append((str(label).strip(), info.get("ids")))
    elif isinstance(mapping, list | tuple):
        for item in mapping:
            if not isinstance(item, Mapping):
                raise _DailyThemeFailure(
                    "invalid_mapping",
                    "关键词映射表格式无效。",
                    retryable=False,
                )
            values.append((str(item.get("label") or "").strip(), item.get("ids")))
    else:
        raise _DailyThemeFailure("invalid_mapping", "关键词映射表格式无效。", retryable=False)

    themes: list[_Theme] = []
    for label, raw_ids in values:
        if not label or not raw_ids:
            continue
        if not isinstance(raw_ids, list | tuple):
            raise _DailyThemeFailure(
                "invalid_mapping",
                "关键词映射表的 ids 必须是列表。",
                retryable=False,
            )
        identifiers = tuple(str(value).strip() for value in raw_ids if str(value).strip())
        if identifiers:
            themes.append(_Theme(label, identifiers))
    if not themes:
        raise _DailyThemeFailure(
            "invalid_mapping",
            "关键词映射表为空或未配置 ID。",
            retryable=False,
        )
    return tuple(themes)


def _publication_replay(raw: Mapping[str, Any] | None, publication_id: str) -> TaskOutcome | None:
    if not isinstance(raw, Mapping) or str(raw.get("publication_id") or "") != publication_id:
        return None
    revision = raw.get("revision")
    theme_index = raw.get("theme_index")
    pool_page = raw.get("pool_page")
    pool_size = raw.get("pool_size")
    pages_scanned = raw.get("pages_scanned", pool_page)
    added_count = raw.get("added_count", pool_size)
    mode = str(raw.get("mode") or "daily").strip().lower()
    theme_label = str(raw.get("theme_label") or "").strip()
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
        or isinstance(theme_index, bool)
        or not isinstance(theme_index, int)
        or theme_index < 0
        or isinstance(pool_page, bool)
        or not isinstance(pool_page, int)
        or pool_page < 0
        or isinstance(pool_size, bool)
        or not isinstance(pool_size, int)
        or pool_size < 0
        or isinstance(pages_scanned, bool)
        or not isinstance(pages_scanned, int)
        or pages_scanned < 0
        or isinstance(added_count, bool)
        or not isinstance(added_count, int)
        or added_count < 0
        or mode not in {"daily", "replenish"}
        or not theme_label
    ):
        raise _DailyThemeFailure(
            "invalid_publication",
            "已提交的每日主题发布回执格式无效。",
            retryable=False,
        )
    return TaskOutcome.succeeded(
        f"每日主题【{theme_label}】已由本 Job 发布，已恢复既有结果。",
        {
            "publication": "already_committed",
            "published": True,
            "publication_id": publication_id,
            "mode": mode,
            "theme_index": theme_index,
            "theme_label": theme_label,
            "pool_page": pool_page,
            "pages_scanned": pages_scanned,
            "pool_size": pool_size,
            "added_count": added_count,
            "empty_result": pool_size == 0,
            "retained_previous_pool": False,
            "publication_revision": revision,
            "publication_conflict": False,
            "detail_failures": 0,
            "replayed": True,
        },
    )


def _next_theme_index(raw: Any, theme_count: int) -> int:
    if raw is None:
        previous = -1
    elif isinstance(raw, bool) or not isinstance(raw, int):
        raise _DailyThemeFailure(
            "invalid_publication",
            "每日主题索引格式无效。",
            retryable=False,
        )
    else:
        previous = raw
    return (previous + 1) % theme_count


def _movie_results(raw: Sequence[Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise _DailyThemeFailure("invalid_provider_payload", "TMDb 每日主题包含无效电影对象。")
        movie = dict(item)
        _movie_id(movie)
        values.append(movie)
    return values


def _movie_id(movie: Mapping[str, Any]) -> int:
    value = _optional_positive_int(movie.get("id"))
    if value is None:
        raise _DailyThemeFailure("invalid_provider_payload", "TMDb 电影对象缺少有效 ID。")
    return value


def _optional_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _eligible(movie: Mapping[str, Any], excluded: set[str]) -> bool:
    title = str(movie.get("title") or movie.get("name") or "")
    return bool(
        str(_movie_id(movie)) not in excluded
        and movie.get("poster_path")
        and _contains_chinese(title)
        and str(movie.get("overview") or "").strip()
    )


def _contains_chinese(value: str) -> bool:
    return any(
        "\u3400" <= char <= "\u4dbf" or "\u4e00" <= char <= "\u9fff" or "\uf900" <= char <= "\ufaff" for char in value
    )


def _cast(movie: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    credits = movie.get("credits")
    if credits is None:
        return []
    if not isinstance(credits, Mapping):
        raise _DailyThemeFailure("invalid_provider_payload", "TMDb 电影详情 credits 格式无效。")
    raw_cast = credits.get("cast", [])
    if not isinstance(raw_cast, list):
        raise _DailyThemeFailure("invalid_provider_payload", "TMDb 电影详情 cast 格式无效。")
    cast: list[Mapping[str, Any]] = []
    for actor in raw_cast[:10]:
        if not isinstance(actor, Mapping):
            raise _DailyThemeFailure("invalid_provider_payload", "TMDb 电影详情包含无效演员对象。")
        cast.append(actor)
    return cast


def _recommendation(movie: Mapping[str, Any], actor_names: Mapping[int, str]) -> dict[str, Any]:
    cast = []
    for actor in _cast(movie):
        actor_id = _optional_positive_int(actor.get("id"))
        fallback_name = actor.get("name")
        chinese_name = actor_names.get(actor_id) if actor_id is not None else None
        cast.append(
            {
                "id": actor.get("id"),
                "name": fallback_name,
                "name_cn": chinese_name if chinese_name and _contains_chinese(chinese_name) else fallback_name,
                "profile_path": actor.get("profile_path"),
                "character": actor.get("character"),
            }
        )
    return {
        "id": movie["id"],
        "title": movie.get("title"),
        "overview": movie.get("overview"),
        "poster_path": movie.get("poster_path"),
        "release_date": movie.get("release_date"),
        "vote_average": movie.get("vote_average"),
        "cast": cast,
        "media_type": "movie",
    }


__all__ = [
    "DAILY_THEME_REPLENISHMENT_EFFECT_TYPE",
    "DailyThemeDefaults",
    "DailyThemeProgress",
    "DailyThemeProvider",
    "DailyThemePoolMutationResult",
    "DailyThemePublishResult",
    "DailyThemeRepository",
    "DailyThemeService",
    "DailyThemeSnapshot",
]
