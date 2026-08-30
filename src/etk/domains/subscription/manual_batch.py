from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import text

from etk.domains.media.subscription_projection import (
    SqlAlchemySubscriptionMediaProjection,
    SubscriptionMediaKey,
)
from etk.platform.db import SqlAlchemyUnitOfWork

from .runtime_repository import SubscriptionQuotaExhaustedError


class ManualSubscriptionSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


class ManualSubscriptionTmdbPort(Protocol):
    def movie(self, tmdb_id: int) -> Mapping[str, Any]: ...

    def tv(self, tmdb_id: int) -> Mapping[str, Any]: ...

    def season(self, tmdb_id: int, season_number: int) -> Mapping[str, Any]: ...


class ManualSubscriptionEffectsPort(Protocol):
    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool: ...

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool: ...


class ManualSubscriptionQuotaPort(Protocol):
    def remaining(self, *, quota_date: date, daily_cap: int) -> int: ...


class ManualSubscriptionProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class ManualSubscriptionWatchlistPort(Protocol):
    def add_watchlist_item(self, tmdb_id: str, item_name: str) -> None: ...


class ManualSubscriptionRepositoryPort(Protocol):
    def media(self, tmdb_id: str, item_type: str) -> Mapping[str, Any] | None: ...

    def series_title(self, tmdb_id: str) -> str | None: ...

    def season_key(self, parent_series_tmdb_id: str, season_number: int) -> SubscriptionMediaKey | None: ...

    def set_status(
        self,
        record: Mapping[str, Any],
        status: str,
        source: Mapping[str, Any] | None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ManualSubscriptionBatchResult:
    requested: int
    processed: int
    business_skipped: int
    provider_failures: tuple[Mapping[str, Any], ...]
    quota_exhausted: bool
    message: str

    @property
    def status(self) -> str:
        if self.provider_failures:
            return "partial" if self.processed > 0 else "failed"
        return "succeeded"

    @property
    def ok(self) -> bool:
        return not self.provider_failures

    def details(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "processed": self.processed,
            "business_skipped": self.business_skipped,
            "provider_failures": [dict(item) for item in self.provider_failures],
            "quota_exhausted": self.quota_exhausted,
            "retryable": False,
        }


class SqlAlchemyManualSubscriptionRepository:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media: SqlAlchemySubscriptionMediaProjection,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media = media

    def media(self, tmdb_id: str, item_type: str) -> Mapping[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, original_title, release_date,
                               poster_path, backdrop_path, overview,
                               parent_series_tmdb_id, season_number
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    {"tmdb_id": tmdb_id, "item_type": item_type},
                )
                .mappings()
                .first()
            )
            return dict(row) if row is not None else None

    def series_title(self, tmdb_id: str) -> str | None:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT title
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                    LIMIT 1
                    """
                ),
                {"tmdb_id": tmdb_id},
            )
            return str(value) if value else None

    def season_key(self, parent_series_tmdb_id: str, season_number: int) -> SubscriptionMediaKey | None:
        with self._unit_of_work() as uow:
            return self._media.lock_season_key(
                uow,
                parent_series_tmdb_id=parent_series_tmdb_id,
                season_number=season_number,
            )

    def set_status(
        self,
        record: Mapping[str, Any],
        status: str,
        source: Mapping[str, Any] | None,
    ) -> None:
        key = SubscriptionMediaKey.from_mapping(record)
        with self._unit_of_work() as uow:
            self._media.set_status(uow, key, status, record, source, None)
            uow.commit()


class ManualSubscriptionBatchService:
    def __init__(
        self,
        repository: ManualSubscriptionRepositoryPort,
        settings: ManualSubscriptionSettingsPort,
        tmdb: ManualSubscriptionTmdbPort,
        effects: ManualSubscriptionEffectsPort,
        quota: ManualSubscriptionQuotaPort,
        watchlist: ManualSubscriptionWatchlistPort,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._tmdb = tmdb
        self._effects = effects
        self._quota = quota
        self._watchlist = watchlist
        self._clock = clock

    def execute(
        self,
        subscribe_requests: Sequence[Mapping[str, Any]],
        progress: ManualSubscriptionProgress,
    ) -> ManualSubscriptionBatchResult:
        requests = tuple(dict(item) for item in subscribe_requests)
        total = len(requests)
        progress.update(0, "正在准备手动订阅任务...")
        if not requests:
            message = "任务完成：没有需要处理的项目。"
            progress.update(100, message)
            return ManualSubscriptionBatchResult(0, 0, 0, (), False, message)

        watchlist_config = _mapping(self._settings.load("watchlist_config"))
        mp_config = _mapping(self._settings.load("mp_config"))
        daily_cap = _daily_cap(mp_config)
        provider_failures: list[Mapping[str, Any]] = []
        processed = 0
        business_skipped = 0
        quota_exhausted = False

        for index, request in enumerate(requests):
            progress.checkpoint()
            tmdb_id = _optional_text(request.get("tmdb_id"))
            item_type = _optional_text(request.get("item_type"))
            title = _optional_text(request.get("title")) or f"ID: {tmdb_id or ''}"
            if not tmdb_id or not item_type or item_type not in {"Movie", "Season", "Series"}:
                business_skipped += 1
                continue
            progress.update(int((index / total) * 100), f"({index + 1}/{total}) 正在处理: {title}")
            if self._remaining_quota(daily_cap) <= 0:
                quota_exhausted = True
                break
            source = _source(request)
            try:
                if item_type == "Movie":
                    success, skipped = self._movie(tmdb_id, title, source, mp_config, progress)
                    if skipped:
                        business_skipped += 1
                        continue
                    if not success:
                        provider_failures.append(_provider_failure("create", tmdb_id, "Movie", None))
                elif item_type == "Season":
                    success, skipped = self._season(
                        request,
                        tmdb_id,
                        title,
                        source,
                        watchlist_config,
                        mp_config,
                        progress,
                    )
                    if skipped:
                        business_skipped += 1
                        continue
                    if not success:
                        provider_failures.append(
                            _provider_failure("create", tmdb_id, "Season", request.get("season_number"))
                        )
                else:
                    failures_before = len(provider_failures)
                    success = self._series(
                        request,
                        tmdb_id,
                        title,
                        source,
                        watchlist_config,
                        mp_config,
                        provider_failures,
                        progress,
                    )
                    if not success and len(provider_failures) == failures_before:
                        business_skipped += 1
            except _QuotaDepleted as exhausted:
                quota_exhausted = True
                if exhausted.item_processed:
                    processed += 1
                break
            except SubscriptionQuotaExhaustedError:
                quota_exhausted = True
                break
            if success:
                processed += 1

        message = f"  ➜ 手动订阅任务完成，成功处理 {processed}/{total} 个项目。"
        progress.update(100, message)
        return ManualSubscriptionBatchResult(
            total,
            processed,
            business_skipped,
            tuple(provider_failures),
            quota_exhausted,
            message,
        )

    def _movie(
        self,
        tmdb_id: str,
        title: str,
        source: Mapping[str, Any] | None,
        mp_config: Mapping[str, Any],
        progress: ManualSubscriptionProgress,
    ) -> tuple[bool, bool]:
        movie_id = _integer(tmdb_id)
        try:
            details = self._tmdb.movie(movie_id)
        except RuntimeError:
            return False, True
        if not _movie_subscribable(details, mp_config, self._today()):
            return False, True
        payload = {"name": title, "tmdbid": movie_id, "type": "电影"}
        progress.checkpoint()
        if not self._effects.create_subscription(
            payload,
            consume_quota=True,
            apply_local_continuation=False,
        ):
            return False, False
        self._repository.set_status(
            {
                "tmdb_id": tmdb_id,
                "item_type": "Movie",
                "title": title,
                "original_title": details.get("original_title"),
                "release_date": details.get("release_date"),
                "poster_path": details.get("poster_path"),
                "backdrop_path": details.get("backdrop_path"),
                "overview": details.get("overview"),
            },
            "SUBSCRIBED",
            source,
        )
        return True, False

    def _season(
        self,
        request: Mapping[str, Any],
        tmdb_id: str,
        title: str,
        source: Mapping[str, Any] | None,
        watchlist_config: Mapping[str, Any],
        mp_config: Mapping[str, Any],
        progress: ManualSubscriptionProgress,
    ) -> tuple[bool, bool]:
        parent_id = _optional_text(request.get("series_tmdb_id")) or _optional_text(
            request.get("parent_series_tmdb_id")
        )
        season_number = _optional_integer(request.get("season_number"))
        existing = None
        if not parent_id:
            existing = self._repository.media(tmdb_id, "Season")
            if existing is not None:
                parent_id = _optional_text(existing.get("parent_series_tmdb_id"))
                if season_number is None:
                    season_number = _optional_integer(existing.get("season_number"))
        if not parent_id or season_number is None or season_number <= 0:
            return False, True
        provider_id = parent_id
        numeric_provider_id = _integer(provider_id)
        if numeric_provider_id <= 0:
            return False, True
        series_title = self._repository.series_title(provider_id) or title
        completed = self._series_completed(numeric_provider_id)
        payload: dict[str, Any] = {
            "name": series_title,
            "tmdbid": numeric_provider_id,
            "type": "电视剧",
            "season": season_number,
        }
        _apply_wash_flags(payload, watchlist_config, mp_config, force_full=completed)
        if not (bool(watchlist_config.get("tg_channel_tracking")) and not completed):
            progress.checkpoint()
            if not self._effects.create_subscription(
                payload,
                consume_quota=True,
                apply_local_continuation=False,
            ):
                return False, False
        season_key = self._repository.season_key(provider_id, season_number)
        local_id = season_key.tmdb_id if season_key is not None else f"{provider_id}_S{season_number}"
        record = dict(existing or {})
        record.update(
            {
                "tmdb_id": local_id,
                "item_type": "Season",
                "title": record.get("title") or title,
                "parent_series_tmdb_id": provider_id,
                "season_number": season_number,
            }
        )
        self._repository.set_status(record, "SUBSCRIBED", source)
        return True, False

    def _series(
        self,
        request: Mapping[str, Any],
        tmdb_id: str,
        title: str,
        source: Mapping[str, Any] | None,
        watchlist_config: Mapping[str, Any],
        mp_config: Mapping[str, Any],
        provider_failures: list[Mapping[str, Any]],
        progress: ManualSubscriptionProgress,
    ) -> bool:
        series_id = _integer(tmdb_id)
        details = self._tmdb.tv(series_id)
        if not details:
            return False
        series_name = _optional_text(details.get("name")) or title
        raw_seasons = details.get("seasons")
        seasons = (
            sorted(
                (
                    dict(season)
                    for season in raw_seasons
                    if isinstance(raw_seasons, Sequence)
                    and not isinstance(raw_seasons, str | bytes)
                    and isinstance(season, Mapping)
                    and (_optional_integer(season.get("season_number")) or 0) > 0
                ),
                key=lambda season: int(season["season_number"]),
            )
            if isinstance(raw_seasons, Sequence) and not isinstance(raw_seasons, str | bytes)
            else []
        )
        if not seasons:
            payload: dict[str, Any] = {"name": series_name, "tmdbid": series_id, "type": "电视剧"}
            _apply_wash_flags(payload, watchlist_config, mp_config, force_full=False)
            progress.checkpoint()
            try:
                submitted = self._effects.create_subscription(
                    payload,
                    consume_quota=True,
                    apply_local_continuation=False,
                )
            except SubscriptionQuotaExhaustedError as exc:
                raise _QuotaDepleted(False) from exc
            if not submitted:
                provider_failures.append(_provider_failure("create", tmdb_id, "Series", None))
            return submitted

        self._watchlist.add_watchlist_item(tmdb_id, series_name)
        any_success = False
        for season in seasons:
            progress.checkpoint()
            season_number = int(season["season_number"])
            air_date = _optional_text(season.get("air_date"))
            season_poster = _optional_text(season.get("poster_path"))
            if not air_date:
                deep = self._tmdb.season(series_id, season_number)
                air_date = _optional_text(deep.get("air_date"))
                if not air_date:
                    episodes = deep.get("episodes")
                    if isinstance(episodes, Sequence) and not isinstance(episodes, str | bytes):
                        dates = sorted(
                            value
                            for episode in episodes
                            if isinstance(episode, Mapping)
                            and (value := _optional_text(episode.get("air_date"))) is not None
                        )
                        air_date = dates[0] if dates else None
                season_poster = season_poster or _optional_text(deep.get("poster_path"))
                if not season.get("overview"):
                    season["overview"] = deep.get("overview")
            record = {
                "tmdb_id": str(season.get("id") or f"{tmdb_id}_S{season_number}"),
                "item_type": "Season",
                "title": season.get("name") or f"第 {season_number} 季",
                "parent_series_tmdb_id": tmdb_id,
                "season_number": season_number,
                "release_date": air_date,
                "poster_path": season_poster or details.get("poster_path"),
                "backdrop_path": details.get("backdrop_path"),
                "overview": season.get("overview"),
            }
            if _future_or_missing(air_date, self._today()):
                self._repository.set_status(record, "PENDING_RELEASE", source)
                any_success = True
                continue

            pending, fake_total = _auto_pending(season, watchlist_config, self._today())
            completed = False if pending else _series_details_completed(details)
            payload = {
                "name": series_name,
                "tmdbid": series_id,
                "type": "电视剧",
                "season": season_number,
            }
            if not pending:
                _apply_wash_flags(payload, watchlist_config, mp_config, force_full=completed)
            tg_owned = bool(watchlist_config.get("tg_channel_tracking")) and not completed
            if not tg_owned:
                progress.checkpoint()
                try:
                    created = self._effects.create_subscription(
                        payload,
                        consume_quota=True,
                        apply_local_continuation=False,
                    )
                except SubscriptionQuotaExhaustedError as exc:
                    raise _QuotaDepleted(any_success) from exc
                if not created:
                    provider_failures.append(_provider_failure("create", tmdb_id, "Season", season_number))
                    continue
                if pending:
                    progress.checkpoint()
                    if not self._effects.update_subscription_status(
                        tmdb_id,
                        season=season_number,
                        status="P",
                        total_episodes=fake_total,
                    ):
                        provider_failures.append(_provider_failure("update_status", tmdb_id, "Season", season_number))
                        continue
            self._repository.set_status(record, "SUBSCRIBED", source)
            any_success = True
        return any_success

    def _remaining_quota(self, daily_cap: int) -> int:
        return self._quota.remaining(quota_date=self._today(), daily_cap=daily_cap)

    def _today(self) -> date:
        current = self._clock()
        if current.tzinfo is None:
            return current.date()
        return current.astimezone(ZoneInfo("Asia/Shanghai")).date()

    def _series_completed(self, tmdb_id: int) -> bool:
        try:
            return _series_details_completed(self._tmdb.tv(tmdb_id))
        except Exception:
            return False


class _QuotaDepleted(RuntimeError):
    def __init__(self, item_processed: bool) -> None:
        super().__init__("subscription quota is exhausted")
        self.item_processed = item_processed


def _provider_failure(action: str, tmdb_id: object, item_type: str, season: object) -> dict[str, Any]:
    return {
        "action": action,
        "tmdb_id": str(tmdb_id),
        "item_type": item_type,
        "season": _optional_integer(season),
    }


def _source(request: Mapping[str, Any]) -> Mapping[str, Any] | None:
    user_id = _optional_text(request.get("user_id"))
    return {"type": "user_request", "user_id": user_id} if user_id else None


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _daily_cap(config: Mapping[str, Any]) -> int:
    value = config.get("resubscribe_daily_cap", 200)
    if isinstance(value, bool):
        return 200
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 200


def _movie_subscribable(details: Mapping[str, Any], config: Mapping[str, Any], today: date) -> bool:
    releases = details.get("release_dates")
    results = releases.get("results") if isinstance(releases, Mapping) else None
    if not isinstance(results, Sequence) or isinstance(results, str | bytes):
        return False
    delay_days = _nonnegative_int(config.get("delay_subscription_days"), default=0)
    earliest: date | None = None
    for country in results:
        country_releases = country.get("release_dates") if isinstance(country, Mapping) else None
        if not isinstance(country_releases, Sequence) or isinstance(country_releases, str | bytes):
            continue
        for release in country_releases:
            if not isinstance(release, Mapping):
                continue
            release_type = _optional_integer(release.get("type"))
            if release_type in {4, 5}:
                return True
            if release_type not in {1, 2, 3}:
                continue
            try:
                candidate = _date_value(str(release.get("release_date") or "").split("T", 1)[0])
            except ValueError:
                continue
            if earliest is None or candidate < earliest:
                earliest = candidate
    return earliest is not None and (today - earliest).days >= delay_days


def _series_details_completed(details: Mapping[str, Any]) -> bool:
    return str(details.get("status") or "").strip() in {"Ended", "Canceled"}


def _auto_pending(
    season: Mapping[str, Any],
    watchlist_config: Mapping[str, Any],
    today: date,
) -> tuple[bool, int]:
    config = watchlist_config.get("auto_pending")
    if not isinstance(config, Mapping) or not bool(config.get("enabled")):
        return False, 0
    try:
        threshold_days = int(config.get("days", 30))
        threshold_episodes = int(config.get("episodes", 1))
        fake_total = int(config.get("default_total_episodes", 99))
        air_date = _date_value(str(season.get("air_date") or ""))
        episode_count = int(season.get("episode_count", 0) or 0)
    except (TypeError, ValueError):
        return False, 0
    days = (today - air_date).days
    return 0 <= days <= threshold_days and episode_count <= threshold_episodes, fake_total


def _apply_wash_flags(
    payload: dict[str, Any],
    watchlist_config: Mapping[str, Any],
    mp_config: Mapping[str, Any],
    *,
    force_full: bool,
) -> None:
    assistant = mp_config.get("subscribe_assistant")
    mode = str(assistant.get("best_version_type") or "").strip() if isinstance(assistant, Mapping) else ""
    if not mode:
        fallback = watchlist_config.get("subscribe_assistant")
        mode = str(fallback.get("best_version_type") or "").strip() if isinstance(fallback, Mapping) else ""
    payload.pop("best_version", None)
    payload.pop("best_version_full", None)
    if mode == "tv":
        payload.update(best_version=1, best_version_full=1)
    elif force_full and mode in {"tv_episode", "completed_full"}:
        payload.update(best_version=1, best_version_full=1)
    elif mode == "tv_episode":
        payload["best_version"] = 1


def _date_value(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _future_or_missing(value: str | None, today: date) -> bool:
    if not value:
        return True
    try:
        return _date_value(value) > today
    except ValueError:
        return False


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ValueError("TMDb identity must be an integer")
    return int(value)


def _optional_integer(value: object) -> int | None:
    if value is None or value == "" or isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _nonnegative_int(value: object, *, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return default
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


def _optional_text(value: object) -> str | None:
    text_value = str(value).strip() if value is not None else ""
    return text_value or None


__all__ = [
    "ManualSubscriptionBatchResult",
    "ManualSubscriptionBatchService",
    "ManualSubscriptionEffectsPort",
    "ManualSubscriptionProgress",
    "ManualSubscriptionQuotaPort",
    "ManualSubscriptionRepositoryPort",
    "ManualSubscriptionSettingsPort",
    "ManualSubscriptionTmdbPort",
    "ManualSubscriptionWatchlistPort",
    "SqlAlchemyManualSubscriptionRepository",
]
