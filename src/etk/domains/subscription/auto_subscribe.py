from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, Protocol
from zoneinfo import ZoneInfo


class AutoSubscriptionSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


class AutoSubscriptionRepositoryPort(Protocol):
    def promote_pending_to_wanted(self) -> int: ...

    def movies_to_revive(self) -> Sequence[Mapping[str, Any]]: ...

    def movies_to_pause(self, *, search_window_days: int) -> Sequence[Mapping[str, Any]]: ...

    def mark_movie_revived(self, tmdb_id: str) -> None: ...

    def mark_movie_paused(self, tmdb_id: str, *, pause_days: int) -> None: ...

    def worklist(self) -> Sequence[Mapping[str, Any]]: ...

    def series_title(self, tmdb_id: str) -> str | None: ...

    def mark_subscribed(self, item: Mapping[str, Any]) -> None: ...


class AutoSubscriptionTmdbPort(Protocol):
    def movie(self, tmdb_id: int) -> Mapping[str, Any]: ...


class AutoSubscriptionQuotaPort(Protocol):
    def remaining(self, *, quota_date: date, daily_cap: int) -> int: ...


class AutoSubscriptionMoviePilotPort(Protocol):
    def update_status(
        self,
        tmdb_id: str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None = None,
    ) -> bool: ...

    def create_movie(self, tmdb_id: str, title: str) -> bool: ...

    def subscribe_item(
        self,
        item: Mapping[str, Any],
        progress: AutoSubscriptionProgress,
    ) -> AutoSubscriptionItemResult: ...


class AutoSubscriptionProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class AutoSourceDisposition(StrEnum):
    MISS = "miss"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    RETRYABLE = "retryable"
    INDETERMINATE = "indeterminate"


class AutoSourceError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        disposition: AutoSourceDisposition,
        error: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.disposition = disposition
        self.error = dict(error or {})


@dataclass(frozen=True, slots=True)
class AutoSourceResult:
    disposition: AutoSourceDisposition
    action_type: str = ""
    error: Mapping[str, Any] | None = None

    @classmethod
    def miss(cls) -> AutoSourceResult:
        return cls(AutoSourceDisposition.MISS)

    @classmethod
    def confirmed(cls, action_type: str) -> AutoSourceResult:
        return cls(AutoSourceDisposition.CONFIRMED, str(action_type or "").strip())


class OrderedAutoSubscriptionSourcePort(Protocol):
    def attempt(
        self,
        source_type: str,
        context: Mapping[str, Any],
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult: ...


class AutoSubscriptionNotificationPort(Protocol):
    def success(self, user_id: str, titles: Sequence[str]) -> None: ...

    def rejected(self, user_id: str, titles: Sequence[str]) -> None: ...

    def summary(self, details: Sequence[Mapping[str, Any]], *, quota_exhausted: bool) -> None: ...


@dataclass(frozen=True, slots=True)
class AutoSubscriptionItemResult:
    processed: bool
    provider_failures: tuple[Mapping[str, Any], ...] = ()
    quota_exhausted: bool = False
    local_projection_applied: bool = True


@dataclass(frozen=True, slots=True)
class AutoSubscriptionResult:
    requested: int
    processed: int
    business_skipped: int
    provider_failures: tuple[Mapping[str, Any], ...]
    quota_exhausted: bool
    promoted: int
    revived: int
    paused: int
    message: str

    @property
    def status(self) -> str:
        if self.provider_failures:
            return "partial" if self.processed > 0 else "failed"
        return "succeeded"

    def details(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "processed": self.processed,
            "business_skipped": self.business_skipped,
            "provider_failures": [dict(item) for item in self.provider_failures],
            "quota_exhausted": self.quota_exhausted,
            "promoted": self.promoted,
            "revived": self.revived,
            "paused": self.paused,
            "retryable": False,
        }


class AutoSubscribeService:
    def __init__(
        self,
        repository: AutoSubscriptionRepositoryPort,
        settings: AutoSubscriptionSettingsPort,
        tmdb: AutoSubscriptionTmdbPort,
        quota: AutoSubscriptionQuotaPort,
        moviepilot: AutoSubscriptionMoviePilotPort,
        sources: OrderedAutoSubscriptionSourcePort,
        notifications: AutoSubscriptionNotificationPort,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._tmdb = tmdb
        self._quota = quota
        self._moviepilot = moviepilot
        self._sources = sources
        self._notifications = notifications
        self._clock = clock
        self._sleep = sleep

    def execute(self, progress: AutoSubscriptionProgress) -> AutoSubscriptionResult:
        progress.update(0, "正在加载订阅策略...")
        strategy = _mapping(self._settings.load("subscription_strategy_config"))
        mp_config = _mapping(self._settings.load("mp_config"))
        daily_cap = _daily_cap(mp_config)
        search_window = _nonnegative_int(mp_config.get("movie_search_window_days"), default=1)
        pause_days = _nonnegative_int(mp_config.get("movie_pause_days"), default=7)
        request_delay = _nonnegative_int(mp_config.get("resubscribe_delay_seconds"), default=0)

        failures: list[Mapping[str, Any]] = []
        processed = 0
        revived = 0
        paused = 0
        if search_window > 0 and pause_days > 0:
            revived, paused, maintenance_failures = self._maintain_movies(
                progress,
                search_window_days=search_window,
                pause_days=pause_days,
            )
            processed += revived + paused
            failures.extend(maintenance_failures)

        progress.checkpoint()
        promoted = self._repository.promote_pending_to_wanted()
        items = tuple(dict(item) for item in self._repository.worklist())
        if not items:
            message = "待订阅列表为空。"
            progress.update(100, message)
            return AutoSubscriptionResult(
                0,
                processed,
                0,
                tuple(failures),
                False,
                promoted,
                revived,
                paused,
                message,
            )

        sources = _source_order(strategy.get("subscription_sources"))
        business_skipped = 0
        quota_exhausted = False
        successful_notifications: dict[str, list[str]] = {}
        rejected_notifications: dict[str, list[str]] = {}
        summary: list[Mapping[str, Any]] = []
        total = len(items)
        progress.update(10, f"发现 {total} 个待处理请求...")

        for index, item in enumerate(items):
            progress.checkpoint()
            title = _text(item.get("title")) or f"ID: {_text(item.get('tmdb_id'))}"
            progress.update(int(10 + (index / total) * 85), f"({index + 1}/{total}) 正在处理: {title}")
            item_type = _text(item.get("item_type"))
            status = (_text(item.get("subscription_status")) or "NONE").upper()
            wanted = status == "WANTED"
            if item_type == "Episode":
                continue
            if wanted and item_type == "Movie" and not self._movie_is_released(item, mp_config):
                business_skipped += 1
                _collect_user_titles(rejected_notifications, item, f"{title}(原因: 不满足发行日期延迟订阅)")
                continue
            if wanted and self._remaining_quota(daily_cap) <= 0:
                quota_exhausted = True
                continue

            context = self._context(item, title)
            source_order = sources if wanted else tuple(source for source in sources if source != "mp")
            success = False
            partial_success = False
            action_type = "MP"
            local_projection_applied = False
            item_failures: list[Mapping[str, Any]] = []
            for source_type in source_order:
                progress.checkpoint()
                if source_type == "mp":
                    result = self._moviepilot.subscribe_item(context, _NestedProgress(progress))
                    item_failures.extend(result.provider_failures)
                    quota_exhausted = quota_exhausted or result.quota_exhausted
                    success = result.processed and not result.provider_failures
                    partial_success = result.processed and bool(result.provider_failures)
                    local_projection_applied = result.local_projection_applied
                    action_type = "MP"
                    if success:
                        break
                    continue
                try:
                    source_result = self._sources.attempt(source_type, context, progress)
                except AutoSourceError as exc:
                    source_result = AutoSourceResult(exc.disposition, source_type, exc.error)
                except Exception as exc:
                    source_result = AutoSourceResult(
                        AutoSourceDisposition.INDETERMINATE,
                        source_type,
                        {
                            "action": source_type,
                            "tmdb_id": str(item.get("tmdb_id") or ""),
                            "item_type": item_type,
                            "error_type": type(exc).__name__,
                            "indeterminate": True,
                        },
                    )
                if source_result.disposition is AutoSourceDisposition.CONFIRMED:
                    success = True
                    action_type = source_result.action_type or source_type
                    break
                if source_result.disposition is AutoSourceDisposition.INDETERMINATE:
                    item_failures.append(
                        source_result.error
                        or {
                            "action": source_type,
                            "tmdb_id": str(item.get("tmdb_id") or ""),
                            "item_type": item_type,
                            "season": _optional_positive_int(item.get("season_number")),
                            "indeterminate": True,
                        }
                    )
                    break

            if not success and item_failures:
                failures.extend(item_failures)
            if not success and partial_success:
                processed += 1
            if success:
                processed += 1
                if wanted and (action_type != "MP" or (item_type != "Series" and not local_projection_applied)):
                    self._repository.mark_subscribed(item)
                _collect_user_titles(successful_notifications, item, _notification_title(context))
                summary.append(
                    {
                        "tmdb_id": str(item.get("tmdb_id") or ""),
                        "item_type": item_type,
                        "title": _notification_title(context),
                        "action": action_type,
                    }
                )
            self._delay(progress, request_delay, index, total)

        self._notify(
            progress,
            successful_notifications,
            rejected_notifications,
            summary,
            quota_exhausted,
        )
        message = "统一订阅任务处理完成。"
        progress.update(100, message)
        return AutoSubscriptionResult(
            total,
            processed,
            business_skipped,
            tuple(failures),
            quota_exhausted,
            promoted,
            revived,
            paused,
            message,
        )

    def _maintain_movies(
        self,
        progress: AutoSubscriptionProgress,
        *,
        search_window_days: int,
        pause_days: int,
    ) -> tuple[int, int, list[Mapping[str, Any]]]:
        revived = 0
        paused = 0
        failures: list[Mapping[str, Any]] = []
        for item in self._repository.movies_to_revive():
            progress.checkpoint()
            tmdb_id = _required_text(item.get("tmdb_id"), "movie tmdb_id")
            title = _text(item.get("title")) or tmdb_id
            confirmed = self._moviepilot.update_status(tmdb_id, season=None, status="R")
            if not confirmed:
                progress.checkpoint()
                confirmed = self._moviepilot.create_movie(tmdb_id, title)
            if confirmed:
                self._repository.mark_movie_revived(tmdb_id)
                revived += 1
            else:
                failures.append(_provider_failure("revive", tmdb_id, "Movie", None))

        for item in self._repository.movies_to_pause(search_window_days=search_window_days):
            progress.checkpoint()
            tmdb_id = _required_text(item.get("tmdb_id"), "movie tmdb_id")
            title = _text(item.get("title")) or tmdb_id
            confirmed = self._moviepilot.update_status(tmdb_id, season=None, status="S")
            if not confirmed:
                progress.checkpoint()
                confirmed = self._moviepilot.create_movie(tmdb_id, title)
                if confirmed:
                    progress.checkpoint()
                    confirmed = self._moviepilot.update_status(tmdb_id, season=None, status="S")
            if confirmed:
                self._repository.mark_movie_paused(tmdb_id, pause_days=pause_days)
                paused += 1
            else:
                failures.append(_provider_failure("pause", tmdb_id, "Movie", None))
        return revived, paused, failures

    def _movie_is_released(self, item: Mapping[str, Any], mp_config: Mapping[str, Any]) -> bool:
        try:
            details = self._tmdb.movie(int(_required_text(item.get("tmdb_id"), "movie tmdb_id")))
        except Exception:
            return False
        return _movie_subscribable(details, mp_config, self._today())

    def _context(self, item: Mapping[str, Any], title: str) -> dict[str, Any]:
        context = dict(item)
        item_type = _text(item.get("item_type"))
        tmdb_id = _required_text(item.get("tmdb_id"), "tmdb_id")
        parent_id: str | None = None
        if item_type in {"Series", "Season", "Episode"}:
            if item_type == "Series":
                parent_id = tmdb_id
            else:
                parent_id = _text(item.get("parent_series_tmdb_id")) or _text(item.get("series_tmdb_id"))
                if not parent_id and "_" in tmdb_id:
                    parent_id = tmdb_id.split("_", 1)[0]
                if item_type == "Season" and not parent_id:
                    parent_id = tmdb_id
            series_title = self._repository.series_title(parent_id) if parent_id else None
            if series_title:
                title = series_title
        context.update(
            {
                "tmdb_id": tmdb_id,
                "item_type": item_type,
                "title": title,
                "parent_series_tmdb_id": parent_id,
                "series_tmdb_id": parent_id,
                "season_number": _optional_positive_int(item.get("season_number")),
                "year": _item_year(item),
                "missing_episode_numbers": _positive_ints(item.get("missing_episode_numbers")),
            }
        )
        return context

    def _remaining_quota(self, daily_cap: int) -> int:
        return self._quota.remaining(quota_date=self._today(), daily_cap=daily_cap)

    def _today(self) -> date:
        current = self._clock()
        if current.tzinfo is None:
            return current.date()
        return current.astimezone(ZoneInfo("Asia/Shanghai")).date()

    def _delay(self, progress: AutoSubscriptionProgress, seconds: int, index: int, total: int) -> None:
        if seconds <= 0 or index >= total - 1:
            return
        progress.checkpoint()
        self._sleep(float(seconds))
        progress.checkpoint()

    def _notify(
        self,
        progress: AutoSubscriptionProgress,
        successes: Mapping[str, Sequence[str]],
        rejected: Mapping[str, Sequence[str]],
        summary: Sequence[Mapping[str, Any]],
        quota_exhausted: bool,
    ) -> None:
        for user_id, titles in successes.items():
            progress.checkpoint()
            try:
                self._notifications.success(user_id, titles)
            except Exception:
                pass
        for user_id, titles in rejected.items():
            progress.checkpoint()
            try:
                self._notifications.rejected(user_id, titles)
            except Exception:
                pass
        progress.checkpoint()
        try:
            self._notifications.summary(summary, quota_exhausted=quota_exhausted)
        except Exception:
            pass


class _NestedProgress:
    def __init__(self, parent: AutoSubscriptionProgress) -> None:
        self._parent = parent

    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        self._parent.checkpoint()


def _provider_failure(
    action: str,
    tmdb_id: object,
    item_type: str,
    season: object,
) -> dict[str, Any]:
    return {
        "action": action,
        "tmdb_id": str(tmdb_id),
        "item_type": item_type,
        "season": _optional_positive_int(season),
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _source_order(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    return tuple(str(item).strip() for item in value if str(item).strip())


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
            release_type = _optional_positive_int(release.get("type"))
            if release_type in {4, 5}:
                return True
            if release_type not in {1, 2, 3}:
                continue
            try:
                candidate = datetime.strptime(
                    str(release.get("release_date") or "").split("T", 1)[0],
                    "%Y-%m-%d",
                ).date()
            except ValueError:
                continue
            if earliest is None or candidate < earliest:
                earliest = candidate
    return earliest is not None and (today - earliest).days >= delay_days


def _collect_user_titles(target: dict[str, list[str]], item: Mapping[str, Any], title: str) -> None:
    sources = item.get("subscription_sources_json")
    if not isinstance(sources, Sequence) or isinstance(sources, str | bytes):
        return
    for source in sources:
        if not isinstance(source, Mapping) or str(source.get("type") or "") != "user_request":
            continue
        user_id = _text(source.get("user_id"))
        if user_id:
            target.setdefault(user_id, []).append(title)


def _notification_title(context: Mapping[str, Any]) -> str:
    title = _text(context.get("title")) or _text(context.get("tmdb_id"))
    if _text(context.get("item_type")) == "Season" and (season := _optional_positive_int(context.get("season_number"))):
        return f"{title} 第 {season} 季"
    return title


def _item_year(item: Mapping[str, Any]) -> str:
    for key in ("release_date", "first_air_date", "air_date", "year"):
        if value := item.get(key):
            if match := re.search(r"((?:19|20)\d{2})", str(value)):
                return match.group(1)
    if match := re.search(r"\(((?:19|20)\d{2})\)", str(item.get("title") or "")):
        return match.group(1)
    return ""


def _positive_ints(value: object) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            import json

            value = json.loads(value)
        except Exception:
            value = re.split(r"[，,\s]+", value.strip()) if value.strip() else []
    values = value if isinstance(value, Sequence) and not isinstance(value, str | bytes) else [value]
    result: list[int] = []
    for item in values:
        parsed = _optional_positive_int(item)
        if parsed is not None and parsed not in result:
            result.append(parsed)
    return sorted(result)


def _daily_cap(config: Mapping[str, Any]) -> int:
    value = config.get("resubscribe_daily_cap", 200)
    if isinstance(value, bool):
        return 200
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 200


def _nonnegative_int(value: object, *, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return default
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


def _optional_positive_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _required_text(value: object, field: str) -> str:
    normalized = _text(value)
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = [
    "AutoSourceDisposition",
    "AutoSourceError",
    "AutoSourceResult",
    "AutoSubscribeService",
    "AutoSubscriptionItemResult",
    "AutoSubscriptionMoviePilotPort",
    "AutoSubscriptionNotificationPort",
    "AutoSubscriptionProgress",
    "AutoSubscriptionQuotaPort",
    "AutoSubscriptionRepositoryPort",
    "AutoSubscriptionResult",
    "AutoSubscriptionSettingsPort",
    "AutoSubscriptionTmdbPort",
    "OrderedAutoSubscriptionSourcePort",
]
