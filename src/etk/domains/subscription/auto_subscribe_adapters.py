from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from etk.integrations import ProviderDisposition, ProviderResponse, TelegramBotPort
from etk.processes.p7_subscription_effects import bind_moviepilot_operation_occurrence

from .auto_subscribe import (
    AutoSourceDisposition,
    AutoSourceError,
    AutoSourceResult,
    AutoSubscriptionItemResult,
    AutoSubscriptionProgress,
)
from .runtime_repository import SubscriptionQuotaExhaustedError


class AutoMoviePilotEffectsPort(Protocol):
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


class AutoSubscriptionNotificationDirectoryPort(Protocol):
    def user_chat_id(self, user_id: str) -> str | None: ...

    def admin_chat_ids(self) -> Sequence[str]: ...


class AutoSubscriptionAdapterSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


class AutoSubscriptionSeriesTmdbPort(Protocol):
    def tv(self, tmdb_id: int) -> Mapping[str, Any]: ...


class AutoSubscriptionMoviePilotGateway:
    def __init__(
        self,
        effects: AutoMoviePilotEffectsPort,
        settings: AutoSubscriptionAdapterSettingsPort,
        tmdb: AutoSubscriptionSeriesTmdbPort,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._effects = effects
        self._settings = settings
        self._tmdb = tmdb
        self._clock = clock

    def update_status(
        self,
        tmdb_id: str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None = None,
        operation_occurrence: str,
    ) -> bool:
        with bind_moviepilot_operation_occurrence(operation_occurrence):
            return self._effects.update_subscription_status(
                tmdb_id,
                season=season,
                status=status,
                total_episodes=total_episodes,
            )

    def create_movie(self, tmdb_id: str, title: str, *, operation_occurrence: str) -> bool:
        with bind_moviepilot_operation_occurrence(operation_occurrence):
            return self._effects.create_subscription(
                {"name": title, "tmdbid": int(tmdb_id), "type": "电影"},
                consume_quota=False,
                apply_local_continuation=False,
            )

    def subscribe_item(
        self,
        item: Mapping[str, Any],
        progress: AutoSubscriptionProgress,
    ) -> AutoSubscriptionItemResult:
        occurrence = _required_text(item.get("operation_occurrence"), "operation_occurrence")
        item_type = _required_text(item.get("item_type"), "item_type")
        tmdb_id = _required_text(item.get("tmdb_id"), "tmdb_id")
        title = _required_text(item.get("title") or tmdb_id, "title")
        with bind_moviepilot_operation_occurrence(occurrence):
            if item_type == "Movie":
                progress.checkpoint()
                return self._create(
                    {"name": title, "tmdbid": int(tmdb_id), "type": "电影"},
                    tmdb_id=tmdb_id,
                    item_type="Movie",
                    season=None,
                )
            if item_type == "Season":
                return self._season(item, title, progress)
        return AutoSubscriptionItemResult(
            False,
            (_provider_failure("unsupported_auto_item", tmdb_id, item_type, item.get("season_number")),),
        )

    def _season(
        self,
        item: Mapping[str, Any],
        title: str,
        progress: AutoSubscriptionProgress,
    ) -> AutoSubscriptionItemResult:
        parent_id = _required_text(
            item.get("parent_series_tmdb_id") or item.get("series_tmdb_id"),
            "parent_series_tmdb_id",
        )
        season = _positive_int(item.get("season_number"), "season_number")
        watchlist = _mapping(self._settings.load("watchlist_config"))
        mp_config = _mapping(self._settings.load("mp_config"))
        try:
            details = dict(self._tmdb.tv(int(parent_id)))
        except Exception:
            details = {}
        pending, fake_total = _auto_pending(details, season, watchlist, self._today())
        completed = False if pending else str(details.get("status") or "").strip() in {"Ended", "Canceled"}
        payload: dict[str, Any] = {
            "name": title,
            "tmdbid": int(parent_id),
            "type": "电视剧",
            "season": season,
        }
        if not pending:
            _apply_wash_flags(payload, watchlist, mp_config, force_full=completed)
        if bool(watchlist.get("tg_channel_tracking")) and not completed:
            return AutoSubscriptionItemResult(True, action_type="TG频道追更")
        progress.checkpoint()
        created = self._create(
            payload,
            tmdb_id=parent_id,
            item_type="Season",
            season=season,
        )
        if not created.processed or created.provider_failures or not pending:
            return created
        progress.checkpoint()
        if not self._effects.update_subscription_status(
            parent_id,
            season=season,
            status="P",
            total_episodes=fake_total,
        ):
            return AutoSubscriptionItemResult(
                False,
                (_provider_failure("update_status", parent_id, "Season", season),),
            )
        return created

    def _create(
        self,
        payload: Mapping[str, Any],
        *,
        tmdb_id: str,
        item_type: str,
        season: int | None,
    ) -> AutoSubscriptionItemResult:
        try:
            confirmed = self._effects.create_subscription(
                payload,
                consume_quota=True,
                apply_local_continuation=False,
            )
        except SubscriptionQuotaExhaustedError:
            return AutoSubscriptionItemResult(False, quota_exhausted=True)
        if confirmed:
            return AutoSubscriptionItemResult(True)
        return AutoSubscriptionItemResult(
            False,
            (_provider_failure("create", tmdb_id, item_type, season),),
        )

    def _today(self) -> date:
        current = self._clock()
        if current.tzinfo is None:
            return current.date()
        return current.astimezone(ZoneInfo("Asia/Shanghai")).date()


class CallableOrderedAutoSubscriptionSources:
    """Normalize narrow source callables without coupling the policy to their implementation."""

    def __init__(
        self,
        callables: Mapping[str, Callable[[Mapping[str, Any]], object]],
        *,
        replay_safe_sources: Sequence[str] = (),
    ) -> None:
        self._callables = dict(callables)
        self._replay_safe_sources = frozenset(str(value).strip() for value in replay_safe_sources)

    def replay_safe(self, source_type: str) -> bool:
        return str(source_type or "").strip() in self._replay_safe_sources

    def attempt(
        self,
        source_type: str,
        context: Mapping[str, Any],
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult:
        progress.checkpoint()
        handler = self._callables.get(source_type)
        if handler is None:
            return AutoSourceResult.miss()
        try:
            result = handler(dict(context))
        except Exception as exc:
            disposition = _exception_disposition(exc)
            raise AutoSourceError(
                str(exc) or f"{source_type} source failed",
                disposition=disposition,
                error={
                    "action": source_type,
                    "tmdb_id": str(context.get("tmdb_id") or ""),
                    "item_type": str(context.get("item_type") or ""),
                    "error_type": type(exc).__name__,
                    "indeterminate": disposition is AutoSourceDisposition.INDETERMINATE,
                },
            ) from exc
        return _source_result(source_type, result)


class TelegramAutoSubscriptionNotifications:
    def __init__(
        self,
        directory: AutoSubscriptionNotificationDirectoryPort,
        telegram: TelegramBotPort,
    ) -> None:
        self._directory = directory
        self._telegram = telegram

    def success(self, user_id: str, titles: Sequence[str]) -> None:
        chat_id = self._directory.user_chat_id(user_id)
        if not chat_id:
            return
        items = "\n".join(f"- {title}" for title in titles)
        _confirmed_notification(
            self._telegram.send_message(
                chat_id,
                f"您的 {len(titles)} 个订阅已成功处理：\n{items}",
            )
        )

    def rejected(self, user_id: str, titles: Sequence[str]) -> None:
        chat_id = self._directory.user_chat_id(user_id)
        if not chat_id:
            return
        items = "\n".join(f"- {title}" for title in titles)
        _confirmed_notification(
            self._telegram.send_message(
                chat_id,
                f"您的部分订阅请求未被处理：\n{items}",
            )
        )

    def summary(self, details: Sequence[Mapping[str, Any]], *, quota_exhausted: bool) -> None:
        if details:
            rows = "\n".join(
                f"- [{str(item.get('action') or '未知')}] {str(item.get('title') or item.get('tmdb_id') or '')}"
                for item in details
            )
            message = f"统一订阅任务完成，成功处理 {len(details)} 项：\n{rows}"
        else:
            message = "统一订阅任务完成，无成功处理的订阅项。"
        if quota_exhausted:
            message += "\n每日订阅配额已用尽，部分项目可能未处理。"
        for chat_id in self._directory.admin_chat_ids():
            _confirmed_notification(self._telegram.send_message(chat_id, message, disable_notification=True))


def _source_result(source_type: str, value: object) -> AutoSourceResult:
    if isinstance(value, AutoSourceResult):
        return value
    if isinstance(value, ProviderResponse):
        if value.disposition is ProviderDisposition.CONFIRMED:
            return AutoSourceResult.confirmed(source_type)
        disposition = {
            ProviderDisposition.REJECTED: AutoSourceDisposition.REJECTED,
            ProviderDisposition.RETRYABLE: AutoSourceDisposition.RETRYABLE,
            ProviderDisposition.INDETERMINATE: AutoSourceDisposition.INDETERMINATE,
        }.get(value.disposition, AutoSourceDisposition.MISS)
        return AutoSourceResult(disposition, source_type)
    if isinstance(value, bool):
        return AutoSourceResult.confirmed(source_type) if value else AutoSourceResult.miss()
    if not isinstance(value, Mapping):
        return AutoSourceResult.miss()
    payload = dict(value)
    if bool(payload.get("success") or payload.get("ok")):
        return AutoSourceResult.confirmed(str(payload.get("action_type") or source_type))
    raw = str(payload.get("disposition") or payload.get("status") or "").strip().lower()
    disposition = {
        "confirmed": AutoSourceDisposition.CONFIRMED,
        "rejected": AutoSourceDisposition.REJECTED,
        "retryable": AutoSourceDisposition.RETRYABLE,
        "indeterminate": AutoSourceDisposition.INDETERMINATE,
    }.get(raw, AutoSourceDisposition.MISS)
    error = None
    if disposition is AutoSourceDisposition.INDETERMINATE:
        error = {
            "action": source_type,
            "error_type": str(payload.get("error_type") or "source_indeterminate"),
            "indeterminate": True,
        }
    return AutoSourceResult(disposition, str(payload.get("action_type") or source_type), error)


def _auto_pending(
    details: Mapping[str, Any],
    season_number: int,
    watchlist: Mapping[str, Any],
    today: date,
) -> tuple[bool, int]:
    config = watchlist.get("auto_pending")
    if not isinstance(config, Mapping) or not bool(config.get("enabled")):
        return False, 0
    try:
        threshold_days = int(config.get("days", 30))
        threshold_episodes = int(config.get("episodes", 1))
        fake_total = int(config.get("default_total_episodes", 99))
    except (TypeError, ValueError):
        return False, 0
    seasons = details.get("seasons")
    if not isinstance(seasons, Sequence) or isinstance(seasons, str | bytes):
        return False, 0
    target = next(
        (
            item
            for item in seasons
            if isinstance(item, Mapping) and _optional_int(item.get("season_number")) == season_number
        ),
        None,
    )
    if target is None:
        return False, 0
    try:
        air_date = datetime.strptime(str(target.get("air_date") or ""), "%Y-%m-%d").date()
        episode_count = int(target.get("episode_count", 0) or 0)
    except (TypeError, ValueError):
        return False, 0
    days = (today - air_date).days
    return 0 <= days <= threshold_days and episode_count <= threshold_episodes, fake_total


def _apply_wash_flags(
    payload: dict[str, Any],
    watchlist: Mapping[str, Any],
    mp_config: Mapping[str, Any],
    *,
    force_full: bool,
) -> None:
    assistant = mp_config.get("subscribe_assistant")
    mode = str(assistant.get("best_version_type") or "").strip() if isinstance(assistant, Mapping) else ""
    if not mode:
        fallback = watchlist.get("subscribe_assistant")
        mode = str(fallback.get("best_version_type") or "").strip() if isinstance(fallback, Mapping) else ""
    payload.pop("best_version", None)
    payload.pop("best_version_full", None)
    if mode == "tv" or (force_full and mode in {"tv_episode", "completed_full"}):
        payload.update(best_version=1, best_version_full=1)
    elif mode == "tv_episode":
        payload["best_version"] = 1


def _provider_failure(action: str, tmdb_id: object, item_type: str, season: object) -> dict[str, Any]:
    return {
        "action": action,
        "tmdb_id": str(tmdb_id),
        "item_type": str(item_type),
        "season": _optional_int(season),
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _positive_int(value: object, field: str) -> int:
    parsed = _optional_int(value)
    if parsed is None or parsed <= 0:
        raise ValueError(f"auto-subscribe {field} must be a positive integer")
    return parsed


def _optional_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"auto-subscribe {field} must not be empty")
    return normalized


def _exception_disposition(exc: Exception) -> AutoSourceDisposition:
    disposition = getattr(exc, "disposition", None)
    if disposition is ProviderDisposition.RETRYABLE:
        return AutoSourceDisposition.RETRYABLE
    if disposition is ProviderDisposition.REJECTED:
        return AutoSourceDisposition.REJECTED
    return AutoSourceDisposition.INDETERMINATE


def _confirmed_notification(value: ProviderResponse[object]) -> None:
    if value.disposition is not ProviderDisposition.CONFIRMED:
        raise RuntimeError(f"Telegram notification was not confirmed: {value.disposition.value}")


__all__ = [
    "AutoSubscriptionMoviePilotGateway",
    "AutoSubscriptionAdapterSettingsPort",
    "AutoSubscriptionNotificationDirectoryPort",
    "AutoSubscriptionSeriesTmdbPort",
    "CallableOrderedAutoSubscriptionSources",
    "TelegramAutoSubscriptionNotifications",
]
