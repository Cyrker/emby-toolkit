from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from etk.platform import Settings


class PlaybackReportErrorKind(StrEnum):
    PLUGIN_NOT_INSTALLED = "plugin_not_installed"
    UNAVAILABLE = "unavailable"


class PlaybackReportError(RuntimeError):
    def __init__(self, kind: PlaybackReportErrorKind) -> None:
        super().__init__(kind.value)
        self.kind = kind


class PlaybackReportingPort(Protocol):
    def report(self, *, user_id: str | None, days: int) -> Sequence[Mapping[str, Any]]: ...

    def item_details(
        self,
        *,
        user_id: str,
        item_ids: Sequence[str],
    ) -> Sequence[Mapping[str, Any]]: ...


class DashboardMediaProjection(Protocol):
    def dashboard_aggregation(self, emby_ids: Sequence[str]) -> dict[str, dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class PlaybackHttpResult:
    payload: dict[str, Any]
    status_code: int = 200


class PlaybackReportingService:
    def __init__(
        self,
        provider: PlaybackReportingPort,
        projection: DashboardMediaProjection,
        *,
        settings: Callable[[], Settings],
    ) -> None:
        self._provider = provider
        self._projection = projection
        self._settings = settings

    def personal_report(
        self,
        user_id: str,
        *,
        days: int,
        media_type: str,
    ) -> PlaybackHttpResult:
        normalized_days = _days(days)
        normalized_type = str(media_type or "all")
        if normalized_type not in {"all", "Movie", "Episode", "Audio", "Video"}:
            return PlaybackHttpResult({"status": "error", "message": "媒体类型无效"}, 400)
        try:
            raw_activity = [dict(item) for item in self._provider.report(user_id=user_id, days=normalized_days)]
        except PlaybackReportError as exc:
            if exc.kind is PlaybackReportErrorKind.PLUGIN_NOT_INSTALLED:
                return PlaybackHttpResult(
                    {"status": "error", "message": "服务端未安装 Playback Reporting 插件"},
                    404,
                )
            return PlaybackHttpResult({"status": "error", "message": "获取数据失败"}, 500)
        if normalized_type != "all":
            raw_activity = [item for item in raw_activity if _item_type(item) == normalized_type]
        top_items = raw_activity[:20]
        episode_ids = tuple(
            dict.fromkeys(_item_id(item) for item in top_items if _item_type(item) == "Episode" and _item_id(item))
        )
        details: dict[str, Mapping[str, Any]] = {}
        if episode_ids:
            try:
                details = {
                    str(item.get("Id")): item
                    for item in self._provider.item_details(
                        user_id=user_id,
                        item_ids=episode_ids,
                    )
                    if item.get("Id")
                }
            except PlaybackReportError:
                details = {}
        total_minutes = sum(int(_duration(item) / 60) for item in raw_activity)
        history = [
            {
                "title": _display_title(item, details),
                "date": _date_value(item),
                "duration": int(_duration(item) / 60),
                "item_type": _item_type(item),
                "item_id": _item_id(item),
            }
            for item in top_items
        ]
        return PlaybackHttpResult(
            {
                "personal": {
                    "total_count": len(raw_activity),
                    "total_minutes": total_minutes,
                    "history_list": history,
                }
            }
        )

    def dashboard(self, *, days: int) -> PlaybackHttpResult:
        normalized_days = _days(days)
        try:
            raw_data = [dict(item) for item in self._provider.report(user_id=None, days=normalized_days)]
        except PlaybackReportError as exc:
            if exc.kind is PlaybackReportErrorKind.PLUGIN_NOT_INSTALLED:
                return PlaybackHttpResult(
                    {"status": "error", "message": "服务端未安装 Playback Reporting 插件"},
                    404,
                )
            return PlaybackHttpResult({"status": "error", "message": "获取仪表盘数据失败"}, 500)
        valid = [item for item in raw_data if _item_type(item) in {"Movie", "Episode"} and _item_id(item)]
        try:
            aggregation = self._projection.dashboard_aggregation([_item_id(item) for item in valid])
        except Exception:
            return PlaybackHttpResult({"status": "error", "message": "获取仪表盘数据失败"}, 500)

        total_hours = 0.0
        users: set[str] = set()
        watched: set[str] = set()
        trend: dict[str, dict[str, float | int]] = {}
        user_rank: defaultdict[str, float] = defaultdict(float)
        media_counter: dict[str, dict[str, Any]] = {}
        for item in valid:
            hours = _duration(item) / 3600
            total_hours += hours
            date = str(_date_value(item) or "")[:10] or "Unknown"
            user_name = str(
                item.get("UserName") or item.get("User") or item.get("user_name") or item.get("user") or "Unknown"
            )
            if user_name != "Unknown":
                users.add(user_name)
                user_rank[user_name] += hours
            if date != "Unknown":
                bucket = trend.setdefault(date, {"count": 0, "hours": 0.0})
                bucket["count"] = int(bucket["count"]) + 1
                bucket["hours"] = float(bucket["hours"]) + hours
            info = aggregation.get(_item_id(item))
            if info is None or not info.get("id"):
                continue
            target_id = str(info["id"])
            watched.add(target_id)
            value = media_counter.setdefault(
                target_id,
                {
                    "id": info.get("emby_id"),
                    "name": info.get("name"),
                    "type": info.get("type"),
                    "poster_path": info.get("poster_path"),
                    "count": 0,
                },
            )
            value["count"] = int(value["count"]) + 1

        dates = sorted(trend)[-normalized_days:]
        ranked_users = sorted(user_rank.items(), key=lambda item: item[1], reverse=True)[:10]
        ranked_media = sorted(media_counter.values(), key=lambda item: int(item["count"]), reverse=True)[:20]
        settings = self._settings()
        return PlaybackHttpResult(
            {
                "total_plays": len(valid),
                "total_duration_hours": round(total_hours, 2),
                "active_users": len(users),
                "watched_items": len(watched),
                "media_rank": ranked_media,
                "hourly_heat": {},
                "emby_url": settings.emby_public_url or settings.emby_server_url,
                "emby_server_id": str(settings.get("emby_server_id") or ""),
                "chart_trend": {
                    "dates": dates,
                    "counts": [int(trend[date]["count"]) for date in dates],
                    "hours": [round(float(trend[date]["hours"]), 1) for date in dates],
                },
                "chart_users": {
                    "names": [name for name, _hours in ranked_users],
                    "hours": [round(hours, 1) for _name, hours in ranked_users],
                },
            }
        )


def _days(value: int) -> int:
    try:
        return max(1, min(int(value), 365))
    except (TypeError, ValueError):
        return 30


def _item_type(item: Mapping[str, Any]) -> str:
    return str(item.get("ItemType") or item.get("Type") or item.get("item_type") or "Video")


def _item_id(item: Mapping[str, Any]) -> str:
    return str(item.get("ItemId") or item.get("item_id") or "")


def _duration(item: Mapping[str, Any]) -> float:
    raw = item.get("PlayDuration") or item.get("Duration") or item.get("duration") or item.get("total_time") or 0
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        if isinstance(raw, str) and ":" in raw:
            try:
                values = [int(value) for value in raw.split(":")]
            except ValueError:
                return 0.0
            if len(values) == 3:
                return float(values[0] * 3600 + values[1] * 60 + values[2])
            if len(values) == 2:
                return float(values[0] * 60 + values[1])
        return 0.0


def _date_value(item: Mapping[str, Any]) -> Any:
    date = item.get("DateCreated") or item.get("Date") or item.get("date")
    time = item.get("time")
    if date and time and " " not in str(date):
        return f"{date} {time}"
    return date


def _display_title(
    item: Mapping[str, Any],
    details: Mapping[str, Mapping[str, Any]],
) -> str:
    item_id = _item_id(item)
    raw_title = str(item.get("Name") or item.get("item_name") or "未知影片")
    if _item_type(item) != "Episode":
        return raw_title
    detail = details.get(item_id)
    if detail is not None:
        series_name = str(detail.get("SeriesName") or "").strip()
        season = detail.get("ParentIndexNumber")
        episode = detail.get("IndexNumber")
        if series_name and season is not None and episode is not None:
            return f"{series_name} - 第 {season} 季 - 第 {episode} 集"
        episode_name = str(detail.get("Name") or "")
        match = re.search(r"(?i)s(\d+)\s*e(\d+)", episode_name)
        if series_name and match:
            return f"{series_name} - 第 {int(match.group(1))} 季 - 第 {int(match.group(2))} 集"
        if series_name:
            return f"{series_name} - {episode_name}" if episode_name else series_name
    match = re.search(r"(?i)s(\d+)\s*e(\d+)", raw_title)
    if match:
        parts = re.split(r"(?i)\s*[-_]?\s*s\d+e\d+", raw_title)
        if parts and parts[0].strip():
            return f"{parts[0].strip().rstrip(' -')} - 第 {int(match.group(1))} 季 - 第 {int(match.group(2))} 集"
    return raw_title


__all__ = [
    "DashboardMediaProjection",
    "PlaybackHttpResult",
    "PlaybackReportError",
    "PlaybackReportErrorKind",
    "PlaybackReportingPort",
    "PlaybackReportingService",
]
