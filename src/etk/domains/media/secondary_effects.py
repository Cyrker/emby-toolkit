from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from etk.domains.operations.jobs import TaskBusyError

from .secondary import normalized_string_list


@dataclass(frozen=True, slots=True)
class SecondaryEffectsHttpResult:
    payload: Any
    status_code: int = 200


class SecondaryEmbyPort(Protocol):
    def configured_libraries(self) -> Sequence[Mapping[str, Any]]: ...

    def user_views(
        self,
        user_id: str,
        token: str,
        query: Mapping[str, str],
    ) -> Mapping[str, Any]: ...


class SecondaryJobPort(Protocol):
    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any: ...


class SecondaryEffectsService:
    _EXCLUDED_LIBRARY_TYPES = frozenset({"music", "boxsets", "playlists"})

    def __init__(self, emby: SecondaryEmbyPort, jobs: SecondaryJobPort) -> None:
        self._emby = emby
        self._jobs = jobs

    def libraries(self) -> SecondaryEffectsHttpResult:
        try:
            values = self._emby.configured_libraries()
        except Exception:
            return SecondaryEffectsHttpResult(
                {"error": "无法获取Emby媒体库列表，请检查连接和日志"},
                500,
            )
        return SecondaryEffectsHttpResult(
            [
                {"Name": item.get("Name"), "Id": item.get("Id")}
                for item in values
                if item.get("Name")
                and item.get("Id")
                and str(item.get("CollectionType") or "").casefold() not in self._EXCLUDED_LIBRARY_TYPES
            ]
        )

    def user_views(
        self,
        user_id: str,
        token: str,
        query: Mapping[str, str],
    ) -> SecondaryEffectsHttpResult:
        normalized_user_id = str(user_id or "").strip()
        normalized_token = str(token or "").strip()
        if not normalized_user_id:
            return SecondaryEffectsHttpResult({"error": "无效的 Emby 用户 ID"}, 400)
        if not normalized_token or set(normalized_token) == {"*"}:
            return SecondaryEffectsHttpResult(
                {"error": "缺少用户访问令牌(api_key或X-Emby-Token)"},
                400,
            )
        try:
            return SecondaryEffectsHttpResult(dict(self._emby.user_views(normalized_user_id, normalized_token, query)))
        except RuntimeError:
            return SecondaryEffectsHttpResult({"error": "无法从真实Emby服务器获取数据"}, 502)
        except Exception:
            return SecondaryEffectsHttpResult({"error": "服务器内部错误"}, 500)

    def submit_auto_tagging(self, payload: Any, *, operation: str) -> SecondaryEffectsHttpResult:
        try:
            parameters = _auto_tagging_parameters(payload, operation=operation)
        except (TypeError, ValueError) as exc:
            return SecondaryEffectsHttpResult({"error": str(exc)}, 400)
        try:
            self._jobs.submit_task(
                "execute-auto-tagging-rules",
                parameters,
                trigger_type="manual",
                max_attempts=2,
                reject_when_busy=True,
            )
        except TaskBusyError:
            return SecondaryEffectsHttpResult({"error": "后台有任务正在运行，请稍后再试。"}, 409)
        except Exception:
            return SecondaryEffectsHttpResult({"error": "任务提交失败"}, 500)
        message = "批量打标任务已启动" if operation == "add" else "批量移除任务已启动"
        return SecondaryEffectsHttpResult({"message": message})


def _auto_tagging_parameters(payload: Any, *, operation: str) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise TypeError("请求参数必须是对象")
    allowed = {"library_ids", "library_name", "tags", "rating_filters"}
    extras = set(payload) - allowed
    if extras:
        raise ValueError(f"包含不支持的参数: {', '.join(sorted(extras))}")
    tags = normalized_string_list(payload.get("tags"))
    if operation == "add" and not tags:
        raise ValueError("标签不能为空")
    return {
        "operation": operation,
        "library_ids": normalized_string_list(payload.get("library_ids")),
        "tags": tags,
        "rating_filters": normalized_string_list(payload.get("rating_filters")),
    }


__all__ = [
    "SecondaryEffectsHttpResult",
    "SecondaryEffectsService",
    "SecondaryEmbyPort",
    "SecondaryJobPort",
]
