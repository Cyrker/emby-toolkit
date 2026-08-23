from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CastEditorResult:
    payload: Any
    status_code: int = 200
    media_type: str = "application/json"


@dataclass(frozen=True, slots=True)
class ManualCastUpdateCommand:
    item_id: str
    cast: tuple[Any, ...]
    item_name: Any


@dataclass(frozen=True, slots=True)
class CastTranslationCommand:
    cast: tuple[Any, ...]
    title: Any = None
    year: Any = None


@dataclass(frozen=True, slots=True)
class ProcessedCastActor:
    emby_person_id: Any = None
    name: Any = None
    role: Any = None
    imdb_id: Any = None
    douban_id: Any = None
    tmdb_id: Any = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "embyPersonId": self.emby_person_id,
            "name": self.name,
            "role": self.role,
            "imdbId": self.imdb_id,
            "doubanId": self.douban_id,
            "tmdbId": self.tmdb_id,
            "matchStatus": "已刷新",
        }


class CastEditorGateway(Protocol):
    def processor_ready(self) -> bool: ...

    def media_for_editing(self, item_id: str) -> Any: ...

    def submit_manual_update(self, command: ManualCastUpdateCommand) -> bool: ...

    def translate_cast(self, command: CastTranslationCommand) -> Any: ...

    def preview_item_details(self, item_id: str) -> dict[str, Any] | None: ...

    def process_preview_cast(
        self,
        item_details: dict[str, Any],
    ) -> tuple[ProcessedCastActor, ...]: ...


class MediaCastEditorService:
    """P5 boundary for the legacy manual cast editor HTTP surface."""

    def __init__(self, gateway: CastEditorGateway) -> None:
        self._gateway = gateway

    def unavailable(self) -> CastEditorResult | None:
        if self._gateway.processor_ready():
            return None
        return CastEditorResult({"error": "核心处理器未就绪。"}, 503)

    def media_for_editing(self, item_id: str) -> CastEditorResult:
        try:
            payload = self._gateway.media_for_editing(str(item_id or ""))
        except Exception:
            logger.exception("failed to load P5 media cast editor data")
            return legacy_http_error_result(500)
        if payload:
            return CastEditorResult(payload)
        return CastEditorResult(
            {"error": f"无法获取项目 {item_id} 的编辑数据，请检查日志。"},
            404,
        )

    def update_cast(
        self,
        item_id: str,
        cast: list[Any],
        *,
        item_name: Any,
    ) -> CastEditorResult:
        command = ManualCastUpdateCommand(
            item_id=str(item_id or ""),
            cast=tuple(cast),
            item_name=item_name,
        )
        try:
            # The legacy route ignores a rejected queue submission and still returns 202.
            self._gateway.submit_manual_update(command)
        except Exception:
            logger.exception("failed to submit P5 manual cast update")
            return legacy_http_error_result(500)
        return CastEditorResult({"message": "手动更新任务已在后台启动。"}, 202)

    def translate_cast(
        self,
        cast: list[Any],
        *,
        title: Any = None,
        year: Any = None,
    ) -> CastEditorResult:
        try:
            payload = self._gateway.translate_cast(CastTranslationCommand(cast=tuple(cast), title=title, year=year))
        except Exception:
            logger.exception("failed to translate P5 cast editor data")
            return CastEditorResult({"error": "服务器在翻译时发生内部错误。"}, 500)
        return CastEditorResult(payload)

    def preview_processed_cast(self, item_id: str) -> CastEditorResult:
        try:
            item_details = self._gateway.preview_item_details(str(item_id or ""))
            if not item_details:
                return CastEditorResult({"error": "无法获取当前媒体的Emby详情"}, 404)
        except Exception as exc:
            logger.exception("failed to load Emby details for P5 cast preview")
            return CastEditorResult({"error": f"获取Emby详情时发生错误: {exc}"}, 500)

        try:
            cast = self._gateway.process_preview_cast(item_details)
        except Exception:
            logger.exception("failed to process P5 cast preview")
            return CastEditorResult({"error": "在服务器端处理演员列表时发生内部错误"}, 500)
        return CastEditorResult([actor.to_payload() for actor in cast])


_LEGACY_ERROR_PAGES = {
    400: (
        "Bad Request",
        "The browser (or proxy) sent a request that this server could not understand.",
    ),
    415: (
        "Unsupported Media Type",
        "Did not attempt to load JSON data because the request Content-Type was not &#39;application/json&#39;.",
    ),
    500: (
        "Internal Server Error",
        "The server encountered an internal error and was unable to complete your request. "
        "Either the server is overloaded or there is an error in the application.",
    ),
}


def legacy_http_error_result(status_code: int) -> CastEditorResult:
    title, description = _LEGACY_ERROR_PAGES[status_code]
    return CastEditorResult(
        "<!doctype html>\n"
        "<html lang=en>\n"
        f"<title>{status_code} {title}</title>\n"
        f"<h1>{title}</h1>\n"
        f"<p>{description}</p>\n",
        status_code,
        "text/html",
    )


__all__ = [
    "CastEditorGateway",
    "CastEditorResult",
    "CastTranslationCommand",
    "ManualCastUpdateCommand",
    "MediaCastEditorService",
    "ProcessedCastActor",
    "legacy_http_error_result",
]
