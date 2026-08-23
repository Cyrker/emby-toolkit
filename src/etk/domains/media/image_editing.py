from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ImageEditingResult:
    payload: Mapping[str, Any]
    status_code: int = 200


@dataclass(frozen=True, slots=True)
class TmdbImageCandidate:
    preview: str
    original: str
    aspect_ratio: Any
    width: int | None
    height: int | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "preview": self.preview,
            "original": self.original,
            "aspect_ratio": self.aspect_ratio,
            "width": self.width,
            "height": self.height,
        }


@dataclass(frozen=True, slots=True)
class TmdbImageCatalog:
    posters: tuple[TmdbImageCandidate, ...]
    backdrops: tuple[TmdbImageCandidate, ...]
    logos: tuple[TmdbImageCandidate, ...]

    def to_payload(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "posters": [image.to_payload() for image in self.posters],
            "backdrops": [image.to_payload() for image in self.backdrops],
            "logos": [image.to_payload() for image in self.logos],
        }


@dataclass(frozen=True, slots=True)
class ImageUpdateCommand:
    item_id: str
    image_type: str
    image_url: str
    image_bytes: bytes | None
    content_type: str | None


@dataclass(frozen=True, slots=True)
class ImageUpdateOutcome:
    success: bool
    message: str
    effect_id: str = ""
    effect_status: str = ""


class ImageEditingGateway(Protocol):
    def processor_ready(self) -> bool: ...

    def tmdb_images(self, item_id: str) -> TmdbImageCatalog: ...

    def update_image(self, command: ImageUpdateCommand) -> ImageUpdateOutcome: ...


class EmbyMediaDetailsNotFoundError(RuntimeError):
    pass


class TmdbIdentityMissingError(RuntimeError):
    pass


class UnsupportedImageMediaTypeError(RuntimeError):
    pass


class TmdbImagesNotFoundError(RuntimeError):
    pass


class MediaImageEditingService:
    """P5 application boundary for manual image candidates and replacement."""

    def __init__(self, gateway: ImageEditingGateway) -> None:
        self._gateway = gateway

    def tmdb_images(self, item_id: str) -> ImageEditingResult:
        unavailable = self.unavailable()
        if unavailable is not None:
            return unavailable
        try:
            catalog = self._gateway.tmdb_images(str(item_id or "").strip())
        except EmbyMediaDetailsNotFoundError:
            return ImageEditingResult({"error": "无法获取 Emby 媒体详情"}, 404)
        except TmdbIdentityMissingError:
            return ImageEditingResult({"error": "该媒体缺少 TMDb ID，无法获取图片"}, 400)
        except UnsupportedImageMediaTypeError:
            return ImageEditingResult({"error": "不支持的媒体类型"}, 400)
        except TmdbImagesNotFoundError:
            return ImageEditingResult({"error": "未在 TMDb 找到图片数据"}, 404)
        except Exception:
            logger.exception("failed to load P5 TMDb image candidates")
            return ImageEditingResult({"error": "获取 TMDb 图片失败"}, 500)
        return ImageEditingResult(catalog.to_payload())

    def update_image(
        self,
        item_id: str,
        *,
        image_type: object,
        image_url: object = "",
        image_bytes: bytes | None = None,
        content_type: str | None = None,
    ) -> ImageEditingResult:
        unavailable = self.unavailable()
        if unavailable is not None:
            return unavailable
        normalized_type = str(image_type or "").strip()
        normalized_url = str(image_url or "")
        if not normalized_type:
            return ImageEditingResult(
                {"error": "缺少 image_type 参数 (可选值: poster, clearlogo, fanart, landscape)"},
                400,
            )
        if not normalized_url and not image_bytes:
            return ImageEditingResult({"error": "必须提供 image_url 或上传 file"}, 400)
        try:
            outcome = self._gateway.update_image(
                ImageUpdateCommand(
                    item_id=str(item_id or "").strip(),
                    image_type=normalized_type,
                    image_url=normalized_url,
                    image_bytes=image_bytes,
                    content_type=content_type,
                )
            )
        except Exception:
            logger.exception("failed to update P5 media image")
            return ImageEditingResult({"error": "服务器内部错误"}, 500)
        if outcome.success:
            return ImageEditingResult({"message": outcome.message})
        payload = {"error": outcome.message}
        if outcome.effect_id:
            payload["effect_id"] = outcome.effect_id
        if outcome.effect_status:
            payload["effect_status"] = outcome.effect_status
        if outcome.effect_status == "indeterminate":
            return ImageEditingResult(payload, 503)
        if outcome.effect_status in {"pending", "claimed", "retryable"}:
            return ImageEditingResult(payload, 202)
        return ImageEditingResult(payload, 500)

    def unavailable(self) -> ImageEditingResult | None:
        if self._gateway.processor_ready():
            return None
        return ImageEditingResult({"error": "核心处理器未就绪。"}, 503)


__all__ = [
    "EmbyMediaDetailsNotFoundError",
    "ImageEditingGateway",
    "ImageEditingResult",
    "ImageUpdateCommand",
    "ImageUpdateOutcome",
    "MediaImageEditingService",
    "TmdbIdentityMissingError",
    "TmdbImageCandidate",
    "TmdbImageCatalog",
    "TmdbImagesNotFoundError",
    "UnsupportedImageMediaTypeError",
]
