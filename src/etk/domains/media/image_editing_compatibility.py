from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .image_editing import (
    EmbyMediaDetailsNotFoundError,
    ImageUpdateCommand,
    ImageUpdateOutcome,
    TmdbIdentityMissingError,
    TmdbImageCandidate,
    TmdbImageCatalog,
    TmdbImagesNotFoundError,
    UnsupportedImageMediaTypeError,
)

_IMAGE_LANGUAGE_PARAMETER = "zh-CN,zh-TW,zh,en,null,ja,ko"
_PREVIEW_BASE_URL = "https://image.tmdb.org/t/p/w500"
_ORIGINAL_BASE_URL = "https://image.tmdb.org/t/p/original"


class LegacyMediaImageEditingGateway:
    """Lazy compatibility adapter for the legacy processor and TMDb clients."""

    @staticmethod
    def processor_ready() -> bool:
        import extensions

        return extensions.media_processor_instance is not None

    def tmdb_images(self, item_id: str) -> TmdbImageCatalog:
        import extensions
        import handler.emby as emby

        processor = extensions.media_processor_instance
        if processor is None:
            raise RuntimeError("media processor became unavailable")
        item_details = emby.get_emby_item_details(
            item_id,
            str(processor.emby_url or ""),
            str(processor.emby_api_key or ""),
            str(processor.emby_user_id or ""),
        )
        if not item_details:
            raise EmbyMediaDetailsNotFoundError(item_id)
        provider_ids = item_details.get("ProviderIds", {})
        tmdb_id = provider_ids.get("Tmdb")
        if not tmdb_id:
            raise TmdbIdentityMissingError(item_id)

        import handler.tmdb as tmdb

        item_type = item_details.get("Type")
        if item_type == "Movie":
            data = tmdb.get_movie_details(
                int(tmdb_id),
                processor.tmdb_api_key,
                append_to_response="images",
                include_image_language=_IMAGE_LANGUAGE_PARAMETER,
            )
        elif item_type == "Series":
            data = tmdb.get_tv_details(
                int(tmdb_id),
                processor.tmdb_api_key,
                append_to_response="images",
                include_image_language=_IMAGE_LANGUAGE_PARAMETER,
            )
        else:
            raise UnsupportedImageMediaTypeError(str(item_type or ""))
        if not data or "images" not in data:
            raise TmdbImagesNotFoundError(str(tmdb_id))
        images = data["images"]
        return TmdbImageCatalog(
            posters=_format_images(images.get("posters", [])),
            backdrops=_format_images(images.get("backdrops", [])),
            logos=_format_images(images.get("logos", [])),
        )

    def update_image(self, command: ImageUpdateCommand) -> ImageUpdateOutcome:
        import extensions

        processor = extensions.media_processor_instance
        if processor is None:
            raise RuntimeError("media processor became unavailable")
        success, message = processor.update_media_image_manually(
            item_id=command.item_id,
            image_type=command.image_type,
            image_url=command.image_url or None,
            image_bytes=command.image_bytes,
            content_type=command.content_type,
        )
        return ImageUpdateOutcome(bool(success), str(message))


def _format_images(values: object) -> tuple[TmdbImageCandidate, ...]:
    if not isinstance(values, Sequence) or isinstance(values, str | bytes | bytearray):
        raise TypeError("TMDb images must be a sequence")
    formatted: list[TmdbImageCandidate] = []
    for raw in values:
        if not isinstance(raw, Mapping):
            raise TypeError("TMDb image must be an object")
        path = raw["file_path"]
        formatted.append(
            TmdbImageCandidate(
                preview=f"{_PREVIEW_BASE_URL}{path}",
                original=f"{_ORIGINAL_BASE_URL}{path}",
                aspect_ratio=raw.get("aspect_ratio", 1),
                width=_optional_int(raw.get("width")),
                height=_optional_int(raw.get("height")),
            )
        )
    return tuple(formatted)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


__all__ = ["LegacyMediaImageEditingGateway"]
