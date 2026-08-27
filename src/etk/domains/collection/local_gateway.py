from __future__ import annotations

import base64
from collections.abc import Mapping
from typing import Any

from .renderer import CoverPreviewRenderer


class P8LocalCollectionGateway:
    """Local-only P8 gateway; external routes remain owned by the compatibility host."""

    def __init__(
        self,
        *,
        renderer: CoverPreviewRenderer,
    ) -> None:
        self._renderer = renderer

    @staticmethod
    def defaults() -> Mapping[str, Any]:
        from utils import (
            DEFAULT_COUNTRY_MAPPING,
            DEFAULT_JUNK_FILE_PATTERNS,
            DEFAULT_KEYWORD_MAPPING,
            DEFAULT_LANGUAGE_MAPPING,
            DEFAULT_RATING_MAPPING,
            DEFAULT_RATING_PRIORITY,
            DEFAULT_RELEASE_GROUP_MAPPING,
            DEFAULT_STUDIO_MAPPING,
            GENRE_TRANSLATION_PATCH,
            UNIFIED_RATING_CATEGORIES,
        )

        return {
            "country_mapping": DEFAULT_COUNTRY_MAPPING,
            "junk_file_patterns": DEFAULT_JUNK_FILE_PATTERNS,
            "keyword_mapping": DEFAULT_KEYWORD_MAPPING,
            "language_mapping": DEFAULT_LANGUAGE_MAPPING,
            "rating_mapping": DEFAULT_RATING_MAPPING,
            "rating_priority": DEFAULT_RATING_PRIORITY,
            "release_group_mapping": DEFAULT_RELEASE_GROUP_MAPPING,
            "studio_mapping": DEFAULT_STUDIO_MAPPING,
            "unified_ratings": UNIFIED_RATING_CATEGORIES,
            "genre_translation": GENRE_TRANSLATION_PATCH,
        }

    def cover_preview(self, payload: Mapping[str, Any]) -> str:
        rendered = self._renderer.render_png(payload)
        return "data:image/png;base64," + base64.b64encode(rendered).decode("ascii")


__all__ = ["P8LocalCollectionGateway"]
