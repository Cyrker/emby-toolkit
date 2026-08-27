from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any


class LegacyDiscoverDefaults:
    @staticmethod
    def language_mapping() -> Sequence[Mapping[str, Any]]:
        from utils import DEFAULT_LANGUAGE_MAPPING

        return deepcopy(DEFAULT_LANGUAGE_MAPPING)

    @staticmethod
    def keyword_mapping() -> Sequence[Mapping[str, Any]]:
        from utils import DEFAULT_KEYWORD_MAPPING

        return deepcopy(DEFAULT_KEYWORD_MAPPING)

    @staticmethod
    def studio_mapping() -> Sequence[Mapping[str, Any]]:
        from utils import DEFAULT_STUDIO_MAPPING

        return deepcopy(DEFAULT_STUDIO_MAPPING)


__all__ = ["LegacyDiscoverDefaults"]
