from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any

from etk.policy_defaults import (
    DEFAULT_KEYWORD_MAPPING,
    DEFAULT_LANGUAGE_MAPPING,
    DEFAULT_RATING_MAPPING,
    DEFAULT_STUDIO_MAPPING,
)


class StaticDiscoverDefaults:
    @staticmethod
    def language_mapping() -> Sequence[Mapping[str, Any]]:
        return deepcopy(DEFAULT_LANGUAGE_MAPPING)

    @staticmethod
    def keyword_mapping() -> Sequence[Mapping[str, Any]]:
        return deepcopy(DEFAULT_KEYWORD_MAPPING)

    @staticmethod
    def studio_mapping() -> Sequence[Mapping[str, Any]]:
        return deepcopy(DEFAULT_STUDIO_MAPPING)

    @staticmethod
    def rating_mapping() -> Mapping[str, Sequence[Mapping[str, Any]]]:
        return deepcopy(DEFAULT_RATING_MAPPING)


__all__ = ["StaticDiscoverDefaults"]
