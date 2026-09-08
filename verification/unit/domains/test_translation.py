from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from etk.domains.translation import ConfiguredBatchTranslation
from etk.platform import Settings


class MemoryTranslationCache:
    def __init__(self, values: Mapping[str, str] | None = None) -> None:
        self.values = dict(values or {})
        self.loads: list[tuple[str, ...]] = []
        self.saves: list[tuple[dict[str, str], str]] = []

    def load_many(self, values: Sequence[str]) -> Mapping[str, str]:
        self.loads.append(tuple(values))
        return {value: self.values[value] for value in values if value in self.values}

    def save_many(self, values: Mapping[str, str], *, engine_used: str) -> int:
        saved = dict(values)
        self.saves.append((saved, engine_used))
        self.values.update(saved)
        return len(saved)


class FixtureTranslation(ConfiguredBatchTranslation):
    def __init__(self, cache: MemoryTranslationCache) -> None:
        super().__init__(
            lambda: Settings.model_validate({"ai_api_key": "fixture-key", "ai_model_name": "fixture-model"}),
            cache,
            batch_size=2,
        )
        self.provider_calls: list[tuple[str, ...]] = []

    def _translate_chunk(self, settings: Settings, api_key: str, values: tuple[str, ...]) -> dict[str, str]:
        assert settings.ai_model_name == "fixture-model"
        assert api_key == "fixture-key"
        self.provider_calls.append(values)
        return {value: f"中文-{value}" for value in values}


def test_translation_uses_persisted_hits_and_only_requests_missing_values() -> None:
    cache = MemoryTranslationCache({"cached": "已缓存"})
    translation = FixtureTranslation(cache)

    result = translation.translate(["cached", "first", "second", "first"])

    assert result == {
        "cached": "已缓存",
        "first": "中文-first",
        "second": "中文-second",
    }
    assert cache.loads == [("cached", "first", "second")]
    assert translation.provider_calls == [("first", "second")]
    assert cache.saves == [
        (
            {"first": "中文-first", "second": "中文-second"},
            "openai:fixture-model",
        )
    ]


def test_translation_cache_failures_do_not_replace_the_provider_outcome() -> None:
    class FailingCache(MemoryTranslationCache):
        def load_many(self, values: Sequence[str]) -> Mapping[str, str]:
            raise RuntimeError("fixture read failure")

        def save_many(self, values: Mapping[str, str], *, engine_used: str) -> int:
            raise RuntimeError("fixture write failure")

    translation = FixtureTranslation(FailingCache())

    assert translation.translate(["value"]) == {"value": "中文-value"}
    assert translation.provider_calls == [("value",)]


def test_full_cache_hit_does_not_require_provider_configuration() -> None:
    cache = MemoryTranslationCache({"cached": "缓存命中"})
    translation = ConfiguredBatchTranslation(lambda: Settings(), cache)

    assert translation.translate(["cached"]) == {"cached": "缓存命中"}


def test_empty_input_remains_side_effect_free() -> None:
    cache = MemoryTranslationCache()
    translation = ConfiguredBatchTranslation(lambda: Any, cache)

    assert translation.translate([]) == {}
    assert cache.loads == []
