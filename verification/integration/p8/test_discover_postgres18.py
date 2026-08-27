from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from etk.domains.media.discover import DiscoverService
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime


class FixtureDefaults:
    @staticmethod
    def language_mapping() -> Sequence[Mapping[str, Any]]:
        return [{"label": "Default", "value": "default"}]

    @staticmethod
    def keyword_mapping() -> Sequence[Mapping[str, Any]]:
        return [{"label": "Default Theme", "ids": [1]}]

    @staticmethod
    def studio_mapping() -> Sequence[Mapping[str, Any]]:
        return [{"label": "Default Studio", "ids": [1]}]


def test_discover_settings_snapshot_and_theme_modulo(p8_runtime: DatabaseRuntime) -> None:
    repository = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    repository.save_many(
        {
            "language_mapping": [{"label": "日本語", "value": "ja"}],
            "keyword_mapping": [
                {"label": "Theme A", "ids": [11]},
                {"label": "Theme B", "ids": [22]},
            ],
            "studio_mapping": [{"label": "Studio", "company_ids": [33]}],
            "recommendation_pool": [{"id": 101}],
            "recommendation_pool_page": 7,
            "recommendation_theme_index": 5,
        }
    )
    service = DiscoverService(settings=repository, defaults=FixtureDefaults())

    assert service.languages().payload == [{"label": "日本語", "value": "ja"}]
    assert service.keywords().payload == [
        {"label": "Theme A", "value": "Theme A"},
        {"label": "Theme B", "value": "Theme B"},
    ]
    assert service.studios().payload == [{"label": "Studio", "value": "Studio", "types": ["movie"]}]
    assert service.daily_recommendation().payload == {
        "theme_name": "Theme B",
        "pool": [{"id": 101}],
    }
    snapshot = repository.load_many(
        (
            "recommendation_pool",
            "recommendation_pool_page",
            "recommendation_theme_index",
            "missing-key",
        )
    )
    assert snapshot == {
        "recommendation_pool": [{"id": 101}],
        "recommendation_pool_page": 7,
        "recommendation_theme_index": 5,
        "missing-key": None,
    }
