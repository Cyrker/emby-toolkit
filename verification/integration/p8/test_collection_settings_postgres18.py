from __future__ import annotations

from copy import deepcopy

from sqlalchemy import text

from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime
from etk.policy_defaults import (
    DEFAULT_COUNTRY_MAPPING,
    DEFAULT_JUNK_FILE_PATTERNS,
    DEFAULT_KEYWORD_MAPPING,
    DEFAULT_LANGUAGE_MAPPING,
    DEFAULT_RATING_MAPPING,
    DEFAULT_RATING_PRIORITY,
    DEFAULT_RELEASE_GROUP_MAPPING,
    DEFAULT_STUDIO_MAPPING,
    UNIFIED_RATING_CATEGORIES,
)

COLLECTION_SETTINGS = {
    "country_mapping": DEFAULT_COUNTRY_MAPPING,
    "junk_file_patterns": DEFAULT_JUNK_FILE_PATTERNS,
    "keyword_mapping": DEFAULT_KEYWORD_MAPPING,
    "language_mapping": DEFAULT_LANGUAGE_MAPPING,
    "native_collections_config": {"auto_sub_enabled": True},
    "rating_mapping": DEFAULT_RATING_MAPPING,
    "rating_priority": DEFAULT_RATING_PRIORITY,
    "release_group_mapping": DEFAULT_RELEASE_GROUP_MAPPING,
    "studio_mapping": DEFAULT_STUDIO_MAPPING,
    "unified_ratings": UNIFIED_RATING_CATEGORIES,
}


def test_collection_settings_round_trip_through_native_postgres18_repository(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    expected = deepcopy(COLLECTION_SETTINGS)

    repository.save_many(expected)

    assert repository.load_many(tuple(expected)) == expected
    assert repository.merge_mapping(
        "native_collections_config",
        {"auto_sub_enabled": False},
        remove_keys=("auto_complete_enabled",),
    ) == {"auto_sub_enabled": False}
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(
            text("SELECT count(*) FROM app_settings WHERE setting_key = ANY(:keys)"),
            {"keys": list(expected)},
        ) == len(expected)
