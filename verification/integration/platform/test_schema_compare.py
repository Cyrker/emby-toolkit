from __future__ import annotations

import copy
import json
from pathlib import Path

from scripts.refactor.compare_schema import compare_catalogs, load_catalog, normalize_catalog

REPO_ROOT = Path(__file__).resolve().parents[3]
CATALOG_PATH = REPO_ROOT / "verification" / "contracts" / "db" / "legacy-pg-catalog.json"
SQL_PATH = REPO_ROOT / "verification" / "contracts" / "db" / "legacy-schema.sql"


def _catalog() -> dict:
    payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    payload["server_version"] = "18.6"
    payload["server_version_num"] = "180006"
    return payload


def test_compare_accepts_sql_path_and_ignores_pg18_not_null_constraints() -> None:
    expected = load_catalog(SQL_PATH)
    actual = _catalog()
    actual["constraints"].append(
        {
            "table": "app_settings",
            "name": "app_settings_setting_key_not_null",
            "type": "not_null",
            "definition": "NOT NULL setting_key",
        }
    )
    result = compare_catalogs(expected, actual)

    assert result["match"] is True
    assert result["server_version_match"] is True
    assert result["counts"]["constraints"] == {"expected": 60, "actual": 60}


def test_compare_normalizes_pg18_partial_index_predicate_parentheses() -> None:
    expected = _catalog()
    actual = copy.deepcopy(expected)
    index = next(item for item in actual["indexes"] if item["name"] == "idx_mm_episode_parent_season_ep_library")
    index["definition"] = (
        "CREATE INDEX idx_mm_episode_parent_season_ep_library ON public.media_metadata "
        "USING btree (parent_series_tmdb_id, season_number, episode_number) "
        "WHERE item_type = 'Episode'::text AND in_library = true"
    )

    result = compare_catalogs(expected, actual)

    assert result["differences"]["indexes"] == {"missing": [], "unexpected": [], "changed": []}
    assert result["match"] is True


def test_compare_does_not_erase_partial_index_boolean_grouping() -> None:
    expected = _catalog()
    actual = copy.deepcopy(expected)
    index = next(item for item in expected["indexes"] if item["name"] == "idx_mm_episode_parent_season_ep_library")
    index["definition"] = (
        "CREATE INDEX idx_mm_episode_parent_season_ep_library ON public.media_metadata "
        "USING btree (parent_series_tmdb_id, season_number, episode_number) "
        "WHERE ((item_type = 'Episode'::text) OR (in_library = true)) AND (season_number IS NOT NULL)"
    )
    actual_index = next(item for item in actual["indexes"] if item["name"] == "idx_mm_episode_parent_season_ep_library")
    actual_index["definition"] = (
        "CREATE INDEX idx_mm_episode_parent_season_ep_library ON public.media_metadata "
        "USING btree (parent_series_tmdb_id, season_number, episode_number) "
        "WHERE item_type = 'Episode'::text OR (in_library = true AND season_number IS NOT NULL)"
    )

    result = compare_catalogs(expected, actual)

    assert len(result["differences"]["indexes"]["changed"]) == 1
    assert result["match"] is False


def test_compare_reports_structured_column_and_version_differences() -> None:
    expected = _catalog()
    actual = copy.deepcopy(expected)
    actual["server_version_num"] = "160014"
    actual["columns"][0]["nullable"] = True

    result = compare_catalogs(expected, actual)

    assert result["match"] is False
    assert result["server_version_match"] is False
    changed = result["differences"]["columns"]["changed"]
    assert changed[0]["key"] == {"table": "actor_subscriptions", "name": "id"}
    assert normalize_catalog(expected)["tables"] == normalize_catalog(actual)["tables"]


def test_compare_reports_parameterized_column_type_differences() -> None:
    expected = _catalog()
    actual = copy.deepcopy(expected)
    expected_column = next(
        item for item in expected["columns"] if item["table"] == "media_image_cache" and item["name"] == "content_hash"
    )
    actual_column = next(
        item for item in actual["columns"] if item["table"] == "media_image_cache" and item["name"] == "content_hash"
    )
    assert expected_column["character_maximum_length"] == 64
    actual_column["character_maximum_length"] = 32

    result = compare_catalogs(expected, actual)

    assert result["match"] is False
    changed = result["differences"]["columns"]["changed"]
    assert len(changed) == 1
    assert changed[0]["key"] == {"table": "media_image_cache", "name": "content_hash"}
    assert changed[0]["expected"]["character_maximum_length"] == 64
    assert changed[0]["actual"]["character_maximum_length"] == 32
