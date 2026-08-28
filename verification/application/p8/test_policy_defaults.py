from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import Any

import utils
from etk.domains.collection.local_gateway import P8LocalCollectionGateway
from etk.policy_defaults import (
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

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"
EXPECTED_DEFAULTS_SHA256 = "b783ca6ea379306d4b2b06f9b1e70ff1e9c77338320cf37d359d4ad6e828ae26"
DEFAULTS = {
    "GENRE_TRANSLATION_PATCH": GENRE_TRANSLATION_PATCH,
    "UNIFIED_RATING_CATEGORIES": UNIFIED_RATING_CATEGORIES,
    "DEFAULT_RATING_PRIORITY": DEFAULT_RATING_PRIORITY,
    "DEFAULT_RATING_MAPPING": DEFAULT_RATING_MAPPING,
    "DEFAULT_RELEASE_GROUP_MAPPING": DEFAULT_RELEASE_GROUP_MAPPING,
    "DEFAULT_JUNK_FILE_PATTERNS": DEFAULT_JUNK_FILE_PATTERNS,
    "DEFAULT_KEYWORD_MAPPING": DEFAULT_KEYWORD_MAPPING,
    "DEFAULT_STUDIO_MAPPING": DEFAULT_STUDIO_MAPPING,
    "DEFAULT_COUNTRY_MAPPING": DEFAULT_COUNTRY_MAPPING,
    "DEFAULT_LANGUAGE_MAPPING": DEFAULT_LANGUAGE_MAPPING,
}
NATIVE_CONSUMERS = (
    "src/etk/domains/collection/compatibility.py",
    "src/etk/domains/collection/local_gateway.py",
    "src/etk/domains/media/compatibility.py",
    "src/etk/domains/media/discover_compatibility.py",
    "src/etk/domains/p115/adapters.py",
    "src/etk/domains/p115/media_info_adapters.py",
    "src/etk/domains/subscription/adapters.py",
)
SETTING_KEYS = (
    "country_mapping",
    "junk_file_patterns",
    "keyword_mapping",
    "language_mapping",
    "native_collections_config",
    "rating_mapping",
    "rating_priority",
    "release_group_mapping",
    "studio_mapping",
    "unified_ratings",
)


def _digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_plaintext_policy_defaults_preserve_exact_legacy_values() -> None:
    assert _digest(DEFAULTS) == EXPECTED_DEFAULTS_SHA256
    for name, value in DEFAULTS.items():
        assert getattr(utils, name) is value


def test_collection_gateway_uses_plaintext_policy_defaults() -> None:
    defaults = P8LocalCollectionGateway.defaults()
    assert defaults == {
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


def test_native_policy_consumers_do_not_import_legacy_utils() -> None:
    for relative_path in NATIVE_CONSUMERS:
        source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_modules = {
            alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        }
        imported_modules.update(
            node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        assert "utils" not in imported_modules, relative_path


def test_collection_settings_have_native_repository_ownership() -> None:
    manifest = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in manifest["standalone_app_settings"]}

    for key in SETTING_KEYS:
        contract = settings[key]
        assert contract["status"] == "new"
        assert contract["new_owner"] == "etk.domains.system.repository.SqlAlchemyAppSettingsRepository"
        assert contract["validation_owner"] == "collection/settings"
        assert contract["fixture"] == "verification/fixtures/http/p8-collection-cover.json"
