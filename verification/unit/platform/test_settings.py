from __future__ import annotations

import json
from pathlib import Path

from etk.platform.config import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_CONTRACT = REPO_ROOT / "verification" / "contracts" / "config-ownership.yaml"


def test_settings_fields_and_defaults_match_the_configuration_contract() -> None:
    contract = json.loads(CONFIG_CONTRACT.read_text(encoding="utf-8"))
    entries = [*contract["bootstrap"], *contract["dynamic"]]
    expected = {entry["key"]: entry["default"] for entry in entries}

    assert set(Settings.model_fields) == set(expected)
    assert Settings().to_runtime_dict() == expected
    assert Settings.secret_keys == {entry["key"] for entry in entries if entry["secret"]}


def test_settings_are_side_effect_free_and_validate_known_values(monkeypatch) -> None:
    monkeypatch.setenv("DB_HOST", "must-not-load-without-adapter")

    settings = Settings.model_validate({"db_port": "5544", "monitor_enabled": "true"})

    assert settings.db_host == "localhost"
    assert settings.db_port == 5544
    assert settings.monitor_enabled is True
    assert settings.is_first_run is False


def test_settings_repr_and_redacted_dict_hide_nested_secrets() -> None:
    settings = Settings.model_validate(
        {
            "db_password": "database-secret",
            "emby_api_key": "emby-secret",
            "future_config": {
                "access_token": "nested-secret",
                "safe": ["visible", {"cookie": "cookie-secret"}],
            },
        }
    )

    rendered = repr(settings)
    redacted = settings.redacted_dict()

    assert "database-secret" not in rendered
    assert "emby-secret" not in rendered
    assert "nested-secret" not in rendered
    assert "cookie-secret" not in rendered
    assert "visible" in rendered
    assert redacted["db_password"] == "<redacted>"
    assert redacted["future_config"]["access_token"] == "<redacted>"


def test_settings_updates_return_a_new_snapshot_and_keep_unknown_keys() -> None:
    original = Settings.model_validate({"custom_future_key": {"enabled": True}})

    updated = original.with_updates({"proxy_port": 9000, "another_future_key": ["a", "b"]})

    assert original.proxy_port == 8097
    assert updated.proxy_port == 9000
    assert updated.get("custom_future_key") == {"enabled": True}
    assert updated.get("another_future_key") == ["a", "b"]


def test_settings_repr_redacts_url_userinfo_and_query_credentials() -> None:
    settings = Settings.model_validate(
        {
            "network_http_proxy_url": "http://proxy-user:proxy-password@proxy.invalid:8080",
            "future_url": "https://service.invalid/path?api_key=query-secret&safe=visible",
        }
    )

    rendered = repr(settings)

    assert "proxy-password" not in rendered
    assert "query-secret" not in rendered
    assert "proxy-user:<redacted>@proxy.invalid" in rendered
    assert "safe=visible" in rendered
