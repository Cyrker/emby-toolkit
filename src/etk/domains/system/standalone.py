from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Protocol

from etk.policy_defaults import DEFAULT_AI_PROMPTS, DEFAULT_TELEGRAM_NOTIFICATION_TEMPLATES

TELEGRAM_TEMPLATES_SETTING = "telegram_notification_templates"
CUSTOM_THEME_SETTING = "custom_theme"
AI_PROMPTS_SETTING = "ai_user_prompts"
MENU_CONFIG_SETTING = "custom_menu_config"

STANDALONE_SETTINGS_REPOSITORY_PROVIDER = "app-settings"
STANDALONE_SETTINGS_USE_CASE_PROVIDER = "system-standalone-settings"


class StandaloneSettingsRepository(Protocol):
    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...

    def delete(self, key: str) -> bool: ...


class StandaloneSettingsDefaults(Protocol):
    def telegram_templates(self) -> Mapping[str, Any]: ...

    def ai_prompts(self) -> Mapping[str, Any]: ...


class PlaintextStandaloneSettingsDefaults:
    def telegram_templates(self) -> Mapping[str, Any]:
        return deepcopy(DEFAULT_TELEGRAM_NOTIFICATION_TEMPLATES)

    def ai_prompts(self) -> Mapping[str, Any]:
        return deepcopy(DEFAULT_AI_PROMPTS)


class StandaloneSettingsService:
    """Serve standalone settings without importing the legacy runtime graph."""

    def __init__(
        self,
        repository: StandaloneSettingsRepository,
        defaults: StandaloneSettingsDefaults,
    ) -> None:
        self._repository = repository
        self._defaults = defaults

    def get_telegram_templates(self) -> dict[str, str]:
        return self._normalize_telegram_templates(self._repository.load(TELEGRAM_TEMPLATES_SETTING))

    def save_telegram_templates(self, payload: Any) -> dict[str, str]:
        source = payload.get("templates") if isinstance(payload, dict) else payload
        templates = self._normalize_telegram_templates(source)
        self._repository.save(TELEGRAM_TEMPLATES_SETTING, templates)
        return templates

    def save_custom_theme(self, theme: Mapping[str, Any]) -> None:
        self._repository.save(CUSTOM_THEME_SETTING, deepcopy(dict(theme)))

    def delete_custom_theme(self) -> bool:
        return self._repository.delete(CUSTOM_THEME_SETTING)

    def get_ai_prompts(self) -> dict[str, Any]:
        prompts = deepcopy(dict(self._defaults.ai_prompts()))
        stored = self._repository.load(AI_PROMPTS_SETTING)
        if isinstance(stored, Mapping):
            prompts.update(deepcopy(dict(stored)))
        return prompts

    def save_ai_prompts(self, prompts: Mapping[str, Any]) -> None:
        self._repository.save(AI_PROMPTS_SETTING, deepcopy(dict(prompts)))

    def reset_ai_prompts(self) -> dict[str, Any]:
        self._repository.delete(AI_PROMPTS_SETTING)
        return deepcopy(dict(self._defaults.ai_prompts()))

    def get_menu_config(self) -> Any:
        menu_config = self._repository.load(MENU_CONFIG_SETTING)
        return deepcopy(menu_config) if menu_config else {}

    def save_menu_config(self, menu_config: Mapping[str, Any] | list[Any]) -> None:
        value = dict(menu_config) if isinstance(menu_config, Mapping) else menu_config
        self._repository.save(MENU_CONFIG_SETTING, deepcopy(value))

    def reset_menu_config(self) -> None:
        self._repository.delete(MENU_CONFIG_SETTING)

    def _normalize_telegram_templates(self, value: Any) -> dict[str, str]:
        source = value if isinstance(value, Mapping) else {}
        normalized = {str(key): str(item) for key, item in self._defaults.telegram_templates().items()}
        for key in normalized:
            if key in source:
                normalized[key] = str(source.get(key) or "")
        return normalized
