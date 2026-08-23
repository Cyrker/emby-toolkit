from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


class LegacyStandaloneSettingsDefaults:
    """Read retained constant payloads lazily until P11 removes the compatibility tree."""

    def telegram_templates(self) -> Mapping[str, Any]:
        import constants

        return deepcopy(constants.DEFAULT_TELEGRAM_NOTIFICATION_TEMPLATES)

    def ai_prompts(self) -> Mapping[str, Any]:
        import utils

        return deepcopy(utils.DEFAULT_AI_PROMPTS)
