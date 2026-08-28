from __future__ import annotations

from typing import Any


class DisabledCompatibilityApplication:
    """Marker passed to compatibility-only effect ports in the native profile."""

    flask_app = None

    def materialize(self) -> Any:
        raise RuntimeError("legacy Flask compatibility is disabled in the native runtime")
