from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SystemResult:
    payload: Any
    status_code: int = 200
