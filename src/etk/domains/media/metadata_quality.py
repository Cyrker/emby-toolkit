from __future__ import annotations

import re
from typing import Any


def placeholder_episode_title(title: Any) -> bool:
    value = str(title or "").strip()
    return not value or bool(re.fullmatch(r"(?:第\s*\d+\s*[集话話]|episode\s*\d+|ep\s*\d+)", value, re.I))


def metadata_gaps(kind: str, title: Any, overview: Any) -> list[str]:
    gaps = []
    if kind == "Episode" and placeholder_episode_title(title):
        gaps.append("分集标题")
    if kind in {"Movie", "Series", "Episode"} and not str(overview or "").strip():
        gaps.append("中文简介")
    return gaps
