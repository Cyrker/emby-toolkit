from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from .models import P115Result

P115_RENAME_CONFIG_KEY = "p115_rename_config"
P115_RENAME_DEFAULT_CUSTOMIZATION = (
    r"(?<!\w)(Baha|CR|B-Global|ABEMA|MyVideo|AMZN|KKTV|friDay|DSNP|LINETV|Crunchyroll|IQ|Hulu|HQ|"
    r"60fps|Paramount\+|LineTV|Linetv|Disney\+|FriDay|HMAX|MAX|NF|IQY|IQ|TX|WeTV|YT|YK|Migu|Mgtv|"
    r"Bilibili|Sohu|Xigua|iTunes)(?!\w)"
)

_FILE_TEMPLATE = (
    "{{title}}{% if year %} ({{year}}){% endif %}"
    "{% if season_episode %} · {{season_episode}}{% endif %}"
    "{% if resolution %} · {{resolution}}{% endif %}"
    "{% if videoCodec %} · {{videoCodec | upper}}{% endif %}"
    "{% if audioCodec %} · {{audioCodec}}{% endif %}"
    "{% if releaseGroup %} · {{releaseGroup}}{% endif %}{{fileExt}}"
)
_MOVIE_FILE_TEMPLATE = (
    "{{title}}{% if year %} ({{year}}){% endif %}"
    "{% if resolution %} · {{resolution}}{% endif %}"
    "{% if videoCodec %} · {{videoCodec | upper}}{% endif %}"
    "{% if audioCodec %} · {{audioCodec}}{% endif %}"
    "{% if releaseGroup %} · {{releaseGroup}}{% endif %}{{fileExt}}"
)

P115_RENAME_CONFIG_DEFAULTS: dict[str, Any] = {
    "keep_original_name": False,
    "main_title_lang": "zh",
    "main_year_en": True,
    "main_tmdb_fmt": "{tmdb=ID}",
    "season_fmt": "Season {02}",
    "main_dir_template": "{{title}}{% if year %} ({{year}}){% endif %} {tmdb={{tmdbid}}}",
    "season_dir_template": "Season {{season_no}}",
    "file_template": _FILE_TEMPLATE,
    "movie_file_template": _MOVIE_FILE_TEMPLATE,
    "tv_file_template": _FILE_TEMPLATE,
    "file_format": [
        "title_zh",
        "sep_dash_space",
        "year",
        "sep_middot_space",
        "s_e",
        "sep_middot_space",
        "resolution",
        "sep_middot_space",
        "codec",
        "sep_middot_space",
        "audio",
        "sep_middot_space",
        "group",
    ],
    "file_tmdb_fmt": "none",
    "video_codec_style": "hevc",
    "hide_audio_channels": False,
    "strm_url_fmt": "standard",
    "customization": P115_RENAME_DEFAULT_CUSTOMIZATION,
}


@dataclass(frozen=True, slots=True)
class P115RenameConfigSnapshot:
    config: dict[str, Any]

    @property
    def revision(self) -> str:
        encoded = json.dumps(
            effective_rename_config(self.config),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


class P115RenameConfigRepository(Protocol):
    def load(self) -> P115RenameConfigSnapshot: ...

    def save(self, config: Mapping[str, Any]) -> P115RenameConfigSnapshot: ...


class P115RenamePreviewGateway(Protocol):
    def preview(
        self,
        *,
        config: Mapping[str, Any],
        templates: Mapping[str, Any],
    ) -> Mapping[str, str]: ...


class P115WashingPriorityConfigGateway(Protocol):
    def save_conflict_mode(self, conflict_mode: object) -> None: ...


class P115RenameConfigService:
    def __init__(
        self,
        *,
        repository: P115RenameConfigRepository,
        preview: P115RenamePreviewGateway,
        washing_priority: P115WashingPriorityConfigGateway,
    ) -> None:
        self._repository = repository
        self._preview = preview
        self._washing_priority = washing_priority

    def get_config(self) -> P115Result:
        snapshot = self._repository.load()
        return P115Result({"success": True, "data": effective_rename_config(snapshot.config)})

    def save_config(self, raw_config: object) -> P115Result:
        config = deepcopy(dict(raw_config)) if isinstance(raw_config, Mapping) else {}
        legacy_conflict_mode = config.pop("conflict_mode", None)
        if legacy_conflict_mode:
            self._washing_priority.save_conflict_mode(legacy_conflict_mode)
        self._repository.save(config)
        return P115Result({"success": True, "message": "重命名规则已保存"})

    def preview_config(self, raw_payload: object) -> P115Result:
        payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
        raw_config = payload.get("config")
        raw_templates = payload.get("templates")
        config = dict(raw_config) if isinstance(raw_config, Mapping) else {}
        templates = dict(raw_templates) if isinstance(raw_templates, Mapping) else {}
        try:
            result = dict(self._preview.preview(config=config, templates=templates))
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 400)
        return P115Result({"success": True, "data": result})


def effective_rename_config(raw_config: Mapping[str, Any]) -> dict[str, Any]:
    config = deepcopy(dict(raw_config))
    effective = deepcopy(P115_RENAME_CONFIG_DEFAULTS)
    effective.update(config)
    effective.pop("conflict_mode", None)
    if not config.get("movie_file_template"):
        effective["movie_file_template"] = config.get("file_template") or effective["movie_file_template"]
    if not config.get("tv_file_template"):
        effective["tv_file_template"] = config.get("file_template") or effective["tv_file_template"]
    if not effective.get("file_template"):
        effective["file_template"] = effective.get("tv_file_template")
    return effective


__all__ = [
    "P115RenameConfigRepository",
    "P115RenameConfigService",
    "P115RenameConfigSnapshot",
    "P115RenamePreviewGateway",
    "P115WashingPriorityConfigGateway",
    "P115_RENAME_CONFIG_DEFAULTS",
    "P115_RENAME_CONFIG_KEY",
    "effective_rename_config",
]
