from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Protocol

from etk.domains.subscription.moviepilot import (
    MoviePilotProviderError,
    MoviePilotWriteOutcome,
)
from etk.platform.jobs import PersistentEffectIndeterminate

from .models import P115Result

MOVIEPILOT_CONFIG_SETTING = "mp_config"
MOVIE_RENAME_SETTING = "MOVIE_RENAME_FORMAT"
TV_RENAME_SETTING = "TV_RENAME_FORMAT"

_ETK_TO_MP_RENAME_VARS = {
    "title_zh": "title",
    "title_en": "en_title",
    "title_orig": "original_title",
    "year_pure": "year",
    "tmdb": "tmdbid",
    "tmdb_id": "tmdbid",
    "clean_title": "name",
    "identify_title": "name",
    "source": "resourceType",
    "resolution": "videoFormat",
    "stream": "webSource",
    "codec": "videoCodec",
    "audio": "audioCodec",
    "audio_count": "audioCodec",
    "group": "releaseGroup",
    "s_e": "season_episode",
    "season_name_s": "season_fmt",
    "file_ext": "fileExt",
}
_MP_TO_ETK_RENAME_VARS = {
    "en_title": "title_en",
    "en_name": "title_en",
    "resourceType": "source",
    "videoFormat": "resolution",
    "webSource": "stream",
    "videoCodec": "codec",
    "audioCodec": "audio",
    "releaseGroup": "group",
}
_MP_RENAME_SUPPORTED_VARS = frozenset(
    {
        "title",
        "en_title",
        "original_title",
        "name",
        "en_name",
        "original_name",
        "year",
        "title_year",
        "type",
        "category",
        "vote_average",
        "poster",
        "backdrop",
        "actors",
        "overview",
        "resourceType",
        "effect",
        "edition",
        "videoFormat",
        "resource_term",
        "releaseGroup",
        "videoCodec",
        "videoBit",
        "audioCodec",
        "fps",
        "webSource",
        "tmdbid",
        "imdbid",
        "doubanid",
        "part",
        "fileExt",
        "customization",
        "season",
        "season_fmt",
        "season_year",
        "episode",
        "season_episode",
        "episode_title",
        "episode_date",
    }
)
_MP_RENAME_ALLOWED_FILTERS = frozenset({"upper", "lower", "string", "default", "trim", "replace", "zfill"})
_MP_RENAME_OPTIONAL_SEGMENT_VARS = frozenset(
    {
        "resourceType",
        "effect",
        "edition",
        "videoFormat",
        "resource_term",
        "releaseGroup",
        "videoCodec",
        "videoBit",
        "audioCodec",
        "fps",
        "webSource",
        "part",
        "customization",
    }
)


class MoviePilotRenameGateway(Protocol):
    def get_system_setting(self, key: str) -> Any: ...

    def set_system_setting(self, key: str, value: Any) -> object: ...


class MoviePilotConfigRevision(Protocol):
    def revision(self, key: str) -> str: ...


class P115RenameMoviePilotService:
    def __init__(
        self,
        *,
        gateway: MoviePilotRenameGateway,
        settings: MoviePilotConfigRevision,
    ) -> None:
        self._gateway = gateway
        self._settings = settings

    def import_templates(self) -> P115Result:
        try:
            movie_template = str(self._gateway.get_system_setting(MOVIE_RENAME_SETTING) or "")
            tv_template = str(self._gateway.get_system_setting(TV_RENAME_SETTING) or "")
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 400)
        movie_parts = _split_from_right(movie_template, 1)
        tv_parts = _split_from_right(tv_template, 2)
        if not movie_parts:
            return P115Result(
                {"success": False, "message": "MoviePilot 电影模板格式不正确，至少需要 主目录/文件名 两段"},
                400,
            )
        if not tv_parts:
            return P115Result(
                {"success": False, "message": "MoviePilot 剧集模板格式不正确，至少需要 主目录/季目录/文件名 三段"},
                400,
            )
        tv_file_template = convert_mp_rename_template_to_etk(tv_parts[2])
        return P115Result(
            {
                "success": True,
                "message": "已读取 MoviePilot 重命名模板",
                "data": {
                    "main_dir_template": convert_mp_rename_template_to_etk(movie_parts[0]),
                    "season_dir_template": convert_mp_rename_template_to_etk(tv_parts[1]),
                    "file_template": tv_file_template,
                    "movie_file_template": convert_mp_rename_template_to_etk(movie_parts[1]),
                    "tv_file_template": tv_file_template,
                    "mp_movie_template": movie_template,
                    "mp_tv_template": tv_template,
                },
            }
        )

    def export_parameters(self, raw_payload: object) -> P115Result:
        payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
        movie_file = payload.get("movie_file_template") or payload.get("file_template")
        tv_file = payload.get("tv_file_template") or payload.get("file_template")
        main_dir, main_unsupported = convert_etk_rename_template_to_mp(payload.get("main_dir_template"))
        season_dir, season_unsupported = convert_etk_rename_template_to_mp(payload.get("season_dir_template"))
        movie_file_template, movie_unsupported = convert_etk_rename_template_to_mp(movie_file)
        tv_file_template, tv_unsupported = convert_etk_rename_template_to_mp(tv_file)
        unsupported = list(dict.fromkeys(main_unsupported + season_unsupported + movie_unsupported + tv_unsupported))
        if unsupported:
            return P115Result(
                {
                    "success": False,
                    "message": (
                        f"MoviePilot 不支持这些 ETK 变量：{'、'.join(unsupported)}。"
                        "请先从模板里删除或改用 MP 支持的变量。"
                    ),
                },
                400,
            )
        movie_template = _join_template(main_dir, _ensure_file_ext(movie_file_template))
        tv_template = _join_template(main_dir, season_dir, _ensure_file_ext(tv_file_template))
        if not movie_template or not tv_template:
            return P115Result(
                {"success": False, "message": "主目录、季目录、电影文件名和剧集文件名模板不能为空"},
                400,
            )
        return P115Result(
            {
                "success": True,
                "data": {
                    "movie_template": movie_template,
                    "tv_template": tv_template,
                    "configuration_revision": self._settings.revision(MOVIEPILOT_CONFIG_SETTING),
                },
            }
        )


class P115MoviePilotRenameExportOperation:
    def __init__(
        self,
        *,
        gateway: MoviePilotRenameGateway,
        settings: MoviePilotConfigRevision,
    ) -> None:
        self._gateway = gateway
        self._settings = settings

    def execute(self, parameters: Mapping[str, Any], progress: Any) -> Mapping[str, Any]:
        movie_template = str(parameters.get("movie_template") or "")
        tv_template = str(parameters.get("tv_template") or "")
        expected_revision = str(parameters.get("configuration_revision") or "")
        if not movie_template or not tv_template:
            raise ValueError("MoviePilot rename templates are required")
        self._assert_revision(expected_revision)
        pairs = ((MOVIE_RENAME_SETTING, movie_template), (TV_RENAME_SETTING, tv_template))
        for index, (key, desired) in enumerate(pairs, start=1):
            progress.checkpoint()
            progress.update(10 + index * 30, f"正在写入 MoviePilot 配置 {key}")
            self._assert_revision(expected_revision)
            current = self._gateway.get_system_setting(key)
            if current == desired:
                continue
            label = f"moviepilot-system-setting:{key}"
            progress.record_started_effect(label)
            try:
                self._gateway.set_system_setting(key, desired)
            except MoviePilotProviderError as exc:
                if exc.outcome is MoviePilotWriteOutcome.REJECTED:
                    progress.record_abandoned_effect(label)
                elif exc.outcome is MoviePilotWriteOutcome.INDETERMINATE:
                    raise PersistentEffectIndeterminate(str(exc)) from exc
                raise
            progress.record_committed_effect(label)
            self._assert_revision(expected_revision)
        progress.update(100, "MoviePilot 重命名模板已确认")
        return {
            "ok": True,
            "mp_movie_template": movie_template,
            "mp_tv_template": tv_template,
        }

    def _assert_revision(self, expected: str) -> None:
        if self._settings.revision(MOVIEPILOT_CONFIG_SETTING) != expected:
            raise RuntimeError("MoviePilot 配置在任务执行期间发生变化")


def convert_etk_rename_template_to_mp(template: object) -> tuple[str, list[str]]:
    text = str(template or "")
    exact_replacements = {
        r"{{\s*season_no\s*}}": "{{ season|string }}.zfill(2)",
        r"{{\s*episode_no\s*}}": "{{ episode|string }}.zfill(2)",
        r"{{\s*season_name_en\s*}}": "Season {{ season|string }}.zfill(2)",
        r"{{\s*season_name_en_no0\s*}}": "Season {{ season }}",
        r"{{\s*season_name_s_no0\s*}}": "S{{ season }}",
        r"{{\s*season_name_zh\s*}}": "第 {{ season }} 季",
        r"{{\s*episode_name_zh\s*}}": "第 {{ episode }} 集",
        r"{{\s*season_episode_zh\s*}}": "第 {{ season }} 季 {{ episode }} 集",
    }
    for pattern, replacement in exact_replacements.items():
        text = re.sub(pattern, replacement, text)

    def replace_tag(match: re.Match[str]) -> str:
        tag = match.group(0)
        return _replace_jinja_code(tag, _ETK_TO_MP_RENAME_VARS)

    text = re.sub(r"({[{%#][\s\S]*?[}%#]})", replace_tag, text)
    text = _dedupe_audio_codec_segments(text)
    text = _wrap_optional_segments(text)
    unsupported: list[str] = []
    keywords = {"if", "endif", "else", "elif", "for", "endfor", "in", "and", "or", "not", "is"}
    keywords.update({"none", "true", "false"})
    for tag in re.findall(r"{[{%][\s\S]*?[}%]}", text):
        for name in re.findall(r"\b[A-Za-z_]\w*\b", _strip_jinja_strings(tag)):
            if name in keywords or name in _MP_RENAME_ALLOWED_FILTERS or name in _MP_RENAME_SUPPORTED_VARS:
                continue
            if name not in unsupported:
                unsupported.append(name)
    return text, unsupported


def convert_mp_rename_template_to_etk(template: object) -> str:
    text = str(template or "")

    def replace_tag(match: re.Match[str]) -> str:
        tag = match.group(0)
        return _replace_jinja_code(tag, _MP_TO_ETK_RENAME_VARS)

    return re.sub(r"({[{%#][\s\S]*?[}%#]})", replace_tag, text)


def _split_from_right(template: object, separator_count: int) -> list[str] | None:
    text = str(template or "").strip()
    parts: list[str] = []
    end = len(text)
    for _ in range(separator_count):
        index = text.rfind("/", 0, end)
        if index < 0:
            return None
        parts.insert(0, text[index + 1 : end].strip())
        end = index
    parts.insert(0, text[:end].strip())
    return parts if all(parts) else None


def _join_template(*parts: object) -> str:
    clean = [str(part or "").strip().strip("/") for part in parts]
    return "/".join(clean) if all(clean) else ""


_JINJA_STRING_RE = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"")


def _strip_jinja_strings(value: str) -> str:
    return _JINJA_STRING_RE.sub("", value)


def _replace_jinja_code(value: str, replacements: Mapping[str, str]) -> str:
    chunks: list[str] = []
    cursor = 0
    for match in _JINJA_STRING_RE.finditer(value):
        chunks.append(_replace_jinja_variables(value[cursor : match.start()], replacements))
        chunks.append(match.group(0))
        cursor = match.end()
    chunks.append(_replace_jinja_variables(value[cursor:], replacements))
    return "".join(chunks)


def _replace_jinja_variables(value: str, replacements: Mapping[str, str]) -> str:
    for old, new in replacements.items():
        value = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])", new, value)
    return value


def _ensure_file_ext(template: object) -> str:
    text = str(template or "").strip()
    return text if re.search(r"\b(fileExt|file_ext)\b", text) else f"{text}{{{{fileExt}}}}"


def _dedupe_audio_codec_segments(template: str) -> str:
    audio_expr = r"\{\{\s*audioCodec(?:\s*\|[^}]*)?\s*\}\}"
    pattern = re.compile(rf"({audio_expr})(?P<sep>\s+(?:[·•]|\-)\s*){audio_expr}")
    previous = None
    text = template
    while previous != text:
        previous = text
        text = pattern.sub(r"\1", text)
    return text


def _wrap_optional_segments(template: str) -> str:
    def replace(match: re.Match[str]) -> str:
        prefix = match.group("prefix")
        expression = match.group("expr").strip()
        variable = re.match(r"([A-Za-z_]\w*)", expression)
        if variable is None or variable.group(1) not in _MP_RENAME_OPTIONAL_SEGMENT_VARS:
            return match.group(0)
        guard = variable.group(1)
        return f"{{% if {guard} %}}{prefix}{{{{{expression}}}}}{{% endif %}}"

    pattern = re.compile(r"(?P<prefix>\s+(?:[·•]|\-)\s*)\{\{\s*(?P<expr>[A-Za-z_]\w*(?:\s*\|[^}]*)?)\s*\}\}")
    return pattern.sub(replace, template)


__all__ = [
    "P115MoviePilotRenameExportOperation",
    "P115RenameMoviePilotService",
    "convert_etk_rename_template_to_mp",
    "convert_mp_rename_template_to_etk",
]
