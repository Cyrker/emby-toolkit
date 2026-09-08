from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

from jinja2.sandbox import SandboxedEnvironment
from pypinyin import Style, pinyin

logger = logging.getLogger(__name__)
_JINJA_ENV = SandboxedEnvironment(autoescape=False)


class P115RenameRenderer:
    """Pure 115 name-template renderer used by native preview and planning."""

    DEFAULT_CUSTOMIZATION = (
        r"(?<!\w)(Baha|CR|B-Global|ABEMA|MyVideo|AMZN|KKTV|friDay|DSNP|LINETV|Crunchyroll|IQ|Hulu|HQ|"
        r"60fps|Paramount\+|LineTV|Linetv|Disney\+|FriDay|HMAX|MAX|NF|IQY|IQ|TX|WeTV|YT|YK|Migu|Mgtv|"
        r"Bilibili|Sohu|Xigua|iTunes)(?!\w)"
    )

    _P115_INVALID_NAME_CHAR_TRANSLATION = str.maketrans(
        {
            "\\": "＼",
            "/": "／",
            ":": "：",
            "*": "＊",
            "?": "？",
            '"': "＂",
            "<": "＜",
            ">": "＞",
            "|": "｜",
        }
    )
    _P115_INVALID_RENDERED_CHAR_TRANSLATION = str.maketrans(
        {
            ":": "：",
            "*": "＊",
            "?": "？",
            '"': "＂",
            "<": "＜",
            ">": "＞",
            "|": "｜",
        }
    )

    def __init__(
        self,
        details: Mapping[str, Any] | None = None,
        tmdb_id: str = "",
        original_title: str = "",
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self.details = dict(details or {})
        self.tmdb_id = str(tmdb_id or "")
        self.original_title = str(original_title or "")
        self.config = dict(config or {})

    @staticmethod
    def normalize_mp_jinja_template(template: object) -> str:
        if not isinstance(template, str):
            return ""
        text = re.sub(
            r"{{\s*([A-Za-z_]\w*)\s*\|\s*string\s*}\s*\.zfill\((\d+)\)\s*}}",
            r"{{ (\1|string).zfill(\2) }}",
            template,
        )
        return re.sub(r"{{\s*fps\s*}}(?:\s*fps|\s*FPS|\s*Fps)", "{{ fps }}", text)

    @staticmethod
    def extract_customization(filename: object, raw_rules: object) -> str:
        if isinstance(raw_rules, str):
            rules: Sequence[object] = raw_rules.splitlines()
        elif isinstance(raw_rules, (list, tuple)):
            rules = raw_rules
        else:
            rules = ()
        matches: list[str] = []
        seen: set[str] = set()
        for raw_rule in rules:
            rule = str(raw_rule or "").strip()
            if not rule:
                continue
            try:
                for match in re.finditer(rule, str(filename or "")):
                    value = match.group(0)
                    if value and value not in seen:
                        seen.add(value)
                        matches.append(value)
            except re.error:
                logger.warning("忽略无效的 115 自定义占位符正则 %r", rule)
        return "@".join(matches)

    @classmethod
    def sanitize_name_component(cls, text: object) -> str:
        return _clean_invisible_chars(text).translate(cls._P115_INVALID_NAME_CHAR_TRANSLATION).strip()

    @classmethod
    def sanitize_rendered_template(cls, text: object) -> str:
        cleaned = _clean_invisible_chars(text).replace("\\", "/")
        cleaned = re.sub(r"[\r\n\t]+", " ", cleaned)
        cleaned = cleaned.translate(cls._P115_INVALID_RENDERED_CHAR_TRANSLATION).strip()
        return cls.cleanup_empty_separators(cleaned)

    @staticmethod
    def cleanup_empty_separators(text: object) -> str:
        cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
        for _ in range(4):
            next_text = re.sub(r"\s*([·•])\s*(?:\1\s*)+", r" \1 ", cleaned)
            next_text = re.sub(r"\s+-\s*(?:-\s*)+", " - ", next_text)
            next_text = re.sub(r"\.{2,}", ".", next_text)
            next_text = re.sub(r"\s+\.\s*(?:\.\s*)+", " . ", next_text)
            next_text = re.sub(r"\s*\.\s*-\s*", " - ", next_text)
            next_text = re.sub(r"\s+([·•.-])\s+([·•.-])\s+", r" \2 ", next_text)
            next_text = re.sub(r"\.+(?=\.[A-Za-z0-9]{1,8}$)", "", next_text)
            next_text = re.sub(r"\s*[·•-]\s*(\.[A-Za-z0-9]{1,8})$", r"\1", next_text)
            next_text = re.sub(r"\s+\.\s+(\.[A-Za-z0-9]{1,8})$", r"\1", next_text)
            next_text = next_text.strip(" ·•.-")
            if next_text == cleaned:
                break
            cleaned = next_text
        return cleaned

    @staticmethod
    def get_title_initial(title: object) -> str:
        for char in str(title or ""):
            if not char.isalnum():
                continue
            try:
                initials = pinyin(char, style=Style.FIRST_LETTER, strict=False)
                value = "".join(item[0].upper() for item in initials if item and item[0].isalnum())
            except Exception:
                value = char
            return str(value or char)[0].upper()
        return ""

    @staticmethod
    def format_video_codec_label(codec: object, style: object = "hevc") -> str:
        text = str(codec or "")
        if not text:
            return ""
        normalized_style = str(style or "hevc").strip().lower().replace(".", "")
        use_h26x = normalized_style in {"h265", "h264", "x265", "x264", "265", "264"}
        text = re.sub(r"(?i)\b(?:HEVC|H[.\s]?265|X265)\b", "H265" if use_h26x else "HEVC", text)
        return re.sub(r"(?i)\b(?:AVC|H[.\s]?264|X264)\b", "H264" if use_h26x else "AVC", text)

    @staticmethod
    def format_audio_label(audio: object, hide_channels: object = False) -> str:
        text = str(audio or "")
        if hide_channels:
            text = re.sub(r"(?<!\d)(?:7[.\s_]?1|5[.\s_]?1|2[.\s_]?0|1[.\s_]?0)(?!\d)", "", text)
            text = re.sub(r"[\s._-]+$", "", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def format_fps_label(fps: object) -> tuple[str, str]:
        text = str(fps or "").strip()
        if not text:
            return "", ""
        match = re.search(r"\d+(?:\.\d+)?", text)
        if not match:
            return text, text
        value = match.group(0)
        return f"{value}fps", value

    def build_template_context(
        self,
        *,
        is_tv: bool = False,
        season_num: object = None,
        episode_num: object = None,
        original_title: object = None,
        original_name: object = None,
        video_info: Mapping[str, Any] | None = None,
        safe_title: object = None,
        file_ext: object = "",
    ) -> dict[str, Any]:
        video = dict(video_info or {})
        date_text = str(self.details.get("date") or "")
        year = date_text[:4] if date_text else ""
        title_zh = self.sanitize_name_component(safe_title or self.details.get("title") or self.original_title)
        title_en = self.sanitize_name_component(
            self.details.get("title_en") or original_title or self.details.get("original_title") or self.original_title
        )
        title_orig = self.sanitize_name_component(
            self.details.get("title_orig")
            or original_title
            or self.details.get("original_title")
            or self.original_title
        )
        season_val = season_num if season_num is not None else (1 if is_tv else None)
        episode_val = episode_num if episode_num is not None else (1 if is_tv else None)
        season_no = f"{int(str(season_val)):02d}" if season_val is not None else ""
        episode_no = f"{int(str(episode_val)):02d}" if episode_val is not None else ""
        season_episode = f"S{season_no}E{episode_no}" if is_tv and season_no and episode_no else ""
        source = video.get("source") or ""
        effect = video.get("effect") or ""
        codec = self.format_video_codec_label(
            video.get("codec") or video.get("videoCodec") or "",
            self.config.get("video_codec_style"),
        )
        audio = self.format_audio_label(
            video.get("audio") or video.get("audioCodec") or "",
            self.config.get("hide_audio_channels"),
        )
        fps_display, fps_value = self.format_fps_label(video.get("fps") or video.get("frame_rate") or "")
        group = video.get("group") or ""
        extension = f".{str(file_ext).lstrip('.')}" if file_ext else ""
        actors_raw = self.details.get("actors") or self.details.get("cast") or []
        if isinstance(actors_raw, str):
            actors = actors_raw
        elif isinstance(actors_raw, list):
            actor_names = []
            for actor in actors_raw[:5]:
                name = actor.get("name") or actor.get("original_name") if isinstance(actor, Mapping) else actor
                if name:
                    actor_names.append(str(name))
            actors = "、".join(actor_names)
        else:
            actors = ""
        original_name_text = _clean_invisible_chars(original_name or safe_title or title_zh)
        name_from_file = os.path.splitext(os.path.basename(original_name_text))[0] if original_name_text else title_zh
        part_match = re.search(
            r"(?i)(?:^|[ ._\-\[\(])(?:part|pt|cd)[ ._\-]*(\d{1,2})(?=$|[ ._\-\]\)])",
            original_name_text,
        )
        part = str(video.get("part") or (part_match.group(1) if part_match else "") or "")
        season_name_en = f"Season {season_no}" if is_tv and season_no else ""
        season_name_en_no0 = f"Season {season_val}" if is_tv and season_val is not None else ""
        season_name_s = f"S{season_no}" if is_tv and season_no else ""
        season_name_s_no0 = f"S{season_val}" if is_tv and season_val is not None else ""
        season_name_zh = f"第 {season_val} 季" if is_tv and season_val is not None else ""
        episode_name_zh = f"第 {episode_val} 集" if is_tv and episode_val is not None else ""
        season_episode_zh = (
            f"第 {season_val} 季 {episode_val} 集"
            if is_tv and season_val is not None and episode_val is not None
            else ""
        )
        edition = video.get("edition") or " ".join(value for value in (source, effect) if value)
        resource_term = video.get("resource_term") or " ".join(
            value for value in (source, effect, video.get("resolution") or "") if value
        )
        return {
            "title": title_zh,
            "title_zh": title_zh,
            "title_initial": self.get_title_initial(title_zh),
            "title_en": title_en,
            "en_title": title_en,
            "en_name": title_en,
            "title_orig": title_orig,
            "original_title": title_orig,
            "name": name_from_file,
            "clean_title": name_from_file,
            "identify_title": name_from_file,
            "year": year,
            "year_pure": year,
            "title_year": f"{title_zh} ({year})" if title_zh and year else title_zh,
            "type": "电视剧" if is_tv else "电影",
            "category": video.get("category") or self.details.get("category") or "",
            "vote_average": self.details.get("vote_average") or "",
            "poster": self.details.get("poster") or self.details.get("poster_path") or "",
            "backdrop": self.details.get("backdrop") or self.details.get("backdrop_path") or "",
            "actors": actors,
            "overview": self.details.get("overview") or "",
            "tmdb": self.tmdb_id,
            "tmdb_id": self.tmdb_id,
            "tmdbid": self.tmdb_id,
            "imdbid": self.details.get("imdb_id") or self.details.get("imdbid") or "",
            "doubanid": self.details.get("douban_id") or self.details.get("doubanid") or "",
            "season": season_val,
            "episode": episode_val,
            "season_no": season_no,
            "episode_no": episode_no,
            "s_e": season_episode,
            "season_episode": season_episode,
            "season_fmt": season_name_s,
            "season_year": str(video.get("season_year") or self.details.get("season_year") or year or ""),
            "episode_title": video.get("episode_title") or self.details.get("episode_title") or "",
            "episode_date": video.get("episode_date") or self.details.get("episode_date") or "",
            "season_name": season_name_en,
            "season_name_en": season_name_en,
            "season_name_en_no0": season_name_en_no0,
            "season_name_s": season_name_s,
            "season_name_s_no0": season_name_s_no0,
            "season_name_zh": season_name_zh,
            "episode_name_zh": episode_name_zh,
            "s_e_zh": season_episode_zh,
            "season_episode_zh": season_episode_zh,
            "resolution": video.get("resolution") or "",
            "source": source,
            "stream": video.get("stream") or "",
            "effect": effect,
            "codec": codec,
            "audio_count": video.get("audio_count") or "",
            "audio": audio,
            "fps": fps_display,
            "fps_value": fps_value,
            "frame_rate": fps_value,
            "group": group,
            "original_name": original_name_text,
            "original_upper": original_name_text.upper(),
            "name_upper": name_from_file.upper(),
            "part": part,
            "file_ext": extension,
            "fileExt": extension,
            "resourceType": source,
            "videoFormat": video.get("resolution") or "",
            "videoCodec": codec,
            "audioCodec": audio,
            "releaseGroup": group,
            "webSource": video.get("stream") or "",
            "videoBit": video.get("videoBit") or video.get("video_bit") or "",
            "resource_term": resource_term,
            "customization": video.get("customization") or "",
            "edition": edition,
        }

    def render_template(self, template: object, **options: Any) -> str:
        normalized = self.normalize_mp_jinja_template(template)
        if not normalized.strip():
            return ""
        try:
            rendered = _JINJA_ENV.from_string(normalized).render(**self.build_template_context(**options))
        except Exception:
            logger.warning("115 重命名模板渲染失败，已回退为空", exc_info=True)
            return ""
        return self.sanitize_rendered_template(rendered) if rendered else ""


def _clean_invisible_chars(text: object) -> str:
    value = str(text or "").replace("\xa0", " ").replace("\u3000", " ")
    value = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]", "", value)
    return re.sub(r"\s+", " ", value).strip()


__all__ = ["P115RenameRenderer"]
