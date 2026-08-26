from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Final


class P115MediaInfoFormatError(ValueError):
    """Raised when raw or historical MediaInfo cannot be normalized safely."""


FormattedMediaInfo = list[dict[str, Any]]
LegacyMediaInfoResponse = dict[str, Any]

RAW_ETK_ALLOWED_KEYS: Final[frozenset[str]] = frozenset(
    {
        "tmdb_id",
        "type",
        "original_language",
        "sha1",
        "preid",
        "season_number",
        "episode_number",
        "quality_source",
    }
)

_AUDIO_LANGUAGE_IDS = frozenset({"chi", "yue", "original", "eng", "jpn", "kor"})
_SUBTITLE_LANGUAGE_IDS = frozenset({"chs", "cht", "original", "eng", "jpn", "kor"})
_DEFAULT_AUDIO_FEATURES = ("公映", "上译", "京译", "长译", "八一", "台配", "粤语")
_DEFAULT_AUDIO_PARAMETERS = (
    "atmos",
    "dts_x",
    "truehd",
    "dts_hd_ma",
    "dts_hd_hra",
    "ddp",
    "dts",
    "flac",
    "ac3",
    "aac",
    "7_1",
    "5_1",
    "2_0",
)
_DEFAULT_SUBTITLE_PRIORITY = (
    "effect",
    "chs",
    "cht",
    "chs_eng",
    "cht_eng",
    "chs_jpn",
    "cht_jpn",
    "chs_kor",
    "cht_kor",
)
_COMMENTARY_PATTERNS = (
    r"导评",
    r"评论",
    r"解说",
    r"audio\s*commentary",
    r"director\s*commentary",
    r"commentary",
    r"comment(?:s|ary)?",
)
_TRADITIONAL_TO_SIMPLIFIED = str.maketrans(
    {
        "粵": "粤",
        "語": "语",
        "國": "国",
        "雙": "双",
        "臺": "台",
        "灣": "湾",
        "聽": "听",
        "導": "导",
        "評": "评",
        "視": "视",
        "體": "体",
        "簡": "简",
        "譯": "译",
        "長": "长",
        "蘋": "苹",
        "華": "华",
    }
)


@dataclass(frozen=True, slots=True)
class P115LanguageMappingRule:
    label: str
    value: str
    aliases: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class P115DefaultStreamPolicy:
    audio_language_priority: tuple[str, ...]
    subtitle_language_priority: tuple[str, ...]
    audio_priority_order: tuple[str, ...]
    audio_features: tuple[str, ...]
    audio_parameter_priority: tuple[str, ...]
    subtitle_priority: tuple[str, ...]
    audio_commentary_priority_enabled: bool


@dataclass(frozen=True, slots=True)
class P115MediaInfoPolicySnapshot:
    """Immutable formatter inputs captured before pure MediaInfo work begins."""

    language_mapping: tuple[P115LanguageMappingRule, ...]
    default_streams: P115DefaultStreamPolicy
    language_mapping_revision: str
    default_stream_config_revision: str

    @classmethod
    def create(
        cls,
        *,
        language_mapping: Sequence[Mapping[str, Any]],
        default_stream_config: Mapping[str, Any],
        language_mapping_revision: str,
        default_stream_config_revision: str,
    ) -> P115MediaInfoPolicySnapshot:
        language_revision = _required_revision(language_mapping_revision, "language_mapping_revision")
        stream_revision = _required_revision(default_stream_config_revision, "default_stream_config_revision")
        if isinstance(language_mapping, (str, bytes, bytearray)) or not isinstance(language_mapping, Sequence):
            raise P115MediaInfoFormatError("language_mapping must be a sequence of objects")
        if not isinstance(default_stream_config, Mapping):
            raise P115MediaInfoFormatError("default_stream_config must be an object")

        rules: list[P115LanguageMappingRule] = []
        for index, raw_rule in enumerate(language_mapping):
            if not isinstance(raw_rule, Mapping):
                raise P115MediaInfoFormatError(f"language_mapping[{index}] must be an object")
            label = str(raw_rule.get("label") or "").strip()
            value = str(raw_rule.get("value") or "").strip().lower()
            aliases = _unique_texts(raw_rule.get("aliases"), lower=True)
            if not label or not value:
                raise P115MediaInfoFormatError(f"language_mapping[{index}] requires label and value")
            rules.append(P115LanguageMappingRule(label, value, aliases))
        if not rules:
            raise P115MediaInfoFormatError("language_mapping must not be empty")

        audio_languages = _clean_language_priority(
            default_stream_config.get("audio_lang_priority"),
            allowed=_AUDIO_LANGUAGE_IDS,
            legacy_value=default_stream_config.get("audio_lang"),
        )
        subtitle_languages = _clean_language_priority(
            default_stream_config.get("subtitle_lang_priority"),
            allowed=_SUBTITLE_LANGUAGE_IDS,
            legacy_value=default_stream_config.get("subtitle_lang"),
        )
        priority_order = list(_unique_allowed(default_stream_config.get("audio_priority_order"), {"param", "feature"}))
        for name in ("param", "feature"):
            if name not in priority_order:
                priority_order.append(name)

        audio_features = _text_config(default_stream_config, "audio_features", _DEFAULT_AUDIO_FEATURES)
        legacy_commentary_defaults = {
            ("公映", "上译", "京译", "长译", "八一", "台配", "粤语", "评论", "导评"),
            ("公映", "上译", "京译", "长译", "央视", "八一", "台配", "粤语", "评论", "导评"),
            ("公映", "上译", "京译", "央视", "长译", "八一", "国配", "台配", "国语", "粤语", "评论", "导评"),
        }
        commentary_enabled = bool(default_stream_config.get("audio_commentary_priority_enabled", False)) or (
            audio_features not in legacy_commentary_defaults
            and any(_is_commentary(feature) for feature in audio_features)
        )
        if not commentary_enabled:
            audio_features = tuple(feature for feature in audio_features if not _is_commentary(feature))

        stream_policy = P115DefaultStreamPolicy(
            audio_language_priority=audio_languages,
            subtitle_language_priority=subtitle_languages,
            audio_priority_order=tuple(priority_order),
            audio_features=audio_features,
            audio_parameter_priority=_text_config(
                default_stream_config,
                "audio_param_priority",
                _DEFAULT_AUDIO_PARAMETERS,
                lower=True,
            ),
            subtitle_priority=_text_config(
                default_stream_config,
                "sub_priority",
                _DEFAULT_SUBTITLE_PRIORITY,
                lower=True,
            ),
            audio_commentary_priority_enabled=commentary_enabled,
        )
        return cls(tuple(rules), stream_policy, language_revision, stream_revision)


def sanitize_raw_ffprobe(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return a detached, share-safe raw ffprobe object without temporary URLs."""

    if not isinstance(raw, Mapping):
        raise P115MediaInfoFormatError("raw ffprobe must be a JSON object")
    sanitized = deepcopy(dict(raw))

    raw_format = sanitized.get("format")
    if isinstance(raw_format, Mapping):
        clean_format = dict(raw_format)
        clean_format.pop("filename", None)
        sanitized["format"] = clean_format

    raw_context = sanitized.get("_etk")
    if isinstance(raw_context, Mapping):
        context = {str(key): deepcopy(value) for key, value in raw_context.items() if str(key) in RAW_ETK_ALLOWED_KEYS}
        if context:
            sanitized["_etk"] = context
        else:
            sanitized.pop("_etk", None)
    else:
        sanitized.pop("_etk", None)
    return sanitized


def format_raw_ffprobe_mediainfo(
    raw: Mapping[str, Any],
    *,
    file_name: str,
    policy: P115MediaInfoPolicySnapshot,
    file_size: int = 0,
    sha1: str = "",
) -> FormattedMediaInfo:
    """Format raw ffprobe JSON into ETK's canonical single-element cache envelope."""

    if not isinstance(policy, P115MediaInfoPolicySnapshot):
        raise P115MediaInfoFormatError("policy must be a P115MediaInfoPolicySnapshot")
    probe = sanitize_raw_ffprobe(raw)
    raw_format = probe.get("format") or {}
    if not isinstance(raw_format, Mapping):
        raise P115MediaInfoFormatError("raw ffprobe format must be an object")
    raw_streams = probe.get("streams")
    if not isinstance(raw_streams, list):
        raise P115MediaInfoFormatError("raw ffprobe streams must be an array")
    if any(not isinstance(stream, Mapping) for stream in raw_streams):
        raise P115MediaInfoFormatError("raw ffprobe streams must contain only objects")
    raw_chapters = probe.get("chapters") or []
    if not isinstance(raw_chapters, list) or any(not isinstance(chapter, Mapping) for chapter in raw_chapters):
        raise P115MediaInfoFormatError("raw ffprobe chapters must be an array of objects")

    name = str(file_name or sha1 or "unknown").strip() or "unknown"
    extension = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    size = _to_int(file_size or raw_format.get("size"))
    duration = _to_float(raw_format.get("duration"))
    runtime_ticks = int(duration * 10_000_000) if duration > 0 else 0
    bitrate = _to_int(raw_format.get("bit_rate"))
    if not bitrate and size and duration > 0:
        bitrate = int(size * 8 / duration)
    container = _container(extension, raw_format.get("format_name"))
    chapters = _format_chapters(raw_chapters)

    media_streams: list[dict[str, Any]] = []
    for raw_stream in raw_streams:
        stream = dict(raw_stream)
        codec_type = str(stream.get("codec_type") or "").lower()
        codec = str(stream.get("codec_name") or "").lower()
        tags = _string_object(stream.get("tags"))
        disposition = _string_object(stream.get("disposition"))
        index = _to_int(stream.get("index"), len(media_streams))
        is_default = bool(disposition.get("default"))
        is_forced = bool(disposition.get("forced"))

        if codec_type == "video":
            if disposition.get("attached_pic"):
                continue
            media_streams.append(
                _format_video_stream(
                    stream,
                    tags=tags,
                    codec=codec,
                    index=index,
                    is_default=is_default,
                    is_forced=is_forced,
                    container_bitrate=bitrate,
                )
            )
        elif codec_type == "audio":
            if not codec or codec in {"unknown", "none", "null", "data"} or codec.isdigit():
                continue
            media_streams.append(
                _format_audio_stream(
                    stream,
                    tags=tags,
                    codec=codec,
                    index=index,
                    is_default=is_default,
                    is_forced=is_forced,
                    policy=policy,
                )
            )
        elif codec_type == "subtitle":
            media_streams.append(
                _format_subtitle_stream(
                    stream,
                    tags=tags,
                    codec=codec,
                    index=index,
                    is_default=is_default,
                    is_forced=is_forced,
                    policy=policy,
                )
            )

    if not media_streams:
        raise P115MediaInfoFormatError("raw ffprobe contains no supported media streams")

    context = _string_object(probe.get("_etk"))
    _select_default_streams(
        media_streams,
        policy.default_streams,
        original_language=str(context.get("original_language") or ""),
        language_mapping=policy.language_mapping,
    )

    source = {
        "Size": size,
        "Type": "Default",
        "Bitrate": bitrate,
        "Formats": [],
        "Chapters": [],
        "IsRemote": True,
        "Protocol": "File",
        "Container": container,
        "MediaStreams": media_streams,
        "RunTimeTicks": runtime_ticks,
        "RequiresClosing": False,
        "RequiresLooping": False,
        "RequiresOpening": False,
        "SupportsProbing": True,
        "IsInfiniteStream": False,
        "HasMixedProtocols": False,
        "SupportsDirectPlay": True,
        "RequiredHttpHeaders": {},
        "SupportsTranscoding": True,
        "SupportsDirectStream": True,
        "ReadAtNativeFramerate": False,
        "AddApiKeyToDirectStreamUrl": False,
    }
    return [{"Chapters": chapters, "MediaSourceInfo": source}]


def normalize_legacy_formatted_mediainfo(raw: Any) -> FormattedMediaInfo:
    """Normalize historical cache shapes into the canonical single-element list."""

    value = _decode_json_value(raw)
    while isinstance(value, list):
        value = next((item for item in value if isinstance(item, (Mapping, list))), None)
        if value is None:
            break
    if not isinstance(value, Mapping):
        raise P115MediaInfoFormatError("ETK media information is not a JSON object")

    root = deepcopy(dict(value))
    source: Any = root.get("MediaSourceInfo")
    while isinstance(source, list):
        source = next((item for item in source if isinstance(item, Mapping)), None)
    media_sources = root.get("MediaSources")
    if source is None and isinstance(media_sources, list):
        source = next((item for item in media_sources if isinstance(item, Mapping)), None)
    if source is None and isinstance(root.get("MediaStreams"), list):
        source = root
    if not isinstance(source, Mapping) or not isinstance(source.get("MediaStreams"), list):
        raise P115MediaInfoFormatError("ETK media information contains no MediaSourceInfo.MediaStreams")

    normalized_source = deepcopy(dict(source))
    chapters = root.get("Chapters")
    if not isinstance(chapters, list):
        chapters = normalized_source.get("Chapters")
    normalized_chapters = deepcopy(chapters) if isinstance(chapters, list) else []
    normalized_source["Chapters"] = []
    return [{"MediaSourceInfo": normalized_source, "Chapters": normalized_chapters}]


def normalize_legacy_mediainfo_response(raw: Any) -> LegacyMediaInfoResponse:
    """Return the historical HTTP/Bridge response object for any supported cache shape."""

    return normalize_legacy_formatted_mediainfo(raw)[0]


def _format_chapters(raw_chapters: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    chapters: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_chapters):
        tags = _string_object(raw.get("tags"))
        chapters.append(
            {
                "Name": tags.get("title") or f"章节 {index + 1}",
                "MarkerType": "Chapter",
                "ChapterIndex": index,
                "StartPositionTicks": int(_to_float(raw.get("start_time")) * 10_000_000),
            }
        )
    return chapters


def _format_video_stream(
    stream: Mapping[str, Any],
    *,
    tags: Mapping[str, Any],
    codec: str,
    index: int,
    is_default: bool,
    is_forced: bool,
    container_bitrate: int,
) -> dict[str, Any]:
    width = _to_int(stream.get("width"))
    height = _to_int(stream.get("height"))
    bit_depth = 0
    for key in ("bits_per_raw_sample", "bits_per_sample"):
        bit_depth = _to_int(stream.get(key))
        if bit_depth:
            break
    pixel_format = str(stream.get("pix_fmt") or "").lower()
    if not bit_depth:
        if "12" in pixel_format:
            bit_depth = 12
        elif "10" in pixel_format:
            bit_depth = 10
        elif pixel_format:
            bit_depth = 8

    frame_rate = _rate_to_float(stream.get("avg_frame_rate")) or _rate_to_float(stream.get("r_frame_rate"))
    color_space = str(stream.get("colorspace") or stream.get("color_space") or "").lower()
    color_transfer = str(stream.get("color_transfer") or "").lower()
    color_primaries = str(stream.get("color_primaries") or "").lower()
    profile = str(stream.get("profile") or "")
    raw_side_data = stream.get("side_data_list")
    side_data: list[Any] = list(raw_side_data) if isinstance(raw_side_data, list) else []
    video_range, extended_type, extended_subtype, extended_description = _video_range(
        side_data,
        color_transfer=color_transfer,
        color_primaries=color_primaries,
    )
    resolution = "4K" if width >= 3800 else "1080p" if width >= 1900 else "720p" if width >= 1200 else ""
    if not resolution and height:
        resolution = f"{height}p"
    codec_display = {
        "hevc": "HEVC",
        "h264": "H264",
        "av1": "AV1",
        "mpeg2video": "MPEG2VIDEO",
        "vc1": "VC1",
    }.get(codec, codec.upper())
    effect = {
        "DolbyVision": "Dolby Vision",
        "HDR10+": "HDR10+",
        "HDR10": "HDR10",
        "HDR": "HDR",
    }.get(extended_type or video_range, "")
    display_title = " ".join(part for part in (resolution, effect, codec_display) if part)
    return {
        "Type": "Video",
        "Codec": codec,
        "Index": index,
        "Level": _to_int(stream.get("level")),
        "Title": str(tags.get("title") or ""),
        "Width": width,
        "Height": height,
        "BitRate": _to_int(stream.get("bit_rate") or container_bitrate),
        "Profile": profile,
        "BitDepth": bit_depth,
        "IsForced": is_forced,
        "Protocol": "File",
        "TimeBase": stream.get("time_base") or "1/1000",
        "IsDefault": is_default,
        "RefFrames": _to_int(stream.get("refs"), 1),
        "ColorSpace": color_space,
        "IsExternal": False,
        "VideoRange": video_range,
        "AspectRatio": f"{width}:{height}" if width and height else "",
        "PixelFormat": pixel_format,
        "DisplayTitle": display_title,
        "IsAnamorphic": False,
        "IsInterlaced": False,
        "ColorTransfer": color_transfer,
        "RealFrameRate": frame_rate,
        "AttachmentSize": 0,
        "ColorPrimaries": color_primaries,
        "AverageFrameRate": frame_rate,
        "ExtendedVideoType": extended_type or "None",
        "IsHearingImpaired": False,
        "ExtendedVideoSubType": extended_subtype,
        "IsTextSubtitleStream": False,
        "SupportsExternalStream": False,
        "ExtendedVideoSubTypeDescription": extended_description,
    }


def _format_audio_stream(
    stream: Mapping[str, Any],
    *,
    tags: Mapping[str, Any],
    codec: str,
    index: int,
    is_default: bool,
    is_forced: bool,
    policy: P115MediaInfoPolicySnapshot,
) -> dict[str, Any]:
    language, display_language, title = _friendly_language(
        tags.get("language"),
        tags.get("title"),
        stream_type="Audio",
        mapping=policy.language_mapping,
    )
    channels = _to_int(stream.get("channels"))
    channel_layout = _channel_layout(channels, stream.get("channel_layout"))
    profile = str(stream.get("profile") or "")
    codec_display = _audio_codec_label(codec, profile, title)
    display_parts = [
        part for part in (display_language if display_language != "未知" else "", codec_display, channel_layout) if part
    ]
    if is_default:
        display_parts.append("(默认)")
    return {
        "Type": "Audio",
        "Codec": codec,
        "Index": index,
        "Title": title,
        "BitRate": _to_int(stream.get("bit_rate")),
        "BitDepth": _to_int(stream.get("bits_per_raw_sample") or stream.get("bits_per_sample")),
        "Channels": channels,
        "IsForced": is_forced,
        "Language": language,
        "Protocol": "File",
        "TimeBase": stream.get("time_base") or "1/1000",
        "IsDefault": is_default,
        "IsExternal": False,
        "SampleRate": _to_int(stream.get("sample_rate")),
        "DisplayTitle": " ".join(display_parts),
        "IsInterlaced": False,
        "ChannelLayout": channel_layout,
        "AttachmentSize": 0,
        "DisplayLanguage": display_language,
        "ExtendedVideoType": "None",
        "IsHearingImpaired": False,
        "ExtendedVideoSubType": "None",
        "IsTextSubtitleStream": False,
        "SupportsExternalStream": False,
        "ExtendedVideoSubTypeDescription": "None",
        "Profile": profile or codec_display,
    }


def _format_subtitle_stream(
    stream: Mapping[str, Any],
    *,
    tags: Mapping[str, Any],
    codec: str,
    index: int,
    is_default: bool,
    is_forced: bool,
    policy: P115MediaInfoPolicySnapshot,
) -> dict[str, Any]:
    language, display_language, title = _friendly_language(
        tags.get("language"),
        tags.get("title"),
        stream_type="Subtitle",
        mapping=policy.language_mapping,
        raw_display_title=tags.get("DisplayTitle"),
    )
    subtitle_codec = _subtitle_codec_label(codec)
    codec_label = f"默认 {subtitle_codec}" if is_default else subtitle_codec
    return {
        "Type": "Subtitle",
        "Codec": subtitle_codec,
        "Index": index,
        "Title": title,
        "IsForced": is_forced,
        "Language": language,
        "Protocol": "File",
        "TimeBase": stream.get("time_base") or "1/1000",
        "IsDefault": is_default,
        "IsExternal": False,
        "DisplayTitle": f"{display_language} ({codec_label})" if display_language != "未知" else f"({codec_label})",
        "IsInterlaced": False,
        "AttachmentSize": 0,
        "DisplayLanguage": display_language,
        "ExtendedVideoType": "None",
        "IsHearingImpaired": False,
        "ExtendedVideoSubType": "None",
        "IsTextSubtitleStream": codec in {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text"},
        "SubtitleLocationType": "InternalStream",
        "SupportsExternalStream": False,
        "ExtendedVideoSubTypeDescription": "None",
    }


def _video_range(
    side_data: Sequence[Any],
    *,
    color_transfer: str,
    color_primaries: str,
) -> tuple[str, str, str, str]:
    side_text = json.dumps(list(side_data), ensure_ascii=False, default=str)
    dv_profile: Any = None
    dv_compatibility: Any = None
    for raw in side_data:
        if not isinstance(raw, Mapping):
            continue
        side_type = str(raw.get("side_data_type") or "").upper()
        if "DOVI" in side_type or "DOLBY VISION" in side_type:
            dv_profile = raw.get("dv_profile") or raw.get("profile")
            dv_compatibility = (
                raw.get("dv_bl_signal_compatibility_id")
                or raw.get("bl_signal_compatibility_id")
                or raw.get("compatibility_id")
            )
            break
    if not dv_profile and ("DOVI" in side_text.upper() or "DOLBY VISION" in side_text.upper()):
        match = re.search(r"dv_profile['\"]?\s*[:=]\s*['\"]?(\d+)", side_text, re.IGNORECASE)
        if match:
            dv_profile = match.group(1)
    if dv_profile:
        profile = str(dv_profile)
        if profile == "8" and str(dv_compatibility or "") == "1":
            subtype, description = "DoviProfile81", "Profile 8.1 (HDR10 compatible)"
        elif profile == "8":
            subtype, description = "DoviProfile8", "Profile 8"
        elif profile == "7":
            subtype = "DoviProfile7"
            description = "Profile 7 (HDR10 compatible)" if color_transfer == "smpte2084" else "Profile 7"
        elif profile == "5":
            subtype, description = "DoviProfile5", "Profile 5"
        else:
            subtype, description = f"DoviProfile{profile}", f"Profile {profile}"
        video_range = "DolbyVision HDR10" if color_transfer == "smpte2084" else "DolbyVision"
        return video_range, "DolbyVision", subtype, description
    upper_side_text = side_text.upper()
    if "HDR10+" in upper_side_text or "SMPTE2094-40" in upper_side_text:
        return "HDR10+", "", "None", "None"
    if color_transfer == "smpte2084":
        return "HDR10", "", "None", "None"
    if color_primaries == "bt2020":
        return "HDR", "", "None", "None"
    return "", "", "None", "None"


def _friendly_language(
    raw_language: Any,
    raw_title: Any,
    *,
    stream_type: str,
    mapping: Sequence[P115LanguageMappingRule],
    raw_display_title: Any = "",
) -> tuple[str, str, str]:
    raw_language_text = str(raw_language or "").strip()
    title = str(raw_title or "").strip().translate(_TRADITIONAL_TO_SIMPLIFIED)
    display_title = str(raw_display_title or "").strip().translate(_TRADITIONAL_TO_SIMPLIFIED)
    title_text = " ".join(part for part in (title, display_title) if part)
    combined = " ".join(part for part in (title_text, raw_language_text) if part)
    language = _language_from_text(title_text, mapping) or _normalize_language_code(raw_language_text, mapping)

    if stream_type == "Subtitle":
        script = _subtitle_script(raw_language_text, title, display_title)
        if script or language in {"chi", "yue"}:
            was_cantonese = language == "yue"
            language = "chi"
            script = script or ("cht" if was_cantonese else "chs")
            second_language = _subtitle_second_language(combined)
            script_label = "繁体" if script == "cht" else "简体"
            display_language = f"中{second_language}双语{script_label}" if second_language else f"中文{script_label}"
        else:
            display_language = _display_language(language, mapping, subtitle=True)
        feature_suffix = "特效" if _has_token(combined, "特效", "effect", "effects", "tx") else ""
        normalized_title = f"{display_language}{feature_suffix}" if display_language != "未知" else feature_suffix
        return language or raw_language_text.lower(), display_language, normalized_title

    display_language = _display_language(language, mapping, subtitle=False)
    normalized_title = title or display_language
    return language or raw_language_text.lower(), display_language, normalized_title


def _select_default_streams(
    streams: list[dict[str, Any]],
    policy: P115DefaultStreamPolicy,
    *,
    original_language: str,
    language_mapping: Sequence[P115LanguageMappingRule],
) -> None:
    original_code = _normalize_language_code(original_language, language_mapping)
    audio_streams = [stream for stream in streams if stream.get("Type") == "Audio"]
    subtitle_streams = [stream for stream in streams if stream.get("Type") == "Subtitle"]
    default_audio: dict[str, Any] | None = None

    if audio_streams:
        candidates = list(audio_streams)
        if not policy.audio_commentary_priority_enabled:
            non_commentary = [stream for stream in candidates if not _is_commentary(_audio_text(stream))]
            if non_commentary:
                candidates = non_commentary
        for preference in policy.audio_language_priority:
            code = original_code if preference == "original" else "chi" if preference in {"chs", "cht"} else preference
            code = _normalize_language_code(code, language_mapping)
            matched = [stream for stream in candidates if str(stream.get("Language") or "").lower() == code]
            if matched:
                candidates = matched
                break
        default_audio = max(candidates, key=lambda stream: _audio_score(stream, policy))
        _mark_single_default(audio_streams, default_audio)

    if subtitle_streams:
        active_features = tuple(
            feature.lower()
            for feature in policy.audio_features
            if feature and default_audio is not None and feature.lower() in _audio_text(default_audio)
        )
        default_subtitle = max(
            subtitle_streams,
            key=lambda stream: _subtitle_score(
                stream,
                policy,
                original_code=original_code,
                active_audio_features=active_features,
            ),
        )
        _mark_single_default(subtitle_streams, default_subtitle)


def _audio_score(stream: Mapping[str, Any], policy: P115DefaultStreamPolicy) -> tuple[int, ...]:
    text = _audio_text(stream)
    parameter_rank = 0
    parameter_count = 0
    for index, parameter in enumerate(policy.audio_parameter_priority):
        if _audio_parameter_matches(stream, parameter, text):
            parameter_rank = len(policy.audio_parameter_priority) - index
            break
    for parameter in policy.audio_parameter_priority:
        if _audio_parameter_matches(stream, parameter, text):
            parameter_count += 1
    feature_rank = 0
    for index, feature in enumerate(policy.audio_features):
        if str(feature).lower() in text:
            feature_rank = max(feature_rank, len(policy.audio_features) - index)
    category = {"param": parameter_rank, "feature": feature_rank}
    return (
        *(category[name] for name in policy.audio_priority_order),
        parameter_count,
        1 if stream.get("IsDefault") else 0,
        _to_int(stream.get("BitRate")),
        -_to_int(stream.get("Index"), 9999),
    )


def _subtitle_score(
    stream: Mapping[str, Any],
    policy: P115DefaultStreamPolicy,
    *,
    original_code: str,
    active_audio_features: Sequence[str],
) -> int:
    text = _normalized_marker_text(
        " ".join(str(stream.get(key) or "") for key in ("Title", "DisplayTitle", "DisplayLanguage", "Language"))
    )
    script = "cht" if "繁体" in text else "chs" if "简体" in text else ""
    language = str(stream.get("Language") or "").lower()
    language_key = script or language
    score = 0
    for index, preference in enumerate(policy.subtitle_language_priority):
        keys = _subtitle_preference_keys(preference, original_code)
        if language_key in keys or script in keys:
            score += (len(policy.subtitle_language_priority) - index) * 10**12
            break
    if not policy.subtitle_language_priority and script:
        score += 10**11
    if active_audio_features and any(feature in text for feature in active_audio_features):
        score += 10**10

    has_english = _has_token(text, "英文", "英语", "中英", "eng", "en")
    has_japanese = _has_token(text, "日文", "日语", "中日", "jpn", "ja")
    has_korean = _has_token(text, "韩文", "韩语", "中韩", "kor", "ko")
    flags = {
        "effect": _has_token(text, "特效", "effect", "effects", "tx"),
        "chs": script == "chs" and not (has_english or has_japanese or has_korean),
        "cht": script == "cht" and not (has_english or has_japanese or has_korean),
        "chs_eng": script == "chs" and has_english,
        "cht_eng": script == "cht" and has_english,
        "chs_jpn": script == "chs" and has_japanese,
        "cht_jpn": script == "cht" and has_japanese,
        "chs_kor": script == "chs" and has_korean,
        "cht_kor": script == "cht" and has_korean,
    }
    for index, name in enumerate(policy.subtitle_priority):
        if flags.get(name, False):
            score += (max(len(policy.subtitle_priority), 1) - index) * 100_000
    if stream.get("IsDefault"):
        score += 1
    return score


def _mark_single_default(streams: Sequence[dict[str, Any]], target: Mapping[str, Any]) -> None:
    for stream in streams:
        selected = stream is target
        stream["IsDefault"] = selected
        if not selected:
            stream["IsForced"] = False
        display = re.sub(r"\(默认\s*", "(", str(stream.get("DisplayTitle") or ""))
        display = display.replace("(默认)", "").replace("默认", "").replace("()", "").strip()
        if selected:
            display = f"{display} (默认)".strip()
        stream["DisplayTitle"] = re.sub(r"\s{2,}", " ", display)


def _audio_parameter_matches(stream: Mapping[str, Any], parameter: str, text: str) -> bool:
    codec = str(stream.get("Codec") or "").lower()
    profile = str(stream.get("Profile") or "").lower()
    layout = str(stream.get("ChannelLayout") or "").lower()
    channels = _to_int(stream.get("Channels"))
    parameter = str(parameter or "").lower()
    dts_x = bool(re.search(r"dts[\s\-:]?x", text))
    dts_hd_ma = (
        "dts-hd ma" in text
        or "dts hd ma" in text
        or (codec == "dts" and any(value in profile for value in ("ma", "master", "xll")))
    )
    dts_hd_hra = (
        "dts-hd hra" in text
        or "dts hd hra" in text
        or (codec == "dts" and ("hra" in profile or "high resolution" in profile))
    )
    checks = {
        "atmos": "atmos" in text or "全景声" in text,
        "dts_x": dts_x,
        "truehd": codec == "truehd" or "truehd" in text or "true hd" in text,
        "dts_hd_ma": dts_hd_ma,
        "dts_hd_hra": dts_hd_hra,
        "ddp": codec == "eac3" or any(value in text for value in ("ddp", "e-ac-3", "eac3", "dolby digital+")),
        "dts": (codec == "dts" or "dts" in text) and not (dts_x or dts_hd_ma or dts_hd_hra),
        "flac": codec == "flac" or "flac" in text,
        "ac3": codec == "ac3" or " ac3" in f" {text}" or "dolby digital" in text,
        "aac": codec == "aac" or "aac" in text,
        "7_1": channels == 8 or "7.1" in text or "7 1" in layout,
        "5_1": channels == 6 or "5.1" in text or "5 1" in layout,
        "2_0": channels == 2 or "2.0" in text or "2 0" in layout or "stereo" in text,
        "stereo": channels == 2 or "stereo" in text,
    }
    return checks.get(parameter, bool(parameter and parameter in text))


def _friendly_rule_code(rule: P115LanguageMappingRule) -> str:
    for alias in rule.aliases:
        if len(alias) == 3 and alias.isalpha():
            return alias
    return rule.value


def _normalize_language_code(value: Any, mapping: Sequence[P115LanguageMappingRule]) -> str:
    normalized = str(value or "").strip().lower().replace("_", "-")
    if not normalized:
        return ""
    if normalized in {
        "guo",
        "guoyu",
        "chs",
        "zh-cn",
        "zh-sg",
        "zh-hans",
        "cmn",
        "mandarin",
        "taiwan",
        "台湾",
        "台灣",
        "国语",
        "普通话",
        "中文",
        "简体",
        "简中",
    }:
        return "chi"
    if normalized in {
        "cht",
        "zh-hk",
        "zh-tw",
        "zh-hant",
        "hk",
        "tw",
        "cantonese",
        "粤语",
        "繁体",
        "繁中",
        "粤配",
        "港配",
        "广东话",
    }:
        return "yue"
    for rule in mapping:
        if normalized in {rule.value, rule.label.lower(), *rule.aliases}:
            return _friendly_rule_code(rule)
    return normalized


def _language_from_text(text: str, mapping: Sequence[P115LanguageMappingRule]) -> str:
    normalized_text = _normalized_marker_text(text)
    if _has_token(normalized_text, "国语", "国配", "公映", "台配", "京译", "上译", "央译", "普通话"):
        return "chi"
    if _has_token(normalized_text, "粤语", "粤配", "港配", "粤英双语", "广东话"):
        return "yue"
    for rule in mapping:
        markers = (rule.label, rule.value, *rule.aliases)
        if any(_has_token(normalized_text, marker) for marker in markers):
            return _friendly_rule_code(rule)
    return ""


def _display_language(
    language: str,
    mapping: Sequence[P115LanguageMappingRule],
    *,
    subtitle: bool,
) -> str:
    if language == "chi":
        return "中文简体" if subtitle else "国语"
    if language == "yue":
        return "中文繁体" if subtitle else "粤语"
    for rule in mapping:
        if language == _friendly_rule_code(rule):
            label = rule.label
            return f"{label[:-1]}文" if subtitle and label.endswith("语") and label != "无语言" else label
    return language.upper() if language else "未知"


def _subtitle_script(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values).lower().replace("_", "-")
    if "繁" in text or _has_token(text, "cht", "tc", "big5", "zh-tw", "zh-hk", "zh-hant", "traditional"):
        return "cht"
    if "简" in text or "簡" in text or _has_token(text, "chs", "sc", "gb", "zh-cn", "zh-hans", "simplified"):
        return "chs"
    if _has_token(text, "chi", "zho", "zh", "cmn", "中文"):
        return "chs"
    return ""


def _subtitle_second_language(text: str) -> str:
    if _has_token(text, "中英", "英文", "英语", "english", "eng"):
        return "英"
    if _has_token(text, "中日", "日文", "日语", "japanese", "jpn"):
        return "日"
    if _has_token(text, "中韩", "韩文", "韩语", "korean", "kor"):
        return "韩"
    return ""


def _subtitle_preference_keys(preference: str, original_code: str) -> tuple[str, ...]:
    preference = str(preference or "").lower()
    if preference == "original":
        if original_code == "chi":
            return ("chs", "cht")
        if original_code == "yue":
            return ("cht", "chs")
        return (original_code,) if original_code in {"eng", "jpn", "kor"} else ()
    return (preference,) if preference in {"chs", "cht", "eng", "jpn", "kor"} else ()


def _audio_text(stream: Mapping[str, Any]) -> str:
    return " ".join(
        str(stream.get(key) or "")
        for key in ("Title", "DisplayTitle", "Codec", "Profile", "ChannelLayout", "DisplayLanguage")
    ).lower()


def _is_commentary(text: str) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in _COMMENTARY_PATTERNS)


def _channel_layout(channels: int, raw_layout: Any) -> str:
    labels = {8: "7.1", 7: "6.1", 6: "5.1", 2: "stereo", 1: "mono"}
    if channels in labels:
        return labels[channels]
    layout = str(raw_layout or "").lower()
    return layout.replace("(side)", "") if layout else str(channels) if channels else ""


def _audio_codec_label(codec: str, profile: str = "", title: str = "") -> str:
    profile_mix = f"{profile} {title}".lower()
    if codec == "truehd":
        return "TRUEHD Atmos" if "atmos" in profile_mix else "TRUEHD"
    if codec == "eac3":
        return "DDP Atmos" if "atmos" in profile_mix else "DDP"
    if codec == "dts":
        if any(value in profile_mix for value in ("ma", "master", "xll")):
            return "DTS-HD MA"
        if "hra" in profile_mix or "high resolution" in profile_mix:
            return "DTS-HD HRA"
        return "DTS"
    return {"ac3": "AC3", "aac": "AAC", "flac": "FLAC", "opus": "OPUS", "mp3": "MP3"}.get(codec, codec.upper())


def _subtitle_codec_label(codec: str) -> str:
    return {
        "hdmv_pgs_subtitle": "PGSSUB",
        "pgssub": "PGSSUB",
        "subrip": "SUBRIP",
        "srt": "SUBRIP",
        "ass": "ASS",
        "ssa": "SSA",
        "webvtt": "VTT",
        "mov_text": "MOV_TEXT",
        "dvd_subtitle": "DVDSUB",
    }.get(codec, codec.upper())


def _container(extension: str, raw_format_name: Any) -> str:
    if extension:
        return extension
    format_name = str(raw_format_name or "").lower()
    if "matroska" in format_name:
        return "mkv"
    if "mov" in format_name or "mp4" in format_name:
        return "mp4"
    return format_name.split(",", 1)[0] if format_name else ""


def _rate_to_float(value: Any) -> float | None:
    text = str(value or "")
    if not text or text == "0/0":
        return None
    try:
        if "/" in text:
            numerator, denominator = text.split("/", 1)
            denominator_value = float(denominator)
            return float(numerator) / denominator_value if denominator_value else None
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _required_revision(value: Any, field: str) -> str:
    revision = str(value or "").strip()
    if not revision:
        raise P115MediaInfoFormatError(f"{field} must be a non-empty string")
    return revision


def _clean_language_priority(value: Any, *, allowed: frozenset[str], legacy_value: Any) -> tuple[str, ...]:
    values = [value] if isinstance(value, str) and value.strip() else value
    result = list(_unique_allowed(values, allowed))
    legacy = str(legacy_value or "").strip().lower()
    if not result and legacy in allowed:
        result.append(legacy)
    return tuple(result)


def _unique_allowed(value: Any, allowed: set[str] | frozenset[str]) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    result: list[str] = []
    for raw in value:
        item = str(raw or "").strip().lower()
        if item in allowed and item not in result:
            result.append(item)
    return tuple(result)


def _unique_texts(value: Any, *, lower: bool = False) -> tuple[str, ...]:
    if isinstance(value, str):
        values: Sequence[Any] = (value,)
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        values = value
    else:
        values = ()
    result: list[str] = []
    for raw in values:
        item = str(raw or "").strip()
        if lower:
            item = item.lower()
        if item and item not in result:
            result.append(item)
    return tuple(result)


def _text_config(
    config: Mapping[str, Any],
    key: str,
    default: Sequence[str],
    *,
    lower: bool = False,
) -> tuple[str, ...]:
    raw = config.get(key)
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raw = default
    return _unique_texts(raw, lower=lower)


def _normalized_marker_text(value: Any) -> str:
    text = str(value or "").lower().translate(_TRADITIONAL_TO_SIMPLIFIED)
    text = re.sub(r"[\._\-+/|\\\[\]【】()（）]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _has_token(text: Any, *markers: Any) -> bool:
    normalized = _normalized_marker_text(text)
    for marker in markers:
        candidate = _normalized_marker_text(marker)
        if not candidate:
            continue
        if re.search(r"[\u4e00-\u9fff]", candidate):
            if candidate in normalized:
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(candidate)}(?![a-z0-9])", normalized):
            return True
    return False


def _decode_json_value(raw: Any) -> Any:
    value = raw
    if isinstance(value, (bytes, bytearray)):
        try:
            value = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise P115MediaInfoFormatError("ETK media information is not valid UTF-8") from exc
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise P115MediaInfoFormatError("ETK media information is not valid JSON") from exc
    return value


def _string_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


__all__ = [
    "FormattedMediaInfo",
    "LegacyMediaInfoResponse",
    "P115DefaultStreamPolicy",
    "P115LanguageMappingRule",
    "P115MediaInfoFormatError",
    "P115MediaInfoPolicySnapshot",
    "RAW_ETK_ALLOWED_KEYS",
    "format_raw_ffprobe_mediainfo",
    "normalize_legacy_formatted_mediainfo",
    "normalize_legacy_mediainfo_response",
    "sanitize_raw_ffprobe",
]
