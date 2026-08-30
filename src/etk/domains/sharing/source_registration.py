from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from etk.domains.p115.media_info_formatter import sanitize_raw_ffprobe
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

_SHA1_PATTERN = re.compile(r"^[0-9A-F]{40}$")
MAX_SHARED_RAW_FFPROBE_BYTES = 8 * 1024 * 1024
MAX_SHARED_RAW_SUMMARY_BYTES = 128 * 1024
_MAX_SHARED_RAW_STRING_BYTES = 64 * 1024
_MAX_SHARED_RAW_SUMMARY_STRING_BYTES = 4 * 1024
_SENSITIVE_RAW_VALUE_PATTERN = re.compile(
    r"(?:https?|ftp|file)://|(?:^|[?&])(?:access_?token|api_?key|password|secret|token)="
    r"|(?:^|[\\/])(?:Users|home|tmp|var/folders|Volumes)[\\/]"
    r"|(?:^|\s)Bearer\s+",
    re.IGNORECASE,
)

_SOURCE_STATE_FIELDS = (
    "id",
    "source_key",
    "source_kind",
    "center_source_id",
    "tmdb_id",
    "item_type",
    "parent_series_tmdb_id",
    "season_number",
    "episode_number",
    "title",
    "release_year",
    "source_provider",
    "status",
    "center_status",
    "manifest_hash",
    "manifest_version",
    "file_count",
    "total_size",
    "sha1",
    "preid",
    "size",
    "file_name",
    "root_fid",
    "root_name",
    "media_signature_json",
    "rapid_meta_json",
)
_FILE_STATE_FIELDS = (
    "id",
    "local_source_id",
    "fid",
    "pick_code",
    "sha1",
    "preid",
    "size",
    "file_name",
    "relative_path",
    "tmdb_id",
    "item_type",
    "season_number",
    "episode_number",
    "media_signature_json",
    "rapid_meta_json",
)


class SharingSourceRegistrationError(ValueError):
    pass


class SharingSourceRegistrationSkipped(SharingSourceRegistrationError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(frozen=True, slots=True)
class PreparedSourceRegistration:
    source_id: int
    source_kind: str
    source_state_hash: str
    raw_payload: Mapping[str, Any]
    raw_readback: Mapping[str, Any]
    provider_payload: Mapping[str, Any]
    provider_readback: Mapping[str, Any]
    local_continuation: Mapping[str, Any]


def freeze_shared_raw_ffprobe(raw: Mapping[str, Any]) -> tuple[dict[str, Any], str, int]:
    """Sanitize and fingerprint one bounded RAW snapshot for deferred upload."""

    sanitized = sanitize_raw_ffprobe(raw)
    _validate_shared_raw_values(sanitized, path="raw", depth=0)
    encoded = canonical_json(sanitized, field="sharing raw ffprobe").encode("utf-8")
    if len(encoded) > MAX_SHARED_RAW_FFPROBE_BYTES:
        raise SharingSourceRegistrationError("RAW 超过 8 MiB 上限，已拒绝登记中心")
    return sanitized, hashlib.sha256(encoded).hexdigest(), len(encoded)


def resolve_shared_raw_ffprobe_reference(
    reference: Mapping[str, Any],
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve a local RAW cache entry only when it matches the frozen reference."""

    sanitized, digest, size_bytes = freeze_shared_raw_ffprobe(raw)
    expected_digest = str(reference.get("sha256") or "").strip().lower()
    expected_size = _positive_integer(reference.get("size_bytes"), "RAW 引用大小")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise SharingSourceRegistrationError("RAW 引用缺少有效哈希")
    if size_bytes != expected_size or digest != expected_digest:
        raise SharingSourceRegistrationError("RAW 缓存在任务执行前发生变化，已拒绝上传")
    return sanitized


def source_registration_state_hash(
    source: Mapping[str, Any],
    files: Sequence[Mapping[str, Any]],
) -> str:
    source_state = {field: _json_value(source.get(field)) for field in _SOURCE_STATE_FIELDS}
    file_states = [
        {field: _json_value(file.get(field)) for field in _FILE_STATE_FIELDS}
        for file in sorted(files, key=_file_sort_key)
    ]
    payload = json.dumps(
        {"source": source_state, "files": file_states},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def prepare_source_registration(
    source: Mapping[str, Any],
    files: Sequence[Mapping[str, Any]],
    *,
    raw_by_sha1: Mapping[str, Mapping[str, Any]],
    formatted_by_sha1: Mapping[str, Any] | None = None,
) -> PreparedSourceRegistration:
    source_id = _positive_integer(source.get("id"), "source id")
    normalized_files = [dict(value) for value in files if isinstance(value, Mapping)]
    if not normalized_files:
        fallback = _file_from_source(source)
        if fallback:
            normalized_files = [fallback]
    if not normalized_files:
        raise SharingSourceRegistrationError("本地共享源没有可登记文件")
    if len(normalized_files) != 1:
        raise SharingSourceRegistrationError("单个本地共享源必须只对应一个可登记文件")

    file = normalized_files[0]
    sha1 = _sha1(file.get("sha1") or source.get("sha1"))
    rapid_meta = _mapping(file.get("rapid_meta_json"))
    source_rapid_meta = _mapping(source.get("rapid_meta_json"))
    pick_code = _first_text(
        file.get("local_sign_holder_pick_code"),
        file.get("pick_code"),
        rapid_meta.get("pick_code"),
        rapid_meta.get("pickcode"),
        rapid_meta.get("pc"),
        source_rapid_meta.get("pick_code"),
        source_rapid_meta.get("pickcode"),
        source_rapid_meta.get("pc"),
    )
    if not pick_code:
        raise SharingSourceRegistrationSkipped(
            "missing_pick_code_virtual_library",
            "缺少 115 pick_code，按虚拟入库/不可秒传记录跳过维护补登",
        )
    if file.get("local_sign_holder") is not True:
        raise SharingSourceRegistrationError("本机无法按 SHA1 反查可签名 pick_code，已拒绝登记中心")
    holder_account_id = _first_text(file.get("local_sign_holder_account_id"))
    if not holder_account_id.isdigit():
        raise SharingSourceRegistrationError("本机 115 持有账号身份无效，已拒绝登记中心")

    raw = raw_by_sha1.get(sha1)
    if not isinstance(raw, Mapping) or not raw:
        raise SharingSourceRegistrationError("RAW/summary_json 缺失，已拒绝登记中心")
    sanitized_raw, raw_digest, raw_size_bytes = freeze_shared_raw_ffprobe(raw)
    formatted = (formatted_by_sha1 or {}).get(sha1)
    summary = build_center_raw_summary(sanitized_raw, formatted, file)
    _validate_shared_raw_values(
        summary,
        path="summary",
        depth=0,
        max_string_bytes=_MAX_SHARED_RAW_SUMMARY_STRING_BYTES,
    )
    if len(canonical_json(summary, field="sharing raw summary").encode("utf-8")) > MAX_SHARED_RAW_SUMMARY_BYTES:
        raise SharingSourceRegistrationError("RAW 展示摘要超过 128 KiB 上限，已拒绝登记中心")
    if not center_raw_summary_usable(summary):
        raise SharingSourceRegistrationError("RAW 存在但无法生成中心展示摘要，已拒绝登记中心")

    item_type = str(source.get("item_type") or file.get("item_type") or "").strip().title()
    declared_kind = str(source.get("source_kind") or "").strip().lower()
    source_kind = "movie" if item_type == "Movie" or declared_kind == "movie" else "episode"
    tmdb_id = _first_text(
        source.get("tmdb_id") if source_kind == "movie" else None,
        source.get("parent_series_tmdb_id"),
        file.get("tmdb_id"),
        source.get("tmdb_id"),
    )
    if not tmdb_id:
        raise SharingSourceRegistrationError("本地共享源缺少 TMDb 身份")
    season_number = _optional_integer(file.get("season_number") or source.get("season_number"))
    episode_number = _optional_integer(file.get("episode_number") or source.get("episode_number"))
    if source_kind == "episode" and (season_number is None or episode_number is None):
        raise SharingSourceRegistrationError("分集共享源缺少季集号")

    file_size = _nonnegative_integer(file.get("size") or source.get("size") or _raw_size(sanitized_raw))
    file_name = _first_text(file.get("file_name"), source.get("file_name"), sha1)
    preid = _first_text(file.get("preid"), rapid_meta.get("preid"), source.get("preid"))
    relative_path = _safe_relative_path(_first_text(file.get("relative_path"), rapid_meta.get("relative_path")))
    final_rapid_meta = {
        "fid": _first_text(file.get("fid"), rapid_meta.get("fid")),
        "pick_code": pick_code,
        "relative_path": relative_path,
        "preid": preid,
    }
    signature = _mapping(file.get("media_signature_json")) or _mapping(source.get("media_signature_json"))
    if not signature:
        signature = {
            key: summary[key]
            for key in ("resolution", "video_codec", "codec", "effect", "fps", "bitrate")
            if summary.get(key) not in (None, "", [], {})
        }
    provider_payload: dict[str, Any] = {
        "tmdb_id": tmdb_id,
        "item_type": "Movie" if source_kind == "movie" else "Episode",
        "title": _first_text(source.get("title"), source.get("root_name"), file_name),
        "release_year": _optional_integer(source.get("release_year")),
        "source_provider": _first_text(source.get("source_provider"), "manual_rapid"),
        "sha1": sha1,
        "preid": preid or None,
        "size": file_size or None,
        "file_name": file_name,
        "quality": _first_text(signature.get("quality"), sanitized_raw.get("_etk", {}).get("quality_source")),
        "has_raw_ffprobe": True,
        "media_signature_json": signature,
        "rapid_meta_json": final_rapid_meta,
    }
    if source_kind == "episode":
        provider_payload.update(season_number=season_number, episode_number=episode_number)
    _validate_shared_raw_values(
        provider_payload,
        path="provider_payload",
        depth=0,
        max_string_bytes=_MAX_SHARED_RAW_SUMMARY_STRING_BYTES,
    )
    provider_payload = normalize_durable_json_object(provider_payload, field="sharing source registration payload")

    center_source_id = _first_text(source.get("center_source_id"))
    provider_readback = _provider_readback(
        source_kind=source_kind,
        center_source_id=center_source_id,
        tmdb_id=tmdb_id,
        season_number=season_number,
        episode_number=episode_number,
        sha1=sha1,
    )
    state_hash = source_registration_state_hash(
        source,
        [dict(value) for value in files if isinstance(value, Mapping)],
    )
    file_record = {
        field: _json_value(file.get(field)) for field in _FILE_STATE_FIELDS if file.get(field) not in (None, "", [], {})
    }
    file_record.update(
        pick_code=pick_code,
        sha1=sha1,
        size=file_size,
        file_name=file_name,
        relative_path=relative_path or None,
        rapid_meta_json=final_rapid_meta,
    )
    return PreparedSourceRegistration(
        source_id=source_id,
        source_kind=source_kind,
        source_state_hash=state_hash,
        raw_payload=normalize_durable_json_object(
            {
                "sha1": sha1,
                "size": file_size or None,
                "raw_reference": {
                    "sha1": sha1,
                    "sha256": raw_digest,
                    "size_bytes": raw_size_bytes,
                },
                "summary_json": summary,
            },
            field="sharing raw upload payload",
        ),
        raw_readback={
            "provider": "shared_center",
            "action": "get_raw_ffprobe",
            "query": {"sha1": sha1},
            "match_key": "sha1",
            "match_value": sha1,
        },
        provider_payload=provider_payload,
        provider_readback=provider_readback,
        local_continuation={
            "type": "confirm_local_source_registration",
            "source_id": source_id,
            "expected_source_state_hash": state_hash,
            "expected_center_source_id": center_source_id,
            "source_kind": source_kind,
            "local_sign_holder": {
                "account_id": holder_account_id,
                "pick_code": pick_code,
                "sha1": sha1,
            },
            "file_record": file_record,
        },
    )


def build_center_raw_summary(
    raw: Mapping[str, Any],
    formatted: Any,
    file: Mapping[str, Any],
) -> dict[str, Any]:
    media_source = _media_source(formatted)
    streams_value = media_source.get("MediaStreams")
    streams: list[Any] = streams_value if isinstance(streams_value, list) else []
    video = next(
        (dict(value) for value in streams if isinstance(value, Mapping) and _stream_type(value) == "video"),
        {},
    )
    audios = [dict(value) for value in streams if isinstance(value, Mapping) and _stream_type(value) == "audio"]
    subtitles = [dict(value) for value in streams if isinstance(value, Mapping) and _stream_type(value) == "subtitle"]
    raw_streams_value = raw.get("streams")
    raw_streams: list[Any] = raw_streams_value if isinstance(raw_streams_value, list) else []
    raw_video = next(
        (
            dict(value)
            for value in raw_streams
            if isinstance(value, Mapping) and str(value.get("codec_type") or "").strip().casefold() == "video"
        ),
        {},
    )
    width = _nonnegative_integer(video.get("Width") or raw_video.get("width"))
    height = _nonnegative_integer(video.get("Height") or raw_video.get("height"))
    codec = _first_text(video.get("Codec"), raw_video.get("codec_name")).lower()
    resolution = _resolution(width, height)
    fps = _rate(
        video.get("AverageFrameRate")
        or video.get("RealFrameRate")
        or raw_video.get("avg_frame_rate")
        or raw_video.get("r_frame_rate")
    )
    raw_format_value = raw.get("format")
    raw_format: Mapping[str, Any] = raw_format_value if isinstance(raw_format_value, Mapping) else {}
    bitrate = _first_text(video.get("BitRate"), media_source.get("Bitrate"), raw_format.get("bit_rate"))
    container = _first_text(media_source.get("Container"), raw_format.get("format_name"))
    size = _nonnegative_integer(file.get("size") or media_source.get("Size") or raw_format.get("size"))
    audio_items = [_track_summary(value) for value in audios]
    subtitle_items = [_track_summary(value) for value in subtitles]
    audio_items = [value for value in audio_items if value]
    subtitle_items = [value for value in subtitle_items if value]
    audio_list = [_first_text(value.get("display"), value.get("title")) for value in audio_items]
    subtitle_list = [_first_text(value.get("display"), value.get("title")) for value in subtitle_items]
    video_display = " · ".join(value for value in (resolution, codec, fps) if value)
    return {
        "resolution": resolution,
        "resolution_display": resolution,
        "width": width,
        "height": height,
        "video_codec": codec,
        "codec": codec,
        "codec_display": codec,
        "effect": _video_effect(video, raw_video),
        "effect_key": _video_effect(video, raw_video),
        "fps": fps,
        "frame_rate": fps,
        "bitrate": bitrate,
        "container": container,
        "video_display": video_display,
        "size": size,
        "size_gb": round(size / 1024 / 1024 / 1024, 2) if size else 0,
        "audio_count": len(audios),
        "subtitle_count": len(subtitles),
        "audio_list": [value for value in audio_list if value][:16],
        "subtitle_list": [value for value in subtitle_list if value][:16],
        "audios": audio_items[:16],
        "subtitles": subtitle_items[:16],
        "formatted_by": "native_mediainfo" if media_source else "raw_fallback",
    }


def center_raw_summary_usable(summary: Mapping[str, Any]) -> bool:
    if not isinstance(summary, Mapping) or not summary:
        return False
    for key in (
        "resolution",
        "width",
        "height",
        "video_codec",
        "codec",
        "video_display",
        "fps",
        "frame_rate",
        "bitrate",
        "audio_list",
        "subtitle_list",
        "audios",
        "subtitles",
    ):
        value = summary.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool) and float(value) <= 0:
            continue
        return True
    return False


def _provider_readback(
    *,
    source_kind: str,
    center_source_id: str,
    tmdb_id: str,
    season_number: int | None,
    episode_number: int | None,
    sha1: str,
) -> dict[str, Any]:
    if center_source_id:
        return {
            "provider": "shared_center",
            "action": "list_sources",
            "query": {"source_id": center_source_id, "limit": 10, "offset": 0},
            "match_fields": {"source_id": center_source_id},
        }
    fields: dict[str, Any] = {"source_kind": source_kind, "tmdb_id": tmdb_id, "sha1": sha1}
    if source_kind == "episode":
        fields.update(season_number=season_number, episode_number=episode_number)
    return {
        "provider": "shared_center",
        "action": "list_sources",
        "query": {
            "source_kind": source_kind,
            "tmdb_id": tmdb_id,
            "item_type": "Movie" if source_kind == "movie" else "Episode",
            "limit": 200,
            "offset": 0,
        },
        "match_fields": fields,
    }


def _file_from_source(source: Mapping[str, Any]) -> dict[str, Any]:
    sha1 = str(source.get("sha1") or "").strip().upper()
    if not sha1:
        return {}
    return {
        "local_source_id": source.get("id"),
        "fid": _mapping(source.get("rapid_meta_json")).get("fid"),
        "pick_code": _mapping(source.get("rapid_meta_json")).get("pick_code"),
        "sha1": sha1,
        "preid": source.get("preid"),
        "size": source.get("size"),
        "file_name": source.get("file_name"),
        "relative_path": _mapping(source.get("rapid_meta_json")).get("relative_path"),
        "tmdb_id": source.get("tmdb_id"),
        "item_type": source.get("item_type"),
        "season_number": source.get("season_number"),
        "episode_number": source.get("episode_number"),
        "media_signature_json": source.get("media_signature_json"),
        "rapid_meta_json": source.get("rapid_meta_json"),
        "local_sign_holder": source.get("local_sign_holder"),
        "local_sign_holder_account_id": source.get("local_sign_holder_account_id"),
        "local_sign_holder_pick_code": source.get("local_sign_holder_pick_code"),
    }


def _validate_shared_raw_values(
    value: Any,
    *,
    path: str,
    depth: int,
    max_string_bytes: int = _MAX_SHARED_RAW_STRING_BYTES,
) -> None:
    if depth > 16:
        raise SharingSourceRegistrationError("RAW 嵌套层级过深，已拒绝登记中心")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _validate_shared_raw_values(
                item,
                path=f"{path}.{key}",
                depth=depth + 1,
                max_string_bytes=max_string_bytes,
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_shared_raw_values(
                item,
                path=f"{path}[{index}]",
                depth=depth + 1,
                max_string_bytes=max_string_bytes,
            )
        return
    if not isinstance(value, str):
        return
    if len(value.encode("utf-8")) > max_string_bytes:
        raise SharingSourceRegistrationError(f"{path} 字符串超过持久化上限")
    if _SENSITIVE_RAW_VALUE_PATTERN.search(value):
        raise SharingSourceRegistrationError(f"{path} 包含 URL、临时路径或凭据形态值，已拒绝登记中心")


def _media_source(value: Any) -> dict[str, Any]:
    current = value
    if isinstance(current, list):
        current = next((item for item in current if isinstance(item, Mapping)), {})
    if not isinstance(current, Mapping):
        return {}
    nested = current.get("MediaSourceInfo")
    if isinstance(nested, Mapping):
        return dict(nested)
    if isinstance(current.get("MediaStreams"), list):
        return dict(current)
    return {}


def _track_summary(stream: Mapping[str, Any]) -> dict[str, str]:
    display = _first_text(stream.get("DisplayTitle"), stream.get("display"))
    title = _first_text(stream.get("Title"), stream.get("title"))
    return {key: value for key, value in {"display": display, "title": title}.items() if value}


def _stream_type(stream: Mapping[str, Any]) -> str:
    return str(stream.get("Type") or stream.get("type") or "").strip().casefold()


def _video_effect(video: Mapping[str, Any], raw_video: Mapping[str, Any]) -> str:
    text = " ".join(
        _first_text(
            video.get("ExtendedVideoType"),
            video.get("ExtendedVideoSubType"),
            video.get("VideoRange"),
            raw_video.get("color_transfer"),
            raw_video.get("side_data_list"),
        ).casefold()
        for _ in (0,)
    )
    if "dolbyvision" in text or "dovi" in text:
        return "Dolby Vision"
    if "hdr10+" in text:
        return "HDR10+"
    if "hdr10" in text:
        return "HDR10"
    if "hdr" in text or "smpte2084" in text:
        return "HDR"
    return ""


def _resolution(width: int, height: int) -> str:
    if width >= 7600:
        return "8K"
    if width >= 3800 or height >= 2100:
        return "4K"
    if width >= 1800 or height >= 1000:
        return "1080p"
    if width >= 1200 or height >= 700:
        return "720p"
    return f"{height}p" if height else ""


def _rate(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if "/" in text:
            numerator, denominator = text.split("/", 1)
            number = float(numerator) / float(denominator)
        else:
            number = float(text)
    except (TypeError, ValueError, ZeroDivisionError):
        return text[:40]
    return f"{number:.3f}".rstrip("0").rstrip(".")


def _raw_size(raw: Mapping[str, Any]) -> int:
    raw_format_value = raw.get("format")
    raw_format: Mapping[str, Any] = raw_format_value if isinstance(raw_format_value, Mapping) else {}
    return _nonnegative_integer(raw_format.get("size"))


def _file_sort_key(value: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(value.get("id") or ""),
        str(value.get("episode_number") or ""),
        str(value.get("sha1") or "").upper(),
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_text(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""


def _safe_relative_path(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    slash_normalized = normalized.replace("\\", "/")
    if slash_normalized.startswith("/") or re.match(r"^[A-Za-z]:/", slash_normalized):
        raise SharingSourceRegistrationError("共享源 relative_path 必须是安全相对路径")
    if any(segment == ".." for segment in slash_normalized.split("/")):
        raise SharingSourceRegistrationError("共享源 relative_path 不得包含父目录跳转")
    return normalized


def _sha1(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if not _SHA1_PATTERN.fullmatch(normalized):
        raise SharingSourceRegistrationError("本地共享源缺少有效 SHA1")
    return normalized


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise SharingSourceRegistrationError(f"{label} 无效")
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise SharingSourceRegistrationError(f"{label} 无效") from exc
    if normalized < 1:
        raise SharingSourceRegistrationError(f"{label} 无效")
    return normalized


def _optional_integer(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _nonnegative_integer(value: Any) -> int:
    if value in (None, "") or isinstance(value, bool):
        return 0
    try:
        return max(0, int(float(str(value).strip())))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "PreparedSourceRegistration",
    "SharingSourceRegistrationError",
    "SharingSourceRegistrationSkipped",
    "build_center_raw_summary",
    "center_raw_summary_usable",
    "prepare_source_registration",
    "source_registration_state_hash",
]
