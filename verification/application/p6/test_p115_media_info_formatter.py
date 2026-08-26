from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from etk.domains.p115.media_info_formatter import (
    P115MediaInfoFormatError,
    P115MediaInfoPolicySnapshot,
    format_raw_ffprobe_mediainfo,
    normalize_legacy_formatted_mediainfo,
    normalize_legacy_mediainfo_response,
    sanitize_raw_ffprobe,
)


def _policy(
    *,
    audio_languages: list[str] | None = None,
    subtitle_languages: list[str] | None = None,
    commentary: bool = False,
) -> P115MediaInfoPolicySnapshot:
    return P115MediaInfoPolicySnapshot.create(
        language_mapping=[
            {
                "label": "国语",
                "value": "zh",
                "aliases": ["chi", "zho", "zh", "chs", "zh-cn", "zh-hans", "cmn", "国语", "中文"],
            },
            {
                "label": "粤语",
                "value": "cn",
                "aliases": ["yue", "cht", "zh-hk", "zh-tw", "zh-hant", "粤语", "繁体"],
            },
            {"label": "英语", "value": "en", "aliases": ["eng", "en", "english", "英语", "英文"]},
            {"label": "日语", "value": "ja", "aliases": ["jpn", "ja", "japanese", "日语", "日文"]},
            {"label": "韩语", "value": "ko", "aliases": ["kor", "ko", "korean", "韩语", "韩文"]},
        ],
        default_stream_config={
            "audio_lang_priority": audio_languages or [],
            "subtitle_lang_priority": subtitle_languages or [],
            "audio_priority_order": ["param", "feature"],
            "audio_features": ["公映", "上译", "粤语"],
            "audio_param_priority": ["atmos", "truehd", "dts", "aac", "5_1", "2_0"],
            "sub_priority": ["effect", "chs", "cht", "chs_eng", "cht_eng"],
            "audio_commentary_priority_enabled": commentary,
        },
        language_mapping_revision="language-fixture-v1",
        default_stream_config_revision="streams-fixture-v1",
    )


def _core_raw() -> dict[str, object]:
    return {
        "format": {
            "filename": "https://fixture.invalid/temporary-signed-url",
            "format_name": "matroska,webm",
            "duration": "120.5",
            "size": "1000",
            "bit_rate": "8000000",
        },
        "streams": [
            {
                "index": 0,
                "codec_type": "video",
                "codec_name": "mjpeg",
                "width": 1000,
                "height": 1000,
                "disposition": {"attached_pic": 1},
            },
            {
                "index": 1,
                "codec_type": "video",
                "codec_name": "hevc",
                "profile": "Main 10",
                "width": 3840,
                "height": 2160,
                "pix_fmt": "yuv420p10le",
                "avg_frame_rate": "24000/1001",
                "color_space": "bt2020nc",
                "color_transfer": "smpte2084",
                "color_primaries": "bt2020",
                "side_data_list": [
                    {
                        "side_data_type": "DOVI configuration record",
                        "dv_profile": 8,
                        "dv_bl_signal_compatibility_id": 1,
                    }
                ],
                "disposition": {"default": 1},
            },
            {
                "index": 2,
                "codec_type": "audio",
                "codec_name": "truehd",
                "profile": "Atmos",
                "channels": 8,
                "sample_rate": "48000",
                "bit_rate": "4000000",
                "tags": {"language": "eng", "title": "English Atmos"},
                "disposition": {"default": 1},
            },
            {
                "index": 3,
                "codec_type": "subtitle",
                "codec_name": "subrip",
                "tags": {"language": "zh-Hans", "title": "简体特效"},
                "disposition": {"forced": 1},
            },
        ],
        "chapters": [
            {"start_time": "0", "tags": {"title": "Opening"}},
            {"start_time": "60.25", "tags": {}},
        ],
        "_etk": {"original_language": "en", "sha1": "A" * 40},
    }


def test_formats_canonical_envelope_with_streams_chapters_ticks_and_dolby_vision() -> None:
    raw = _core_raw()
    original = deepcopy(raw)

    formatted = format_raw_ffprobe_mediainfo(
        raw,
        file_name="Fixture.Movie.2160p.mkv",
        file_size=2000,
        sha1="A" * 40,
        policy=_policy(),
    )

    assert raw == original
    assert len(formatted) == 1
    envelope = formatted[0]
    assert envelope["Chapters"] == [
        {"Name": "Opening", "MarkerType": "Chapter", "ChapterIndex": 0, "StartPositionTicks": 0},
        {"Name": "章节 2", "MarkerType": "Chapter", "ChapterIndex": 1, "StartPositionTicks": 602_500_000},
    ]
    source = envelope["MediaSourceInfo"]
    assert source["Chapters"] == []
    assert source["IsRemote"] is True
    assert source["Protocol"] == "File"
    assert source["Container"] == "mkv"
    assert source["Size"] == 2000
    assert source["Bitrate"] == 8_000_000
    assert source["RunTimeTicks"] == 1_205_000_000

    streams = source["MediaStreams"]
    assert [stream["Type"] for stream in streams] == ["Video", "Audio", "Subtitle"]
    video = streams[0]
    assert video["Index"] == 1
    assert video["BitDepth"] == 10
    assert video["RealFrameRate"] == pytest.approx(24000 / 1001)
    assert video["VideoRange"] == "DolbyVision HDR10"
    assert video["ExtendedVideoType"] == "DolbyVision"
    assert video["ExtendedVideoSubType"] == "DoviProfile81"
    assert video["DisplayTitle"] == "4K Dolby Vision HEVC"

    audio = streams[1]
    assert audio["Language"] == "eng"
    assert audio["DisplayLanguage"] == "英语"
    assert audio["ChannelLayout"] == "7.1"
    assert audio["Profile"] == "Atmos"
    assert audio["IsDefault"] is True

    subtitle = streams[2]
    assert subtitle["Codec"] == "SUBRIP"
    assert subtitle["Language"] == "chi"
    assert subtitle["DisplayLanguage"] == "中文简体"
    assert subtitle["Title"] == "中文简体特效"
    assert subtitle["IsTextSubtitleStream"] is True
    assert subtitle["IsDefault"] is True
    assert subtitle["DisplayTitle"] == "中文简体 (SUBRIP) (默认)"


@pytest.mark.parametrize(
    ("stream", "video_range", "display_title"),
    [
        (
            {"color_transfer": "smpte2084", "color_primaries": "bt2020"},
            "HDR10",
            "1080p HDR10 HEVC",
        ),
        (
            {"side_data_list": [{"side_data_type": "HDR10+ SMPTE2094-40"}]},
            "HDR10+",
            "1080p HDR10+ HEVC",
        ),
        ({"color_primaries": "bt2020"}, "HDR", "1080p HDR HEVC"),
    ],
)
def test_formats_hdr_variants(stream: dict[str, object], video_range: str, display_title: str) -> None:
    raw_stream = {
        "index": 0,
        "codec_type": "video",
        "codec_name": "hevc",
        "width": 1920,
        "height": 1080,
        **stream,
    }
    formatted = format_raw_ffprobe_mediainfo(
        {"format": {"duration": 1}, "streams": [raw_stream]},
        file_name="fixture.mkv",
        policy=_policy(),
    )
    video = formatted[0]["MediaSourceInfo"]["MediaStreams"][0]
    assert video["VideoRange"] == video_range
    assert video["DisplayTitle"] == display_title


def test_immutable_policy_snapshot_drives_audio_and_subtitle_defaults() -> None:
    language_mapping = [
        {"label": "英语", "value": "en", "aliases": ["eng", "en"]},
        {"label": "日语", "value": "ja", "aliases": ["jpn", "ja"]},
        {"label": "国语", "value": "zh", "aliases": ["chi", "zh", "chs"]},
        {"label": "粤语", "value": "cn", "aliases": ["yue", "cht", "zh-hant"]},
    ]
    config = {
        "audio_lang_priority": ["jpn", "eng"],
        "subtitle_lang_priority": ["cht", "chs"],
        "audio_priority_order": ["param", "feature"],
        "audio_features": [],
        "audio_param_priority": ["dts", "aac"],
        "sub_priority": ["effect", "chs", "cht"],
    }
    policy = P115MediaInfoPolicySnapshot.create(
        language_mapping=language_mapping,
        default_stream_config=config,
        language_mapping_revision="language-revision",
        default_stream_config_revision="stream-revision",
    )
    language_mapping[0]["aliases"].append("mutated")
    config["audio_lang_priority"].reverse()

    assert policy.language_mapping_revision == "language-revision"
    assert policy.default_stream_config_revision == "stream-revision"
    assert policy.language_mapping[0].aliases == ("eng", "en")
    assert policy.default_streams.audio_language_priority == ("jpn", "eng")
    with pytest.raises(FrozenInstanceError):
        policy.language_mapping_revision = "changed"  # type: ignore[misc]

    formatted = format_raw_ffprobe_mediainfo(
        {
            "format": {"duration": 10},
            "streams": [
                {
                    "index": 0,
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 1920,
                    "height": 1080,
                },
                {
                    "index": 1,
                    "codec_type": "audio",
                    "codec_name": "dts",
                    "channels": 6,
                    "bit_rate": 2_000_000,
                    "tags": {"language": "eng", "title": "English DTS"},
                    "disposition": {"default": 1},
                },
                {
                    "index": 2,
                    "codec_type": "audio",
                    "codec_name": "aac",
                    "channels": 2,
                    "bit_rate": 128_000,
                    "tags": {"language": "jpn", "title": "Japanese AAC"},
                },
                {
                    "index": 3,
                    "codec_type": "subtitle",
                    "codec_name": "ass",
                    "tags": {"language": "zh-Hans", "title": "简体"},
                    "disposition": {"default": 1, "forced": 1},
                },
                {
                    "index": 4,
                    "codec_type": "subtitle",
                    "codec_name": "ass",
                    "tags": {"language": "zh-Hant", "title": "繁体"},
                },
            ],
        },
        file_name="fixture.mkv",
        policy=policy,
    )
    streams = formatted[0]["MediaSourceInfo"]["MediaStreams"]
    audio = [stream for stream in streams if stream["Type"] == "Audio"]
    subtitles = [stream for stream in streams if stream["Type"] == "Subtitle"]
    assert [stream["Language"] for stream in audio if stream["IsDefault"]] == ["jpn"]
    assert [stream["DisplayLanguage"] for stream in subtitles if stream["IsDefault"]] == ["中文繁体"]
    assert audio[0]["IsForced"] is False
    assert subtitles[0]["IsForced"] is False


def test_commentary_audio_is_not_selected_by_codec_or_original_default_unless_enabled() -> None:
    raw = {
        "format": {"duration": 10},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
            {
                "index": 1,
                "codec_type": "audio",
                "codec_name": "truehd",
                "profile": "Atmos",
                "channels": 8,
                "bit_rate": 5_000_000,
                "tags": {"language": "eng", "title": "Director Commentary Atmos"},
                "disposition": {"default": 1},
            },
            {
                "index": 2,
                "codec_type": "audio",
                "codec_name": "aac",
                "channels": 2,
                "bit_rate": 128_000,
                "tags": {"language": "eng", "title": "English"},
            },
        ],
    }
    formatted = format_raw_ffprobe_mediainfo(raw, file_name="fixture.mkv", policy=_policy())
    audio = [stream for stream in formatted[0]["MediaSourceInfo"]["MediaStreams"] if stream["Type"] == "Audio"]
    assert [stream["Index"] for stream in audio if stream["IsDefault"]] == [2]

    commentary_enabled = format_raw_ffprobe_mediainfo(
        raw,
        file_name="fixture.mkv",
        policy=_policy(commentary=True),
    )
    enabled_audio = [
        stream for stream in commentary_enabled[0]["MediaSourceInfo"]["MediaStreams"] if stream["Type"] == "Audio"
    ]
    assert [stream["Index"] for stream in enabled_audio if stream["IsDefault"]] == [1]


def test_sanitizer_removes_temporary_url_and_filters_etk_without_mutating_input() -> None:
    raw = {
        "format": {"filename": "https://fixture.invalid/signed", "duration": "1"},
        "streams": [],
        "_etk": {
            "tmdb_id": "1001",
            "type": "movie",
            "original_language": "ja",
            "sha1": "A" * 40,
            "preid": "fixture-preid",
            "season_number": 1,
            "episode_number": 2,
            "quality_source": "BluRay",
            "account_id": "must-not-survive",
            "direct_url": "https://fixture.invalid/private",
        },
    }
    original = deepcopy(raw)

    sanitized = sanitize_raw_ffprobe(raw)

    assert raw == original
    assert sanitized["format"] == {"duration": "1"}
    assert sanitized["_etk"] == {
        "tmdb_id": "1001",
        "type": "movie",
        "original_language": "ja",
        "sha1": "A" * 40,
        "preid": "fixture-preid",
        "season_number": 1,
        "episode_number": 2,
        "quality_source": "BluRay",
    }


@pytest.mark.parametrize(
    "legacy",
    [
        [{"Chapters": [{"Name": "Top"}], "MediaSourceInfo": {"MediaStreams": [], "Chapters": []}}],
        {"Chapters": [{"Name": "Top"}], "MediaSourceInfo": [{"MediaStreams": []}]},
        {"Chapters": [{"Name": "Top"}], "MediaSources": [{"MediaStreams": []}]},
        {"Chapters": [{"Name": "Top"}], "MediaStreams": []},
        {"MediaSourceInfo": {"MediaStreams": [], "Chapters": [{"Name": "Nested"}]}},
    ],
)
def test_normalizes_historical_formatted_shapes(legacy: object) -> None:
    original = deepcopy(legacy)
    normalized = normalize_legacy_formatted_mediainfo(legacy)

    assert legacy == original
    assert len(normalized) == 1
    assert normalized[0]["MediaSourceInfo"]["MediaStreams"] == []
    assert normalized[0]["MediaSourceInfo"]["Chapters"] == []
    expected_chapter = "Nested" if isinstance(legacy, dict) and "Chapters" not in legacy else "Top"
    assert normalized[0]["Chapters"] == [{"Name": expected_chapter}]
    assert normalize_legacy_mediainfo_response(json.dumps(legacy, ensure_ascii=False)) == normalized[0]
    assert normalize_legacy_mediainfo_response(json.dumps(legacy, ensure_ascii=False).encode()) == normalized[0]


@pytest.mark.parametrize(
    "invalid",
    [None, [], "not-json", {}, {"MediaSourceInfo": {}}, {"MediaSources": []}, {"MediaStreams": {}}],
)
def test_normalization_rejects_invalid_historical_shapes(invalid: object) -> None:
    with pytest.raises(P115MediaInfoFormatError):
        normalize_legacy_mediainfo_response(invalid)


@pytest.mark.parametrize(
    "raw",
    [
        {},
        {"format": [], "streams": []},
        {"format": {}, "streams": {}},
        {"format": {}, "streams": ["not-an-object"]},
        {"format": {}, "streams": [], "chapters": {}},
    ],
)
def test_formatter_rejects_invalid_raw_shapes(raw: dict[str, object]) -> None:
    with pytest.raises(P115MediaInfoFormatError):
        format_raw_ffprobe_mediainfo(raw, file_name="fixture.mkv", policy=_policy())
