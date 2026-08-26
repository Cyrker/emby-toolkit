from __future__ import annotations

from datetime import date

from etk.domains.subscription.resubscribe import (
    ResubscribeItemKey,
    ResubscribePolicySnapshot,
    _provider_payload,
)


def _policy() -> ResubscribePolicySnapshot:
    return ResubscribePolicySnapshot(
        quota_date=date(2026, 8, 26),
        daily_cap=2,
        delay_seconds=2,
        language_keywords={
            "chi": ("国语", "Mandarin"),
            "sub_chi": ("简中", "CHS"),
            "sub_yue": ("繁中", "CHT"),
        },
        release_group_mapping={"朋友": ("FRDS", "Yumi")},
    )


def test_moviepilot_payload_preserves_exclusion_and_custom_filter_golden_contract() -> None:
    payload = _provider_payload(
        ResubscribeItemKey("100", "Movie"),
        {
            "custom_resubscribe_enabled": True,
            "resubscribe_resolution_enabled": True,
            "resubscribe_resolution_threshold": 3840,
            "resubscribe_quality_enabled": True,
            "resubscribe_quality_include": ["WEB-DL", "BluRay"],
            "resubscribe_codec_enabled": True,
            "resubscribe_codec_include": ["hevc"],
            "resubscribe_effect_enabled": True,
            "resubscribe_effect_include": ["hdr", "dovi_p8"],
            "resubscribe_audio_enabled": True,
            "resubscribe_audio_missing_languages": ["chi"],
            "resubscribe_subtitle_enabled": True,
            "resubscribe_subtitle_missing_languages": ["zh"],
        },
        {"item_name": "Fixture Movie", "release_group_raw": ["朋友"]},
        _policy(),
    )

    assert payload == {
        "name": "Fixture Movie",
        "tmdbid": 100,
        "type": "电影",
        "best_version_full": 1,
        "exclude": "(?:FRDS|Yumi)",
        "resolution": "4k",
        "quality": "WEB-DL,BluRay",
        "effect": "dovi",
        "include": (
            "(?=.*(hevc|h265|x265))(?=.*(dovi|dolby|dv))(?=.*hdr)(?=.*(Mandarin|国语))(?=.*(CHS|CHT|简中|繁中))"
        ),
    }


def test_non_custom_rule_ignores_custom_filters_but_season_mode_remains_authoritative() -> None:
    rule = {
        "custom_resubscribe_enabled": False,
        "resubscribe_resolution_enabled": True,
        "resubscribe_resolution_threshold": 3840,
        "resubscribe_entire_season": False,
    }
    partial = _provider_payload(
        ResubscribeItemKey("200", "Season", 3),
        rule,
        {"item_name": "Fixture Series - 第 3 季"},
        _policy(),
    )
    complete = _provider_payload(
        ResubscribeItemKey("200", "Season", 3),
        {**rule, "custom_resubscribe_enabled": True, "resubscribe_entire_season": True},
        {"item_name": "Fixture Series - 第 3 季"},
        _policy(),
    )

    assert partial == {
        "name": "Fixture Series",
        "tmdbid": 200,
        "type": "电视剧",
        "best_version_full": 1,
        "season": 3,
    }
    assert "resolution" not in partial
    assert complete["best_version"] == 1
    assert complete["resolution"] == "4k"
