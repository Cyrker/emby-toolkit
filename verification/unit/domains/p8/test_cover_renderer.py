from __future__ import annotations

import base64
import hashlib
import json
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image, ImageChops

from etk.domains.collection.local_gateway import P8LocalCollectionGateway
from etk.domains.collection.renderer import (
    DeterministicCoverRenderer,
    PillowCoverPreviewRenderer,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURES = REPO_ROOT / "verification" / "fixtures" / "images" / "p8"
FONT = REPO_ROOT / "fonts" / "en_font.ttf"
FONT_SHA256 = "3b4b214b1a64d406a6b158ba06e6295cdfe988bdc177dacbdc766c0504453a5c"
MAX_CHANNEL_DELTA = 0
MAX_CHANGED_PIXEL_RATIO = 0.0


def _payload() -> dict[str, object]:
    source = (FIXTURES / "cover-preview-input.png").read_bytes()
    return {
        "base_image": "data:image/png;base64," + base64.b64encode(source).decode("ascii"),
        "item_count": 27,
        "badge_style": "badge",
        "badge_size_ratio": 0.12,
    }


def test_badge_preview_is_deterministic_and_matches_fixed_image_threshold() -> None:
    renderer = PillowCoverPreviewRenderer(FONT)

    first = renderer.render_png(_payload())
    second = renderer.render_png(_payload())
    expected = Image.open(FIXTURES / "cover-preview-badge.png").convert("RGBA")
    actual = Image.open(BytesIO(first)).convert("RGBA")
    difference = ImageChops.difference(expected, actual)
    difference_bytes = difference.tobytes()
    max_delta = max(difference_bytes)
    changed_pixels = sum(
        1 for offset in range(0, len(difference_bytes), 4) if any(difference_bytes[offset : offset + 4])
    )
    changed_ratio = changed_pixels / (difference.width * difference.height)

    assert renderer.font_digest == FONT_SHA256
    assert first == second
    assert actual.size == expected.size == (320, 180)
    assert actual.mode == expected.mode == "RGBA"
    assert max_delta <= MAX_CHANNEL_DELTA
    assert changed_ratio <= MAX_CHANGED_PIXEL_RATIO


def test_local_gateway_encodes_renderer_bytes_as_png_data_url() -> None:
    gateway = P8LocalCollectionGateway(renderer=PillowCoverPreviewRenderer(FONT))

    encoded = gateway.cover_preview(_payload())

    assert encoded.startswith("data:image/png;base64,")
    assert base64.b64decode(encoded.split(",", 1)[1]) == (FIXTURES / "cover-preview-badge.png").read_bytes()


def _full_renderer() -> DeterministicCoverRenderer:
    return DeterministicCoverRenderer(
        zh_font_path=REPO_ROOT / "fonts" / "zh_font.ttf",
        en_font_path=REPO_ROOT / "fonts" / "en_font.ttf",
    )


def _sources() -> tuple[bytes, ...]:
    return tuple((FIXTURES / f"cover-source-{index}.png").read_bytes() for index in range(1, 10))


def _assert_rgba_exact(actual_png: bytes, expected_name: str) -> None:
    expected = Image.open(FIXTURES / expected_name).convert("RGBA")
    actual = Image.open(BytesIO(actual_png)).convert("RGBA")
    difference = ImageChops.difference(expected, actual)
    assert actual.size == expected.size
    assert actual.mode == expected.mode == "RGBA"
    assert difference.getbbox() is None


@pytest.mark.parametrize(
    ("case_id", "style", "source_count", "blur", "config", "expected_name"),
    [
        (
            "single_1",
            "single_1",
            1,
            False,
            {"show_item_count": True, "badge_style": "badge", "badge_size_ratio": 0.09},
            "cover-single-1.png",
        ),
        ("single_2", "single_2", 1, False, {}, "cover-single-2.png"),
        ("multi_1_gradient", "multi_1", 9, False, {}, "cover-multi-1-gradient.png"),
        ("multi_1_blur", "multi_1", 9, True, {}, "cover-multi-1-blur.png"),
    ],
)
def test_full_cover_styles_match_fixed_rgba_fixtures(
    case_id: str,
    style: str,
    source_count: int,
    blur: bool,
    config: dict[str, object],
    expected_name: str,
) -> None:
    manifest = json.loads((FIXTURES / "cover-render-manifest.json").read_text(encoding="utf-8"))
    renderer = _full_renderer()
    result = renderer.render(
        style=style,
        sources=_sources()[:source_count],
        title_zh="示例合集",
        title_en="FIXTURE COLLECTION",
        item_count=27,
        blur=blur,
        seed=20260827,
        config=config,
    )

    _assert_rgba_exact(result.png, expected_name)
    assert (result.width, result.height, result.mode) == (1920, 1080, "RGBA")
    assert result.cache_key == manifest["cases"][case_id]["cache_key"]
    assert hashlib.sha256(result.png).hexdigest() == manifest["cases"][case_id]["png_sha256"]
    assert renderer.font_digest == manifest["font_digest"]


@pytest.mark.parametrize(
    ("case_id", "status", "release_date", "expected_name"),
    [
        ("missing_wanted", "WANTED", None, "cover-missing-wanted.png"),
        ("missing_release", "PENDING_RELEASE", "2026-12-31", "cover-missing-release.png"),
    ],
)
def test_missing_poster_styles_match_fixed_rgba_fixtures(
    case_id: str,
    status: str,
    release_date: str | None,
    expected_name: str,
) -> None:
    manifest = json.loads((FIXTURES / "cover-render-manifest.json").read_text(encoding="utf-8"))
    renderer = _full_renderer()
    result = renderer.render_missing_poster(
        status=status,
        source=_sources()[0],
        release_date=release_date,
    )

    assert result is not None
    _assert_rgba_exact(result.png, expected_name)
    assert (result.width, result.height, result.mode) == (500, 750, "RGBA")
    assert result.cache_key == manifest["cases"][case_id]["cache_key"]
    assert hashlib.sha256(result.png).hexdigest() == manifest["cases"][case_id]["png_sha256"]
    assert renderer.render_missing_poster(status="NONE", source=None) is None


def test_cache_identity_includes_source_order_and_reuses_immutable_result() -> None:
    renderer = _full_renderer()
    sources = _sources()[:3]
    parameters = {
        "style": "multi_1",
        "title_zh": "缓存",
        "title_en": "CACHE",
        "item_count": 3,
        "blur": False,
        "seed": 7,
        "config": {},
    }
    first = renderer.render(sources=sources, **parameters)
    second = renderer.render(sources=sources, **parameters)
    reversed_result = renderer.render(sources=tuple(reversed(sources)), **parameters)
    third = renderer.render(sources=sources, **parameters)

    assert first.cache_key == second.cache_key == third.cache_key
    assert first.png is second.png and second.png is third.png
    assert reversed_result.cache_key != first.cache_key
    assert reversed_result.png != first.png


def test_explicit_style_tuning_changes_single_and_multi_rendering() -> None:
    renderer = _full_renderer()
    sources = _sources()
    common = {
        "title_zh": "配置生效",
        "title_en": "CONFIGURED",
        "item_count": None,
        "seed": 17,
    }

    single_default = renderer.render(style="single_1", sources=sources[:1], blur=False, config={}, **common)
    single_configured = renderer.render(
        style="single_1",
        sources=sources[:1],
        blur=False,
        config={"zh_font_size": 0.7, "en_font_size": 1.3, "blur_size": 4, "color_ratio": 0.2},
        **common,
    )
    multi_default = renderer.render(style="multi_1", sources=sources, blur=True, config={}, **common)
    multi_configured = renderer.render(
        style="multi_1",
        sources=sources,
        blur=True,
        config={
            "zh_font_size_multi_1": 0.7,
            "en_font_size_multi_1": 1.3,
            "blur_size_multi_1": 4,
            "color_ratio_multi_1": 0.2,
        },
        **common,
    )

    assert single_configured.png != single_default.png
    assert multi_configured.png != multi_default.png
