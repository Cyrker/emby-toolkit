from __future__ import annotations

import base64
import hashlib
import json
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any, Protocol

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps, ImageStat

FULL_COVER_SIZE = (1920, 1080)
MISSING_POSTER_SIZE = (500, 750)
RENDERER_REVISION = "p8-cover-v1"
_MISSING_STATUS = {
    "WANTED": ("待订阅", "QUEUED", "#2196F3"),
    "SUBSCRIBED": ("已订阅", "ACTIVE", "#FF9800"),
    "PENDING_RELEASE": ("未上映", "COMING SOON", "#9C27B0"),
    "PAUSED": ("暂无资源", "NO SOURCES", "#9E9E9E"),
    "IGNORED": ("已忽略", "IGNORED", "#F44336"),
}


class CoverPreviewRenderer(Protocol):
    @property
    def font_digest(self) -> str: ...

    def render_png(self, payload: Mapping[str, Any]) -> bytes: ...


class PillowCoverPreviewRenderer:
    """Render the existing badge preview from immutable in-memory inputs."""

    def __init__(self, font_path: Path) -> None:
        resolved = font_path.resolve()
        if not resolved.is_file():
            raise FileNotFoundError(resolved)
        self._font_bytes = resolved.read_bytes()
        self._font_digest = hashlib.sha256(self._font_bytes).hexdigest()

    @property
    def font_digest(self) -> str:
        return self._font_digest

    def render_png(self, payload: Mapping[str, Any]) -> bytes:
        encoded = str(payload["base_image"])
        if "," in encoded:
            encoded = encoded.split(",", 1)[1]
        image = Image.open(BytesIO(base64.b64decode(encoded))).convert("RGBA")
        rendered = _draw_badge(
            image=image,
            item_count=payload.get("item_count", 99),
            font_bytes=self._font_bytes,
            style=payload.get("badge_style", "badge"),
            size_ratio=payload.get("badge_size_ratio", 0.12),
        )
        output = BytesIO()
        rendered.save(output, format="PNG")
        return output.getvalue()


@dataclass(frozen=True, slots=True)
class RenderedCover:
    cache_key: str
    png: bytes
    width: int
    height: int
    mode: str = "RGBA"


class DeterministicCoverRenderer:
    """Pure in-memory renderer for the three frozen P8 cover styles and placeholders."""

    def __init__(
        self,
        *,
        zh_font_path: Path,
        en_font_path: Path,
    ) -> None:
        self._zh_font = _read_font(zh_font_path)
        self._en_font = _read_font(en_font_path)
        self._font_digest = hashlib.sha256(self._zh_font + self._en_font).hexdigest()
        self._cache: dict[str, bytes] = {}
        self._cache_lock = Lock()

    @property
    def font_digest(self) -> str:
        return self._font_digest

    def render(
        self,
        *,
        style: str,
        sources: Sequence[bytes],
        title_zh: str,
        title_en: str = "",
        item_count: int | str | None = None,
        blur: bool = False,
        seed: int = 0,
        config: Mapping[str, Any] | None = None,
    ) -> RenderedCover:
        normalized_style = str(style or "").strip()
        if normalized_style not in {"single_1", "single_2", "multi_1"}:
            raise ValueError(f"unsupported cover style: {normalized_style}")
        if not sources:
            raise ValueError("at least one source image is required")
        normalized_config = dict(config or {})
        key = self.cache_key(
            style=normalized_style,
            sources=sources,
            title_zh=title_zh,
            title_en=title_en,
            item_count=item_count,
            blur=blur,
            seed=seed,
            config=normalized_config,
        )
        with self._cache_lock:
            cached = self._cache.get(key)
        if cached is None:
            images = tuple(_decode_image(value) for value in sources)
            if normalized_style == "single_1":
                image = self._single_1(
                    images[0],
                    title_zh,
                    title_en,
                    seed=seed,
                    config=normalized_config,
                )
            elif normalized_style == "single_2":
                image = self._single_2(
                    images[0],
                    title_zh,
                    title_en,
                    seed=seed,
                    config=normalized_config,
                )
            else:
                image = self._multi_1(
                    images,
                    title_zh,
                    title_en,
                    blur=blur,
                    seed=seed,
                    config=normalized_config,
                )
            if normalized_config.get("show_item_count") and item_count is not None:
                image = _draw_badge(
                    image=image,
                    item_count=item_count,
                    font_bytes=self._zh_font,
                    style=normalized_config.get("badge_style", "badge"),
                    size_ratio=normalized_config.get("badge_size_ratio", 0.12),
                    base_color=_dominant_color(images[0]),
                )
            cached = _png_bytes(image)
            with self._cache_lock:
                cached = self._cache.setdefault(key, cached)
        return RenderedCover(key, cached, FULL_COVER_SIZE[0], FULL_COVER_SIZE[1])

    def render_missing_poster(
        self,
        *,
        status: str,
        source: bytes | None,
        release_date: str | None = None,
    ) -> RenderedCover | None:
        normalized_status = str(status or "").strip().upper()
        if normalized_status == "NONE":
            return None
        main_text, sub_text, accent = _MISSING_STATUS.get(
            normalized_status,
            _MISSING_STATUS["WANTED"],
        )
        if normalized_status == "PENDING_RELEASE" and release_date:
            main_text = str(release_date)
        key_payload = {
            "renderer_revision": RENDERER_REVISION,
            "kind": "missing-poster",
            "status": normalized_status,
            "release_date": str(release_date or ""),
            "source_sha256": hashlib.sha256(source or b"").hexdigest(),
            "font_digest": self._font_digest,
        }
        key = hashlib.sha256(_canonical_json(key_payload)).hexdigest()
        with self._cache_lock:
            cached = self._cache.get(key)
        if cached is None:
            image = (
                ImageOps.fit(_decode_image(source), MISSING_POSTER_SIZE, method=Image.Resampling.LANCZOS)
                if source
                else Image.new("RGBA", MISSING_POSTER_SIZE, "#1A1A1A")
            )
            image = ImageEnhance.Color(image.convert("RGBA")).enhance(0.6)
            image = ImageEnhance.Brightness(image).enhance(0.4)
            draw = ImageDraw.Draw(image)
            draw.rectangle(
                [0, 0, image.width - 1, image.height - 1],
                outline=accent,
                width=15,
            )
            main_font = _font(self._zh_font, 52)
            sub_font = _font(self._en_font, 22)
            main_box = draw.textbbox((0, 0), main_text, font=main_font)
            sub_box = draw.textbbox((0, 0), sub_text, font=sub_font)
            main_size = (main_box[2] - main_box[0], main_box[3] - main_box[1])
            sub_size = (sub_box[2] - sub_box[0], sub_box[3] - sub_box[1])
            capsule_width = max(300, max(main_size[0], sub_size[0]) + 120)
            capsule_height = main_size[1] + sub_size[1] + 50
            capsule_x = (image.width - capsule_width) // 2
            capsule_y = image.height - capsule_height - 100
            draw.rounded_rectangle(
                [capsule_x, capsule_y, capsule_x + capsule_width, capsule_y + capsule_height],
                radius=20,
                fill=(0, 0, 0, 200),
            )
            draw.text(
                ((image.width - main_size[0]) // 2, capsule_y + 25),
                main_text,
                font=main_font,
                fill="#FFFFFF",
            )
            draw.text(
                (
                    (image.width - sub_size[0]) // 2,
                    capsule_y + 25 + main_size[1] + 8,
                ),
                sub_text,
                font=sub_font,
                fill=accent,
            )
            cached = _png_bytes(image)
            with self._cache_lock:
                cached = self._cache.setdefault(key, cached)
        return RenderedCover(key, cached, MISSING_POSTER_SIZE[0], MISSING_POSTER_SIZE[1])

    def cache_key(
        self,
        *,
        style: str,
        sources: Sequence[bytes],
        title_zh: str,
        title_en: str,
        item_count: int | str | None,
        blur: bool,
        seed: int,
        config: Mapping[str, Any],
    ) -> str:
        payload = {
            "renderer_revision": RENDERER_REVISION,
            "style": style,
            "source_sha256": [hashlib.sha256(value).hexdigest() for value in sources],
            "title_zh": str(title_zh),
            "title_en": str(title_en),
            "item_count": item_count,
            "blur": bool(blur),
            "seed": int(seed),
            "config": dict(config),
            "font_digest": self._font_digest,
        }
        return hashlib.sha256(_canonical_json(payload)).hexdigest()

    def _single_1(
        self,
        source: Image.Image,
        title_zh: str,
        title_en: str,
        *,
        seed: int,
        config: Mapping[str, Any],
    ) -> Image.Image:
        rng = random.Random(seed)
        base = _tinted_background(
            source,
            blur_radius=_blur_size(config, "blur_size", 48),
            color_ratio=_color_ratio(config, "color_ratio", 0.72),
        )
        card = _rounded(ImageOps.fit(source, (760, 760), method=Image.Resampling.LANCZOS), 90)
        center = (1430, 540)
        for angle, opacity, offset in ((34, 110, (-70, 20)), (17, 155, (-35, 10))):
            tint = Image.new("RGBA", card.size, (*_shift_color(_dominant_color(source), rng), opacity))
            layer = Image.blend(card, tint, 0.45)
            _paste_rotated(base, layer, center=(center[0] + offset[0], center[1] + offset[1]), angle=angle)
        _paste_rotated(base, card, center=center, angle=0)
        self._draw_titles(
            base,
            title_zh,
            title_en,
            accent=_dominant_color(source),
            font_scale=_font_scale(config, "zh_font_size", "en_font_size"),
        )
        return base

    def _single_2(
        self,
        source: Image.Image,
        title_zh: str,
        title_en: str,
        *,
        seed: int,
        config: Mapping[str, Any],
    ) -> Image.Image:
        rng = random.Random(seed)
        dominant = _dominant_color(source)
        if "blur_size" in config or "color_ratio" in config:
            base = _tinted_background(
                source,
                blur_radius=_blur_size(config, "blur_size", 48),
                color_ratio=_color_ratio(config, "color_ratio", 0.72),
            )
        else:
            base = _gradient_background(FULL_COVER_SIZE, _darken_color(dominant, 0.55), (242, 242, 242))
        draw = ImageDraw.Draw(base, "RGBA")
        for _index in range(7):
            radius = rng.randint(70, 180)
            x = rng.randint(880, 1880)
            y = rng.randint(-80, 1080)
            draw.ellipse([x - radius, y - radius, x + radius, y + radius], fill=(*dominant, 30))
        poster = _rounded(ImageOps.fit(source, (660, 900), method=Image.Resampling.LANCZOS), 64)
        shadow = Image.new("RGBA", poster.size, (0, 0, 0, 120)).filter(ImageFilter.GaussianBlur(24))
        base.alpha_composite(shadow, (1170, 120))
        base.alpha_composite(poster, (1140, 90))
        self._draw_titles(
            base,
            title_zh,
            title_en,
            accent=dominant,
            font_scale=_font_scale(config, "zh_font_size", "en_font_size"),
        )
        return base

    def _multi_1(
        self,
        sources: Sequence[Image.Image],
        title_zh: str,
        title_en: str,
        *,
        blur: bool,
        seed: int,
        config: Mapping[str, Any],
    ) -> Image.Image:
        del seed
        dominant = _dominant_color(sources[0])
        base = (
            _tinted_background(
                sources[0],
                blur_radius=_blur_size(config, "blur_size_multi_1", 55),
                color_ratio=_color_ratio(config, "color_ratio_multi_1", 0.78),
            )
            if blur
            else _gradient_background(FULL_COVER_SIZE, _darken_color(dominant, 0.6), (245, 245, 245))
        )
        ordered = [sources[index % len(sources)] for index in range(9)]
        positions = ((980, -100), (1270, -25), (1560, -100))
        for column, (x, y) in enumerate(positions):
            column_layer = Image.new("RGBA", (330, 1320), (0, 0, 0, 0))
            for row in range(3):
                poster = _rounded(
                    ImageOps.fit(
                        ordered[column * 3 + row],
                        (300, 420),
                        method=Image.Resampling.LANCZOS,
                    ),
                    28,
                )
                column_layer.alpha_composite(poster, (15, row * 438))
            _paste_rotated(base, column_layer, center=(x + 165, y + 660), angle=-7)
        self._draw_titles(
            base,
            title_zh,
            title_en,
            accent=dominant,
            shadow=blur,
            font_scale=_font_scale(config, "zh_font_size_multi_1", "en_font_size_multi_1"),
        )
        return base

    def _draw_titles(
        self,
        image: Image.Image,
        title_zh: str,
        title_en: str,
        *,
        accent: tuple[int, int, int],
        shadow: bool = True,
        font_scale: tuple[float, float] = (1.0, 1.0),
    ) -> None:
        draw = ImageDraw.Draw(image, "RGBA")
        zh_font = _font(self._zh_font, round(150 * font_scale[0]))
        en_font = _font(self._en_font, round(54 * font_scale[1]))
        x, y = 80, 350
        if shadow:
            draw.text((x + 5, y + 5), str(title_zh), font=zh_font, fill=(0, 0, 0, 90))
        draw.text((x, y), str(title_zh), font=zh_font, fill=(255, 255, 255, 245))
        if title_en:
            en_y = y + 190
            draw.rectangle([x, en_y + 6, x + 18, en_y + 72], fill=(*accent, 255))
            if shadow:
                draw.text((x + 42, en_y + 4), str(title_en), font=en_font, fill=(0, 0, 0, 80))
            draw.text((x + 38, en_y), str(title_en), font=en_font, fill=(255, 255, 255, 235))


def _read_font(path: Path) -> bytes:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return resolved.read_bytes()


def _font(value: bytes, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try:
        return ImageFont.truetype(BytesIO(value), size=max(1, int(size)))
    except Exception:
        return ImageFont.load_default(size=max(1, int(size)))


def _font_scale(config: Mapping[str, Any], zh_key: str, en_key: str) -> tuple[float, float]:
    return (_positive_float(config.get(zh_key), 1.0), _positive_float(config.get(en_key), 1.0))


def _blur_size(config: Mapping[str, Any], key: str, default: int) -> int:
    try:
        value = int(config[key])
    except (KeyError, TypeError, ValueError):
        return default
    return value if value >= 0 else default


def _color_ratio(config: Mapping[str, Any], key: str, default: float) -> float:
    try:
        value = float(config[key])
    except (KeyError, TypeError, ValueError):
        return default
    return value if 0 <= value <= 1 else default


def _positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _decode_image(value: bytes) -> Image.Image:
    with Image.open(BytesIO(value)) as source:
        return source.convert("RGBA")


def _png_bytes(image: Image.Image) -> bytes:
    output = BytesIO()
    image.convert("RGBA").save(output, format="PNG", optimize=False, compress_level=9)
    return output.getvalue()


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _dominant_color(image: Image.Image) -> tuple[int, int, int]:
    sample = ImageOps.fit(image.convert("RGB"), (32, 32), method=Image.Resampling.BILINEAR)
    mean = ImageStat.Stat(sample).mean
    return tuple(max(24, min(231, int(value))) for value in mean[:3])  # type: ignore[return-value]


def _shift_color(color: tuple[int, int, int], rng: random.Random) -> tuple[int, int, int]:
    return tuple(max(0, min(255, channel + rng.randint(-28, 28))) for channel in color)  # type: ignore[return-value]


def _tinted_background(
    source: Image.Image,
    *,
    blur_radius: int,
    color_ratio: float,
) -> Image.Image:
    background = ImageOps.fit(source.convert("RGBA"), FULL_COVER_SIZE, method=Image.Resampling.LANCZOS)
    background = background.filter(ImageFilter.GaussianBlur(max(0, int(blur_radius))))
    dominant = _dominant_color(source)
    tint = Image.new("RGBA", FULL_COVER_SIZE, (*_darken_color(dominant, 0.72), 255))
    return Image.blend(background, tint, max(0.0, min(1.0, float(color_ratio))))


def _rounded(image: Image.Image, radius: int) -> Image.Image:
    value = image.convert("RGBA")
    mask = Image.new("L", value.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [0, 0, value.width - 1, value.height - 1],
        radius=max(0, int(radius)),
        fill=255,
    )
    value.putalpha(mask)
    return value


def _paste_rotated(
    canvas: Image.Image,
    image: Image.Image,
    *,
    center: tuple[int, int],
    angle: float,
) -> None:
    rotated = image.convert("RGBA").rotate(
        angle,
        resample=Image.Resampling.BICUBIC,
        expand=True,
    )
    canvas.alpha_composite(
        rotated,
        (center[0] - rotated.width // 2, center[1] - rotated.height // 2),
    )


def _gradient_background(
    size: tuple[int, int],
    start: tuple[int, int, int],
    end: tuple[int, int, int],
) -> Image.Image:
    width, height = size
    image = Image.new("RGBA", size)
    draw = ImageDraw.Draw(image)
    for x in range(width):
        ratio = (x / max(1, width - 1)) ** 0.7
        color = tuple(int(start[index] * (1 - ratio) + end[index] * ratio) for index in range(3))
        draw.line([(x, 0), (x, height)], fill=(*color, 255))
    return image


def _draw_badge(
    *,
    image: Image.Image,
    item_count: Any,
    font_bytes: bytes,
    style: Any = "badge",
    size_ratio: Any = 0.12,
    base_color: tuple[int, int, int] | None = None,
) -> Image.Image:
    if not item_count:
        return image

    canvas_height = image.height
    image = image.convert("RGBA")
    badge_font_size = int(canvas_height * float(size_ratio))
    margin = int(canvas_height * 0.04)
    count_text = str(item_count)
    try:
        badge_font = ImageFont.truetype(BytesIO(font_bytes), size=badge_font_size)
    except Exception:
        badge_font = ImageFont.load_default(size=badge_font_size)

    if style == "badge":
        temporary_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
        text_bbox = temporary_draw.textbbox((0, 0), count_text, font=badge_font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        badge_padding_h = int(badge_font_size * 0.4)
        badge_padding_v = int(badge_font_size * 0.2)
        badge_width = int(text_width + badge_padding_h * 2)
        badge_height = int(text_height + badge_padding_v * 2)
        badge_position = (margin, margin)
        badge_rectangle = (
            badge_position[0],
            badge_position[1],
            badge_position[0] + badge_width,
            badge_position[1] + badge_height,
        )
        badge_fill = (*_darken_color(base_color, 0.3), 190) if base_color is not None else (40, 40, 40, 180)
        badge_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
        ImageDraw.Draw(badge_layer).rounded_rectangle(
            badge_rectangle,
            radius=int(badge_height * 0.3),
            fill=badge_fill,
        )
        image = Image.alpha_composite(image, badge_layer)
        draw = ImageDraw.Draw(image)
        center = (
            badge_position[0] + badge_width / 2,
            badge_position[1] + badge_height / 2,
        )
        draw.text(
            (center[0] + 2, center[1] + 2),
            count_text,
            font=badge_font,
            fill=(0, 0, 0, 100),
            anchor="mm",
        )
        draw.text(center, count_text, font=badge_font, fill=(255, 255, 255, 240), anchor="mm")
        return image

    if style == "ribbon":
        ribbon_width = int(badge_font_size * 3.0)
        fold_size = int(ribbon_width * 0.5)
        ribbon_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
        ribbon_draw = ImageDraw.Draw(ribbon_layer)
        ribbon_draw.polygon(
            [(0, 0), (ribbon_width, 0), (0, ribbon_width)],
            fill=(250, 222, 135, 250),
        )
        ribbon_draw.polygon(
            [(0, 0), (fold_size, 0), (0, fold_size)],
            fill=(0, 0, 0, 0),
        )
        image = Image.alpha_composite(image, ribbon_layer)

        temporary_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
        text_bbox = temporary_draw.textbbox((0, 0), count_text, font=badge_font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        text_canvas_size = int(math.sqrt(text_width**2 + text_height**2) * 1.5)
        text_canvas = Image.new("RGBA", (text_canvas_size, text_canvas_size), (0, 0, 0, 0))
        text_draw = ImageDraw.Draw(text_canvas)
        center = (text_canvas_size / 2, text_canvas_size / 2)
        text_draw.text(
            (center[0] + 2, center[1] + 2),
            count_text,
            font=badge_font,
            fill=(0, 0, 0, 80),
            anchor="mm",
        )
        text_draw.text(
            center,
            count_text,
            font=badge_font,
            fill=(89, 52, 2, 245),
            anchor="mm",
        )
        rotated_text = text_canvas.rotate(45, resample=Image.Resampling.BICUBIC, expand=True)
        paste_center = int(ribbon_width * 0.38)
        final_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
        final_layer.paste(
            rotated_text,
            (
                paste_center - rotated_text.width // 2,
                paste_center - rotated_text.height // 2,
            ),
        )
        return Image.alpha_composite(image, final_layer)

    return image


def _darken_color(color: tuple[int, int, int], factor: float) -> tuple[int, int, int]:
    return (
        int(color[0] * factor),
        int(color[1] * factor),
        int(color[2] * factor),
    )


__all__ = [
    "CoverPreviewRenderer",
    "DeterministicCoverRenderer",
    "PillowCoverPreviewRenderer",
    "RenderedCover",
]
