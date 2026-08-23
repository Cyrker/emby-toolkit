from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from .models import MediaItemType, MetadataAggregate

IMAGE_TYPES = ("Primary", "Art", "Backdrop", "Banner", "Logo", "Thumb", "Disc")
_CHARACTER_ROLE_DISPLAY_PREFIX_RE = re.compile(r"^(?:(?:饰|配)[\s\u3000]+)+")
_VOICE_GENRES = frozenset({"Animation", "动画", "Documentary", "纪录", "记录"})


@dataclass(frozen=True, slots=True)
class MediaPresentationPolicy:
    keyword_to_tags: bool = False
    studio_to_chinese: bool = False
    actor_role_add_prefix: bool = False
    max_actors: int = 50
    max_episode_actors: int = 0
    keyword_mapping: tuple[Mapping[str, Any], ...] = ()
    studio_mapping: tuple[Mapping[str, Any], ...] = ()


def normalize_media_path(value: object) -> str:
    path = str(value or "").strip().replace("\\", "/")
    if not path:
        return ""
    while "//" in path:
        path = path.replace("//", "/")
    return path.rstrip("/") or "/"


def build_image_language_priority(original_language: object, preference: object) -> list[tuple[str, ...]]:
    original = str(original_language or "").strip().lower()
    chinese = ("zh-cn", "zh-tw", "zh-hk", "zh-sg", "zh", "cn")
    english_or_textless = ("en", "null")
    groups: list[tuple[str, ...]] = []

    def add(values: Iterable[str]) -> None:
        normalized = tuple(value for value in values if value)
        if normalized and normalized not in groups:
            groups.append(normalized)

    if str(preference or "").strip().lower() == "original":
        add(chinese if original.startswith("zh") or original == "cn" else (original,))
        add(english_or_textless)
        if not (original.startswith("zh") or original == "cn"):
            add(chinese)
    else:
        add(("zh-cn",))
        add(("zh-tw", "zh-hk", "zh-sg", "zh", "cn"))
        add(english_or_textless)
        if original and not original.startswith("zh") and original not in {"cn", "en"}:
            add((original,))
    return groups


def build_image_language_parameter(priority_groups: Iterable[Iterable[str]]) -> str:
    tmdb_codes = {
        "zh-cn": "zh-CN",
        "zh-tw": "zh-TW",
        "zh-hk": "zh-HK",
        "zh-sg": "zh-SG",
    }
    values: list[str] = []
    for group in priority_groups:
        for raw in group:
            value = tmdb_codes.get(raw, raw)
            if value != "cn" and value not in values:
                values.append(value)
    return ",".join(values)


def sort_image_candidates(
    images: Iterable[Mapping[str, Any]] | None,
    priority_groups: Sequence[Sequence[str]],
) -> list[dict[str, Any]]:
    candidates = [dict(item) for item in images or () if item.get("file_path")]

    def rank(item: Mapping[str, Any]) -> tuple[int, float, int]:
        language = str(item.get("iso_639_1") or "null").strip().lower()
        group_rank = len(priority_groups)
        for index, group in enumerate(priority_groups):
            if language in group:
                group_rank = index
                break
        try:
            vote_average = float(item.get("vote_average") or 0)
        except (TypeError, ValueError):
            vote_average = 0
        try:
            vote_count = int(item.get("vote_count") or 0)
        except (TypeError, ValueError):
            vote_count = 0
        return group_rank, -vote_average, -vote_count

    return sorted(candidates, key=rank)


def preferred_image_candidates(
    images: Iterable[Mapping[str, Any]] | None,
    priority_groups: Sequence[Sequence[str]],
) -> list[dict[str, Any]]:
    ordered = sort_image_candidates(images, priority_groups)
    for group in priority_groups:
        selected = [item for item in ordered if str(item.get("iso_639_1") or "null").strip().lower() in group]
        if selected:
            return selected
    return ordered


def select_image_path(
    images: Iterable[Mapping[str, Any]] | None,
    priority_groups: Sequence[Sequence[str]],
) -> str | None:
    candidates = sort_image_candidates(images, priority_groups)
    return str(candidates[0]["file_path"]) if candidates else None


def normalize_image_rules(values: Iterable[Mapping[str, Any]] | None) -> list[dict[str, int | str]]:
    by_type: dict[str, dict[str, int | str]] = {}
    for value in values or ():
        image_type = str(value.get("type") or value.get("Type") or "").strip().title()
        if image_type not in IMAGE_TYPES:
            continue
        try:
            limit = int(value.get("limit", value.get("Limit", 0)) or 0)
            min_width = int(value.get("min_width", value.get("MinWidth", 0)) or 0)
        except (TypeError, ValueError):
            continue
        if limit <= 0:
            continue
        by_type[image_type] = {
            "type": image_type,
            "limit": min(20, limit) if image_type == "Backdrop" else 1,
            "min_width": max(0, min(10000, min_width)),
        }
    return [by_type[image_type] for image_type in IMAGE_TYPES if image_type in by_type]


def select_policy_candidates(
    candidates: Iterable[Mapping[str, Any]],
    rules: Iterable[Mapping[str, Any]],
    *,
    previous_images: Iterable[Mapping[str, Any]] = (),
    preserve_manual: bool = True,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    previous = [dict(item) for item in previous_images]
    for raw_rule in rules:
        image_type = str(raw_rule.get("type") or "")
        limit = int(raw_rule.get("limit") or 0)
        min_width = int(raw_rule.get("min_width") or 0)
        seen: set[str] = set()
        matches: list[dict[str, Any]] = []
        for raw_candidate in candidates:
            if raw_candidate.get("type") != image_type:
                continue
            width = raw_candidate.get("width")
            if width is not None:
                try:
                    if int(width) < min_width:
                        continue
                except (TypeError, ValueError):
                    continue
            source_url = str(raw_candidate.get("source_url") or "").strip()
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            matches.append(dict(raw_candidate))
            if len(matches) >= limit:
                break
        if preserve_manual:
            manual = next(
                (dict(item) for item in previous if item.get("type") == image_type and item.get("manual")),
                None,
            )
            if manual is not None:
                matches = [
                    manual,
                    *(item for item in matches if item.get("source_url") != manual.get("source_url")),
                ][:limit]
        for index, candidate in enumerate(matches):
            candidate["index"] = index
            selected.append(candidate)
    return selected


def stored_policy_images(images: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for image in images:
        value = dict(image)
        value.pop("url", None)
        value.pop("thumbnail_url", None)
        values.append(value)
    return values


def format_metadata_payload(
    aggregate: MetadataAggregate,
    policy: MediaPresentationPolicy,
    *,
    screenshot_token: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    row = dict(aggregate.row)
    requested_type = aggregate.requested_type
    actor_limit = policy.max_actors if policy.max_actors > 0 else 50
    if requested_type is MediaItemType.EPISODE and policy.max_episode_actors > 0:
        actor_limit = policy.max_episode_actors

    actors = sorted(
        (dict(actor) for actor in aggregate.actors),
        key=lambda actor: _integer(actor.get("order"), 999),
    )[:actor_limit]
    genres = _json_list(row.get("genres_json"))
    genre_names = {str(item.get("name") if isinstance(item, Mapping) else item).strip() for item in genres if item}
    people: list[dict[str, Any]] = []
    for actor in actors:
        name = str(actor.get("name") or actor.get("primary_name") or "").strip()
        if not name:
            continue
        people.append(
            {
                "name": name,
                "role": _format_actor_role(
                    actor.get("role") or actor.get("character"),
                    add_prefix=policy.actor_role_add_prefix,
                    voice_role=bool(genre_names & _VOICE_GENRES),
                ),
                "type": "Actor",
                "tmdb_id": str(actor.get("tmdb_id") or actor.get("tmdb_person_id") or ""),
                "image_url": _tmdb_image_url(actor.get("profile_path"), "w500"),
                "order": _integer(actor.get("order"), 999),
            }
        )
    for director in aggregate.directors:
        name = str(director.get("name") or "").strip()
        if name:
            people.append(
                {
                    "name": name,
                    "type": "Director",
                    "tmdb_id": str(director.get("tmdb_id") or director.get("id") or ""),
                    "image_url": _tmdb_image_url(director.get("profile_path"), "w500"),
                    "order": 1000,
                }
            )
    people.sort(key=lambda person: _integer(person.get("order"), 999))

    tags = (
        _mapped_labels(
            _json_list(row.get("keywords_json")),
            policy.keyword_mapping,
            "ids",
        )
        if policy.keyword_to_tags
        else _source_names(_json_list(row.get("keywords_json")))
    )
    company_values = _json_list(row.get("production_companies_json"))
    network_values = _json_list(row.get("networks_json"))
    if policy.studio_to_chinese:
        studios = _mapped_labels(network_values, policy.studio_mapping, "network_ids")
        for label in _mapped_labels(company_values, policy.studio_mapping, "company_ids"):
            if label not in studios:
                studios.append(label)
    else:
        studios = _source_names([*network_values, *company_values])

    screenshot_url: str | None = None
    screenshot_hash = str(row.get("screenshot_hash") or "")
    if (
        requested_type is MediaItemType.EPISODE
        and not row.get("poster_path")
        and screenshot_hash
        and screenshot_token is not None
    ):
        screenshot_url = screenshot_token(screenshot_hash)
    official_ratings = _json_mapping(row.get("official_rating_json"))
    identity = aggregate.identity
    return {
        "item_type": requested_type.value,
        "tmdb_id": str(row.get("tmdb_id") or ""),
        "series_tmdb_id": identity.tmdb_id if identity.media_type == "tv" else "",
        "imdb_id": row.get("imdb_id"),
        "name": row.get("title"),
        "original_title": row.get("original_title"),
        "overview": row.get("overview"),
        "tagline": row.get("tagline"),
        "premiere_date": _text_date(row.get("release_date")),
        "end_date": _text_date(row.get("last_air_date")),
        "production_year": row.get("release_year"),
        "community_rating": row.get("rating"),
        "official_rating": official_ratings.get("US"),
        "custom_rating": row.get("custom_rating"),
        "runtime_minutes": row.get("runtime_minutes"),
        "genres": [item.get("name") if isinstance(item, Mapping) else item for item in genres],
        "tags": tags,
        "studios": studios,
        "season_number": (row.get("season_number") if row.get("season_number") is not None else identity.season_number)
        if requested_type in {MediaItemType.SEASON, MediaItemType.EPISODE}
        else None,
        "episode_number": (
            row.get("episode_number") if row.get("episode_number") is not None else identity.episode_number
        )
        if requested_type is MediaItemType.EPISODE
        else None,
        "actors_ready": bool(row.get("actors_ready")),
        "people": people,
        "collections": [dict(collection) for collection in aggregate.collections],
        "_screenshot_hash": screenshot_hash,
        "_emby_item_ids": [str(value) for value in _json_list(row.get("emby_item_ids_json")) if value],
        "images": {
            "primary": _tmdb_image_url(row.get("poster_path"), "original") or screenshot_url,
            "backdrop": _tmdb_image_url(row.get("backdrop_path"), "original") or screenshot_url,
            "logo": _tmdb_image_url(row.get("logo_path"), "original"),
            "thumb": _tmdb_image_url(row.get("thumb_path"), "original") or screenshot_url,
        },
    }


def _format_actor_role(value: object, *, add_prefix: bool, voice_role: bool) -> str:
    role = _CHARACTER_ROLE_DISPLAY_PREFIX_RE.sub("", str(value or "").strip()).strip()
    if not role or role in {"演员", "配音"} or not add_prefix:
        return role
    return f"{'配' if voice_role else '饰'} {role}"


def _source_names(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = value.get("name") if isinstance(value, Mapping) else value
        text = str(name or "").strip()
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def _mapped_labels(
    values: Iterable[Any],
    mapping: Iterable[Mapping[str, Any]],
    id_field: str,
) -> list[str]:
    id_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    for entry in mapping:
        label = str(entry.get("label") or "").strip()
        if not label:
            continue
        for value in entry.get(id_field) or ():
            id_map[str(value)] = label
        for value in entry.get("en") or ():
            name = str(value or "").strip()
            if name:
                name_map[name.casefold()] = label
    result: list[str] = []
    for value in values:
        item_id = value.get("id") if isinstance(value, Mapping) else None
        item_name = value.get("name") if isinstance(value, Mapping) else value
        label = id_map.get(str(item_id)) if item_id is not None else None
        if not label and item_name:
            label = name_map.get(str(item_name).strip().casefold())
        if label and label not in result:
            result.append(label)
    return result


def _tmdb_image_url(path: object, size: str) -> str | None:
    if not path:
        return None
    value = str(path)
    if value.startswith(("http://", "https://", "etk-cache://")):
        return value
    return f"https://image.tmdb.org/t/p/{size}/{value.lstrip('/')}"


def _json_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _json_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text_date(value: Any) -> str | None:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value) if value else None


def _integer(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
