from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class MediaItemType(StrEnum):
    MOVIE = "Movie"
    SERIES = "Series"
    SEASON = "Season"
    EPISODE = "Episode"
    BOXSET = "Boxset"

    @classmethod
    def parse(cls, value: object, *, allow_boxset: bool = False) -> MediaItemType | None:
        normalized = str(value or "").strip().title()
        try:
            item_type = cls(normalized)
        except ValueError:
            return None
        if item_type is cls.BOXSET and not allow_boxset:
            return None
        return item_type


@dataclass(frozen=True, slots=True)
class MediaIdentity:
    tmdb_id: str
    media_type: str
    item_type: MediaItemType
    season_number: int | None = None
    episode_number: int | None = None

    def __post_init__(self) -> None:
        if not self.tmdb_id:
            raise ValueError("media identity requires tmdb_id")
        if self.media_type not in {"movie", "tv"}:
            raise ValueError("media identity type must be movie or tv")
        if self.item_type is MediaItemType.MOVIE and self.media_type != "movie":
            raise ValueError("Movie identity must use movie media type")
        if self.item_type in {MediaItemType.SERIES, MediaItemType.SEASON, MediaItemType.EPISODE}:
            if self.media_type != "tv":
                raise ValueError("television identities must use tv media type")


@dataclass(frozen=True, slots=True)
class MetadataAggregate:
    identity: MediaIdentity
    requested_type: MediaItemType
    row: Mapping[str, Any]
    actors: tuple[Mapping[str, Any], ...] = ()
    directors: tuple[Mapping[str, Any], ...] = ()
    collections: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class MediaInfoEditContext:
    item_id: str
    sha1: str
    mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...]
    title: str
    item_type: str
    parent_series_tmdb_id: str = ""
    season_number: int | None = None
    episode_number: int | None = None


@dataclass(frozen=True, slots=True)
class MediaInfoEpisode:
    emby_id: str
    season_number: int | None
    episode_number: int | None
    title: str


@dataclass(frozen=True, slots=True)
class ImagePolicyKey:
    item_type: MediaItemType
    tmdb_id: str
    season_number: int = -1
    episode_number: int = -1

    @classmethod
    def create(
        cls,
        item_type: MediaItemType | str,
        tmdb_id: object,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> ImagePolicyKey:
        parsed = (
            item_type if isinstance(item_type, MediaItemType) else MediaItemType.parse(item_type, allow_boxset=True)
        )
        normalized_id = str(tmdb_id or "").strip()
        if parsed is None or not normalized_id:
            raise ValueError("image policy requires a valid item type and TMDb id")
        return cls(
            item_type=parsed,
            tmdb_id=normalized_id,
            season_number=int(season_number) if season_number is not None else -1,
            episode_number=int(episode_number) if episode_number is not None else -1,
        )


@dataclass(frozen=True, slots=True)
class ImagePolicyRecord:
    key: ImagePolicyKey
    rules: tuple[Mapping[str, Any], ...]
    images: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ServiceResult:
    payload: Mapping[str, Any]
    status_code: int = 200
    headers: Mapping[str, str] | None = None
