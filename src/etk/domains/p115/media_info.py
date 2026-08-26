from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from .auth_directory import P115CredentialStore
from .fingerprints import canonical_sha1


class P115MediaInfoError(RuntimeError):
    pass


class P115MediaInfoInvalidIdentity(P115MediaInfoError):
    pass


class P115MediaInfoNotFound(P115MediaInfoError):
    pass


class P115MediaInfoInvalidCache(P115MediaInfoError):
    pass


@dataclass(frozen=True, slots=True)
class P115MediaInfoCacheRecord:
    sha1: str
    formatted: object | None
    raw: Mapping[str, Any] | None
    cache_revision: int
    raw_schema_version: int
    formatter_revision: str
    probe_revision: str
    hit_count: int

    @classmethod
    def create(
        cls,
        *,
        sha1: object,
        formatted: object | None,
        raw: object | None,
        cache_revision: object,
        raw_schema_version: object,
        formatter_revision: object,
        probe_revision: object,
        hit_count: object,
    ) -> P115MediaInfoCacheRecord:
        normalized_sha1 = canonical_sha1(sha1)
        revision = _nonnegative_integer(cache_revision, label="cache revision")
        raw_version = _positive_integer(raw_schema_version, label="raw schema version")
        hits = _nonnegative_integer(hit_count, label="cache hit count")
        normalized_formatter = _revision_text(formatter_revision, label="formatter revision")
        normalized_probe = _revision_text(probe_revision, label="probe revision")
        if raw is not None and not isinstance(raw, Mapping):
            raise ValueError("raw ffprobe cache must be an object")
        return cls(
            sha1=normalized_sha1,
            formatted=formatted,
            raw=dict(raw) if isinstance(raw, Mapping) else None,
            cache_revision=revision,
            raw_schema_version=raw_version,
            formatter_revision=normalized_formatter,
            probe_revision=normalized_probe,
            hit_count=hits,
        )


@dataclass(frozen=True, slots=True)
class P115MediaInfoIdentity:
    tmdb_id: str
    media_type: str
    sha1: str
    original_language: str = ""
    season_number: int | None = None
    episode_number: int | None = None
    preid: str = ""

    def __post_init__(self) -> None:
        if not self.tmdb_id:
            raise ValueError("MediaInfo identity requires TMDb ID")
        if self.media_type not in {"movie", "tv"}:
            raise ValueError("MediaInfo identity requires movie or tv type")
        canonical_sha1(self.sha1)
        if self.preid:
            canonical_sha1(self.preid)


@dataclass(frozen=True, slots=True)
class P115MediaInfoRestoreTarget:
    tmdb_id: str
    item_type: str
    title: str
    path: str
    file_name: str
    emby_item_id: str
    sha1: str
    pick_code: str
    file_size: int
    original_language: str = ""
    parent_series_tmdb_id: str = ""
    season_number: int | None = None
    episode_number: int | None = None

    def __post_init__(self) -> None:
        tmdb_id = str(self.tmdb_id or "").strip()
        item_type = str(self.item_type or "").strip().title()
        title = str(self.title or "").strip()
        path = str(self.path or "").strip()
        file_name = str(self.file_name or "").strip()
        emby_item_id = str(self.emby_item_id or "").strip()
        sha1 = canonical_sha1(self.sha1)
        pick_code = str(self.pick_code or "").strip()
        original_language = str(self.original_language or "").strip()
        parent_series_tmdb_id = str(self.parent_series_tmdb_id or "").strip()
        if not tmdb_id or item_type not in {"Movie", "Episode"}:
            raise ValueError("MediaInfo restore target must be a Movie or Episode")
        if not path or not file_name or not emby_item_id:
            raise ValueError("MediaInfo restore target requires path, file name, and Emby ItemID")
        if not pick_code:
            raise ValueError("MediaInfo restore target requires PickCode")
        if isinstance(self.file_size, bool) or int(self.file_size) < 0:
            raise ValueError("MediaInfo restore target file size must be non-negative")
        for field, value in {
            "tmdb_id": tmdb_id,
            "item_type": item_type,
            "title": title,
            "path": path,
            "file_name": file_name,
            "emby_item_id": emby_item_id,
            "sha1": sha1,
            "pick_code": pick_code,
            "original_language": original_language,
            "parent_series_tmdb_id": parent_series_tmdb_id,
        }.items():
            object.__setattr__(self, field, value)
        object.__setattr__(self, "file_size", int(self.file_size))

    @property
    def media_tmdb_id(self) -> str:
        return self.tmdb_id if self.item_type == "Movie" else self.parent_series_tmdb_id

    @property
    def probe_context(self) -> dict[str, Any]:
        values: dict[str, Any] = {
            "tmdb_id": self.media_tmdb_id,
            "type": self.item_type,
            "original_language": self.original_language,
            "season_number": self.season_number,
            "episode_number": self.episode_number,
        }
        return {key: value for key, value in values.items() if value not in (None, "")}


class P115MediaProbeItemStatus(StrEnum):
    PENDING = "pending"
    PROBING = "probing"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class P115MediaProbeTarget(Protocol):
    @property
    def sha1(self) -> str: ...

    @property
    def pick_code(self) -> str: ...

    @property
    def file_name(self) -> str: ...


@dataclass(frozen=True, slots=True)
class P115MediaProbeReservation:
    id: str
    status: P115MediaProbeItemStatus
    attempts: int
    claim_token: str = ""
    cache_revision: int | None = None
    result: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.attempts < 0:
            raise ValueError("media probe reservation attempts must be non-negative")
        if self.cache_revision is not None and self.cache_revision < 0:
            raise ValueError("media probe reservation cache revision must be non-negative")
        if self.result is not None:
            object.__setattr__(self, "result", dict(self.result))
        if self.error is not None:
            object.__setattr__(self, "error", dict(self.error))

    @property
    def claimed(self) -> bool:
        return self.status is P115MediaProbeItemStatus.PROBING and bool(self.claim_token)


class P115MediaInfoCacheRepository(Protocol):
    def find_by_sha1(
        self,
        sha1: str,
        *,
        record_hit: bool = False,
    ) -> P115MediaInfoCacheRecord | None: ...

    def resolve_sha1_by_pick_code(self, pick_code: str, *, account_id: str) -> str | None: ...


class P115MediaInfoFormattedCacheWriter(Protocol):
    def replace_formatted_if_revision(
        self,
        sha1: str,
        *,
        expected_cache_revision: int,
        formatted: object,
    ) -> P115MediaInfoCacheRecord | None: ...


class P115MediaInfoCacheService:
    """Pure cache/identity query boundary for P6.6 Bridge endpoints."""

    def __init__(
        self,
        repository: P115MediaInfoCacheRepository,
        credentials: P115CredentialStore,
    ) -> None:
        self._repository = repository
        self._credentials = credentials

    def formatted_by_sha1(self, sha1: object) -> dict[str, Any]:
        normalized_sha1 = _request_sha1(sha1)
        record = self._repository.find_by_sha1(normalized_sha1, record_hit=True)
        if record is None or record.formatted is None:
            raise P115MediaInfoNotFound("media info cache not found")
        try:
            from .media_info_formatter import normalize_legacy_mediainfo_response

            return normalize_legacy_mediainfo_response(record.formatted)
        except (TypeError, ValueError, KeyError) as exc:
            raise P115MediaInfoInvalidCache("invalid media info cache") from exc

    def formatted_by_pick_code(self, pick_code: object) -> dict[str, Any]:
        return self.formatted_by_sha1(self.sha1_by_pick_code(pick_code))

    def sha1_by_pick_code(self, pick_code: object) -> str:
        normalized_pick_code = str(pick_code or "").strip()
        account_id = str(self._credentials.load().account_id or "").strip()
        if not normalized_pick_code or not account_id.isdigit():
            raise P115MediaInfoNotFound("media info cache not found")
        sha1 = self._repository.resolve_sha1_by_pick_code(
            normalized_pick_code,
            account_id=account_id,
        )
        if not sha1:
            raise P115MediaInfoNotFound("media info cache not found")
        return _request_sha1(sha1)

    def has_formatted_by_sha1(self, sha1: object) -> bool:
        normalized_sha1 = _request_sha1(sha1)
        record = self._repository.find_by_sha1(normalized_sha1)
        return record is not None and record.formatted is not None

    def metadata_identity_by_sha1(self, sha1: object) -> P115MediaInfoIdentity:
        normalized_sha1 = _request_sha1(sha1)
        record = self._repository.find_by_sha1(normalized_sha1)
        if record is None or not isinstance(record.raw, Mapping):
            raise P115MediaInfoNotFound("metadata identity not found")
        identity = _metadata_identity(record.raw, fallback_sha1=normalized_sha1)
        if identity is None:
            raise P115MediaInfoNotFound("metadata identity not found")
        return identity

    def metadata_identity_by_pick_code(self, pick_code: object) -> P115MediaInfoIdentity:
        try:
            sha1 = self.sha1_by_pick_code(pick_code)
        except P115MediaInfoNotFound as exc:
            raise P115MediaInfoNotFound("metadata identity not found") from exc
        return self.metadata_identity_by_sha1(sha1)


def _metadata_identity(raw: Mapping[str, Any], *, fallback_sha1: str) -> P115MediaInfoIdentity | None:
    context = raw.get("_etk")
    if not isinstance(context, Mapping):
        return None
    tmdb_id = str(context.get("tmdb_id") or "").strip()
    raw_type = str(context.get("type") or "").strip().casefold()
    if raw_type in {"movie", "movies", "film", "电影"}:
        media_type = "movie"
    elif raw_type in {"tv", "series", "season", "episode", "电视剧", "剧集", "季", "集", "分集"}:
        media_type = "tv"
    else:
        return None
    if not tmdb_id:
        return None
    try:
        sha1 = canonical_sha1(context.get("sha1") or fallback_sha1)
        preid = canonical_sha1(context.get("preid"), allow_empty=True)
    except ValueError:
        return None
    return P115MediaInfoIdentity(
        tmdb_id=tmdb_id,
        media_type=media_type,
        sha1=sha1,
        original_language=str(context.get("original_language") or "").strip(),
        season_number=_optional_integer(context.get("season_number")),
        episode_number=_optional_integer(context.get("episode_number")),
        preid=preid,
    )


def _request_sha1(value: object) -> str:
    try:
        return canonical_sha1(value)
    except ValueError as exc:
        raise P115MediaInfoInvalidIdentity("invalid sha1") from exc


def _optional_integer(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _nonnegative_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be non-negative")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be non-negative") from exc
    if parsed < 0:
        raise ValueError(f"{label} must be non-negative")
    return parsed


def _positive_integer(value: object, *, label: str) -> int:
    parsed = _nonnegative_integer(value, label=label)
    if parsed < 1:
        raise ValueError(f"{label} must be positive")
    return parsed


def _revision_text(value: object, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 64:
        raise ValueError(f"{label} is invalid")
    if any(not (character.isalnum() or character in "._:-") for character in normalized):
        raise ValueError(f"{label} is invalid")
    return normalized


__all__ = [
    "P115MediaInfoCacheRecord",
    "P115MediaInfoCacheRepository",
    "P115MediaInfoCacheService",
    "P115MediaInfoError",
    "P115MediaInfoFormattedCacheWriter",
    "P115MediaInfoIdentity",
    "P115MediaInfoInvalidCache",
    "P115MediaInfoInvalidIdentity",
    "P115MediaInfoNotFound",
    "P115MediaInfoRestoreTarget",
    "P115MediaProbeItemStatus",
    "P115MediaProbeReservation",
    "P115MediaProbeTarget",
]
