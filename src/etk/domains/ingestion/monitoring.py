from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from etk.platform.jobs.json_payload import canonical_json, normalize_json_object

INGESTION_MONITOR_TOPIC = "ingestion.monitor.requested"
QUIET_WINDOW_SECONDS = 3
STABILITY_INTERVAL_SECONDS = 1
STABILITY_TIMEOUT_SECONDS = 60
PHYSICAL_STABLE_MATCHES = 3

AUDIO_EXTENSIONS = frozenset({".flac", ".m4a", ".mp3", ".aac", ".wav", ".ape", ".ogg", ".opus"})
TEMPORARY_SUFFIXES = (".part", ".!qb", ".crdownload", ".tmp", ".aria2")
ETK_STRM_MARKERS = ("/api/p115/play/", "/api/p115/virtual-play/")


class MonitorSource(StrEnum):
    WATCHDOG = "watchdog"
    SCAN = "scan"
    ACTIVE = "active"


class MonitorEventType(StrEnum):
    UPSERT = "upsert"
    DELETED = "deleted"


class MonitorClassification(StrEnum):
    PHYSICAL = "physical"
    STRM = "strm"
    REFRESH_ONLY = "refresh_only"
    DELETED = "deleted"


class MonitorItemStatus(StrEnum):
    PENDING = "pending"
    STABILITY_WAIT = "stability_wait"
    READY = "ready"
    CLAIMED = "claimed"
    RETRYABLE = "retryable"
    SUSPENDED = "suspended"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    SUPERSEDED = "superseded"

    @property
    def terminal(self) -> bool:
        return self in {
            MonitorItemStatus.CONFIRMED,
            MonitorItemStatus.REJECTED,
            MonitorItemStatus.INDETERMINATE,
            MonitorItemStatus.SUPERSEDED,
        }


class BindingStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"

    @property
    def terminal(self) -> bool:
        return self in {BindingStatus.CONFIRMED, BindingStatus.REJECTED, BindingStatus.INDETERMINATE}


class StabilityOutcome(StrEnum):
    WAITING = "waiting"
    READY = "ready"
    MISSING = "missing"
    TIMED_OUT = "timed_out"


class IngestionMonitorError(RuntimeError):
    pass


class IngestionMonitorConflictError(IngestionMonitorError):
    pass


class IngestionMonitorNotFoundError(IngestionMonitorError):
    pass


class IngestionMonitorStateError(IngestionMonitorError):
    pass


@dataclass(frozen=True, slots=True)
class MonitorIntent:
    producer_key: str
    source: MonitorSource
    event_type: MonitorEventType
    path: str
    path_key: str
    related_path: str | None
    correlation_id: UUID | None
    monitor_root: str
    config_revision: str
    refresh_delay_seconds: int
    classification: MonitorClassification
    observed_at: datetime
    available_at: datetime
    stability_deadline: datetime
    suspended: bool

    def request(self) -> dict[str, Any]:
        return {
            "source": self.source.value,
            "event_type": self.event_type.value,
            "path": self.path,
            "path_key": self.path_key,
            "related_path": self.related_path,
            "correlation_id": str(self.correlation_id) if self.correlation_id is not None else None,
            "monitor_root": self.monitor_root,
            "config_revision": self.config_revision,
            "refresh_delay_seconds": self.refresh_delay_seconds,
            "classification": self.classification.value,
            "suspended": self.suspended,
        }

    @property
    def request_hash(self) -> bytes:
        return hashlib.sha256(canonical_json(self.request(), field="monitor intent").encode()).digest()


@dataclass(frozen=True, slots=True)
class FileObservation:
    exists: bool
    size: int | None = None
    mtime_ns: int | None = None

    def __post_init__(self) -> None:
        if self.exists and (self.size is None or self.size < 0):
            raise ValueError("existing file observation requires a non-negative size")
        if self.mtime_ns is not None and self.mtime_ns < 0:
            raise ValueError("file observation mtime_ns must not be negative")


@dataclass(frozen=True, slots=True)
class StabilityDecision:
    outcome: StabilityOutcome
    last_size: int | None
    last_mtime_ns: int | None
    stable_matches: int


@dataclass(frozen=True, slots=True)
class MonitorItemProjection:
    id: UUID
    event_sequence: int
    producer_key: str
    request_hash: bytes
    source: MonitorSource
    event_type: MonitorEventType
    path: str
    path_key: str
    related_path: str | None
    correlation_id: UUID | None
    monitor_root: str
    config_revision: str
    refresh_delay_seconds: int
    classification: MonitorClassification
    status: MonitorItemStatus
    attempts: int
    available_at: datetime
    stability_deadline: datetime
    last_size: int | None
    last_mtime_ns: int | None
    stable_matches: int
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PendingBindingProjection:
    id: UUID
    binding_key: str
    request_hash: bytes
    monitor_item_id: UUID
    path: str
    path_key: str
    sha1: str | None
    pick_code: str | None
    media_info: dict[str, Any] | None
    status: BindingStatus
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    emby_item_id: str | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ReservationResult:
    item: MonitorItemProjection
    created: bool
    superseded_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class BindingReservationResult:
    binding: PendingBindingProjection
    created: bool


@dataclass(frozen=True, slots=True)
class TransitionResult:
    item: MonitorItemProjection
    changed: bool


@dataclass(frozen=True, slots=True)
class BindingTransitionResult:
    binding: PendingBindingProjection
    changed: bool


@dataclass(frozen=True, slots=True)
class MonitorPolicy:
    enabled: bool
    extensions: tuple[str, ...]
    exclude_paths: tuple[str, ...]
    refresh_delay_seconds: int = 0
    quiet_window_seconds: int = QUIET_WINDOW_SECONDS
    stability_timeout_seconds: int = STABILITY_TIMEOUT_SECONDS

    @classmethod
    def create(
        cls,
        *,
        enabled: bool,
        extensions: Iterable[str],
        exclude_paths: Iterable[str] = (),
        refresh_delay_seconds: int = 0,
    ) -> MonitorPolicy:
        normalized_extensions = tuple(
            sorted({_normalize_extension(value) for value in extensions if str(value).strip()})
        )
        normalized_excludes = tuple(
            sorted({normalize_observed_path(value) for value in exclude_paths if str(value or "").strip()})
        )
        return cls(
            bool(enabled),
            normalized_extensions,
            normalized_excludes,
            max(0, int(refresh_delay_seconds)),
        )

    @property
    def revision(self) -> str:
        payload = {
            "enabled": self.enabled,
            "extensions": list(self.extensions),
            "exclude_paths": [normalize_media_path(path) for path in self.exclude_paths],
            "refresh_delay_seconds": self.refresh_delay_seconds,
            "quiet_window_seconds": self.quiet_window_seconds,
            "stability_timeout_seconds": self.stability_timeout_seconds,
        }
        return hashlib.sha256(canonical_json(payload, field="monitor policy").encode()).hexdigest()

    def accepts(self, path: object) -> bool:
        normalized = normalize_observed_path(path)
        filename = os.path.basename(normalized)
        if not filename or filename.startswith(".") or filename.casefold().endswith(TEMPORARY_SUFFIXES):
            return False
        return os.path.splitext(filename)[1].casefold() in self.extensions

    def classification(self, path: object, event_type: MonitorEventType) -> MonitorClassification:
        if event_type is MonitorEventType.DELETED:
            return MonitorClassification.DELETED
        normalized = normalize_observed_path(path)
        extension = os.path.splitext(normalized)[1].casefold()
        if extension in AUDIO_EXTENSIONS or self.is_excluded(normalized):
            return MonitorClassification.REFRESH_ONLY
        if extension == ".strm":
            return MonitorClassification.STRM
        return MonitorClassification.PHYSICAL

    def is_excluded(self, path: object) -> bool:
        key = normalize_media_path(path)
        return any(
            key == normalize_media_path(root) or key.startswith(f"{normalize_media_path(root)}/")
            for root in self.exclude_paths
        )

    def created(
        self,
        path: object,
        *,
        producer_key: str,
        monitor_root: object,
        observed_at: datetime | None = None,
        source: MonitorSource = MonitorSource.WATCHDOG,
    ) -> tuple[MonitorIntent, ...]:
        if not self.accepts(path):
            return ()
        return (
            self._intent(
                path,
                producer_key=producer_key,
                source=source,
                event_type=MonitorEventType.UPSERT,
                monitor_root=monitor_root,
                observed_at=observed_at,
            ),
        )

    def modified(self, _path: object, **_options: object) -> tuple[MonitorIntent, ...]:
        return ()

    def deleted(
        self,
        path: object,
        *,
        producer_key: str,
        monitor_root: object,
        observed_at: datetime | None = None,
        source: MonitorSource = MonitorSource.WATCHDOG,
    ) -> tuple[MonitorIntent, ...]:
        if not self.accepts(path):
            return ()
        return (
            self._intent(
                path,
                producer_key=producer_key,
                source=source,
                event_type=MonitorEventType.DELETED,
                monitor_root=monitor_root,
                observed_at=observed_at,
            ),
        )

    def moved(
        self,
        source_path: object,
        destination_path: object,
        *,
        producer_key: str,
        monitor_root: object,
        observed_at: datetime | None = None,
        source: MonitorSource = MonitorSource.WATCHDOG,
        correlation_id: UUID | None = None,
    ) -> tuple[MonitorIntent, ...]:
        observed = _aware(observed_at or datetime.now(UTC), "observed_at")
        correlation = correlation_id or uuid5(
            NAMESPACE_URL,
            f"etk:ingestion-monitor:{source.value}:{producer_key}",
        )
        intents: list[MonitorIntent] = []
        if self.accepts(source_path):
            intents.append(
                self._intent(
                    source_path,
                    producer_key=f"{producer_key}:src",
                    source=source,
                    event_type=MonitorEventType.DELETED,
                    monitor_root=monitor_root,
                    observed_at=observed,
                    related_path=destination_path,
                    correlation_id=correlation,
                )
            )
        if self.accepts(destination_path):
            intents.append(
                self._intent(
                    destination_path,
                    producer_key=f"{producer_key}:dest",
                    source=source,
                    event_type=MonitorEventType.UPSERT,
                    monitor_root=monitor_root,
                    observed_at=observed,
                    related_path=source_path,
                    correlation_id=correlation,
                )
            )
        return tuple(intents)

    def _intent(
        self,
        path: object,
        *,
        producer_key: str,
        source: MonitorSource,
        event_type: MonitorEventType,
        monitor_root: object,
        observed_at: datetime | None,
        related_path: object | None = None,
        correlation_id: UUID | None = None,
    ) -> MonitorIntent:
        key = _nonempty(producer_key, "producer_key", maximum=512)
        normalized = normalize_observed_path(path)
        root = normalize_observed_path(monitor_root)
        observed = _aware(observed_at or datetime.now(UTC), "observed_at")
        available = observed + timedelta(seconds=self.quiet_window_seconds)
        return MonitorIntent(
            producer_key=key,
            source=source,
            event_type=event_type,
            path=normalized,
            path_key=normalize_media_path(normalized),
            related_path=normalize_observed_path(related_path) if related_path is not None else None,
            correlation_id=correlation_id,
            monitor_root=root,
            config_revision=self.revision,
            refresh_delay_seconds=self.refresh_delay_seconds,
            classification=self.classification(normalized, event_type),
            observed_at=observed,
            available_at=available,
            stability_deadline=available + timedelta(seconds=self.stability_timeout_seconds),
            suspended=not self.enabled,
        )


def evaluate_stability(
    item: MonitorItemProjection,
    observation: FileObservation,
    *,
    observed_at: datetime | None = None,
) -> StabilityDecision:
    now = _aware(observed_at or datetime.now(UTC), "observed_at")
    if item.event_type is MonitorEventType.DELETED:
        return StabilityDecision(StabilityOutcome.READY, None, None, 0)
    if now >= item.stability_deadline:
        return StabilityDecision(
            StabilityOutcome.TIMED_OUT,
            observation.size,
            observation.mtime_ns,
            item.stable_matches,
        )
    if not observation.exists:
        return StabilityDecision(StabilityOutcome.MISSING, None, None, 0)
    assert observation.size is not None
    if item.path.casefold().endswith(".strm") and observation.size > 0:
        return StabilityDecision(StabilityOutcome.READY, observation.size, observation.mtime_ns, 0)
    stable_matches = item.stable_matches + 1 if observation.size > 0 and observation.size == item.last_size else 0
    outcome = StabilityOutcome.READY if stable_matches >= PHYSICAL_STABLE_MATCHES else StabilityOutcome.WAITING
    return StabilityDecision(outcome, observation.size, observation.mtime_ns, stable_matches)


def valid_etk_strm_content(value: str | bytes | None) -> bool:
    if value is None:
        return False
    text = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
    sample = text[:2048].strip()
    return bool(sample) and any(marker in sample for marker in ETK_STRM_MARKERS)


def normalize_media_path(value: object) -> str:
    return str(value or "").strip().replace("\\", "/").rstrip("/").casefold()


def normalize_observed_path(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("path must not be empty")
    return os.path.normpath(raw)


def normalized_json_object(value: Mapping[str, Any] | None, *, field: str) -> dict[str, Any] | None:
    return normalize_json_object(dict(value), field=field) if value is not None else None


def _normalize_extension(value: object) -> str:
    extension = str(value or "").strip().casefold().replace("*", "")
    if not extension:
        raise ValueError("monitor extension must not be empty")
    return extension if extension.startswith(".") else f".{extension}"


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def _nonempty(value: object, field: str, *, maximum: int | None = None) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if maximum is not None and len(normalized) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return normalized


__all__ = [
    "AUDIO_EXTENSIONS",
    "BindingReservationResult",
    "BindingStatus",
    "BindingTransitionResult",
    "ETK_STRM_MARKERS",
    "FileObservation",
    "INGESTION_MONITOR_TOPIC",
    "IngestionMonitorConflictError",
    "IngestionMonitorError",
    "IngestionMonitorNotFoundError",
    "IngestionMonitorStateError",
    "MonitorClassification",
    "MonitorEventType",
    "MonitorIntent",
    "MonitorItemProjection",
    "MonitorItemStatus",
    "MonitorPolicy",
    "MonitorSource",
    "PendingBindingProjection",
    "ReservationResult",
    "StabilityDecision",
    "StabilityOutcome",
    "TransitionResult",
    "evaluate_stability",
    "normalize_media_path",
    "normalize_observed_path",
    "valid_etk_strm_content",
]
