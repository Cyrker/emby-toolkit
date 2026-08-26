from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID, uuid5

UPLOAD_MONITOR_CONFIG_KEY = "p115_upload_monitor_config"
UPLOAD_MONITOR_TOPIC = "p115.upload.requested"
UPLOAD_MONITOR_TOPICS = (UPLOAD_MONITOR_TOPIC,)
UPLOAD_IGNORED_EXTENSIONS = frozenset({".strm"})
UPLOAD_IGNORED_SUFFIXES = (
    ".!qb",
    ".part",
    ".partial",
    ".filepart",
    ".crdownload",
    ".download",
    ".opdownload",
    ".aria2",
    ".tmp",
    ".temp",
    ".td",
    ".xltd",
    ".ut!",
    ".!ut",
    ".bc!",
    ".jc!",
)
_OPERATION_NAMESPACE = UUID("9027bd48-ee23-58fb-8902-bc115a6ca9f8")


class UploadMonitorConfigRevisionChanged(RuntimeError):
    pass


class UploadSource(StrEnum):
    WATCHDOG = "watchdog"
    SCAN = "scan"


class UploadEventType(StrEnum):
    UPSERT = "upsert"
    DELETED = "deleted"


class UploadOperationStatus(StrEnum):
    OBSERVED = "observed"
    STABILITY_WAIT = "stability_wait"
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    REMOTE_CONFIRMED = "remote_confirmed"
    PROJECTING_LOCAL = "projecting_local"
    INGESTION_PENDING = "ingestion_pending"
    RETRYABLE = "retryable"
    SUSPENDED = "suspended"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    SUPERSEDED = "superseded"

    @property
    def terminal(self) -> bool:
        return self in {
            UploadOperationStatus.CONFIRMED,
            UploadOperationStatus.REJECTED,
            UploadOperationStatus.INDETERMINATE,
            UploadOperationStatus.SUPERSEDED,
        }

    @property
    def request_may_have_left_process(self) -> bool:
        return self in {
            UploadOperationStatus.REQUESTING,
            UploadOperationStatus.AWAITING_READBACK,
            UploadOperationStatus.REMOTE_CONFIRMED,
            UploadOperationStatus.PROJECTING_LOCAL,
            UploadOperationStatus.INGESTION_PENDING,
        }


class UploadProviderDisposition(StrEnum):
    CONFIRMED = "confirmed"
    RETRYABLE = "retryable"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class UploadFileFingerprint:
    size: int
    mtime_ns: int
    device: int
    inode: int

    def __post_init__(self) -> None:
        if min(self.size, self.mtime_ns, self.device, self.inode) < 0:
            raise ValueError("upload file fingerprint values must not be negative")

    def to_dict(self) -> dict[str, int]:
        return {
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "device": self.device,
            "inode": self.inode,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> UploadFileFingerprint:
        return cls(
            size=int(value["size"]),
            mtime_ns=int(value["mtime_ns"]),
            device=int(value.get("device") or 0),
            inode=int(value.get("inode") or 0),
        )


@dataclass(frozen=True, slots=True)
class UploadMonitorControl:
    config_revision: str
    applied_revision: str | None
    generation: int
    updated_at: datetime
    applied_at: datetime | None

    @property
    def applied(self) -> bool:
        return bool(self.config_revision and self.config_revision == self.applied_revision)


@dataclass(frozen=True, slots=True)
class UploadMonitorIntent:
    id: UUID
    operation_key: str
    request_hash: bytes
    source: UploadSource
    event_type: UploadEventType
    correlation_id: UUID | None
    mapping_id: str
    config_revision: str
    mapping: Mapping[str, Any]
    path: str
    path_key: str
    relative_path: str
    fingerprint: UploadFileFingerprint | None
    available_at: datetime
    stability_deadline: datetime
    completed_source: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class UploadOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    source: UploadSource
    event_type: UploadEventType
    correlation_id: UUID | None
    mapping_id: str
    config_revision: str
    mapping: Mapping[str, Any]
    path: str
    path_key: str
    relative_path: str
    fingerprint: UploadFileFingerprint | None
    status: UploadOperationStatus
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
    upload_backend: str | None
    upload_session: Mapping[str, Any] | None
    remote_receipt: Mapping[str, Any] | None
    projection: Mapping[str, Any] | None
    result: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_source: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class UploadReservation:
    operation: UploadOperation
    created: bool


@dataclass(frozen=True, slots=True)
class UploadTransition:
    operation: UploadOperation
    changed: bool


@dataclass(frozen=True, slots=True)
class UploadedSource:
    record_key: str
    operation_id: UUID
    mapping_id: str
    config_revision: str
    local_path: str
    path_key: str
    relative_path: str
    target_cid: str
    mode: str
    fingerprint: UploadFileFingerprint
    remote_receipt: Mapping[str, Any]
    derived_paths: tuple[str, ...]
    derived_ownership: Mapping[str, str]
    completed_at: datetime
    operation_sequence: int = 0

    @property
    def pick_code(self) -> str:
        return str(self.remote_receipt.get("pick_code") or "").strip()

    def payload(self) -> dict[str, Any]:
        return {
            "operation_id": str(self.operation_id),
            "mapping_id": self.mapping_id,
            "config_revision": self.config_revision,
            "local_path": self.local_path,
            "path_key": self.path_key,
            "relative_path": self.relative_path,
            "target_cid": self.target_cid,
            "mode": self.mode,
            "fingerprint": self.fingerprint.to_dict(),
            "pick_code": self.pick_code,
            "remote_receipt": dict(self.remote_receipt),
            "derived_paths": list(self.derived_paths),
            "derived_ownership": dict(self.derived_ownership),
            "completed_at": self.completed_at.astimezone(UTC).isoformat(),
            "operation_sequence": self.operation_sequence,
        }

    def snapshot(self) -> dict[str, Any]:
        return {"record_key": self.record_key, **self.payload()}

    @classmethod
    def from_snapshot(cls, value: Mapping[str, Any]) -> UploadedSource:
        fingerprint = value.get("fingerprint")
        receipt = value.get("remote_receipt")
        derived_paths = value.get("derived_paths")
        derived_ownership = value.get("derived_ownership")
        try:
            completed_at = datetime.fromisoformat(str(value.get("completed_at") or "").replace("Z", "+00:00"))
            operation_id = UUID(str(value.get("operation_id") or ""))
        except (TypeError, ValueError) as exc:
            raise ValueError("completed upload source snapshot identity is invalid") from exc
        if completed_at.tzinfo is None or not isinstance(fingerprint, Mapping) or not isinstance(receipt, Mapping):
            raise ValueError("completed upload source snapshot is incomplete")
        normalized_derived_paths: tuple[str, ...] = ()
        if isinstance(derived_paths, list):
            normalized_derived_paths = tuple(str(path) for path in derived_paths if str(path or ""))
        source = cls(
            record_key=str(value.get("record_key") or "").strip(),
            operation_id=operation_id,
            mapping_id=str(value.get("mapping_id") or "").strip(),
            config_revision=str(value.get("config_revision") or "").strip(),
            local_path=str(value.get("local_path") or ""),
            path_key=str(value.get("path_key") or ""),
            relative_path=str(value.get("relative_path") or ""),
            target_cid=str(value.get("target_cid") or "").strip(),
            mode=str(value.get("mode") or "keep").strip(),
            fingerprint=UploadFileFingerprint.from_mapping(fingerprint),
            remote_receipt=dict(receipt),
            derived_paths=normalized_derived_paths,
            derived_ownership=(
                {str(path): str(digest) for path, digest in derived_ownership.items()}
                if isinstance(derived_ownership, Mapping)
                else {}
            ),
            completed_at=completed_at.astimezone(UTC),
            operation_sequence=int(value.get("operation_sequence") or 0),
        )
        backend = str(source.remote_receipt.get("backend") or "").strip()
        account_id = str(source.remote_receipt.get("account_id") or "").strip()
        if (
            not source.record_key
            or not source.mapping_id
            or not source.local_path
            or not source.path_key
            or not source.target_cid
            or backend not in {"openapi", "cookie"}
            or not account_id.isdigit()
        ):
            raise ValueError("completed upload source snapshot is not identity-pinned")
        return source


@dataclass(frozen=True, slots=True)
class UploadProviderOutcome:
    disposition: UploadProviderDisposition
    backend: str
    request_sent: bool
    session: Mapping[str, Any] | None = None
    receipt: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class UploadProjectionOutcome:
    derived_paths: tuple[str, ...]
    ingestion_event_key: str | None
    details: Mapping[str, Any]


class P115UploadMonitorRepository(Protocol):
    def load_config(self) -> Mapping[str, Any] | None: ...

    def ensure_config(self, config: Mapping[str, Any], revision: str) -> UploadMonitorControl: ...

    def save_config(self, config: Mapping[str, Any], revision: str) -> UploadMonitorControl: ...

    def control(self) -> UploadMonitorControl | None: ...

    def apply_config(self, revision: str) -> UploadMonitorControl: ...

    def reserve(self, intent: UploadMonitorIntent) -> UploadReservation: ...

    def read(self, operation_id: UUID) -> UploadOperation | None: ...

    def source_by_path(self, mapping_id: str, path_key: str) -> UploadedSource | None: ...

    def status(self) -> Mapping[str, Any]: ...


class P115UploadProvider(Protocol):
    def execute(
        self,
        operation: UploadOperation,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome: ...

    def readback(self, operation: UploadOperation) -> UploadProviderOutcome: ...

    def delete(
        self,
        operation: UploadOperation,
        source: UploadedSource,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome: ...

    def readback_delete(self, operation: UploadOperation, source: UploadedSource) -> UploadProviderOutcome: ...


class P115UploadProjection(Protocol):
    def project(self, operation: UploadOperation) -> UploadProjectionOutcome: ...

    def delete_source(self, operation: UploadOperation) -> Mapping[str, Any]: ...

    def delete_uploaded_source(self, operation: UploadOperation, source: UploadedSource) -> Mapping[str, Any]: ...


class P115UploadMonitorConfigService:
    def __init__(
        self,
        repository: P115UploadMonitorRepository,
        default_extensions: Callable[[], Iterable[str]],
    ) -> None:
        self._repository = repository
        self._default_extensions = default_extensions

    def get(self) -> dict[str, Any]:
        return normalize_upload_monitor_config(
            self._repository.load_config(),
            default_extensions=self._default_extensions(),
        )

    def snapshot(self) -> tuple[dict[str, Any], str]:
        config = self.get()
        return config, upload_monitor_config_revision(config)

    def ensure_persisted(self) -> tuple[dict[str, Any], str]:
        config, revision = self.snapshot()
        self._repository.ensure_config(config, revision)
        return self.snapshot()

    def save(self, value: object) -> tuple[dict[str, Any], UploadMonitorControl]:
        config = normalize_upload_monitor_config(
            value,
            default_extensions=self._default_extensions(),
        )
        revision = upload_monitor_config_revision(config)
        return config, self._repository.save_config(config, revision)

    def status(self) -> dict[str, Any]:
        return dict(self._repository.status())

    def control(self) -> UploadMonitorControl | None:
        return self._repository.control()


class P115UploadMonitorIngressService:
    def __init__(
        self,
        *,
        config: P115UploadMonitorConfigService,
        repository: P115UploadMonitorRepository,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._clock = clock or (lambda: datetime.now(UTC))

    def enabled(self) -> bool:
        config = self._config.get()
        return bool(config["enabled"] and any(mapping.get("enabled") for mapping in config["mappings"]))

    def ensure_config(self) -> tuple[dict[str, Any], str]:
        return self._config.ensure_persisted()

    def apply_config(self) -> UploadMonitorControl:
        _config, revision = self.ensure_config()
        return self._repository.apply_config(revision)

    def scan_existing(self) -> int:
        config, revision = self._config.snapshot()
        if not config["enabled"]:
            return 0
        submitted = 0
        for mapping in config["mappings"]:
            root = str(mapping["local_dir"])
            if not mapping.get("enabled") or not os.path.isdir(root) or os.path.islink(root):
                continue
            for current, directories, files in os.walk(root, topdown=True, followlinks=False):
                directories[:] = sorted(
                    [name for name in directories if not os.path.islink(os.path.join(current, name))],
                    key=str.casefold,
                )
                for filename in sorted(files, key=str.casefold):
                    path = os.path.join(current, filename)
                    reservations = self.record_event(
                        "created",
                        path,
                        source="scan",
                        source_event_key=hashlib.sha256(
                            f"p115-upload-scan\0{revision}\0{_path_key(path)}".encode()
                        ).hexdigest(),
                    )
                    submitted += sum(1 for reservation in reservations if reservation.created)
        return submitted

    def record_event(
        self,
        event_type: str,
        path: str,
        *,
        related_path: str | None = None,
        source: str = "watchdog",
        source_event_key: str | None = None,
        correlation_id: UUID | None = None,
    ) -> tuple[UploadReservation, ...]:
        source_value = UploadSource(str(source or "").strip().casefold())
        normalized_event = str(event_type or "").strip().casefold()
        if normalized_event == "moved":
            if not related_path:
                raise ValueError("moved upload event requires related_path")
            correlation = correlation_id or uuid5(
                _OPERATION_NAMESPACE,
                f"move\0{_path_key(related_path)}\0{_path_key(path)}",
            )
            values = [
                self._record_one(
                    UploadEventType.DELETED,
                    related_path,
                    source=source_value,
                    source_event_key=f"{source_event_key}:source" if source_event_key else None,
                    correlation_id=correlation,
                ),
                self._record_one(
                    UploadEventType.UPSERT,
                    path,
                    source=source_value,
                    source_event_key=f"{source_event_key}:destination" if source_event_key else None,
                    correlation_id=correlation,
                ),
            ]
            return tuple(value for value in values if value is not None)
        event = UploadEventType.DELETED if normalized_event == "deleted" else UploadEventType.UPSERT
        reservation = self._record_one(
            event,
            path,
            source=source_value,
            source_event_key=source_event_key,
            correlation_id=correlation_id,
        )
        return (reservation,) if reservation is not None else ()

    def _record_one(
        self,
        event_type: UploadEventType,
        path: str,
        *,
        source: UploadSource,
        source_event_key: str | None,
        correlation_id: UUID | None,
        _revision_retry: bool = True,
    ) -> UploadReservation | None:
        config, revision = self._config.snapshot()
        if not config["enabled"]:
            return None
        mapping = matching_upload_mapping(path, config["mappings"])
        if mapping is None or not upload_path_is_accepted(path, mapping):
            return None
        now = self._clock().astimezone(UTC)
        source_identity = ""
        completed_source: Mapping[str, Any] | None = None
        fingerprint: UploadFileFingerprint | None = None
        if event_type is UploadEventType.UPSERT:
            try:
                fingerprint = observe_upload_file(path, str(mapping["local_dir"]))
            except (FileNotFoundError, NotADirectoryError, PermissionError, OSError, ValueError):
                return None
            source_identity = canonical_json(fingerprint.to_dict())
        else:
            if str(mapping.get("mode") or "") != "keep":
                return None
            completed = self._repository.source_by_path(str(mapping["id"]), _path_key(path))
            if completed is None:
                return None
            if completed.mode != "keep":
                return None
            source_identity = f"{completed.record_key}:{completed.operation_id}"
            completed_source = completed.snapshot()
        relative_path = _safe_relative_path(path, str(mapping["local_dir"]))
        basis = {
            "source": source.value,
            "source_event_key": str(source_event_key or ""),
            "event_type": event_type.value,
            "config_revision": revision,
            "mapping_id": str(mapping["id"]),
            "path_key": _path_key(path),
            "source_identity": source_identity,
        }
        operation_key = hashlib.sha256(canonical_json(basis).encode()).hexdigest()
        request = {
            **basis,
            "mapping": dict(mapping),
            "relative_path": relative_path,
            "fingerprint": fingerprint.to_dict() if fingerprint is not None else None,
            "completed_source": completed_source,
        }
        intent = UploadMonitorIntent(
            id=uuid5(_OPERATION_NAMESPACE, operation_key),
            operation_key=operation_key,
            request_hash=hashlib.sha256(canonical_json(request).encode()).digest(),
            source=source,
            event_type=event_type,
            correlation_id=correlation_id,
            mapping_id=str(mapping["id"]),
            config_revision=revision,
            mapping=dict(mapping),
            path=os.path.normpath(str(path)),
            path_key=_path_key(path),
            relative_path=relative_path,
            fingerprint=fingerprint,
            available_at=now + timedelta(seconds=2 if event_type is UploadEventType.DELETED else 3),
            stability_deadline=now + timedelta(seconds=60),
            completed_source=completed_source,
        )
        try:
            return self._repository.reserve(intent)
        except UploadMonitorConfigRevisionChanged:
            if not _revision_retry:
                raise
            return self._record_one(
                event_type,
                path,
                source=source,
                source_event_key=source_event_key,
                correlation_id=correlation_id,
                _revision_retry=False,
            )


def normalize_upload_monitor_config(
    value: object,
    *,
    default_extensions: Iterable[str],
) -> dict[str, Any]:
    raw = value if isinstance(value, Mapping) else {}
    defaults = _normalize_extensions(default_extensions) - UPLOAD_IGNORED_EXTENSIONS
    mappings: list[dict[str, Any]] = []
    seen: set[str] = set()
    raw_mapping_value = raw.get("mappings")
    raw_mappings: list[Any] = raw_mapping_value if isinstance(raw_mapping_value, list) else []
    for item in raw_mappings:
        if not isinstance(item, Mapping):
            continue
        local_dir = os.path.normpath(str(item.get("local_dir") or "").strip())
        target_cid = str(item.get("target_cid") or "").strip()
        if not local_dir or not target_cid or target_cid == "0":
            continue
        dedupe_key = _path_key(local_dir)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        mode = str(item.get("mode") or "keep").strip().casefold()
        if mode not in {"keep", "delete"}:
            mode = "keep"
        raw_extensions = item.get("extensions")
        extensions = (
            _normalize_extensions(raw_extensions) if isinstance(raw_extensions, list) else defaults
        ) - UPLOAD_IGNORED_EXTENSIONS
        mappings.append(
            {
                "id": str(item.get("id") or upload_mapping_id(local_dir, target_cid)).strip(),
                "enabled": _as_bool(item.get("enabled"), True),
                "local_dir": local_dir,
                "target_cid": target_cid,
                "target_name": str(item.get("target_name") or target_cid).strip(),
                "mode": mode,
                "extensions": sorted(extensions),
            }
        )
    return {"enabled": _as_bool(raw.get("enabled"), False), "mappings": mappings}


def upload_monitor_config_revision(config: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(config)).encode()).hexdigest()


def upload_mapping_id(local_dir: str, target_cid: str) -> str:
    raw = f"{os.path.normcase(local_dir)}\0{target_cid}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()[:16]


def matching_upload_mapping(path: object, mappings: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    matches = [
        dict(mapping)
        for mapping in mappings
        if mapping.get("enabled") and _path_in_root(path, mapping.get("local_dir"))
    ]
    return max(matches, key=lambda mapping: len(os.path.abspath(str(mapping["local_dir"])))) if matches else None


def upload_path_is_accepted(path: object, mapping: Mapping[str, Any]) -> bool:
    value = str(path or "")
    if not value or os.path.basename(value).startswith("."):
        return False
    lower = value.casefold()
    if lower.endswith(UPLOAD_IGNORED_SUFFIXES):
        return False
    extension = os.path.splitext(lower)[1]
    raw_extensions = mapping.get("extensions")
    extension_values: Iterable[object] = raw_extensions if isinstance(raw_extensions, list) else ()
    return extension not in UPLOAD_IGNORED_EXTENSIONS and extension in _normalize_extensions(extension_values)


def observe_upload_file(path: str, root: str) -> UploadFileFingerprint:
    absolute_path = os.path.abspath(path)
    absolute_root = os.path.abspath(root)
    if os.path.realpath(absolute_root) != absolute_root:
        raise ValueError("upload monitor root must not be a symlink")
    if not _path_in_root(absolute_path, absolute_root):
        raise ValueError("upload path is outside its mapping root")
    details = os.stat(absolute_path, follow_symlinks=False)
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise ValueError("upload path must be a regular non-symlink file")
    if not _path_in_root(os.path.realpath(absolute_path), absolute_root):
        raise ValueError("upload path resolves outside its mapping root")
    return UploadFileFingerprint(
        size=int(details.st_size),
        mtime_ns=int(details.st_mtime_ns),
        device=int(details.st_dev),
        inode=int(details.st_ino),
    )


def uploaded_source_record_key(mapping_id: str, path_key: str) -> str:
    return f"{str(mapping_id).strip()}:{str(path_key).strip()}"


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _normalize_extensions(values: Iterable[object]) -> frozenset[str]:
    extensions: set[str] = set()
    for value in values or ():
        extension = str(value or "").strip().casefold().replace("*", "")
        if not extension:
            continue
        extensions.add(extension if extension.startswith(".") else f".{extension}")
    return frozenset(extensions)


def _as_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _path_key(value: object) -> str:
    return os.path.normcase(os.path.abspath(os.path.normpath(str(value or "").strip())))


def _path_in_root(path: object, root: object) -> bool:
    try:
        absolute_root = os.path.abspath(str(root))
        return os.path.commonpath([os.path.abspath(str(path)), absolute_root]) == absolute_root
    except (OSError, TypeError, ValueError):
        return False


def _safe_relative_path(path: str, root: str) -> str:
    if not _path_in_root(path, root):
        raise ValueError("upload path is outside its mapping root")
    relative = os.path.normpath(os.path.relpath(path, root))
    if relative == ".." or relative.startswith(f"..{os.sep}"):
        raise ValueError("upload relative path escapes its mapping root")
    return relative


__all__ = [
    "P115UploadMonitorConfigService",
    "P115UploadMonitorIngressService",
    "P115UploadMonitorRepository",
    "P115UploadProjection",
    "P115UploadProvider",
    "UPLOAD_MONITOR_CONFIG_KEY",
    "UPLOAD_MONITOR_TOPIC",
    "UPLOAD_MONITOR_TOPICS",
    "UploadEventType",
    "UploadFileFingerprint",
    "UploadMonitorControl",
    "UploadMonitorConfigRevisionChanged",
    "UploadMonitorIntent",
    "UploadOperation",
    "UploadOperationStatus",
    "UploadProjectionOutcome",
    "UploadProviderDisposition",
    "UploadProviderOutcome",
    "UploadReservation",
    "UploadSource",
    "UploadTransition",
    "UploadedSource",
    "canonical_json",
    "matching_upload_mapping",
    "normalize_upload_monitor_config",
    "observe_upload_file",
    "upload_mapping_id",
    "upload_monitor_config_revision",
    "upload_path_is_accepted",
    "uploaded_source_record_key",
]
