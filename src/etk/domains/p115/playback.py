from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID, uuid4, uuid5

from etk.compatibility.persistent_runtime_state import RuntimeStateBackend
from etk.platform.tasks import TaskOutcome

from .models import P115AuthorizationPoll, P115AuthorizationSession

PLAY_POOL_CONFIG_KEY = "p115_play_pool_config"
PLAY_POOL_SESSIONS_KEY = "p115_play_pool_sessions"
PLAY_POOL_REWARDS_KEY = "p115_play_pool_user_cookie_rewards"
TEMP_DIR_CONFIG_KEY = "p115_temp_dir_config"
VIRTUAL_PLAY_SESSIONS_KEY = "p115_virtual_play_sessions"
PLAYBACK_AGGREGATE_TYPE = "p115_playback_operation"
PLAYBACK_TOPIC = "p115.playback.requested"
PLAYBACK_TOPICS = (PLAYBACK_TOPIC,)
PLAY_POOL_OPENAPI_CLIENT_ID = "100196261"
PLAY_POOL_DEFAULT_SPEEDTEST_THRESHOLD_MBPS = 5.0
PLAYBACK_LEASE_TTL_SECONDS = 12 * 60 * 60
PLAYBACK_QR_TTL_SECONDS = 10 * 60
PLAYBACK_NAMESPACE = UUID("c1190cd6-8cdc-47b6-908c-7030660e07bd")

_SHA1 = re.compile(r"[A-F0-9]{40}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SECRET_FIELDS = frozenset(
    {
        "access_token",
        "authorization",
        "cookie",
        "direct_url",
        "refresh_token",
        "sign_key",
        "sign_val",
    }
)


class PlaybackMode(StrEnum):
    POOL = "pool"
    COPY = "copy"
    VIRTUAL = "virtual"
    MAINTENANCE = "maintenance"


class PlaybackOperationKind(StrEnum):
    PREPARE = "prepare"
    ENSURE_TEMP_DIR = "ensure_temp_dir"
    SPEEDTEST = "speedtest"
    RECLAIM = "reclaim"


class PlaybackOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    REMOTE_CONFIRMED = "remote_confirmed"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"

    @property
    def terminal(self) -> bool:
        return self in {
            PlaybackOperationStatus.CONFIRMED,
            PlaybackOperationStatus.REJECTED,
            PlaybackOperationStatus.INDETERMINATE,
        }


class PlaybackArtifactStatus(StrEnum):
    AVAILABLE = "available"
    RECLAIM_PENDING = "reclaim_pending"
    RECLAIMED = "reclaimed"
    INDETERMINATE = "indeterminate"


class PlaybackLeaseStatus(StrEnum):
    ACTIVE = "active"
    RELEASED = "released"
    EXPIRED = "expired"

    @property
    def active(self) -> bool:
        return self is PlaybackLeaseStatus.ACTIVE


class PlaybackQrStatus(StrEnum):
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    EXPIRED = "expired"
    ERROR = "error"


class PlaybackProviderDisposition(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    RETRYABLE = "retryable"
    INDETERMINATE = "indeterminate"


class P115PlaybackError(RuntimeError):
    pass


class P115PlaybackConflictError(P115PlaybackError):
    pass


class P115PlaybackStateError(P115PlaybackError):
    pass


class P115PlaybackNotFoundError(P115PlaybackError):
    pass


@dataclass(frozen=True, slots=True)
class PlaybackOperationIntent:
    operation_key: str
    request_hash: bytes
    kind: PlaybackOperationKind
    mode: PlaybackMode
    account_id: str
    provider_backend: str
    request: Mapping[str, Any]
    available_at: datetime
    artifact_key: str | None = None
    artifact_generation: int | None = None

    @classmethod
    def create(
        cls,
        *,
        semantic_key: str,
        kind: PlaybackOperationKind,
        mode: PlaybackMode,
        account_id: str,
        provider_backend: str,
        request: Mapping[str, Any],
        available_at: datetime | None = None,
        artifact_key: str | None = None,
        artifact_generation: int | None = None,
    ) -> PlaybackOperationIntent:
        normalized = safe_playback_json(request, field="playback operation request")
        operation_key = sha256_text(semantic_key)
        return cls(
            operation_key=operation_key,
            request_hash=bytes.fromhex(sha256_json(normalized)),
            kind=kind,
            mode=mode,
            account_id=_required_text(account_id, "account_id", maximum=255),
            provider_backend=_backend(provider_backend),
            request=normalized,
            available_at=_aware(
                available_at or datetime.now(UTC) - timedelta(seconds=1),
                "available_at",
            ),
            artifact_key=_optional_digest(artifact_key, "artifact_key"),
            artifact_generation=(
                _positive_int(artifact_generation, "artifact_generation") if artifact_generation is not None else None
            ),
        )


@dataclass(frozen=True, slots=True)
class PlaybackOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    kind: PlaybackOperationKind
    mode: PlaybackMode
    account_id: str
    provider_backend: str
    request: Mapping[str, Any]
    status: PlaybackOperationStatus
    state_revision: int
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    provider_request_started_at: datetime | None
    provider_session: Mapping[str, Any] | None
    remote_receipt: Mapping[str, Any] | None
    readback: Mapping[str, Any] | None
    result: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime
    artifact_key: str | None = None
    artifact_generation: int | None = None


@dataclass(frozen=True, slots=True)
class PlaybackReservation:
    operation: PlaybackOperation
    created: bool


@dataclass(frozen=True, slots=True)
class PlaybackTransition:
    operation: PlaybackOperation
    changed: bool


@dataclass(frozen=True, slots=True)
class PlaybackArtifactIntent:
    artifact_key: str
    generation: int
    operation_id: UUID
    mode: PlaybackMode
    account_id: str
    provider_backend: str
    target_cid: str
    source_sha1: str
    source_size: int
    source_pick_code: str
    source_fid: str
    file_name: str
    receipt: Mapping[str, Any]
    reclaim_not_before: datetime


@dataclass(frozen=True, slots=True)
class PlaybackArtifact:
    id: UUID
    artifact_key: str
    generation: int
    operation_id: UUID
    mode: PlaybackMode
    account_id: str
    provider_backend: str
    target_cid: str
    source_sha1: str
    source_size: int
    source_pick_code: str
    source_fid: str
    file_name: str
    remote_fid: str
    remote_pick_code: str
    status: PlaybackArtifactStatus
    state_revision: int
    remote_receipt: Mapping[str, Any]
    reclaim_not_before: datetime
    last_verified_at: datetime | None
    reclaimed_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PlaybackLeaseIntent:
    lease_key: str
    artifact_id: UUID
    mode: PlaybackMode
    owner_user_id: str
    item_id: str
    play_session_id: str
    client_key: str
    user_agent_hash: str
    source: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class PlaybackLease:
    id: UUID
    lease_key: str
    artifact_id: UUID
    mode: PlaybackMode
    owner_user_id: str
    item_id: str
    play_session_id: str
    client_key: str
    user_agent_hash: str
    source: str
    status: PlaybackLeaseStatus
    state_revision: int
    expires_at: datetime
    cleanup_not_before: datetime
    released_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PlaybackLeaseReservation:
    lease: PlaybackLease
    created: bool


@dataclass(frozen=True, slots=True)
class PlaybackQrSession:
    id: UUID
    owner_user_id: str
    provider_uid: str
    mode: str
    app_type: str
    status: PlaybackQrStatus
    provider_session: P115AuthorizationSession
    credentials: Mapping[str, Any] | None
    error_message: str
    expires_at: datetime
    polled_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PlaybackProviderOutcome:
    disposition: PlaybackProviderDisposition
    request_sent: bool
    receipt: Mapping[str, Any] | None = None
    session: Mapping[str, Any] | None = None
    readback: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class PlaybackPrepareRequest:
    mode: PlaybackMode
    source_pick_code: str = ""
    file_name: str = ""
    item_id: str = ""
    play_session_id: str = ""
    user_id: str = ""
    source: str = ""
    client_key: str = ""
    client_name: str = ""
    user_agent: str = ""
    own_account_only: bool = False
    virtual_id: int | None = None
    sha1: str = ""


@dataclass(frozen=True, slots=True)
class PlaybackPrepared:
    operation: PlaybackOperation
    artifact: PlaybackArtifact
    lease: PlaybackLease
    direct_url: str = field(default="", repr=False)


@dataclass(frozen=True, slots=True)
class PlaybackUpstreamStream:
    status_code: int
    headers: Mapping[str, str]
    chunks: Sequence[bytes] | Any
    close: Callable[[], None]


@dataclass(frozen=True, slots=True)
class PlaybackDirectResponse:
    kind: str
    status_code: int
    body: str = ""
    location: str = field(default="", repr=False)
    stream: PlaybackUpstreamStream | None = field(default=None, repr=False)


class P115PlaybackRepository(Protocol):
    def reserve(self, intent: PlaybackOperationIntent) -> PlaybackReservation: ...

    def read(self, operation_id: UUID) -> PlaybackOperation | None: ...

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> PlaybackOperation | None: ...

    def checkpoint_request(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        session: Mapping[str, Any],
        lease_seconds: float,
    ) -> PlaybackTransition: ...

    def mark_remote_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        receipt: Mapping[str, Any],
        *,
        readback: Mapping[str, Any] | None = None,
    ) -> PlaybackTransition: ...

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
    ) -> PlaybackTransition: ...

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        session: Mapping[str, Any] | None,
        error: Mapping[str, Any],
        retry_after_seconds: float,
    ) -> PlaybackTransition: ...

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        artifact: PlaybackArtifactIntent | None = None,
    ) -> PlaybackTransition: ...

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
    ) -> PlaybackTransition: ...

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
    ) -> PlaybackTransition: ...

    def artifact_by_key(self, artifact_key: str) -> PlaybackArtifact | None: ...

    def artifact(self, artifact_id: UUID) -> PlaybackArtifact | None: ...

    def latest_artifact_generation(self, artifact_key: str) -> int: ...

    def reserve_lease(self, intent: PlaybackLeaseIntent) -> PlaybackLeaseReservation: ...

    def lease(self, lease_id: UUID) -> PlaybackLease | None: ...

    def active_counts(self, account_ids: Sequence[str]) -> Mapping[str, int]: ...

    def release_matching(
        self,
        *,
        play_session_id: str,
        item_id: str,
        owner_user_id: str,
        client_key: str,
        cleanup_delays: Mapping[PlaybackMode, float],
    ) -> tuple[PlaybackLease, ...]: ...

    def release_lease(
        self,
        lease_id: UUID,
        *,
        cleanup_delay_seconds: float,
    ) -> PlaybackLease: ...

    def release_account_leases(
        self,
        account_id: str,
        *,
        cleanup_delays: Mapping[PlaybackMode, float],
    ) -> tuple[PlaybackLease, ...]: ...

    def expire_leases(self, *, limit: int = 100) -> tuple[PlaybackLease, ...]: ...

    def reclaimable_artifacts(self, *, limit: int = 100) -> tuple[PlaybackArtifact, ...]: ...

    def expire_stale_claims(self, *, limit: int = 100) -> int: ...

    def reject_dead_dispatches(self, *, limit: int = 100) -> int: ...

    def expire_qr_sessions(self, *, limit: int = 100) -> int: ...

    def create_qr_session(
        self,
        *,
        owner_user_id: str,
        mode: str,
        app_type: str,
        provider_session: P115AuthorizationSession,
        ttl_seconds: float,
    ) -> PlaybackQrSession: ...

    def qr_session(self, *, owner_user_id: str, provider_uid: str = "") -> PlaybackQrSession | None: ...

    def update_qr_session(
        self,
        session_id: UUID,
        *,
        status: PlaybackQrStatus,
        credentials: Mapping[str, Any] | None = None,
        error_message: str = "",
    ) -> PlaybackQrSession: ...


class P115PlaybackProvider(Protocol):
    def execute(
        self,
        operation: PlaybackOperation,
        *,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome: ...

    def readback(self, operation: PlaybackOperation) -> PlaybackProviderOutcome: ...


class P115PlaybackSourceGateway(Protocol):
    def snapshot(self, request: PlaybackPrepareRequest) -> Mapping[str, Any]: ...

    def primary_account(self) -> Mapping[str, Any]: ...


class P115DirectLinkGateway(Protocol):
    def resolve_primary(self, pick_code: str, user_agent: str) -> str: ...

    def resolve_artifact(self, artifact: PlaybackArtifact, user_agent: str) -> str: ...

    def stream(self, url: str, *, user_agent: str, range_header: str = "") -> PlaybackUpstreamStream: ...


class P115PlaybackAuthorizationGateway(Protocol):
    def create_openapi_qrcode(self, client_id: str) -> tuple[dict[str, Any], P115AuthorizationSession]: ...

    def poll_openapi_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll: ...

    def create_cookie_qrcode(self, app_type: str) -> tuple[dict[str, Any], P115AuthorizationSession]: ...

    def poll_cookie_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll: ...


class P115PlaybackUserGateway(Protocol):
    def display_name(self, user_id: str) -> str: ...

    def expand_allowed_user_ids(self, user_ids: Sequence[str]) -> Sequence[str]: ...

    def reward_summary(self, user_id: str) -> Mapping[str, Any]: ...

    def grant_reward(
        self,
        account: Mapping[str, Any],
        *,
        notify_user: bool,
        speed_text: str,
        error_text: str,
    ) -> Mapping[str, Any]: ...

    def notify_daily_results(self, results: Sequence[Mapping[str, Any]]) -> None: ...


class P115PlaybackConfigService:
    """Atomic owner of the five legacy playback config projections."""

    def __init__(
        self,
        backend: RuntimeStateBackend,
        users: P115PlaybackUserGateway,
    ) -> None:
        self._backend = backend
        self._users = users

    def snapshot(self) -> dict[str, Any]:
        def mutate(current: dict[str, Any] | None, now: datetime):
            normalized = normalize_pool_config(current, now=now)
            changed = current != normalized
            return normalized, deepcopy(normalized), changed

        return self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate)

    def public_config(self) -> dict[str, Any]:
        config = self.snapshot()
        accounts = [self.public_account(account, config=config) for account in config["accounts"]]
        daily_limit = _safe_float(config.get("daily_traffic_limit_gb"), 0.0)
        usable = [
            account
            for account in config["accounts"]
            if account.get("enabled") and has_account_auth(account) and not account_daily_limited(account, daily_limit)
        ]
        threshold = speedtest_threshold_mbps(config.get("auto_speedtest_threshold_mbps"))
        return {
            "enabled": bool(config["enabled"]),
            "accounts": accounts,
            "usable_count": len(usable),
            "temp_dir_name": "ETK临时目录",
            "auto_speedtest_enabled": True,
            "auto_speedtest_threshold_mbps": threshold,
            "auto_speedtest_threshold_text": f"{threshold:.2f} MB/s" if threshold > 0 else "",
            "daily_traffic_limit_gb": daily_limit,
            "daily_traffic_limit_text": human_gb_limit(daily_limit),
        }

    def save_settings(self, values: Mapping[str, Any]) -> dict[str, Any]:
        incoming = dict(values) if isinstance(values, Mapping) else {}

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_pool_config(current, now=now)
            if "enabled" in incoming:
                config["enabled"] = bool(incoming.get("enabled"))
            if "auto_speedtest_threshold_mbps" in incoming:
                config["auto_speedtest_threshold_mbps"] = speedtest_threshold_mbps(
                    incoming.get("auto_speedtest_threshold_mbps")
                )
            if "daily_traffic_limit_gb" in incoming:
                config["daily_traffic_limit_gb"] = max(
                    0.0,
                    _safe_float(incoming.get("daily_traffic_limit_gb"), 0.0),
                )
            config["updated_at"] = _iso(now)
            return config, deepcopy(config), True

        self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate)
        return self.public_config()

    def account(self, account_id: str) -> dict[str, Any] | None:
        wanted = str(account_id or "").strip()
        return next((deepcopy(item) for item in self.snapshot()["accounts"] if item["id"] == wanted), None)

    def account_by_owner(self, owner_type: str, owner_user_id: str) -> dict[str, Any] | None:
        owner = normalize_owner_type(owner_type)
        user_id = str(owner_user_id or "").strip()
        for account in reversed(self.snapshot()["accounts"]):
            if account["owner_type"] == owner and account["owner_user_id"] == user_id:
                return deepcopy(account)
        return None

    def public_account(
        self,
        account: Mapping[str, Any] | None,
        *,
        config: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not account:
            return {}
        current = dict(config or self.snapshot())
        source = dict(account)
        out = {key: deepcopy(value) for key, value in source.items() if key not in _SECRET_FIELDS}
        out.pop("allowed_effective_user_ids", None)
        out["cookie_mask"] = mask_cookie(source.get("cookie"))
        out["has_cookie"] = bool(str(source.get("cookie") or "").strip())
        out["has_openapi"] = bool(str(source.get("access_token") or "").strip())
        out["owner_type"] = normalize_owner_type(source.get("owner_type"))
        out["owner_type_text"] = "用户自有" if out["owner_type"] == "user" else "管理员小号"
        out["owner_user_id"] = str(source.get("owner_user_id") or "").strip()
        out["owner_user_name"] = self._users.display_name(out["owner_user_id"]) if out["owner_user_id"] else ""
        out["shared"] = safe_bool(source.get("shared"), out["owner_type"] != "user")
        out["traffic_text"] = human_bytes(source.get("traffic_bytes"))
        out["daily_traffic_text"] = human_bytes(source.get("daily_traffic_bytes"))
        limit = _safe_float(current.get("daily_traffic_limit_gb"), 0.0)
        out["daily_traffic_limit_text"] = human_gb_limit(limit)
        out["daily_traffic_limited"] = account_daily_limited(source, limit)
        speed = _safe_int(source.get("last_speed_bps"), 0)
        out["last_speed_text"] = f"{human_bytes(speed)}/s" if speed > 0 else "未测速"
        return out

    def upsert_account(self, payload: Mapping[str, Any], *, account_id: str = "") -> dict[str, Any]:
        incoming = dict(payload) if isinstance(payload, Mapping) else {}
        expanded: list[str] | None = None
        if "allowed_user_ids" in incoming:
            selected = normalize_user_ids(incoming.get("allowed_user_ids"))
            expanded = normalize_user_ids(self._users.expand_allowed_user_ids(selected))

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_pool_config(current, now=now)
            target = _find_mutable_account(
                config["accounts"],
                account_id=account_id,
                owner_type=incoming.get("owner_type"),
                owner_user_id=incoming.get("owner_user_id"),
            )
            if target is None:
                target = {
                    "id": uuid4().hex,
                    "play_count": 0,
                    "traffic_bytes": 0,
                    "active_count": 0,
                    "created_at": _iso(now),
                }
                config["accounts"].append(target)
            _apply_account_payload(target, incoming, now=now, expanded_user_ids=expanded)
            config["updated_at"] = _iso(now)
            return config, deepcopy(target), True

        return self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate)

    def delete_account(self, account_id: str) -> bool:
        wanted = str(account_id or "").strip()

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_pool_config(current, now=now)
            before = len(config["accounts"])
            config["accounts"] = [item for item in config["accounts"] if item["id"] != wanted]
            changed = len(config["accounts"]) < before
            if changed:
                config["updated_at"] = _iso(now)
            return config, changed, changed or current != config

        return bool(self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate))

    def patch_account(self, account_id: str, patch: Mapping[str, Any]) -> dict[str, Any]:
        incoming = dict(patch)

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_pool_config(current, now=now)
            target = next((item for item in config["accounts"] if item["id"] == str(account_id)), None)
            if target is None:
                return config, {}, current != config
            target.update(deepcopy(incoming))
            target["updated_at"] = _iso(now)
            config["updated_at"] = _iso(now)
            return config, deepcopy(target), True

        return self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate)

    def compare_and_swap_tokens(
        self,
        account_id: str,
        *,
        expected_access_token: str,
        access_token: str,
        refresh_token: str,
    ) -> dict[str, Any] | None:
        wanted = str(account_id or "").strip()

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_pool_config(current, now=now)
            target = next((item for item in config["accounts"] if item["id"] == wanted), None)
            if target is None:
                return config, None, current != config
            if str(target.get("access_token") or "") != str(expected_access_token or ""):
                return config, deepcopy(target), current != config
            target["access_token"] = str(access_token or "").strip()
            if refresh_token:
                target["refresh_token"] = str(refresh_token).strip()
            target["updated_at"] = _iso(now)
            target.pop("last_error", None)
            config["updated_at"] = _iso(now)
            return config, deepcopy(target), True

        return self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate)

    def apply_account_projection(
        self,
        projection_key: str,
        account_id: str,
        *,
        patch: Mapping[str, Any] | None = None,
        play_count_delta: int = 0,
        traffic_bytes_delta: int = 0,
    ) -> tuple[dict[str, Any], bool]:
        receipt_key = _digest(projection_key, "projection_key")

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_pool_config(current, now=now)
            receipts = config.get("_native_projection_receipts")
            receipts = dict(receipts) if isinstance(receipts, Mapping) else {}
            target = next((item for item in config["accounts"] if item["id"] == str(account_id)), None)
            if target is None or receipt_key in receipts:
                return config, (deepcopy(target or {}), False), current != config
            target.update(deepcopy(dict(patch or {})))
            if play_count_delta:
                target["play_count"] = max(0, _safe_int(target.get("play_count"), 0) + int(play_count_delta))
            if traffic_bytes_delta:
                size = max(0, int(traffic_bytes_delta))
                today = now.astimezone(UTC).date().isoformat()
                daily = _safe_int(target.get("daily_traffic_bytes"), 0)
                if str(target.get("daily_traffic_date") or "") != today:
                    daily = 0
                target["traffic_bytes"] = max(0, _safe_int(target.get("traffic_bytes"), 0) + size)
                target["daily_traffic_date"] = today
                target["daily_traffic_bytes"] = daily + size
            target["updated_at"] = _iso(now)
            config["updated_at"] = _iso(now)
            receipts[receipt_key] = _iso(now)
            config["_native_projection_receipts"] = receipts
            return config, (deepcopy(target), True), True

        return self._backend.mutate(PLAY_POOL_CONFIG_KEY, mutate)

    def select_account(
        self,
        *,
        user_id: str,
        own_account_only: bool,
        active_counts: Mapping[str, int],
    ) -> dict[str, Any] | None:
        config = self.snapshot()
        if not config.get("enabled"):
            return None
        normalized_user = str(user_id or "").strip()
        candidates: list[dict[str, Any]] = []
        for source in config["accounts"]:
            account = deepcopy(source)
            if not account.get("enabled") or not has_account_auth(account):
                continue
            if own_account_only and not account_owned_by_user(account, normalized_user):
                continue
            if not account_allowed_for_user(account, normalized_user):
                continue
            if account_daily_limited(account, config.get("daily_traffic_limit_gb")):
                continue
            owner_type = normalize_owner_type(account.get("owner_type"))
            owner_user_id = str(account.get("owner_user_id") or "").strip()
            priority = 0 if normalized_user and owner_type == "user" and owner_user_id == normalized_user else 1
            if owner_type != "user":
                priority = 2
            account["_priority"] = priority
            account["_active_count"] = max(0, int(active_counts.get(account["id"], 0)))
            candidates.append(account)
        candidates.sort(
            key=lambda item: (
                item["_priority"],
                item["_active_count"],
                -_safe_int(item.get("last_speed_bps"), 0),
                _safe_float(item.get("last_used_at"), 0.0),
            )
        )
        return candidates[0] if candidates else None

    def temp_config(self) -> dict[str, Any]:
        def mutate(current: dict[str, Any] | None, now: datetime):
            normalized = normalize_temp_config(current, now=now)
            return normalized, deepcopy(normalized), current != normalized

        return self._backend.mutate(TEMP_DIR_CONFIG_KEY, mutate)

    def save_temp_config(
        self,
        *,
        cid: str,
        cleanup_cron: str,
        projection_key: str | None = None,
    ) -> dict[str, Any]:
        receipt_key = _optional_digest(projection_key, "projection_key")

        def mutate(current: dict[str, Any] | None, now: datetime):
            config = normalize_temp_config(current, now=now)
            receipts = config.get("_native_projection_receipts")
            receipts = dict(receipts) if isinstance(receipts, Mapping) else {}
            if receipt_key and receipt_key in receipts:
                return config, deepcopy(config), current != config
            config.update(
                {
                    "name": "ETK临时目录",
                    "cid": str(cid or "").strip(),
                    "cleanup_cron": str(cleanup_cron or "").strip(),
                    "updated_at": _iso(now),
                }
            )
            if receipt_key:
                receipts[receipt_key] = _iso(now)
                config["_native_projection_receipts"] = receipts
            return config, deepcopy(config), True

        return self._backend.mutate(TEMP_DIR_CONFIG_KEY, mutate)

    def reward_summary(self, user_id: str) -> dict[str, Any]:
        return dict(self._users.reward_summary(str(user_id or "").strip()))

    def grant_reward(
        self,
        account: Mapping[str, Any],
        *,
        notify_user: bool,
        speed_text: str = "",
        error_text: str = "",
    ) -> dict[str, Any]:
        return dict(
            self._users.grant_reward(
                account,
                notify_user=notify_user,
                speed_text=speed_text,
                error_text=error_text,
            )
        )

    def notify_daily_results(self, results: Sequence[Mapping[str, Any]]) -> None:
        self._users.notify_daily_results(results)


class P115PlaybackQrService:
    def __init__(
        self,
        repository: P115PlaybackRepository,
        authorization: P115PlaybackAuthorizationGateway,
    ) -> None:
        self._repository = repository
        self._authorization = authorization

    def create(self, owner_user_id: str, *, mode: str, app_type: str) -> dict[str, Any]:
        owner = _required_text(owner_user_id, "owner_user_id", maximum=255)
        normalized_mode = "openapi" if str(mode or "").strip().casefold() == "openapi" else "cookie"
        app = str(app_type or "alipaymini").strip() or "alipaymini"
        if normalized_mode == "openapi":
            data, provider_session = self._authorization.create_openapi_qrcode(PLAY_POOL_OPENAPI_CLIENT_ID)
        else:
            data, provider_session = self._authorization.create_cookie_qrcode(app)
        session = self._repository.create_qr_session(
            owner_user_id=owner,
            mode=normalized_mode,
            app_type=app,
            provider_session=provider_session,
            ttl_seconds=PLAYBACK_QR_TTL_SECONDS,
        )
        return {
            "success": True,
            "data": {
                "qrcode": data.get("qrcode"),
                "uid": session.provider_uid,
            },
        }

    def poll(self, owner_user_id: str, *, provider_uid: str = "") -> dict[str, Any]:
        owner = _required_text(owner_user_id, "owner_user_id", maximum=255)
        session = self._repository.qr_session(owner_user_id=owner, provider_uid=provider_uid)
        if session is None or session.status is PlaybackQrStatus.EXPIRED:
            return {"success": False, "status": "expired", "message": "请先获取二维码"}
        if session.status is PlaybackQrStatus.SUCCEEDED:
            return {
                "success": True,
                "status": "success",
                "message": "OpenAPI 授权成功" if session.mode == "openapi" else "Cookie 获取成功",
                "data": dict(session.credentials or {}),
            }
        if session.status is PlaybackQrStatus.ERROR:
            return {"success": False, "status": "error", "message": session.error_message or "检查状态失败"}
        outcome = (
            self._authorization.poll_openapi_qrcode(session.provider_session)
            if session.mode == "openapi"
            else self._authorization.poll_cookie_qrcode(session.provider_session)
        )
        if outcome.status == "waiting":
            return {"success": True, "status": "waiting", "message": "等待扫码"}
        if outcome.status == "expired":
            self._repository.update_qr_session(session.id, status=PlaybackQrStatus.EXPIRED)
            return {"success": False, "status": "expired", "message": "二维码已过期"}
        if outcome.status != "success":
            self._repository.update_qr_session(
                session.id,
                status=PlaybackQrStatus.ERROR,
                error_message=outcome.message or "检查状态失败",
            )
            return {"success": False, "status": "error", "message": outcome.message or "检查状态失败"}
        credentials = (
            {
                "access_token": outcome.access_token,
                "refresh_token": outcome.refresh_token,
            }
            if session.mode == "openapi"
            else {"cookie": outcome.cookie, "app_type": outcome.app_type or session.app_type}
        )
        updated = self._repository.update_qr_session(
            session.id,
            status=PlaybackQrStatus.SUCCEEDED,
            credentials=credentials,
        )
        return {
            "success": True,
            "status": "success",
            "message": "OpenAPI 授权成功" if session.mode == "openapi" else "Cookie 获取成功",
            "data": dict(updated.credentials or {}),
        }


def normalize_pool_config(value: Mapping[str, Any] | None, *, now: datetime) -> dict[str, Any]:
    data = dict(value) if isinstance(value, Mapping) else {}
    accounts: list[dict[str, Any]] = []
    raw_accounts = data.get("accounts")
    for raw in raw_accounts if isinstance(raw_accounts, list) else []:
        if not isinstance(raw, Mapping):
            continue
        account = dict(raw)
        account["id"] = str(account.get("id") or uuid4().hex)
        account["alias"] = str(account.get("alias") or "小号").strip()[:40] or "小号"
        account["cookie"] = str(account.get("cookie") or "").strip()
        account["access_token"] = str(account.get("access_token") or "").strip()
        account["refresh_token"] = str(account.get("refresh_token") or "").strip()
        account["app_type"] = str(account.get("app_type") or "alipaymini").strip() or "alipaymini"
        account["enabled"] = bool(account.get("enabled", True))
        account["owner_type"] = normalize_owner_type(account.get("owner_type"))
        account["owner_user_id"] = str(account.get("owner_user_id") or "").strip()
        account["shared"] = safe_bool(account.get("shared"), account["owner_type"] != "user")
        account["temp_cid"] = str(account.get("temp_cid") or "").strip()
        account["play_count"] = max(0, _safe_int(account.get("play_count"), 0))
        account["traffic_bytes"] = max(0, _safe_int(account.get("traffic_bytes"), 0))
        account["active_count"] = max(0, _safe_int(account.get("active_count"), 0))
        account["last_speed_bps"] = max(0, _safe_int(account.get("last_speed_bps"), 0))
        account["allowed_user_ids"] = normalize_user_ids(account.get("allowed_user_ids"))
        account["allowed_effective_user_ids"] = normalize_user_ids(account.get("allowed_effective_user_ids"))
        today = now.astimezone(UTC).date().isoformat()
        if str(account.get("daily_traffic_date") or "") != today:
            account["daily_traffic_date"] = today
            account["daily_traffic_bytes"] = 0
        else:
            account["daily_traffic_bytes"] = max(0, _safe_int(account.get("daily_traffic_bytes"), 0))
        account.pop("last_failed_at", None)
        accounts.append(account)
    out = {
        "enabled": bool(data.get("enabled", False)),
        "auto_speedtest_enabled": True,
        "auto_speedtest_threshold_mbps": speedtest_threshold_mbps(data.get("auto_speedtest_threshold_mbps")),
        "daily_traffic_limit_gb": max(0.0, _safe_float(data.get("daily_traffic_limit_gb"), 0.0)),
        "accounts": accounts,
        "updated_at": str(data.get("updated_at") or ""),
    }
    receipts = data.get("_native_projection_receipts")
    if isinstance(receipts, Mapping):
        out["_native_projection_receipts"] = {str(key): str(timestamp) for key, timestamp in receipts.items()}
    return out


def normalize_temp_config(value: Mapping[str, Any] | None, *, now: datetime) -> dict[str, Any]:
    data = dict(value) if isinstance(value, Mapping) else {}
    cid = str(data.get("cid") or "").strip()
    cleanup = str(data.get("cleanup_cron") or ("0 * * * *" if cid else "")).strip()
    out: dict[str, Any] = {
        "name": "ETK临时目录",
        "cid": cid,
        "cleanup_cron": cleanup,
        "updated_at": str(data.get("updated_at") or ""),
    }
    receipts = data.get("_native_projection_receipts")
    if isinstance(receipts, Mapping):
        out["_native_projection_receipts"] = {str(key): str(timestamp) for key, timestamp in receipts.items()}
    return out


def operation_id(operation_key: str) -> UUID:
    return uuid5(PLAYBACK_NAMESPACE, _digest(operation_key, "operation_key"))


def artifact_key(
    *,
    mode: PlaybackMode,
    account_id: str,
    provider_backend: str,
    target_cid: str,
    source_sha1: str,
    source_size: int,
    source_pick_code: str,
    source_fid: str,
) -> str:
    return sha256_text(
        "|".join(
            (
                mode.value,
                str(account_id),
                _backend(provider_backend),
                str(target_cid),
                normalize_sha1(source_sha1),
                str(max(0, int(source_size))),
                str(source_pick_code or "").strip(),
                str(source_fid or "").strip(),
            )
        )
    )


def lease_key(request: PlaybackPrepareRequest, artifact_digest: str) -> str:
    base = [request.mode.value, _digest(artifact_digest, "artifact_key")]
    play_session = str(request.play_session_id or "").strip()
    if play_session:
        base.append(f"ps:{play_session}")
    else:
        base.extend(
            (
                str(request.item_id or "").strip(),
                str(request.user_id or "").strip(),
                str(request.client_key or "").strip(),
            )
        )
    return sha256_text("|".join(base))


def user_agent_hash(value: str) -> str:
    return sha256_text(str(value or ""))


def safe_playback_json(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    normalized = json.loads(
        json.dumps(dict(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
    )
    if not isinstance(normalized, dict):
        raise ValueError(f"{field} must be a JSON object")
    _reject_secret_fields(normalized, path=field)
    return normalized


def safe_receipt(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    return safe_playback_json(value, field=field)


def sha256_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def normalize_sha1(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if _SHA1.fullmatch(text) else ""


def has_account_auth(account: Mapping[str, Any]) -> bool:
    return bool(str(account.get("cookie") or "").strip() or str(account.get("access_token") or "").strip())


def account_backend(account: Mapping[str, Any]) -> str:
    return "openapi" if str(account.get("access_token") or "").strip() else "cookie"


def normalize_owner_type(value: Any) -> str:
    return "user" if str(value or "").strip().casefold() == "user" else "admin"


def normalize_user_ids(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for raw in value:
        item = str(raw or "").strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def account_allowed_for_user(account: Mapping[str, Any], user_id: str) -> bool:
    allowed = normalize_user_ids(account.get("allowed_effective_user_ids")) or normalize_user_ids(
        account.get("allowed_user_ids")
    )
    if not allowed:
        return True
    normalized = str(user_id or "").strip()
    return bool(normalized and normalized in set(allowed))


def account_owned_by_user(account: Mapping[str, Any], user_id: str) -> bool:
    normalized = str(user_id or "").strip()
    return bool(
        normalized
        and normalize_owner_type(account.get("owner_type")) == "user"
        and str(account.get("owner_user_id") or "").strip() == normalized
    )


def account_daily_limited(account: Mapping[str, Any], limit_gb: Any = 0.0) -> bool:
    limit = max(0.0, _safe_float(limit_gb, 0.0))
    return bool(limit > 0 and _safe_int(account.get("daily_traffic_bytes"), 0) >= int(limit * 1024**3))


def speedtest_threshold_mbps(value: Any) -> float:
    threshold = _safe_float(value, PLAY_POOL_DEFAULT_SPEEDTEST_THRESHOLD_MBPS)
    return threshold if threshold > 0 else PLAY_POOL_DEFAULT_SPEEDTEST_THRESHOLD_MBPS


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().casefold()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def mask_cookie(cookie: Any) -> str:
    text = str(cookie or "")
    if not text:
        return ""
    match = re.search(r"(?:^|;\s*)UID=([^;]+)", text)
    if match:
        raw = match.group(1)
        return f"UID={raw[:4]}***{raw[-3:]}" if len(raw) > 7 else "UID=***"
    return f"{text[:8]}***"


def human_bytes(value: Any) -> str:
    size = float(max(0, _safe_int(value, 0)))
    units = ("B", "KB", "MB", "GB", "TB")
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"


def human_gb_limit(value: Any) -> str:
    limit = max(0.0, _safe_float(value, 0.0))
    return f"{limit:g} GB/天" if limit > 0 else "不限"


def cleanup_delay(mode: PlaybackMode) -> float:
    return {
        PlaybackMode.POOL: 0.0,
        PlaybackMode.COPY: 10 * 60.0,
        PlaybackMode.VIRTUAL: 3 * 60 * 60.0,
        PlaybackMode.MAINTENANCE: 0.0,
    }[mode]


def task_outcome_from_daily_result(result: Mapping[str, Any]) -> TaskOutcome:
    details = dict(result)
    message = str(details.pop("message", "小号池每日测速完成"))
    if details.get("skipped"):
        return TaskOutcome.succeeded(message, details)
    if int(details.get("failed_count") or 0) > 0:
        return TaskOutcome.partial(message, details)
    return TaskOutcome.succeeded(message, details)


def _apply_account_payload(
    target: dict[str, Any],
    payload: Mapping[str, Any],
    *,
    now: datetime,
    expanded_user_ids: list[str] | None,
) -> None:
    if "alias" in payload:
        target["alias"] = str(payload.get("alias") or "小号").strip()[:40] or "小号"
    else:
        target.setdefault("alias", "小号")
    for key in ("cookie", "access_token", "refresh_token"):
        if key in payload:
            target[key] = str(payload.get(key) or "").strip()
    if "app_type" in payload:
        target["app_type"] = str(payload.get("app_type") or "alipaymini").strip() or "alipaymini"
    else:
        target.setdefault("app_type", "alipaymini")
    if "enabled" in payload:
        target["enabled"] = bool(payload.get("enabled"))
    else:
        target.setdefault("enabled", True)
    if "owner_type" in payload:
        target["owner_type"] = normalize_owner_type(payload.get("owner_type"))
    else:
        target.setdefault("owner_type", "admin")
    if "owner_user_id" in payload:
        target["owner_user_id"] = str(payload.get("owner_user_id") or "").strip()
    else:
        target.setdefault("owner_user_id", "")
    if "shared" in payload:
        target["shared"] = safe_bool(payload.get("shared"), target["owner_type"] != "user")
    else:
        target.setdefault("shared", target["owner_type"] != "user")
    if "allowed_user_ids" in payload:
        target["allowed_user_ids"] = normalize_user_ids(payload.get("allowed_user_ids"))
        target["allowed_effective_user_ids"] = list(expanded_user_ids or [])
    else:
        target["allowed_user_ids"] = normalize_user_ids(target.get("allowed_user_ids"))
        target["allowed_effective_user_ids"] = normalize_user_ids(target.get("allowed_effective_user_ids"))
    for key in (
        "last_error",
        "last_speed_at",
        "last_speed_bps",
        "temp_cid",
    ):
        if key in payload:
            target[key] = deepcopy(payload[key])
    target["updated_at"] = _iso(now)
    target.setdefault("temp_cid", "")


def _find_mutable_account(
    accounts: list[dict[str, Any]],
    *,
    account_id: str,
    owner_type: Any,
    owner_user_id: Any,
) -> dict[str, Any] | None:
    wanted = str(account_id or "").strip()
    if wanted:
        return next((item for item in accounts if item["id"] == wanted), None)
    owner = normalize_owner_type(owner_type)
    user_id = str(owner_user_id or "").strip()
    if owner == "user" and user_id:
        return next(
            (item for item in reversed(accounts) if item["owner_type"] == "user" and item["owner_user_id"] == user_id),
            None,
        )
    return None


def _reject_secret_fields(value: Any, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().casefold()
            if normalized in _SECRET_FIELDS or normalized.endswith("_url"):
                raise ValueError(f"{path} contains forbidden secret field {key!r}")
            _reject_secret_fields(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_secret_fields(item, path=f"{path}[{index}]")


def _backend(value: Any) -> str:
    backend = str(value or "").strip().casefold()
    if backend not in {"openapi", "cookie"}:
        raise ValueError("provider_backend must be openapi or cookie")
    return backend


def _digest(value: Any, field: str) -> str:
    digest = str(value or "").strip().casefold()
    if _SHA256.fullmatch(digest) is None:
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return digest


def _optional_digest(value: Any, field: str) -> str | None:
    if value in (None, ""):
        return None
    return _digest(value, field)


def _required_text(value: Any, field: str, *, maximum: int) -> str:
    text = str(value or "").strip()
    if not text or len(text) > maximum:
        raise ValueError(f"{field} must be a 1-{maximum} character string")
    return text


def _positive_int(value: Any, field: str) -> int:
    number = int(value)
    if number <= 0:
        raise ValueError(f"{field} must be positive")
    return number


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", [], {}):
            return default
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", [], {}):
            return default
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


__all__ = [
    "PLAYBACK_LEASE_TTL_SECONDS",
    "PLAYBACK_AGGREGATE_TYPE",
    "PLAYBACK_NAMESPACE",
    "PLAYBACK_QR_TTL_SECONDS",
    "PLAYBACK_TOPIC",
    "PLAYBACK_TOPICS",
    "PLAY_POOL_CONFIG_KEY",
    "PLAY_POOL_DEFAULT_SPEEDTEST_THRESHOLD_MBPS",
    "PLAY_POOL_OPENAPI_CLIENT_ID",
    "PLAY_POOL_REWARDS_KEY",
    "PLAY_POOL_SESSIONS_KEY",
    "TEMP_DIR_CONFIG_KEY",
    "VIRTUAL_PLAY_SESSIONS_KEY",
    "P115DirectLinkGateway",
    "P115PlaybackAuthorizationGateway",
    "P115PlaybackConfigService",
    "P115PlaybackConflictError",
    "P115PlaybackError",
    "P115PlaybackNotFoundError",
    "P115PlaybackProvider",
    "P115PlaybackQrService",
    "P115PlaybackRepository",
    "P115PlaybackSourceGateway",
    "P115PlaybackStateError",
    "P115PlaybackUserGateway",
    "PlaybackArtifact",
    "PlaybackArtifactIntent",
    "PlaybackArtifactStatus",
    "PlaybackDirectResponse",
    "PlaybackLease",
    "PlaybackLeaseIntent",
    "PlaybackLeaseReservation",
    "PlaybackLeaseStatus",
    "PlaybackMode",
    "PlaybackOperation",
    "PlaybackOperationIntent",
    "PlaybackOperationKind",
    "PlaybackOperationStatus",
    "PlaybackPrepareRequest",
    "PlaybackPrepared",
    "PlaybackProviderDisposition",
    "PlaybackProviderOutcome",
    "PlaybackQrSession",
    "PlaybackQrStatus",
    "PlaybackReservation",
    "PlaybackTransition",
    "PlaybackUpstreamStream",
    "account_allowed_for_user",
    "account_backend",
    "account_daily_limited",
    "account_owned_by_user",
    "artifact_key",
    "cleanup_delay",
    "has_account_auth",
    "human_bytes",
    "human_gb_limit",
    "lease_key",
    "mask_cookie",
    "normalize_owner_type",
    "normalize_pool_config",
    "normalize_sha1",
    "normalize_temp_config",
    "normalize_user_ids",
    "operation_id",
    "safe_bool",
    "safe_playback_json",
    "safe_receipt",
    "sha256_json",
    "sha256_text",
    "speedtest_threshold_mbps",
    "task_outcome_from_daily_result",
    "user_agent_hash",
]
