from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any, Protocol

from etk.domains.p115.fingerprints import canonical_sha1
from etk.domains.p115.maintenance_transport import P115PrimaryOperationsGateway
from etk.domains.p115.media_probe import (
    P115MediaProbeRequest,
    P115MediaProbeStatus,
)
from etk.domains.p115.models import P115CredentialSnapshot, P115Result
from etk.domains.p115.organize_plan import (
    P115SourceNode,
)

_VIDEO_EXTENSIONS = frozenset({"avi", "flv", "iso", "m2ts", "mkv", "mov", "mp4", "mpg", "rmvb", "ts", "wmv"})
_SIDECAR_EXTENSIONS = frozenset({"ass", "idx", "nfo", "srt", "ssa", "sub", "sup", "vtt"})
_MEDIA_EXTENSIONS = _VIDEO_EXTENSIONS | _SIDECAR_EXTENSIONS
_SEASON_EPISODE = re.compile(r"(?i)(?:^|[^A-Za-z0-9])S(\d{1,4})[ ._-]*E(\d{1,4})(?:[^A-Za-z0-9]|$)")
_EPISODE = re.compile(r"(?i)(?:^|[^A-Za-z0-9])E(?:P)?[ ._-]*(\d{1,4})(?:[^A-Za-z0-9]|$)")
_INVALID_NAME = str.maketrans(
    {"\\": "＼", "/": "／", ":": "：", "*": "＊", "?": "？", '"': "＂", "<": "＜", ">": "＞", "|": "｜"}
)


class MoviePilotTransferRemoteClient(Protocol):
    @property
    def account_id(self) -> str: ...

    def list_children(self, parent_id: str) -> Sequence[Mapping[str, Any]]: ...

    def file_info(self, file_id: str) -> Mapping[str, Any]: ...


class MoviePilotTransferRemoteGateway(Protocol):
    def open(
        self,
        *,
        expected_account_id: str | None = None,
        expected_backend: str | None = None,
        expected_credential_fingerprint: str | None = None,
    ) -> MoviePilotTransferRemoteClient | None: ...


class MoviePilotCredentialStore(Protocol):
    def load(self) -> P115CredentialSnapshot: ...


class MoviePilotMediaInfoPort(Protocol):
    def enrich(
        self,
        request: P115MediaProbeRequest,
        *,
        operation_key: str,
        progress: object,
    ) -> MoviePilotMediaInfoResult: ...


class MoviePilotMediaInfoResult(Protocol):
    @property
    def status(self) -> P115MediaProbeStatus: ...

    @property
    def diagnostic(self) -> str: ...


class MoviePilotOrganizePlanPort(Protocol):
    def create(self, raw_payload: object) -> P115Result: ...


class MoviePilotOrganizeExecutionPort(Protocol):
    def prepare(
        self,
        plan_id: str,
        approved_intent_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any: ...


class MoviePilotExecutionRepositoryPort(Protocol):
    def find_by_intent(self, approved_intent_id: str) -> Any: ...


class ExplicitMoviePilotRecognitionGateway:
    """Recognition boundary for MP events whose TMDb identity is already authoritative."""

    def recognize(
        self,
        node: P115SourceNode,
        *,
        hints: Mapping[str, Any],
        episode_rules: Sequence[Mapping[str, Any]],
        policy: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        del episode_rules, policy
        title = str(hints.get("title") or "").strip()
        media_type = str(hints.get("media_type") or "").strip().casefold()
        tmdb_id = str(hints.get("tmdb_id") or "").strip()
        if not title or media_type not in {"movie", "tv"} or not tmdb_id:
            raise ValueError("MoviePilot recognition hints are incomplete")
        return {
            "title": title,
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "year": _optional_int(hints.get("year")),
            "season_number": _optional_int(hints.get("season_number")),
            "episode_number": _optional_int(hints.get("episode_number")),
            "sidecar": bool(hints.get("sidecar")),
            "source": "moviepilot_webhook",
            "confidence": "high",
            "evidence": ["moviepilot_webhook", f"source_file:{node.id}"],
        }


class MoviePilotOriginalNameGateway:
    """Keep MP-produced file names while moving the complete episode batch together."""

    def render_names(
        self,
        node: P115SourceNode,
        *,
        recognition: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> Mapping[str, str]:
        del config
        title = _safe_component(recognition.get("title"))
        year = _optional_int(recognition.get("year"))
        main_dir = f"{title} ({year})" if year else title
        season = _optional_int(recognition.get("season_number"))
        season_dir = f"Season {season:02d}" if recognition.get("media_type") == "tv" and season is not None else ""
        file_name = _safe_file_name(node.name)
        return {"main_dir": main_dir, "season_dir": season_dir, "file_name": file_name}


class NativeMoviePilotTransferProcessor:
    """Prepare an immutable native organize execution or a passthrough STRM reconcile Job."""

    def __init__(
        self,
        *,
        credentials: MoviePilotCredentialStore,
        remote: MoviePilotTransferRemoteGateway | P115PrimaryOperationsGateway,
        media_info: MoviePilotMediaInfoPort,
        plans: MoviePilotOrganizePlanPort,
        executions: MoviePilotOrganizeExecutionPort,
        execution_repository: MoviePilotExecutionRepositoryPort,
        jobs: Callable[[], Any],
    ) -> None:
        self._credentials = credentials
        self._remote = remote
        self._media_info = media_info
        self._plans = plans
        self._executions = executions
        self._execution_repository = execution_repository
        self._jobs = jobs

    def process(
        self,
        *,
        event_key: str,
        event_type: str,
        file: Mapping[str, Any],
        passthrough: bool,
    ) -> Mapping[str, Any]:
        event_identity = _digest(event_key)
        file_id = _required_text(file.get("file_id"), "file_id")
        tmdb_id = _required_text(file.get("tmdb_id"), "tmdb_id")
        if passthrough:
            submitted = self._jobs().submit_task(
                "full-sync-strm",
                {"home_video_only": False},
                trigger_type="system",
                idempotency_scope="moviepilot-transfer-passthrough",
                idempotency_key=event_identity,
                max_attempts=2,
                queue_name="native.serial",
            )
            return {
                "event_type": event_type,
                "file_id": file_id,
                "tmdb_id": tmdb_id,
                "passthrough": True,
                "job_id": str(submitted.projection.job.id),
                "job_reused": submitted.reused,
                "reconciliation": "full-sync-strm",
            }

        approved_intent = f"moviepilot-transfer:{event_identity}"
        existing = self._execution_repository.find_by_intent(approved_intent)
        if existing is not None:
            return _execution_result(existing, event_type=event_type, file_id=file_id, tmdb_id=tmdb_id, reused=True)

        account_id = _required_text(file.get("account_id"), "account_id")
        backend = _required_text(file.get("provider_backend"), "provider_backend").casefold()
        credential_fingerprint = _digest(file.get("credential_fingerprint"))
        snapshot = self._credentials.load()
        current_backend, current_fingerprint = _credential_binding(snapshot, backend)
        if (
            not account_id.isdigit()
            or str(snapshot.account_id or "").strip() != account_id
            or current_backend != backend
            or current_fingerprint != credential_fingerprint
        ):
            raise RuntimeError("MoviePilot transfer 115 credential changed before preparation")
        client = self._remote.open(
            expected_account_id=account_id,
            expected_backend=backend,
            expected_credential_fingerprint=credential_fingerprint,
        )
        if client is None:
            raise RuntimeError("MoviePilot transfer 115 client is unavailable")
        parent_id = _required_text(file.get("parent_id"), "parent_id")
        children = tuple(dict(item) for item in client.list_children(parent_id))
        target = next((item for item in children if _item_id(item) == file_id), None)
        if target is None:
            target = _payload_mapping(client.file_info(file_id))
        if _item_id(target) != file_id:
            raise RuntimeError("MoviePilot transfer provider readback did not match the target file")
        selected = _related_files(children, target, file)
        if not selected:
            selected = (target,)

        recognition_hints: dict[str, dict[str, Any]] = {}
        source_file_ids: list[str] = []
        for item in selected:
            selected_id = _required_text(_item_id(item), "selected_file_id")
            extension = _extension(item)
            if extension not in _MEDIA_EXTENSIONS:
                continue
            season, episode = _identity_numbers(item, file)
            hint = {
                "title": _required_text(file.get("title") or tmdb_id, "title"),
                "media_type": _media_type(file.get("media_type")),
                "tmdb_id": tmdb_id,
                "season_number": season,
                "episode_number": episode,
                "sidecar": extension in _SIDECAR_EXTENSIONS,
            }
            recognition_hints[selected_id] = hint
            source_file_ids.append(selected_id)
            if extension in _VIDEO_EXTENSIONS:
                self._ensure_media_info(
                    account_id=account_id,
                    event_key=event_identity,
                    item=item,
                    hint=hint,
                )
        if not source_file_ids:
            raise RuntimeError("MoviePilot transfer batch contains no supported media files")
        self._assert_credential_binding(account_id, backend, credential_fingerprint)

        plan_result = self._plans.create(
            {
                "source_cid": parent_id,
                "source_name": _required_text(file.get("title") or tmdb_id, "source_name"),
                "source_file_ids": source_file_ids,
                "request_intent_id": approved_intent,
                "recognition_hints": recognition_hints,
            }
        )
        plan = _ready_plan(plan_result.payload, plan_result.status_code)
        self._assert_credential_binding(account_id, backend, credential_fingerprint)
        reservation = self._executions.prepare(
            str(plan["plan_id"]),
            approved_intent,
            trace_id=f"moviepilot-webhook:{event_identity[:32]}",
        )
        return _execution_result(
            reservation.execution,
            event_type=event_type,
            file_id=file_id,
            tmdb_id=tmdb_id,
            reused=not reservation.created,
        )

    def _ensure_media_info(
        self,
        *,
        account_id: str,
        event_key: str,
        item: Mapping[str, Any],
        hint: Mapping[str, Any],
    ) -> None:
        sha1 = canonical_sha1(_sha1(item))
        pick_code = _required_text(_pick_code(item), "pick_code")
        file_name = _required_text(_item_name(item), "file_name")
        request = P115MediaProbeRequest(
            account_id=account_id,
            sha1=sha1,
            pick_code=pick_code,
            file_name=file_name,
            file_size=_size(item),
            context={
                "tmdb_id": hint["tmdb_id"],
                "type": hint["media_type"],
                "season_number": hint.get("season_number"),
                "episode_number": hint.get("episode_number"),
            },
        )
        result = self._media_info.enrich(
            request,
            operation_key=f"moviepilot:{event_key}:{_item_id(item)}",
            progress=object(),
        )
        if result.status is P115MediaProbeStatus.CONFIRMED:
            return
        if result.status is P115MediaProbeStatus.REJECTED:
            raise ValueError(f"MoviePilot media probe was rejected: {result.diagnostic}")
        if result.status is P115MediaProbeStatus.CANCELLED:
            raise RuntimeError("MoviePilot media probe was cancelled")
        raise RuntimeError(f"MoviePilot media probe is retryable: {result.diagnostic}")

    def _assert_credential_binding(
        self,
        account_id: str,
        backend: str,
        credential_fingerprint: str,
    ) -> None:
        current = self._credentials.load()
        current_account = str(current.account_id or "").strip()
        try:
            current_backend, current_fingerprint = _credential_binding(current, backend)
        except RuntimeError as exc:
            raise RuntimeError("MoviePilot transfer 115 credential changed during preparation") from exc
        if current_account != account_id or current_backend != backend or current_fingerprint != credential_fingerprint:
            raise RuntimeError("MoviePilot transfer 115 credential changed during preparation")


def _related_files(
    children: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any],
    event_file: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    target_season, target_episode = _identity_numbers(target, event_file)
    media_type = _media_type(event_file.get("media_type"))
    target_stem = _normalized_stem(_item_name(target))
    target_id = _item_id(target)
    selected: list[Mapping[str, Any]] = [target] if _extension(target) in _MEDIA_EXTENSIONS else []
    for item in children:
        if _extension(item) not in _MEDIA_EXTENSIONS:
            continue
        if target_id and _item_id(item) == target_id:
            continue
        if media_type == "tv" and target_episode is not None:
            season, episode = _identity_numbers(item, event_file)
            if episode != target_episode or season != target_season:
                continue
        elif not _stems_related(target_stem, _normalized_stem(_item_name(item))):
            continue
        selected.append(item)
    return tuple(selected)


def _identity_numbers(item: Mapping[str, Any], fallback: Mapping[str, Any]) -> tuple[int | None, int | None]:
    name = _item_name(item)
    match = _SEASON_EPISODE.search(name)
    if match:
        return int(match.group(1)), int(match.group(2))
    episode_match = _EPISODE.search(name)
    season = _optional_int(fallback.get("season_number"))
    episode = int(episode_match.group(1)) if episode_match else _optional_int(fallback.get("episode_number"))
    return season, episode


def _ready_plan(payload: object, status_code: int) -> Mapping[str, Any]:
    value: Mapping[str, Any] = payload if isinstance(payload, Mapping) else {}
    raw_data = value.get("data")
    data: Mapping[str, Any] = raw_data if isinstance(raw_data, Mapping) else {}
    if status_code >= 400 or value.get("success") is not True or data.get("status") != "ready":
        raw_errors = data.get("errors")
        errors = raw_errors if isinstance(raw_errors, list) else []
        codes = [str(item.get("code") or "") for item in errors if isinstance(item, Mapping)]
        raise RuntimeError(f"MoviePilot organize Plan is not ready: {codes or value.get('message') or status_code}")
    return data


def _execution_result(
    execution: Any,
    *,
    event_type: str,
    file_id: str,
    tmdb_id: str,
    reused: bool,
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "file_id": file_id,
        "tmdb_id": tmdb_id,
        "passthrough": False,
        "execution_id": str(execution.execution_id),
        "plan_id": str(execution.plan_id),
        "job_id": str(execution.job_id),
        "job_reused": reused,
        "reconciliation": "p115-organize-plan",
    }


def _payload_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    data = value.get("data")
    if isinstance(data, list):
        data = data[0] if data else {}
    return dict(data) if isinstance(data, Mapping) else dict(value)


def _item_id(value: Mapping[str, Any]) -> str:
    return str(value.get("fid") or value.get("file_id") or value.get("id") or "").strip()


def _item_name(value: Mapping[str, Any]) -> str:
    return str(value.get("fn") or value.get("file_name") or value.get("name") or value.get("n") or "").strip()


def _sha1(value: Mapping[str, Any]) -> str:
    return str(value.get("sha1") or value.get("sha") or value.get("file_sha1") or "").strip().upper()


def _pick_code(value: Mapping[str, Any]) -> str:
    return str(value.get("pc") or value.get("pick_code") or value.get("pickcode") or "").strip()


def _size(value: Mapping[str, Any]) -> int:
    raw = value.get("fs") or value.get("size") or value.get("file_size") or 0
    if isinstance(raw, bool):
        return 0
    try:
        return max(int(raw), 0)
    except (TypeError, ValueError):
        return 0


def _extension(value: Mapping[str, Any]) -> str:
    return PurePosixPath(_item_name(value)).suffix.casefold().lstrip(".")


def _media_type(value: object) -> str:
    normalized = str(value or "").strip().casefold()
    if normalized in {"tv", "series", "电视剧", "剧集"}:
        return "tv"
    if normalized in {"movie", "电影"}:
        return "movie"
    raise ValueError("MoviePilot transfer media_type is invalid")


def _safe_component(value: object) -> str:
    normalized = str(value or "").strip().translate(_INVALID_NAME)
    if not normalized or normalized in {".", ".."}:
        raise ValueError("MoviePilot title is invalid")
    return normalized


def _safe_file_name(value: object) -> str:
    normalized = str(value or "").strip().replace("/", "／").replace("\\", "＼")
    if not normalized or normalized in {".", ".."}:
        raise ValueError("MoviePilot file name is invalid")
    return normalized.translate(_INVALID_NAME)


def _normalized_stem(value: str) -> str:
    stem = PurePosixPath(value).stem.casefold()
    stem = re.sub(r"(?i)[._ -]*(?:chs|cht|zho|chi|zh|eng|jpn|kor|sc|tc|zh-cn|zh-tw)$", "", stem)
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", stem)


def _stems_related(left: str, right: str) -> bool:
    return bool(left and right and left == right)


def freeze_moviepilot_credential_binding(
    snapshot: P115CredentialSnapshot,
    priority: str,
) -> dict[str, str]:
    account_id = str(snapshot.account_id or "").strip()
    if not account_id.isdigit():
        raise RuntimeError("MoviePilot transfer requires a stable 115 account identity")
    backend, fingerprint = _credential_binding(snapshot, priority)
    return {
        "account_id": account_id,
        "provider_backend": backend,
        "credential_fingerprint": fingerprint,
    }


def _credential_binding(snapshot: P115CredentialSnapshot, priority: str) -> tuple[str, str]:
    preferred = str(priority or "openapi").strip().casefold()
    order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
    backend = next(
        (
            candidate
            for candidate in order
            if str(snapshot.cookie if candidate == "cookie" else snapshot.access_token or "").strip()
        ),
        "",
    )
    if not backend:
        raise RuntimeError("MoviePilot transfer requires a usable 115 credential")
    credential = str(snapshot.cookie if backend == "cookie" else snapshot.access_token or "").strip()
    app_type = str(snapshot.app_type or "").strip() if backend == "cookie" else ""
    fingerprint = hashlib.sha256(f"{backend}\0{app_type}\0{credential}".encode()).hexdigest()
    return backend, fingerprint


def _optional_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"MoviePilot transfer requires {field}")
    return normalized


def _digest(value: object) -> str:
    normalized = _required_text(value, "event_key").lower()
    if len(normalized) != 64:
        raise ValueError("MoviePilot event key is invalid")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("MoviePilot event key is invalid") from exc
    return normalized


__all__ = [
    "ExplicitMoviePilotRecognitionGateway",
    "freeze_moviepilot_credential_binding",
    "MoviePilotCredentialStore",
    "MoviePilotOriginalNameGateway",
    "MoviePilotTransferRemoteClient",
    "MoviePilotTransferRemoteGateway",
    "NativeMoviePilotTransferProcessor",
]
