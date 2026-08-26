from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from .fingerprints import canonical_sha1
from .media_info_adapters import (
    BoundedRangeReader,
    ControlledProcessOutcome,
    ControlledProcessOutput,
    ControlledProcessProfile,
    ControlledProcessRunner,
    ControlledProcessStatus,
    P115DirectUrl,
    PreIdCandidates,
    preid_from_range,
    resolve_cached_preid,
)
from .media_info_formatter import (
    FormattedMediaInfo,
    P115MediaInfoFormatError,
    P115MediaInfoPolicySnapshot,
    format_raw_ffprobe_mediainfo,
    sanitize_raw_ffprobe,
)

_DEFAULT_USER_AGENT = "Mozilla/5.0"
_FFPROBE_TIMEOUT_SECONDS = 45.0
_FFPROBE_STDOUT_LIMIT = 8 * 1024 * 1024
_FFPROBE_STDERR_LIMIT = 256 * 1024
_PREID_BYTES = 128 * 1024
_MEDIA_PROBE_IMPLEMENTATION_REVISION = "p6-probe-v1"


class P115MediaProbeStatus(StrEnum):
    CONFIRMED = "confirmed"
    RETRYABLE = "retryable"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class P115MediaProbeRequest:
    account_id: str
    sha1: str
    pick_code: str
    file_name: str
    file_size: int = 0
    context: Mapping[str, Any] = field(default_factory=dict)
    preid_candidates: PreIdCandidates = field(default_factory=PreIdCandidates)

    def __post_init__(self) -> None:
        if not str(self.account_id or "").strip().isdigit():
            raise ValueError("media probe requires a stable numeric 115 account ID")
        canonical_sha1(self.sha1)
        if not str(self.pick_code or "").strip() or not str(self.file_name or "").strip():
            raise ValueError("media probe requires PickCode and file name")
        if isinstance(self.file_size, bool) or int(self.file_size) < 0:
            raise ValueError("media probe file size must be non-negative")
        if not isinstance(self.context, Mapping):
            raise ValueError("media probe context must be an object")


@dataclass(frozen=True, slots=True)
class P115MediaProbeResult:
    status: P115MediaProbeStatus
    diagnostic: str
    formatted: FormattedMediaInfo | None = field(default=None, repr=False)
    raw: Mapping[str, Any] | None = field(default=None, repr=False)
    provider_backend: str = ""
    probe_method: str = ""
    preid: str = ""
    warnings: tuple[str, ...] = ()

    @property
    def confirmed(self) -> bool:
        return self.status is P115MediaProbeStatus.CONFIRMED


class P115DirectUrlGateway(Protocol):
    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl | None: ...


class P115IsoProbeGateway(Protocol):
    def probe(
        self,
        request: P115MediaProbeRequest,
        direct: P115DirectUrl,
        *,
        cancel: Callable[[], bool] | None,
    ) -> Mapping[str, Any] | None: ...


class P115MediaInfoPolicyProvider(Protocol):
    def snapshot(self) -> P115MediaInfoPolicySnapshot: ...


RangeReaderFactory = Callable[[P115DirectUrl, Callable[[], bool] | None], BoundedRangeReader]


class P115MediaProbeService:
    """Run bounded ffprobe/ISO work and return a sanitized cache-ready result."""

    def __init__(
        self,
        *,
        direct_urls: P115DirectUrlGateway,
        processes: ControlledProcessRunner,
        iso: P115IsoProbeGateway,
        policies: P115MediaInfoPolicyProvider,
        range_reader_factory: RangeReaderFactory | None = None,
    ) -> None:
        self._direct_urls = direct_urls
        self._processes = processes
        self._iso = iso
        self._policies = policies
        self._range_reader_factory = range_reader_factory or _default_range_reader

    def probe(
        self,
        request: P115MediaProbeRequest,
        *,
        cancel: Callable[[], bool] | None = None,
        policy: P115MediaInfoPolicySnapshot | None = None,
    ) -> P115MediaProbeResult:
        if cancel is not None and cancel():
            return P115MediaProbeResult(P115MediaProbeStatus.CANCELLED, "media probe was cancelled before start")
        direct = self._direct_urls.resolve(
            request.pick_code,
            expected_account_id=request.account_id,
            user_agent=_DEFAULT_USER_AGENT,
        )
        if direct is None:
            return P115MediaProbeResult(P115MediaProbeStatus.RETRYABLE, "115 direct URL is unavailable")
        if cancel is not None and cancel():
            return P115MediaProbeResult(P115MediaProbeStatus.CANCELLED, "media probe was cancelled")

        file_name = str(request.file_name).strip()
        if file_name.casefold().endswith(".iso"):
            try:
                raw = self._iso.probe(request, direct, cancel=cancel)
            except Exception:
                raw = None
            if raw is None:
                raw, process_outcome = self._run_ffprobe(request, direct, cancel=cancel)
                if raw is None:
                    return _process_failure(process_outcome, backend=direct.backend)
                probe_method = "ffprobe_iso_fallback"
            else:
                probe_method = "iso"
        else:
            raw, process_outcome = self._run_ffprobe(request, direct, cancel=cancel)
            if raw is None:
                return _process_failure(process_outcome, backend=direct.backend)
            probe_method = "ffprobe"

        if not _has_video_stream(raw):
            return P115MediaProbeResult(
                P115MediaProbeStatus.REJECTED,
                "media probe returned no video stream",
                provider_backend=direct.backend,
                probe_method=probe_method,
            )

        warnings: list[str] = []
        resolution = resolve_cached_preid(request.preid_candidates)
        preid = resolution.value if resolution is not None else ""
        if not preid:
            try:
                preid = preid_from_range(self._range_reader_factory(direct, cancel))
            except Exception:
                warnings.append("preid_unavailable")

        context = {
            key: value
            for key, value in dict(request.context).items()
            if key
            in {
                "tmdb_id",
                "type",
                "original_language",
                "season_number",
                "episode_number",
                "quality_source",
            }
            and value not in (None, "", [], {})
        }
        context["sha1"] = canonical_sha1(request.sha1)
        if preid:
            context["preid"] = canonical_sha1(preid)
        detached_raw = dict(raw)
        detached_raw["_etk"] = context
        try:
            sanitized = sanitize_raw_ffprobe(detached_raw)
            formatted = format_raw_ffprobe_mediainfo(
                sanitized,
                file_name=file_name,
                file_size=request.file_size,
                sha1=request.sha1,
                policy=policy or self._policies.snapshot(),
            )
        except (P115MediaInfoFormatError, TypeError, ValueError):
            return P115MediaProbeResult(
                P115MediaProbeStatus.REJECTED,
                "media probe output could not be formatted",
                provider_backend=direct.backend,
                probe_method=probe_method,
                warnings=tuple(warnings),
            )
        return P115MediaProbeResult(
            P115MediaProbeStatus.CONFIRMED,
            "media probe completed",
            formatted=formatted,
            raw=sanitized,
            provider_backend=direct.backend,
            probe_method=probe_method,
            preid=preid,
            warnings=tuple(warnings),
        )

    def _run_ffprobe(
        self,
        request: P115MediaProbeRequest,
        direct: P115DirectUrl,
        *,
        cancel: Callable[[], bool] | None,
    ) -> tuple[Mapping[str, Any] | None, ControlledProcessOutcome]:
        arguments = [
            "-hide_banner",
            "-v",
            "error",
            "-user_agent",
            direct.user_agent,
            "-rw_timeout",
            "15000000",
            "-analyzeduration",
            "20000000",
            "-probesize",
            "20000000",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            "-show_chapters",
            direct.url,
        ]
        outcome = self._processes.run(
            ControlledProcessProfile.FFPROBE,
            arguments,
            max_stdin_bytes=0,
            max_stdout_bytes=_FFPROBE_STDOUT_LIMIT,
            max_stderr_bytes=_FFPROBE_STDERR_LIMIT,
            timeout_seconds=_FFPROBE_TIMEOUT_SECONDS,
            output=ControlledProcessOutput.JSON,
            cancel=cancel,
        )
        if outcome.status is ControlledProcessStatus.TIMED_OUT and request.file_name.casefold().endswith(
            (".mp4", ".m4v", ".mov")
        ):
            fast_arguments = list(arguments)
            fast_arguments[fast_arguments.index("-analyzeduration") + 1] = "0"
            fast_arguments[fast_arguments.index("-probesize") + 1] = "32"
            insert_at = fast_arguments.index("-print_format")
            fast_arguments[insert_at:insert_at] = ["-max_probe_packets", "1"]
            outcome = self._processes.run(
                ControlledProcessProfile.FFPROBE,
                fast_arguments,
                max_stdin_bytes=0,
                max_stdout_bytes=_FFPROBE_STDOUT_LIMIT,
                max_stderr_bytes=_FFPROBE_STDERR_LIMIT,
                timeout_seconds=_FFPROBE_TIMEOUT_SECONDS,
                output=ControlledProcessOutput.JSON,
                cancel=cancel,
            )
        raw = outcome.parsed_output
        return (dict(raw), outcome) if outcome.succeeded and isinstance(raw, Mapping) else (None, outcome)


def media_probe_operation_key(source: str, identity: str, sha1: str) -> str:
    payload = f"{source.strip()}\0{identity.strip()}\0{canonical_sha1(sha1)}".encode()
    return hashlib.sha256(payload).hexdigest()


def media_info_formatter_revision(policy: P115MediaInfoPolicySnapshot) -> str:
    payload = f"p6-v1\0{policy.language_mapping_revision}\0{policy.default_stream_config_revision}".encode()
    return hashlib.sha256(payload).hexdigest()


def media_probe_provenance_key(formatter_revision: str) -> str:
    normalized_formatter_revision = str(formatter_revision or "").strip()
    if not normalized_formatter_revision or len(normalized_formatter_revision) > 64:
        raise ValueError("MediaInfo formatter revision is invalid")
    payload = f"{_MEDIA_PROBE_IMPLEMENTATION_REVISION}\0{normalized_formatter_revision}".encode()
    return hashlib.sha256(payload).hexdigest()


def media_probe_result_revision(result: P115MediaProbeResult) -> str:
    method = str(result.probe_method or "ffprobe").strip().lower().replace("_", "-")
    return f"p6-{method}-v1"[:64]


def _process_failure(
    outcome: ControlledProcessOutcome,
    *,
    backend: str,
) -> P115MediaProbeResult:
    if outcome.status is ControlledProcessStatus.CANCELLED:
        status = P115MediaProbeStatus.CANCELLED
    elif outcome.status in {
        ControlledProcessStatus.TIMED_OUT,
        ControlledProcessStatus.NONZERO,
        ControlledProcessStatus.START_FAILED,
    }:
        status = P115MediaProbeStatus.RETRYABLE
    else:
        status = P115MediaProbeStatus.REJECTED
    return P115MediaProbeResult(
        status,
        outcome.diagnostic,
        provider_backend=backend,
        probe_method="ffprobe",
    )


def _has_video_stream(raw: Mapping[str, Any]) -> bool:
    streams = raw.get("streams")
    return isinstance(streams, list) and any(
        isinstance(stream, Mapping) and str(stream.get("codec_type") or "").casefold() == "video" for stream in streams
    )


def _default_range_reader(
    direct: P115DirectUrl,
    cancel: Callable[[], bool] | None,
) -> BoundedRangeReader:
    return BoundedRangeReader(
        direct.url,
        headers={"User-Agent": direct.user_agent, "Accept": "*/*", "Connection": "close"},
        max_requests=4,
        max_bytes=_PREID_BYTES,
        deadline_seconds=45,
        request_timeout_seconds=45,
        max_redirects=3,
        cancel=cancel,
    )


__all__ = [
    "P115DirectUrl",
    "P115DirectUrlGateway",
    "P115IsoProbeGateway",
    "P115MediaInfoPolicyProvider",
    "P115MediaProbeRequest",
    "P115MediaProbeResult",
    "P115MediaProbeService",
    "P115MediaProbeStatus",
    "media_info_formatter_revision",
    "media_probe_operation_key",
    "media_probe_provenance_key",
    "media_probe_result_revision",
]
