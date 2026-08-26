from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from .auth_directory import P115CredentialStore
from .fingerprints import P115FingerprintAccountChanged, P115FingerprintConflict, canonical_sha1
from .media_info import (
    P115MediaInfoCacheRecord,
    P115MediaProbeItemStatus,
    P115MediaProbeReservation,
    P115MediaProbeTarget,
)
from .media_info_formatter import (
    P115MediaInfoFormatError,
    P115MediaInfoPolicySnapshot,
    format_raw_ffprobe_mediainfo,
    normalize_legacy_formatted_mediainfo,
    sanitize_raw_ffprobe,
)
from .media_probe import (
    P115MediaInfoPolicyProvider,
    P115MediaProbeRequest,
    P115MediaProbeResult,
    P115MediaProbeStatus,
    media_info_formatter_revision,
    media_probe_provenance_key,
    media_probe_result_revision,
)
from .strm_sync import P115StrmMediaInfoEnrichmentResult


class P115StrmMediaInfoRepository(Protocol):
    def find_by_sha1(
        self,
        sha1: str,
        *,
        record_hit: bool = False,
    ) -> P115MediaInfoCacheRecord | None: ...

    def reserve_probe(
        self,
        *,
        operation_key: str,
        provenance_key: str,
        source: str,
        account_id: str,
        target: P115MediaProbeTarget,
        lease_seconds: int = 120,
    ) -> P115MediaProbeReservation: ...

    def confirm_probe(
        self,
        reservation: P115MediaProbeReservation,
        *,
        account_id: str,
        sha1: str,
        pick_code: str,
        provider_backend: str,
        result: Mapping[str, Any] | None = None,
        formatted: Sequence[Mapping[str, Any]] | None = None,
        raw: Mapping[str, Any] | None = None,
        formatter_revision: str = "",
        probe_revision: str = "",
        raw_schema_version: int = 1,
    ) -> int: ...

    def finish_probe(
        self,
        reservation: P115MediaProbeReservation,
        *,
        status: P115MediaProbeItemStatus,
        provider_backend: str = "",
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> None: ...


class P115MediaProbeUseCase(Protocol):
    def probe(
        self,
        request: P115MediaProbeRequest,
        *,
        cancel: Callable[[], bool] | None = None,
        policy: P115MediaInfoPolicySnapshot | None = None,
    ) -> P115MediaProbeResult: ...


class P115StrmMediaInfoEnrichmentService:
    """Persist one STRM MediaInfo result before local projection effects may start."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        repository: P115StrmMediaInfoRepository,
        probe: P115MediaProbeUseCase,
        policies: P115MediaInfoPolicyProvider,
    ) -> None:
        self._credentials = credentials
        self._repository = repository
        self._probe = probe
        self._policies = policies

    def enrich(
        self,
        request: P115MediaProbeRequest,
        *,
        operation_key: str,
        progress: Any,
    ) -> P115StrmMediaInfoEnrichmentResult:
        _checkpoint(progress)
        operation = str(operation_key or "").strip()
        if not operation or len(operation) > 255:
            raise ValueError("STRM MediaInfo operation key is invalid")
        if str(self._credentials.load().account_id or "").strip() != request.account_id:
            raise P115FingerprintAccountChanged("115 account changed before STRM MediaInfo enrichment")

        policy: P115MediaInfoPolicySnapshot | None
        policy_error: Exception | None = None
        try:
            policy = self._policies.snapshot()
        except (P115MediaInfoFormatError, TypeError, ValueError) as exc:
            policy = None
            policy_error = exc
            provenance_key = media_probe_provenance_key("invalid-policy-v1")
            formatter_revision = ""
        else:
            formatter_revision = media_info_formatter_revision(policy)
            provenance_key = media_probe_provenance_key(formatter_revision)

        reservation = self._repository.reserve_probe(
            operation_key=operation,
            provenance_key=provenance_key,
            source="strm_sync",
            account_id=request.account_id,
            target=request,
        )
        if not reservation.claimed:
            return _reservation_replay(reservation)

        if policy_error is not None:
            return self._finish_failure(
                reservation,
                P115MediaProbeStatus.REJECTED,
                diagnostic="MediaInfo formatter policy is invalid",
                error_type=type(policy_error).__name__,
            )
        if policy is None:
            raise RuntimeError("MediaInfo formatter policy was not loaded")

        cache = self._repository.find_by_sha1(request.sha1)
        if cache is not None and cache.cache_revision != reservation.cache_revision:
            return self._finish_failure(
                reservation,
                P115MediaProbeStatus.RETRYABLE,
                diagnostic="MediaInfo cache changed before enrichment",
                error_type="cache_revision_changed",
            )

        if cache is not None and cache.formatted is not None and cache.formatter_revision == formatter_revision:
            try:
                normalize_legacy_formatted_mediainfo(cache.formatted)
            except (P115MediaInfoFormatError, TypeError, ValueError, KeyError):
                pass
            else:
                try:
                    self._repository.confirm_probe(
                        reservation,
                        account_id=request.account_id,
                        sha1=request.sha1,
                        pick_code=request.pick_code,
                        provider_backend="cache",
                        result={
                            "cache_reused": True,
                            "diagnostic": "formatted MediaInfo cache reused",
                            "provider_backend": "cache",
                            "probe_method": "cache",
                        },
                    )
                except (P115FingerprintAccountChanged, P115FingerprintConflict, RuntimeError, ValueError) as exc:
                    return self._confirmation_failure(reservation, exc)
                return P115StrmMediaInfoEnrichmentResult(
                    P115MediaProbeStatus.CONFIRMED,
                    "formatted MediaInfo cache reused",
                    provider_backend="cache",
                    cache_reused=True,
                )

        if cache is not None and cache.raw is not None:
            try:
                raw, formatted = _format_cached_raw(request, cache.raw, policy)
            except (P115MediaInfoFormatError, TypeError, ValueError, KeyError):
                pass
            else:
                try:
                    self._repository.confirm_probe(
                        reservation,
                        account_id=request.account_id,
                        sha1=request.sha1,
                        pick_code=request.pick_code,
                        provider_backend="raw",
                        result={
                            "cache_reused": False,
                            "diagnostic": "RAW MediaInfo cache reformatted",
                            "provider_backend": "raw",
                            "probe_method": "raw",
                        },
                        formatted=formatted,
                        raw=raw,
                        formatter_revision=formatter_revision,
                        probe_revision=cache.probe_revision,
                        raw_schema_version=cache.raw_schema_version,
                    )
                except (P115FingerprintAccountChanged, P115FingerprintConflict, RuntimeError, ValueError) as exc:
                    return self._confirmation_failure(reservation, exc)
                return P115StrmMediaInfoEnrichmentResult(
                    P115MediaProbeStatus.CONFIRMED,
                    "RAW MediaInfo cache reformatted",
                    provider_backend="raw",
                )

        probe_result = self._probe.probe(
            request,
            cancel=lambda: _cancel_requested(progress),
            policy=policy,
        )
        if not probe_result.confirmed or probe_result.formatted is None or probe_result.raw is None:
            status = (
                probe_result.status
                if probe_result.status is not P115MediaProbeStatus.CONFIRMED
                else P115MediaProbeStatus.REJECTED
            )
            return self._finish_failure(
                reservation,
                status,
                diagnostic=probe_result.diagnostic,
                error_type="probe_incomplete",
                provider_backend=probe_result.provider_backend,
                probe_method=probe_result.probe_method,
            )

        try:
            self._repository.confirm_probe(
                reservation,
                account_id=request.account_id,
                sha1=request.sha1,
                pick_code=request.pick_code,
                provider_backend=probe_result.provider_backend,
                result={
                    "cache_reused": False,
                    "diagnostic": probe_result.diagnostic,
                    "provider_backend": probe_result.provider_backend,
                    "probe_method": probe_result.probe_method,
                    "preid": probe_result.preid,
                    "warnings": list(probe_result.warnings),
                },
                formatted=probe_result.formatted,
                raw=probe_result.raw,
                formatter_revision=formatter_revision,
                probe_revision=media_probe_result_revision(probe_result),
            )
        except (P115FingerprintAccountChanged, P115FingerprintConflict, RuntimeError, ValueError) as exc:
            return self._confirmation_failure(reservation, exc)
        return P115StrmMediaInfoEnrichmentResult(
            P115MediaProbeStatus.CONFIRMED,
            probe_result.diagnostic,
            provider_backend=probe_result.provider_backend,
        )

    def _confirmation_failure(
        self,
        reservation: P115MediaProbeReservation,
        exc: Exception,
    ) -> P115StrmMediaInfoEnrichmentResult:
        return self._finish_failure(
            reservation,
            P115MediaProbeStatus.RETRYABLE,
            diagnostic="MediaInfo result could not be confirmed",
            error_type=type(exc).__name__,
        )

    def _finish_failure(
        self,
        reservation: P115MediaProbeReservation,
        status: P115MediaProbeStatus,
        *,
        diagnostic: str,
        error_type: str,
        provider_backend: str = "",
        probe_method: str = "",
    ) -> P115StrmMediaInfoEnrichmentResult:
        item_status = {
            P115MediaProbeStatus.RETRYABLE: P115MediaProbeItemStatus.RETRYABLE,
            P115MediaProbeStatus.REJECTED: P115MediaProbeItemStatus.REJECTED,
            P115MediaProbeStatus.CANCELLED: P115MediaProbeItemStatus.CANCELLED,
        }.get(status, P115MediaProbeItemStatus.REJECTED)
        safe_diagnostic = str(diagnostic or "MediaInfo enrichment failed")[:500]
        self._repository.finish_probe(
            reservation,
            status=item_status,
            provider_backend=provider_backend,
            result={"probe_method": str(probe_method or "")[:64]},
            error={"code": str(error_type or "probe_failed")[:64], "message": safe_diagnostic},
        )
        return P115StrmMediaInfoEnrichmentResult(
            status,
            safe_diagnostic,
            provider_backend=provider_backend,
        )


def _reservation_replay(reservation: P115MediaProbeReservation) -> P115StrmMediaInfoEnrichmentResult:
    result = dict(reservation.result or {})
    error = dict(reservation.error or {})
    if reservation.status is P115MediaProbeItemStatus.CONFIRMED:
        status = P115MediaProbeStatus.CONFIRMED
    elif reservation.status is P115MediaProbeItemStatus.REJECTED:
        status = P115MediaProbeStatus.REJECTED
    elif reservation.status is P115MediaProbeItemStatus.CANCELLED:
        status = P115MediaProbeStatus.CANCELLED
    else:
        status = P115MediaProbeStatus.RETRYABLE
    diagnostic = str(result.get("diagnostic") or error.get("message") or "MediaInfo probe is already active")[:500]
    return P115StrmMediaInfoEnrichmentResult(
        status,
        diagnostic,
        provider_backend=str(result.get("provider_backend") or "")[:32],
        cache_reused=bool(result.get("cache_reused")),
    )


def _format_cached_raw(
    request: P115MediaProbeRequest,
    cached_raw: Mapping[str, Any],
    policy: P115MediaInfoPolicySnapshot,
) -> tuple[Mapping[str, Any], list[dict[str, Any]]]:
    detached = dict(cached_raw)
    context = detached.get("_etk")
    raw_context: dict[str, Any] = dict(context) if isinstance(context, Mapping) else {}
    existing_sha1 = canonical_sha1(raw_context.get("sha1"), allow_empty=True)
    if existing_sha1 and existing_sha1 != request.sha1:
        raise P115FingerprintConflict("RAW MediaInfo cache belongs to another SHA1")
    raw_context.update(dict(request.context))
    raw_context["sha1"] = request.sha1
    detached["_etk"] = raw_context
    sanitized = sanitize_raw_ffprobe(detached)
    formatted = format_raw_ffprobe_mediainfo(
        sanitized,
        file_name=request.file_name,
        file_size=request.file_size,
        sha1=request.sha1,
        policy=policy,
    )
    return sanitized, formatted


def _checkpoint(progress: Any) -> None:
    checkpoint = getattr(progress, "checkpoint", None)
    if callable(checkpoint):
        checkpoint()


def _cancel_requested(progress: Any) -> bool:
    reader = getattr(progress, "control_state", None)
    if not callable(reader):
        return False
    state = reader()
    return bool(isinstance(state, Mapping) and (state.get("cancel_requested") or state.get("timed_out")))


__all__ = [
    "P115StrmMediaInfoEnrichmentService",
    "P115StrmMediaInfoRepository",
]
