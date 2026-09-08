from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

import httpx

from etk.domains.media.editing import MediaCommandOutcome, MediaCommandStatus
from etk.domains.media.models import MediaInfoEditContext
from etk.platform import Settings
from etk.platform.tasks import TaskOutcome

from .auth_directory import P115CredentialStore
from .directory_sync import P115TaskProgress
from .media_info import (
    P115MediaInfoCacheRecord,
    P115MediaInfoRestoreTarget,
    P115MediaProbeItemStatus,
    P115MediaProbeReservation,
    P115MediaProbeTarget,
)
from .media_info_adapters import PreIdCandidates
from .media_info_formatter import (
    FormattedMediaInfo,
    P115MediaInfoFormatError,
    P115MediaInfoPolicySnapshot,
    format_raw_ffprobe_mediainfo,
    normalize_legacy_formatted_mediainfo,
    sanitize_raw_ffprobe,
)
from .media_probe import (
    P115MediaProbeRequest,
    P115MediaProbeResult,
    P115MediaProbeStatus,
    media_info_formatter_revision,
    media_probe_operation_key,
    media_probe_provenance_key,
    media_probe_result_revision,
)

_EMBY_BATCH_SIZE = 100
_EMBY_FIELDS = "MediaSources,MediaStreams"
_TEXT_SUBTITLE_CODECS = frozenset({"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text"})


class P115MediaInfoRestoreError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class EmbyMediaInfoInspection:
    requested_ids: tuple[str, ...]
    returned_ids: frozenset[str]
    missing_video_ids: frozenset[str]
    failed_ids: frozenset[str]

    @property
    def unreturned_ids(self) -> frozenset[str]:
        return frozenset(self.requested_ids) - self.returned_ids


class P115EmbyMediaInfoQueryGateway(Protocol):
    def inspect(self, item_ids: Sequence[str]) -> EmbyMediaInfoInspection: ...


class P115MediaInfoRestoreRepository(Protocol):
    def list_restore_targets(self, *, account_id: str) -> tuple[P115MediaInfoRestoreTarget, ...]: ...

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

    def finish_probe(
        self,
        reservation: P115MediaProbeReservation,
        *,
        status: P115MediaProbeItemStatus,
        provider_backend: str = "",
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> None: ...


class P115FingerprintRepairUseCase(Protocol):
    def execute(self, progress: P115TaskProgress) -> TaskOutcome: ...


class P115MediaProbeUseCase(Protocol):
    def probe(
        self,
        request: P115MediaProbeRequest,
        *,
        cancel: Callable[[], bool] | None = None,
        policy: P115MediaInfoPolicySnapshot | None = None,
    ) -> P115MediaProbeResult: ...


class P115MediaInfoPolicyProvider(Protocol):
    def snapshot(self) -> P115MediaInfoPolicySnapshot: ...


class P115MediaInfoCommandGateway(Protocol):
    def apply_mediainfo(
        self,
        context: MediaInfoEditContext,
        mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
        *,
        operation_key: str | None = None,
        expected_cache_revision: int | None = None,
    ) -> MediaCommandOutcome: ...

    def apply_generated_mediainfo(
        self,
        context: MediaInfoEditContext,
        mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
        raw: Mapping[str, Any],
        *,
        probe_item_id: str,
        probe_claim_token: str,
        account_id: str,
        provider_backend: str,
        formatter_revision: str,
        probe_revision: str,
        probe_result: Mapping[str, Any] | None = None,
        raw_schema_version: int = 1,
    ) -> MediaCommandOutcome: ...

    def execute(self, effect_id: UUID) -> MediaCommandOutcome: ...


class HttpxEmbyMediaInfoQueryGateway:
    """Read Emby item stream state in bounded batches without treating omissions as missing data."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def inspect(self, item_ids: Sequence[str]) -> EmbyMediaInfoInspection:
        requested = tuple(dict.fromkeys(str(value or "").strip() for value in item_ids if str(value or "").strip()))
        if not requested:
            return EmbyMediaInfoInspection((), frozenset(), frozenset(), frozenset())
        settings = self._settings()
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        api_key = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise P115MediaInfoRestoreError("Emby query configuration is incomplete")

        returned: dict[str, bool] = {}
        failed: set[str] = set()
        with self._client_factory() as client:
            for start in range(0, len(requested), _EMBY_BATCH_SIZE):
                batch = requested[start : start + _EMBY_BATCH_SIZE]
                try:
                    response = client.get(
                        f"{base_url}/Items",
                        headers={"X-Emby-Token": api_key, "Accept": "application/json"},
                        params={"Ids": ",".join(batch), "Limit": str(len(batch)), "Fields": _EMBY_FIELDS},
                    )
                    if response.status_code != 200:
                        failed.update(batch)
                        continue
                    items = _emby_response_items(response)
                except (httpx.HTTPError, ValueError, TypeError):
                    failed.update(batch)
                    continue
                requested_batch = frozenset(batch)
                for item in items:
                    item_id = str(item.get("Id") or "").strip()
                    if not item_id or item_id not in requested_batch:
                        continue
                    returned[item_id] = returned.get(item_id, False) or _has_emby_video_dimensions(item)

        returned_ids = frozenset(returned)
        return EmbyMediaInfoInspection(
            requested,
            returned_ids,
            frozenset(item_id for item_id, has_dimensions in returned.items() if not has_dimensions),
            frozenset(item_id for item_id in failed if item_id not in returned_ids),
        )

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=max(1.0, float(settings.emby_api_timeout)),
        )


class P115MediaInfoRestoreService:
    """Restore Emby MediaInfo from cache, raw ffprobe, or one persistent online probe per asset."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        repository: P115MediaInfoRestoreRepository,
        fingerprints: P115FingerprintRepairUseCase,
        emby: P115EmbyMediaInfoQueryGateway,
        probe: P115MediaProbeUseCase,
        policies: P115MediaInfoPolicyProvider,
        commands: P115MediaInfoCommandGateway,
    ) -> None:
        self._credentials = credentials
        self._repository = repository
        self._fingerprints = fingerprints
        self._emby = emby
        self._probe = probe
        self._policies = policies
        self._commands = commands

    def execute(
        self,
        progress: P115TaskProgress,
        *,
        operation_key: str,
        force_full_update: bool = False,
        emby_item_ids: Sequence[str] | None = None,
    ) -> TaskOutcome:
        normalized_operation = _operation_key(operation_key)
        mode = "full" if force_full_update else "incremental"
        stats = _restore_stats(mode)
        progress.update(0, "正在补齐 115 指纹并准备媒体信息还原...")

        initial_account = self._account_id()
        if not initial_account:
            return _failed(progress, "无法确认稳定的 115 账号身份，媒体信息未修改。", stats)
        fingerprint_outcome = self._fingerprints.execute(_SubProgress(progress, ceiling=8))
        stats["fingerprint_status"] = fingerprint_outcome.status.value
        stats["fingerprint_failed_assets"] = _detail_int(fingerprint_outcome.details, "failed_assets")
        if self._account_id() != initial_account:
            stats["account_drift"] = True
            return _failed(progress, "115 账号在指纹补齐期间发生变化，媒体信息未继续处理。", stats)

        targets = self._repository.list_restore_targets(account_id=initial_account)
        requested_ids = frozenset(
            str(value or "").strip() for value in (emby_item_ids or ()) if str(value or "").strip()
        )
        if emby_item_ids is not None:
            targets = tuple(target for target in targets if target.emby_item_id in requested_ids)
        stats["restore_targets"] = len(targets)
        stats["unresolved_after_fingerprint_repair"] = _detail_int(
            fingerprint_outcome.details,
            "failed_assets",
        )
        if not targets:
            message = "没有可注入媒体信息的在库项目。"
            if stats["unresolved_after_fingerprint_repair"]:
                return _failed(progress, "没有身份完整的媒体版本可供还原。", stats)
            progress.update(100, message)
            return TaskOutcome.skipped(message, stats)

        selected = targets
        if not force_full_update:
            progress.update(9, "正在检查 Emby 中缺失媒体信息的实际版本...")
            try:
                inspection = self._emby.inspect(tuple(target.emby_item_id for target in targets))
            except P115MediaInfoRestoreError as exc:
                stats["emby_query_failed"] = len(targets)
                return _failed(progress, str(exc), stats)
            selected = tuple(target for target in targets if target.emby_item_id in inspection.missing_video_ids)
            stats["emby_returned"] = len(inspection.returned_ids)
            stats["emby_query_failed"] = len(inspection.failed_ids)
            stats["emby_unreturned"] = len(inspection.unreturned_ids - inspection.failed_ids)
            stats["emby_already_complete"] = len(inspection.returned_ids - inspection.missing_video_ids)

        stats["selected"] = len(selected)
        if not selected:
            message = "Emby 中没有缺失媒体信息的实际版本。"
            if stats["emby_query_failed"]:
                return _failed(progress, "Emby 媒体信息检查失败，未把未返回项目误判为缺失。", stats)
            progress.update(100, message)
            return TaskOutcome.skipped(message, stats)

        try:
            policy = self._policies.snapshot()
        except (P115MediaInfoFormatError, TypeError, ValueError) as exc:
            return _failed(progress, f"媒体信息格式策略无效：{exc}", stats)
        formatter_revision = media_info_formatter_revision(policy)

        def cancel() -> bool:
            return _progress_cancel_requested(progress)

        for index, target in enumerate(selected):
            progress.checkpoint()
            if self._account_id() != initial_account:
                stats["account_drift"] = True
                stats["failed"] += len(selected) - index
                break
            progress.update(
                10 + int(index / max(len(selected), 1) * 88),
                f"正在还原媒体信息 ({index + 1}/{len(selected)})...",
            )
            try:
                self._restore_one(
                    target,
                    account_id=initial_account,
                    task_operation_key=normalized_operation,
                    force_full_update=force_full_update,
                    policy=policy,
                    formatter_revision=formatter_revision,
                    cancel=cancel,
                    progress=progress,
                    stats=stats,
                )
            except Exception:
                progress.checkpoint()
                stats["failed"] += 1

        message = (
            f"媒体信息还原完成：成功 {stats['restored']}，失败 {stats['failed']}，"
            f"缓存命中 {stats['cache_hits']}，RAW 重建 {stats['raw_reformatted']}，在线探测 {stats['online_probed']}。"
        )
        errors = (
            int(stats["failed"]) + int(stats["emby_query_failed"]) + int(stats["unresolved_after_fingerprint_repair"])
        )
        if errors:
            progress.update(-1, message)
            if stats["restored"]:
                return TaskOutcome.partial(message, stats)
            return TaskOutcome.failed(message, stats)
        progress.update(100, message)
        return TaskOutcome.succeeded(message, stats)

    def _restore_one(
        self,
        target: P115MediaInfoRestoreTarget,
        *,
        account_id: str,
        task_operation_key: str,
        force_full_update: bool,
        policy: P115MediaInfoPolicySnapshot,
        formatter_revision: str,
        cancel: Callable[[], bool],
        progress: P115TaskProgress,
        stats: dict[str, Any],
    ) -> None:
        cache = self._repository.find_by_sha1(target.sha1)
        cache_revision_before_reservation = cache.cache_revision if cache is not None else None
        item_operation = media_probe_operation_key(
            "restore_mediainfo",
            f"{task_operation_key}:{target.emby_item_id}",
            target.sha1,
        )
        if not force_full_update and cache is not None and cache.formatted is not None:
            try:
                formatted = normalize_legacy_formatted_mediainfo(cache.formatted)
            except (P115MediaInfoFormatError, TypeError, ValueError, KeyError):
                stats["invalid_formatted_cache"] += 1
            else:
                outcome = self._apply_cached(
                    target,
                    formatted,
                    cache_revision=cache.cache_revision,
                    operation_key=item_operation,
                    progress=progress,
                )
                stats["cache_hits"] += 1
                _record_command_outcome(outcome, stats)
                return

        raw_formatted: tuple[FormattedMediaInfo, Mapping[str, Any]] | None = None
        if cache is not None and cache.raw is not None and not _has_unidentified_text_subtitle(cache.raw):
            try:
                raw_formatted = _format_cached_raw(target, cache.raw, policy)
            except (P115MediaInfoFormatError, TypeError, ValueError, KeyError):
                stats["invalid_raw_cache"] += 1
        elif cache is not None and cache.raw is not None:
            stats["stale_raw_subtitle"] += 1

        reservation = self._repository.reserve_probe(
            operation_key=item_operation,
            provenance_key=media_probe_provenance_key(formatter_revision),
            source="restore_mediainfo",
            account_id=account_id,
            target=target,
        )
        replay = self._resume_terminal(reservation, progress=progress, stats=stats)
        if replay:
            return
        if not reservation.claimed:
            stats["active_probe_claims"] += 1
            stats["failed"] += 1
            return
        if reservation.cache_revision != cache_revision_before_reservation:
            self._repository.finish_probe(
                reservation,
                status=P115MediaProbeItemStatus.RETRYABLE,
                error={"code": "cache_revision_changed_before_restore_decision"},
            )
            stats["failed"] += 1
            stats["retryable_probes"] += 1
            return

        if raw_formatted is not None:
            formatted, sanitized_raw = raw_formatted
            outcome = self._apply_generated(
                target,
                formatted,
                sanitized_raw,
                reservation=reservation,
                account_id=account_id,
                provider_backend="raw",
                formatter_revision=formatter_revision,
                probe_revision="raw-cache-v1",
                probe_result={"probe_method": "raw_reformat"},
                progress=progress,
            )
            stats["raw_reformatted"] += 1
            _record_command_outcome(outcome, stats)
            return

        stats["online_probe_attempts"] += 1
        result = self._probe.probe(
            P115MediaProbeRequest(
                account_id=account_id,
                sha1=target.sha1,
                pick_code=target.pick_code,
                file_name=target.file_name,
                file_size=target.file_size,
                context=target.probe_context,
                preid_candidates=PreIdCandidates(cached_raw=cache.raw if cache is not None else None),
            ),
            cancel=cancel,
            policy=policy,
        )
        if result.status is not P115MediaProbeStatus.CONFIRMED or result.formatted is None or result.raw is None:
            self._finish_failed_probe(reservation, result)
            stats["failed"] += 1
            if result.status is P115MediaProbeStatus.CANCELLED:
                stats["cancelled"] += 1
                progress.checkpoint()
            elif result.status is P115MediaProbeStatus.RETRYABLE:
                stats["retryable_probes"] += 1
            else:
                stats["rejected_probes"] += 1
            return

        if self._account_id() != account_id:
            self._repository.finish_probe(
                reservation,
                status=P115MediaProbeItemStatus.RETRYABLE,
                provider_backend=result.provider_backend,
                error={"code": "account_changed"},
            )
            stats["account_drift"] = True
            stats["failed"] += 1
            return
        outcome = self._apply_generated(
            target,
            result.formatted,
            result.raw,
            reservation=reservation,
            account_id=account_id,
            provider_backend=result.provider_backend,
            formatter_revision=formatter_revision,
            probe_revision=media_probe_result_revision(result),
            probe_result={
                "probe_method": result.probe_method,
                "preid": result.preid,
                "warnings": list(result.warnings),
            },
            progress=progress,
        )
        stats["online_probed"] += 1
        _record_command_outcome(outcome, stats)

    def _apply_cached(
        self,
        target: P115MediaInfoRestoreTarget,
        formatted: FormattedMediaInfo,
        *,
        cache_revision: int,
        operation_key: str,
        progress: P115TaskProgress,
    ) -> MediaCommandOutcome:
        label = f"emby.mediainfo.apply:{target.emby_item_id}"
        _record_progress_effect(progress, "record_started_effect", label)
        try:
            outcome = self._commands.apply_mediainfo(
                _edit_context(target, formatted),
                tuple(formatted),
                operation_key=operation_key,
                expected_cache_revision=cache_revision,
            )
        except Exception:
            _record_progress_effect(progress, "record_abandoned_effect", label)
            raise
        _finish_progress_effect(progress, label, outcome)
        return outcome

    def _apply_generated(
        self,
        target: P115MediaInfoRestoreTarget,
        formatted: FormattedMediaInfo,
        raw: Mapping[str, Any],
        *,
        reservation: P115MediaProbeReservation,
        account_id: str,
        provider_backend: str,
        formatter_revision: str,
        probe_revision: str,
        probe_result: Mapping[str, Any],
        progress: P115TaskProgress,
    ) -> MediaCommandOutcome:
        label = f"emby.mediainfo.apply:{target.emby_item_id}"
        _record_progress_effect(progress, "record_started_effect", label)
        try:
            outcome = self._commands.apply_generated_mediainfo(
                _edit_context(target, formatted),
                tuple(formatted),
                raw,
                probe_item_id=reservation.id,
                probe_claim_token=reservation.claim_token,
                account_id=account_id,
                provider_backend=provider_backend,
                formatter_revision=formatter_revision,
                probe_revision=probe_revision,
                probe_result=probe_result,
            )
        except Exception:
            _record_progress_effect(progress, "record_abandoned_effect", label)
            raise
        _finish_progress_effect(progress, label, outcome)
        return outcome

    def _resume_terminal(
        self,
        reservation: P115MediaProbeReservation,
        *,
        progress: P115TaskProgress,
        stats: dict[str, Any],
    ) -> bool:
        if reservation.status is P115MediaProbeItemStatus.REJECTED:
            stats["rejected_probes"] += 1
            stats["failed"] += 1
            return True
        if reservation.status is P115MediaProbeItemStatus.CANCELLED:
            stats["cancelled"] += 1
            stats["failed"] += 1
            progress.checkpoint()
            return True
        if reservation.status is not P115MediaProbeItemStatus.CONFIRMED:
            return False
        effect_id = str((reservation.result or {}).get("effect_id") or "").strip()
        try:
            parsed_effect_id = UUID(effect_id)
        except ValueError:
            stats["failed"] += 1
            return True
        label = f"emby.mediainfo.apply:{effect_id}"
        _record_progress_effect(progress, "record_started_effect", label)
        try:
            outcome = self._commands.execute(parsed_effect_id)
        except Exception:
            _record_progress_effect(progress, "record_abandoned_effect", label)
            raise
        _finish_progress_effect(progress, label, outcome)
        stats["resumed_effects"] += 1
        _record_command_outcome(outcome, stats)
        return True

    def _finish_failed_probe(
        self,
        reservation: P115MediaProbeReservation,
        result: P115MediaProbeResult,
    ) -> None:
        status = {
            P115MediaProbeStatus.RETRYABLE: P115MediaProbeItemStatus.RETRYABLE,
            P115MediaProbeStatus.REJECTED: P115MediaProbeItemStatus.REJECTED,
            P115MediaProbeStatus.CANCELLED: P115MediaProbeItemStatus.CANCELLED,
        }.get(result.status, P115MediaProbeItemStatus.REJECTED)
        self._repository.finish_probe(
            reservation,
            status=status,
            provider_backend=result.provider_backend,
            result={"probe_method": result.probe_method, "warnings": list(result.warnings)},
            error={"code": result.status.value, "diagnostic": result.diagnostic[:500]},
        )

    def _account_id(self) -> str:
        value = str(self._credentials.load().account_id or "").strip()
        return value if value.isdigit() else ""


@dataclass(slots=True)
class _SubProgress:
    parent: P115TaskProgress
    ceiling: int

    def update(self, progress: int, message: str) -> None:
        bounded = 0 if progress < 0 else max(0, min(100, int(progress)))
        self.parent.update(int(bounded / 100 * self.ceiling), message)

    def checkpoint(self) -> None:
        self.parent.checkpoint()


def _emby_response_items(response: httpx.Response) -> tuple[Mapping[str, Any], ...]:
    payload = response.json()
    if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
        raise ValueError("Emby Items response is invalid")
    items = payload["Items"]
    if any(not isinstance(item, Mapping) for item in items):
        raise ValueError("Emby Items response contains an invalid entry")
    return tuple(dict(item) for item in items)


def _has_emby_video_dimensions(item: Mapping[str, Any]) -> bool:
    streams: list[Any] = []
    root_streams = item.get("MediaStreams")
    if isinstance(root_streams, list):
        streams.extend(root_streams)
    sources = item.get("MediaSources")
    if isinstance(sources, list):
        for source in sources:
            if isinstance(source, Mapping) and isinstance(source.get("MediaStreams"), list):
                streams.extend(source["MediaStreams"])
    return any(
        isinstance(stream, Mapping)
        and str(stream.get("Type") or "").casefold() == "video"
        and bool(stream.get("Width") or stream.get("Height"))
        for stream in streams
    )


def _format_cached_raw(
    target: P115MediaInfoRestoreTarget,
    raw: Mapping[str, Any],
    policy: P115MediaInfoPolicySnapshot,
) -> tuple[FormattedMediaInfo, Mapping[str, Any]]:
    detached = dict(raw)
    raw_context = detached.get("_etk")
    context: dict[str, Any] = dict(raw_context) if isinstance(raw_context, Mapping) else {}
    context.update(target.probe_context)
    context["sha1"] = target.sha1
    detached["_etk"] = context
    sanitized = sanitize_raw_ffprobe(detached)
    formatted = format_raw_ffprobe_mediainfo(
        sanitized,
        file_name=target.file_name,
        file_size=target.file_size,
        sha1=target.sha1,
        policy=policy,
    )
    return formatted, sanitized


def _has_unidentified_text_subtitle(raw: Mapping[str, Any]) -> bool:
    streams = raw.get("streams")
    if not isinstance(streams, list):
        return False
    for stream in streams:
        if not isinstance(stream, Mapping):
            continue
        if str(stream.get("codec_type") or "").casefold() != "subtitle":
            continue
        if str(stream.get("codec_name") or "").casefold() not in _TEXT_SUBTITLE_CODECS:
            continue
        tags = stream.get("tags")
        language = str(tags.get("language") or "").casefold() if isinstance(tags, Mapping) else ""
        if language in {"", "und"}:
            return True
    return False


def _edit_context(target: P115MediaInfoRestoreTarget, formatted: FormattedMediaInfo) -> MediaInfoEditContext:
    return MediaInfoEditContext(
        item_id=target.emby_item_id,
        sha1=target.sha1,
        mediainfo=tuple(formatted),
        title=target.title,
        item_type=target.item_type,
        parent_series_tmdb_id=target.parent_series_tmdb_id,
        season_number=target.season_number,
        episode_number=target.episode_number,
    )


def _operation_key(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 255:
        raise ValueError("MediaInfo restore operation key must be 1-255 characters")
    return normalized


def _progress_cancel_requested(progress: P115TaskProgress) -> bool:
    reader = getattr(progress, "control_state", None)
    if not callable(reader):
        return False
    state = reader()
    return bool(isinstance(state, Mapping) and (state.get("cancel_requested") or state.get("timed_out")))


def _record_progress_effect(progress: P115TaskProgress, method_name: str, label: str) -> None:
    method = getattr(progress, method_name, None)
    if callable(method):
        method(label)


def _finish_progress_effect(progress: P115TaskProgress, label: str, outcome: MediaCommandOutcome) -> None:
    if outcome.status is MediaCommandStatus.CONFIRMED:
        _record_progress_effect(progress, "record_committed_effect", label)
    elif outcome.status is not MediaCommandStatus.INDETERMINATE:
        _record_progress_effect(progress, "record_abandoned_effect", label)


def _record_command_outcome(outcome: MediaCommandOutcome, stats: dict[str, Any]) -> None:
    if outcome.status is MediaCommandStatus.CONFIRMED:
        stats["restored"] += 1
    elif outcome.status is MediaCommandStatus.INDETERMINATE:
        stats["indeterminate_effects"] += 1
        stats["failed"] += 1
    elif outcome.status is MediaCommandStatus.PENDING:
        stats["pending_effects"] += 1
        stats["failed"] += 1
    else:
        stats["rejected_effects"] += 1
        stats["failed"] += 1


def _detail_int(details: Mapping[str, Any], field: str) -> int:
    value = details.get(field)
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return 0


def _restore_stats(mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "fingerprint_status": "",
        "fingerprint_failed_assets": 0,
        "restore_targets": 0,
        "unresolved_after_fingerprint_repair": 0,
        "selected": 0,
        "emby_returned": 0,
        "emby_unreturned": 0,
        "emby_query_failed": 0,
        "emby_already_complete": 0,
        "cache_hits": 0,
        "invalid_formatted_cache": 0,
        "raw_reformatted": 0,
        "invalid_raw_cache": 0,
        "stale_raw_subtitle": 0,
        "online_probed": 0,
        "online_probe_attempts": 0,
        "retryable_probes": 0,
        "rejected_probes": 0,
        "cancelled": 0,
        "active_probe_claims": 0,
        "resumed_effects": 0,
        "restored": 0,
        "failed": 0,
        "rejected_effects": 0,
        "indeterminate_effects": 0,
        "pending_effects": 0,
        "account_drift": False,
    }


def _failed(progress: P115TaskProgress, message: str, stats: Mapping[str, Any]) -> TaskOutcome:
    progress.update(-1, message)
    return TaskOutcome.failed(message, stats)


__all__ = [
    "EmbyMediaInfoInspection",
    "HttpxEmbyMediaInfoQueryGateway",
    "P115EmbyMediaInfoQueryGateway",
    "P115MediaInfoRestoreError",
    "P115MediaInfoRestoreService",
]
