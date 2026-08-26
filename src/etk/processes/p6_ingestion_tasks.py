from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from etk.compatibility.ingestion_monitor import PersistentIngestionIngress
from etk.platform import Settings
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome
from etk.platform.tasks.catalog import P6_INGESTION_NATIVE_TASK_KEYS


class IngestionScanProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class IngestionKnownPathSource(Protocol):
    def known_asset_paths(self) -> Iterable[str]: ...


class P6IngestionScanService:
    """Deterministically submit legacy-compatible catch-up candidates to durable intake."""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        ingress: PersistentIngestionIngress,
        known_paths: IngestionKnownPathSource,
    ) -> None:
        self._settings = settings
        self._ingress = ingress
        self._known_paths = known_paths

    def execute(self, progress: IngestionScanProgress) -> TaskOutcome:
        settings = self._settings()
        roots = sorted(
            {str(value or "").strip() for value in settings.monitor_paths if str(value or "").strip()},
            key=_path_key,
        )
        if not settings.monitor_enabled or not roots:
            return TaskOutcome.skipped(
                "实时监控未启用或未配置路径，跳过查漏扫描。",
                _details(scanned=0, submitted=0, skipped_known=0, failed=0),
            )

        extensions = _extensions(settings.monitor_extensions)
        exclusions = tuple(
            sorted(
                {str(value or "").strip() for value in settings.monitor_exclude_dirs if str(value or "").strip()},
                key=_path_key,
            )
        )
        maximum = max(0, int(settings.monitor_scan_max_tasks))
        batch_size = max(1, int(settings.monitor_scan_batch_size))
        config_revision = _config_revision(settings, roots, exclusions, extensions)
        known = {_path_key(path) for path in self._known_paths.known_asset_paths() if str(path or "").strip()}
        candidates: list[str] = []
        scanned = 0
        skipped_known = 0
        triggered_directories = 0
        missing_roots: list[str] = []

        progress.update(1, "正在确定性扫描监控目录...")
        for root in roots:
            progress.checkpoint()
            if not os.path.isdir(root):
                missing_roots.append(root)
                continue
            for directory, dirnames, filenames in os.walk(root):
                progress.checkpoint()
                dirnames[:] = sorted(dirnames, key=str.casefold)
                filenames = sorted(filenames, key=str.casefold)
                if _is_excluded(directory, exclusions):
                    dirnames[:] = []
                    continue
                unknown: list[str] = []
                for filename in filenames:
                    if filename.startswith(".") or Path(filename).suffix.casefold() not in extensions:
                        continue
                    path = os.path.join(directory, filename)
                    scanned += 1
                    if _path_key(path) in known:
                        skipped_known += 1
                        continue
                    unknown.append(path)
                if not unknown:
                    continue
                representative = unknown[0]
                if representative.casefold().endswith(".strm"):
                    selected = [path for path in unknown if path.casefold().endswith(".strm")]
                else:
                    selected = [representative]
                candidates.extend(selected)
                triggered_directories += 1
                if maximum and triggered_directories >= maximum:
                    break
            if maximum and triggered_directories >= maximum:
                break

        submitted = 0
        failures: list[dict[str, str]] = []
        total = len(candidates)
        for start in range(0, total, batch_size):
            progress.checkpoint()
            batch = candidates[start : start + batch_size]
            for path in batch:
                progress.checkpoint()
                source_event_key = hashlib.sha256(
                    f"scan-monitor-folders\0{config_revision}\0{_path_key(path)}".encode()
                ).hexdigest()
                try:
                    self._ingress.record_event(
                        "created",
                        path,
                        source="scan",
                        source_event_key=source_event_key,
                    )
                except Exception as exc:
                    failures.append({"file": os.path.basename(path), "error": type(exc).__name__})
                else:
                    known.add(_path_key(path))
                    submitted += 1
            completed = min(start + len(batch), total)
            progress.update(
                80 + int((completed / max(total, 1)) * 19),
                f"正在提交查漏项目 {completed}/{total}",
            )

        details = _details(
            scanned=scanned,
            submitted=submitted,
            skipped_known=skipped_known,
            failed=len(failures) + len(missing_roots),
            triggered_directories=triggered_directories,
            limit_reached=bool(maximum and triggered_directories >= maximum),
            missing_root_count=len(missing_roots),
            failures=failures,
        )
        if failures:
            return TaskOutcome.partial("监控目录查漏扫描部分提交失败。", details)
        if missing_roots and submitted == 0:
            return TaskOutcome.failed("监控目录均不可用，查漏扫描未提交项目。", details)
        if missing_roots:
            return TaskOutcome.partial("监控目录查漏扫描完成，但部分根目录不可用。", details)
        return TaskOutcome.succeeded("监控目录查漏扫描已写入持久队列。", details)


class P6IngestionTaskRouter:
    def __init__(self, scan: P6IngestionScanService) -> None:
        self._scan = scan

    def execute(self, message: TaskMessage, progress: IngestionScanProgress) -> TaskOutcome:
        if not isinstance(message, TaskMessage):
            raise TypeError("P6 ingestion task router requires a TaskMessage")
        if message.task_key not in P6_INGESTION_NATIVE_TASK_KEYS:
            raise LookupError(f"unknown P6 ingestion task key: {message.task_key}")
        if message.processor is not ProcessorIdentity.MEDIA:
            raise ValueError(f"task {message.task_key!r} requires the media processor identity")
        if message.payload.version != 1:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")
        if message.payload.data:
            raise ValueError(f"task {message.task_key!r} does not accept parameters")
        result = self._scan.execute(progress)
        return TaskOutcome(
            result.status,
            result.message,
            {
                **result.details,
                "task_key": message.task_key,
                "native_handler": type(self).__name__,
                "compatibility_fallback": False,
            },
        )


def _extensions(values: Iterable[str]) -> frozenset[str]:
    extensions: set[str] = set()
    for value in values:
        extension = str(value or "").strip().replace("*", "").casefold()
        if not extension:
            continue
        extensions.add(extension if extension.startswith(".") else f".{extension}")
    return frozenset(extensions)


def _path_key(value: object) -> str:
    return os.path.normpath(str(value or "").strip()).replace("\\", "/").rstrip("/").casefold()


def _is_excluded(path: str, exclusions: Iterable[str]) -> bool:
    candidate = _path_key(path)
    return any(candidate == _path_key(root) or candidate.startswith(f"{_path_key(root)}/") for root in exclusions)


def _config_revision(
    settings: Settings,
    roots: list[str],
    exclusions: tuple[str, ...],
    extensions: frozenset[str],
) -> str:
    payload = {
        "enabled": settings.monitor_enabled,
        "roots": roots,
        "exclusions": list(exclusions),
        "extensions": sorted(extensions),
        "scan_batch_size": max(1, int(settings.monitor_scan_batch_size)),
        "scan_max_tasks": max(0, int(settings.monitor_scan_max_tasks)),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _details(
    *,
    scanned: int,
    submitted: int,
    skipped_known: int,
    failed: int,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "scanned": scanned,
        "submitted": submitted,
        "skipped_known": skipped_known,
        "failed": failed,
        **extra,
    }


__all__ = [
    "IngestionKnownPathSource",
    "IngestionScanProgress",
    "P6IngestionScanService",
    "P6IngestionTaskRouter",
]
