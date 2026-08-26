from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Protocol
from urllib.parse import unquote, urlsplit

import httpx

from .fingerprints import (
    P115FileFingerprint,
    P115FingerprintAccountChanged,
    P115FingerprintHints,
    canonical_sha1,
)
from .media_info_formatter import P115MediaInfoPolicySnapshot

PREID_PREFIX_BYTES = 128 * 1024
_IO_CHUNK_BYTES = 64 * 1024
_CONTENT_RANGE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+)", re.IGNORECASE)
_SENSITIVE_HEADER = re.compile(
    r"(?i)\b(?:cookie|authorization|proxy-authorization|x-api-key|x-emby-token)\s*[:=]\s*[^\r\n]*"
)
_HTTP_URL = re.compile(r"https?://[^\s\"']+", re.IGNORECASE)

CancelCheck = Callable[[], bool]
Clock = Callable[[], float]
HttpClientFactory = Callable[..., httpx.Client]


@dataclass(frozen=True, slots=True)
class P115DirectUrl:
    url: str = field(repr=False)
    user_agent: str
    backend: str
    method: str

    def __post_init__(self) -> None:
        if not str(self.url or "").startswith(("http://", "https://")):
            raise ValueError("115 direct URL must use HTTP or HTTPS")
        if self.backend not in {"openapi", "cookie"}:
            raise ValueError("115 direct URL backend is invalid")
        if not self.user_agent or not self.method:
            raise ValueError("115 direct URL evidence is incomplete")


class ControlledProcessProfile(StrEnum):
    FFPROBE = "ffprobe"
    FFMPEG = "ffmpeg"


class ControlledProcessOutput(StrEnum):
    BYTES = "bytes"
    JSON = "json"


class ControlledProcessStatus(StrEnum):
    SUCCEEDED = "succeeded"
    NONZERO = "nonzero"
    INVALID_OUTPUT = "invalid_output"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    OUTPUT_LIMIT = "output_limit"
    START_FAILED = "start_failed"


@dataclass(frozen=True, slots=True)
class ControlledProcessOutcome:
    status: ControlledProcessStatus
    return_code: int | None
    diagnostic: str
    stdout: bytes = field(default=b"", repr=False)
    stderr: bytes = field(default=b"", repr=False)
    parsed_output: object | None = field(default=None, repr=False)
    limit_stream: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status is ControlledProcessStatus.SUCCEEDED


class ControlledProcessRunner:
    """Run only fixed media-tool profiles with bounded pipes and deterministic teardown."""

    def __init__(
        self,
        *,
        executable_overrides: Mapping[ControlledProcessProfile, str] | None = None,
        termination_grace_seconds: float = 0.25,
        poll_interval_seconds: float = 0.01,
        clock: Clock = time.monotonic,
    ) -> None:
        if termination_grace_seconds < 0:
            raise ValueError("termination grace must not be negative")
        if poll_interval_seconds <= 0:
            raise ValueError("process poll interval must be positive")
        self._executables = {
            ControlledProcessProfile.FFPROBE: "ffprobe",
            ControlledProcessProfile.FFMPEG: "ffmpeg",
        }
        for profile, executable in dict(executable_overrides or {}).items():
            normalized = str(executable or "").strip()
            if not normalized:
                raise ValueError("controlled process executable must not be empty")
            self._executables[ControlledProcessProfile(profile)] = normalized
        self._termination_grace_seconds = float(termination_grace_seconds)
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._clock = clock

    def run(
        self,
        profile: ControlledProcessProfile,
        arguments: Sequence[str],
        *,
        stdin: bytes = b"",
        max_stdin_bytes: int,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        timeout_seconds: float,
        output: ControlledProcessOutput = ControlledProcessOutput.BYTES,
        cancel: CancelCheck | None = None,
    ) -> ControlledProcessOutcome:
        normalized_profile = ControlledProcessProfile(profile)
        normalized_arguments = tuple(str(argument) for argument in arguments)
        if any("\x00" in argument for argument in normalized_arguments):
            raise ValueError("controlled process arguments must not contain NUL bytes")
        if max_stdin_bytes < 0 or max_stdout_bytes <= 0 or max_stderr_bytes <= 0:
            raise ValueError("controlled process pipe limits are invalid")
        if timeout_seconds <= 0:
            raise ValueError("controlled process timeout must be positive")
        output_kind = ControlledProcessOutput(output)

        if len(stdin) > max_stdin_bytes:
            return ControlledProcessOutcome(
                ControlledProcessStatus.OUTPUT_LIMIT,
                None,
                "controlled process input exceeded its byte limit",
                limit_stream="stdin",
            )
        if _is_cancelled(cancel):
            return ControlledProcessOutcome(
                ControlledProcessStatus.CANCELLED,
                None,
                "controlled process was cancelled before start",
            )

        started_at = self._clock()
        command = [self._executables[normalized_profile], *normalized_arguments]
        try:
            process = subprocess.Popen(  # noqa: S603 - executable is selected from a fixed profile map.
                command,
                stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                start_new_session=os.name == "posix",
            )
        except (OSError, ValueError):
            return ControlledProcessOutcome(
                ControlledProcessStatus.START_FAILED,
                None,
                "controlled process could not be started",
            )

        assert process.stdout is not None
        assert process.stderr is not None
        stdout = bytearray()
        stderr = bytearray()
        limit_event = threading.Event()
        limit_lock = threading.Lock()
        limit_stream: list[str] = []

        stdout_thread = threading.Thread(
            target=_drain_pipe,
            args=(process.stdout, stdout, max_stdout_bytes, "stdout", limit_event, limit_lock, limit_stream),
            name="etk-ffprobe-stdout",
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_drain_pipe,
            args=(process.stderr, stderr, max_stderr_bytes, "stderr", limit_event, limit_lock, limit_stream),
            name="etk-ffprobe-stderr",
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()

        stdin_thread: threading.Thread | None = None
        if stdin:
            assert process.stdin is not None
            stdin_thread = threading.Thread(
                target=_write_pipe,
                args=(process.stdin, stdin),
                name="etk-ffprobe-stdin",
                daemon=True,
            )
            stdin_thread.start()

        deadline = started_at + float(timeout_seconds)
        terminal_status: ControlledProcessStatus | None = None
        while process.poll() is None:
            if limit_event.is_set():
                terminal_status = ControlledProcessStatus.OUTPUT_LIMIT
            elif _is_cancelled(cancel):
                terminal_status = ControlledProcessStatus.CANCELLED
            elif self._clock() >= deadline:
                terminal_status = ControlledProcessStatus.TIMED_OUT
            if terminal_status is not None:
                self._terminate_and_reap(process)
                break
            time.sleep(self._poll_interval_seconds)

        if process.poll() is None:
            process.wait()
        if stdin_thread is not None:
            stdin_thread.join()
        stdout_thread.join()
        stderr_thread.join()

        if terminal_status is None and limit_event.is_set():
            terminal_status = ControlledProcessStatus.OUTPUT_LIMIT
        captured_stdout = bytes(stdout)
        captured_stderr = _redact_diagnostic(bytes(stderr))
        selected_limit = limit_stream[0] if limit_stream else None

        if terminal_status is ControlledProcessStatus.OUTPUT_LIMIT:
            return ControlledProcessOutcome(
                terminal_status,
                process.returncode,
                "controlled process output exceeded its byte limit",
                stdout=captured_stdout,
                stderr=captured_stderr,
                limit_stream=selected_limit,
            )
        if terminal_status is ControlledProcessStatus.CANCELLED:
            return ControlledProcessOutcome(
                terminal_status,
                process.returncode,
                "controlled process was cancelled",
                stdout=captured_stdout,
                stderr=captured_stderr,
            )
        if terminal_status is ControlledProcessStatus.TIMED_OUT:
            return ControlledProcessOutcome(
                terminal_status,
                process.returncode,
                "controlled process timed out",
                stdout=captured_stdout,
                stderr=captured_stderr,
            )
        if process.returncode != 0:
            return ControlledProcessOutcome(
                ControlledProcessStatus.NONZERO,
                process.returncode,
                "controlled process exited with a non-zero status",
                stdout=captured_stdout,
                stderr=captured_stderr,
            )
        if output_kind is ControlledProcessOutput.JSON:
            try:
                parsed = json.loads(captured_stdout.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return ControlledProcessOutcome(
                    ControlledProcessStatus.INVALID_OUTPUT,
                    process.returncode,
                    "controlled process returned invalid JSON output",
                    stdout=captured_stdout,
                    stderr=captured_stderr,
                )
            return ControlledProcessOutcome(
                ControlledProcessStatus.SUCCEEDED,
                process.returncode,
                "controlled process completed",
                stdout=captured_stdout,
                stderr=captured_stderr,
                parsed_output=parsed,
            )
        return ControlledProcessOutcome(
            ControlledProcessStatus.SUCCEEDED,
            process.returncode,
            "controlled process completed",
            stdout=captured_stdout,
            stderr=captured_stderr,
        )

    def _terminate_and_reap(self, process: subprocess.Popen[bytes]) -> None:
        _signal_process(process, signal.SIGTERM)
        grace_deadline = self._clock() + self._termination_grace_seconds
        while _process_group_exists(process) and self._clock() < grace_deadline:
            process.poll()
            time.sleep(min(self._poll_interval_seconds, max(0.0, grace_deadline - self._clock())))
        if _process_group_exists(process):
            _signal_process(process, signal.SIGKILL)
        process.wait()


def _drain_pipe(
    pipe: BinaryIO,
    target: bytearray,
    limit: int,
    stream_name: str,
    limit_event: threading.Event,
    limit_lock: threading.Lock,
    limit_stream: list[str],
) -> None:
    read = getattr(pipe, "read1", pipe.read)
    try:
        while chunk := read(_IO_CHUNK_BYTES):
            remaining = max(0, limit - len(target))
            if remaining:
                target.extend(chunk[:remaining])
            if len(chunk) > remaining:
                with limit_lock:
                    if not limit_stream:
                        limit_stream.append(stream_name)
                limit_event.set()
    except OSError:
        return
    finally:
        try:
            pipe.close()
        except OSError:
            pass


def _write_pipe(pipe: BinaryIO, content: bytes) -> None:
    try:
        for offset in range(0, len(content), _IO_CHUNK_BYTES):
            pipe.write(content[offset : offset + _IO_CHUNK_BYTES])
            pipe.flush()
    except (BrokenPipeError, OSError):
        pass
    finally:
        try:
            pipe.close()
        except OSError:
            pass


def _signal_process(process: subprocess.Popen[bytes], sig: signal.Signals) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, sig)
        elif process.poll() is not None:
            return
        elif sig is signal.SIGTERM:
            process.terminate()
        else:
            process.kill()
    except OSError:
        pass


def _process_group_exists(process: subprocess.Popen[bytes]) -> bool:
    if os.name != "posix":
        return process.poll() is None
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_cancelled(cancel: CancelCheck | None) -> bool:
    return bool(cancel is not None and cancel())


def _redact_diagnostic(value: bytes) -> bytes:
    text = value.decode("utf-8", "replace")
    text = _SENSITIVE_HEADER.sub("<redacted-header>", text)
    text = _HTTP_URL.sub("<redacted-url>", text)
    return text.encode("utf-8")


class RangeReadErrorKind(StrEnum):
    INVALID_URL = "invalid_url"
    INVALID_RANGE = "invalid_range"
    REQUEST_LIMIT = "request_limit"
    BYTE_LIMIT = "byte_limit"
    DEADLINE = "deadline"
    CANCELLED = "cancelled"
    REDIRECT_LIMIT = "redirect_limit"
    TIMEOUT = "timeout"
    TRANSPORT = "transport"
    STATUS = "status"
    CONTENT_RANGE = "content_range"
    LENGTH = "length"


class RangeReadError(RuntimeError):
    def __init__(
        self,
        kind: RangeReadErrorKind,
        diagnostic: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(diagnostic)
        self.kind = kind
        self.diagnostic = diagnostic
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class RangeReadMetrics:
    requests: int
    bytes_received: int


class BoundedRangeReader:
    """Read exact HTTP byte ranges under one operation-wide request/byte/deadline budget."""

    def __init__(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        max_requests: int,
        max_bytes: int,
        deadline_seconds: float,
        request_timeout_seconds: float,
        max_redirects: int = 3,
        chunk_size: int = _IO_CHUNK_BYTES,
        cancel: CancelCheck | None = None,
        client_factory: HttpClientFactory = httpx.Client,
        clock: Clock = time.monotonic,
    ) -> None:
        try:
            parsed = urlsplit(str(url or ""))
        except ValueError:
            raise RangeReadError(
                RangeReadErrorKind.INVALID_URL,
                "range reader requires an HTTP or HTTPS URL",
            ) from None
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            raise RangeReadError(RangeReadErrorKind.INVALID_URL, "range reader requires an HTTP or HTTPS URL")
        if max_requests <= 0 or max_bytes <= 0:
            raise ValueError("range request and byte budgets must be positive")
        if deadline_seconds <= 0 or request_timeout_seconds <= 0:
            raise ValueError("range deadlines and timeouts must be positive")
        if max_redirects < 0 or chunk_size <= 0:
            raise ValueError("range redirect and chunk limits are invalid")
        self._url = str(url)
        self._headers = {str(key): str(value) for key, value in dict(headers or {}).items()}
        self._max_requests = int(max_requests)
        self._max_bytes = int(max_bytes)
        self._deadline_at = clock() + float(deadline_seconds)
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._max_redirects = int(max_redirects)
        self._chunk_size = int(chunk_size)
        self._cancel = cancel
        self._client_factory = client_factory
        self._clock = clock
        self._requests = 0
        self._bytes_received = 0

    @property
    def metrics(self) -> RangeReadMetrics:
        return RangeReadMetrics(self._requests, self._bytes_received)

    def read(self, start: int, length: int) -> bytes:
        normalized_start = int(start)
        normalized_length = int(length)
        if normalized_start < 0 or normalized_length <= 0:
            raise RangeReadError(RangeReadErrorKind.INVALID_RANGE, "range start and length are invalid")
        self._checkpoint()
        if self._requests >= self._max_requests:
            raise RangeReadError(RangeReadErrorKind.REQUEST_LIMIT, "range request budget was exhausted")
        if self._bytes_received + normalized_length > self._max_bytes:
            raise RangeReadError(RangeReadErrorKind.BYTE_LIMIT, "range byte budget was exhausted")

        end = normalized_start + normalized_length - 1
        headers = dict(self._headers)
        headers["Range"] = f"bytes={normalized_start}-{end}"
        headers["Accept-Encoding"] = "identity"
        remaining = self._remaining_seconds()
        timeout = httpx.Timeout(min(self._request_timeout_seconds, remaining))
        redirect_ceiling = min(self._max_redirects, self._max_requests - self._requests - 1)
        request_budget_limits_redirects = redirect_ceiling < self._max_redirects
        self._requests += 1
        try:
            with self._client_factory(
                timeout=timeout,
                follow_redirects=True,
                max_redirects=redirect_ceiling,
                trust_env=False,
            ) as client:
                with client.stream("GET", self._url, headers=headers) as response:
                    self._requests += len(response.history)
                    if any(
                        item.request.url.scheme.lower() not in {"http", "https"}
                        for item in (*response.history, response)
                    ):
                        raise RangeReadError(
                            RangeReadErrorKind.INVALID_URL,
                            "range redirect target was not HTTP or HTTPS",
                        )
                    if len(response.history) > redirect_ceiling:
                        raise RangeReadError(
                            RangeReadErrorKind.REQUEST_LIMIT
                            if request_budget_limits_redirects
                            else RangeReadErrorKind.REDIRECT_LIMIT,
                            "range request budget was exhausted"
                            if request_budget_limits_redirects
                            else "range redirect budget was exhausted",
                        )
                    if response.status_code != 206:
                        raise RangeReadError(
                            RangeReadErrorKind.STATUS,
                            f"range request returned HTTP {response.status_code}",
                            status_code=response.status_code,
                        )
                    _validate_content_range(response.headers.get("Content-Range"), normalized_start, end)

                    content = bytearray()
                    for chunk in response.iter_bytes(chunk_size=self._chunk_size):
                        self._checkpoint()
                        self._bytes_received += len(chunk)
                        if self._bytes_received > self._max_bytes:
                            raise RangeReadError(RangeReadErrorKind.BYTE_LIMIT, "range byte budget was exhausted")
                        if len(content) + len(chunk) > normalized_length:
                            raise RangeReadError(RangeReadErrorKind.LENGTH, "range response length was not exact")
                        content.extend(chunk)
                        self._checkpoint()
                    if len(content) != normalized_length:
                        raise RangeReadError(RangeReadErrorKind.LENGTH, "range response length was not exact")
                    return bytes(content)
        except RangeReadError:
            raise
        except httpx.TooManyRedirects:
            self._requests += redirect_ceiling
            raise RangeReadError(
                RangeReadErrorKind.REQUEST_LIMIT
                if request_budget_limits_redirects
                else RangeReadErrorKind.REDIRECT_LIMIT,
                "range request budget was exhausted"
                if request_budget_limits_redirects
                else "range redirect budget was exhausted",
            ) from None
        except httpx.TimeoutException:
            raise RangeReadError(RangeReadErrorKind.TIMEOUT, "range request timed out") from None
        except httpx.HTTPError:
            raise RangeReadError(RangeReadErrorKind.TRANSPORT, "range request transport failed") from None
        except Exception:
            raise RangeReadError(RangeReadErrorKind.TRANSPORT, "range request transport failed") from None

    def _checkpoint(self) -> None:
        if _is_cancelled(self._cancel):
            raise RangeReadError(RangeReadErrorKind.CANCELLED, "range request was cancelled")
        self._remaining_seconds()

    def _remaining_seconds(self) -> float:
        remaining = self._deadline_at - self._clock()
        if remaining <= 0:
            raise RangeReadError(RangeReadErrorKind.DEADLINE, "range operation deadline expired")
        return remaining


def _validate_content_range(value: str | None, expected_start: int, expected_end: int) -> None:
    match = _CONTENT_RANGE.fullmatch(str(value or "").strip())
    if match is None:
        raise RangeReadError(RangeReadErrorKind.CONTENT_RANGE, "range response Content-Range was invalid")
    start, end, total = (int(part) for part in match.groups())
    if start != expected_start or end != expected_end or total <= end:
        raise RangeReadError(RangeReadErrorKind.CONTENT_RANGE, "range response Content-Range was invalid")


class PreIdSource(StrEnum):
    FILE = "file"
    FILE_RAW = "file_raw"
    CACHE_RAW = "cache_raw"
    RAPID_HINT = "rapid_hint"
    SHARED_RAPID = "shared_rapid"


@dataclass(frozen=True, slots=True)
class PreIdCandidates:
    file_preid: object = None
    file_raw: object = None
    cached_raw: object = None
    rapid_hint_preid: object = None
    shared_rapid_preid: object = None


@dataclass(frozen=True, slots=True)
class PreIdResolution:
    value: str
    source: PreIdSource


class ExactRangeReader(Protocol):
    def read(self, start: int, length: int) -> bytes: ...


def normalize_preid(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if re.fullmatch(r"[A-F0-9]{40}", normalized) else ""


def resolve_cached_preid(candidates: PreIdCandidates) -> PreIdResolution | None:
    ordered = (
        (PreIdSource.FILE, normalize_preid(candidates.file_preid)),
        (PreIdSource.FILE_RAW, _extract_raw_preid(candidates.file_raw)),
        (PreIdSource.CACHE_RAW, _extract_raw_preid(candidates.cached_raw)),
        (PreIdSource.RAPID_HINT, normalize_preid(candidates.rapid_hint_preid)),
        (PreIdSource.SHARED_RAPID, normalize_preid(candidates.shared_rapid_preid)),
    )
    for source, value in ordered:
        if value:
            return PreIdResolution(value, source)
    return None


def compute_preid(prefix: bytes) -> str:
    if len(prefix) != PREID_PREFIX_BYTES:
        raise ValueError("preid requires exactly the first 128 KiB")
    return hashlib.sha1(prefix).hexdigest().upper()


def preid_from_range(reader: ExactRangeReader) -> str:
    return compute_preid(reader.read(0, PREID_PREFIX_BYTES))


def _extract_raw_preid(value: object) -> str:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return ""
    if not isinstance(raw, Mapping):
        return ""
    context = raw.get("_etk")
    if not isinstance(context, Mapping):
        return ""
    return normalize_preid(context.get("preid") or context.get("pre_sha1") or context.get("pre_sha1_128k"))


_P115_PLAY_PATH = re.compile(r"/p115/play/([^/?#]+)(?:[/?#]|$)")
_P115_VIRTUAL_PATH = re.compile(r"/api/p115/virtual-play/\d+/([A-Fa-f0-9]{40})(?:[/?#]|$)")
_VIDEO_SUFFIXES = frozenset(
    {".avi", ".flv", ".iso", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mpg", ".mpeg", ".rmvb", ".ts", ".webm", ".wmv"}
)
_MAX_STRM_BYTES = 64 * 1024


class LocalP115FingerprintHintGateway:
    """Read only root-contained STRM hints and parse stable ETK playback identities."""

    def inspect(self, path: str, *, local_root: str) -> P115FingerprintHints:
        raw_path = str(path or "").strip()
        if not raw_path:
            return P115FingerprintHints()
        target = raw_path
        if _path_suffix(raw_path) == ".strm" and not _is_http_url(raw_path):
            target = self._read_strm(raw_path, local_root=local_root) or raw_path
        pick_code = _pick_code_from_url(target)
        virtual_match = _P115_VIRTUAL_PATH.search(target)
        sha1 = canonical_sha1(virtual_match.group(1), allow_empty=True) if virtual_match else ""
        local_paths = _local_path_candidates(raw_path, target, local_root=local_root)
        return P115FingerprintHints(
            pick_code=pick_code,
            sha1=sha1,
            local_paths=local_paths,
            virtual=virtual_match is not None,
        )

    @staticmethod
    def _read_strm(path: str, *, local_root: str) -> str:
        root_value = str(local_root or "").strip()
        if not root_value:
            return ""
        try:
            root = Path(root_value).resolve(strict=True)
            candidate = Path(path).resolve(strict=True)
            if (
                not candidate.is_relative_to(root)
                or not candidate.is_file()
                or candidate.stat().st_size > _MAX_STRM_BYTES
            ):
                return ""
            return candidate.read_text(encoding="utf-8").strip().replace("\\", "/")
        except (OSError, UnicodeError, ValueError):
            return ""


class AccountBoundP115FingerprintClient:
    """Validate one PickCode lookup against one fixed 115 account and FID."""

    def __init__(
        self,
        client: object,
        *,
        account_id: str,
        backend: str,
        account_reader: Callable[[], str],
        id_resolver: Callable[[str], str],
    ) -> None:
        self._client = client
        self._account_id = str(account_id or "").strip()
        self.backend = str(backend or "").strip().lower()
        self._account_reader = account_reader
        self._id_resolver = id_resolver
        if not self._account_id.isdigit():
            raise ValueError("fingerprint client requires a stable numeric account ID")
        if self.backend not in {"openapi", "cookie"}:
            raise ValueError("fingerprint client backend is invalid")

    def lookup(self, pick_code: str) -> P115FileFingerprint | None:
        normalized_pick_code = str(pick_code or "").strip()
        if not normalized_pick_code:
            return None
        self._assert_account()
        file_id = str(self._id_resolver(normalized_pick_code) or "").strip()
        if not file_id.isdigit():
            return None
        method = getattr(self._client, "fs_get_info", None)
        if not callable(method):
            return None
        response = method(file_id)
        self._assert_account()
        data = _file_info_data(response)
        if data is None:
            return None
        returned_id = str(data.get("fid") or data.get("file_id") or data.get("id") or "").strip()
        returned_pick_code = str(data.get("pc") or data.get("pick_code") or data.get("pickcode") or "").strip()
        if returned_id != file_id or returned_pick_code != normalized_pick_code:
            raise RuntimeError("115 fingerprint response identity did not match the requested PickCode")
        parent_id = str(data.get("parent_id") or data.get("pid") or data.get("cid") or "").strip()
        if not parent_id:
            paths = data.get("paths")
            if isinstance(paths, list) and paths and isinstance(paths[-1], Mapping):
                parent_id = str(paths[-1].get("file_id") or paths[-1].get("cid") or "").strip()
        size = _nonnegative_size(
            data.get("size_byte") or data.get("fs") or data.get("size") or data.get("file_size") or 0
        )
        return P115FileFingerprint(
            file_id=returned_id,
            parent_id=parent_id,
            name=str(data.get("fn") or data.get("n") or data.get("file_name") or data.get("name") or ""),
            sha1=str(data.get("sha1") or data.get("sha") or data.get("file_sha1") or ""),
            pick_code=returned_pick_code,
            size=size,
            account_id=self._account_id,
        )

    def _assert_account(self) -> None:
        if str(self._account_reader() or "").strip() != self._account_id:
            raise P115FingerprintAccountChanged("115 account changed during fingerprint lookup")


class LegacyP115FingerprintRemoteGateway:
    """P11 compatibility seam that opens an account-bound read-only file-info client."""

    def open(self, *, expected_account_id: str) -> AccountBoundP115FingerprintClient | None:
        from handler.p115_service import P115Service, get_115_account_id

        account_id = str(expected_account_id or "").strip()
        if not account_id.isdigit() or str(get_115_account_id() or "").strip() != account_id:
            return None
        client = P115Service.get_client(expected_account_id=account_id)
        if client is None:
            return None
        backend = _client_backend(client)
        if not backend:
            return None
        return AccountBoundP115FingerprintClient(
            client,
            account_id=account_id,
            backend=backend,
            account_reader=lambda: str(get_115_account_id() or "").strip(),
            id_resolver=_pick_code_to_id,
        )


class LegacyP115DirectUrlGateway:
    """Resolve one short-lived download URL while pinning the current 115 account."""

    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl | None:
        from handler.p115_service import P115Service, get_115_account_id

        account_id = str(expected_account_id or "").strip()
        normalized_pick_code = str(pick_code or "").strip()
        if (
            not account_id.isdigit()
            or not normalized_pick_code
            or str(get_115_account_id() or "").strip() != account_id
        ):
            return None
        client = P115Service.get_client(expected_account_id=account_id)
        if client is None:
            return None
        method = getattr(client, "resolve_download_url", None)
        if not callable(method):
            return None
        try:
            raw = method(
                normalized_pick_code,
                user_agent=str(user_agent or "Mozilla/5.0"),
                return_backend=True,
            )
        except Exception:
            return None
        if str(get_115_account_id() or "").strip() != account_id:
            return None
        if not isinstance(raw, tuple) or len(raw) != 2:
            return None
        url, backend = raw
        normalized_url = str(url or "").strip()
        normalized_backend = str(backend or "").strip().lower()
        if not normalized_url or normalized_backend not in {"openapi", "cookie"}:
            return None
        return P115DirectUrl(
            normalized_url,
            str(user_agent or "Mozilla/5.0"),
            normalized_backend,
            "resolve_download_url",
        )


class LegacyP115MediaInfoPolicyProvider:
    """Freeze legacy dynamic policy values before pure formatter execution."""

    def snapshot(self) -> P115MediaInfoPolicySnapshot:
        from database import settings_db
        from utils import DEFAULT_LANGUAGE_MAPPING

        raw_language = settings_db.get_setting("language_mapping")
        language_mapping = raw_language if isinstance(raw_language, list) else DEFAULT_LANGUAGE_MAPPING
        raw_streams = settings_db.get_setting("p115_default_stream_config")
        stream_config = dict(raw_streams) if isinstance(raw_streams, Mapping) else {}
        language_revision = _json_revision(language_mapping)
        stream_revision = _json_revision(stream_config)
        return P115MediaInfoPolicySnapshot.create(
            language_mapping=language_mapping,
            default_stream_config=stream_config,
            language_mapping_revision=language_revision,
            default_stream_config_revision=stream_revision,
        )


def _pick_code_from_url(value: str) -> str:
    match = _P115_PLAY_PATH.search(str(value or ""))
    return unquote(match.group(1)).strip() if match else ""


def _local_path_candidates(path: str, target: str, *, local_root: str) -> tuple[str, ...]:
    root_value = str(local_root or "").strip()
    if not root_value:
        return ()
    try:
        root = Path(root_value).resolve(strict=False)
    except (OSError, ValueError):
        return ()
    candidates: list[str] = []
    raw_path = str(path or "").strip()
    if raw_path and not _is_http_url(raw_path):
        try:
            relative = Path(raw_path).resolve(strict=False).relative_to(root).as_posix()
        except (OSError, ValueError):
            relative = ""
        if relative:
            candidates.append(relative)
            if relative.lower().endswith(".strm"):
                target_suffix = _path_suffix(target)
                if target_suffix in _VIDEO_SUFFIXES:
                    candidates.append(str(PurePosixPath(relative).with_suffix(target_suffix)))
    if target and not _is_http_url(target):
        try:
            target_relative = Path(target).resolve(strict=False).relative_to(root).as_posix()
        except (OSError, ValueError):
            target_relative = ""
        if target_relative:
            candidates.append(target_relative)
    return tuple(dict.fromkeys(candidates))


def _path_suffix(value: str) -> str:
    parsed = urlsplit(str(value or ""))
    path = parsed.path if parsed.scheme else str(value or "").split("#", 1)[0].split("?", 1)[0]
    return PurePosixPath(unquote(path).replace("\\", "/")).suffix.lower()


def _is_http_url(value: str) -> bool:
    try:
        parsed = urlsplit(str(value or ""))
    except ValueError:
        return False
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def _file_info_data(value: object) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping) or value.get("state") is not True:
        return None
    data = value.get("data")
    if isinstance(data, list):
        data = data[0] if data else None
    return data if isinstance(data, Mapping) else None


def _client_backend(client: object) -> str:
    method = getattr(client, "p115_organize_backends", None)
    if callable(method):
        raw_values = method()
        if not isinstance(raw_values, Iterable) or isinstance(raw_values, (str, bytes, bytearray)):
            return ""
        values = tuple(str(value or "").strip().lower() for value in raw_values)
        return next((value for value in values if value in {"openapi", "cookie"}), "")
    return ""


def _pick_code_to_id(pick_code: str) -> str:
    try:
        from p115pickcode import to_id

        return str(to_id(pick_code))
    except ImportError:
        try:
            module = importlib.import_module("p115client.tool.iterdir")
            fallback = getattr(module, "to_id", None)
            return str(fallback(pick_code)) if callable(fallback) else ""
        except (ImportError, AttributeError):
            return ""


def _nonnegative_size(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _json_revision(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


__all__ = [
    "BoundedRangeReader",
    "ControlledProcessOutcome",
    "ControlledProcessOutput",
    "ControlledProcessProfile",
    "ControlledProcessRunner",
    "ControlledProcessStatus",
    "ExactRangeReader",
    "AccountBoundP115FingerprintClient",
    "LegacyP115FingerprintRemoteGateway",
    "LegacyP115DirectUrlGateway",
    "LegacyP115MediaInfoPolicyProvider",
    "LocalP115FingerprintHintGateway",
    "PREID_PREFIX_BYTES",
    "P115DirectUrl",
    "PreIdCandidates",
    "PreIdResolution",
    "PreIdSource",
    "RangeReadError",
    "RangeReadErrorKind",
    "RangeReadMetrics",
    "compute_preid",
    "normalize_preid",
    "preid_from_range",
    "resolve_cached_preid",
]
