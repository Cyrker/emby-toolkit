from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, Protocol

from .media_info_adapters import (
    BoundedRangeReader,
    ControlledProcessOutput,
    ControlledProcessProfile,
    ControlledProcessRunner,
    ControlledProcessStatus,
    P115DirectUrl,
    RangeReadError,
    RangeReadErrorKind,
    RangeReadMetrics,
)

if TYPE_CHECKING:
    from .media_probe import P115MediaProbeRequest


_SECTOR_BYTES = 2048
_M2TS_PACKET_BYTES = 192
_MAX_OFFSET = 1 << 50
_MAX_UDF_DESCRIPTORS = 1024
_MAX_PARTITION_MAPS = 32
_MAX_ALLOCATION_DESCRIPTORS = 256
_MAX_FILE_ENTRY_BYTES = 64 * 1024
_MAX_DIRECTORY_BYTES = 8 * 1024 * 1024
_MAX_DIRECTORY_ENTRIES = 4096
_MAX_WRAPPER_DIRECTORIES = 512
_MAX_PLAYLISTS = 2048
_MAX_PLAYLIST_ITEMS = 2048
_MAX_PLAYLIST_BYTES = 4 * 1024 * 1024
_MAX_CLPI_BYTES = 4 * 1024 * 1024
_MAX_CLPI_STREAMS = 96
_MAX_METADATA_BYTES = 32 * 1024 * 1024
_MAX_MEDIA_FILE_BYTES = 256 * 1024 * 1024 * 1024
_MAX_NESTED_ISO_BYTES = 256 * 1024 * 1024 * 1024
_MAX_FFPROBE_STREAMS = 256
_FFPROBE_STDOUT_BYTES = 2 * 1024 * 1024
_FFPROBE_STDERR_BYTES = 256 * 1024

CancelCheck = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class P115IsoProbeLimits:
    """Operation-wide I/O limits for one native ISO probe."""

    max_range_requests: int = 512
    max_range_bytes: int = 128 * 1024 * 1024
    deadline_seconds: float = 90.0
    request_timeout_seconds: float = 20.0
    max_redirects: int = 3
    max_wrapper_depth: int = 4
    max_cached_range_bytes: int = 24 * 1024 * 1024
    max_m2ts_sample_bytes: int = 16 * 1024 * 1024
    max_pcr_sample_bytes: int = 8 * 1024 * 1024
    process_timeout_seconds: float = 45.0

    def __post_init__(self) -> None:
        positive = (
            self.max_range_requests,
            self.max_range_bytes,
            self.deadline_seconds,
            self.request_timeout_seconds,
            self.max_cached_range_bytes,
            self.max_m2ts_sample_bytes,
            self.max_pcr_sample_bytes,
            self.process_timeout_seconds,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("ISO probe budgets must be positive")
        if self.max_redirects < 0 or not 0 <= self.max_wrapper_depth <= 4:
            raise ValueError("ISO probe redirect or wrapper limit is invalid")


class P115IsoProbeStatus(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    BUDGET_EXHAUSTED = "budget_exhausted"
    TOOL_FAILED = "tool_failed"


@dataclass(frozen=True, slots=True)
class P115IsoProbeOutcome:
    status: P115IsoProbeStatus
    diagnostic: str
    raw: Mapping[str, Any] | None = field(default=None, repr=False)

    @property
    def confirmed(self) -> bool:
        return self.status is P115IsoProbeStatus.CONFIRMED


class _ExactRangeReader(Protocol):
    @property
    def metrics(self) -> RangeReadMetrics: ...

    def read(self, start: int, length: int) -> bytes: ...


P115IsoRangeReaderFactory = Callable[[P115DirectUrl, CancelCheck | None], BoundedRangeReader]


class NativeP115IsoProbeGateway:
    """Read a Blu-ray UDF image with bounded ranges and bounded media-tool execution."""

    def __init__(
        self,
        processes: ControlledProcessRunner,
        *,
        range_reader_factory: P115IsoRangeReaderFactory | None = None,
        limits: P115IsoProbeLimits | None = None,
    ) -> None:
        self._processes = processes
        self._limits = limits or P115IsoProbeLimits()
        self._range_reader_factory = range_reader_factory or self._default_range_reader

    def probe(
        self,
        request: P115MediaProbeRequest,
        direct: P115DirectUrl,
        *,
        cancel: CancelCheck | None,
    ) -> Mapping[str, Any] | None:
        return self.probe_outcome(request, direct, cancel=cancel).raw

    def probe_outcome(
        self,
        request: P115MediaProbeRequest,
        direct: P115DirectUrl,
        *,
        cancel: CancelCheck | None,
    ) -> P115IsoProbeOutcome:
        if _cancelled(cancel):
            return P115IsoProbeOutcome(P115IsoProbeStatus.CANCELLED, "ISO probe was cancelled")
        if not str(request.file_name or "").casefold().endswith(".iso"):
            return P115IsoProbeOutcome(P115IsoProbeStatus.REJECTED, "ISO probe requires an ISO file")
        try:
            base_reader = self._range_reader_factory(direct, cancel)
            reader = _CachedRangeReader(
                base_reader,
                max_cached_bytes=self._limits.max_cached_range_bytes,
                cancel=cancel,
            )
            raw = self._probe_bluray(request, reader, cancel=cancel)
        except RangeReadError as exc:
            if exc.kind is RangeReadErrorKind.CANCELLED:
                return P115IsoProbeOutcome(P115IsoProbeStatus.CANCELLED, "ISO probe was cancelled")
            if exc.kind in {
                RangeReadErrorKind.REQUEST_LIMIT,
                RangeReadErrorKind.BYTE_LIMIT,
                RangeReadErrorKind.DEADLINE,
                RangeReadErrorKind.TIMEOUT,
                RangeReadErrorKind.REDIRECT_LIMIT,
            }:
                return P115IsoProbeOutcome(
                    P115IsoProbeStatus.BUDGET_EXHAUSTED,
                    "ISO probe exhausted its bounded range budget",
                )
            return P115IsoProbeOutcome(P115IsoProbeStatus.REJECTED, "ISO probe could not read the image")
        except _ProbeCancelled:
            return P115IsoProbeOutcome(P115IsoProbeStatus.CANCELLED, "ISO probe was cancelled")
        except _ProcessFailure:
            return P115IsoProbeOutcome(P115IsoProbeStatus.TOOL_FAILED, "ISO media sample could not be probed")
        except (_ParseFailure, ArithmeticError, UnicodeError, ValueError):
            return P115IsoProbeOutcome(P115IsoProbeStatus.REJECTED, "ISO image was not a supported Blu-ray")
        except Exception:
            return P115IsoProbeOutcome(P115IsoProbeStatus.REJECTED, "ISO probe failed closed")
        return P115IsoProbeOutcome(P115IsoProbeStatus.CONFIRMED, "ISO probe completed", raw)

    def _default_range_reader(
        self,
        direct: P115DirectUrl,
        cancel: CancelCheck | None,
    ) -> BoundedRangeReader:
        return BoundedRangeReader(
            direct.url,
            headers={
                "User-Agent": direct.user_agent,
                "Accept": "*/*",
                "Connection": "close",
            },
            max_requests=self._limits.max_range_requests,
            max_bytes=self._limits.max_range_bytes,
            deadline_seconds=self._limits.deadline_seconds,
            request_timeout_seconds=self._limits.request_timeout_seconds,
            max_redirects=self._limits.max_redirects,
            cancel=cancel,
        )

    def _probe_bluray(
        self,
        request: P115MediaProbeRequest,
        reader: _CachedRangeReader,
        *,
        cancel: CancelCheck | None,
    ) -> dict[str, Any]:
        _checkpoint(cancel)
        iso = _UdfIsoReader(reader)
        root = iso.root()
        bdmv, wrapper_depth = _find_bdmv_dir(iso, root, max_depth=self._limits.max_wrapper_depth)
        nested_iso_count = 0
        if bdmv is None:
            nested = _open_single_nested_iso(iso, root)
            if nested is not None:
                iso = nested
                root = iso.root()
                bdmv, wrapper_depth = _find_bdmv_dir(iso, root, max_depth=self._limits.max_wrapper_depth)
                nested_iso_count = 1
        if bdmv is None:
            raise _ParseFailure

        playlist_dir = iso.find(bdmv, "PLAYLIST")
        stream_dir = iso.find(bdmv, "STREAM")
        clipinf_dir = iso.find(bdmv, "CLIPINF")
        if playlist_dir is None or stream_dir is None or clipinf_dir is None:
            raise _ParseFailure

        stream_entries = _media_files(iso.list_dir(stream_dir), suffix=".m2ts")
        if not stream_entries:
            for child in iso.list_dir(bdmv):
                if not child.is_directory:
                    continue
                nested_stream = iso.find(child, "STREAM")
                nested_entries = _media_files(iso.list_dir(nested_stream), suffix=".m2ts") if nested_stream else []
                if nested_entries:
                    stream_entries = nested_entries
                    break
        if not stream_entries or len(stream_entries) > _MAX_DIRECTORY_ENTRIES:
            raise _ParseFailure

        iso.prefetch_file_entries(stream_entries, max_gap=256 * 1024, max_chunk=16 * 1024 * 1024)
        stream_sizes: dict[str, int] = {}
        stream_by_name: dict[str, _UdfEntry] = {}
        for entry in stream_entries:
            _checkpoint(cancel)
            fields, _ = iso.entry_info(entry)
            if fields.info_length <= 0 or fields.info_length > _MAX_MEDIA_FILE_BYTES:
                raise _ParseFailure
            normalized_name = entry.name.upper()
            stream_sizes[normalized_name] = fields.info_length
            stream_by_name[normalized_name] = entry
        if not stream_sizes:
            raise _ParseFailure

        playlist_entries = _media_files(iso.list_dir(playlist_dir), suffix=".mpls")
        if len(playlist_entries) > _MAX_PLAYLISTS:
            raise _ParseFailure
        iso.prefetch_file_entries(playlist_entries, max_gap=256 * 1024, max_chunk=16 * 1024 * 1024)
        iso.prefetch_files(playlist_entries, max_gap=256 * 1024, max_chunk=16 * 1024 * 1024)

        playlists: list[_Playlist] = []
        for entry in playlist_entries:
            _checkpoint(cancel)
            parsed = _parse_mpls(iso.file_data(entry, max_bytes=_MAX_PLAYLIST_BYTES), stream_sizes)
            if parsed is not None:
                playlists.append(parsed)
        selection = _select_main_stream(playlists, stream_sizes)
        main_entry = stream_by_name.get(selection.main_clip)
        if main_entry is None:
            raise _ParseFailure

        clpi_entry = iso.find(clipinf_dir, f"{selection.main_clip.rsplit('.', 1)[0]}.CLPI")
        streams: list[dict[str, Any]] = []
        stream_probe_method = "clpi"
        if clpi_entry is not None:
            iso.prefetch_file_entries([clpi_entry], max_gap=0, max_chunk=1024 * 1024)
            iso.prefetch_files([clpi_entry], max_gap=0, max_chunk=1024 * 1024)
            streams = _parse_clpi_streams(iso.file_data(clpi_entry, max_bytes=_MAX_CLPI_BYTES))

        physical_ranges = iso.physical_ranges_for_file(main_entry)
        main_size = stream_sizes[selection.main_clip]
        sampled_head: bytes | None = None
        if not _has_video_stream(streams):
            streams, sampled_head = self._probe_m2ts_sample(
                iso,
                physical_ranges,
                main_size,
                cancel=cancel,
            )
            stream_probe_method = "m2ts_ffprobe"

        duration = selection.duration
        duration_method = "mpls" if duration > 0 else "unavailable"
        if duration <= 0:
            duration = _m2ts_duration_from_pcr(
                iso,
                physical_ranges,
                main_size,
                sample_size=self._limits.max_pcr_sample_bytes,
                head=sampled_head,
            )
            if duration > 0:
                duration_method = "pcr"

        if not _has_video_stream(streams):
            raise _ParseFailure
        _checkpoint(cancel)

        size = int(request.file_size or main_size or 0)
        raw_format: dict[str, Any] = {
            "format_name": "bluray,iso",
            "format_long_name": "Blu-ray ISO",
            "duration": f"{duration:.3f}" if duration > 0 else "",
            "size": str(size),
        }
        if duration > 0 and size > 0:
            raw_format["bit_rate"] = str(int(size * 8 / duration))
        metrics = reader.metrics
        return {
            "format": raw_format,
            "streams": streams,
            "chapters": [],
            "_iso_probe": {
                "method": "udf_bluray",
                "udf_partition": iso.partition_mode,
                "wrapper_depth": wrapper_depth,
                "nested_iso_count": nested_iso_count,
                "playlist_count": len(playlists),
                "playlist_item_count": selection.playlist_items,
                "stream_file_count": len(stream_entries),
                "selection_basis": selection.basis,
                "stream_probe_method": stream_probe_method,
                "duration_method": duration_method,
                "range_requests": metrics.requests,
                "range_bytes": metrics.bytes_received,
            },
        }

    def _probe_m2ts_sample(
        self,
        iso: _UdfIsoReader,
        ranges: Sequence[tuple[int, int]],
        file_size: int,
        *,
        cancel: CancelCheck | None,
    ) -> tuple[list[dict[str, Any]], bytes]:
        if not ranges or file_size <= 0:
            raise _ProcessFailure
        probe_size = min(int(file_size), self._limits.max_m2ts_sample_bytes)
        sample = _read_file_slice(iso, ranges, 0, probe_size)
        if len(sample) != probe_size:
            raise _ProcessFailure
        outcome = self._processes.run(
            ControlledProcessProfile.FFPROBE,
            (
                "-hide_banner",
                "-v",
                "error",
                "-f",
                "mpegts",
                "-analyzeduration",
                "20000000",
                "-probesize",
                str(probe_size),
                "-print_format",
                "json",
                "-show_streams",
                "pipe:0",
            ),
            stdin=sample,
            max_stdin_bytes=self._limits.max_m2ts_sample_bytes,
            max_stdout_bytes=_FFPROBE_STDOUT_BYTES,
            max_stderr_bytes=_FFPROBE_STDERR_BYTES,
            timeout_seconds=self._limits.process_timeout_seconds,
            output=ControlledProcessOutput.JSON,
            cancel=cancel,
        )
        if outcome.status is ControlledProcessStatus.CANCELLED:
            raise _ProbeCancelled
        parsed = outcome.parsed_output
        if not outcome.succeeded or not isinstance(parsed, Mapping):
            raise _ProcessFailure
        raw_streams = parsed.get("streams")
        if (
            not isinstance(raw_streams, list)
            or len(raw_streams) > _MAX_FFPROBE_STREAMS
            or any(not isinstance(stream, Mapping) for stream in raw_streams)
        ):
            raise _ProcessFailure
        streams = [dict(stream) for stream in raw_streams]
        if not _has_video_stream(streams):
            raise _ProcessFailure
        return streams, sample


class _ParseFailure(RuntimeError):
    pass


class _ProcessFailure(RuntimeError):
    pass


class _ProbeCancelled(RuntimeError):
    pass


class _CachedRangeReader:
    def __init__(
        self,
        reader: _ExactRangeReader,
        *,
        max_cached_bytes: int,
        cancel: CancelCheck | None,
    ) -> None:
        self._reader = reader
        self._max_cached_bytes = int(max_cached_bytes)
        self._cancel = cancel
        self._cached_bytes = 0
        self._intervals: list[tuple[int, int, bytes]] = []

    @property
    def metrics(self) -> RangeReadMetrics:
        return self._reader.metrics

    def read(self, start: int, length: int) -> bytes:
        _checkpoint(self._cancel)
        normalized_start, normalized_length = _checked_range(start, length)
        end = normalized_start + normalized_length
        for cached_start, cached_end, data in self._intervals:
            if normalized_start >= cached_start and end <= cached_end:
                return data[normalized_start - cached_start : end - cached_start]
        return self._reader.read(normalized_start, normalized_length)

    def prefetch(
        self,
        ranges: Sequence[tuple[int, int]],
        *,
        max_gap: int,
        max_chunk: int,
    ) -> None:
        clean: list[tuple[int, int]] = []
        for start, length in ranges:
            normalized_start, normalized_length = _checked_range(start, length)
            clean.append((normalized_start, normalized_start + normalized_length))
        if not clean:
            return
        clean.sort()
        merged: list[tuple[int, int]] = []
        current_start, current_end = clean[0]
        for start, end in clean[1:]:
            if start <= current_end + max_gap and end - current_start <= max_chunk:
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))
        for start, end in merged:
            _checkpoint(self._cancel)
            if any(start >= cached_start and end <= cached_end for cached_start, cached_end, _ in self._intervals):
                continue
            data = self._reader.read(start, end - start)
            self._store(start, data)

    def _store(self, start: int, data: bytes) -> None:
        if len(data) > self._max_cached_bytes:
            return
        while self._intervals and self._cached_bytes + len(data) > self._max_cached_bytes:
            _, _, removed = self._intervals.pop(0)
            self._cached_bytes -= len(removed)
        self._intervals.append((start, start + len(data), data))
        self._cached_bytes += len(data)


class _MappedRangeReader:
    def __init__(
        self,
        reader: _ExactRangeReader,
        ranges: Sequence[tuple[int, int]],
        *,
        logical_size: int,
    ) -> None:
        if len(ranges) > _MAX_ALLOCATION_DESCRIPTORS:
            raise _ParseFailure
        self._reader = reader
        self._ranges = tuple((_checked_range(start, length)) for start, length in ranges)
        self._physical_size = sum(length for _, length in self._ranges)
        self._logical_size = int(logical_size)
        if self._logical_size <= 0 or self._logical_size > self._physical_size:
            raise _ParseFailure

    @property
    def metrics(self) -> RangeReadMetrics:
        return self._reader.metrics

    def read(self, start: int, length: int) -> bytes:
        normalized_start, normalized_length = _checked_range(start, length)
        if normalized_start + normalized_length > self._logical_size:
            raise _ParseFailure
        remaining = normalized_length
        logical_cursor = 0
        target = normalized_start
        chunks: list[bytes] = []
        for physical_start, span in self._ranges:
            if target >= logical_cursor + span:
                logical_cursor += span
                continue
            local_start = max(0, target - logical_cursor)
            take = min(span - local_start, remaining)
            chunks.append(self._reader.read(physical_start + local_start, take))
            target += take
            remaining -= take
            if remaining == 0:
                break
            logical_cursor += span
        if remaining:
            raise _ParseFailure
        return b"".join(chunks)


@dataclass(frozen=True, slots=True)
class _UdfEntry:
    name: str
    flags: int
    extent_length: int
    lbn: int
    part: int

    @property
    def is_directory(self) -> bool:
        return bool(self.flags & 2)


@dataclass(frozen=True, slots=True)
class _FileEntryFields:
    allocation_type: int
    info_length: int
    allocation_length: int
    allocation_start: int


@dataclass(frozen=True, slots=True)
class _AllocationDescriptor:
    extent_type: int
    length: int
    lbn: int
    part: int | None
    embedded_start: int | None = None


class _UdfIsoReader:
    def __init__(self, reader: _CachedRangeReader | _MappedRangeReader) -> None:
        self.reader = reader
        self._part_start = 0
        self._physical_part = -1
        self._metadata_part = -1
        self._fsd_length = 0
        self._fsd_lbn = 0
        self._fsd_part = 0
        self._metadata = b""
        self._entry_cache: dict[tuple[int, int, int], tuple[_FileEntryFields, tuple[_AllocationDescriptor, ...]]] = {}
        self._directory_cache: dict[tuple[int, int, int], tuple[_UdfEntry, ...]] = {}
        self._init_udf()

    @property
    def metrics(self) -> RangeReadMetrics:
        return self.reader.metrics

    @property
    def partition_mode(self) -> str:
        return "metadata" if self._fsd_part == self._metadata_part else "physical"

    def read(self, start: int, length: int) -> bytes:
        return self.reader.read(start, length)

    def sector(self, number: int, count: int = 1) -> bytes:
        if number < 0 or count <= 0 or count > _MAX_UDF_DESCRIPTORS:
            raise _ParseFailure
        return self.read(number * _SECTOR_BYTES, count * _SECTOR_BYTES)

    def root(self) -> _UdfEntry:
        fsd = self.read_partition(self._fsd_part, self._fsd_lbn, max(_SECTOR_BYTES, self._fsd_length))
        return _UdfEntry(
            "/",
            2,
            _le32(fsd, 400) & 0x3FFFFFFF,
            _le32(fsd, 404),
            _le16(fsd, 408),
        )

    def list_dir(self, entry: _UdfEntry | None) -> list[_UdfEntry]:
        if entry is None or not entry.is_directory:
            return []
        key = (entry.part, entry.lbn, entry.extent_length)
        cached = self._directory_cache.get(key)
        if cached is not None:
            return list(cached)
        data = self.file_data(entry, max_bytes=_MAX_DIRECTORY_BYTES)
        parsed = tuple(_parse_directory(data))
        if len(self._directory_cache) >= _MAX_DIRECTORY_ENTRIES:
            raise _ParseFailure
        self._directory_cache[key] = parsed
        return list(parsed)

    def find(self, entry: _UdfEntry, name: str) -> _UdfEntry | None:
        target = str(name or "").casefold()
        return next((child for child in self.list_dir(entry) if child.name.casefold() == target), None)

    def entry_info(self, entry: _UdfEntry) -> tuple[_FileEntryFields, tuple[_AllocationDescriptor, ...]]:
        key = (entry.part, entry.lbn, entry.extent_length)
        cached = self._entry_cache.get(key)
        if cached is not None:
            return cached
        length = max(_SECTOR_BYTES, int(entry.extent_length))
        if length > _MAX_FILE_ENTRY_BYTES:
            raise _ParseFailure
        raw = self.read_partition(entry.part, entry.lbn, length)
        value = (_file_entry_fields(raw), _allocation_descriptors(raw))
        if len(self._entry_cache) >= _MAX_DIRECTORY_ENTRIES:
            raise _ParseFailure
        self._entry_cache[key] = value
        return value

    def file_data(self, entry: _UdfEntry, *, max_bytes: int) -> bytes:
        fields, descriptors = self.entry_info(entry)
        if fields.info_length < 0 or fields.info_length > max_bytes:
            raise _ParseFailure
        if fields.allocation_type == 3:
            descriptor = descriptors[0] if descriptors else None
            if descriptor is None or descriptor.embedded_start is None:
                raise _ParseFailure
            raw = self.read_partition(entry.part, entry.lbn, max(_SECTOR_BYTES, entry.extent_length))
            end = descriptor.embedded_start + fields.info_length
            if end > len(raw):
                raise _ParseFailure
            return raw[descriptor.embedded_start : end]

        chunks: list[bytes] = []
        remaining = fields.info_length
        for descriptor in descriptors:
            if remaining <= 0:
                break
            if descriptor.extent_type != 0 or descriptor.length <= 0:
                raise _ParseFailure
            part = entry.part if descriptor.part is None else descriptor.part
            take = min(descriptor.length, remaining)
            chunks.append(self.read_partition(part, descriptor.lbn, take))
            remaining -= take
        if remaining:
            raise _ParseFailure
        return b"".join(chunks)

    def physical_ranges_for_file(self, entry: _UdfEntry) -> tuple[tuple[int, int], ...]:
        fields, descriptors = self.entry_info(entry)
        if fields.allocation_type == 3:
            return ()
        ranges: list[tuple[int, int]] = []
        remaining = fields.info_length
        for descriptor in descriptors:
            part = entry.part if descriptor.part is None else descriptor.part
            if descriptor.extent_type != 0 or part != self._physical_part or descriptor.length <= 0:
                return ()
            take = min(descriptor.length, remaining)
            if take > 0:
                ranges.append((self._physical_offset(descriptor.lbn), take))
                remaining -= take
            if remaining == 0:
                break
        if remaining:
            return ()
        if len(ranges) > _MAX_ALLOCATION_DESCRIPTORS:
            raise _ParseFailure
        return tuple(ranges)

    def prefetch_file_entries(
        self,
        entries: Sequence[_UdfEntry],
        *,
        max_gap: int,
        max_chunk: int,
    ) -> None:
        if not isinstance(self.reader, _CachedRangeReader):
            return
        ranges = [
            (self._physical_offset(entry.lbn), max(_SECTOR_BYTES, entry.extent_length))
            for entry in entries
            if entry.part == self._physical_part and max(_SECTOR_BYTES, entry.extent_length) <= _MAX_FILE_ENTRY_BYTES
        ]
        self.reader.prefetch(ranges, max_gap=max_gap, max_chunk=max_chunk)

    def prefetch_files(
        self,
        entries: Sequence[_UdfEntry],
        *,
        max_gap: int,
        max_chunk: int,
    ) -> None:
        if not isinstance(self.reader, _CachedRangeReader):
            return
        ranges = []
        for entry in entries:
            fields, _ = self.entry_info(entry)
            if fields.info_length <= max_chunk:
                ranges.extend(self.physical_ranges_for_file(entry))
        self.reader.prefetch(ranges, max_gap=max_gap, max_chunk=max_chunk)

    def read_partition(self, part: int, lbn: int, length: int) -> bytes:
        if lbn < 0 or length <= 0:
            raise _ParseFailure
        if part == self._physical_part:
            return self.read(self._physical_offset(lbn), length)
        if part == self._metadata_part:
            start = lbn * _SECTOR_BYTES
            end = start + length
            if start < 0 or end > len(self._metadata):
                raise _ParseFailure
            return self._metadata[start:end]
        raise _ParseFailure

    def _physical_offset(self, lbn: int) -> int:
        offset = (self._part_start + int(lbn)) * _SECTOR_BYTES
        if offset < 0 or offset >= _MAX_OFFSET:
            raise _ParseFailure
        return offset

    def _init_udf(self) -> None:
        avdp = self.sector(256)
        if _le16(avdp, 0) != 2:
            raise _ParseFailure
        descriptor_length = _le32(avdp, 16)
        descriptor_lbn = _le32(avdp, 20)
        if (
            descriptor_length <= 0
            or descriptor_length > _MAX_UDF_DESCRIPTORS * _SECTOR_BYTES
            or descriptor_length % _SECTOR_BYTES
        ):
            raise _ParseFailure
        descriptors = self.read(descriptor_lbn * _SECTOR_BYTES, descriptor_length)

        logical_volume: bytes | None = None
        partition_found = False
        for offset in range(0, len(descriptors), _SECTOR_BYTES):
            descriptor = descriptors[offset : offset + _SECTOR_BYTES]
            tag = _le16(descriptor, 0)
            if tag == 5:
                self._part_start = _le32(descriptor, 188)
                partition_found = True
            elif tag == 6:
                logical_volume = descriptor
            elif tag == 8:
                break
        if logical_volume is None or not partition_found:
            raise _ParseFailure

        self._fsd_length = _le32(logical_volume, 248) & 0x3FFFFFFF
        self._fsd_lbn = _le32(logical_volume, 252)
        self._fsd_part = _le16(logical_volume, 256)
        if self._fsd_length <= 0 or self._fsd_length > _MAX_FILE_ENTRY_BYTES:
            raise _ParseFailure

        maps_length = _le32(logical_volume, 264)
        maps_count = _le32(logical_volume, 268)
        maps_start = 440
        maps_end = maps_start + maps_length
        if maps_count <= 0 or maps_count > _MAX_PARTITION_MAPS or maps_end > len(logical_volume):
            raise _ParseFailure
        metadata_file_lbn: int | None = None
        position = maps_start
        for map_index in range(maps_count):
            if position + 2 > maps_end:
                raise _ParseFailure
            map_type = logical_volume[position]
            map_length = logical_volume[position + 1]
            if map_length < 2 or position + map_length > maps_end:
                raise _ParseFailure
            raw = logical_volume[position : position + map_length]
            if map_type == 1 and map_length >= 6 and self._physical_part < 0:
                self._physical_part = map_index
            elif map_type == 2 and map_length >= 44:
                identifier = raw[5:28].rstrip(b"\0").decode("latin1", "ignore")
                if "Metadata Partition" in identifier:
                    self._metadata_part = map_index
                    metadata_file_lbn = _le32(raw, 40)
            position += map_length
        if position != maps_end or self._physical_part < 0:
            raise _ParseFailure

        if metadata_file_lbn is not None:
            metadata_entry = self.read(self._physical_offset(metadata_file_lbn), _SECTOR_BYTES)
            fields = _file_entry_fields(metadata_entry)
            descriptors = _allocation_descriptors(metadata_entry)
            if fields.info_length <= 0 or fields.info_length > _MAX_METADATA_BYTES:
                raise _ParseFailure
            chunks: list[bytes] = []
            remaining = fields.info_length
            for descriptor in descriptors:
                if (
                    descriptor.extent_type != 0
                    or descriptor.length <= 0
                    or descriptor.part not in {None, self._physical_part}
                ):
                    raise _ParseFailure
                take = min(descriptor.length, remaining)
                chunks.append(self.read(self._physical_offset(descriptor.lbn), take))
                remaining -= take
                if remaining == 0:
                    break
            if remaining:
                raise _ParseFailure
            self._metadata = b"".join(chunks)
        if self._fsd_part not in {self._physical_part, self._metadata_part}:
            raise _ParseFailure
        if self._fsd_part == self._metadata_part and not self._metadata:
            raise _ParseFailure


def _file_entry_fields(data: bytes) -> _FileEntryFields:
    tag = _le16(data, 0)
    allocation_type = _le16(data, 34) & 7
    if tag == 261:
        extended_length = _le32(data, 168)
        info_length = _le64(data, 56)
        allocation_length = _le32(data, 172)
        allocation_start = 176 + extended_length
    elif tag == 266:
        extended_length = _le32(data, 208)
        info_length = _le64(data, 56)
        allocation_length = _le32(data, 212)
        allocation_start = 216 + extended_length
    else:
        raise _ParseFailure
    if allocation_type not in {0, 1, 3}:
        raise _ParseFailure
    if allocation_length < 0 or allocation_start + allocation_length > len(data):
        raise _ParseFailure
    return _FileEntryFields(allocation_type, info_length, allocation_length, allocation_start)


def _allocation_descriptors(data: bytes) -> tuple[_AllocationDescriptor, ...]:
    fields = _file_entry_fields(data)
    if fields.allocation_type == 3:
        if fields.info_length > fields.allocation_length:
            raise _ParseFailure
        return (_AllocationDescriptor(0, fields.allocation_length, 0, None, fields.allocation_start),)
    descriptor_bytes = 8 if fields.allocation_type == 0 else 16
    if fields.allocation_length % descriptor_bytes:
        raise _ParseFailure
    count = fields.allocation_length // descriptor_bytes
    if count > _MAX_ALLOCATION_DESCRIPTORS:
        raise _ParseFailure
    result: list[_AllocationDescriptor] = []
    position = fields.allocation_start
    for _ in range(count):
        raw_length = _le32(data, position)
        lbn = _le32(data, position + 4)
        part = _le16(data, position + 8) if fields.allocation_type == 1 else None
        position += descriptor_bytes
        length = raw_length & 0x3FFFFFFF
        if raw_length or lbn:
            result.append(_AllocationDescriptor(raw_length >> 30, length, lbn, part))
    return tuple(result)


def _parse_directory(data: bytes) -> list[_UdfEntry]:
    if len(data) > _MAX_DIRECTORY_BYTES:
        raise _ParseFailure
    position = 0
    result: list[_UdfEntry] = []
    while position + 38 <= len(data):
        if _le16(data, position) == 0 and not data[position : min(len(data), position + 64)].strip(b"\0"):
            break
        if _le16(data, position) != 257:
            raise _ParseFailure
        name_length = data[position + 19]
        implementation_length = _le16(data, position + 36)
        record_length = (38 + implementation_length + name_length + 3) & ~3
        if record_length < 40 or position + record_length > len(data):
            raise _ParseFailure
        name_start = position + 38 + implementation_length
        raw_name = data[name_start : name_start + name_length]
        name = _decode_udf_name(raw_name)
        if name:
            result.append(
                _UdfEntry(
                    name,
                    data[position + 18],
                    _le32(data, position + 20) & 0x3FFFFFFF,
                    _le32(data, position + 24),
                    _le16(data, position + 28),
                )
            )
            if len(result) > _MAX_DIRECTORY_ENTRIES:
                raise _ParseFailure
        position += record_length
    return result


def _decode_udf_name(raw: bytes) -> str:
    if not raw:
        return ""
    compression = raw[0]
    payload = raw[1:]
    if compression == 8:
        return payload.decode("latin1", "ignore")
    if compression == 16:
        return payload.decode("utf-16-be", "ignore")
    return ""


@dataclass(frozen=True, slots=True)
class _Playlist:
    clips: tuple[tuple[str, float], ...]
    duration: float
    unique_size: int
    max_clip_size: int


@dataclass(frozen=True, slots=True)
class _MainSelection:
    main_clip: str
    duration: float
    basis: str
    playlist_items: int


def _parse_mpls(data: bytes, stream_sizes: Mapping[str, int]) -> _Playlist | None:
    if len(data) > _MAX_PLAYLIST_BYTES or not data.startswith(b"MPLS") or len(data) < 64:
        return None
    playlist_start = _be32(data, 8)
    if playlist_start <= 0 or playlist_start + 10 > len(data):
        return None
    item_count = _be16(data, playlist_start + 6)
    if item_count <= 0 or item_count > _MAX_PLAYLIST_ITEMS:
        return None
    position = playlist_start + 10
    clips: list[tuple[str, float]] = []
    duration = 0.0
    for _ in range(item_count):
        if position + 22 > len(data):
            return None
        item_length = _be16(data, position)
        item_end = position + 2 + item_length
        if item_length < 20 or item_end > len(data):
            return None
        raw_clip = data[position + 2 : position + 7]
        try:
            clip_stem = raw_clip.decode("ascii")
        except UnicodeDecodeError:
            return None
        if re.fullmatch(r"[A-Za-z0-9_-]{5}", clip_stem) is None:
            return None
        clip = f"{clip_stem}.M2TS".upper()
        in_time = _be32(data, position + 14)
        out_time = _be32(data, position + 18)
        seconds = max(0, out_time - in_time) / 45_000.0
        duration += seconds
        clips.append((clip, seconds))
        position = item_end
    unique = {clip for clip, _ in clips}
    return _Playlist(
        tuple(clips),
        duration,
        sum(int(stream_sizes.get(name, 0)) for name in unique),
        max((int(stream_sizes.get(name, 0)) for name in unique), default=0),
    )


def _select_main_stream(playlists: Sequence[_Playlist], stream_sizes: Mapping[str, int]) -> _MainSelection:
    if not stream_sizes:
        raise _ParseFailure
    largest_stream = max(stream_sizes, key=lambda name: int(stream_sizes[name]))
    if not playlists:
        return _MainSelection(largest_stream, 0.0, "largest_stream", 0)
    ordered = sorted(
        playlists,
        key=lambda item: (item.max_clip_size, item.unique_size, item.duration),
        reverse=True,
    )
    referenced = {clip for playlist in ordered for clip, _ in playlist.clips}
    largest_playlist_size = max(playlist.unique_size for playlist in ordered)
    if largest_stream not in referenced and int(stream_sizes[largest_stream]) > largest_playlist_size * 2:
        return _MainSelection(largest_stream, 0.0, "largest_unreferenced", 0)
    selected = ordered[0]
    main_clip = max({clip for clip, _ in selected.clips}, key=lambda name: int(stream_sizes.get(name, 0)))
    return _MainSelection(main_clip, selected.duration, "playlist_max_clip", len(selected.clips))


def _find_bdmv_dir(
    iso: _UdfIsoReader,
    root: _UdfEntry,
    *,
    max_depth: int,
) -> tuple[_UdfEntry | None, int]:
    queue: list[tuple[_UdfEntry, int]] = [(root, 0)]
    candidates: list[tuple[tuple[int, int, int, int], _UdfEntry, int]] = []
    seen: set[tuple[int, int]] = set()
    while queue:
        if len(seen) >= _MAX_WRAPPER_DIRECTORIES:
            raise _ParseFailure
        current, depth = queue.pop(0)
        key = (current.part, current.lbn)
        if key in seen:
            continue
        seen.add(key)
        for child in iso.list_dir(current):
            if not child.is_directory:
                continue
            if child.name.casefold() == "bdmv":
                counts = []
                for name in ("PLAYLIST", "STREAM", "CLIPINF"):
                    directory = iso.find(child, name)
                    counts.append(len(iso.list_dir(directory)) if directory is not None else -1)
                score = (
                    sum(count > 0 for count in counts),
                    sum(count >= 0 for count in counts),
                    sum(max(0, count) for count in counts),
                    -depth,
                )
                candidates.append((score, child, depth))
            elif depth < max_depth:
                queue.append((child, depth + 1))
    if not candidates:
        return None, 0
    _, candidate, depth = max(candidates, key=lambda item: item[0])
    return candidate, depth


def _open_single_nested_iso(iso: _UdfIsoReader, root: _UdfEntry) -> _UdfIsoReader | None:
    candidates = [
        entry for entry in iso.list_dir(root) if not entry.is_directory and entry.name.casefold().endswith(".iso")
    ]
    if len(candidates) != 1:
        return None
    entry = candidates[0]
    fields, _ = iso.entry_info(entry)
    if fields.info_length <= 0 or fields.info_length > _MAX_NESTED_ISO_BYTES:
        return None
    ranges = iso.physical_ranges_for_file(entry)
    if not ranges or sum(length for _, length in ranges) < fields.info_length:
        return None
    mapped = _MappedRangeReader(iso.reader, ranges, logical_size=fields.info_length)
    return _UdfIsoReader(mapped)


def _media_files(entries: Sequence[_UdfEntry], *, suffix: str) -> list[_UdfEntry]:
    normalized = suffix.casefold()
    return [entry for entry in entries if not entry.is_directory and entry.name.casefold().endswith(normalized)]


def _parse_clpi_streams(data: bytes) -> list[dict[str, Any]]:
    if len(data) > _MAX_CLPI_BYTES or not data.startswith(b"HDMV") or len(data) < 64:
        return []
    program_start = _be32(data, 12)
    if program_start <= 0 or program_start + 8 > len(data):
        return []
    program_length = _be32(data, program_start)
    program_end = program_start + 4 + program_length
    if program_end > len(data):
        return []

    def score(items: Sequence[Mapping[str, Any]]) -> int:
        return sum(1 for item in items if item.get("codec_type") in {"video", "audio", "subtitle"})

    best: list[dict[str, Any]] = []
    for delta in range(10, 20):
        for mode in ("payload", "total"):
            position = program_start + delta
            items: list[dict[str, Any]] = []
            while position + 3 <= program_end and len(items) < _MAX_CLPI_STREAMS:
                pid = _be16(data, position)
                length = data[position + 2]
                if length < 4:
                    break
                if mode == "payload":
                    raw = data[position + 3 : position + 3 + length]
                    next_position = position + 3 + length
                else:
                    raw = data[position + 3 : position + length]
                    next_position = position + length
                if next_position > program_end or next_position <= position or not raw:
                    break
                item = _clpi_stream(raw, pid, len(items))
                if item is None:
                    break
                items.append(item)
                position = next_position
            if score(items) > score(best):
                best = items
    return [item for item in best if item["codec_type"] in {"video", "audio", "subtitle"}]


def _clpi_stream(raw: bytes, pid: int, index: int) -> dict[str, Any] | None:
    kind, codec, profile = _codec_from_clpi(raw[0])
    item: dict[str, Any] = {
        "index": index,
        "id": pid,
        "codec_type": kind,
        "codec_name": codec,
        "profile": profile,
        "tags": {},
        "disposition": {},
    }
    if kind == "other":
        return item
    if kind == "video":
        if len(raw) < 4:
            return None
        video_format = (raw[1] >> 4) & 0x0F
        frame_rate_code = raw[1] & 0x0F
        width, height = {
            1: (720, 480),
            2: (720, 576),
            3: (720, 480),
            4: (1920, 1080),
            5: (1280, 720),
            6: (1920, 1080),
            7: (720, 576),
            8: (3840, 2160),
        }.get(video_format, (0, 0))
        frame_rate = {
            1: "24000/1001",
            2: "24/1",
            3: "25/1",
            4: "30000/1001",
            6: "50/1",
            7: "60000/1001",
        }.get(frame_rate_code, "0/0")
        item.update(
            {
                "width": width,
                "height": height,
                "avg_frame_rate": frame_rate,
                "r_frame_rate": frame_rate,
                "display_aspect_ratio": "16:9" if (raw[2] >> 4) == 3 else "",
            }
        )
        if codec == "hevc":
            item["pix_fmt"] = "yuv420p10le"
            item["bits_per_raw_sample"] = "10"
    elif kind == "audio":
        if len(raw) < 5:
            return None
        language = raw[2:5].decode("latin1", "ignore").strip("\0")
        channels = _audio_channels(raw[1], codec)
        item["tags"] = {"language": language}
        item["channels"] = channels
        item["sample_rate"] = _sample_rate(raw[1])
        if channels:
            item["channel_layout"] = {8: "7.1", 6: "5.1", 2: "stereo", 1: "mono"}.get(channels, "")
    else:
        if len(raw) < 4:
            return None
        item["tags"] = {"language": raw[1:4].decode("latin1", "ignore").strip("\0")}
    return item


def _codec_from_clpi(coding_type: int) -> tuple[str, str, str]:
    video = {0x01: "mpeg1video", 0x02: "mpeg2video", 0x1B: "h264", 0xEA: "vc1", 0x24: "hevc"}
    audio = {
        0x80: ("pcm_bluray", ""),
        0x81: ("ac3", ""),
        0x82: ("dts", ""),
        0x83: ("truehd", "Dolby TrueHD"),
        0x84: ("eac3", ""),
        0x85: ("dts", "DTS-HD HRA"),
        0x86: ("dts", "DTS-HD MA"),
        0xA1: ("eac3", "Secondary Audio"),
        0xA2: ("dts", "Secondary Audio"),
    }
    subtitle = {0x90: "hdmv_pgs_subtitle", 0x91: "hdmv_pgs_subtitle", 0x92: "text"}
    if coding_type in video:
        return "video", video[coding_type], ""
    if coding_type in audio:
        codec, profile = audio[coding_type]
        return "audio", codec, profile
    if coding_type in subtitle:
        return "subtitle", subtitle[coding_type], ""
    return "other", f"0x{coding_type:02x}", ""


def _audio_channels(format_rate: int, codec: str) -> int:
    del codec
    audio_format = (format_rate >> 4) & 0x0F
    if audio_format == 1:
        return 1
    if audio_format == 3:
        return 2
    if audio_format == 6:
        return 6
    if audio_format == 12:
        return 8
    return 0


def _sample_rate(format_rate: int) -> str:
    return {1: "48000", 4: "96000", 5: "192000"}.get(format_rate & 0x0F, "")


def _read_file_slice(
    iso: _UdfIsoReader,
    ranges: Sequence[tuple[int, int]],
    offset: int,
    length: int,
) -> bytes:
    normalized_offset = max(0, int(offset))
    remaining = max(0, int(length))
    logical_cursor = 0
    chunks: list[bytes] = []
    for physical_start, span in ranges:
        if remaining <= 0:
            break
        if normalized_offset >= logical_cursor + span:
            logical_cursor += span
            continue
        local_start = max(0, normalized_offset - logical_cursor)
        take = min(span - local_start, remaining)
        if take > 0:
            chunks.append(iso.read(physical_start + local_start, take))
            normalized_offset += take
            remaining -= take
        logical_cursor += span
    if remaining:
        raise _ParseFailure
    return b"".join(chunks)


@dataclass(frozen=True, slots=True)
class _PcrSample:
    first: int
    last: int
    count: int


def _m2ts_pcr_values(data: bytes, logical_offset: int) -> dict[int, _PcrSample]:
    values: dict[int, _PcrSample] = {}
    first_packet = (-int(logical_offset)) % _M2TS_PACKET_BYTES
    for offset in range(first_packet, len(data) - _M2TS_PACKET_BYTES + 1, _M2TS_PACKET_BYTES):
        packet = data[offset + 4 : offset + _M2TS_PACKET_BYTES]
        if len(packet) != 188 or packet[0] != 0x47 or not (packet[3] & 0x20):
            continue
        adaptation_length = packet[4]
        if adaptation_length < 7 or adaptation_length > 183 or not (packet[5] & 0x10):
            continue
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        base = (packet[6] << 25) | (packet[7] << 17) | (packet[8] << 9) | (packet[9] << 1) | (packet[10] >> 7)
        extension = ((packet[10] & 1) << 8) | packet[11]
        value = base * 300 + extension
        current = values.get(pid)
        values[pid] = (
            _PcrSample(value, value, 1) if current is None else _PcrSample(current.first, value, current.count + 1)
        )
    return values


def _m2ts_duration_from_pcr(
    iso: _UdfIsoReader,
    ranges: Sequence[tuple[int, int]],
    file_size: int,
    *,
    sample_size: int,
    head: bytes | None = None,
) -> float:
    if not ranges or file_size <= 0:
        return 0.0
    bounded_sample = min(int(sample_size), int(file_size))
    tail_offset = max(0, int(file_size) - bounded_sample)
    head_data = head[:bounded_sample] if head is not None and len(head) >= bounded_sample else None
    if head_data is None:
        head_data = _read_file_slice(iso, ranges, 0, bounded_sample)
    tail_data = head_data if tail_offset == 0 else _read_file_slice(iso, ranges, tail_offset, bounded_sample)
    head_values = _m2ts_pcr_values(head_data, 0)
    tail_values = _m2ts_pcr_values(tail_data, tail_offset)
    common = set(head_values) & set(tail_values)
    if not common:
        return 0.0
    pid = max(common, key=lambda item: head_values[item].count + tail_values[item].count)
    first = head_values[pid].first
    last = tail_values[pid].last
    wrap = (1 << 33) * 300
    if last < first:
        last += wrap
    duration = (last - first) / 27_000_000.0
    return duration if 0 < duration < 24 * 60 * 60 else 0.0


def _has_video_stream(streams: Sequence[Mapping[str, Any]]) -> bool:
    return any(str(stream.get("codec_type") or "").casefold() == "video" for stream in streams)


def _checked_range(start: int, length: int) -> tuple[int, int]:
    normalized_start = int(start)
    normalized_length = int(length)
    if normalized_start < 0 or normalized_length <= 0 or normalized_start + normalized_length > _MAX_OFFSET:
        raise _ParseFailure
    return normalized_start, normalized_length


def _le16(data: bytes, offset: int) -> int:
    return _integer(data, offset, 2, "little")


def _le32(data: bytes, offset: int) -> int:
    return _integer(data, offset, 4, "little")


def _le64(data: bytes, offset: int) -> int:
    return _integer(data, offset, 8, "little")


def _be16(data: bytes, offset: int) -> int:
    return _integer(data, offset, 2, "big")


def _be32(data: bytes, offset: int) -> int:
    return _integer(data, offset, 4, "big")


def _integer(data: bytes, offset: int, length: int, byteorder: Literal["little", "big"]) -> int:
    if offset < 0 or offset + length > len(data):
        raise _ParseFailure
    return int.from_bytes(data[offset : offset + length], byteorder)


def _cancelled(cancel: CancelCheck | None) -> bool:
    return bool(cancel is not None and cancel())


def _checkpoint(cancel: CancelCheck | None) -> None:
    if _cancelled(cancel):
        raise _ProbeCancelled


__all__ = [
    "NativeP115IsoProbeGateway",
    "P115IsoProbeLimits",
    "P115IsoProbeOutcome",
    "P115IsoProbeStatus",
    "P115IsoRangeReaderFactory",
]
