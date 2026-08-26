from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import pytest

from etk.domains.p115.iso_probe import (
    NativeP115IsoProbeGateway,
    P115IsoProbeOutcome,
    P115IsoProbeStatus,
)
from etk.domains.p115.media_info_adapters import (
    ControlledProcessOutcome,
    ControlledProcessStatus,
    P115DirectUrl,
    RangeReadError,
    RangeReadErrorKind,
    RangeReadMetrics,
)
from etk.domains.p115.media_info_formatter import (
    P115MediaInfoPolicySnapshot,
    format_raw_ffprobe_mediainfo,
)
from etk.domains.p115.media_probe import P115MediaProbeRequest

_SECTOR = 2048
_SHA1 = "A" * 40


@dataclass
class _Node:
    name: str
    children: list[_Node] | None = None
    data: bytes = b""
    declared_size: int | None = None
    fe_lbn: int = 0
    data_lbn: int = 0

    @property
    def directory(self) -> bool:
        return self.children is not None


def _directory(name: str, *children: _Node) -> _Node:
    return _Node(name, list(children))


def _file(name: str, data: bytes, *, declared_size: int | None = None) -> _Node:
    return _Node(name, None, data, declared_size)


class _MemoryRangeReader:
    def __init__(
        self,
        content: bytes,
        *,
        max_requests: int = 4096,
        max_bytes: int = 256 * 1024 * 1024,
        cancel: Callable[[], bool] | None = None,
    ) -> None:
        self.content = content
        self.max_requests = max_requests
        self.max_bytes = max_bytes
        self.cancel = cancel
        self.requests = 0
        self.bytes_received = 0

    @property
    def metrics(self) -> RangeReadMetrics:
        return RangeReadMetrics(self.requests, self.bytes_received)

    def read(self, start: int, length: int) -> bytes:
        if self.cancel is not None and self.cancel():
            raise RangeReadError(RangeReadErrorKind.CANCELLED, "fixture range cancelled")
        if self.requests >= self.max_requests:
            raise RangeReadError(RangeReadErrorKind.REQUEST_LIMIT, "fixture request budget")
        if self.bytes_received + length > self.max_bytes:
            raise RangeReadError(RangeReadErrorKind.BYTE_LIMIT, "fixture byte budget")
        end = start + length
        if start < 0 or length <= 0 or end > len(self.content):
            raise RangeReadError(RangeReadErrorKind.LENGTH, "fixture exact range failed")
        self.requests += 1
        self.bytes_received += length
        return self.content[start:end]


class _FailingRangeReader:
    def __init__(self, kind: RangeReadErrorKind) -> None:
        self.kind = kind

    @property
    def metrics(self) -> RangeReadMetrics:
        return RangeReadMetrics(0, 0)

    def read(self, _start: int, _length: int) -> bytes:
        raise RangeReadError(self.kind, "https://secret.invalid Cookie: secret-value")


class _FakeProcesses:
    def __init__(self, outcomes: Sequence[ControlledProcessOutcome] = ()) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def run(self, profile: object, arguments: Sequence[str], **kwargs: Any) -> ControlledProcessOutcome:
        self.calls.append({"profile": profile, "arguments": tuple(arguments), **kwargs})
        return self.outcomes.pop(0)


def _request(name: str = "Fixture.iso", *, size: int = 0) -> P115MediaProbeRequest:
    return P115MediaProbeRequest(
        account_id="115001",
        sha1=_SHA1,
        pick_code="pc-fixture",
        file_name=name,
        file_size=size,
    )


def _direct() -> P115DirectUrl:
    return P115DirectUrl(
        "https://cdn.fixture.invalid/disc.iso?token=must-not-survive",
        "Fixture-UA Cookie/secret",
        "openapi",
        "fixture-direct-url",
    )


def _gateway(
    image: bytes,
    *,
    processes: _FakeProcesses | None = None,
) -> tuple[NativeP115IsoProbeGateway, _MemoryRangeReader, _FakeProcesses]:
    reader = _MemoryRangeReader(image)
    fake_processes = processes or _FakeProcesses()
    gateway = NativeP115IsoProbeGateway(
        fake_processes,  # type: ignore[arg-type]
        range_reader_factory=lambda _direct_url, _cancel: reader,  # type: ignore[arg-type,return-value]
    )
    return gateway, reader, fake_processes


def _put(
    buffer: bytearray,
    offset: int,
    value: int,
    length: int,
    byteorder: Literal["little", "big"] = "little",
) -> None:
    buffer[offset : offset + length] = int(value).to_bytes(length, byteorder)


def _file_entry(
    *,
    info_length: int,
    data_lbn: int,
    allocation_length: int,
    data_part: int | None,
) -> bytes:
    entry = bytearray(_SECTOR)
    _put(entry, 0, 261, 2)
    _put(entry, 34, 1 if data_part is not None else 0, 2)
    _put(entry, 56, info_length, 8)
    if info_length:
        if data_part is None:
            _put(entry, 172, 8, 4)
            _put(entry, 176, allocation_length, 4)
            _put(entry, 180, data_lbn, 4)
        else:
            _put(entry, 172, 16, 4)
            _put(entry, 176, allocation_length, 4)
            _put(entry, 180, data_lbn, 4)
            _put(entry, 184, data_part, 2)
    return bytes(entry)


def _fid(node: _Node, *, part: int) -> bytes:
    raw_name = b"\x08" + node.name.encode("latin1")
    length = (38 + len(raw_name) + 3) & ~3
    record = bytearray(length)
    _put(record, 0, 257, 2)
    record[18] = 2 if node.directory else 0
    record[19] = len(raw_name)
    _put(record, 20, _SECTOR, 4)
    _put(record, 24, node.fe_lbn, 4)
    _put(record, 28, part, 2)
    record[38 : 38 + len(raw_name)] = raw_name
    return bytes(record)


def _build_udf(root: _Node, *, metadata_partition: bool = False) -> bytes:
    part_start = 300
    partition_part = 1 if metadata_partition else 0
    next_partition_lbn = 1
    next_physical_file_lbn = 1000

    def assign(node: _Node) -> None:
        nonlocal next_partition_lbn, next_physical_file_lbn
        node.fe_lbn = next_partition_lbn
        next_partition_lbn += 1
        if node.directory:
            for child in node.children or []:
                assign(child)
            records = b"".join(_fid(child, part=partition_part) for child in node.children or [])
            node.data = records
            if records:
                node.data_lbn = next_partition_lbn
                next_partition_lbn += max(1, (len(records) + _SECTOR - 1) // _SECTOR)
        elif metadata_partition:
            node.data_lbn = next_physical_file_lbn
            next_physical_file_lbn += max(1, (len(node.data) + _SECTOR - 1) // _SECTOR)
        else:
            node.data_lbn = next_partition_lbn
            next_partition_lbn += max(1, (len(node.data) + _SECTOR - 1) // _SECTOR)

    assign(root)
    image = bytearray((part_start + max(next_partition_lbn, 16)) * _SECTOR)

    avdp = bytearray(_SECTOR)
    _put(avdp, 0, 2, 2)
    _put(avdp, 16, 3 * _SECTOR, 4)
    _put(avdp, 20, 257, 4)
    image[256 * _SECTOR : 257 * _SECTOR] = avdp

    partition = bytearray(_SECTOR)
    _put(partition, 0, 5, 2)
    _put(partition, 188, part_start, 4)
    logical = bytearray(_SECTOR)
    _put(logical, 0, 6, 2)
    _put(logical, 248, _SECTOR, 4)
    _put(logical, 252, 0, 4)
    _put(logical, 256, partition_part, 2)
    if metadata_partition:
        physical_map = bytearray(6)
        physical_map[:2] = b"\x01\x06"
        metadata_map = bytearray(64)
        metadata_map[:2] = b"\x02\x40"
        metadata_map[5:28] = b"*UDF Metadata Partition"
        _put(metadata_map, 40, 10, 4)
        maps = bytes(physical_map + metadata_map)
        _put(logical, 264, len(maps), 4)
        _put(logical, 268, 2, 4)
    else:
        maps = b"\x01\x06\0\0\0\0"
        _put(logical, 264, len(maps), 4)
        _put(logical, 268, 1, 4)
    logical[440 : 440 + len(maps)] = maps
    terminator = bytearray(_SECTOR)
    _put(terminator, 0, 8, 2)
    descriptors = bytes(partition + logical + terminator)
    image[257 * _SECTOR : 260 * _SECTOR] = descriptors

    fsd = bytearray(_SECTOR)
    _put(fsd, 400, _SECTOR, 4)
    _put(fsd, 404, root.fe_lbn, 4)
    _put(fsd, 408, partition_part, 2)

    partition_blob = bytearray(next_partition_lbn * _SECTOR)
    partition_blob[:_SECTOR] = fsd

    def render(node: _Node) -> None:
        declared_size = len(node.data) if node.declared_size is None else node.declared_size
        data_part = 0 if metadata_partition and not node.directory else None
        allocation_length = declared_size
        entry = _file_entry(
            info_length=declared_size,
            data_lbn=node.data_lbn,
            allocation_length=allocation_length,
            data_part=data_part,
        )
        partition_blob[node.fe_lbn * _SECTOR : (node.fe_lbn + 1) * _SECTOR] = entry
        if node.directory:
            if node.data:
                start = node.data_lbn * _SECTOR
                partition_blob[start : start + len(node.data)] = node.data
            for child in node.children or []:
                render(child)
        elif metadata_partition:
            start = (part_start + node.data_lbn) * _SECTOR
            end = start + len(node.data)
            if end > len(image):
                image.extend(b"\0" * (end - len(image)))
            image[start:end] = node.data
        else:
            start = node.data_lbn * _SECTOR
            partition_blob[start : start + len(node.data)] = node.data

    render(root)
    if metadata_partition:
        metadata_lbn = 20
        metadata_entry = _file_entry(
            info_length=len(partition_blob),
            data_lbn=metadata_lbn,
            allocation_length=len(partition_blob),
            data_part=None,
        )
        entry_start = (part_start + 10) * _SECTOR
        image[entry_start : entry_start + _SECTOR] = metadata_entry
        metadata_start = (part_start + metadata_lbn) * _SECTOR
        metadata_end = metadata_start + len(partition_blob)
        if metadata_end > len(image):
            image.extend(b"\0" * (metadata_end - len(image)))
        image[metadata_start:metadata_end] = partition_blob
    else:
        partition_start = part_start * _SECTOR
        partition_end = partition_start + len(partition_blob)
        if partition_end > len(image):
            image.extend(b"\0" * (partition_end - len(image)))
        image[partition_start:partition_end] = partition_blob
    return bytes(image)


def _mpls(*items: tuple[str, float]) -> bytes:
    playlist_start = 64
    data = bytearray(playlist_start + 10 + 22 * len(items))
    data[:8] = b"MPLS0200"
    _put(data, 8, playlist_start, 4, "big")
    _put(data, playlist_start + 6, len(items), 2, "big")
    position = playlist_start + 10
    timeline = 0
    for clip, seconds in items:
        _put(data, position, 20, 2, "big")
        data[position + 2 : position + 7] = clip.encode("ascii")
        _put(data, position + 14, timeline, 4, "big")
        timeline += int(seconds * 45_000)
        _put(data, position + 18, timeline, 4, "big")
        position += 22
    return bytes(data)


def _clpi(*raw_streams: bytes) -> bytes:
    program_start = 64
    records = bytearray()
    for index, raw in enumerate(raw_streams):
        record = bytearray(3 + len(raw))
        _put(record, 0, 0x1011 + index, 2, "big")
        record[2] = len(raw)
        record[3:] = raw
        records.extend(record)
    program_length = 6 + len(records)
    data = bytearray(program_start + 10 + len(records))
    data[:8] = b"HDMV0200"
    _put(data, 12, program_start, 4, "big")
    _put(data, program_start, program_length, 4, "big")
    data[program_start + 10 :] = records
    return bytes(data)


def _m2ts_with_pcr(*base_values: int) -> bytes:
    content = bytearray(192 * len(base_values))
    for index, base in enumerate(base_values):
        packet = bytearray(188)
        packet[0] = 0x47
        packet[1] = 0x10
        packet[2] = 0x01
        packet[3] = 0x20
        packet[4] = 7
        packet[5] = 0x10
        packet[6] = (base >> 25) & 0xFF
        packet[7] = (base >> 17) & 0xFF
        packet[8] = (base >> 9) & 0xFF
        packet[9] = (base >> 1) & 0xFF
        packet[10] = ((base & 1) << 7) | 0x7E
        packet[11] = 0
        content[index * 192 + 4 : (index + 1) * 192] = packet
    return bytes(content)


def _bluray_root(
    *,
    streams: Sequence[_Node],
    playlists: Sequence[_Node],
    clipinfos: Sequence[_Node],
    wrapper_depth: int = 0,
) -> _Node:
    bdmv = _directory(
        "BDMV",
        _directory("PLAYLIST", *playlists),
        _directory("STREAM", *streams),
        _directory("CLIPINF", *clipinfos),
    )
    wrapped = bdmv
    for depth in reversed(range(wrapper_depth)):
        wrapped = _directory(f"WRAP{depth}", wrapped)
    return _directory("/", wrapped)


def _policy() -> P115MediaInfoPolicySnapshot:
    return P115MediaInfoPolicySnapshot.create(
        language_mapping=(
            {"label": "English", "value": "eng", "aliases": ["en"]},
            {"label": "Chinese", "value": "chi", "aliases": ["zho"]},
        ),
        default_stream_config={"audio_lang_priority": ["eng"], "subtitle_lang_priority": ["eng"]},
        language_mapping_revision="fixture-language-v1",
        default_stream_config_revision="fixture-stream-v1",
    )


def test_physical_udf_wrapper_mpls_clpi_and_formatter_contract() -> None:
    root = _bluray_root(
        streams=(
            _file("00001.m2ts", b"small", declared_size=2_000),
            _file("00002.m2ts", b"selected", declared_size=8_000),
        ),
        playlists=(
            _file("00001.mpls", _mpls(("00001", 120.0))),
            _file("00002.mpls", _mpls(("00002", 90.0))),
        ),
        clipinfos=(
            _file("00001.clpi", _clpi(bytes((0x1B, 0x41, 0x30, 0)))),
            _file(
                "00002.clpi",
                _clpi(
                    bytes((0x05, 0, 0, 0)),
                    bytes((0x24, 0x81, 0x30, 0)),
                    bytes((0x83, 0xC1)) + b"eng",
                    bytes((0x90,)) + b"eng",
                ),
            ),
        ),
        wrapper_depth=4,
    )
    image = _build_udf(root)
    gateway, reader, processes = _gateway(image)

    raw = gateway.probe(_request(size=len(image)), _direct(), cancel=None)

    assert raw is not None
    assert processes.calls == []
    assert raw["format"]["duration"] == "90.000"
    video = next(stream for stream in raw["streams"] if stream["codec_type"] == "video")
    assert (video["codec_name"], video["width"], video["height"]) == ("hevc", 3840, 2160)
    assert "color_space" not in video
    assert "color_primaries" not in video
    assert "color_transfer" not in video
    assert raw["_iso_probe"] == {
        "method": "udf_bluray",
        "udf_partition": "physical",
        "wrapper_depth": 4,
        "nested_iso_count": 0,
        "playlist_count": 2,
        "playlist_item_count": 1,
        "stream_file_count": 2,
        "selection_basis": "playlist_max_clip",
        "stream_probe_method": "clpi",
        "duration_method": "mpls",
        "range_requests": reader.requests,
        "range_bytes": reader.bytes_received,
    }
    formatted = format_raw_ffprobe_mediainfo(
        raw,
        file_name="Fixture.iso",
        file_size=len(image),
        sha1=_SHA1,
        policy=_policy(),
    )
    assert formatted[0]["MediaSourceInfo"]["Container"] == "iso"
    formatted_video = next(
        stream for stream in formatted[0]["MediaSourceInfo"]["MediaStreams"] if stream["Type"] == "Video"
    )
    assert formatted_video["VideoRange"] == ""


def test_metadata_partition_uses_largest_unreferenced_m2ts_heuristic() -> None:
    root = _bluray_root(
        streams=(
            _file("00001.m2ts", b"small", declared_size=1_000),
            _file("99999.m2ts", b"large", declared_size=3_001),
        ),
        playlists=(_file("00001.mpls", _mpls(("00001", 20.0))),),
        clipinfos=(
            _file("00001.clpi", _clpi(bytes((0x1B, 0x41, 0x30, 0)))),
            _file("99999.clpi", _clpi(bytes((0x24, 0x81, 0x30, 0)))),
        ),
    )
    image = _build_udf(root, metadata_partition=True)
    gateway, _, _ = _gateway(image)

    raw = gateway.probe(_request(size=len(image)), _direct(), cancel=None)

    assert raw is not None
    assert raw["_iso_probe"]["udf_partition"] == "metadata"
    assert raw["_iso_probe"]["selection_basis"] == "largest_unreferenced"
    assert raw["_iso_probe"]["playlist_item_count"] == 0
    assert raw["format"]["duration"] == ""
    assert raw["streams"][0]["codec_name"] == "hevc"


def test_single_nested_iso_is_supported_but_wrapper_depth_five_fails_closed() -> None:
    inner_root = _bluray_root(
        streams=(_file("00001.m2ts", b"stream", declared_size=1_000),),
        playlists=(_file("00001.mpls", _mpls(("00001", 10.0))),),
        clipinfos=(_file("00001.clpi", _clpi(bytes((0x1B, 0x41, 0x30, 0)))),),
    )
    inner_image = _build_udf(inner_root)
    outer_image = _build_udf(_directory("/", _file("only-disc.iso", inner_image)))
    nested_gateway, _, _ = _gateway(outer_image)

    nested = nested_gateway.probe(_request(size=len(outer_image)), _direct(), cancel=None)

    assert nested is not None
    assert nested["_iso_probe"]["nested_iso_count"] == 1
    assert nested["_iso_probe"]["wrapper_depth"] == 0

    too_deep = _build_udf(
        _bluray_root(
            streams=(_file("00001.m2ts", b"stream", declared_size=1_000),),
            playlists=(_file("00001.mpls", _mpls(("00001", 10.0))),),
            clipinfos=(_file("00001.clpi", _clpi(bytes((0x1B, 0x41, 0x30, 0)))),),
            wrapper_depth=5,
        )
    )
    deep_gateway, _, _ = _gateway(too_deep)
    outcome = deep_gateway.probe_outcome(_request(size=len(too_deep)), _direct(), cancel=None)
    assert outcome.status is P115IsoProbeStatus.REJECTED
    assert outcome.raw is None


def test_missing_clpi_uses_controlled_ffprobe_and_head_tail_pcr_duration() -> None:
    m2ts = _m2ts_with_pcr(90_000, 540_000, 990_000)
    root = _bluray_root(
        streams=(_file("00001.m2ts", m2ts),),
        playlists=(),
        clipinfos=(),
    )
    image = _build_udf(root)
    processes = _FakeProcesses(
        (
            ControlledProcessOutcome(
                ControlledProcessStatus.SUCCEEDED,
                0,
                "controlled process completed",
                parsed_output={
                    "streams": [
                        {
                            "index": 0,
                            "codec_type": "video",
                            "codec_name": "hevc",
                            "width": 3840,
                            "height": 2160,
                        }
                    ]
                },
            ),
        )
    )
    gateway, _, _ = _gateway(image, processes=processes)

    raw = gateway.probe(_request(size=len(image)), _direct(), cancel=None)

    assert raw is not None
    assert raw["format"]["duration"] == "10.000"
    assert raw["_iso_probe"]["stream_probe_method"] == "m2ts_ffprobe"
    assert raw["_iso_probe"]["duration_method"] == "pcr"
    assert raw["_iso_probe"]["selection_basis"] == "largest_stream"
    assert len(processes.calls) == 1
    call = processes.calls[0]
    assert call["arguments"][-1] == "pipe:0"
    assert call["stdin"] == m2ts
    assert call["max_stdin_bytes"] == 16 * 1024 * 1024
    assert "shell" not in call


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (RangeReadErrorKind.REQUEST_LIMIT, P115IsoProbeStatus.BUDGET_EXHAUSTED),
        (RangeReadErrorKind.BYTE_LIMIT, P115IsoProbeStatus.BUDGET_EXHAUSTED),
        (RangeReadErrorKind.DEADLINE, P115IsoProbeStatus.BUDGET_EXHAUSTED),
        (RangeReadErrorKind.CANCELLED, P115IsoProbeStatus.CANCELLED),
    ],
)
def test_range_budgets_and_cancellation_return_stable_redacted_outcomes(
    kind: RangeReadErrorKind,
    expected: P115IsoProbeStatus,
) -> None:
    gateway = NativeP115IsoProbeGateway(
        _FakeProcesses(),  # type: ignore[arg-type]
        range_reader_factory=lambda _direct_url, _cancel: _FailingRangeReader(kind),  # type: ignore[arg-type,return-value]
    )

    outcome = gateway.probe_outcome(_request(), _direct(), cancel=None)

    assert outcome.status is expected
    assert outcome.raw is None
    rendered = repr(outcome).casefold()
    assert "secret.invalid" not in rendered
    assert "secret-value" not in rendered
    assert "cookie" not in rendered


def test_pre_cancel_skips_range_and_diagnostics_never_store_url_headers_or_names() -> None:
    factory_calls = 0

    def factory(_direct_url: P115DirectUrl, _cancel: Callable[[], bool] | None) -> Any:
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("pre-cancel must not construct a range reader")

    cancelled_gateway = NativeP115IsoProbeGateway(
        _FakeProcesses(),  # type: ignore[arg-type]
        range_reader_factory=factory,
    )
    cancelled = cancelled_gateway.probe_outcome(_request(), _direct(), cancel=lambda: True)
    assert cancelled.status is P115IsoProbeStatus.CANCELLED
    assert factory_calls == 0

    root = _bluray_root(
        streams=(_file("54321.m2ts", b"stream", declared_size=1_000),),
        playlists=(_file("sensitive-title.mpls", _mpls(("54321", 10.0))),),
        clipinfos=(_file("54321.clpi", _clpi(bytes((0x1B, 0x41, 0x30, 0)))),),
    )
    image = _build_udf(root)
    gateway, _, _ = _gateway(image)
    raw = gateway.probe(_request("private-name.iso", size=len(image)), _direct(), cancel=None)
    assert raw is not None
    diagnostics = json.dumps(raw["_iso_probe"], sort_keys=True).casefold()
    for secret in (
        "cdn.fixture.invalid",
        "must-not-survive",
        "cookie",
        "fixture-ua",
        "private-name",
        "sensitive-title",
        "54321",
    ):
        assert secret not in diagnostics
    assert set(raw["_iso_probe"]) == {
        "method",
        "udf_partition",
        "wrapper_depth",
        "nested_iso_count",
        "playlist_count",
        "playlist_item_count",
        "stream_file_count",
        "selection_basis",
        "stream_probe_method",
        "duration_method",
        "range_requests",
        "range_bytes",
    }


def test_outcome_type_does_not_expose_raw_in_repr() -> None:
    outcome = P115IsoProbeOutcome(
        P115IsoProbeStatus.CONFIRMED,
        "ISO probe completed",
        {"secret": "must-not-render"},
    )
    assert outcome.confirmed is True
    assert "must-not-render" not in repr(outcome)
