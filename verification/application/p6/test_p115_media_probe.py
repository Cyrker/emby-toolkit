from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from etk.domains.p115.media_info_adapters import (
    PREID_PREFIX_BYTES,
    ControlledProcessOutcome,
    ControlledProcessStatus,
    PreIdCandidates,
)
from etk.domains.p115.media_info_formatter import P115MediaInfoPolicySnapshot
from etk.domains.p115.media_probe import (
    P115DirectUrl,
    P115MediaProbeRequest,
    P115MediaProbeService,
    P115MediaProbeStatus,
)

SHA_A = "A" * 40


class FakeDirectUrls:
    def __init__(self, value: P115DirectUrl | None = None) -> None:
        self.value = value or P115DirectUrl(
            "https://cdn.fixture.invalid/media?token=secret",
            "Fixture-UA",
            "openapi",
            "download_url",
        )
        self.calls: list[tuple[str, str, str]] = []

    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl | None:
        self.calls.append((pick_code, expected_account_id, user_agent))
        return self.value


class FakeProcesses:
    def __init__(self, outcomes: list[ControlledProcessOutcome]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def run(self, profile: object, arguments: list[str], **kwargs: Any) -> ControlledProcessOutcome:
        self.calls.append({"profile": profile, "arguments": list(arguments), **kwargs})
        return self.outcomes.pop(0)


class FakeIso:
    def __init__(self, raw: Mapping[str, Any] | None = None) -> None:
        self.raw = raw
        self.calls: list[tuple[P115MediaProbeRequest, P115DirectUrl]] = []

    def probe(
        self,
        request: P115MediaProbeRequest,
        direct: P115DirectUrl,
        *,
        cancel: Callable[[], bool] | None,
    ) -> Mapping[str, Any] | None:
        del cancel
        self.calls.append((request, direct))
        return self.raw


class FakePolicies:
    def snapshot(self) -> P115MediaInfoPolicySnapshot:
        return P115MediaInfoPolicySnapshot.create(
            language_mapping=({"label": "English", "value": "eng", "aliases": ["en"]},),
            default_stream_config={
                "audio_lang_priority": ["eng"],
                "subtitle_lang_priority": ["eng"],
            },
            language_mapping_revision="fixture-language-v1",
            default_stream_config_revision="fixture-stream-v1",
        )


class FakeRangeReader:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[int, int]] = []

    def read(self, start: int, length: int) -> bytes:
        self.calls.append((start, length))
        return self.content


def _raw() -> dict[str, Any]:
    return {
        "format": {
            "filename": "https://cdn.fixture.invalid/media?token=secret",
            "format_name": "matroska,webm",
            "duration": "60.0",
            "size": "1024",
        },
        "streams": [
            {
                "index": 0,
                "codec_type": "video",
                "codec_name": "hevc",
                "width": 3840,
                "height": 2160,
                "disposition": {"default": 1},
                "tags": {},
            }
        ],
        "chapters": [],
    }


def _success(raw: Mapping[str, Any] | None = None) -> ControlledProcessOutcome:
    return ControlledProcessOutcome(
        ControlledProcessStatus.SUCCEEDED,
        0,
        "controlled process completed",
        parsed_output=dict(raw or _raw()),
    )


def _request(file_name: str = "Fixture.mkv", **values: Any) -> P115MediaProbeRequest:
    return P115MediaProbeRequest(
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-a",
        file_name=file_name,
        file_size=1024,
        context={"tmdb_id": "101", "type": "movie", "temporary_url": "must-not-survive"},
        **values,
    )


def test_probe_runs_bounded_ffprobe_computes_preid_and_sanitizes_raw() -> None:
    processes = FakeProcesses([_success()])
    range_reader = FakeRangeReader(b"x" * PREID_PREFIX_BYTES)
    service = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=processes,  # type: ignore[arg-type]
        iso=FakeIso(),
        policies=FakePolicies(),
        range_reader_factory=lambda _direct, _cancel: range_reader,  # type: ignore[return-value]
    )

    result = service.probe(_request())

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.confirmed is True
    assert result.provider_backend == "openapi"
    assert result.probe_method == "ffprobe"
    assert result.preid and len(result.preid) == 40 and result.preid == result.preid.upper()
    assert result.raw is not None
    assert "filename" not in result.raw["format"]
    assert result.raw["_etk"] == {
        "tmdb_id": "101",
        "type": "movie",
        "sha1": SHA_A,
        "preid": result.preid,
    }
    assert processes.calls[0]["max_stdout_bytes"] == 8 * 1024 * 1024
    assert processes.calls[0]["timeout_seconds"] == 45
    assert processes.calls[0]["arguments"][-1].startswith("https://cdn.fixture.invalid/")
    assert range_reader.calls == [(0, PREID_PREFIX_BYTES)]


def test_probe_reuses_cached_preid_without_range_read() -> None:
    processes = FakeProcesses([_success()])
    range_calls: list[P115DirectUrl] = []
    service = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=processes,  # type: ignore[arg-type]
        iso=FakeIso(),
        policies=FakePolicies(),
        range_reader_factory=lambda direct, _cancel: range_calls.append(direct),  # type: ignore[return-value]
    )

    result = service.probe(_request(preid_candidates=PreIdCandidates(cached_raw={"_etk": {"preid": "B" * 40}})))

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.preid == "B" * 40
    assert range_calls == []


def test_mp4_timeout_uses_legacy_fast_probe_profile_once() -> None:
    timeout = ControlledProcessOutcome(ControlledProcessStatus.TIMED_OUT, -15, "controlled process timed out")
    processes = FakeProcesses([timeout, _success()])
    service = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=processes,  # type: ignore[arg-type]
        iso=FakeIso(),
        policies=FakePolicies(),
        range_reader_factory=lambda _direct, _cancel: FakeRangeReader(b"x" * PREID_PREFIX_BYTES),  # type: ignore[return-value]
    )

    result = service.probe(_request("Fixture.mp4"))

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert len(processes.calls) == 2
    fast = processes.calls[1]["arguments"]
    assert fast[fast.index("-analyzeduration") + 1] == "0"
    assert fast[fast.index("-probesize") + 1] == "32"
    assert fast[fast.index("-max_probe_packets") + 1] == "1"


def test_iso_probe_precedes_generic_ffprobe() -> None:
    iso = FakeIso(_raw())
    processes = FakeProcesses([])
    service = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=processes,  # type: ignore[arg-type]
        iso=iso,
        policies=FakePolicies(),
        range_reader_factory=lambda _direct, _cancel: FakeRangeReader(b"x" * PREID_PREFIX_BYTES),  # type: ignore[return-value]
    )

    result = service.probe(_request("Fixture.iso"))

    assert result.status is P115MediaProbeStatus.CONFIRMED
    assert result.probe_method == "iso"
    assert len(iso.calls) == 1
    assert processes.calls == []


def test_probe_maps_cancel_output_limit_and_no_video_without_fake_success() -> None:
    cancelled = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=FakeProcesses([]),  # type: ignore[arg-type]
        iso=FakeIso(),
        policies=FakePolicies(),
    ).probe(_request(), cancel=lambda: True)
    assert cancelled.status is P115MediaProbeStatus.CANCELLED

    output_limit = ControlledProcessOutcome(
        ControlledProcessStatus.OUTPUT_LIMIT,
        -15,
        "controlled process output exceeded its byte limit",
    )
    limited = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=FakeProcesses([output_limit]),  # type: ignore[arg-type]
        iso=FakeIso(),
        policies=FakePolicies(),
    ).probe(_request())
    assert limited.status is P115MediaProbeStatus.REJECTED
    assert limited.formatted is None

    no_video = _raw()
    no_video["streams"] = []
    rejected = P115MediaProbeService(
        direct_urls=FakeDirectUrls(),
        processes=FakeProcesses([_success(no_video)]),  # type: ignore[arg-type]
        iso=FakeIso(),
        policies=FakePolicies(),
    ).probe(_request())
    assert rejected.status is P115MediaProbeStatus.REJECTED
    assert rejected.diagnostic == "media probe returned no video stream"
