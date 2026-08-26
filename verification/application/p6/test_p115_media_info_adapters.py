from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from etk.domains.p115.fingerprints import P115FingerprintAccountChanged
from etk.domains.p115.media_info_adapters import (
    PREID_PREFIX_BYTES,
    AccountBoundP115FingerprintClient,
    BoundedRangeReader,
    ControlledProcessOutput,
    ControlledProcessProfile,
    ControlledProcessRunner,
    ControlledProcessStatus,
    LocalP115FingerprintHintGateway,
    PreIdCandidates,
    PreIdSource,
    RangeReadError,
    RangeReadErrorKind,
    compute_preid,
    preid_from_range,
    resolve_cached_preid,
)


def _process_runner() -> ControlledProcessRunner:
    return ControlledProcessRunner(
        executable_overrides={
            ControlledProcessProfile.FFPROBE: sys.executable,
            ControlledProcessProfile.FFMPEG: sys.executable,
        },
        termination_grace_seconds=0.02,
        poll_interval_seconds=0.005,
    )


def _run_python(
    script: str,
    *,
    stdin: bytes = b"",
    max_stdin_bytes: int = 1024,
    max_stdout_bytes: int = 4096,
    max_stderr_bytes: int = 4096,
    timeout_seconds: float = 1,
    output: ControlledProcessOutput = ControlledProcessOutput.BYTES,
    cancel: Callable[[], bool] | None = None,
):
    return _process_runner().run(
        ControlledProcessProfile.FFPROBE,
        ("-c", script),
        stdin=stdin,
        max_stdin_bytes=max_stdin_bytes,
        max_stdout_bytes=max_stdout_bytes,
        max_stderr_bytes=max_stderr_bytes,
        timeout_seconds=timeout_seconds,
        output=output,
        cancel=cancel,
    )


def test_controlled_process_success_reads_bounded_stdin_and_parses_json() -> None:
    outcome = _run_python(
        "import json, sys; print(json.dumps({'size': len(sys.stdin.buffer.read())}))",
        stdin=b"fixture-input",
        max_stdin_bytes=32,
        output=ControlledProcessOutput.JSON,
    )

    assert outcome.status is ControlledProcessStatus.SUCCEEDED
    assert outcome.succeeded is True
    assert outcome.return_code == 0
    assert outcome.parsed_output == {"size": 13}


def test_controlled_process_nonzero_redacts_sensitive_diagnostics() -> None:
    outcome = _run_python(
        "import sys; "
        "sys.stderr.write('https://cdn.example.test/media?token=secret\\nCookie: session-secret\\n' "
        "+ 'Authorization: Bearer auth-secret\\n'); "
        "raise SystemExit(7)"
    )

    diagnostic = (outcome.diagnostic.encode() + outcome.stderr + repr(outcome).encode()).lower()
    assert outcome.status is ControlledProcessStatus.NONZERO
    assert outcome.return_code == 7
    assert b"cdn.example" not in diagnostic
    assert b"session-secret" not in diagnostic
    assert b"auth-secret" not in diagnostic
    assert b"cookie" not in diagnostic
    assert b"authorization" not in diagnostic


def test_controlled_process_reports_invalid_json_without_throwing() -> None:
    outcome = _run_python("print('not-json')", output=ControlledProcessOutput.JSON)

    assert outcome.status is ControlledProcessStatus.INVALID_OUTPUT
    assert outcome.return_code == 0
    assert outcome.parsed_output is None


def test_controlled_process_timeout_kills_the_whole_process_group_and_reaps_it(tmp_path: Path) -> None:
    marker = tmp_path / "orphan-child"
    child = (
        "import signal, time; from pathlib import Path; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"time.sleep(0.25); Path({str(marker)!r}).write_text('orphan', encoding='utf-8')"
    )
    parent = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(5)"

    started = time.monotonic()
    outcome = _run_python(parent, timeout_seconds=0.05)
    elapsed = time.monotonic() - started
    time.sleep(0.35)

    assert outcome.status is ControlledProcessStatus.TIMED_OUT
    assert outcome.return_code is not None
    assert elapsed < 1
    assert not marker.exists()


def test_controlled_process_cancel_terminates_and_reaps_the_process() -> None:
    cancelled = threading.Event()
    timer = threading.Timer(0.05, cancelled.set)
    timer.start()
    try:
        outcome = _run_python("import time; time.sleep(5)", cancel=cancelled.is_set)
    finally:
        timer.cancel()

    assert outcome.status is ControlledProcessStatus.CANCELLED
    assert outcome.return_code is not None


@pytest.mark.parametrize(
    ("script", "expected_stream"),
    [
        ("import sys, time; sys.stdout.buffer.write(b'x' * 100000); sys.stdout.flush(); time.sleep(5)", "stdout"),
        ("import sys, time; sys.stderr.buffer.write(b'x' * 100000); sys.stderr.flush(); time.sleep(5)", "stderr"),
    ],
)
def test_controlled_process_output_caps_terminate_the_process(script: str, expected_stream: str) -> None:
    outcome = _run_python(
        script,
        max_stdout_bytes=1024,
        max_stderr_bytes=1024,
        timeout_seconds=1,
    )

    assert outcome.status is ControlledProcessStatus.OUTPUT_LIMIT
    assert outcome.limit_stream == expected_stream
    assert len(outcome.stdout) <= 1024
    assert len(outcome.stderr) <= 1024
    assert outcome.return_code is not None


def test_controlled_process_stdin_cap_rejects_before_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_spawn(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("input over limit must not spawn a process")

    monkeypatch.setattr(subprocess, "Popen", unexpected_spawn)
    outcome = _run_python("print('not reached')", stdin=b"12345", max_stdin_bytes=4)

    assert outcome.status is ControlledProcessStatus.OUTPUT_LIMIT
    assert outcome.limit_stream == "stdin"
    assert outcome.return_code is None


def _client_factory(handler: Callable[[httpx.Request], httpx.Response]):
    transport = httpx.MockTransport(handler)

    def create(**kwargs: Any) -> httpx.Client:
        return httpx.Client(transport=transport, **kwargs)

    return create


def _range_reader(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    url: str = "https://cdn.example.test/media?token=secret",
    **overrides: Any,
) -> BoundedRangeReader:
    options: dict[str, Any] = {
        "headers": {"Cookie": "session-secret", "User-Agent": "fixture"},
        "max_requests": 4,
        "max_bytes": 1024,
        "deadline_seconds": 1,
        "request_timeout_seconds": 1,
        "max_redirects": 2,
        "chunk_size": 2,
        "client_factory": _client_factory(handler),
    }
    options.update(overrides)
    return BoundedRangeReader(url, **options)


def test_bounded_range_reader_requires_exact_206_content_range_and_length() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Range"] == "bytes=4-7"
        assert request.headers["Cookie"] == "session-secret"
        return httpx.Response(206, headers={"Content-Range": "bytes 4-7/20"}, content=b"data")

    reader = _range_reader(handler)

    assert reader.read(4, 4) == b"data"
    assert reader.metrics.requests == 1
    assert reader.metrics.bytes_received == 4


@pytest.mark.parametrize(
    ("response", "kind"),
    [
        (httpx.Response(200, content=b"data"), RangeReadErrorKind.STATUS),
        (
            httpx.Response(206, headers={"Content-Range": "bytes 5-8/20"}, content=b"data"),
            RangeReadErrorKind.CONTENT_RANGE,
        ),
        (
            httpx.Response(206, headers={"Content-Range": "bytes 4-7/20"}, content=b"dat"),
            RangeReadErrorKind.LENGTH,
        ),
    ],
)
def test_bounded_range_reader_rejects_status_content_range_and_short_body(
    response: httpx.Response,
    kind: RangeReadErrorKind,
) -> None:
    reader = _range_reader(lambda _request: response)

    with pytest.raises(RangeReadError) as caught:
        reader.read(4, 4)

    assert caught.value.kind is kind
    rendered = repr(caught.value).lower()
    assert "cdn.example" not in rendered
    assert "session-secret" not in rendered
    assert "cookie" not in rendered


def test_bounded_range_reader_enforces_request_and_byte_budgets_before_network() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(206, headers={"Content-Range": "bytes 0-3/20"}, content=b"data")

    request_limited = _range_reader(handler, max_requests=1)
    assert request_limited.read(0, 4) == b"data"
    with pytest.raises(RangeReadError) as request_error:
        request_limited.read(0, 4)
    assert request_error.value.kind is RangeReadErrorKind.REQUEST_LIMIT

    byte_limited = _range_reader(handler, max_bytes=3)
    with pytest.raises(RangeReadError) as byte_error:
        byte_limited.read(0, 4)
    assert byte_error.value.kind is RangeReadErrorKind.BYTE_LIMIT
    assert calls == 1


def test_bounded_range_reader_enforces_redirect_budget_without_exposing_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/media":
            return httpx.Response(302, headers={"Location": "/redirected"})
        return httpx.Response(206, headers={"Content-Range": "bytes 0-3/20"}, content=b"data")

    reader = _range_reader(handler, max_redirects=0)

    with pytest.raises(RangeReadError) as caught:
        reader.read(0, 4)

    assert caught.value.kind is RangeReadErrorKind.REDIRECT_LIMIT
    assert "cdn.example" not in repr(caught.value).lower()
    assert "secret" not in repr(caught.value).lower()


def test_bounded_range_reader_counts_redirects_against_the_request_budget() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "/again"})

    reader = _range_reader(handler, max_requests=1, max_redirects=2)

    with pytest.raises(RangeReadError) as caught:
        reader.read(0, 4)

    assert caught.value.kind is RangeReadErrorKind.REQUEST_LIMIT
    assert reader.metrics.requests == 1


class _SlowStream(httpx.SyncByteStream):
    def __iter__(self):
        yield b"da"
        time.sleep(0.03)
        yield b"ta"


def test_bounded_range_reader_enforces_operation_deadline_during_streaming() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 0-3/20"},
            stream=_SlowStream(),
        )

    reader = _range_reader(handler, deadline_seconds=0.01)

    with pytest.raises(RangeReadError) as caught:
        reader.read(0, 4)

    assert caught.value.kind is RangeReadErrorKind.DEADLINE
    assert reader.metrics.bytes_received < 4


class _CancellingStream(httpx.SyncByteStream):
    def __init__(self, cancelled: threading.Event) -> None:
        self._cancelled = cancelled

    def __iter__(self):
        yield b"da"
        self._cancelled.set()
        yield b"ta"


def test_bounded_range_reader_checks_cancellation_between_stream_chunks() -> None:
    cancelled = threading.Event()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 0-3/20"},
            stream=_CancellingStream(cancelled),
        )

    reader = _range_reader(handler, cancel=cancelled.is_set)

    with pytest.raises(RangeReadError) as caught:
        reader.read(0, 4)

    assert caught.value.kind is RangeReadErrorKind.CANCELLED
    assert reader.metrics.bytes_received == 2


def test_preid_hashes_exactly_the_first_128_kib_as_uppercase_sha1() -> None:
    prefix = bytes(range(256)) * (PREID_PREFIX_BYTES // 256)

    class Reader:
        def read(self, start: int, length: int) -> bytes:
            assert (start, length) == (0, PREID_PREFIX_BYTES)
            return prefix

    expected = hashlib.sha1(prefix).hexdigest().upper()
    assert preid_from_range(Reader()) == expected
    assert compute_preid(prefix) == expected
    assert len(expected) == 40
    with pytest.raises(ValueError, match="exactly"):
        compute_preid(prefix[:-1])


def test_cached_preid_resolution_has_fixed_pure_priority_and_legacy_raw_compatibility() -> None:
    values = {
        "file": "1" * 40,
        "file_raw": "2" * 40,
        "cache_raw": "3" * 40,
        "hint": "4" * 40,
        "shared": "5" * 40,
    }
    resolution = resolve_cached_preid(
        PreIdCandidates(
            file_preid=values["file"].lower(),
            file_raw={"_etk": {"preid": values["file_raw"]}},
            cached_raw={"_etk": {"preid": values["cache_raw"]}},
            rapid_hint_preid=values["hint"],
            shared_rapid_preid=values["shared"],
        )
    )
    fallback = resolve_cached_preid(
        PreIdCandidates(
            file_preid="invalid",
            file_raw=json.dumps({"_etk": {"pre_sha1_128k": values["file_raw"].lower()}}),
            cached_raw={"_etk": {"preid": values["cache_raw"]}},
        )
    )

    assert resolution is not None
    assert resolution.value == values["file"]
    assert resolution.source is PreIdSource.FILE
    assert fallback is not None
    assert fallback.value == values["file_raw"]
    assert fallback.source is PreIdSource.FILE_RAW
    assert resolve_cached_preid(PreIdCandidates()) is None


def test_preid_from_bounded_range_rejects_a_short_206_response() -> None:
    prefix = b"x" * (PREID_PREFIX_BYTES - 1)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            206,
            headers={"Content-Range": f"bytes 0-{PREID_PREFIX_BYTES - 1}/{PREID_PREFIX_BYTES * 2}"},
            content=prefix,
        )

    reader = _range_reader(
        handler,
        max_bytes=PREID_PREFIX_BYTES,
        chunk_size=32 * 1024,
    )

    with pytest.raises(RangeReadError) as caught:
        preid_from_range(reader)

    assert caught.value.kind is RangeReadErrorKind.LENGTH


def test_local_fingerprint_hints_read_only_root_contained_strm(tmp_path: Path) -> None:
    root = tmp_path / "library"
    movie = root / "Movies" / "Fixture.strm"
    movie.parent.mkdir(parents=True)
    movie.write_text("https://fixture.invalid/p115/play/pc-fixture/Fixture.mkv", encoding="utf-8")

    hints = LocalP115FingerprintHintGateway().inspect(str(movie), local_root=str(root))

    assert hints.pick_code == "pc-fixture"
    assert hints.sha1 == ""
    assert hints.virtual is False
    assert hints.local_paths == ("Movies/Fixture.strm", "Movies/Fixture.mkv")


def test_local_fingerprint_hints_mark_virtual_strm(tmp_path: Path) -> None:
    root = tmp_path / "library"
    movie = root / "Fixture.strm"
    movie.parent.mkdir(parents=True)
    sha1 = "A" * 40
    movie.write_text(
        f"https://fixture.invalid/api/p115/virtual-play/7/{sha1}/Fixture.mkv",
        encoding="utf-8",
    )

    hints = LocalP115FingerprintHintGateway().inspect(str(movie), local_root=str(root))

    assert hints.virtual is True
    assert hints.sha1 == sha1
    assert hints.pick_code == ""


def test_local_fingerprint_hints_do_not_read_outside_configured_root(tmp_path: Path) -> None:
    root = tmp_path / "library"
    root.mkdir()
    outside = tmp_path / "outside.strm"
    outside.write_text("https://fixture.invalid/p115/play/pc-secret/Movie.mkv", encoding="utf-8")

    hints = LocalP115FingerprintHintGateway().inspect(str(outside), local_root=str(root))

    assert hints.pick_code == ""
    assert hints.local_paths == ()


class _FingerprintInfoClient:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[str] = []

    def fs_get_info(self, file_id: str) -> dict[str, Any]:
        self.calls.append(file_id)
        return self.response


def test_account_bound_fingerprint_client_validates_fid_pickcode_and_account() -> None:
    account = ["115001"]
    provider = _FingerprintInfoClient(
        {
            "state": True,
            "data": {
                "fid": "9001",
                "parent_id": "8001",
                "fn": "Fixture.mkv",
                "sha1": "B" * 40,
                "pc": "pc-fixture",
                "size": 123,
            },
        }
    )
    client = AccountBoundP115FingerprintClient(
        provider,
        account_id="115001",
        backend="openapi",
        account_reader=lambda: account[0],
        id_resolver=lambda _pick_code: "9001",
    )

    value = client.lookup("pc-fixture")

    assert value is not None
    assert value.file_id == "9001"
    assert value.sha1 == "B" * 40
    assert value.account_id == "115001"
    assert provider.calls == ["9001"]

    account[0] = "115002"
    with pytest.raises(P115FingerprintAccountChanged, match="account changed"):
        client.lookup("pc-fixture")


def test_account_bound_fingerprint_client_rejects_provider_identity_mismatch() -> None:
    provider = _FingerprintInfoClient(
        {
            "state": True,
            "data": {
                "fid": "9002",
                "parent_id": "8001",
                "fn": "Fixture.mkv",
                "sha1": "B" * 40,
                "pc": "pc-other",
                "size": 123,
            },
        }
    )
    client = AccountBoundP115FingerprintClient(
        provider,
        account_id="115001",
        backend="openapi",
        account_reader=lambda: "115001",
        id_resolver=lambda _pick_code: "9001",
    )

    with pytest.raises(RuntimeError, match="did not match"):
        client.lookup("pc-fixture")
