from __future__ import annotations

import struct
from typing import Any, cast

import pytest

from etk.domains.p115.intro_detection import (
    IntroDetectionPolicySnapshot,
    IntroEpisodeSnapshot,
    IntroFingerprintKind,
    IntroTriggerMode,
)
from etk.domains.p115.intro_detection_adapters import (
    AccountBoundIntroDirectUrlGateway,
    ChromaprintProcessAdapter,
    IntroDetectionAccountChanged,
    IntroFingerprintExtraction,
    IntroFingerprintExtractionStatus,
    P115IntroFingerprintAdapter,
    _slice_cached_fingerprint,
    fingerprint_window,
)
from etk.domains.p115.media_info_adapters import (
    ControlledProcessOutcome,
    ControlledProcessStatus,
    P115DirectUrl,
)


def _episode(number: int, *, runtime_seconds: float = 1500) -> IntroEpisodeSnapshot:
    return IntroEpisodeSnapshot.create(
        item_id=f"item-{number}",
        series_tmdb_id="series-42",
        season_number=1,
        episode_number=number,
        sha1=f"{number:040X}",
        pick_code=f"pick-{number}",
        runtime_seconds=runtime_seconds,
    )


class _Runner:
    def __init__(self, outcome: ControlledProcessOutcome) -> None:
        self.outcome = outcome
        self.calls: list[tuple[Any, ...]] = []

    def run(self, *args: Any, **kwargs: Any) -> ControlledProcessOutcome:
        self.calls.append((args, kwargs))
        return self.outcome


def test_chromaprint_adapter_uses_frozen_audio_format_and_bounded_pipes() -> None:
    values = tuple(range(40))
    runner = _Runner(
        ControlledProcessOutcome(
            ControlledProcessStatus.SUCCEEDED,
            0,
            "",
            stdout=struct.pack("<40i", *values),
        )
    )
    adapter = ChromaprintProcessAdapter(cast(Any, runner), timeout_seconds=12)
    direct_url = P115DirectUrl(
        "https://fixture.invalid/media?token=secret",
        "fixture-agent",
        "openapi",
        "download_url",
    )

    outcome = adapter.extract(
        direct_url,
        window_start_seconds=5,
        sample_seconds=180,
        audio_stream_index=2,
        cancel=lambda: False,
    )

    assert outcome.status is IntroFingerprintExtractionStatus.SUCCEEDED
    assert outcome.values == values
    arguments = runner.calls[0][0][1]
    assert ("-ac", "1") == tuple(arguments[arguments.index("-ac") : arguments.index("-ac") + 2])
    assert ("-ar", "16000") == tuple(arguments[arguments.index("-ar") : arguments.index("-ar") + 2])
    assert ("-map", "0:2") == tuple(arguments[arguments.index("-map") : arguments.index("-map") + 2])
    limits = runner.calls[0][1]
    assert limits["timeout_seconds"] == 12
    assert limits["max_stdout_bytes"] > 0
    assert limits["max_stderr_bytes"] > 0
    assert "secret" not in repr(direct_url)


def test_chromaprint_adapter_maps_cancel_without_exposing_process_output() -> None:
    runner = _Runner(
        ControlledProcessOutcome(
            ControlledProcessStatus.CANCELLED,
            -15,
            "controlled process was cancelled",
        )
    )
    adapter = ChromaprintProcessAdapter(cast(Any, runner))

    outcome = adapter.extract(
        P115DirectUrl("https://fixture.invalid/media", "ua", "cookie", "download_url"),
        window_start_seconds=0,
        sample_seconds=180,
        audio_stream_index=None,
        cancel=lambda: True,
    )

    assert outcome.status is IntroFingerprintExtractionStatus.CANCELLED
    assert outcome.values == ()


class _Identity:
    def __init__(self, values: list[str]) -> None:
        self.values = values

    def account_id(self) -> str:
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


class _Resolver:
    def resolve_download_url(self, pick_code: str, *, user_agent: str) -> P115DirectUrl:
        assert pick_code == "pick-1"
        assert user_agent
        return P115DirectUrl("https://fixture.invalid/media", user_agent, "openapi", "download_url")


def test_direct_url_gateway_fences_account_before_and_after_resolution() -> None:
    gateway = AccountBoundIntroDirectUrlGateway(_Identity(["115001", "115002"]), _Resolver())

    with pytest.raises(IntroDetectionAccountChanged):
        gateway.resolve("115001", "pick-1")


class _BoundUrls:
    def __init__(self) -> None:
        self.current_account = "115001"
        self.resolve_calls = 0
        self.account_checks = 0

    def assert_account(self, account_id: str) -> None:
        self.account_checks += 1
        if self.current_account != account_id:
            raise IntroDetectionAccountChanged("fixture account changed")

    def resolve(
        self,
        account_id: str,
        pick_code: str,
        *,
        provider_backend: str = "",
    ) -> P115DirectUrl:
        self.resolve_calls += 1
        self.assert_account(account_id)
        assert pick_code == "pick-1"
        assert provider_backend == "openapi"
        return P115DirectUrl("https://fixture.invalid/media", "ua", "openapi", "download_url")


class _FingerprintProcess:
    def __init__(self, direct_urls: _BoundUrls) -> None:
        self._direct_urls = direct_urls
        self.calls = 0

    def extract(self, *_args: Any, **_kwargs: Any) -> IntroFingerprintExtraction:
        self.calls += 1
        if self.calls == 2:
            self._direct_urls.current_account = "115002"
        return IntroFingerprintExtraction(
            IntroFingerprintExtractionStatus.SUCCEEDED,
            180,
            0,
            tuple(range(40)),
        )


def test_cached_direct_url_rechecks_account_before_and_after_every_reuse() -> None:
    direct_urls = _BoundUrls()
    process = _FingerprintProcess(direct_urls)
    adapter = P115IntroFingerprintAdapter(
        cast(Any, direct_urls),
        cast(Any, process),
        success_delay_seconds=0,
    )

    first = adapter.extract(
        "115001",
        _episode(1),
        IntroFingerprintKind.INTRO,
        sample_seconds=180,
        audio_stream_index=None,
        cancel=lambda: False,
    )
    assert first.succeeded

    with pytest.raises(IntroDetectionAccountChanged, match="fixture account changed"):
        adapter.extract(
            "115001",
            _episode(1),
            IntroFingerprintKind.INTRO,
            sample_seconds=180,
            audio_stream_index=None,
            cancel=lambda: False,
        )

    assert direct_urls.resolve_calls == 1
    assert process.calls == 2
    assert direct_urls.account_checks >= 5


def test_credits_window_is_tail_bounded_and_requires_runtime() -> None:
    assert fingerprint_window(_episode(1, runtime_seconds=1000), IntroFingerprintKind.CREDITS, 180) == (820, 180)
    assert fingerprint_window(_episode(1, runtime_seconds=100), IntroFingerprintKind.CREDITS, 180) == (0, 100)
    assert fingerprint_window(_episode(1, runtime_seconds=0), IntroFingerprintKind.CREDITS, 180) == (0, 0)


def test_legacy_credits_coverage_slices_180_seconds_from_normalized_300_second_tail() -> None:
    values = tuple(range(100))

    sliced = _slice_cached_fingerprint(
        values,
        300,
        180,
        IntroFingerprintKind.CREDITS,
    )

    assert sliced == values[-60:]


def test_policy_snapshot_keeps_empty_library_allowlist_as_all_libraries() -> None:
    snapshot = IntroDetectionPolicySnapshot.create(
        trigger_mode=IntroTriggerMode.IMPORT,
        credits_enabled=False,
        library_ids=(),
    )

    assert snapshot.permits_library("any-library")
