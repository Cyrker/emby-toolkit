from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from etk.domains.p115 import (
    P115ExecutionRunResult,
    P115ExecutionStatus,
    P115TaskProgress,
)
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus
from etk.processes.p6_p115_tasks import P6P115TaskRouter

EXECUTION_ID = UUID("00000000-0000-0000-0000-000000000641")
JOB_ID = UUID("00000000-0000-0000-0000-000000000642")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000643")
WORKER_ID = "fixture-worker"


class FakeDirectorySync:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("fixture sync", {"cached": 2})
        self.calls: list[P115TaskProgress] = []

    def execute(self, progress: P115TaskProgress) -> TaskOutcome:
        self.calls.append(progress)
        return self.outcome


class FakeStrmSync:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("fixture STRM sync", {"strm_written": 2})
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        progress: P115TaskProgress,
        *,
        home_video_only: bool = False,
    ) -> TaskOutcome:
        self.calls.append({"progress": progress, "home_video_only": home_video_only})
        return self.outcome


class FakeFingerprintRepair:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("fixture fingerprint repair", {"fixed_assets": 2})
        self.calls: list[P115TaskProgress] = []

    def execute(self, progress: P115TaskProgress) -> TaskOutcome:
        self.calls.append(progress)
        return self.outcome


class FakePlaybackMaintenance:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.result = result or {
            "failed_count": 0,
            "results": [{"account_id": "fixture-account", "success": True}],
            "message": "fixture playback speedtest complete",
        }
        self.calls: list[P115TaskProgress] = []

    def daily_speedtest(self, progress: P115TaskProgress) -> dict[str, Any]:
        self.calls.append(progress)
        return dict(self.result)


class FakeIntroBackfill:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("fixture intro backfill", {"count": 2})
        self.calls: list[dict[str, Any]] = []

    def execute(self, progress: P115TaskProgress, *, force: bool = True) -> TaskOutcome:
        self.calls.append({"progress": progress, "force": force})
        return self.outcome


class FakeReconciliationResult:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.result = result or {
            "reconciliation": {"outcome": "applied"},
            "execution": {"status": "confirmed"},
            "external_mutations_executed": 0,
        }

    def to_dict(self) -> dict[str, Any]:
        return dict(self.result)


class FakeOrganizeExecution:
    def __init__(
        self,
        run_result: P115ExecutionRunResult | None = None,
        reconciliation_result: FakeReconciliationResult | None = None,
    ) -> None:
        self.run_result = run_result or P115ExecutionRunResult(
            P115ExecutionStatus.CONFIRMED,
            "fixture organize complete",
            {"execution_id": str(EXECUTION_ID)},
            True,
        )
        self.reconciliation_result = reconciliation_result or FakeReconciliationResult()
        self.execute_calls: list[dict[str, Any]] = []
        self.reconcile_calls: list[dict[str, Any]] = []

    def execute(
        self,
        execution_id: UUID,
        progress: P115TaskProgress,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str | None = None,
    ) -> P115ExecutionRunResult:
        self.execute_calls.append(
            {
                "execution_id": execution_id,
                "progress": progress,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "worker_id": worker_id,
            }
        )
        return self.run_result

    def reconcile(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        *,
        actor: str | None = None,
    ) -> FakeReconciliationResult:
        self.reconcile_calls.append(
            {
                "execution_id": execution_id,
                "approved_intent_id": approved_intent_id,
                "actor": actor,
            }
        )
        return self.reconciliation_result


class FakeProgress:
    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        return None


def _message(
    task_key: str = "sync-115-directory-tree",
    *,
    processor: ProcessorIdentity = ProcessorIdentity.MEDIA,
    payload: dict[str, object] | None = None,
    metadata: dict[str, object] | None = None,
    version: int = 1,
) -> TaskMessage:
    return TaskMessage.create(
        task_key=task_key,
        processor=processor,
        payload=payload,
        payload_version=version,
        metadata=metadata,
    )


def _scan_message(
    *,
    payload: dict[str, object] | None = None,
    metadata: dict[str, object] | None = None,
) -> TaskMessage:
    return _message(
        "scan-organize-115",
        payload=payload if payload is not None else {"execution_id": str(EXECUTION_ID)},
        metadata=metadata
        if metadata is not None
        else {
            "job_id": str(JOB_ID),
            "attempt_id": str(ATTEMPT_ID),
            "worker_id": WORKER_ID,
        },
    )


def test_directory_sync_preserves_typed_outcome_and_marks_no_legacy_fallback() -> None:
    directory_sync = FakeDirectorySync(TaskOutcome.partial("one root failed", {"roots_failed": 1}))
    organize_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(directory_sync, organize_execution)
    progress = FakeProgress()

    outcome = router.execute(_message(), progress)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "roots_failed": 1,
        "task_key": "sync-115-directory-tree",
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert directory_sync.calls == [progress]
    assert organize_execution.execute_calls == []


@pytest.mark.parametrize(
    ("task_key", "payload", "home_video_only"),
    [
        ("full-sync-strm", None, False),
        ("full-sync-strm", {"home_video_only": True}, True),
        ("full-sync-strm", {"home_video_only": False}, False),
        ("monitor-115-life-events", None, True),
    ],
)
def test_strm_tasks_use_native_service_and_preserve_legacy_home_video_payload(
    task_key: str,
    payload: dict[str, object] | None,
    home_video_only: bool,
) -> None:
    strm_sync = FakeStrmSync(TaskOutcome.partial("fixture partial", {"roots_failed": 1}))
    router = P6P115TaskRouter(FakeDirectorySync(), FakeOrganizeExecution(), strm_sync)
    progress = FakeProgress()

    outcome = router.execute(_message(task_key, payload=payload), progress)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "roots_failed": 1,
        "task_key": task_key,
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert strm_sync.calls == [{"progress": progress, "home_video_only": home_video_only}]


@pytest.mark.parametrize(
    ("task_key", "payload", "error"),
    [
        ("full-sync-strm", {"unexpected": True}, "unsupported parameters"),
        ("full-sync-strm", {"home_video_only": "true"}, "must be a boolean"),
        ("monitor-115-life-events", {"home_video_only": True}, "does not accept parameters"),
    ],
)
def test_strm_tasks_reject_invalid_payload_before_native_execution(
    task_key: str,
    payload: dict[str, object],
    error: str,
) -> None:
    strm_sync = FakeStrmSync()
    router = P6P115TaskRouter(FakeDirectorySync(), FakeOrganizeExecution(), strm_sync)

    with pytest.raises(ValueError, match=error):
        router.execute(_message(task_key, payload=payload), FakeProgress())

    assert strm_sync.calls == []


def test_scan_organize_passes_exact_execution_and_p4_attempt_ownership() -> None:
    directory_sync = FakeDirectorySync()
    organize_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(directory_sync, organize_execution)
    progress = FakeProgress()

    outcome = router.execute(_scan_message(), progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.message == "fixture organize complete"
    assert outcome.details == {
        "execution_id": str(EXECUTION_ID),
        "p4_terminalized": True,
        "task_key": "scan-organize-115",
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert organize_execution.execute_calls == [
        {
            "execution_id": EXECUTION_ID,
            "progress": progress,
            "job_id": JOB_ID,
            "attempt_id": ATTEMPT_ID,
            "worker_id": WORKER_ID,
        }
    ]
    assert directory_sync.calls == []


@pytest.mark.parametrize(
    "status",
    [
        P115ExecutionStatus.REJECTED,
        P115ExecutionStatus.INDETERMINATE,
        P115ExecutionStatus.MANUAL_REVIEW,
    ],
)
def test_scan_organize_terminal_failure_preserves_p4_terminal_marker(
    status: P115ExecutionStatus,
) -> None:
    organize_execution = FakeOrganizeExecution(
        P115ExecutionRunResult(status, "fixture terminal failure", {"status": status.value}, True)
    )
    router = P6P115TaskRouter(FakeDirectorySync(), organize_execution)

    outcome = router.execute(_scan_message(), FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["status"] == status.value
    assert outcome.details["p4_terminalized"] is True


def test_scan_organize_busy_result_is_a_regular_worker_failure() -> None:
    organize_execution = FakeOrganizeExecution(
        P115ExecutionRunResult(
            P115ExecutionStatus.RUNNING,
            "fixture execution is busy",
            {"status": "running"},
            False,
        )
    )
    router = P6P115TaskRouter(FakeDirectorySync(), organize_execution)

    outcome = router.execute(_scan_message(), FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["p4_terminalized"] is False


@pytest.mark.parametrize(
    ("message", "error"),
    [
        (_scan_message(payload={}), "requires exactly"),
        (
            _scan_message(payload={"execution_id": str(EXECUTION_ID), "items": []}),
            "unsupported=.*items",
        ),
        (
            _scan_message(metadata={"attempt_id": str(ATTEMPT_ID), "worker_id": WORKER_ID}),
            "job_id.*UUID",
        ),
        (
            _scan_message(metadata={"job_id": str(JOB_ID), "worker_id": WORKER_ID}),
            "attempt_id.*UUID",
        ),
        (
            _scan_message(metadata={"job_id": str(JOB_ID), "attempt_id": str(ATTEMPT_ID)}),
            "worker_id.*non-empty",
        ),
        (_scan_message(payload={"execution_id": "not-a-uuid"}), "execution_id.*UUID"),
    ],
)
def test_scan_organize_rejects_invalid_envelope_before_execution(
    message: TaskMessage,
    error: str,
) -> None:
    directory_sync = FakeDirectorySync()
    organize_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(directory_sync, organize_execution)

    with pytest.raises(ValueError, match=error):
        router.execute(message, FakeProgress())

    assert organize_execution.execute_calls == []
    assert organize_execution.reconcile_calls == []
    assert directory_sync.calls == []


def test_manual_correction_is_reconciliation_only_and_reports_zero_external_mutations() -> None:
    directory_sync = FakeDirectorySync()
    organize_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(directory_sync, organize_execution)

    outcome = router.execute(
        _message(
            "manual-correct-organize-records",
            payload={
                "execution_id": str(EXECUTION_ID),
                "approved_reconciliation_intent_id": "fixture-reconciliation-intent",
            },
            metadata={"requested_by_user_id": "fixture-admin"},
        ),
        FakeProgress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "reconciliation": {"outcome": "applied"},
        "execution": {"status": "confirmed"},
        "external_mutations_executed": 0,
        "p4_terminalized": False,
        "task_key": "manual-correct-organize-records",
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert organize_execution.reconcile_calls == [
        {
            "execution_id": EXECUTION_ID,
            "approved_intent_id": "fixture-reconciliation-intent",
            "actor": "fixture-admin",
        }
    ]
    assert organize_execution.execute_calls == []
    assert directory_sync.calls == []


def test_manual_correction_uses_the_dedicated_readback_service() -> None:
    mutation_execution = FakeOrganizeExecution()
    readback_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        mutation_execution,
        organize_reconciliation=readback_execution,
    )

    outcome = router.execute(
        _message(
            "manual-correct-organize-records",
            payload={
                "execution_id": str(EXECUTION_ID),
                "approved_reconciliation_intent_id": "fixture-reconciliation-intent",
            },
        ),
        FakeProgress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert mutation_execution.execute_calls == []
    assert mutation_execution.reconcile_calls == []
    assert len(readback_execution.reconcile_calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"items": [{"record_id": 1}]},
        {"execution_id": str(EXECUTION_ID)},
        {
            "execution_id": str(EXECUTION_ID),
            "approved_reconciliation_intent_id": "fixture-intent",
            "record_ids": [1],
        },
        {
            "execution_id": "not-a-uuid",
            "approved_reconciliation_intent_id": "fixture-intent",
        },
        {
            "execution_id": str(EXECUTION_ID),
            "approved_reconciliation_intent_id": " ",
        },
    ],
)
def test_manual_correction_rejects_legacy_or_invalid_payload_before_reconciliation(
    payload: dict[str, object],
) -> None:
    organize_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(FakeDirectorySync(), organize_execution)

    with pytest.raises(ValueError):
        router.execute(
            _message("manual-correct-organize-records", payload=payload),
            FakeProgress(),
        )

    assert organize_execution.reconcile_calls == []
    assert organize_execution.execute_calls == []


@pytest.mark.parametrize(
    ("message", "error_type", "error"),
    [
        (_message(payload={"unexpected": True}), ValueError, "does not accept parameters"),
        (
            _message(processor=ProcessorIdentity.ACTOR),
            ValueError,
            "media processor identity",
        ),
        (_message(version=2), ValueError, "payload version"),
        (_message("unknown-p6-task"), LookupError, "unknown P6 115 task key"),
    ],
)
def test_router_rejects_invalid_common_envelope_before_any_use_case(
    message: TaskMessage,
    error_type: type[Exception],
    error: str,
) -> None:
    directory_sync = FakeDirectorySync()
    organize_execution = FakeOrganizeExecution()
    router = P6P115TaskRouter(directory_sync, organize_execution)

    with pytest.raises(error_type, match=error):
        router.execute(message, FakeProgress())

    assert directory_sync.calls == []
    assert organize_execution.execute_calls == []
    assert organize_execution.reconcile_calls == []


def test_fingerprint_repair_uses_native_p6_use_case_without_processor() -> None:
    repair = FakeFingerprintRepair(TaskOutcome.partial("fixture partial", {"fixed_assets": 1, "failed_assets": 1}))
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        fingerprint_repair=repair,
    )
    progress = FakeProgress()

    outcome = router.execute(_message("repair-p115-fingerprints"), progress)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "fixed_assets": 1,
        "failed_assets": 1,
        "task_key": "repair-p115-fingerprints",
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert repair.calls == [progress]


def test_fingerprint_repair_rejects_payload_before_use_case() -> None:
    repair = FakeFingerprintRepair()
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        fingerprint_repair=repair,
    )

    with pytest.raises(ValueError, match="does not accept parameters"):
        router.execute(
            _message("repair-p115-fingerprints", payload={"unexpected": True}),
            FakeProgress(),
        )

    assert repair.calls == []


def test_playback_daily_speedtest_uses_native_service_and_preserves_partial_result() -> None:
    playback = FakePlaybackMaintenance(
        {
            "failed_count": 1,
            "results": [{"account_id": "fixture-account", "success": False}],
            "message": "fixture playback speedtest partial",
        }
    )
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        playback=playback,
    )
    progress = FakeProgress()

    outcome = router.execute(_message("play-pool-daily-speedtest"), progress)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "failed_count": 1,
        "results": [{"account_id": "fixture-account", "success": False}],
        "task_key": "play-pool-daily-speedtest",
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert playback.calls == [progress]


def test_playback_daily_speedtest_rejects_payload_before_use_case() -> None:
    playback = FakePlaybackMaintenance()
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        playback=playback,
    )

    with pytest.raises(ValueError, match="does not accept parameters"):
        router.execute(
            _message("play-pool-daily-speedtest", payload={"unexpected": True}),
            FakeProgress(),
        )

    assert playback.calls == []


def test_intro_backfill_keeps_frozen_watchlist_processor_and_native_empty_payload() -> None:
    intro = FakeIntroBackfill(TaskOutcome.partial("fixture intro partial", {"count": 2, "duplicates": 1}))
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        intro_backfill=intro,
    )
    progress = FakeProgress()

    outcome = router.execute(
        _message(
            "intro-fingerprint-backfill",
            processor=ProcessorIdentity.WATCHLIST,
        ),
        progress,
    )

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "count": 2,
        "duplicates": 1,
        "task_key": "intro-fingerprint-backfill",
        "native_handler": "P6P115TaskRouter",
        "compatibility_fallback": False,
    }
    assert intro.calls == [{"progress": progress, "force": True}]


def test_intro_backfill_accepts_explicit_non_forced_route_submission() -> None:
    intro = FakeIntroBackfill()
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        intro_backfill=intro,
    )
    progress = FakeProgress()

    outcome = router.execute(
        _message(
            "intro-fingerprint-backfill",
            processor=ProcessorIdentity.WATCHLIST,
            payload={"force": False},
        ),
        progress,
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert intro.calls == [{"progress": progress, "force": False}]


@pytest.mark.parametrize(
    ("processor", "payload", "error"),
    [
        (ProcessorIdentity.MEDIA, None, "watchlist processor identity"),
        (ProcessorIdentity.WATCHLIST, {"limit": 10}, "unsupported parameters"),
    ],
)
def test_intro_backfill_rejects_processor_or_payload_drift(
    processor: ProcessorIdentity,
    payload: dict[str, object] | None,
    error: str,
) -> None:
    intro = FakeIntroBackfill()
    router = P6P115TaskRouter(
        FakeDirectorySync(),
        FakeOrganizeExecution(),
        intro_backfill=intro,
    )

    with pytest.raises(ValueError, match=error):
        router.execute(
            _message(
                "intro-fingerprint-backfill",
                processor=processor,
                payload=payload,
            ),
            FakeProgress(),
        )

    assert intro.calls == []
