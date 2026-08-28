from __future__ import annotations

from typing import Any

from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome
from etk.processes.p6_p115_tasks import P6P115TaskRouter


class _Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        return None


class _Backfill:
    def __init__(self) -> None:
        self.progress: Any = None

    def execute(self, progress: Any) -> TaskOutcome:
        self.progress = progress
        progress.update(10, "matching")
        return TaskOutcome.succeeded("done", {"updated_count": 2})


def test_p6_router_dispatches_organize_record_tmdb_backfill_without_payload() -> None:
    backfill = _Backfill()
    router = P6P115TaskRouter(
        organize_record_tmdb_backfill=backfill,
    )
    progress = _Progress()
    message = TaskMessage.create(
        task_key="backfill-organize-record-tmdb-ids",
        processor=ProcessorIdentity.MEDIA,
    )

    result = router.execute(message, progress)

    assert result.to_result() == {
        "ok": True,
        "status": "succeeded",
        "message": "done",
        "details": {
            "updated_count": 2,
            "task_key": "backfill-organize-record-tmdb-ids",
            "native_handler": "P6P115TaskRouter",
            "compatibility_fallback": False,
        },
    }
    assert backfill.progress is progress
    assert progress.updates == [(10, "matching")]


def test_p6_router_rejects_backfill_payload_or_missing_use_case() -> None:
    message = TaskMessage.create(
        task_key="backfill-organize-record-tmdb-ids",
        processor=ProcessorIdentity.MEDIA,
        payload={"unexpected": True},
    )
    router = P6P115TaskRouter()

    try:
        router.execute(message, _Progress())
    except ValueError as exc:
        assert "does not accept parameters" in str(exc)
    else:
        raise AssertionError("payload must be rejected")

    empty = TaskMessage.create(
        task_key="backfill-organize-record-tmdb-ids",
        processor=ProcessorIdentity.MEDIA,
    )
    try:
        router.execute(empty, _Progress())
    except RuntimeError as exc:
        assert "backfill use case is unavailable" in str(exc)
    else:
        raise AssertionError("missing use case must be rejected")
