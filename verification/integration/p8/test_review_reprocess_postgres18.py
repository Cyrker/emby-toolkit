from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.domains.operations.review_reprocess import (
    REVIEW_REPROCESS_ONE_TASK_KEY,
    PersistentReviewJobGateway,
    SqlAlchemyReviewReprocessRepository,
)
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore
from etk.platform.tasks import TaskRegistry

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification/fixtures/tasks/ordered-task-catalog.json"


def test_review_lookup_and_fixed_internal_job_envelope_are_durable_and_idempotent(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator)
                VALUES ('fixture-admin', 'Fixture Admin', TRUE)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO failed_log (item_id, item_name, reason, failed_at)
                VALUES ('item-1', 'Latest Name', 'Latest Reason', NOW())
                """
            )
        )
    repository = SqlAlchemyReviewReprocessRepository(p8_runtime.unit_of_work)
    jobs = JobSubmissionService(
        PostgresJobStore(p8_runtime.unit_of_work),
        TaskRegistry.from_fixture(TASK_FIXTURE),
    )
    gateway = PersistentReviewJobGateway(jobs)

    item = repository.item("item-1")
    first = gateway.submit_one(
        item_id="item-1",
        item_name="Latest Name",
        reason="Latest Reason",
        requested_by_user_id="fixture-admin",
        trace_id="trace-review",
        idempotency_key="review-request",
    )
    replay = gateway.submit_one(
        item_id="item-1",
        item_name="Latest Name",
        reason="Latest Reason",
        requested_by_user_id="fixture-admin",
        trace_id="trace-review",
        idempotency_key="review-request",
    )

    assert item == {
        "item_id": "item-1",
        "item_name": "Latest Name",
        "reason": "Latest Reason",
    }
    assert replay.projection.job.id == first.projection.job.id
    assert replay.reused is True
    with pytest.raises(TaskBusyError):
        gateway.submit_all(
            reason_filter=None,
            requested_by_user_id="fixture-admin",
            trace_id=None,
            idempotency_key=None,
        )

    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT task_key, request_payload_json, requested_by_user_id,
                           trace_id, max_attempts, timeout_seconds
                    FROM job_runs
                    """
                )
            )
            .mappings()
            .one()
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM job_attempts")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM idempotency_records")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 1

    assert row["task_key"] == REVIEW_REPROCESS_ONE_TASK_KEY
    assert row["requested_by_user_id"] == "fixture-admin"
    assert row["trace_id"] == "trace-review"
    assert row["max_attempts"] == 1
    assert row["timeout_seconds"] == 3600
    assert row["request_payload_json"] == {
        "payload_version": 1,
        "parameters": {
            "item_id": "item-1",
            "item_name_for_ui": "Latest Name",
            "failure_reason": "Latest Reason",
        },
        "processor_type": "media",
        "display_name": "重新处理: Latest Name",
        "silent": False,
    }
    assert "legacy_operation" not in row["request_payload_json"]
