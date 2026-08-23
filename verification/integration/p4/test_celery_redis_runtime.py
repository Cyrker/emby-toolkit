from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import pytest
from redis import Redis
from sqlalchemy import text

from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import AttemptStatus, JobProjection, JobStatus, PostgresJobStore
from etk.processes.celery_app import EXECUTE_JOB_TASK, create_celery_app
from etk.processes.runtime_config import TaskRuntimeConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
REDIS_IMAGE = "redis:7-alpine@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf"
OPERATION_MODULE = "verification.integration.p4.test_celery_redis_runtime"

pytestmark = pytest.mark.skipif(
    os.environ.get("ETK_RUN_CELERY_REDIS_INTEGRATION") != "1",
    reason="set ETK_RUN_CELERY_REDIS_INTEGRATION=1 to start disposable Redis and Celery processes",
)


def _safe_point_operation(processor: Any, *, marker: str, mode: str) -> dict[str, Any]:
    """Test-only, importable operation with no external service calls."""

    marker_path = Path(marker)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.with_suffix(".started").write_text(str(os.getpid()), encoding="utf-8")
    if mode == "block-once":
        first_marker = marker_path.with_suffix(".first")
        try:
            descriptor = os.open(first_marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return {"second_attempt": True}
        else:
            os.close(descriptor)
    elif mode != "safe-point":
        raise ValueError(f"unknown test operation mode: {mode}")

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if processor is not None and processor.is_stop_requested():
            marker_path.with_suffix(".stopped").write_text("observed", encoding="utf-8")
            return {"stop_observed": True}
        time.sleep(0.05)
    raise TimeoutError("test operation did not observe a cooperative stop before its safety deadline")


@dataclass(slots=True)
class RedisContainer:
    name: str
    url: str

    def start(self) -> None:
        state = _docker(
            "inspect",
            "--format",
            "{{.State.Running}} {{.State.Paused}}",
            self.name,
            check=False,
        )
        if state.returncode != 0:
            raise AssertionError(f"disposable Redis container disappeared: {state.stderr}")
        running, paused = state.stdout.split()
        if running != "true":
            _docker("start", self.name)
        elif paused == "true":
            _docker("unpause", self.name)
        _wait_for_redis(self.url)

    def stop(self) -> None:
        state = _docker(
            "inspect",
            "--format",
            "{{.State.Running}} {{.State.Paused}}",
            self.name,
            check=False,
        )
        if state.returncode == 0:
            running, paused = state.stdout.split()
            if running == "true" and paused != "true":
                # Pausing makes the real Redis endpoint unavailable while preserving
                # Docker Desktop's dynamically allocated loopback-only host port.
                _docker("pause", self.name)

    def flush(self) -> None:
        client = Redis.from_url(self.url, socket_connect_timeout=1, socket_timeout=1)
        try:
            client.flushdb()
        finally:
            client.close()


@dataclass(frozen=True, slots=True)
class RuntimeEnvironment:
    environment: dict[str, str]
    config: TaskRuntimeConfig
    data_dir: Path
    queue_name: str


@dataclass(slots=True)
class ManagedProcess:
    process: subprocess.Popen[str]
    stream: TextIO
    log_path: Path

    def read_log(self) -> str:
        if not self.stream.closed:
            self.stream.flush()
        return self.log_path.read_text(encoding="utf-8", errors="replace") if self.log_path.exists() else ""

    def stop(self, *, timeout: float = 35) -> int:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.kill()
            raise AssertionError(f"process ignored SIGTERM:\n{self.read_log()}") from exc
        finally:
            self.stream.close()
        return code

    def kill(self) -> None:
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.stream.close()


@pytest.fixture(scope="session")
def p4_redis_container() -> Iterator[RedisContainer]:
    info = _docker("info", "--format", "{{.ServerVersion}}", check=False)
    if info.returncode != 0:
        pytest.fail(f"Docker is required for the P4 Celery/Redis integration gate: {info.stderr}")

    name = f"etk-p4-redis-{uuid4().hex[:12]}"
    started = _docker(
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        "com.etk.verification=p4-celery-redis",
        "--publish",
        "127.0.0.1::6379",
        REDIS_IMAGE,
        "redis-server",
        "--appendonly",
        "no",
        "--save",
        "",
        check=False,
    )
    if started.returncode != 0:
        pytest.fail(f"unable to start disposable Redis: {started.stderr}")
    try:
        port = _docker(
            "inspect",
            "--format",
            '{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}',
            name,
        ).stdout.strip()
        controller = RedisContainer(name=name, url=f"redis://127.0.0.1:{port}/15")
        controller.start()
        yield controller
    finally:
        _docker("rm", "--force", "--volumes", name, check=False)


@pytest.fixture()
def p4_runtime_environment(
    p4_redis_container: RedisContainer,
    migrated_p4_postgres_url: str,
    tmp_path: Path,
) -> Iterator[RuntimeEnvironment]:
    p4_redis_container.start()
    p4_redis_container.flush()
    database = urlsplit(migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1))
    data_dir = tmp_path / "config"
    home_dir = tmp_path / "home"
    data_dir.mkdir()
    home_dir.mkdir()
    queue_name = f"p4.integration.{uuid4().hex}"
    environment = os.environ.copy()
    environment.update(
        {
            "APP_DATA_DIR": str(data_dir),
            "CONFIG_DIR": str(data_dir),
            "DB_HOST": database.hostname or "127.0.0.1",
            "DB_PORT": str(database.port or 5432),
            "DB_USER": database.username or "",
            "DB_PASSWORD": database.password or "",
            "DB_NAME": database.path.lstrip("/"),
            "ETK_DATABASE_URL": migrated_p4_postgres_url,
            "ETK_REDIS_URL": p4_redis_container.url,
            "ETK_TASK_QUEUE": queue_name,
            "ETK_JOB_HEARTBEAT_SECONDS": "1",
            "ETK_JOB_LEASE_SECONDS": "4",
            "ETK_COORDINATOR_INTERVAL_SECONDS": "0.2",
            "ETK_OUTBOX_BATCH_SIZE": "10",
            "ETK_OUTBOX_CLAIM_TIMEOUT_SECONDS": "2",
            "ETK_WORKER_CONCURRENCY": "1",
            "ETK_WORKER_SHUTDOWN_TIMEOUT_SECONDS": "5",
            "ETK_COORDINATOR_SCHEDULES_ENABLED": "false",
            "HOME": str(home_dir),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    config = TaskRuntimeConfig(
        broker_url=p4_redis_container.url,
        queue_name=queue_name,
        heartbeat_seconds=1,
        lease_seconds=4,
        coordinator_interval_seconds=0.2,
        outbox_batch_size=10,
        outbox_claim_timeout_seconds=2,
        worker_concurrency=1,
        worker_shutdown_timeout_seconds=5,
        schedules_enabled=False,
    )
    try:
        yield RuntimeEnvironment(environment, config, data_dir, queue_name)
    finally:
        p4_redis_container.start()
        p4_redis_container.flush()


def test_redis_outage_preserves_job_and_outbox_then_recovers_delivery(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_redis_container: RedisContainer,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    p4_redis_container.stop()
    created = _create_empty_chain_job(p4_store, p4_runtime_environment.queue_name)
    with p4_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM job_runs")) == 1
        assert connection.scalar(text("SELECT count(*) FROM job_attempts")) == 1
        assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 1
    coordinator = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator")
    worker: ManagedProcess | None = None
    try:

        def publish_failed() -> bool:
            with p4_runtime.engine.connect() as connection:
                row = (
                    connection.execute(text("SELECT status, publish_attempts, last_error FROM outbox_events"))
                    .mappings()
                    .one()
                )
            return row["status"] == "pending" and row["publish_attempts"] >= 1 and bool(row["last_error"])

        _wait_until(publish_failed, description="Coordinator to persist a real Redis publish failure", timeout=20)
        projection = p4_store.get_job(created.job.id)
        assert projection is not None
        assert projection.job.status is JobStatus.QUEUED

        p4_redis_container.start()
        worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
        _wait_for_worker(worker, p4_runtime_environment.config)
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        with p4_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT status FROM outbox_events")) == "processed"
    finally:
        p4_redis_container.start()
        if worker is not None:
            assert worker.stop() == 0
        assert coordinator.stop() == 128 + signal.SIGTERM


def test_duplicate_celery_delivery_for_same_attempt_executes_once(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    created = _create_empty_chain_job(p4_store, p4_runtime_environment.queue_name)
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id, count=2)
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert completed.current_attempt.status is AttemptStatus.SUCCEEDED
        _wait_until(
            lambda: "Duplicate or stale delivery skipped" in worker.read_log(),
            description="both duplicate Celery messages to be consumed",
            timeout=15,
        )
        with p4_runtime.engine.connect() as connection:
            attempts = connection.execute(
                text("SELECT attempt_no, status FROM job_attempts WHERE job_id = :job_id ORDER BY attempt_no"),
                {"job_id": created.job.id},
            ).all()
        assert attempts == [(1, "succeeded")]
    finally:
        assert worker.stop() == 0


def test_sigkill_worker_child_recovers_expired_lease_and_retries(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "kill-recovery"
    created = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=marker,
        mode="block-once",
        max_attempts=2,
    )
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    coordinator: ManagedProcess | None = None
    replacement: ManagedProcess | None = None
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        coordinator = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator")
        running = _wait_for_job(p4_store, created.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(marker.with_suffix(".first").exists, description="first worker attempt to enter the test operation")
        worker_id = running.current_attempt.worker_id
        assert worker_id is not None
        execution_pid = int(worker_id.rsplit(":", 1)[1])
        os.kill(execution_pid, signal.SIGKILL)
        if execution_pid == worker.process.pid:
            worker.process.wait(timeout=10)
            worker.stream.close()
            replacement = _start_worker(p4_runtime_environment, tmp_path / "replacement-worker")
            _wait_for_worker(replacement, p4_runtime_environment.config)

        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=45)
        assert completed.current_attempt.attempt_no == 2
        with p4_runtime.engine.connect() as connection:
            attempts = connection.execute(
                text(
                    "SELECT attempt_no, status, error_type FROM job_attempts WHERE job_id = :job_id ORDER BY attempt_no"
                ),
                {"job_id": created.job.id},
            ).all()
        assert attempts == [(1, "lost", "worker_lost"), (2, "succeeded", None)]
    finally:
        if coordinator is not None:
            assert coordinator.stop() == 128 + signal.SIGTERM
        if replacement is not None:
            assert replacement.stop() == 0
        elif worker.process.poll() is None:
            assert worker.stop() == 0


@pytest.mark.parametrize(
    ("control", "terminal_status", "attempt_status"),
    [
        ("cancel", JobStatus.CANCELLED, AttemptStatus.CANCELLED),
        ("timeout", JobStatus.TIMED_OUT, AttemptStatus.TIMED_OUT),
    ],
)
def test_worker_cooperative_safe_point_persists_cancel_and_timeout(
    control: str,
    terminal_status: JobStatus,
    attempt_status: AttemptStatus,
    p4_store: PostgresJobStore,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / control
    created = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=marker,
        mode="safe-point",
        timeout_seconds=1 if control == "timeout" else None,
    )
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id)
        _wait_for_job(p4_store, created.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(marker.with_suffix(".started").exists, description="test operation to reach its safe point")
        if control == "cancel":
            requested = p4_store.request_cancel(created.job.id)
            assert requested.job.status is JobStatus.CANCEL_REQUESTED
        completed = _wait_for_job(p4_store, created.job.id, {terminal_status}, timeout=20)
        assert completed.current_attempt.status is attempt_status
        assert marker.with_suffix(".stopped").read_text(encoding="utf-8") == "observed"
    finally:
        assert worker.stop() == 0


def test_two_workers_keep_root_backlog_globally_serial(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    first_marker = tmp_path / "serial-first"
    second_marker = tmp_path / "serial-second"
    first = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=first_marker,
        mode="safe-point",
    )
    second = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=second_marker,
        mode="safe-point",
    )
    workers = [
        _start_worker(p4_runtime_environment, tmp_path / "worker-1"),
        _start_worker(p4_runtime_environment, tmp_path / "worker-2"),
    ]
    try:
        for worker in workers:
            _wait_for_worker(worker, p4_runtime_environment.config)

        _send_delivery(p4_runtime_environment.config, first.job.id, first.attempt.id)
        _wait_for_job(p4_store, first.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(first_marker.with_suffix(".started").exists, description="first root operation to start")

        _send_delivery(p4_runtime_environment.config, second.job.id, second.attempt.id)

        def dispatch_was_deferred() -> bool:
            with p4_runtime.engine.connect() as connection:
                row = connection.execute(
                    text(
                        """
                        SELECT status, available_at > created_at
                        FROM outbox_events
                        WHERE aggregate_id = :attempt_id
                        """
                    ),
                    {"attempt_id": str(second.attempt.id)},
                ).one()
            return row == ("pending", True)

        _wait_until(
            dispatch_was_deferred,
            description="second root delivery to be deferred while the first root is running",
        )
        assert not second_marker.with_suffix(".started").exists()
        queued = p4_store.get_job(second.job.id)
        assert queued is not None and queued.job.status is JobStatus.QUEUED

        p4_store.request_cancel(first.job.id)
        _wait_for_job(p4_store, first.job.id, {JobStatus.CANCELLED}, timeout=10)

        _send_delivery(p4_runtime_environment.config, second.job.id, second.attempt.id)
        _wait_for_job(p4_store, second.job.id, {JobStatus.RUNNING}, timeout=20)
        _wait_until(second_marker.with_suffix(".started").exists, description="second root operation to start")
        p4_store.request_cancel(second.job.id)
        _wait_for_job(p4_store, second.job.id, {JobStatus.CANCELLED}, timeout=10)
    finally:
        for worker in workers:
            if worker.process.poll() is None:
                assert worker.stop() == 0


def test_two_real_coordinators_elect_one_leader_and_fail_over_on_sigterm(
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    first = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator-1")
    second = _start_coordinator(p4_runtime_environment, tmp_path / "coordinator-2")
    processes = [first, second]
    try:

        def elected() -> tuple[ManagedProcess, ManagedProcess] | None:
            states = [_health_state(process.log_path.with_suffix(".health.json")) for process in processes]
            if states.count("leader") == 1 and states.count("standby") == 1:
                return processes[states.index("leader")], processes[states.index("standby")]
            return None

        leader, standby = _wait_until(elected, description="exactly one Coordinator leader", timeout=30)
        assert leader.stop() == 128 + signal.SIGTERM
        assert _health_state(leader.log_path.with_suffix(".health.json")) == "stopped"
        _wait_until(
            lambda: _health_state(standby.log_path.with_suffix(".health.json")) == "leader",
            description="standby Coordinator to acquire the released PostgreSQL session lock",
            timeout=20,
        )
        assert standby.stop() == 128 + signal.SIGTERM
        assert _health_state(standby.log_path.with_suffix(".health.json")) == "stopped"
    finally:
        for process in processes:
            if process.process.poll() is None:
                process.kill()


def test_worker_executes_empty_chain_then_stops_cleanly_on_sigterm(
    p4_store: PostgresJobStore,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    created = _create_empty_chain_job(p4_store, p4_runtime_environment.queue_name)
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id)
        _wait_for_job(p4_store, created.job.id, {JobStatus.SUCCEEDED}, timeout=30)
        assert worker.stop() == 0
        log = worker.read_log()
        assert "Warm shutdown" in log
        assert "Worker runtime shutdown failed" not in log
        assert "Traceback (most recent call last)" not in log
    finally:
        if worker.process.poll() is None:
            worker.kill()


def test_active_worker_sigterm_reaches_cooperative_safe_point_before_shutdown_timeout(
    p4_store: PostgresJobStore,
    p4_runtime_environment: RuntimeEnvironment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "active-sigterm"
    created = _create_operation_job(
        p4_store,
        p4_runtime_environment.queue_name,
        marker=marker,
        mode="safe-point",
    )
    worker = _start_worker(p4_runtime_environment, tmp_path / "worker")
    try:
        _wait_for_worker(worker, p4_runtime_environment.config)
        _send_delivery(p4_runtime_environment.config, created.job.id, created.attempt.id)
        _wait_for_job(p4_store, created.job.id, {JobStatus.RUNNING}, timeout=30)
        _wait_until(marker.with_suffix(".started").exists, description="active operation to reach its safe point")

        assert worker.stop(timeout=p4_runtime_environment.config.worker_shutdown_timeout_seconds + 3) == 0
        assert marker.with_suffix(".stopped").read_text(encoding="utf-8") == "observed"
        completed = _wait_for_job(p4_store, created.job.id, {JobStatus.CANCELLED}, timeout=5)
        assert completed.current_attempt.status is AttemptStatus.CANCELLED
    finally:
        if worker.process.poll() is None:
            worker.kill()


def _create_empty_chain_job(store: PostgresJobStore, queue_name: str):
    return store.create_job(
        task_key="task-chain-high-freq",
        queue_name=queue_name,
        payload={
            "payload_version": 1,
            "parameters": {},
            "processor_type": "media",
            "display_name": "P4 empty task-chain smoke",
            "silent": True,
        },
    )


def _create_operation_job(
    store: PostgresJobStore,
    queue_name: str,
    *,
    marker: Path,
    mode: str,
    max_attempts: int = 1,
    timeout_seconds: int | None = None,
):
    return store.create_job(
        task_key="p4-test-safe-point",
        queue_name=queue_name,
        max_attempts=max_attempts,
        timeout_seconds=timeout_seconds,
        payload={
            "payload_version": 1,
            "parameters": {"marker": str(marker), "mode": mode},
            "positional_parameters": [],
            "processor_type": "media",
            "display_name": "P4 test-only safe point",
            "silent": True,
            "legacy_operation": {"module": OPERATION_MODULE, "qualname": "_safe_point_operation"},
        },
    )


def _start_worker(runtime: RuntimeEnvironment, log_path: Path) -> ManagedProcess:
    return _start_process(
        [
            sys.executable,
            "main.py",
            "worker",
            "--log-level",
            "INFO",
            "--concurrency",
            "1",
            "--hostname",
            f"p4-{uuid4().hex[:8]}@%h",
        ],
        runtime.environment,
        log_path.with_suffix(".log"),
    )


def _start_coordinator(runtime: RuntimeEnvironment, log_path: Path) -> ManagedProcess:
    health_path = log_path.with_suffix(".health.json")
    return _start_process(
        [sys.executable, "main.py", "coordinator", "--health-file", str(health_path)],
        runtime.environment,
        log_path.with_suffix(".log"),
    )


def _start_process(command: list[str], environment: Mapping[str, str], log_path: Path) -> ManagedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stream = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        env=dict(environment),
        stdout=stream,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    return ManagedProcess(process=process, stream=stream, log_path=log_path)


def _wait_for_worker(worker: ManagedProcess, config: TaskRuntimeConfig) -> None:
    app = create_celery_app(config)
    try:

        def ping() -> bool:
            _assert_running(worker)
            return bool(app.control.ping(timeout=0.5))

        _wait_until(ping, description="Celery Worker ping", timeout=30)
    finally:
        app.close()


def _send_delivery(config: TaskRuntimeConfig, job_id: UUID, attempt_id: UUID, *, count: int = 1) -> None:
    app = create_celery_app(config)
    try:
        for _ in range(count):
            app.send_task(
                EXECUTE_JOB_TASK,
                kwargs={"event_id": str(uuid4()), "job_id": str(job_id), "attempt_id": str(attempt_id)},
                task_id=str(attempt_id),
                queue=config.queue_name,
                routing_key=config.queue_name,
                serializer="json",
            )
    finally:
        app.close()


def _wait_for_job(
    store: PostgresJobStore,
    job_id: UUID,
    statuses: set[JobStatus],
    *,
    timeout: float,
) -> JobProjection:
    def selected() -> JobProjection | None:
        projection = store.get_job(job_id)
        if projection is not None and projection.job.status in statuses:
            return projection
        return None

    return _wait_until(
        selected,
        description=f"Job {job_id} to reach {[status.value for status in statuses]}",
        timeout=timeout,
    )


def _health_state(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    state = payload.get("state")
    return str(state) if state is not None else None


def _wait_until(
    predicate: Callable[[], Any],
    *,
    description: str,
    timeout: float = 15,
) -> Any:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
        except BaseException as exc:
            last_error = exc
        else:
            if value:
                return value
        time.sleep(0.1)
    if last_error is not None:
        raise AssertionError(f"timed out waiting for {description}") from last_error
    raise AssertionError(f"timed out waiting for {description}")


def _assert_running(process: ManagedProcess) -> None:
    code = process.process.poll()
    if code is not None:
        raise AssertionError(f"process exited with {code}:\n{process.read_log()}")


def _wait_for_redis(url: str) -> None:
    client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        _wait_until(client.ping, description="disposable Redis PING", timeout=15)
    finally:
        client.close()


def _docker(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if check and result.returncode != 0:
        raise AssertionError(f"docker {' '.join(arguments)} failed:\n{result.stderr}")
    return result
