from __future__ import annotations

from celery import Celery
from celery.exceptions import Reject
from celery.signals import setup_logging, worker_process_shutdown, worker_shutdown, worker_shutting_down
from kombu import Queue

from .runtime_config import TaskRuntimeConfig

EXECUTE_JOB_TASK = "etk.jobs.execute"
EXECUTE_COLLECTION_OPERATION_TASK = "etk.collection.operation.execute"
EXECUTE_IDENTITY_OPERATION_TASK = "etk.identity.operation.execute"
EXECUTE_MEDIA_EFFECT_TASK = "etk.media.effects.execute"
EXECUTE_INGESTION_MONITOR_TASK = "etk.ingestion.monitor.execute"
EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK = "etk.p115.intro_detection.execute"
EXECUTE_P115_PLAYBACK_OPERATION_TASK = "etk.p115.playback.execute"
EXECUTE_P115_UPLOAD_OPERATION_TASK = "etk.p115.upload.execute"
EXECUTE_SUBSCRIPTION_OPERATION_TASK = "etk.subscription.operation.execute"
EXECUTE_DATABASE_MAINTENANCE_TASK = "etk.database.maintenance.execute"
EXECUTE_SHARING_OPERATION_TASK = "etk.sharing.operation.execute"


@setup_logging.connect
def _configure_worker_logging(loglevel=None, **_kwargs) -> None:
    import logging
    import os

    from etk.platform.observability import configure_logging

    configure_logging(
        json_output=os.environ.get("ETK_LOG_FORMAT", "console").casefold() == "json",
        level=loglevel if loglevel is not None else logging.INFO,
    )


def create_celery_app(config: TaskRuntimeConfig | None = None) -> Celery:
    """Create the JSON-only Celery transport without materializing Worker runtime."""

    runtime = config or TaskRuntimeConfig.from_environ()
    app = Celery("etk", broker=runtime.broker_url)
    app.conf.update(
        accept_content=["json"],
        broker_connection_retry=True,
        broker_connection_retry_on_startup=True,
        broker_connection_timeout=3,
        broker_transport_options={
            "socket_connect_timeout": 2,
            "socket_timeout": 2,
            "visibility_timeout": max(runtime.lease_seconds * 4, 300),
        },
        enable_utc=True,
        result_backend=None,
        task_acks_late=True,
        task_default_exchange="etk.jobs",
        task_default_exchange_type="direct",
        task_default_queue=runtime.queue_name,
        task_default_routing_key=runtime.queue_name,
        task_ignore_result=True,
        task_queues=(
            Queue(
                runtime.queue_name,
                exchange="etk.jobs",
                routing_key=runtime.queue_name,
                durable=True,
            ),
        ),
        task_reject_on_worker_lost=True,
        task_publish_retry=False,
        task_routes={
            EXECUTE_JOB_TASK: {"queue": runtime.queue_name},
            EXECUTE_COLLECTION_OPERATION_TASK: {"queue": runtime.queue_name},
            EXECUTE_IDENTITY_OPERATION_TASK: {"queue": runtime.queue_name},
            EXECUTE_MEDIA_EFFECT_TASK: {"queue": runtime.queue_name},
            EXECUTE_INGESTION_MONITOR_TASK: {"queue": runtime.queue_name},
            EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK: {"queue": runtime.queue_name},
            EXECUTE_P115_PLAYBACK_OPERATION_TASK: {"queue": runtime.queue_name},
            EXECUTE_P115_UPLOAD_OPERATION_TASK: {"queue": runtime.queue_name},
            EXECUTE_SUBSCRIPTION_OPERATION_TASK: {"queue": runtime.queue_name},
            EXECUTE_DATABASE_MAINTENANCE_TASK: {"queue": runtime.queue_name},
            EXECUTE_SHARING_OPERATION_TASK: {"queue": runtime.queue_name},
        },
        task_serializer="json",
        timezone="Asia/Shanghai",
        worker_cancel_long_running_tasks_on_connection_loss=True,
        worker_enable_remote_control=True,
        worker_prefetch_multiplier=1,
        worker_soft_shutdown_timeout=runtime.worker_shutdown_timeout_seconds,
    )

    @app.task(
        bind=True,
        name=EXECUTE_JOB_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_job(_task, *, event_id: str, job_id: str, attempt_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute(
                event_id=event_id,
                job_id=job_id,
                attempt_id=attempt_id,
            )
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_MEDIA_EFFECT_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_media_effect(_task, *, effect_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_media_effect(effect_id=effect_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_INGESTION_MONITOR_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_ingestion_monitor(_task, *, item_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_ingestion_monitor(item_id=item_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_P115_UPLOAD_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_p115_upload_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_p115_upload_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_P115_PLAYBACK_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_p115_playback_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_p115_playback_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_p115_intro_detection_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_p115_intro_detection_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_SUBSCRIPTION_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_subscription_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_subscription_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_COLLECTION_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_collection_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_collection_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_IDENTITY_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_identity_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_identity_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_DATABASE_MAINTENANCE_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_database_maintenance(
        _task,
        *,
        operation_id: str,
        artifact_id: str | None = None,
    ) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_database_maintenance(
                operation_id=operation_id,
                artifact_id=artifact_id,
            )
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    @app.task(
        bind=True,
        name=EXECUTE_SHARING_OPERATION_TASK,
        acks_late=True,
        ignore_result=True,
        reject_on_worker_lost=True,
        serializer="json",
        typing=True,
    )
    def execute_sharing_operation(_task, *, operation_id: str) -> None:
        from .worker import WorkerShutdownRequested, get_worker_runtime, worker_shutdown_requested

        if worker_shutdown_requested():
            raise Reject("Worker shutdown is in progress", requeue=True)
        try:
            get_worker_runtime().execute_sharing_operation(operation_id=operation_id)
        except WorkerShutdownRequested as exc:
            raise Reject(str(exc), requeue=True) from exc

    return app


_CELERY_APP: Celery | None = None


def get_celery_app() -> Celery:
    global _CELERY_APP
    if _CELERY_APP is None:
        _CELERY_APP = create_celery_app()
    return _CELERY_APP


# Celery's ``-A`` loader expects a module-level application. Creating this
# object is side-effect free: no broker, database, task graph, or processors
# are contacted until a process command or task is executed.
app = get_celery_app()


@worker_shutting_down.connect
def _request_worker_shutdown(**_kwargs: object) -> None:
    from .worker import request_worker_shutdown

    request_worker_shutdown()


@worker_process_shutdown.connect
@worker_shutdown.connect
def _close_worker_runtime(**_kwargs: object) -> None:
    from .worker import close_worker_runtime

    close_worker_runtime()


__all__ = [
    "EXECUTE_COLLECTION_OPERATION_TASK",
    "EXECUTE_JOB_TASK",
    "EXECUTE_IDENTITY_OPERATION_TASK",
    "EXECUTE_DATABASE_MAINTENANCE_TASK",
    "EXECUTE_INGESTION_MONITOR_TASK",
    "EXECUTE_MEDIA_EFFECT_TASK",
    "EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK",
    "EXECUTE_P115_PLAYBACK_OPERATION_TASK",
    "EXECUTE_P115_UPLOAD_OPERATION_TASK",
    "EXECUTE_SUBSCRIPTION_OPERATION_TASK",
    "EXECUTE_SHARING_OPERATION_TASK",
    "app",
    "create_celery_app",
    "get_celery_app",
]
