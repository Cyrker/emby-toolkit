from __future__ import annotations

import heapq
import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import count
from typing import Any

logger = logging.getLogger(__name__)

_MAX_WORKERS = 8
_MAX_PENDING_TASKS = 256


class BackgroundCapacityError(RuntimeError):
    """Raised when compatibility work exceeds the bounded dispatcher capacity."""


class BackgroundHandle:
    """Compatibility handle for legacy ``greenlet.kill()/join()/ready()`` call sites."""

    def __init__(self, dispatcher: _BackgroundDispatcher, name: str):
        self._dispatcher = dispatcher
        self._name = name
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._state = "pending"

    def kill(self) -> None:
        self._dispatcher.cancel(self)

    cancel = kill

    def join(self, timeout: float | None = None) -> None:
        self._done.wait(timeout)

    def ready(self) -> bool:
        return self._done.is_set()

    def _try_start(self) -> bool:
        with self._lock:
            if self._state != "pending":
                return False
            self._state = "running"
            return True

    def _finish(self) -> None:
        with self._lock:
            self._state = "done"
            self._done.set()

    def _cancel_pending(self) -> bool:
        with self._lock:
            if self._state != "pending":
                return False
            self._state = "cancelled"
            self._done.set()
            return True


@dataclass(slots=True)
class _BackgroundTask:
    handle: BackgroundHandle
    function: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(order=True, slots=True)
class _ScheduledTask:
    deadline: float
    sequence: int
    task: _BackgroundTask = field(compare=False)


class _BackgroundDispatcher:
    """Bounded daemon worker pool with a single delayed-task scheduler."""

    def __init__(self) -> None:
        self._queue: queue.Queue[_BackgroundTask] = queue.Queue(maxsize=_MAX_PENDING_TASKS)
        self._scheduled: list[_ScheduledTask] = []
        self._sequence = count()
        self._schedule_condition = threading.Condition()
        self._active_lock = threading.Lock()
        self._active: set[BackgroundHandle] = set()
        self._accepting = True
        self._shutdown_started = False
        self._workers = tuple(
            threading.Thread(
                target=self._worker_loop,
                daemon=True,
                name=f"etk-compat-worker-{index + 1}",
            )
            for index in range(_MAX_WORKERS)
        )
        self._scheduler = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="etk-compat-scheduler",
        )
        for worker in self._workers:
            worker.start()
        self._scheduler.start()

    @property
    def shutdown_started(self) -> bool:
        return self._shutdown_started

    @property
    def stopped(self) -> bool:
        return not self._scheduler.is_alive() and all(not worker.is_alive() for worker in self._workers)

    def submit(
        self,
        function: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> BackgroundHandle:
        task = self._new_task(function, args, kwargs)
        try:
            self._queue.put_nowait(task)
        except queue.Full as exc:
            self.cancel(task.handle)
            raise BackgroundCapacityError("compatibility background queue is full") from exc
        return task.handle

    def schedule(
        self,
        delay: float,
        function: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> BackgroundHandle:
        task = self._new_task(function, args, kwargs)
        scheduled = _ScheduledTask(
            deadline=time.monotonic() + max(0.0, float(delay)),
            sequence=next(self._sequence),
            task=task,
        )
        with self._schedule_condition:
            if not self._accepting:
                self.cancel(task.handle)
                raise RuntimeError("compatibility background dispatcher is stopping")
            heapq.heappush(self._scheduled, scheduled)
            self._schedule_condition.notify()
        return task.handle

    def cancel(self, handle: BackgroundHandle) -> None:
        if handle._cancel_pending():
            self._discard(handle)
            with self._schedule_condition:
                self._scheduled = [item for item in self._scheduled if not item.task.handle.ready()]
                heapq.heapify(self._scheduled)
                self._schedule_condition.notify()

    def active_count(self) -> int:
        with self._active_lock:
            return len(self._active)

    def shutdown(self, timeout: float = 5.0) -> bool:
        if not self._shutdown_started:
            self._shutdown_started = True
            self._accepting = False
            with self._schedule_condition:
                self._schedule_condition.notify_all()
            with self._active_lock:
                active = tuple(self._active)
            for handle in active:
                self.cancel(handle)

        deadline = time.monotonic() + max(0.0, float(timeout))
        for thread in (self._scheduler, *self._workers):
            if thread is threading.current_thread():
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)
        return self.stopped and self.active_count() == 0

    def _new_task(
        self,
        function: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> _BackgroundTask:
        if not self._accepting:
            raise RuntimeError("compatibility background dispatcher is stopping")
        name = getattr(function, "__name__", "task")
        handle = BackgroundHandle(self, name)
        with self._active_lock:
            if not self._accepting:
                raise RuntimeError("compatibility background dispatcher is stopping")
            if len(self._active) >= _MAX_PENDING_TASKS:
                raise BackgroundCapacityError("compatibility background capacity is exhausted")
            self._active.add(handle)
        return _BackgroundTask(handle=handle, function=function, args=args, kwargs=kwargs)

    def _worker_loop(self) -> None:
        while True:
            try:
                task = self._queue.get(timeout=0.1)
            except queue.Empty:
                if self._shutdown_started:
                    return
                continue
            try:
                if not task.handle._try_start():
                    continue
                try:
                    task.function(*task.args, **task.kwargs)
                except Exception:
                    logger.exception("Detached compatibility task failed: %s", task.handle._name)
                finally:
                    task.handle._finish()
                    self._discard(task.handle)
            finally:
                self._queue.task_done()

    def _scheduler_loop(self) -> None:
        while True:
            with self._schedule_condition:
                while True:
                    if self._shutdown_started:
                        return
                    while self._scheduled and self._scheduled[0].task.handle.ready():
                        heapq.heappop(self._scheduled)
                    if not self._scheduled:
                        self._schedule_condition.wait()
                        continue
                    remaining = self._scheduled[0].deadline - time.monotonic()
                    if remaining > 0:
                        self._schedule_condition.wait(remaining)
                        continue
                    scheduled = heapq.heappop(self._scheduled)
                    break
            if scheduled.task.handle.ready():
                continue
            try:
                self._queue.put_nowait(scheduled.task)
            except queue.Full:
                logger.error("Delayed compatibility task rejected: background queue is full")
                self.cancel(scheduled.task.handle)

    def _discard(self, handle: BackgroundHandle) -> None:
        with self._active_lock:
            self._active.discard(handle)


_DISPATCHER_LOCK = threading.RLock()
_DISPATCHER: _BackgroundDispatcher | None = None
_BACKGROUND_ACCEPTING = True


def _get_dispatcher() -> _BackgroundDispatcher:
    global _DISPATCHER
    with _DISPATCHER_LOCK:
        if not _BACKGROUND_ACCEPTING:
            raise RuntimeError("compatibility background runtime is stopping")
        if _DISPATCHER is None:
            _DISPATCHER = _BackgroundDispatcher()
        return _DISPATCHER


def spawn(function: Callable[..., Any], *args: Any, **kwargs: Any) -> BackgroundHandle:
    """Submit immediate detached work to the bounded compatibility dispatcher."""
    return _get_dispatcher().submit(function, args, kwargs)


def spawn_later(
    delay: float,
    function: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> BackgroundHandle:
    """Schedule delayed work without creating one operating-system thread per timer."""
    return _get_dispatcher().schedule(delay, function, args, kwargs)


def resume_background_tasks() -> None:
    """Allow a freshly starting runtime to create a new dispatcher on first use."""
    global _BACKGROUND_ACCEPTING, _DISPATCHER
    with _DISPATCHER_LOCK:
        if _DISPATCHER is not None and _DISPATCHER.shutdown_started:
            if not _DISPATCHER.stopped:
                raise RuntimeError("previous compatibility background dispatcher is still stopping")
            _DISPATCHER = None
        _BACKGROUND_ACCEPTING = True


def stop_background_tasks(timeout: float = 5.0) -> bool:
    """Cancel pending work, wait for running work, and stop all dispatcher threads."""
    global _BACKGROUND_ACCEPTING, _DISPATCHER
    with _DISPATCHER_LOCK:
        _BACKGROUND_ACCEPTING = False
        dispatcher = _DISPATCHER
    if dispatcher is None:
        return True
    stopped = dispatcher.shutdown(timeout)
    if stopped:
        with _DISPATCHER_LOCK:
            if _DISPATCHER is dispatcher:
                _DISPATCHER = None
    return stopped


def active_background_task_count() -> int:
    with _DISPATCHER_LOCK:
        dispatcher = _DISPATCHER
    return dispatcher.active_count() if dispatcher is not None else 0
