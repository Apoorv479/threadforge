import threading
from enum import Enum
from typing import Any, Callable

from .metrics import Metrics
from .queue import BackpressurePolicy, TaskQueue
from .scaler import AdaptiveWorkerPool
from .task import Task


class EngineState(str, Enum):
    """Lifecycle states of the ThreadForge engine."""

    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPING = "stopping"
    STOPPED = "stopped"


class ThreadForge:
    """
    Concurrent job execution engine powered by Python threads.
    """

    def __init__(
        self,
        workers: int = 4,
        queue_size: int = 100,
        backpressure: BackpressurePolicy = (
            BackpressurePolicy.BLOCK
        ),
        max_workers: int | None = None,
    ):
        if workers <= 0:
            raise ValueError(
                "workers must be greater than zero"
            )

        if max_workers is None:
            max_workers = workers

        if max_workers < workers:
            raise ValueError(
                "max_workers must be >= workers"
            )

        self.task_queue = TaskQueue(
            max_size=queue_size
        )

        self.backpressure = backpressure

        self.metrics = Metrics()

        self.stop_event = threading.Event()

        self._workers = workers
        self._max_workers = max_workers

        self.worker_pool: AdaptiveWorkerPool | None = None

        self._state = EngineState.CREATED

        self._state_lock = threading.Lock()

    def start(self) -> None:
        """Start the execution engine."""

        with self._state_lock:

            if self._state == EngineState.RUNNING:
                return

            if self._state != EngineState.CREATED:
                raise RuntimeError(
                    "ThreadForge can only be started "
                    "from CREATED state"
                )

            self.worker_pool = AdaptiveWorkerPool(
                task_queue=self.task_queue,
                stop_event=self.stop_event,
                metrics=self.metrics,
                min_workers=self._workers,
                max_workers=self._max_workers,
            )

            self.worker_pool.start()

            self._state = EngineState.RUNNING

    def submit(
        self,
        fn: Callable[..., Any],
        *args: Any,
        priority: int = 0,
        max_retries: int = 0,
        **kwargs: Any,
    ) -> Task:
        """
        Submit a task to the execution engine.
        """

        with self._state_lock:

            if self._state != EngineState.RUNNING:
                raise RuntimeError(
                    "Cannot submit tasks when engine is "
                    f"{self._state.value}"
                )

        if max_retries < 0:
            raise ValueError(
                "max_retries cannot be negative"
            )

        task = Task(
            fn=fn,
            args=args,
            kwargs=kwargs,
            priority=priority,
            max_retries=max_retries,
        )

        self.task_queue.put(
            task,
            policy=self.backpressure,
        )

        self.metrics.record_submission()

        return task

    def wait(self) -> None:
        """Wait until all queued tasks are processed."""

        self.task_queue.join()

    def shutdown(self) -> None:
        """
        Gracefully shut down the engine.

        New tasks are rejected while existing queued and
        running tasks are allowed to complete.
        """

        with self._state_lock:

            if self._state == EngineState.STOPPED:
                return

            if self._state == EngineState.CREATED:
                self._state = EngineState.STOPPED
                return

            if self._state != EngineState.RUNNING:
                return

            self._state = EngineState.DRAINING

        # Stop accepting new work, but allow workers
        # to finish existing work.
        self.task_queue.join()

        with self._state_lock:
            self._state = EngineState.STOPPING

        self.stop_event.set()

        if self.worker_pool is not None:
            self.worker_pool.shutdown()

        with self._state_lock:
            self._state = EngineState.STOPPED

    def stats(self):
        """Return a snapshot of engine metrics."""

        return self.metrics.snapshot()

    @property
    def state(self) -> EngineState:
        """Return the current engine state."""

        with self._state_lock:
            return self._state

    @property
    def worker_count(self) -> int:
        """Return the current worker count."""

        if self.worker_pool is None:
            return 0

        return self.worker_pool.worker_count
