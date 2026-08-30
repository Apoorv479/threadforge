
import threading
from typing import Any, Callable

from .metrics import Metrics
from .queue import BackpressurePolicy, TaskQueue
from .scaler import AdaptiveWorkerPool
from .task import Task


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

        self._started = False

    def start(self) -> None:
        """Start the execution engine."""

        if self._started:
            return

        self._started = True

        self.worker_pool = AdaptiveWorkerPool(
            task_queue=self.task_queue,
            stop_event=self.stop_event,
            metrics=self.metrics,
            min_workers=self._workers,
            max_workers=self._max_workers,
        )

        self.worker_pool.start()

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

        if not self._started:
            raise RuntimeError(
                "ThreadForge must be started before "
                "submitting tasks"
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
        """Stop the execution engine."""

        if not self._started:
            return

        self.stop_event.set()

        if self.worker_pool is not None:
            self.worker_pool.shutdown()

        self._started = False

    def stats(self):
        """Return a snapshot of engine metrics."""

        return self.metrics.snapshot()

    @property
    def worker_count(self) -> int:
        """Return the current worker count."""

        if self.worker_pool is None:
            return 0

        return self.worker_pool.worker_count
