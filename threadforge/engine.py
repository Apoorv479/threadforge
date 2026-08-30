import threading
from typing import Any, Callable

from .metrics import Metrics
from .queue import BackpressurePolicy, TaskQueue
from .task import Task
from .worker import Worker


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
    ):
        if workers <= 0:
            raise ValueError(
                "workers must be greater than zero"
            )

        self.task_queue = TaskQueue(
            max_size=queue_size
        )

        self.backpressure = backpressure

        self.metrics = Metrics()

        self.stop_event = threading.Event()

        self.workers = [
            Worker(
                task_queue=self.task_queue,
                stop_event=self.stop_event,
                worker_id=i,
                metrics=self.metrics,
            )
            for i in range(workers)
        ]

        self._started = False

    def start(self) -> None:
        """Start all worker threads."""

        if self._started:
            return

        self._started = True

        for worker in self.workers:
            worker.start()

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
        """Stop all worker threads."""

        if not self._started:
            return

        self.stop_event.set()

        for worker in self.workers:
            worker.join()

        self._started = False

    def stats(self):
        """Return a snapshot of engine metrics."""

        return self.metrics.snapshot()
