import threading
import time

from .metrics import Metrics
from .queue import TaskQueue
from .task import Task


class Worker(threading.Thread):
    """
    Worker thread that continuously consumes tasks.
    """

    def __init__(
        self,
        task_queue: TaskQueue,
        stop_event: threading.Event,
        worker_id: int,
        metrics: Metrics,
    ):
        super().__init__(
            name=f"threadforge-worker-{worker_id}"
        )

        self.task_queue = task_queue
        self.stop_event = stop_event
        self.worker_id = worker_id
        self.metrics = metrics

        self.completed_tasks = 0
        self.failed_tasks = 0
        self.retried_tasks = 0

    def run(self) -> None:
        """Worker execution loop."""

        while not self.stop_event.is_set():

            try:
                task = self.task_queue.get(
                    block=True,
                    timeout=0.5,
                )

            except Exception:
                continue

            try:
                self._execute(task)

            finally:
                self.task_queue.task_done()

    def _execute(self, task: Task) -> None:
        """Execute task and update metrics."""

        task.mark_running()

        self.metrics.record_start()

        start_time = time.perf_counter()

        try:
            result = task.execute()

            execution_time = (
                time.perf_counter() - start_time
            )

            task.mark_completed(result)

            self.metrics.record_completion(
                execution_time
            )

            self.completed_tasks += 1

        except BaseException as exc:

            execution_time = (
                time.perf_counter() - start_time
            )

            if task.can_retry():

                task.prepare_retry()

                self.metrics.record_retry()

                self.retried_tasks += 1

                self.task_queue.put(task)

                # This execution is no longer active.
                self.metrics.record_failure(
                    execution_time
                )

            else:

                task.mark_failed(exc)

                self.metrics.record_failure(
                    execution_time
                )

                self.failed_tasks += 1
