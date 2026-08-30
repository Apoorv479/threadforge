import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricsSnapshot:
    """Immutable snapshot of engine metrics."""

    submitted: int
    completed: int
    failed: int
    retried: int
    running: int
    total_execution_time: float
    average_execution_time: float


class Metrics:
    """
    Thread-safe metrics collector for ThreadForge.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._retried = 0
        self._running = 0

        self._total_execution_time = 0.0

    def record_submission(self) -> None:
        """Record a newly submitted task."""

        with self._lock:
            self._submitted += 1

    def record_start(self) -> None:
        """Record the start of task execution."""

        with self._lock:
            self._running += 1

    def record_completion(
        self,
        execution_time: float,
    ) -> None:
        """Record a successful task execution."""

        with self._lock:
            self._running -= 1
            self._completed += 1
            self._total_execution_time += execution_time

    def record_failure(
        self,
        execution_time: float,
    ) -> None:
        """Record a permanently failed task."""

        with self._lock:
            self._running -= 1
            self._failed += 1
            self._total_execution_time += execution_time

    def record_retry_failure(
        self,
        execution_time: float,
    ) -> None:
        """Record an execution attempt that will be retried."""

        with self._lock:
            self._running -= 1
            self._total_execution_time += execution_time

    def record_retry(self) -> None:
        """Record a task retry."""

        with self._lock:
            self._retried += 1

    def snapshot(self) -> MetricsSnapshot:
        """Return a consistent metrics snapshot."""

        with self._lock:
            if self._completed + self._failed > 0:
                average = (
                    self._total_execution_time
                    / (self._completed + self._failed)
                )
            else:
                average = 0.0

            return MetricsSnapshot(
                submitted=self._submitted,
                completed=self._completed,
                failed=self._failed,
                retried=self._retried,
                running=self._running,
                total_execution_time=self._total_execution_time,
                average_execution_time=average,
            )
