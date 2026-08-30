from dataclasses import dataclass, field
from enum import Enum
import threading
from typing import Any, Callable, Optional
from uuid import uuid4


class TaskState(str, Enum):
    """Lifecycle states of a task."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Task:
    """
    Represents a unit of work submitted to ThreadForge.
    """

    fn: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)

    priority: int = 0
    task_id: str = field(
        default_factory=lambda: str(uuid4())
    )

    max_retries: int = 0
    retry_count: int = 0

    state: TaskState = TaskState.QUEUED

    _result: Optional[Any] = None
    _error: Optional[BaseException] = None

    _completion_event: threading.Event = field(
        default_factory=threading.Event,
        init=False,
        repr=False,
    )

    _state_lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )

    def execute(self) -> Any:
        """Execute the underlying callable."""
        return self.fn(*self.args, **self.kwargs)

    def can_retry(self) -> bool:
        """Return True if the task can be retried."""
        return self.retry_count < self.max_retries

    def mark_running(self) -> None:
        """Transition task to RUNNING."""
        with self._state_lock:
            self.state = TaskState.RUNNING

    def mark_completed(self, result: Any) -> None:
        """Transition task to COMPLETED and store result."""

        with self._state_lock:
            self._result = result
            self.state = TaskState.COMPLETED

        self._completion_event.set()

    def mark_failed(self, error: BaseException) -> None:
        """Transition task to FAILED and store error."""

        with self._state_lock:
            self._error = error
            self.state = TaskState.FAILED

        self._completion_event.set()

    def prepare_retry(self) -> None:
        """Prepare the task for another execution attempt."""

        with self._state_lock:
            self.retry_count += 1
            self.state = TaskState.QUEUED
            self._error = None

    def result(self, timeout: Optional[float] = None) -> Any:
        """
        Wait for task completion and return its result.

        Raises:
            TimeoutError: if the task does not finish in time.
            BaseException: the original task exception if execution failed.
        """

        completed = self._completion_event.wait(
            timeout=timeout
        )

        if not completed:
            raise TimeoutError(
                f"Task {self.task_id} did not complete "
                f"within the given timeout"
            )

        with self._state_lock:
            if self.state == TaskState.FAILED:
                if self._error is not None:
                    raise self._error

            return self._result

    @property
    def error(self) -> Optional[BaseException]:
        """Return the task error, if any."""

        with self._state_lock:
            return self._error
