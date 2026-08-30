from dataclasses import dataclass, field
from enum import Enum
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
    task_id: str = field(default_factory=lambda: str(uuid4()))

    max_retries: int = 0
    retry_count: int = 0

    state: TaskState = TaskState.QUEUED

    result: Optional[Any] = None
    error: Optional[BaseException] = None

    def execute(self) -> Any:
        """Execute the underlying callable."""
        return self.fn(*self.args, **self.kwargs)

    def can_retry(self) -> bool:
        """Return True if the task can be retried."""
        return self.retry_count < self.max_retries
