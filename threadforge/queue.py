import queue
from enum import Enum

from .exceptions import QueueFullError
from .task import Task


class BackpressurePolicy(str, Enum):
    """Behavior when the task queue reaches capacity."""

    BLOCK = "block"
    REJECT = "reject"
    DROP_LOW_PRIORITY = "drop_low_priority"


class TaskQueue:
    """
    Thread-safe bounded priority queue.

    Lower priority numbers represent higher priority.
    """

    def __init__(self, max_size: int = 100):
        if max_size <= 0:
            raise ValueError(
                "max_size must be greater than zero"
            )

        self.max_size = max_size

        self._queue = queue.PriorityQueue(
            maxsize=max_size
        )

    def put(
        self,
        task: Task,
        policy: BackpressurePolicy = BackpressurePolicy.BLOCK,
        timeout: float | None = None,
    ) -> None:
        """
        Add a task according to the selected backpressure policy.
        """

        if policy == BackpressurePolicy.BLOCK:
            self._queue.put(
                (task.priority, task.task_id, task),
                block=True,
                timeout=timeout,
            )
            return

        if policy == BackpressurePolicy.REJECT:
            try:
                self._queue.put_nowait(
                    (
                        task.priority,
                        task.task_id,
                        task,
                    )
                )
            except queue.Full as exc:
                raise QueueFullError(
                    "Task queue is full"
                ) from exc

            return

        if policy == BackpressurePolicy.DROP_LOW_PRIORITY:
            self._put_with_priority_drop(task)
            return

        raise ValueError(
            f"Unsupported backpressure policy: {policy}"
        )

    def _put_with_priority_drop(
        self,
        task: Task,
    ) -> None:
        """
        Insert a task by dropping the lowest-priority
        queued task when necessary.
        """

        try:
            self._queue.put_nowait(
                (
                    task.priority,
                    task.task_id,
                    task,
                )
            )
            return

        except queue.Full:
            pass

        items = []

        dropped = None

        while True:
            try:
                item = self._queue.get_nowait()
                items.append(item)
            except queue.Empty:
                break

        if items:
            lowest = max(
                items,
                key=lambda item: (
                    item[0],
                    item[1],
                ),
            )

            if task.priority < lowest[0]:
                items.remove(lowest)
                dropped = lowest[2]

        if dropped is None:
            for item in items:
                self._queue.put_nowait(item)

            raise QueueFullError(
                "Task queue is full and incoming task "
                "does not have sufficient priority"
            )

        for item in items:
            self._queue.put_nowait(item)

        self._queue.put_nowait(
            (
                task.priority,
                task.task_id,
                task,
            )
        )

    def get(
        self,
        block: bool = True,
        timeout: float | None = None,
    ) -> Task:
        """Remove and return the highest-priority task."""

        _, _, task = self._queue.get(
            block=block,
            timeout=timeout,
        )

        return task

    def task_done(self) -> None:
        """Mark a retrieved task as processed."""

        self._queue.task_done()

    def join(self) -> None:
        """Wait until all queued tasks are processed."""

        self._queue.join()

    def qsize(self) -> int:
        """Return the approximate queue size."""

        return self._queue.qsize()

    def full(self) -> bool:
        """Return whether the queue is full."""

        return self._queue.full()

    def empty(self) -> bool:
        """Return whether the queue is empty."""

        return self._queue.empty()
