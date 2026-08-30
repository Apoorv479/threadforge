import queue

from .task import Task


class TaskQueue:
    """
    Thread-safe bounded priority queue for tasks.
    """

    def __init__(self, max_size: int = 100):
        if max_size <= 0:
            raise ValueError("max_size must be greater than zero")

        self._queue = queue.PriorityQueue(maxsize=max_size)

    def put(self, task: Task, block: bool = True, timeout: float | None = None) -> None:
        """
        Add a task to the queue.

        Lower priority numbers are executed first.
        """
        self._queue.put(
            (task.priority, task.task_id, task),
            block=block,
            timeout=timeout,
        )

    def get(self, block: bool = True, timeout: float | None = None) -> Task:
        """Remove and return the highest-priority task."""
        _, _, task = self._queue.get(
            block=block,
            timeout=timeout,
        )

        return task

    def task_done(self) -> None:
        """Mark a previously retrieved task as completed."""
        self._queue.task_done()

    def join(self) -> None:
        """Block until all queued tasks are processed."""
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
