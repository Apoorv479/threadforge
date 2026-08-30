class ThreadForgeError(Exception):
    """Base exception for ThreadForge."""


class QueueFullError(ThreadForgeError):
    """Raised when a task cannot be accepted by the queue."""
