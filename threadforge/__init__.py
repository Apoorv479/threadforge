"""ThreadForge - lightweight concurrent job execution engine."""

from .engine import ThreadForge
from .queue import BackpressurePolicy
from .task import Task, TaskState

__version__ = "0.1.0"

__all__ = [
    "ThreadForge",
    "Task",
    "TaskState",
    "BackpressurePolicy",
]
