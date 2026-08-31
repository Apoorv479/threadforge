"""ThreadForge - lightweight concurrent job execution engine."""

from .engine import EngineState, ThreadForge
from .metrics import Metrics, MetricsSnapshot
from .queue import BackpressurePolicy
from .scaler import AdaptiveWorkerPool
from .task import Task, TaskState

__version__ = "0.1.0"

__all__ = [
    "ThreadForge",
    "EngineState",
    "Task",
    "TaskState",
    "BackpressurePolicy",
    "Metrics",
    "MetricsSnapshot",
    "AdaptiveWorkerPool",
]
