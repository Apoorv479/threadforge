import pytest

from threadforge import (
    BackpressurePolicy,
    ThreadForge,
)
from threadforge.exceptions import QueueFullError
from threadforge.queue import TaskQueue
from threadforge.task import Task


def dummy_task():
    return "done"


def test_reject_policy():
    task_queue = TaskQueue(max_size=1)

    first = Task(fn=dummy_task)
    second = Task(fn=dummy_task)

    task_queue.put(
        first,
        policy=BackpressurePolicy.REJECT,
    )

    with pytest.raises(QueueFullError):
        task_queue.put(
            second,
            policy=BackpressurePolicy.REJECT,
        )


def test_priority_scheduling():
    task_queue = TaskQueue(max_size=10)

    low = Task(
        fn=dummy_task,
        priority=10,
    )

    high = Task(
        fn=dummy_task,
        priority=1,
    )

    task_queue.put(low)
    task_queue.put(high)

    first = task_queue.get()
    task_queue.task_done()

    second = task_queue.get()
    task_queue.task_done()

    assert first is high
    assert second is low


def test_engine_rejects_when_queue_is_full():
    engine = ThreadForge(
        workers=1,
        queue_size=1,
        backpressure=BackpressurePolicy.REJECT,
    )

    engine.start()

    engine.submit(
        dummy_task
    )

    try:
        with pytest.raises(QueueFullError):
            engine.submit(
                dummy_task
            )
    finally:
        engine.shutdown()
