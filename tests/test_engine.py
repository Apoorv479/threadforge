import pytest

from threadforge.engine import ThreadForge
from threadforge.task import TaskState


def test_tasks_are_executed():
    engine = ThreadForge(
        workers=2,
        queue_size=10,
    )

    engine.start()

    tasks = [
        engine.submit(
            lambda value=value: value * 2
        )
        for value in range(5)
    ]

    engine.wait()
    engine.shutdown()

    results = [
        task.result()
        for task in tasks
    ]

    assert results == [
        0,
        2,
        4,
        6,
        8,
    ]


def test_failed_task_raises_original_exception():
    def failing_task():
        raise ValueError("something went wrong")

    engine = ThreadForge(
        workers=1,
        queue_size=10,
    )

    engine.start()

    task = engine.submit(failing_task)

    engine.wait()
    engine.shutdown()

    assert task.state == TaskState.FAILED

    with pytest.raises(
        ValueError,
        match="something went wrong",
    ):
        task.result()


def test_task_retry():
    attempts = 0

    def unstable_task():
        nonlocal attempts

        attempts += 1

        if attempts < 3:
            raise ValueError("temporary failure")

        return "success"

    engine = ThreadForge(
        workers=1,
        queue_size=10,
    )

    engine.start()

    task = engine.submit(
        unstable_task,
        max_retries=3,
    )

    engine.wait()
    engine.shutdown()

    assert task.result() == "success"

    assert attempts == 3
    assert task.retry_count == 2
    assert task.state == TaskState.COMPLETED


def test_retry_limit():
    attempts = 0

    def permanently_failing_task():
        nonlocal attempts

        attempts += 1

        raise RuntimeError("permanent failure")

    engine = ThreadForge(
        workers=1,
        queue_size=10,
    )

    engine.start()

    task = engine.submit(
        permanently_failing_task,
        max_retries=2,
    )

    engine.wait()
    engine.shutdown()

    assert attempts == 3
    assert task.retry_count == 2
    assert task.state == TaskState.FAILED

    with pytest.raises(
        RuntimeError,
        match="permanent failure",
    ):
        task.result()
