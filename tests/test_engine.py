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
        task.result
        for task in tasks
    ]

    assert results == [
        0,
        2,
        4,
        6,
        8,
    ]

    assert all(
        task.state == TaskState.COMPLETED
        for task in tasks
    )


def test_failed_task_has_failed_state():
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
    assert isinstance(
        task.error,
        ValueError,
    )


def test_task_starts_as_queued():
    engine = ThreadForge(
        workers=1,
        queue_size=10,
    )

    engine.start()

    task = engine.submit(
        lambda: "hello"
    )

    engine.wait()
    engine.shutdown()

    assert task.state == TaskState.COMPLETED
