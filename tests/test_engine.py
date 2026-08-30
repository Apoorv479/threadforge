import time

from threadforge.engine import ThreadForge


def test_tasks_are_executed():
    engine = ThreadForge(
        workers=2,
        queue_size=10,
    )

    engine.start()

    tasks = [
        engine.submit(lambda value=value: value * 2)
        for value in range(5)
    ]

    engine.wait()
    engine.shutdown()

    results = [task.result for task in tasks]

    assert results == [0, 2, 4, 6, 8]


def test_tasks_execute_concurrently():
    engine = ThreadForge(
        workers=4,
        queue_size=10,
    )

    engine.start()

    start = time.perf_counter()

    tasks = [
        engine.submit(time.sleep, 1)
        for _ in range(4)
    ]

    engine.wait()
    engine.shutdown()

    elapsed = time.perf_counter() - start

    assert elapsed < 2
