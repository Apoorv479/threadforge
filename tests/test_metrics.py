from threadforge import ThreadForge


def test_metrics_count_completed_tasks():
    engine = ThreadForge(
        workers=2,
        queue_size=10,
    )

    engine.start()

    tasks = [
        engine.submit(
            lambda value=value: value + 1
        )
        for value in range(5)
    ]

    engine.wait()

    stats = engine.stats()

    engine.shutdown()

    assert stats.submitted == 5
    assert stats.completed == 5
    assert stats.failed == 0
    assert stats.running == 0


def test_metrics_count_retries():
    attempts = 0

    def unstable():
        nonlocal attempts

        attempts += 1

        if attempts < 3:
            raise RuntimeError("temporary")

        return "success"

    engine = ThreadForge(
        workers=1,
        queue_size=10,
    )

    engine.start()

    task = engine.submit(
        unstable,
        max_retries=3,
    )

    engine.wait()

    stats = engine.stats()

    engine.shutdown()

    assert task.result() == "success"

    assert stats.submitted == 1
    assert stats.completed == 1
    assert stats.failed == 0
    assert stats.retried == 2
    assert stats.running == 0


def test_metrics_count_permanent_failure():
    def failing():
        raise RuntimeError("failure")

    engine = ThreadForge(
        workers=1,
        queue_size=10,
    )

    engine.start()

    task = engine.submit(
        failing,
        max_retries=2,
    )

    engine.wait()

    stats = engine.stats()

    engine.shutdown()

    assert stats.submitted == 1
    assert stats.completed == 0
    assert stats.failed == 1
    assert stats.retried == 2
    assert stats.running == 0
