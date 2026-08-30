import time

from threadforge import ThreadForge


def test_worker_pool_starts_with_min_workers():
    engine = ThreadForge(
        workers=2,
        max_workers=4,
        queue_size=20,
    )

    engine.start()

    assert engine.worker_count == 2

    engine.shutdown()


def test_worker_pool_scales_up():
    engine = ThreadForge(
        workers=1,
        max_workers=3,
        queue_size=100,
    )

    engine.start()

    for _ in range(30):
        engine.submit(
            time.sleep,
            0.5,
        )

    deadline = time.monotonic() + 5

    while (
        engine.worker_count < 2
        and time.monotonic() < deadline
    ):
        time.sleep(0.1)

    assert engine.worker_count >= 2

    engine.wait()
    engine.shutdown()
