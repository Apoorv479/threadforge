import time

import pytest

from threadforge import EngineState, ThreadForge


def test_engine_starts_in_created_state():
    engine = ThreadForge()

    assert engine.state == EngineState.CREATED


def test_engine_enters_running_state():
    engine = ThreadForge(
        workers=2,
    )

    engine.start()

    assert engine.state == EngineState.RUNNING

    engine.shutdown()


def test_engine_enters_stopped_state():
    engine = ThreadForge(
        workers=2,
    )

    engine.start()
    engine.shutdown()

    assert engine.state == EngineState.STOPPED


def test_submit_after_shutdown_is_rejected():
    engine = ThreadForge(
        workers=1,
    )

    engine.start()
    engine.shutdown()

    with pytest.raises(RuntimeError):
        engine.submit(
            lambda: "hello"
        )


def test_shutdown_drains_queued_tasks():
    completed = []

    def task(value):
        time.sleep(0.05)
        completed.append(value)

    engine = ThreadForge(
        workers=2,
        queue_size=20,
    )

    engine.start()

    for value in range(10):
        engine.submit(
            task,
            value,
        )

    engine.shutdown()

    assert len(completed) == 10
    assert engine.state == EngineState.STOPPED


def test_shutdown_without_start():
    engine = ThreadForge()

    engine.shutdown()

    assert engine.state == EngineState.STOPPED


def test_double_shutdown_is_safe():
    engine = ThreadForge(
        workers=2,
    )

    engine.start()

    engine.shutdown()
    engine.shutdown()

    assert engine.state == EngineState.STOPPED
