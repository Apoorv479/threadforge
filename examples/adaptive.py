import time

from threadforge import ThreadForge


def process(value: int) -> int:
    time.sleep(0.2)
    return value * 2


def main() -> None:
    engine = ThreadForge(
        workers=2,
        max_workers=6,
        queue_size=100,
        
    )

    engine.start()

    tasks = []

    for value in range(50):
        task = engine.submit(process, value)
        tasks.append(task)

    while engine.worker_count < 6:
        print(
            f"workers={engine.worker_count}, "
            f"queue={engine.task_queue.qsize()}"
        )

        time.sleep(0.5)

    engine.wait()

    stats = engine.stats()

    print()
    print("Final metrics")
    print("-------------")
    print(f"submitted: {stats.submitted}")
    print(f"completed: {stats.completed}")
    print(f"failed: {stats.failed}")
    print(f"retried: {stats.retried}")

    engine.shutdown()


if __name__ == "__main__":
    main()
