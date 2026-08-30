import time

from threadforge import ThreadForge


def process(value: int) -> int:
    time.sleep(0.1)
    return value * 2


def main() -> None:
    engine = ThreadForge(
        workers=4,
        queue_size=20,
    )

    engine.start()

    tasks = [
        engine.submit(process, i)
        for i in range(20)
    ]

    engine.wait()

    stats = engine.stats()

    engine.shutdown()

    print("ThreadForge Metrics")
    print("-------------------")

    print(
        f"Submitted: "
        f"{stats.submitted}"
    )

    print(
        f"Completed: "
        f"{stats.completed}"
    )

    print(
        f"Failed: "
        f"{stats.failed}"
    )

    print(
        f"Retried: "
        f"{stats.retried}"
    )

    print(
        f"Running: "
        f"{stats.running}"
    )

    print(
        f"Total execution time: "
        f"{stats.total_execution_time:.4f}s"
    )

    print(
        f"Average execution time: "
        f"{stats.average_execution_time:.4f}s"
    )


if __name__ == "__main__":
    main()
