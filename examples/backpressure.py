from threadforge import (
    BackpressurePolicy,
    ThreadForge,
)


def process(value: int) -> int:
    return value * 2


def main() -> None:
    engine = ThreadForge(
        workers=2,
        queue_size=5,
        backpressure=BackpressurePolicy.REJECT,
    )

    engine.start()

    tasks = []

    for value in range(20):
        try:
            task = engine.submit(process, value)
            tasks.append(task)

        except Exception as exc:
            print(
                f"Rejected task {value}: {exc}"
            )

    engine.wait()
    engine.shutdown()

    for task in tasks:
        print(
            task.task_id,
            task.result(),
        )


if __name__ == "__main__":
    main()
