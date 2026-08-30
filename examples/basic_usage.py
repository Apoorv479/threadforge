import time

from threadforge.engine import ThreadForge


def process_job(job_id: int) -> str:
    print(f"Processing job {job_id}")

    time.sleep(1)

    print(f"Completed job {job_id}")

    return f"result-{job_id}"


def main() -> None:
    engine = ThreadForge(
        workers=3,
        queue_size=10,
    )

    engine.start()

    tasks = []

    for i in range(8):
        task = engine.submit(
            process_job,
            i,
        )

        tasks.append(task)

    engine.wait()
    engine.shutdown()

    for task in tasks:
        print(
            task.task_id,
            task.result,
        )


if __name__ == "__main__":
    main()
