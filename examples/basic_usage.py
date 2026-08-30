from threadforge.engine import ThreadForge


def process_job(job_id: int) -> str:
    return f"processed-{job_id}"


def main() -> None:
    engine = ThreadForge(
        workers=3,
        queue_size=10,
    )

    engine.start()

    tasks = []

    for job_id in range(8):
        task = engine.submit(
            process_job,
            job_id,
        )

        tasks.append(task)

    engine.wait()
    engine.shutdown()

    for task in tasks:
        print(
            f"{task.task_id} | "
            f"{task.state.value} | "
            f"{task.result}"
        )


if __name__ == "__main__":
    main()
