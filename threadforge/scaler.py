import threading
import time

from .metrics import Metrics
from .queue import TaskQueue
from .worker import Worker


class AdaptiveWorkerPool:
    """
    Dynamically adjusts the number of worker threads
    according to queue pressure.
    """

    def __init__(
        self,
        task_queue: TaskQueue,
        stop_event: threading.Event,
        metrics: Metrics,
        min_workers: int,
        max_workers: int,
        scale_up_threshold: int = 10,
        scale_down_threshold: int = 2,
        check_interval: float = 1.0,
        cooldown: float = 2.0,
    ):
        if min_workers <= 0:
            raise ValueError(
                "min_workers must be greater than zero"
            )

        if max_workers < min_workers:
            raise ValueError(
                "max_workers must be >= min_workers"
            )

        self.task_queue = task_queue
        self.stop_event = stop_event
        self.metrics = metrics

        self.min_workers = min_workers
        self.max_workers = max_workers

        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold

        self.check_interval = check_interval
        self.cooldown = cooldown

        self._workers: list[Worker] = []
        self._lock = threading.Lock()

        self._last_scale_time = 0.0

        self._controller = threading.Thread(
            target=self._control_loop,
            name="threadforge-scaler",
            daemon=True,
        )

    def start(self) -> None:
        """Start the adaptive worker pool."""

        with self._lock:
            if self._workers:
                return

            for worker_id in range(self.min_workers):
                self._start_worker(worker_id)

        self._controller.start()

    def _start_worker(
        self,
        worker_id: int,
    ) -> None:
        """Create and start a new worker."""

        worker = Worker(
            task_queue=self.task_queue,
            stop_event=self.stop_event,
            worker_id=worker_id,
            metrics=self.metrics,
        )

        self._workers.append(worker)

        worker.start()

    def _control_loop(self) -> None:
        """Continuously evaluate worker pool pressure."""

        while not self.stop_event.is_set():

            time.sleep(self.check_interval)

            if self.stop_event.is_set():
                break

            self._evaluate()

    def _evaluate(self) -> None:
        """Decide whether the pool should scale."""

        queue_depth = self.task_queue.qsize()

        with self._lock:
            self._remove_stopped_workers()

            worker_count = len(self._workers)

        now = time.monotonic()

        if now - self._last_scale_time < self.cooldown:
            return

        if (
            queue_depth >= self.scale_up_threshold
            and worker_count < self.max_workers
        ):
            self._scale_up()

        elif (
            queue_depth <= self.scale_down_threshold
            and worker_count > self.min_workers
        ):
            self._scale_down()

    def _scale_up(self) -> None:
        """Add one worker."""

        with self._lock:

            if len(self._workers) >= self.max_workers:
                return

            worker_id = self._next_worker_id()

            self._start_worker(worker_id)

            self._last_scale_time = time.monotonic()

    def _scale_down(self) -> None:
        """Retire one worker."""

        with self._lock:

            if len(self._workers) <= self.min_workers:
                return

            worker = self._workers[-1]

            worker.stop()

            self._last_scale_time = time.monotonic()

    def _remove_stopped_workers(self) -> None:
        """Remove workers that have already terminated."""

        alive_workers = []

        for worker in self._workers:

            if worker.is_alive():
                alive_workers.append(worker)

        self._workers = alive_workers

    def _next_worker_id(self) -> int:
        """Generate the next worker ID."""

        existing_ids = {
            worker.worker_id
            for worker in self._workers
        }

        worker_id = 0

        while worker_id in existing_ids:
            worker_id += 1

        return worker_id

    @property
    def worker_count(self) -> int:
        """Return the current worker count."""

        with self._lock:
            self._remove_stopped_workers()

            return len(self._workers)

    def shutdown(self) -> None:
        """Stop all workers and the controller."""

        with self._lock:

            workers = list(self._workers)

        for worker in workers:
            worker.stop()

        for worker in workers:
            worker.join()

        self._controller.join()
