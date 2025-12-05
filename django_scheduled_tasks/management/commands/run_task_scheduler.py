import logging
import math
import os
import signal
import sys
from argparse import ArgumentParser, ArgumentTypeError, BooleanOptionalAction
from datetime import timedelta
from types import FrameType

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.utils.autoreload import DJANGO_AUTORELOAD_ENV, run_with_reloader

from django_scheduled_tasks.base import scheduler

package_logger = logging.getLogger("django_scheduled_tasks")
logger = logging.getLogger("django_scheduled_tasks.run_task_scheduler")


class Scheduler:
    def __init__(self, *, interval: float):
        self.interval = timedelta(seconds=interval)
        self.running = True
        self.shutdown_event = None

    def shutdown(self, signum: int, frame: FrameType | None) -> None:
        if not self.running:
            logger.warning("Received %s - forcing shutdown.", signal.strsignal(signum))
            self.reset_signals()
            sys.exit(1)

        logger.warning(
            "Received %s - shutting down gracefully... (press Ctrl+C again to force)",
            signal.strsignal(signum),
        )
        self.running = False
        if self.shutdown_event:
            self.shutdown_event.set()

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.shutdown)

    def reset_signals(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, signal.SIG_DFL)

    def run(self) -> None:
        import threading

        logger.info(
            "Starting scheduler with %d registered schedules, interval=%.1fs",
            len(scheduler.schedules),
            self.interval.total_seconds(),
        )

        self.shutdown_event = threading.Event()
        scheduler.run_scheduling_loop(self.shutdown_event, self.interval)

        close_old_connections()
        logger.info("Scheduler stopped.")


def valid_interval(val: str) -> float:
    num = float(val)
    if not math.isfinite(num):
        raise ArgumentTypeError("Must be a finite floating point value")
    if num <= 0:
        raise ArgumentTypeError("Must be greater than zero")
    return num


class Command(BaseCommand):
    help = "Run a task scheduler that enqueues scheduled tasks based on their schedule."

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--interval",
            nargs="?",
            default=1,
            type=valid_interval,
            help="The interval (in seconds) between checking for tasks to schedule (default: %(default)r)",
        )
        parser.add_argument(
            "--reload",
            action=BooleanOptionalAction,
            default=settings.DEBUG,
            help="Reload the scheduler on code changes. (default: DEBUG)",
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            package_logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            package_logger.setLevel(logging.INFO)
        else:
            package_logger.setLevel(logging.DEBUG)

        if not package_logger.hasHandlers():
            package_logger.addHandler(logging.StreamHandler(self.stdout))

    def handle(
        self,
        *,
        verbosity: int,
        interval: float,
        reload: bool,
        **options: dict,
    ) -> None:
        self.configure_logging(verbosity)

        sched = Scheduler(interval=interval)

        if reload:
            if os.environ.get(DJANGO_AUTORELOAD_ENV) == "true":
                sched.configure_signals()

            run_with_reloader(sched.run)
        else:
            sched.configure_signals()
            sched.run()
