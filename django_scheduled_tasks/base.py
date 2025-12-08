from __future__ import annotations
import abc
import datetime
import hashlib
import logging
import threading
import time
from datetime import timedelta
from typing import Annotated, Callable, TYPE_CHECKING, Any

from dateutil import tz
from django.db import DatabaseError, connections
from django.tasks import Task, TaskResult
from django.utils import timezone
from pydantic import BaseModel, ConfigDict, PlainSerializer, PlainValidator, TypeAdapter
from pydantic_extra_types.cron import CronStr

if TYPE_CHECKING:
    from .models import ScheduledTaskRunLog

type Json = dict[str, Json] | list[Json] | str | int | float | bool | None

logger = logging.getLogger(__name__)

cron_validator = TypeAdapter(CronStr)


def _task_to_import_string(task: Task) -> str:
    """Convert a Task object to its import string representation."""
    return f"{task.func.__module__}.{task.func.__qualname__}"


def _validate_task(value: Any) -> Task:
    if hasattr(value, "func") and callable(value.func):
        return value
    raise ValueError("Expected a Task instance")


TaskField = Annotated[
    Task,
    PlainValidator(_validate_task),
    PlainSerializer(_task_to_import_string, return_type=str),
]


class TaskSchedule(BaseModel, abc.ABC):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    task_args: tuple[Json, ...] = ()
    task_kwargs: dict[str, Json] = {}
    task: TaskField

    @abc.abstractmethod
    def get_next_scheduled_time(
        self,
        previous_scheduled: datetime.datetime | None,
        now: datetime.datetime,
    ) -> datetime.datetime:
        """
        Calculate the next scheduled run time for this task.

        Args:
            previous_scheduled: the previously scheduled run time, or None if first run.
            now: the current time, timezone-aware.

        Returns: the next time the task should (have) run.
        """
        ...

    def to_sha_bytes(self) -> bytes:
        return hashlib.sha256(
            self.model_dump_json(round_trip=False).encode("utf-8")
        ).digest()

    def __hash__(self) -> int:
        return hash(self.to_sha_bytes())


class PeriodicSchedule(TaskSchedule):
    period: timedelta

    def get_next_scheduled_time(
        self,
        previous_scheduled: datetime.datetime | None,
        now: datetime.datetime,
    ) -> datetime.datetime:
        if previous_scheduled is None:
            return now

        next_time = previous_scheduled + self.period
        while next_time <= now:
            next_time += self.period
        return next_time


class CrontabSchedule(TaskSchedule):
    cron_schedule: CronStr
    timezone_str: str | None = None

    def _to_target_tz(self, dt: datetime.datetime) -> datetime.datetime:
        """Convert datetime to the target timezone for cron calculation."""
        if self.timezone_str:
            return dt.astimezone(tz.gettz(self.timezone_str))
        return dt

    def get_next_scheduled_time(
        self,
        previous_scheduled: datetime.datetime | None,
        now: datetime.datetime,
    ) -> datetime.datetime:
        if previous_scheduled is None:
            return self.cron_schedule.next_after(start_date=self._to_target_tz(now))

        next_time = self.cron_schedule.next_after(
            start_date=self._to_target_tz(previous_scheduled)
        )
        if next_time <= now:
            next_time = self.cron_schedule.next_after(
                start_date=self._to_target_tz(now)
            )
        return next_time


def get_run_logs(
    task_schedules: set[TaskSchedule],
) -> dict[TaskSchedule, "ScheduledTaskRunLog | None"]:
    task_hash_map: dict[bytes, TaskSchedule] = {
        task.to_sha_bytes(): task for task in task_schedules
    }
    from .models import ScheduledTaskRunLog

    run_logs = ScheduledTaskRunLog.objects.filter(task_hash__in=task_hash_map.keys())
    run_log_map = {task_hash_map[run.task_hash]: run for run in run_logs}
    return {schedule: run_log_map.get(schedule) for schedule in task_schedules}


class TaskScheduler:
    def __init__(self):
        self.schedules: set[TaskSchedule] = set()

    def add_scheduled_task(self, schedule: TaskSchedule):
        self.schedules.add(schedule)

    def run_scheduling_loop(
        self,
        shutdown_event: threading.Event,
        interval: timedelta = timedelta(seconds=1),
    ):
        """
        Poll for scheduled tasks to run, and run them, until shut down by shutdown_event.
        """
        from .models import ScheduledTaskRunLog

        while not shutdown_event.is_set():
            now = timezone.now()
            run_logs = get_run_logs(self.schedules)

            for schedule, run_log in run_logs.items():
                next_scheduled = run_log.next_scheduled_run_time if run_log else None

                try:
                    if next_scheduled is None:
                        next_scheduled = schedule.get_next_scheduled_time(None, now)
                        if next_scheduled <= now:
                            task_result = self._enqueue_task(schedule)
                            new_next = schedule.get_next_scheduled_time(
                                next_scheduled, now
                            )
                            ScheduledTaskRunLog.create_or_update_run_log(
                                schedule,
                                task_id=self._get_task_id(task_result),
                                last_run_time=now,
                                last_scheduled_run_time=next_scheduled,
                                next_scheduled_run_time=new_next,
                            )
                        else:
                            ScheduledTaskRunLog.create_or_update_run_log(
                                schedule,
                                next_scheduled_run_time=next_scheduled,
                            )
                    elif next_scheduled <= now:
                        logger.info(
                            f"Enqueing task {schedule.task}, is/was due {next_scheduled}."
                        )
                        task_result = self._enqueue_task(schedule)
                        new_next = schedule.get_next_scheduled_time(next_scheduled, now)
                        ScheduledTaskRunLog.create_or_update_run_log(
                            schedule,
                            task_id=self._get_task_id(task_result),
                            last_run_time=now,
                            last_scheduled_run_time=next_scheduled,
                            next_scheduled_run_time=new_next,
                        )
                except DatabaseError as e:
                    logger.warning(
                        f"Database error while attempting to store task run log: {e!r}. "
                        "Closing database connection if unusable or obsolete, will try again next cycle."
                    )
                    connections[
                        ScheduledTaskRunLog.objects.db
                    ].close_if_unusable_or_obsolete()

            time.sleep(interval.total_seconds())

    def _enqueue_task(self, schedule: TaskSchedule) -> TaskResult:
        return schedule.task.enqueue(*schedule.task_args, **schedule.task_kwargs)

    def _get_task_id(self, task_result: TaskResult) -> str | None:
        if task_result.task.get_backend().supports_get_result:
            return task_result.id
        return None


scheduler = TaskScheduler()


def periodic_task(
    *,
    interval: timedelta = None,
    call_args: tuple = (),
    call_kwargs: dict[str, Any] = None,
    schedule_store: TaskScheduler = scheduler,
    task: Task = None,
) -> Callable[[Task], Task] | Task:
    """
    Register a task to be executed periodically.

    Can be used as a decorator or called directly with a task argument:
        @periodic_task(interval=timedelta(seconds=60))
        @task
        def my_task(): ...

        # Or:
        periodic_task(interval=timedelta(seconds=60), task=my_task)
    """

    def register(t: Task) -> Task:
        schedule = PeriodicSchedule(
            task=t,
            period=interval,
            task_args=call_args,
            task_kwargs=call_kwargs or {},
        )
        schedule_store.add_scheduled_task(schedule)
        return t

    if task is not None:
        return register(task)
    return register


def cron_task(
    *,
    cron_schedule: str,
    timezone_str: str | None = None,
    call_args: tuple = (),
    call_kwargs: dict[str, Any] = None,
    schedule_store: TaskScheduler = scheduler,
    task: Task = None,
) -> Callable[[Task], Task] | Task:
    # Let pydantic handle the string validation
    cron_str = cron_validator.validate_python(cron_schedule)

    def register(t: Task) -> Task:
        schedule = CrontabSchedule(
            task=t,
            cron_schedule=cron_str,
            task_args=call_args,
            task_kwargs=call_kwargs or {},
            timezone_str=timezone_str,
        )
        schedule_store.add_scheduled_task(schedule)
        return t

    if task is not None:
        return register(task)
    return register
