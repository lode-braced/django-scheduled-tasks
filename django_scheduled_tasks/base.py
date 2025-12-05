from __future__ import annotations
import abc
import datetime
import hashlib
import threading
import time
from datetime import timedelta
from typing import Annotated, Callable, TYPE_CHECKING, Any
from django.tasks import Task, TaskResult
from django.utils import timezone
from pydantic import BaseModel, ConfigDict, PlainSerializer, PlainValidator

if TYPE_CHECKING:
    from .models import ScheduledTaskRunLog

type Json = dict[str, Json] | list[Json] | str | int | float | bool | None


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
    def run_at(
        self,
        now: datetime.datetime,
        last_schedule_time: datetime.datetime | None = None,
        last_result: TaskResult | None = None,
    ) -> datetime.datetime:
        """
        Return the next time the task for this schedule should run.
        Args:
            now: the current time, timezone-aware.
            last_schedule_time: the last scheduled execution time of this task, if any.
            last_result: the last scheduled execution of this task, if any. May be none even if last_schedule_time is not,
            e.g., if the task backend does not support storing task results.
        Returns: the next time the task should run.
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

    def run_at(
        self,
        now: datetime.datetime,
        last_execution_time: datetime.datetime = None,
        last_execution: TaskResult | None = None,
    ) -> datetime.datetime:
        if last_execution_time is not None:
            return last_execution_time + self.period
        else:
            return now


def get_last_runs(
    task_schedules: set[TaskSchedule],
) -> dict[TaskSchedule, "ScheduledTaskRunLog | None"]:
    task_hash_map: dict[bytes, TaskSchedule] = {
        task.to_sha_bytes(): task for task in task_schedules
    }
    from .models import ScheduledTaskRunLog

    last_runs = ScheduledTaskRunLog.objects.filter(task_hash__in=task_hash_map.keys())
    run_log_map = {task_hash_map[run.task_hash]: run for run in last_runs}
    # Return all schedules, with None for those without run logs
    return {schedule: run_log_map.get(schedule) for schedule in task_schedules}


class TaskScheduler:
    def __init__(self):
        self.schedules: set[TaskSchedule] = set()

    def add_scheduled_task(self, schedule: TaskSchedule):
        self.schedules.add(schedule)

    def get_next_run_times(self) -> dict[TaskSchedule, datetime.datetime]:
        task_run_logs = get_last_runs(self.schedules)
        return {
            schedule: schedule.run_at(
                timezone.now(),
                last_run_info.last_run_time if last_run_info else None,
                schedule.task.get_result(last_run_info.last_run_task_id)
                if last_run_info
                and schedule.task.get_backend().supports_get_result
                and last_run_info.last_run_task_id
                else None,
            )
            for schedule, last_run_info in task_run_logs.items()
        }

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
            next_run_times = self.get_next_run_times()
            for schedule, next_run_time in next_run_times.items():
                if next_run_time <= timezone.now():
                    task_result: TaskResult = schedule.task.enqueue(
                        *schedule.task_args, **schedule.task_kwargs
                    )
                    # store the task id _if_ the backend supports it
                    if task_result.task.get_backend().supports_get_result:
                        task_id = task_result.id
                    else:
                        task_id = None
                    ScheduledTaskRunLog.create_or_update_run_log(
                        schedule, task_id, next_run_time
                    )
            time.sleep(interval.total_seconds())


scheduler = TaskScheduler()


def periodic_task(
    *,
    interval: timedelta = None,
    call_args: tuple = (),
    call_kwargs: dict[str, Any] = None,
    schedule_store: TaskScheduler = scheduler,
) -> Callable[[Task], Task]:
    """
    Wrap a task to be executed periodically.
    """

    def wrapper(task: Task) -> Task:
        schedule = PeriodicSchedule(
            task=task,
            period=interval,
            task_args=call_args,
            task_kwargs=call_kwargs or {},
        )
        schedule_store.add_scheduled_task(schedule)
        return task

    return wrapper
