from datetime import datetime
from typing import Self

from django.db import models
from django.utils import timezone

from django_scheduled_tasks.base import TaskSchedule


class ScheduledTaskRunLog(models.Model):
    """
    Stores scheduling state for a task, allowing the scheduler to track timing even across restarts.
    """

    # task data, args, and schedule, stored as a sha 256 hash of the total.
    task_hash = models.BinaryField(max_length=32, unique=True)
    last_run_time = models.DateTimeField(null=True, blank=True)
    last_scheduled_run_time = models.DateTimeField(null=True, blank=True)
    next_scheduled_run_time = models.DateTimeField(null=True, blank=True)
    last_run_task_id = models.CharField(max_length=64, null=True, blank=True)

    @classmethod
    def create_or_update_run_log(
        cls,
        task_schedule: TaskSchedule,
        task_id: str | None = None,
        last_run_time: datetime | None = None,
        last_scheduled_run_time: datetime | None = None,
        next_scheduled_run_time: datetime | None = None,
    ) -> Self:
        defaults = {}
        if last_run_time is not None:
            defaults["last_run_time"] = last_run_time
        if last_scheduled_run_time is not None:
            defaults["last_scheduled_run_time"] = last_scheduled_run_time
        if next_scheduled_run_time is not None:
            defaults["next_scheduled_run_time"] = next_scheduled_run_time
        if task_id is not None:
            defaults["last_run_task_id"] = task_id

        return cls.objects.update_or_create(
            task_hash=task_schedule.to_sha_bytes(),
            defaults=defaults,
        )[0]
