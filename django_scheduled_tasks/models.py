from datetime import datetime
from typing import Self

from django.db import models
from django.utils import timezone

from django_scheduled_tasks.base import TaskSchedule


class ScheduledTaskRunLog(models.Model):
    """
    Stores the last time a task was run, to allow the task scheduler to determine the next run time, even if restarted.
    """

    # task data, args, and schedule, stored as a sha 256 hash of the total.
    task_hash = models.BinaryField(max_length=32, unique=True)
    last_run_time = models.DateTimeField(null=True, blank=True)
    last_run_task_id = models.CharField(max_length=64, null=True, blank=True)

    @classmethod
    def create_or_update_run_log(
        cls,
        task_schedule: TaskSchedule,
        task_id: str = None,
        scheduled_time: datetime = None,
    ) -> Self:
        return cls.objects.update_or_create(
            task_hash=task_schedule.to_sha_bytes(),
            defaults={
                "last_run_time": scheduled_time or timezone.now(),
                "last_run_task_id": task_id,
            },
        )[0]
