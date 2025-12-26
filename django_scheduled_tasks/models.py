from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Self

from django.db import models

if TYPE_CHECKING:
    from django_scheduled_tasks.base import TaskSchedule


class ScheduledTaskRunLog(models.Model):
    """
    Stores scheduling state for code-defined tasks, allowing the scheduler to track timing even across restarts.
    """

    # task data, args, and schedule, stored as a sha 256 hash of the total.
    task_hash = models.BinaryField(max_length=32, unique=True)
    last_run_time = models.DateTimeField(null=True, blank=True)
    last_scheduled_run_time = models.DateTimeField(null=True, blank=True)
    next_scheduled_run_time = models.DateTimeField(null=True, blank=True)
    last_run_task_id = models.CharField(max_length=64, null=True, blank=True)

    # For code-defined task management
    enabled = models.BooleanField(default=True)
    task_name = models.CharField(max_length=255, blank=True, default="")
    schedule_type = models.CharField(max_length=50, blank=True, default="")
    schedule_description = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        verbose_name = "Scheduled task"
        verbose_name_plural = "Scheduled tasks"

    def __str__(self) -> str:
        return self.task_name or f"Task {self.task_hash.hex()[:8]}"

    @classmethod
    def create_or_update_run_log(
        cls,
        task_schedule: "TaskSchedule",
        task_id: str | None = None,
        last_run_time: datetime | None = None,
        last_scheduled_run_time: datetime | None = None,
        next_scheduled_run_time: datetime | None = None,
        task_name: str | None = None,
        schedule_type: str | None = None,
        schedule_description: str | None = None,
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
        if task_name is not None:
            defaults["task_name"] = task_name
        if schedule_type is not None:
            defaults["schedule_type"] = schedule_type
        if schedule_description is not None:
            defaults["schedule_description"] = schedule_description

        return cls.objects.update_or_create(
            task_hash=task_schedule.to_sha_bytes(),
            defaults=defaults,
        )[0]
