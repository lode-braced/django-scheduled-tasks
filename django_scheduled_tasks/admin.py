from django.contrib import admin, messages

from .base import scheduler
from .models import ScheduledTaskRunLog


@admin.register(ScheduledTaskRunLog)
class ScheduledTaskRunLogAdmin(admin.ModelAdmin):
    """Admin for scheduled tasks defined in code."""

    list_display = [
        "task_name",
        "schedule_type",
        "schedule_description",
        "enabled",
        "next_scheduled_run_time",
        "last_run_time",
    ]
    list_filter = ["enabled", "schedule_type"]
    search_fields = ["task_name"]
    list_editable = ["enabled"]
    ordering = ["task_name"]
    actions = ["run_task_now"]

    readonly_fields = [
        "task_hash_display",
        "task_name",
        "schedule_type",
        "schedule_description",
        "last_run_time",
        "last_scheduled_run_time",
        "next_scheduled_run_time",
        "last_run_task_id",
    ]

    fieldsets = [
        (
            None,
            {
                "fields": [
                    "task_name",
                    "schedule_type",
                    "schedule_description",
                    "enabled",
                ]
            },
        ),
        (
            "Scheduling State",
            {
                "fields": [
                    "next_scheduled_run_time",
                    "last_run_time",
                    "last_scheduled_run_time",
                    "last_run_task_id",
                ],
                "classes": ["collapse"],
            },
        ),
        (
            "Technical",
            {
                "fields": ["task_hash_display"],
                "classes": ["collapse"],
            },
        ),
    ]

    def task_hash_display(self, obj):
        return obj.task_hash.hex()

    task_hash_display.short_description = "Task hash"

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    @admin.action(description="Run selected tasks now")
    def run_task_now(self, request, queryset):
        # Build a lookup of task_hash -> schedule from the global scheduler
        schedule_by_hash = {
            schedule.to_sha_bytes(): schedule for schedule in scheduler.schedules
        }

        enqueued = 0
        not_found = 0

        for run_log in queryset:
            schedule = schedule_by_hash.get(bytes(run_log.task_hash))
            if schedule:
                schedule.task.enqueue(*schedule.task_args, **schedule.task_kwargs)
                enqueued += 1
            else:
                not_found += 1

        if enqueued:
            self.message_user(
                request,
                f"Enqueued {enqueued} task(s).",
                messages.SUCCESS,
            )
        if not_found:
            self.message_user(
                request,
                f"{not_found} task(s) not found in scheduler (may not be registered).",
                messages.WARNING,
            )
