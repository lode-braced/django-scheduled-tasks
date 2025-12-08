from django.contrib import admin

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
