# Fixes existing installations where task_hash was created as BinaryField in 0001_initial.
# New installations create task_hash as CharField directly in 0001_initial, so this is a no-op for them.

from django.db import migrations, models


def binary_to_hex(apps, schema_editor):
    ScheduledTaskRunLog = apps.get_model(
        "django_scheduled_tasks", "ScheduledTaskRunLog"
    )
    for record in ScheduledTaskRunLog.objects.all():
        raw = record.task_hash
        if raw is not None:
            # BinaryField returns a memoryview; convert to hex string
            record.task_hash_char = bytes(raw).hex()
            record.save(update_fields=["task_hash_char"])


def hex_to_binary(apps, schema_editor):
    ScheduledTaskRunLog = apps.get_model(
        "django_scheduled_tasks", "ScheduledTaskRunLog"
    )
    for record in ScheduledTaskRunLog.objects.all():
        hex_val = record.task_hash_char
        if hex_val:
            record.task_hash = bytes.fromhex(hex_val)
            record.save(update_fields=["task_hash"])


class Migration(migrations.Migration):
    dependencies = [
        (
            "django_scheduled_tasks",
            "0003_alter_scheduledtaskrunlog_options_and_more",
        ),
    ]

    operations = [
        # 1. Add a temporary CharField to hold the converted hex values
        migrations.AddField(
            model_name="scheduledtaskrunlog",
            name="task_hash_char",
            field=models.CharField(max_length=64, null=True, blank=True),
        ),
        # 2. Copy binary → hex into the temp column
        migrations.RunPython(binary_to_hex, reverse_code=hex_to_binary),
        # 3. Drop the old BinaryField
        migrations.RemoveField(
            model_name="scheduledtaskrunlog",
            name="task_hash",
        ),
        # 4. Rename the temp column to task_hash
        migrations.RenameField(
            model_name="scheduledtaskrunlog",
            old_name="task_hash_char",
            new_name="task_hash",
        ),
        # 5. Apply the final constraints (unique, not null)
        migrations.AlterField(
            model_name="scheduledtaskrunlog",
            name="task_hash",
            field=models.CharField(max_length=64, unique=True),
        ),
    ]
