# Fixes existing installations where task_hash was created as BinaryField in 0001_initial.
# New installations create task_hash as CharField directly in 0001_initial, so this is a no-op for them.

from django.db import migrations, models


def binary_to_hex(apps, schema_editor):
    ScheduledTaskRunLog = apps.get_model(
        "django_scheduled_tasks", "ScheduledTaskRunLog"
    )
    for record in ScheduledTaskRunLog.objects.all():
        raw = record.task_hash
        if raw is None:
            continue
        # New installs already have a hex CharField — nothing to convert
        if isinstance(raw, str):
            record.task_hash_char = raw
        else:
            # BinaryField returns a memoryview; convert to hex string
            record.task_hash_char = bytes(raw).hex()
        record.save(update_fields=["task_hash_char"])


def hex_to_binary(apps, schema_editor):
    ScheduledTaskRunLog = apps.get_model(
        "django_scheduled_tasks", "ScheduledTaskRunLog"
    )
    # Detect whether task_hash is a BinaryField or CharField in the current state
    task_hash_field = ScheduledTaskRunLog._meta.get_field("task_hash")
    is_binary = task_hash_field.get_internal_type() == "BinaryField"

    for record in ScheduledTaskRunLog.objects.all():
        hex_val = record.task_hash_char
        if not hex_val:
            continue
        # Old installs reverse to a BinaryField; new installs reverse to a CharField
        record.task_hash = bytes.fromhex(hex_val) if is_binary else hex_val
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
        # 2. Reverse must repopulate task_hash before making it non-null again
        migrations.AlterField(
            model_name="scheduledtaskrunlog",
            name="task_hash",
            field=models.CharField(max_length=64, unique=True, null=True),
        ),
        # 3. Copy binary → hex into the temp column
        migrations.RunPython(binary_to_hex, reverse_code=hex_to_binary),
        # 4. Drop the old BinaryField
        migrations.RemoveField(
            model_name="scheduledtaskrunlog",
            name="task_hash",
        ),
        # 5. Rename the temp column to task_hash
        migrations.RenameField(
            model_name="scheduledtaskrunlog",
            old_name="task_hash_char",
            new_name="task_hash",
        ),
        # 6. Apply the final constraints (unique, not null)
        migrations.AlterField(
            model_name="scheduledtaskrunlog",
            name="task_hash",
            field=models.CharField(max_length=64, unique=True),
        ),
    ]
