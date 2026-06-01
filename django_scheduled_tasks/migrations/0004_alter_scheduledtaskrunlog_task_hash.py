# Fixes existing installations where task_hash was created as BinaryField in 0001_initial.
# New installations create task_hash as CharField directly in 0001_initial, so this is a no-op for them.

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        (
            "django_scheduled_tasks",
            "0003_alter_scheduledtaskrunlog_options_and_more",
        ),
    ]

    operations = [
        migrations.AlterField(
            model_name="scheduledtaskrunlog",
            name="task_hash",
            field=models.CharField(max_length=64, unique=True),
        ),
    ]
