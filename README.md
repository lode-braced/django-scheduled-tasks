# Django-scheduled-tasks: task scheduling for the Django tasks framework

A Django app that allows scheduling for the
[Django 6.0 task framework](https://docs.djangoproject.com/en/6.0/topics/tasks/).

## Installation & Usage

First, make sure your
[task backend is setup](https://docs.djangoproject.com/en/6.0/topics/tasks/#configuring-a-task-backend).
I'd recommend starting with the database backend in
[django-taks](https://github.com/RealOrangeOne/django-tasks).

Then, add the scheduled tasks:

```
pip install django-scheduled-tasks
```

Add the django_scheduled_tasks module to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    ...,
    "django_scheduled_tasks",
]
```

define some tasks to run periodically, by wrapping around an existing task, either as a decorator or by calling
`periodic_task` directly:

```python
from django.tasks import task
from django_scheduled_tasks import periodic_task
from datetime import timedelta


# note the order of the decorators! Make sure periodic_task is above task
@periodic_task(interval=timedelta(hours=2))
@task
def run_hourly():
    ...


# or call periodic task with a task directly:
@task
def some_existing_task(some_arg: str):
    ...


periodic_task(interval=timedelta(hours=3), call_args=("some_arg_value",), task=some_existing_task)
```

For cron-style scheduling, use `cron_task` with a cron expression:

```python
from django_scheduled_tasks import cron_task
from django.tasks import task

# Run at 9am every day
@cron_task(cron_schedule="0 9 * * *")
@task
def daily_report():
    ...


# Run at 9am in a specific timezone
@cron_task(cron_schedule="0 9 * * *", timezone_str="Europe/Brussels")
@task
def timezoned_scheduled_task():
    ...
```

Lastly, run one instance of the scheduler as a part of your application:

```
python manage.py run_task_scheduler
```
