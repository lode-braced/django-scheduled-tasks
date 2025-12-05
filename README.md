from datetime import timedeltafrom dev.settings import INSTALLED_APPS

# Django-scheduled-tasks: task scheduling for the Django tasks framework

A Django app that allows scheduling for the
[Django 6.0 task framework](https://docs.djangoproject.com/en/6.0/topics/tasks/).

## Installation

First, make sure your
[task backend is setup](https://docs.djangoproject.com/en/6.0/topics/tasks/#configuring-a-task-backend).
I'd recommend starting with the database backend in
[django-taks](https://github.com/RealOrangeOne/django-tasks)

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


periodic_task(interval=timedelta(hours=3), call_args=("some_arg_value",))
```
