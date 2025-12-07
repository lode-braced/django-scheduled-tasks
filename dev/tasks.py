import random
from datetime import timedelta

from django.tasks import task
from django.utils import timezone

from django_scheduled_tasks.base import cron_task, periodic_task


@periodic_task(interval=timedelta(seconds=5))
@task
def heartbeat():
    print(f"[{timezone.now()}] Heartbeat task executed")


@periodic_task(interval=timedelta(seconds=10))
@task
def random_number():
    number = random.randint(1, 100)
    print(f"[{timezone.now()}] Random number: {number}")
    return number


@periodic_task(interval=timedelta(seconds=15), call_kwargs={"name": "Developer"})
@task
def greet(name: str):
    message = f"Hello, {name}!"
    print(f"[{timezone.now()}] {message}")
    return message


@cron_task(cron_schedule="* * * * *")
@task
def every_minute():
    print(f"[{timezone.now()}] Every minute cron task executed")
