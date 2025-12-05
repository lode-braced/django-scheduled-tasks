import threading
from datetime import timedelta

from django.tasks import task
from django.utils.timezone import now
from freezegun import freeze_time

from django_scheduled_tasks.base import (
    scheduler,
    periodic_task,
    PeriodicSchedule,
    TaskScheduler,
)
from django_scheduled_tasks.models import ScheduledTaskRunLog


@task
def foo(*args, **kwargs):
    return


@task
def bar(*args, **kwargs):
    return


call_log: list[str] = []


@task
def tracking_task(value: str):
    call_log.append(value)


@freeze_time("2025-01-01")
def test_periodic_task_registration(db):
    test_task = periodic_task(
        interval=timedelta(seconds=1),
    )(foo)
    task_run_times = scheduler.get_next_run_times()
    # Check the newly created task exists via its schedule, and is scheduled to run now.
    schedules_for_task = [s for s in task_run_times if s.task == test_task]
    assert len(schedules_for_task) == 1
    assert task_run_times[schedules_for_task[0]] == now()


def test_task_schedule_serialization_uses_import_string():
    schedule = PeriodicSchedule(
        task=bar,
        period=timedelta(seconds=60),
        task_args=(),
        task_kwargs={},
    )
    dumped = schedule.model_dump()
    # The task should be serialized as its import string
    assert dumped["task"] == "tests.test_task_scheduler.bar"


def test_task_schedule_hash_is_stable():
    schedule1 = PeriodicSchedule(
        task=bar,
        period=timedelta(seconds=60),
        task_args=("arg1",),
        task_kwargs={"key": "value"},
    )
    schedule2 = PeriodicSchedule(
        task=bar,
        period=timedelta(seconds=60),
        task_args=("arg1",),
        task_kwargs={"key": "value"},
    )
    assert schedule1.to_sha_bytes() == schedule2.to_sha_bytes()


def test_different_schedules_have_different_hashes():
    schedule1 = PeriodicSchedule(
        task=foo,
        period=timedelta(seconds=60),
        task_args=(),
        task_kwargs={},
    )
    schedule2 = PeriodicSchedule(
        task=bar,
        period=timedelta(seconds=60),
        task_args=(),
        task_kwargs={},
    )
    assert schedule1.to_sha_bytes() != schedule2.to_sha_bytes()


def test_run_task_loop(transactional_db):
    import time
    from django_tasks.backends.database.models import DBTaskResult

    test_scheduler = TaskScheduler()
    schedule = PeriodicSchedule(
        task=tracking_task,
        period=timedelta(milliseconds=200),
        task_args=("test",),
    )
    test_scheduler.add_scheduled_task(schedule)

    shutdown_event = threading.Event()
    loop_interval = timedelta(milliseconds=10)

    thread = threading.Thread(
        target=test_scheduler.run_scheduling_loop,
        args=(shutdown_event, loop_interval),
    )
    thread.start()

    try:
        # Task should execute immediately (no previous run)
        time.sleep(0.05)
        assert DBTaskResult.objects.count() == 1
        run_log = ScheduledTaskRunLog.objects.get(task_hash=schedule.to_sha_bytes())
        first_run_time = run_log.last_run_time

        # Wait less than the period - no new run should occur
        time.sleep(0.05)
        assert DBTaskResult.objects.count() == 1
        run_log.refresh_from_db()
        assert run_log.last_run_time == first_run_time

        # Wait past the period - second run should occur
        time.sleep(0.2)
        assert DBTaskResult.objects.count() == 2
        run_log.refresh_from_db()
        assert run_log.last_run_time > first_run_time
    finally:
        shutdown_event.set()
        thread.join(timeout=1)
