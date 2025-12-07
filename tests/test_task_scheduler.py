import datetime
import threading
from datetime import timedelta

from dateutil import tz
from django.tasks import task
from django.utils import timezone
from django.utils.timezone import now
from freezegun import freeze_time

from django_scheduled_tasks.base import (
    scheduler,
    periodic_task,
    cron_task,
    PeriodicSchedule,
    CrontabSchedule,
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


# --- General TaskSchedule tests ---


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


# --- PeriodicSchedule tests ---


@freeze_time("2025-01-01")
def test_periodic_task_registration(db):
    test_task = periodic_task(
        interval=timedelta(seconds=1),
    )(foo)
    # Check the newly created task exists in the scheduler
    schedules_for_task = [s for s in scheduler.schedules if s.task == test_task]
    assert len(schedules_for_task) == 1
    # First run (no previous scheduled time) should run immediately
    assert schedules_for_task[0].get_next_scheduled_time(None, now()) == now()


def test_periodic_schedule_avoids_slippage():
    """Next scheduled time is based on previous scheduled time, not now."""
    schedule = PeriodicSchedule(
        task=foo,
        period=timedelta(seconds=10),
        task_args=(),
        task_kwargs={},
    )
    previous = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    now_time = datetime.datetime(2025, 1, 1, 12, 0, 11, tzinfo=datetime.UTC)
    # Even though now is 11 seconds later, next should be previous + 10s = 12:00:10
    # But since 12:00:10 <= now, it advances to 12:00:20
    next_scheduled = schedule.get_next_scheduled_time(previous, now_time)
    assert next_scheduled == datetime.datetime(
        2025, 1, 1, 12, 0, 20, tzinfo=datetime.UTC
    )


def test_periodic_schedule_thundering_herd_prevention():
    """If scheduler was down and multiple runs were missed, only advance to next future time."""
    schedule = PeriodicSchedule(
        task=foo,
        period=timedelta(seconds=10),
        task_args=(),
        task_kwargs={},
    )
    previous = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    # Scheduler was down for 55 seconds - 5 runs were missed
    now_time = datetime.datetime(2025, 1, 1, 12, 0, 55, tzinfo=datetime.UTC)
    next_scheduled = schedule.get_next_scheduled_time(previous, now_time)
    # Should skip to next future time (12:01:00), not run 5 times
    assert next_scheduled == datetime.datetime(
        2025, 1, 1, 12, 1, 0, tzinfo=datetime.UTC
    )


# --- CrontabSchedule tests ---


def test_cron_task_registration():
    test_scheduler = TaskScheduler()
    test_task = cron_task(
        cron_schedule="0 9 * * *",
        schedule_store=test_scheduler,
    )(foo)

    assert len(test_scheduler.schedules) == 1
    schedule = next(iter(test_scheduler.schedules))
    assert isinstance(schedule, CrontabSchedule)
    assert schedule.task == test_task
    assert schedule.cron_schedule == "0 9 * * *"
    assert schedule.timezone_str is None
    with freeze_time("2025-01-01"):
        # First run (no previous) should wait for next cron slot.
        assert schedule.get_next_scheduled_time(
            None, timezone.now()
        ) == datetime.datetime(
            2025,
            1,
            1,
            9,
            tzinfo=datetime.UTC,
        )


def test_cron_task_registration_with_timezone():
    test_scheduler = TaskScheduler()
    test_task = cron_task(
        cron_schedule="0 9 * * *",
        timezone_str="Europe/Brussels",
        schedule_store=test_scheduler,
    )(bar)

    assert len(test_scheduler.schedules) == 1
    schedule = next(iter(test_scheduler.schedules))
    assert isinstance(schedule, CrontabSchedule)
    assert schedule.task == test_task
    assert schedule.cron_schedule == "0 9 * * *"
    assert schedule.timezone_str == "Europe/Brussels"
    with freeze_time("2025-01-01"):
        assert schedule.get_next_scheduled_time(
            None, timezone.now()
        ) == datetime.datetime(
            2025,
            1,
            1,
            9,
            tzinfo=tz.gettz("Europe/Brussels"),
        )


def test_cron_schedule_thundering_herd_prevention():
    """If scheduler was down and cron slots were missed, skip to next future slot."""
    schedule = CrontabSchedule(
        task=foo,
        cron_schedule="* * * * *",  # every minute
        task_args=(),
        task_kwargs={},
    )
    previous = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    # Scheduler was down for 5 minutes
    now_time = datetime.datetime(2025, 1, 1, 12, 5, 30, tzinfo=datetime.UTC)
    next_scheduled = schedule.get_next_scheduled_time(previous, now_time)
    # Should skip to next future minute (12:06:00), not run 5 times
    assert next_scheduled == datetime.datetime(
        2025, 1, 1, 12, 6, 0, tzinfo=datetime.UTC
    )


# --- Scheduler loop tests ---


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
