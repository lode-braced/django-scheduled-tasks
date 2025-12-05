"""
Django settings for manual testing of django_scheduled_tasks.

Run the scheduler with:
    python manage.py run_task_scheduler --settings=dev.settings

Run the task worker with:
    python manage.py db_worker --settings=dev.settings
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "django-insecure-dev-only-not-for-production"

DEBUG = True

ALLOWED_HOSTS = []

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_tasks.backends.database",
    "django_scheduled_tasks",
    "dev",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "dev.sqlite3",
    }
}

USE_TZ = True
TIME_ZONE = "UTC"

TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.database.DatabaseBackend",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
