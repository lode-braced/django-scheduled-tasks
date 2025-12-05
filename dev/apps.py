from django.apps import AppConfig


class DevConfig(AppConfig):
    name = "dev"
    verbose_name = "Development Testing App"

    def ready(self):
        from dev import tasks  # noqa: F401
