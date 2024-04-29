from django.apps import AppConfig


class DeepdiveConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "deepdive"

    def ready(self):
        # implictly connect signal handlers decorated with @receiver
        # importing in the method scope to minimize side-effects of importing code
        from deepdive import signals
        from deepdive.auth import signals
