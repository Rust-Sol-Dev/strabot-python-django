from __future__ import annotations

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class AlertsConfig(AppConfig):
    name = "stratbot.alerts"
    verbose_name = _("Alerts")

    def ready(self):
        try:
            import stratbot.alerts.signals  # noqa F401
        except ImportError:
            pass
