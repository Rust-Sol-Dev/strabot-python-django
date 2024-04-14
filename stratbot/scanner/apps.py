from __future__ import annotations

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class TradingConfig(AppConfig):
    name = "stratbot.scanner"
    verbose_name = _("Scanner")

    def ready(self):
        try:
            import stratbot.scanner.signals  # noqa F401
        except ImportError:
            pass
