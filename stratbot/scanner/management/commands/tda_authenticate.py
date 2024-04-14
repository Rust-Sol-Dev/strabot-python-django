from __future__ import annotations

from django.core.management.base import BaseCommand

from config.integrations.tda.ops import obtain_credentials


class Command(BaseCommand):
    """
    Use https://github.com/alexgolec/tda-api (the current TDA third-party library we're
    using) with their selenium integration to authenticate with TDA and retrieve + store
    their credentials the way we're currently storing them (at the time of writing file
    but that could change down the line. See other comments/notes potentially).
    """

    help = __doc__

    def handle(self, *args, **options):
        obtain_credentials()
