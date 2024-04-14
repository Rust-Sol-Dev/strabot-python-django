from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from stratbot.scanner.ops.live_loop.crypto import CryptoLoop
from stratbot.scanner.ops.live_loop.stocks import StocksLoop


class Command(BaseCommand):
    """
    Run a live loop.
    """

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "--stocks",
            action="store_true",
            help="Run the loop for stocks.",
        )
        parser.add_argument(
            "--crypto",
            action="store_true",
            help="Run the loop for crypto.",
        )

    def handle(self, *args, **options):
        run_stocks = options.get("stocks")
        run_crypto = options.get("crypto")
        if (int(bool(run_stocks)) + int(bool(run_crypto))) != 1:
            raise CommandError(
                "Must provide exactly one of --stocks or --crypto when running this "
                "command."
            )

        loop: StocksLoop | CryptoLoop
        if run_stocks:
            loop = StocksLoop()
        else:
            loop = CryptoLoop()

        loop.run()
