from __future__ import annotations
import asyncio
import logging

from django.core.management.base import BaseCommand, CommandError
from stratbot.scanner.integrations.binance.consumer import BinanceCandleConsumer
from stratbot.scanner.integrations.polygon.consumer import PolygonCandleConsumer
from stratbot.scanner.integrations.alpaca.consumer import AlpacaCandleConsumer


log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)


class Command(BaseCommand):
    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "--binance",
            action="store_true",
            help="Run the loop for Binance.",
        )
        parser.add_argument(
            "--alpaca",
            action="store_true",
            help="Run the loop for Alpaca.",
        )

    def handle(self, *args, **options):
        run_binance = options.get("binance")
        run_alpaca = options.get("alpaca")

        if (int(bool(run_binance)) + int(bool(run_alpaca))) != 1:
            raise CommandError(
                "Must provide exactly one of --alpaca, --binance when running"
                "command."
            )

        candles: PolygonCandleConsumer | BinanceCandleConsumer | AlpacaCandleConsumer
        if run_binance:
            candles = BinanceCandleConsumer()
        elif run_alpaca:
            candles = AlpacaCandleConsumer()
        else:
            raise CommandError(
                "Must provide exactly one of --alpaca, --binance when running"
                "command."
            )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(candles.run())
        except KeyboardInterrupt:
            log.info("exiting..")
