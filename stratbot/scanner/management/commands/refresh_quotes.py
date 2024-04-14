from __future__ import annotations
import asyncio
import logging

from django.core.management.base import BaseCommand, CommandError
from stratbot.scanner.integrations.binance.consumer import BinanceQuoteConsumer
# from stratbot.scanner.integrations.polygon.consumer import PolygonQuoteConsumer
from stratbot.scanner.integrations.alpaca.consumer import AlpacaQuoteConsumer

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "--alpaca",
            action="store_true",
            help="Run the loop for stocks.",
        )
        parser.add_argument(
            "--binance",
            action="store_true",
            help="Run the loop for Binance.",
        )

    def handle(self, *args, **options):
        run_stocks = options.get("alpaca")
        run_binance = options.get("binance")

        if (int(bool(run_stocks)) + int(bool(run_binance))) != 1:
            raise CommandError("Must provide exactly one of --stocks or --binance when running this command.")

        quote_consumer: AlpacaQuoteConsumer | BinanceQuoteConsumer
        if run_stocks:
            quote_consumer = AlpacaQuoteConsumer()
        else:
            quote_consumer = BinanceQuoteConsumer()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(quote_consumer.run())
        except KeyboardInterrupt:
            log.info("exiting..")
