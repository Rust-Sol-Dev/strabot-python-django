import asyncio
import logging
from django.core.management.base import BaseCommand
from stratbot.scanner.integrations.binance.ws import binance_ws


log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run Binance WebSockets: real-time data (quotes, candles, trades)'

    def handle(self, *args, **kwargs):
        try:
            while True:
                asyncio.run(binance_ws.connect())
        except KeyboardInterrupt:
            binance_ws.flush_stream()
            log.info("Exiting..")
