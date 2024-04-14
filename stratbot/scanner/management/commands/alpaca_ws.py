import asyncio
import logging
from django.core.management.base import BaseCommand
from stratbot.scanner.integrations.alpaca.ws import alpaca_ws


log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run Alpaca WebSockets streaming'

    def handle(self, *args, **kwargs):
        try:
            while True:
                asyncio.run(alpaca_ws.connect())
        except KeyboardInterrupt:
            alpaca_ws.flush_stream()
            log.info("Exiting..")
