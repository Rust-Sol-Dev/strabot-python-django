import asyncio
import logging
from django.core.management.base import BaseCommand
from stratbot.scanner.integrations.polygon.ws import polygon_ws


log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run PolygonB WebSockets: real-time data (quotes, candles, trades)'

    def handle(self, *args, **kwargs):
        try:
            while True:
                asyncio.run(polygon_ws.connect())
        except KeyboardInterrupt:
            polygon_ws.flush_stream()
            log.info("Exiting..")
