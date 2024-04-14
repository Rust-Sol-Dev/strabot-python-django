import logging
from datetime import time, datetime, timedelta
from time import perf_counter

import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from confluent_kafka import Consumer
from django.conf import settings
from django.db import IntegrityError, OperationalError
from django.utils import timezone
from orjson import orjson

from stratbot.scanner.models.symbols import SymbolType, SymbolRec
from stratbot.scanner.models.pricerecs import StockPriceRec, CryptoPriceRec

log = logging.getLogger(__name__)


class BaseConsumer:
    def __init__(self, symbol_type: SymbolType, exchange_id: str, topic: str, group_id: str):
        self.symbol_type = symbol_type
        self.exchange_id = exchange_id
        self.topic = topic
        self.group_id = group_id
        self._set_consumer()

        self.symbolrecs = SymbolRec.objects.filter(symbol_type=self.symbol_type)
        self.symbols = set(self.symbolrecs.values_list('symbol', flat=True))
        self.pricerec_model = StockPriceRec if symbol_type == SymbolType.STOCK else CryptoPriceRec
        self.last_symbol_refresh = timezone.now()
        self.quotes = {}

    def _set_consumer(self):
        conf = {
            'bootstrap.servers': ','.join(settings.REDPANDA_BROKERS),
            'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
            'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
            'sasl.username': settings.REDPANDA_USERNAME,
            'sasl.password': settings.REDPANDA_PASSWORD,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest'
        }
        self.consumer = Consumer(**conf)

    @sync_to_async
    def _save_pricerec_to_db(self, parsed_bar):
        try:
            self.pricerec_model.objects.create(**parsed_bar)
        except (IntegrityError, OperationalError):
            pass

    @sync_to_async
    def _bulk_update_quotes(self):
        for symbolrec in self.symbolrecs:
            if symbolrec.symbol not in self.quotes:
                continue
            symbolrec.price = self.quotes[symbolrec.symbol]
            symbolrec.as_of = timezone.now()
        SymbolRec.objects.bulk_update(self.symbolrecs, ['price', 'as_of'])
        log.info(f'{self.exchange_id}: updated {len(self.quotes)} {self.symbol_type} quotes')
        self.quotes = {}

    async def bar_to_timescale(self, bar, symbol_type: SymbolType):
        # timestamp = datetime.fromtimestamp(bar['timestamp'] / 1000.0)
        # if timestamp < datetime.now() - timedelta(hours=1):
        #     return
        s = perf_counter()
        bar['time'] = pd.to_datetime(bar['timestamp'], unit='ms', utc=True)
        dt = bar['time'].tz_convert(pytz.timezone('America/New_York'))
        rth = time(9, 30) <= dt.time() < time(16, 0)
        if symbol_type == SymbolType.STOCK and not rth:
            logging.debug(f'bar_to_timescale: skipping {bar["symbol"]}, outside of RTH')
            return
        if symbol_type == SymbolType.CRYPTO:
            bar['exchange'] = self.exchange_id

        del bar['timestamp']
        symbol = bar.get('symbol')
        await self._save_pricerec_to_db(bar)
        elapsed = perf_counter() - s
        log.info(f'write timeseries bar for [{symbol}] in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

    @staticmethod
    async def handle_message(msg):
        key = msg.key().decode('utf-8') if msg.key() else None
        # value = msg.value().decode('utf-8')
        # value_json = json.loads(value)
        value = orjson.loads(msg.value())
        log.debug(f'{key=}: {value}')
        return key, value

    async def process_message(self, key, msg):
        ...

    async def run(self):
        log.info('Kafka Consumer has been initiated...')
        available_topics = self.consumer.list_topics().topics
        log.info(f'Available topics to consume: {len(available_topics)}')
        self.consumer.subscribe([self.topic])
        while True:
            msg = self.consumer.poll(0.001)
            if msg is None:
                continue
            if msg.error():
                log.error('{}'.format(msg.error()))
                continue
            else:
                key, value = await self.handle_message(msg)
                await self.process_message(key, value)
