import asyncio
import logging
from datetime import timedelta

from asgiref.sync import sync_to_async
from confluent_kafka import Producer
from django.conf import settings
from django.db import OperationalError, InterfaceError
from django.utils import timezone
import orjson
import websockets

from stratbot.scanner.models.symbols import SymbolRec, SymbolType


log = logging.getLogger(__name__)


class WebsocketBridge:
    """
    Bridges websocket data to a redpanda/kafka stream
    Times are in seconds
    """
    WEBSOCKET_TIMEOUT = 5
    STATUS_UPDATE_DELAY = 5
    SYMBOL_REFRESH_DELAY = 60

    def __init__(
            self,
            symbol_type: SymbolType,
            url: str,
            subscriptions: set[str] | None = None,
            api_key: str | None = None,
            api_secret: str | None = None,
    ):
        self.symbol_type = symbol_type
        self.url = url
        self.subscriptions = subscriptions
        self.api_key = api_key
        self.api_secret = api_secret
        self.exchange_id = None

        self.symbolrecs = SymbolRec.objects.filter(symbol_type=self.symbol_type)
        self.symbols = set(self.symbolrecs.values_list('symbol', flat=True))
        self.last_symbol_refresh = timezone.now()

        self.last_status_update = timezone.now()
        self.total_messages = 0
        self.messages_per_period = 0
        self.started_at = timezone.now()
        self.websocket_disconnects = 0

        self._set_producer()

    def _set_producer(self):
        conf = {
            'bootstrap.servers': ','.join(settings.REDPANDA_BROKERS),
            'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
            'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
            'sasl.username': settings.REDPANDA_USERNAME,
            'sasl.password': settings.REDPANDA_PASSWORD,
        }
        self.producer = Producer(**conf)

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            log.debug(f'%% Message failed delivery: {err}')
        else:
            log.debug(f'%% Message delivered to {msg.topic()} [{msg.partition()}')

    async def msg_to_broker(self, topic: str, value: str | dict, key: str = None) -> None:
        json_value = orjson.dumps(value)
        self.producer.produce(topic, key=key, value=json_value, callback=None)
        self.producer.poll(0)
        log.debug(f'sent message to stream: {value}')

    def flush_stream(self):
        log.info('flushing stream..')
        self.producer.flush(10)

    @staticmethod
    async def recv_with_timeout(ws, timeout: int = WEBSOCKET_TIMEOUT):
        try:
            message = await asyncio.wait_for(ws.recv(), timeout)
            return message
        except asyncio.TimeoutError:
            return None

    async def _authenticate(self, ws: websockets.WebSocketClientProtocol):
        ...

    async def connect(self):
        async with websockets.connect(self.url) as ws:
            await self._authenticate(ws)
            await self.subscribe(ws)
            while True:
                try:
                    if msg := await self.recv_with_timeout(ws):
                        self.total_messages += 1
                        self.messages_per_period += 1
                        await self.handle_message(msg)
                    await self.status_update()
                    await self.check_symbol_refresh(ws)
                except websockets.ConnectionClosed:
                    log.info("connection closed.")
                    self.websocket_disconnects += 1
                    break
                except (OperationalError, InterfaceError) as e:
                    log.error(f'database error: {e}')
                    await asyncio.sleep(5)
                    continue
                # except Exception as e:
                #     log.error(f'unexpected error: {e}')
                #     continue
            log.info(f'reconnecting..')
            await asyncio.sleep(2)
            await self.connect()

    def _build_subscriptions(self, lowercase: bool = False) -> set[str]:
        """
        Build a set of subscriptions from the subscriptions list.
        lowercase: bool - if True, will lowercase all symbols
        """
        subscriptions = set()
        for subscription in self.subscriptions:
            if '{}' in subscription:
                for symbol in self.symbols:
                    if lowercase:
                        symbol = symbol.lower()
                    subscriptions.add(subscription.format(symbol))
            else:
                subscriptions.add(subscription)
        return subscriptions

    async def subscribe(self, ws: websockets.WebSocketClientProtocol):
        ...

    async def unsubscribe(self, ws: websockets.WebSocketClientProtocol):
        ...

    async def handle_message(self, msg):
        ...

    async def status_update(self):
        if self.last_status_update < timezone.now() - timedelta(seconds=self.STATUS_UPDATE_DELAY):
            current_time = timezone.now()
            uptime_seconds = (current_time - self.started_at).total_seconds()
            uptime_days = uptime_seconds / 60 / 60 / 24
            msgs_per_sec = self.messages_per_period / (current_time - self.last_status_update).total_seconds()
            started_human = self.started_at.strftime('%Y-%m-%d %H:%M:%S')
            log.info(
                f'{self.exchange_id}: {self.total_messages} msgs '
                f'({self.messages_per_period} msgs/period, {msgs_per_sec:.2f} sec/avg) | '
                f'WSD: {self.websocket_disconnects} | '
                f'FROM: {started_human} | UP: {uptime_days:.2f} days'
             )
            self.last_status_update = timezone.now()
            self.messages_per_period = 0

    async def check_symbol_refresh(self, ws):
        if self.last_symbol_refresh < timezone.now() - timedelta(seconds=self.SYMBOL_REFRESH_DELAY):
            symbols_added, symbols_removed = await self.refresh_symbols()

            if symbols_added:
                await self.subscribe(ws)
                log.debug(f'{self.exchange_id}: symbols added: {symbols_added}')

            if symbols_removed:
                await self.unsubscribe(ws)
                log.debug(f'{self.exchange_id}: symbols removed: {symbols_removed}')

    @sync_to_async
    def refresh_symbols(self):
        try:
            self.symbolrecs = SymbolRec.objects.filter(symbol_type=self.symbol_type)
        except (OperationalError, InterfaceError) as e:
            log.error(f'database error: {e}')
            return set(), set()

        original_symbols = self.symbols
        self.symbols = set(self.symbolrecs.values_list('symbol', flat=True))

        symbols_added = self.symbols - original_symbols
        symbols_removed = original_symbols - self.symbols

        self.last_symbol_refresh = timezone.now()
        log.info(f'{self.exchange_id}: refreshed symbols. currently watching: {len(self.symbols)}')

        return symbols_added, symbols_removed
