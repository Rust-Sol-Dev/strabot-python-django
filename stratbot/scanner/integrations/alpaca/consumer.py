import logging
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta, timezone

from stratbot.scanner.integrations.consumer import BaseConsumer
from stratbot.scanner.models.symbols import SymbolRec, SymbolType
from .exchange import ID as EXCHANGE_ID


log = logging.getLogger(__name__)


def rfc3339_to_milliseconds(timestamp):
    dt = datetime.fromisoformat(timestamp)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return int((dt - epoch).total_seconds() * 1000)


class AlpacaQuoteConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            SymbolType.STOCK,
            EXCHANGE_ID,
            f'^{EXCHANGE_ID}.prices',
            f'alpaca-price-consumer',
        )
        self.symbolrecs = SymbolRec.objects.filter(symbol_type=self.symbol_type)
        self.quotes = defaultdict(float)
        self.last_update = datetime.now(tz=timezone.utc)

    async def process_message(self, symbol, msg):
        try:
            self.quotes[symbol] = msg
        except (KeyError, IndexError):
            return

        if datetime.now(tz=timezone.utc) - self.last_update > timedelta(seconds=1):
            await self._bulk_update_quotes()
            self.last_update = datetime.now(tz=timezone.utc)


class AlpacaCandleConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            SymbolType.STOCK,
            EXCHANGE_ID,
            f'^{EXCHANGE_ID}.bars',
            'alpaca-candle-consumer',
        )

    async def process_message(self, symbol, msg):
        log.debug(f'{symbol}: {msg}')

        if symbol not in self.symbols:
            return

        if bar := await self.handle_tf_aggregate(msg):
            historical_bar = self._convert_to_historial_format(bar)
            await self.bar_to_timescale(historical_bar, self.symbol_type)

    @staticmethod
    async def handle_tf_aggregate(msg) -> dict:
        symbol = msg.get('S')
        open_ = msg.get('o')
        high = msg.get('h')
        low = msg.get('l')
        close = msg.get('c')
        vol = msg.get('v')
        timestamp = msg.get('t')

        try:
            bar = {
                'S': symbol,
                'o': float(open_),
                'h': float(high),
                'l': float(low),
                'c': float(close),
                'v': int(vol),
                't': rfc3339_to_milliseconds(timestamp)
            }
        except (ValueError, TypeError):
            return {}
        return bar

    @staticmethod
    def _convert_to_historial_format(bar) -> OrderedDict:
        return OrderedDict([
            ('timestamp', bar['t']),
            ('open', bar['o']),
            ('high', bar['h']),
            ('low', bar['l']),
            ('close', bar['c']),
            ('volume', bar['v']),
            ('symbol', bar['S']),
        ])
