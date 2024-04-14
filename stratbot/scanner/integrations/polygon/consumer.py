import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from stratbot.scanner.integrations.consumer import BaseConsumer
from stratbot.scanner.models.symbols import SymbolType
from .exchange import ID as EXCHANGE_ID

log = logging.getLogger(__name__)


class PolygonQuoteConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            SymbolType.STOCK,
            EXCHANGE_ID,
            f'{EXCHANGE_ID}.FMV',
            f'polygon-quote-consumer',
        )
        self.last_update = datetime.now(tz=timezone.utc)

    async def process_message(self, symbol, msg):
        quote = msg.get('fmv')
        self.quotes[symbol] = quote
        if datetime.now() - self.last_update > timedelta(seconds=1):
            await self._bulk_update_quotes()
            self.last_update = datetime.utcnow()


class PolygonCandleConsumer(BaseConsumer):
    """
    TODO: remove this? polygon has suggested using snapshots instead of websockets for minute candles
    """
    def __init__(self):
        super().__init__(
            SymbolType.STOCK,
            EXCHANGE_ID,
            f'{EXCHANGE_ID}.AM',
            'polygon-candle-consumer',
        )

    async def process_message(self, symbol, msg):
        log.debug(f'{symbol}: {msg}')
        if bar := await self.handle_tf_aggregate(msg):
            historical_bar = self._convert_to_historial_format(bar)
            await self.bar_to_timescale(historical_bar, self.symbol_type)

    @staticmethod
    async def handle_tf_aggregate(msg) -> OrderedDict:
        """https://polygon.io/docs/stocks/ws_stocks_am"""
        symbol = msg.get('sym')
        s_timestamp = msg.get('s')
        e_timestamp = msg.get('e')
        open_ = msg.get('o')
        high = msg.get('h')
        low = msg.get('l')
        close = msg.get('c')
        vol = msg.get('v')
        accumulated_vol = msg.get('av', 0)
        opening_price = msg.get('op', 0)
        vwap = msg.get('vw', 0)
        vwap_today = msg.get('a', 0)
        avg_trade_size = msg.get('z', 0)

        bar = OrderedDict([
            ('s', int(s_timestamp)),
            ('e', int(e_timestamp)),
            ('o', float(open_)),
            ('h', float(high)),
            ('l', float(low)),
            ('c', float(close)),
            ('v', int(vol)),
            ('av', int(accumulated_vol)),
            ('op', float(opening_price)),
            ('vw', float(vwap)),
            ('a', float(vwap_today)),
            ('z', int(avg_trade_size)),
            ('sym', symbol),
        ])
        return bar

    @staticmethod
    def _convert_to_historial_format(bar) -> OrderedDict:
        return OrderedDict([
            ('timestamp', bar['s']),
            ('open', bar['o']),
            ('high', bar['h']),
            ('low', bar['l']),
            ('close', bar['c']),
            ('volume', bar['v']),
            ('vwap', bar['vw']),
            ('symbol', bar['sym']),
        ])
