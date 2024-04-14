import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from time import perf_counter

import pandas as pd

from stratbot.scanner.integrations.consumer import BaseConsumer
from stratbot.scanner.models.symbols import SymbolType
from .exchange import ID as EXCHANGE_ID


log = logging.getLogger(__name__)


class BinanceQuoteConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            SymbolType.CRYPTO,
            EXCHANGE_ID,
            'BINANCE.markPriceUpdate',
            f'binance-quote-consumer',
        )
        self.last_update = datetime.now(tz=timezone.utc)

    async def process_message(self, symbol, msg):
        quote = msg.get('p')
        self.quotes[symbol] = quote
        if datetime.now(tz=timezone.utc) - self.last_update > timedelta(seconds=1):
            await self._bulk_update_quotes()
            self.last_update = datetime.now(tz=timezone.utc)


class BinanceCandleConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            SymbolType.CRYPTO,
            EXCHANGE_ID,
            'BINANCE.continuous_kline1m',
            'binance-candle-consumer',
        )

    async def process_message(self, key, msg):
        if bar := await self.handle_tf_aggregate(msg):
            if bar.get('kline_closed'):
                historical_bar = self._to_historical_format(bar)
                await self.bar_to_timescale(historical_bar, self.symbol_type)
                # self.bar_queue.append(bar)
        # if datetime.now() - self.last_update > timedelta(seconds=1):
        #     await self._bulk_update_db()
        #     self.last_update = datetime.utcnow()

    @staticmethod
    async def handle_tf_aggregate(msg) -> OrderedDict:
        symbol = msg.get('ps')
        event_timestamp = msg.get('E')
        kline_closed = msg.get('x')
        contract_type = msg.get('ct')
        s_timestamp = msg.get('t')
        e_timestamp = msg.get('T')
        interval = msg.get('i')
        open_ = msg.get('o')
        high = msg.get('h')
        low = msg.get('l')
        close = msg.get('c')
        volume = msg.get('v')
        trades = msg.get('n')
        quote_volume = msg.get('q')
        taker_buy_volume = msg.get('V')
        taker_buy_quote_volume = msg.get('Q')

        bar = OrderedDict([
            ('event_timestamp', int(event_timestamp)),
            ('timestamp', int(int(s_timestamp))),
            ('open', float(open_)),
            ('high', float(high)),
            ('low', float(low)),
            ('close', float(close)),
            ('volume', float(quote_volume)),
            ('contract_type', contract_type),
            ('symbol', symbol),
            ('kline_closed', bool(kline_closed)),
        ])
        return bar

    @staticmethod
    def _to_historical_format(bar) -> OrderedDict:
        return OrderedDict([
            ('timestamp', bar['timestamp']),
            ('open', bar['open']),
            ('high', bar['high']),
            ('low', bar['low']),
            ('close', bar['close']),
            ('volume', bar['volume']),
            ('symbol', bar['symbol']),
            ('contract_type', bar['contract_type']),
        ])
