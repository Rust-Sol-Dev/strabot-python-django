import asyncio
import json
import logging
from datetime import datetime
from itertools import islice

import pandas as pd
import pytz
import websockets
from alpaca.data import StockSnapshotRequest, Snapshot, StockBarsRequest
from alpaca.data.historical import StockHistoricalDataClient
from orjson import orjson

from stratbot.scanner.models.symbols import SymbolType
from . import exchange
from .clients import client
from stratbot.scanner.integrations.websockets_bridge import WebsocketBridge
from stratbot.scanner.models.timeframes import Timeframe, TIMEFRAMES_INTRADAY
from stratbot.scanner.ops.candles.metrics import filter_premarket


log = logging.getLogger(__name__)


class AlpacaBridge:
    def __init__(self, client: StockHistoricalDataClient):
        self.client = client
        self.exchange_id = exchange.ID
        self.session = None

    def historical(
            self,
            symbols: list[str] | str,
            tf: Timeframe,
            start_date: datetime,
            end_date: datetime = None,
    ) -> dict[str, pd.DataFrame] | pd.DataFrame:

        end_date = end_date or datetime.now(pytz.UTC)
        request_params = StockBarsRequest(
                                symbol_or_symbols=symbols,
                                timeframe=exchange.INTERVALS[tf],
                                start=start_date,
                                end=end_date,
                         )
        try:
            bars_df = self.client.get_stock_bars(request_params).df
        except (AttributeError, KeyError) as e:
            log.error(f'error fetching bars: {e}')
            return {}

        dfs = {}
        for symbol, df in bars_df.groupby(level=0):
            df.reset_index('symbol', inplace=True)
            df.index.name = 'time'
            if tf in TIMEFRAMES_INTRADAY:
                df = filter_premarket(df)
            df = df.rename(columns={'trade_count': 'transactions'})
            logging.debug(f'{symbol} [{tf}]: {len(df)} bars')
            dfs[str(symbol)] = df

        if isinstance(symbols, str):
            return dfs[symbols]
        return dfs

    def snapshot(self, symbols: list[str] | str) -> dict[str, Snapshot]:
        request_params = StockSnapshotRequest(
            symbol_or_symbols=symbols,
            feed='sip'
        )
        snapshot = self.client.get_stock_snapshot(request_params)
        return snapshot


class AlpacaWebsocketBridge(WebsocketBridge):
    def __init__(self, url: str, api_key: str, api_secret: str, subscriptions: set[str] | dict[str, str] | None = None):
        super().__init__(
            SymbolType.STOCK,
            url,
            subscriptions,
            api_key,
            api_secret,
        )
        self.exchange_id = exchange.ID

    async def _authenticate(self, ws):
        await ws.send(json.dumps({"action": "auth", "key": self.api_key, 'secret': self.api_secret}))

    @staticmethod
    def chunks(iterable):
        """Yield successive N-sized chunks from iterable."""
        it = iter(iterable)
        while chunk := list(islice(it, 250)):
            yield chunk

    async def subscribe(self, ws: websockets.WebSocketClientProtocol):
        # {"action":"subscribe","trades":["AAPL"],"quotes":["AMD","CLDR"],"bars":["*"]}
        for sub in ['bars', 'quotes', 'trades']:
            for chunk in self.chunks(self.symbols):
                payload = {
                    "action": "subscribe",
                    sub: chunk,
                }
                await ws.send(json.dumps(payload))
                await asyncio.sleep(0.5)
        # payload = {
        #     "action": "subscribe",
        #     'trades': ['*'],
        # }
        # await ws.send(json.dumps(payload))
        # await asyncio.sleep(0.5)

    async def handle_message(self, msg):
        msgs = orjson.loads(msg)
        if not isinstance(msgs, list):
            msgs = [msgs]

        for message in msgs:
            log.debug(message)
            ev = message.get('T')
            symbol = message.get('S')
            match ev:
                case 't':
                    topic = f'{self.exchange_id}.trades'
                    await self.msg_to_broker(topic, key=symbol, value=message)
                case 'q':
                    topic = f'{self.exchange_id}.quotes'
                    await self.msg_to_broker(topic, key=symbol, value=message)
                case 'b':
                    topic = f'{self.exchange_id}.bars'
                    await self.msg_to_broker(topic, key=symbol, value=message)


alpaca_bridge = AlpacaBridge(client)
