import asyncio
import json
from datetime import datetime, timedelta
import logging
from time import perf_counter
import pytz

import pandas as pd
import websockets
from binance.um_futures import UMFutures
from orjson import orjson

from stratbot.scanner.integrations.websockets_bridge import WebsocketBridge
from stratbot.scanner.models.symbols import SymbolType
from stratbot.scanner.models.timeframes import Timeframe
from . import exchange
from .clients import client


log = logging.getLogger(__name__)


def build_datetime_index(
        start_date: datetime,
        end_date: datetime,
        timeframe: Timeframe,
        chunk_size: int = 1000
) -> list[tuple[datetime, datetime]]:

    if timeframe not in (Timeframe.MINUTES_1, Timeframe.DAYS_1):
        raise ValueError('Invalid timeframe')

    interval_ms = 60000 if timeframe == Timeframe.MINUTES_1 else 86400000
    interval_delta = timedelta(milliseconds=interval_ms)
    total_intervals = (end_date - start_date) // interval_delta
    chunks = []
    for i in range(0, total_intervals, chunk_size):
        chunk_start_date = start_date + (i * interval_delta)
        chunk_end_date = min(start_date + ((i + chunk_size) * interval_delta), end_date)
        chunks.append((chunk_start_date, chunk_end_date))
    return chunks


def aggs_to_df(symbol: str, aggs: list[list]) -> pd.DataFrame:
    if not aggs:
        return pd.DataFrame()
    df = pd.DataFrame(data=aggs)
    df.rename(columns={
        0: 'timestamp',
        1: 'open',
        2: 'high',
        3: 'low',
        4: 'close',
        5: 'volume'},
        inplace=True,
    )
    df['symbol'] = symbol
    df['exchange'] = exchange.ID
    df['contract_type'] = 'PERPETUAL'
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)
    return df


def kline_to_aggs(row):
    open_timestamp = int(int(row[0]))
    open_ = float(row[1])
    high = float(row[2])
    low = float(row[3])
    close = float(row[4])
    volume = float(row[5])
    close_timestamp = int(int(row[6]))
    quote_volume = float(row[7])
    num_trades = int(row[8])
    taker_buy_volume = float(row[9])
    taker_buy_quote_volume = float(row[10])

    return [
        open_timestamp,
        open_,
        high,
        low,
        close,
        quote_volume,
    ]


class BinanceBridge:
    def __init__(self, client: UMFutures):
        super().__init__()
        self.client = client
        self.exchange_id = exchange.ID

    def _fetch_aggs(
            self,
            symbol: str,
            timeframe: Timeframe,
            start_date_ms: int,
            end_date_ms: int
    ) -> list:

        klines = self.client.continuous_klines(
            symbol,
            'PERPETUAL',
            interval=exchange.INTERVALS[timeframe],
            startTime=start_date_ms,
            endTime=end_date_ms,
            limit=1000
        )
        aggs = [kline_to_aggs(row) for row in klines]
        return aggs

    def historical(
            self,
            symbol: str,
            tf: Timeframe,
            start_date: datetime,
            end_date: datetime = None
    ) -> pd.DataFrame:

        start_date = start_date.replace(tzinfo=pytz.utc)
        end_date = end_date or datetime.utcnow().replace(tzinfo=pytz.utc)
        end_date = end_date.replace(tzinfo=pytz.utc)
        chunks = build_datetime_index(start_date, end_date, tf)
        aggs = []
        for chunk in chunks:
            start_date_ms, end_date_ms = [int(dt.timestamp() * 1000) for dt in chunk]
            agg = self._fetch_aggs(symbol, tf, start_date_ms, end_date_ms)
            aggs.extend(agg)
        df = aggs_to_df(symbol, aggs)
        logging.info(f'Binance: {symbol} [{tf}] queried, {len(aggs)} records')
        return df


class AsyncBinanceBridge:
    def __init__(self, client: UMFutures):
        super().__init__()
        self.client = client
        self.exchange_id = exchange.ID

    async def _fetch_aggs(
            self,
            symbol: str,
            tf: Timeframe,
            start_date_ms: int,
            end_date_ms: int
    ) -> list:

        klines = self.client.continuous_klines(
            symbol,
            'PERPETUAL',
            interval=exchange.INTERVALS[tf],
            startTime=start_date_ms,
            endTime=end_date_ms,
            limit=1000
        )
        aggs = [kline_to_aggs(row) for row in klines]
        return aggs

    async def historical(
            self,
            symbol: str,
            tf: Timeframe,
            start_date: datetime,
            end_date: datetime = None
    ) -> tuple[str, pd.DataFrame]:

        start_date = start_date.replace(tzinfo=pytz.utc)
        end_date = end_date or datetime.utcnow().replace(tzinfo=pytz.utc)
        end_date = end_date.replace(tzinfo=pytz.utc)
        chunks = build_datetime_index(start_date, end_date, tf)
        aggs = []
        for chunk in chunks:
            start_date_ms, end_date_ms = [int(dt.timestamp() * 1000) for dt in chunk]
            agg = await self._fetch_aggs(symbol, tf, start_date_ms, end_date_ms)
            aggs.append(agg)

        #     tasks.append(self._query_historical(symbol, timeframe, start_date_ms, end_date_ms))
        # results = await asyncio.gather(*tasks)
        # aggs = [item for sublist in results for item in sublist]
        df = aggs_to_df(symbol, aggs)
        logging.info(f'Binance: {symbol} [{tf}] queried, {len(aggs)} records')
        return symbol, df

    async def bulk_query_historical(self, historical_map: list[tuple[str, Timeframe, datetime]]):
        s = perf_counter()
        tasks = []
        for symbol, timeframe, start_date in historical_map:
            tasks.append(self.historical(symbol, tf=timeframe, start_date=start_date))
        results = await asyncio.gather(*tasks)
        elapsed = perf_counter() - s
        log.info(f'Binance: {len(historical_map)} symbols queried in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        return results

    def quotes(self) -> dict:
        return {rec['symbol']: float(rec['price']) for rec in self.client.ticker_price()}

    def markets(self) -> dict:
        return self.client.ticker_24hr_price_change()

    def markets_by_volume(self, min_volume_usd: int = 25_000_000) -> list:
        excludes = ['USDTUSDT']
        symbols = []
        for rec in self.markets():
            symbol = rec['symbol']
            if float(rec['quoteVolume']) > min_volume_usd and symbol not in excludes and symbol.endswith('USDT'):
                symbols.append(symbol)
        return symbols


class BinanceWebsocketBridge(WebsocketBridge):
    def __init__(self, websocket_uri: str, subscriptions: set[str]):
        super().__init__(
            SymbolType.CRYPTO,
            url=websocket_uri,
            subscriptions=subscriptions,
        )
        self.exchange_id = exchange.ID

    async def subscribe(self, ws: websockets.WebSocketClientProtocol):
        subscriptions = self._build_subscriptions(lowercase=True)
        chunk_size = 200

        for i in range(0, len(subscriptions), chunk_size):
            chunk = list(subscriptions)[i:i + chunk_size]

            # wscat {"method": "SUBSCRIBE", "params": ["btcusdt_perpetual@continuousKline_1m"], "id": 1}
            payload = {
                "method": "SUBSCRIBE",
                "params": chunk,
                'id': 1
            }
            await ws.send(json.dumps(payload))
            await asyncio.sleep(0.5)

    async def unsubscribe(self, ws: websockets.WebSocketClientProtocol):
        # TODO: unsubscribe from symbols that are no longer needed
        pass

    async def _handle_kline(self, symbol, message):
        k = message.pop('k')
        interval = k.get('i')
        message.update(k)
        message['ts'] = message.get('t') / 1000

        topic = f'{self.exchange_id}.continuous_kline{interval}'
        await self.msg_to_broker(topic, key=symbol, value=message)

        if k.get('x'):
            topic = f'{self.exchange_id}.closed_kline{interval}'
            await self.msg_to_broker(topic, key=symbol, value=message)

    async def handle_message(self, msg):
        msgs = orjson.loads(msg)
        if not isinstance(msgs, list):
            msgs = [msgs]

        for message in msgs:
            log.debug(message)
            ev = message.get('e')
            match ev:
                case 'continuous_kline' | 'kline':
                    symbol = message.get('ps')
                    await self._handle_kline(symbol, message)
                case 'trade':
                    symbol = message.get('s')
                    topic = f'{self.exchange_id}.trade'
                    message['ts'] = message.get('T') / 1000
                    await self.msg_to_broker(topic, key=symbol, value=message)
                case 'aggTrade':
                    symbol = message.get('s')
                    topic = f'{self.exchange_id}.aggTrade'
                    await self.msg_to_broker(topic, key=symbol, value=message)
                case '24hrTicker':
                    symbol = message.get('s')
                    topic = f'{self.exchange_id}.24hrTicker'
                    await self.msg_to_broker(topic, key=symbol, value=message)
                case 'forceOrder':
                    o = message.get('o')
                    symbol = o.get('s')
                    topic = f'{self.exchange_id}.forceOrder'
                    await self.msg_to_broker(topic, key=symbol, value=message)
                case 'markPriceUpdate':
                    symbol = message.get('s')
                    topic = f'{self.exchange_id}.markPriceUpdate'
                    await self.msg_to_broker(topic, key=symbol, value=message)


binance_bridge = BinanceBridge(client)
async_binance_bridge = AsyncBinanceBridge(client)
