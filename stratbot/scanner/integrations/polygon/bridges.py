from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime, timedelta
from time import perf_counter

import aiohttp
import pandas as pd
import polygon.exceptions
from django.conf import settings
from orjson import orjson
from polygon import RESTClient
from polygon.rest.models.aggs import Agg
from urllib3 import HTTPResponse

from stratbot.scanner.models.timeframes import TIMEFRAMES_INTRADAY
from stratbot.scanner.models.symbols import SymbolType
from stratbot.scanner.models.timeframes import Timeframe
from stratbot.scanner.ops.candles.metrics import filter_premarket
from . import exchange
from .clients import client
from ..websockets_bridge import WebsocketBridge


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class PolygonBridge:
    def __init__(self, client: RESTClient):
        self.client = client
        self.exchange_id = exchange.ID
        self.session = None

    def _agg_to_df(self, symbol: str, agg: list[Agg], timeframe: Timeframe) -> pd.DataFrame:
        """Convert the Polygon REST API response to a Pandas DataFrame"""
        # if not agg:
        #     return pd.DataFrame()
        drop_columns = ['otc']
        df = pd.DataFrame(data=agg)
        df['symbol'] = symbol
        # df['exchange'] = self.exchange_id
        df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('time', inplace=True)
        if timeframe in TIMEFRAMES_INTRADAY:
            df = filter_premarket(df)
        df.drop(columns=drop_columns, inplace=True)
        return df

    def _json_to_df(self, symbol: str, json_data: dict[str, any], timeframe: Timeframe) -> pd.DataFrame:
        try:
            df = pd.DataFrame(data=json_data['results'])
        except KeyError:
            return pd.DataFrame()
        df = df[['t', 'o', 'h', 'l', 'c', 'v', 'vw']]
        df['symbol'] = symbol
        df['exchange'] = self.exchange_id
        df['time'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        df.set_index('time', inplace=True)
        df['o'] = df['o'].astype(float)
        df['h'] = df['h'].astype(float)
        df['l'] = df['l'].astype(float)
        df['c'] = df['c'].astype(float)
        df['v'] = df['v'].astype(int)
        df['vw'] = df['vw'].astype(float)

        if timeframe in TIMEFRAMES_INTRADAY:
            df = filter_premarket(df)

        df.rename(
            columns={
                't': 'timestamp',
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'vw': 'vwap'
            },
            inplace=True
        )
        return df

    def historical(
            self,
            symbol: str,
            tf: Timeframe,
            start_date: datetime,
            end_date: datetime = None,
    ) -> pd.DataFrame:
        multiplier, timespan = exchange.INTERVALS[tf]
        end_date = end_date if end_date else datetime.utcnow()
        s = perf_counter()
        try:
            agg = self.client.get_aggs(
                symbol,
                multiplier,
                timespan,
                from_=start_date,
                to=end_date,
                limit=50000,
            )
        except (polygon.exceptions.AuthError, polygon.exceptions.BadResponse) as e:
            log.error(f'polygon.io exception [{tf=}]: {symbol} - {e}')
            return pd.DataFrame()
        elapsed = perf_counter() - s
        log.info(f'polygon.io: {symbol} [{tf}] queried, {len(agg)} records in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        df = self._agg_to_df(symbol, agg, timeframe=tf)
        return df

    def snapshot(self):
        resp: HTTPResponse = self.client.get_snapshot_all('stocks', raw=True)
        return orjson.loads(resp.data)


class AsyncPolygonBridge(PolygonBridge):
    """polygon.io API: https://polygon.io/docs/stocks/getting-started"""
    BASE_URL = 'https://api.polygon.io'

    def __init__(self, client: RESTClient, api_key: str):
        super().__init__(client)
        self.api_key = api_key
        self.session = None
        self.exchange_id = exchange.ID

    async def historical(
            self,
            symbol: str,
            tf: Timeframe,
            start_date: datetime,
            end_date: datetime = None,
    ) -> tuple:
        s = perf_counter()
        multiplier, timespan = exchange.INTERVALS[tf]
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date or datetime.utcnow().strftime('%Y-%m-%d')
        url = f'{self.BASE_URL}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start_date}/{end_date}?apiKey={self.api_key}&limit=50000'
        log.debug(f'{symbol}: {url}')
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        async with self.session.get(url) as response:
            response.raise_for_status()
            json_response = await response.json()
        elapsed = perf_counter() - s
        log.info(f'polygon.io: {symbol} [{tf}] queried in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        parsed_df = self._json_to_df(symbol, json_response, timeframe=tf)
        return symbol, parsed_df

    async def bulk_query_historical(self, symbols: set, timeframe: Timeframe):
        start_dates = {
            Timeframe.MINUTES_1: datetime.now() - timedelta(days=7),
            Timeframe.DAYS_1: datetime.now() - timedelta(days=365 * 5)
        }
        s = perf_counter()
        try:
            start_date = start_dates[timeframe]
        except KeyError as e:
            raise ValueError(f'Invalid timeframe: {e}')
        tasks = []
        self.session = aiohttp.ClientSession()
        for symbol in symbols:
            tasks.append(self.historical(symbol, start_date=start_date, tf=timeframe))
        results = await asyncio.gather(*tasks)
        await self.session.close()
        elapsed = perf_counter() - s
        log.info(f'polygon.io: {len(symbols)} symbols queried in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        return results


class PolygonWebsocketBridge(WebsocketBridge):
    """
    https://polygon.io/docs/websockets/getting-started
    """
    def __init__(self, url: str, api_key: str, subscriptions: set[str]):
        super().__init__(
            SymbolType.STOCK,
            url,
            subscriptions,
            api_key,
        )
        self.exchange_id = exchange.ID

    async def _authenticate(self, ws):
        await ws.send(json.dumps({"action": "auth", "params": self.api_key}))
        await asyncio.sleep(1)

    async def subscribe(self, ws):
        subscriptions = self._build_subscriptions()
        payload = {
            "action": "subscribe",
            "params": " ".join(subscriptions)
        }
        await ws.send(json.dumps(payload))

    async def handle_message(self, msg):
        messages = orjson.loads(msg)
        for msg in messages:
            log.debug(msg)
            ev = msg.get('ev')
            symbol = msg.get('sym')
            match ev:
                case 'status':
                    status = msg.get('status')
                    msg = msg.get('message')
                    log.info(f"{status}: {msg}")
                    continue
                case _:
                    topic = f'{self.exchange_id}.{ev}'
            await self.msg_to_broker(topic, key=symbol, value=msg)


polygon_bridge = PolygonBridge(client)
async_polygon_bridge = AsyncPolygonBridge(client, settings.POLYGON_API_KEY)
