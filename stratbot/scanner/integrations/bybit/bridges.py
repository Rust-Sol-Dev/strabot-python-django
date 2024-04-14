from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from time import perf_counter

import pandas as pd
import requests
from ccxt import bybit

from .clients import client, async_client
from stratbot.scanner.models.timeframes import Timeframe


logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


INTERVALS = {
    '1': '1m',
    '5': '5m',
    'D': '1d',
}


class BybitBridge:
    def __init__(self, client: bybit):
        super().__init__()
        self.client = client

    @staticmethod
    def _to_df(symbol: str, agg: list) -> pd.DataFrame:
        df = pd.DataFrame(data=agg)
        df.rename(columns={0: 'time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
        df['symbol'] = symbol
        df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)
        df.set_index('time', inplace=True)
        # df.sort_index(inplace=True)
        # df.drop(columns=[], inplace=True)
        return df

    def historical(self, symbol: str, start_date: datetime, timeframe: Timeframe = None) -> pd.DataFrame:
        """
        Fetch historical data from Bybit. Historical candles from Bybit include the most recent unclosed candle
        :param symbol:
        :param start_date:
        :param timeframe:
        :return: pd.DataFrame
        """
        start_timestamp = int(start_date.timestamp()) * 1000
        all_aggs = []
        while start_timestamp < self.client.milliseconds():
            agg = self.client.fetch_ohlcv(symbol, INTERVALS[timeframe], since=start_timestamp)
            if len(agg):
                start_timestamp = agg[-1][0] + 1
                all_aggs.extend(agg)
            else:
                break
        logging.info(f'Bybit: {symbol} [{timeframe} {timeframe.unit}] queried, {len(all_aggs)} records')
        return self._to_df(symbol, all_aggs)

    def historical_new(self, symbol: str, start_date: datetime, timeframe: Timeframe) -> [str, pd.DataFrame]:
        """Fetch historical data from Bybit. Historical candles from Bybit include the most recent unclosed candle"""
        start_timestamp = int(start_date.timestamp()) * 1000
        all_aggs = []
        while start_timestamp < self.client.milliseconds():
            proxies = {
                'http': 'http://165.227.33.133:3128',
                'https': 'http://165.227.33.133:3128',
            }
            response = requests.get(
                f'https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={timeframe}',
                proxies=proxies,
            )
            agg = response.json()
            try:
                agg = agg['result']['list']
                for row in agg:
                    timestamp = int(int(row[0]) / 1000)
                    open_ = float(row[1])
                    high = float(row[2])
                    low = float(row[3])
                    close = float(row[4])
                    volume = float(row[5])
                    turnover = float(row[6])
                    all_aggs.append([timestamp, open_, high, low, close, volume, turnover])
                start_timestamp = all_aggs[-1][0] + 1
            except (KeyError, IndexError):
                break
        logging.info(f'Bybit: {symbol} [{timeframe} {timeframe.unit}] queried, {len(all_aggs)} records')
        return symbol, self._to_df(symbol, all_aggs)

    def markets(self, symbols: list = None) -> dict:
        return self.client.fetch_tickers(symbols)

    def markets_by_volume(self, min_usd: int = 25_000_000) -> list:
        excludes = ['USDT/USD', 'USDT-PERP']
        symbols = []
        for _, data in self.markets().items():
            symbol = data['info']['symbol']
            if float(data['info']['turnover24h']) > min_usd and symbol not in excludes and symbol.endswith('USDT'):
                symbols.append(symbol)
        return symbols


class AsyncBybitBridge:
    def __init__(self, client: bybit):
        self.client = client

    @staticmethod
    def _to_df(symbol: str, agg: list) -> pd.DataFrame:
        df = pd.DataFrame(data=agg)
        df.rename(columns={0: 'time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
        df['symbol'] = symbol
        df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)
        df.set_index('time', inplace=True)
        # df.sort_index(inplace=True)
        # df.drop(columns=[], inplace=True)
        return df

    async def historical(self, symbol: str, start_date: datetime, timeframe: Timeframe) -> [str, pd.DataFrame]:
        """Fetch historical data from Bybit. Historical candles from Bybit include the most recent unclosed candle"""
        start_timestamp = int(start_date.timestamp()) * 1000
        all_aggs = []
        while start_timestamp < self.client.milliseconds():
            client.verbose = True
            agg = await self.client.fetch_ohlcv(symbol, INTERVALS[timeframe], since=start_timestamp)
            if len(agg):
                start_timestamp = agg[-1][0] + 1
                all_aggs.extend(agg)
            else:
                break
        logging.info(f'Bybit: {symbol} [{timeframe} {timeframe.unit}] queried, {len(all_aggs)} records')
        return symbol, self._to_df(symbol, all_aggs)

    async def historical_new(self, symbol: str, start_date: datetime, timeframe: Timeframe) -> [str, pd.DataFrame]:
        """Fetch historical data from Bybit. Historical candles from Bybit include the most recent unclosed candle"""
        start_timestamp = int(start_date.timestamp()) * 1000
        all_aggs = []
        while start_timestamp < self.client.milliseconds():
            client.verbose = True
            # agg = await self.client.fetch_ohlcv(symbol, INTERVALS[timeframe], since=start_timestamp)
            proxies = {
                'http': 'http://165.227.33.133:3128',
                'https': 'http://165.227.33.133:3128',
            }
            response = requests.get(
                f'https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={timeframe}',
                proxies=proxies,
            )
            agg = response.json()
            try:
                agg = agg['result']['list']
                for row in agg:
                    timestamp = int(int(row[0]) / 1000)
                    open_ = float(row[1])
                    high = float(row[2])
                    low = float(row[3])
                    close = float(row[4])
                    volume = float(row[5])
                    turnover = float(row[6])
                    all_aggs.append([timestamp, open_, high, low, close, volume, turnover])
                start_timestamp = all_aggs[-1][0] + 1
            except (KeyError, IndexError):
                break
        logging.info(f'Bybit: {symbol} [{timeframe} {timeframe.unit}] queried, {len(all_aggs)} records')
        return symbol, self._to_df(symbol, all_aggs)

    async def bulk_query_historical(self, symbols: set, timeframe: Timeframe):
        s = perf_counter()
        if timeframe == Timeframe.DAYS_1:
            start_date = datetime(2020, 1, 1)
        else:
            # TODO: change to 3 after testing
            start_date = datetime.now() - timedelta(days=1)
        tasks = []
        for symbol in symbols:
            tasks.append(self.historical_new(symbol, start_date=start_date, timeframe=timeframe))
        results = await asyncio.gather(*tasks)
        elapsed = perf_counter() - s
        # await self.client.close()
        logger.info(f'Bybit: {len(symbols)} symbols queried in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        return results

    def markets(self, symbols: list = None) -> dict:
        # return self.client.fetch_tickers(symbols)
        proxies = {
            'http': 'http://165.227.33.133:3128',
            'https': 'http://165.227.33.133:3128',
        }
        response = requests.get(' https://api.bybit.com/v5/market/instruments-info?category=spot', proxies=proxies)
        return response.json()

def markets_by_volume(self, min_usd: int = 25_000_000) -> list:
        excludes = ['USDT/USD', 'USDT-PERP']
        symbols = []
        for _, data in self.markets().items():
            symbol = data['info']['symbol']
            if float(data['info']['turnover24h']) > min_usd and symbol not in excludes and symbol.endswith('USDT'):
                symbols.append(symbol)
        return symbols


bybit_bridge = BybitBridge(client)
# async_bybit_bridge = AsyncBybitBridge(client)
async_bybit_bridge = AsyncBybitBridge(async_client)
