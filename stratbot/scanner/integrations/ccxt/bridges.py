from __future__ import annotations
import logging
from datetime import datetime

import pandas as pd

from stratbot.scanner.models.timeframes import Timeframe


class CcxtBridge:
    INTERVALS = {
        '1': '1m',
        '5': '5m',
        'D': '1d',
    }

    def __init__(self, client):
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
        Fetch historical data from a Ccxt source. Historical candles include the most recent unclosed candle
        :param symbol:
        :param start_date:
        :param timeframe:
        :return: pd.DataFrame
        """
        start_timestamp = int(start_date.timestamp()) * 1000
        all_aggs = []
        while start_timestamp < self.client.milliseconds():
            agg = self.client.fetch_ohlcv(symbol, self.INTERVALS[timeframe], since=start_timestamp)
            if len(agg):
                start_timestamp = agg[-1][0] + 1
                all_aggs.extend(agg)
            else:
                break
        logging.info(f'{symbol} [{timeframe} {timeframe.unit}] queried, {len(all_aggs)} records')
        return self._to_df(symbol, all_aggs)

    def markets(self, symbols: list = None) -> dict:
        return self.client.fetch_tickers(symbols)


class AsyncCcxtBridge:
    def __init__(self, client):
        self.client = client


# ccxt_bridge = CcxtBridge(client)
# async_ccxt_bridge = AsyncCcxtBridge(async_client)
