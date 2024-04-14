import logging

import pandas as pd
from redis_timeseries_manager import RedisTimeseriesManager

from v1.perf import func_timer

logger = logging.getLogger(__name__)


# TODO: how to use the django connection?
# con = get_redis_connection("markets")
settings = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 10,
    'password': None,
}

one_hour = 60 * 60
one_day = 60 * 60 * 24
one_week = one_day * 7
one_month = one_day * 30
one_year = one_day * 365


class BaseTimeseriesCache(RedisTimeseriesManager):
    _name: str
    _lines: list[str]
    _timeframes: dict[str, dict[str, int]]

    @func_timer
    def write_df(self, symbol_type: str, symbol: str, df: pd.DataFrame):
        df['timestamp'] = df['timestamp'] / 1000
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        records = [[int(row[0]), row[1], row[2], row[3], row[4], row[5]] for row in df.values.tolist()]
        self.insert(
            c1=symbol_type,
            c2=symbol,
            data=records,
            create_inplace=True,
        )
        logger.debug(f'cached: {symbol} ({symbol_type})')

    @func_timer
    def read_df(self, symbol_type: str, symbol: str, timeframe: str = 'raw') -> pd.DataFrame:
        if timeframe == '1':
            timeframe = 'raw'
        df = self.read(c1=symbol_type, c2=symbol, timeframe=timeframe, return_as='df')[1]
        if df.empty:
            return df
        df['timestamp'] = df['time']
        df['time'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('time', inplace=True)
        return df


class StockTimeseriesCache(BaseTimeseriesCache):
    _name = 'alpaca'
    # _lines = ['s', 'e', 'o', 'h', 'l', 'c', 'v', 'av', 'op', 'vw', 'a', 'z', 'sym']
    # _lines = ['o', 'h', 'l', 'c', 'v', 'av', 'op', 'vw', 'a', 'z']
    _lines = ['o', 'h', 'l', 'c', 'v']
    _timeframes = {
        'raw': {'retention_secs': one_month},
        '1': {'retention_secs': one_week, 'bucket_size_secs': 60},
        '5': {'retention_secs': one_week, 'bucket_size_secs': 60 * 5},
        '15': {'retention_secs': one_week, 'bucket_size_secs': 60 * 15},
        '30': {'retention_secs': one_week, 'bucket_size_secs': 60 * 30},
        '60': {'retention_secs': one_week, 'bucket_size_secs': one_hour, 'align_timestamp': 60 * 30 * 1000},
        '4h': {'retention_secs': one_month, 'bucket_size_secs': one_hour * 4},
        'd': {'retention_secs': one_year * 5, 'bucket_size_secs': one_day},
        # 'w': {'retention_secs': one_year * 5, 'bucket_size_secs': one_week, 'align_timestamp': one_day * 3 * 1000},
    }

    def _create_rule(
            self, c1: str,
            c2: str,
            line: str,
            timeframe_name: str,
            timeframe_specs: dict,
            source_key: str,
            dest_key: str
    ):
        match line:
            case 'o': aggregation_type = 'first'
            case 'h': aggregation_type = 'max'
            case 'l': aggregation_type = 'min'
            case 'c': aggregation_type = 'last'
            case 'v': aggregation_type = 'sum'
            case _: return
        bucket_size_secs = timeframe_specs['bucket_size_secs']
        # align_timestamp = timeframe_specs['align_timestamp'] or 0
        # self._set_rule(source_key, dest_key, aggregation_type, bucket_size_secs, align_timestamp)
        self._set_rule(source_key, dest_key, aggregation_type, bucket_size_secs)


class CryptoTimeseriesCache(BaseTimeseriesCache):
    _name = 'crypto'
    _lines = ['o', 'h', 'l', 'c', 'v']
    _timeframes = {
        'raw': {'retention_secs': one_month},
        '1': {'retention_secs': one_week, 'bucket_size_secs': 60},
        '5': {'retention_secs': one_week, 'bucket_size_secs': 60 * 5},
        '15': {'retention_secs': one_week, 'bucket_size_secs': 60 * 15},
        '30': {'retention_secs': one_week, 'bucket_size_secs': 60 * 30},
        '60': {'retention_secs': one_week, 'bucket_size_secs': one_hour},
        '4h': {'retention_secs': one_month, 'bucket_size_secs': one_hour * 4},
        '6h': {'retention_secs': one_month, 'bucket_size_secs': one_hour * 6},
        '12h': {'retention_secs': one_month, 'bucket_size_secs': one_hour * 12},
        'd': {'retention_secs': one_year * 5, 'bucket_size_secs': one_day},
        'w': {'retention_secs': one_year * 5, 'bucket_size_secs': one_week, 'align_timestamp': one_day * 3 * 1000},
    }


stockcache = StockTimeseriesCache(**settings)
cryptocache = CryptoTimeseriesCache(**settings)
