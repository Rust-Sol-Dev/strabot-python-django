from pathlib import Path

from arcticdb import Arctic
from time import perf_counter
from itertools import chain
from dateutil.relativedelta import relativedelta
import logging
import multiprocessing
import asyncio

from arcticdb import Arctic
from django.core.cache import cache
import pandas as pd
import redis

from stratbot.scanner.models.exchange_calendar import ExchangeCalendar
from stratbot.scanner.ops import historical
from stratbot.scanner.models.pricerecs import *
from stratbot.scanner.models.symbols import SymbolRec, Setup
# from stratbot.scanner.ops.candles import stratify_df
from stratbot.scanner import tasks
# from stratbot.scanner.ops.storage import from_cache, to_cache
from v1.engines import CryptoEngine, StockEngine

logger = logging.getLogger(__name__)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 25)
pd.set_option('display.expand_frame_repr', False)

tf = Timeframe.DAYS_1

ac = Arctic('s3://100.79.194.66:9000?access=qGZ4VaZ5zw6eJlnXyjIx&secret=2sEWrG4nqqM7bIOxAK512fY79rICxLS5aQ9CUDqd')
scanner = StockEngine(timeframes=[tf])
s = perf_counter()
scanner.load_pricerecs()
elapsed = perf_counter() - s
print(f'load pricerecs completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

s = perf_counter()
columns = ['open', 'high', 'low', 'close', 'volume', 'symbol']
lib = ac.get_library('stock', create_if_missing=True)
all_dfs = []
for symbol in scanner.pricerecs.keys():
    # df = scanner.pricerecs[symbol][tf]
    # df = df[columns]
    # lib.write(f'{symbol}:{tf}', df)

    # load from library
    df = lib.read(f'{symbol}:{tf}').data
    df.reset_index(inplace=True)
    # df = stratify_df(df)
    # print(df.tail())
    all_dfs.append(df)

merged_df = pd.concat(all_dfs, ignore_index=True)
pivot_df = merged_df.pivot(index='time', columns='symbol', values='close')
correlation_matrix = pivot_df.corr()

highly_correlated_pairs = []
threshold = 0.95

# Iterate over the correlation matrix and add stock pairs with high correlation to the list
for i in range(len(correlation_matrix.columns)):
    for j in range(i+1, len(correlation_matrix.columns)):  # Avoid self-comparison and duplicate pairs
        if correlation_matrix.iloc[i, j] > threshold:
            highly_correlated_pairs.append((correlation_matrix.columns[i], correlation_matrix.columns[j], correlation_matrix.iloc[i, j]))

# Print out the list of highly correlated stock pairs
for pair in highly_correlated_pairs:
    print(f"Stocks {pair[0]} and {pair[1]} have a high correlation of {pair[2]:.2f}")

elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

        # read from parquet, write to library
        # s2 = perf_counter()
        # parquet_file_path = f'data/{symbol}-{tf}.parquet'
        # df = pd.read_parquet(parquet_file_path)
        # lib.write(f'{symbol}:{tf}', df)
        # elapsed = perf_counter() - s2
        # print(f'read from parquet, store in library completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

        # read from parquet, write to redis
        # s2 = perf_counter()
        # parquet_file_path = f'data/{symbol}-{tf}.parquet'
        # df = pd.read_parquet(parquet_file_path)
        # cache.set(f'TEST-{symbol}:{tf}', df)
        # elapsed = perf_counter() - s2
        # print(f'read from parquet, write to redis completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

        # read from redis
        # s2 = perf_counter()
        # df = cache.get(f'TEST-{symbol}:{tf}')
        # elapsed = perf_counter() - s2
        # print(f'read from redis in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

        # read from library, write to parquet
        # s2 = perf_counter()
        # df = lib.read(f'{symbol}:{tf}').data
        # df.to_parquet(f'data/{symbol}-{tf}.parquet')
        # elapsed = perf_counter() - s2
        # print(f'single read/write completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


AGG_DEFAULTS = {
    # 'symbol': 'first',
    # 'exchange': 'first',
    'o': 'first',
    'h': 'max',
    'l': 'min',
    'c': 'last',
    'v': 'sum',
}

RESAMPLE_PERIODS = {
    Timeframe.MINUTES_5: '5min',
    Timeframe.MINUTES_15: '15min',
    Timeframe.MINUTES_30: '30min',
    Timeframe.MINUTES_60: '60min',
    Timeframe.HOURS_4: '4h',
    Timeframe.HOURS_6: '6h',
    Timeframe.HOURS_12: '12h',
    Timeframe.WEEKS_1: 'W',
    Timeframe.MONTHS_1: 'MS',
    Timeframe.QUARTERS_1: 'QS',
    Timeframe.YEARS_1: 'YS',
}

def historical_resample(symbol_type: str, timeframe: Timeframe, df: pd.DataFrame = None):
    match timeframe:
        case Timeframe.MINUTES_60:
            kwargs = {}
            if symbol_type == SymbolType.STOCK:
                kwargs = {'offset': '30min'}
            df = df.resample('60min', **kwargs).agg(AGG_DEFAULTS).dropna()
        case Timeframe.HOURS_4:
            if symbol_type == SymbolType.STOCK:
                df.index = df.index.tz_convert(MARKET_TIMEZONE)
                df = df.resample('4h', offset='30min').agg(AGG_DEFAULTS).dropna()
                df.index = df.index.tz_convert(pytz.UTC)
            else:
                df = df.resample('4h').agg(AGG_DEFAULTS).dropna()
        case Timeframe.DAYS_1:
            pass
        case Timeframe.WEEKS_1:
            df_weekly = df.copy()
            df_weekly.index = df_weekly.index - pd.to_timedelta(df_weekly.index.dayofweek, unit='D')
            df = df_weekly.resample('W-MON').agg(AGG_DEFAULTS).dropna()
        case _:
            df = df.resample(RESAMPLE_PERIODS[timeframe]).agg(AGG_DEFAULTS).dropna()
    return df


def parse_df(df: pd.DataFrame):
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df.rename(columns={
        'open': 'o',
        'high': 'h',
        'low': 'l',
        'close': 'c',
        'volume': 'v',
    }, inplace=True)
    return df


def create_parquet_files(symbol_type: SymbolType):
    symbolrecs = SymbolRec.objects.filter(symbol_type=symbol_type)
    for symbolrec in symbolrecs:
        print(f'processing {symbolrec.symbol}...')
        df = parse_df(symbolrec.one_db.tail(50000).copy())
        df.to_parquet(f'./data/{symbolrec.symbol_type}/{symbolrec.symbol}_1.parquet')
        df = parse_df(symbolrec.daily_db.copy())
        df.to_parquet(f'./data/{symbolrec.symbol_type}/{symbolrec.symbol}_D.parquet')


def backfill_from_parquet(symbol_type: SymbolType, tf: Timeframe):
    symbols = SymbolRec.objects.filter(symbol_type=symbol_type).values_list('symbol', flat=True)
    parquet_path = Path(f'./data/{symbol_type}')

    dfs = defaultdict(dict)
    for symbol in symbols:

        parquet_file_one = parquet_path / f'{symbol}_1.parquet'
        if parquet_file_one.exists():
            print('load file:', parquet_file_one)
            df = parse_df(pd.read_parquet(parquet_file_one))
            dfs[symbol]['1'] = df
            print(df.tail())
        else:
            print('file not found:', parquet_file_one)

        parquet_file_daily = parquet_path / 'crypto' / f'{symbol}_D.parquet'
        if parquet_file_daily.exists():
            print('load file:', parquet_file_daily)
            df = parse_df(pd.read_parquet(parquet_file_daily))
            print(df.tail())
            dfs[symbol]['D'] = df

        else:
            print('file not found:', parquet_file_daily)
    return dfs


def backfill_from_parquet_new(symbol: str, symbol_type: SymbolType, tf: Timeframe):
    parquet_file = Path(f'./data/{symbol_type}/{symbol}_{tf}.parquet')
    df = pd.DataFrame()
    if parquet_file.exists():
        print('load file:', parquet_file)
        df = pd.read_parquet(parquet_file)
        print(df.tail())
    else:
        print('file not found:', parquet_file)
    return df


def parquet_to_redis(symbol_type: SymbolType):
    symbols = SymbolRec.objects.filter(symbol_type=symbol_type).values_list('symbol', flat=True)
    for symbol in symbols:
        print(f'processing {symbol}...')
        dfs = initialize_symbol_dfs(symbol, symbol_type)
        for tf, df in dfs.items():
            serialized_df = pickle.dumps(df)
            r.set(f'df:{symbol_type}:{symbol}:{tf}', serialized_df)


def initialize_symbol_dfs(symbol: str, symbol_type: SymbolType):
    dfs = {}

    df = backfill_from_parquet_new(symbol, symbol_type, Timeframe.MINUTES_1)
    dfs[Timeframe.MINUTES_15] = historical_resample(symbol_type, Timeframe.MINUTES_15, df)
    dfs[Timeframe.MINUTES_30] = historical_resample(symbol_type, Timeframe.MINUTES_30, df)
    dfs[Timeframe.MINUTES_60] = historical_resample(symbol_type, Timeframe.MINUTES_60, df)
    dfs[Timeframe.HOURS_4] = historical_resample(symbol_type, Timeframe.HOURS_4, df)
    dfs[Timeframe.HOURS_6] = historical_resample(symbol_type, Timeframe.HOURS_6, df)
    dfs[Timeframe.HOURS_12] = historical_resample(symbol_type, Timeframe.HOURS_12, df)

    df = backfill_from_parquet_new(symbol, symbol_type, Timeframe.DAYS_1)
    dfs[Timeframe.DAYS_1] = df
    dfs[Timeframe.WEEKS_1] = historical_resample(symbol_type, Timeframe.WEEKS_1, df)
    dfs[Timeframe.MONTHS_1] = historical_resample(symbol_type, Timeframe.MONTHS_1, df)
    dfs[Timeframe.QUARTERS_1] = historical_resample(symbol_type, Timeframe.QUARTERS_1, df)
    dfs[Timeframe.YEARS_1] = historical_resample(symbol_type, Timeframe.YEARS_1, df)
    return dfs
