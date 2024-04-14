from time import perf_counter
import logging
import pickle

import redis
import pandas as pd

from stratbot.scanner.integrations.alpaca.bridges import alpaca_bridge
from stratbot.scanner.models.exchange_calendar import ExchangeCalendar
from stratbot.scanner.ops import historical
from stratbot.scanner.models.pricerecs import *
from stratbot.scanner.models.symbols import SymbolRec, Setup
from stratbot.scanner.ops.candles.metrics import stratify_df
from stratbot.scanner import tasks
from v1.engines import CryptoEngine, StockEngine
from stratbot.scanner.ops.candles.redis_timeseries import cryptocache, stockcache

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 25)
pd.set_option('display.expand_frame_repr', False)

symbolrec = SymbolRec.objects.get(symbol='ARKB')
spy = SymbolRec.objects.get(symbol='SPY')
btc = SymbolRec.objects.get(symbol='BTCUSDT')

r = redis.Redis(host='127.0.0.1', db=0, port=6379)

s = perf_counter()
for symbolrec in SymbolRec.objects.filter(symbol_type='crypto'):
    # df_one = pd.read_parquet(f'data/{symbolrec.symbol_type}/{symbolrec.symbol}_1.parquet')
    # df_daily = pd.read_parquet(f'data/{symbolrec.symbol_type}/{symbolrec.symbol}_D.parquet')

    # df_one = symbolrec.one.copy().tail(5000)
    # df_daily = symbolrec.daily.copy()
    # df.reset_index(inplace=True)
    # df['timestamp'] = df['time'].apply(lambda x: x.timestamp())
    # stockcache.write_df('stock', symbolrec.symbol, df)

    # df = stockcache.read_df('stock', symbolrec.symbol, '5')
    # print(df.tail())
    # s2 = perf_counter()
    # df_resampled = df.resample('60min').agg({'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'})
    # df_resampled = df.resample('D').agg({'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'})
    # elapsed2 = perf_counter() - s2
    # print(f'completed in {elapsed2 * 1000:.4f} ms ({elapsed2:.2f} s)')

    # serialized_df_one = pickle.dumps(df_one)
    # r.set(f'df:crypto:{symbolrec.symbol}:1', serialized_df_one)

    # serialized_df_daily = pickle.dumps(df_daily)
    # r.set(f'df:crypto:{symbolrec.symbol}:D', serialized_df_daily)

    s2 = perf_counter()
    df_bytes = r.get(f'df:crypto:{symbolrec.symbol}:15')
    df_retrieved = pickle.loads(df_bytes)
    elapsed2 = perf_counter() - s2
    print(f'completed in {elapsed2 * 1000:.4f} ms ({elapsed2:.2f} s)')
    print(df_retrieved)

elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
