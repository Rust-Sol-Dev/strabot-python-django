from time import perf_counter

from arcticdb import Arctic, VersionedItem
from arcticdb.version_store.library import Library
import pandas as pd

from stratbot.scanner.models.symbols import SymbolType, SymbolRec
from stratbot.scanner.models.timeframes import Timeframe


s3_key = 'DO00GX36LP7VVAK9ULYK'
s3_secret = 'V/z7c0DuYZ+l4TUMJ9E+3b6E/dYeqRGDz+8hbyuUVSU'
ac = Arctic(f's3s://nyc3.digitaloceanspaces.com:arcticdb?access={s3_key}&secret={s3_secret}')


from dataflows.timeframe_ops import historical_resample
from time import perf_counter
from collections import defaultdict

pricerecs: dict[str, dict[str, pd.DataFrame]] = defaultdict(lambda: defaultdict(pd.DataFrame))


s = perf_counter()
all_dfs = {}
symbolrecs = SymbolRec.objects.filter(symbol_type='crypto')
for symbolrec in symbolrecs:
    symbol = symbolrec.symbol
    print(symbol)
    df = getattr(symbolrec, symbolrec.TF_MAP['1'])
    del df['id']
    all_dfs[symbol] = df
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

s = perf_counter()
for symbol, df in all_dfs.items():
    for tf in ['15', '30', '60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y']:
        new_df = historical_resample('crypto', tf, df)
        pricerecs[symbol][tf] = new_df.tail(2)
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


def populate_arcticdb(symbol_type: SymbolType, library: Library, tf: Timeframe = Timeframe.MINUTES_1):
    s = perf_counter()
    all_dfs = {}
    symbolrecs = SymbolRec.objects.filter(symbol_type=symbol_type)
    for symbolrec in symbolrecs:
        symbol = symbolrec.symbol
        df = getattr(symbolrec, symbolrec.TF_MAP[tf])
        all_dfs[symbol] = df.reset_index()
        # not needed if dataframes have not been "stratified"
        # df['strat_id'] = df['strat_id'].astype('str')
        # df['candle_shape'] = df['candle_shape'].astype('str')
        del df['id']
        # library.write(f'{symbol}-{tf}', df)
        # df.to_parquet(f'data/{symbol_type}/{symbol}-{tf}.parquet')
        # print(f'wrote {symbol} [{tf}]')
    elapsed = perf_counter() - s
    print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


def init_library(library: Library, symbol_type: SymbolType):
    s = perf_counter()
    symbols = SymbolRec.objects.filter(symbol_type=symbol_type).values_list('symbol', flat=True)
    symbols_with_tf = [symbol + '-1' for symbol in symbols]

    results = library.read_batch(symbols_with_tf)
    for result in results:
        df = result.data
        symbol, tf = result.symbol.split('-')
        # library.write(f'{symbol}-{tf}', df)
        pricerecs[symbol][tf] = df
    elapsed = perf_counter() - s
    print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

    library_symbol_count = len(library.list_symbols())
    print(f'{library_symbol_count} symbols loaded from library')




def bar_to_arcticdb(symbol__df):
    symbol, df = symbol__df

    global pricerecs
    existing_df = pricerecs[symbol]['1']

    s = perf_counter()
    # new_df = pd.concat(df)
    new_df = df

    # check for overlapping index values, if found, remove the old values, else merge
    overlap = existing_df.index.isin(new_df.index)
    if overlap.any():
        df = pd.concat([existing_df, new_df])
        df = df[~df.index.duplicated(keep='last')]
    else:
        df = pd.concat([existing_df, new_df])
    df.sort_index(inplace=True)

    pricerecs[symbol]['1'] = df
    last_timestamp[symbol] = df.index[-1].timestamp()

    # library.update(f'{symbol}-1', df)
    # mem_library.update(f'{symbol}-1', df, upsert=True)
    # mem_library.append(f'{symbol}-1', df)
    # mem_library
    elapsed = perf_counter() - s
    print(f'write timeseries bar for [{symbol}] in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

    for tf in ['60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y']:
        s = perf_counter()
        resampled_df = historical_resample(tf, df)
        pricerecs[symbol][tf] = resampled_df
        opens[symbol][tf] = resampled_df.iloc[-1]['open']
        elapsed = perf_counter() - s
        print(f'resampled {symbol} [{tf}] in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

    # return symbol, pricerecs[symbol]['D'].tail(1)
    return symbol, df.tail()
