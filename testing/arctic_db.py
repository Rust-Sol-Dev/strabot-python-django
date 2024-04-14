from arcticdb import Arctic
from v1.engines import CryptoEngine, StockEngine
from time import perf_counter


# s = perf_counter()
# scanner = StockEngine(timeframes=['1', 'D'])
# scanner.load_pricerecs_for_arctic()
# elapsed = perf_counter() - s
# print(f'load pricerecs completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


ac = Arctic('s3://127.0.0.1:9000:arcticdb?access=sN5uVDagrqZ5f0OqrJEG&secret=SPcRt7hUCd6QQM2wrgKSrlTfZnLGq7w9ZmOYInlV')
lib = ac.get_library('stock', create_if_missing=True)

ac2 = Arctic('lmdb://data')
lib2 = ac2.get_library('stock', create_if_missing=True)


columns = ['symbol', 'open', 'high', 'low', 'close', 'volume']
all_dfs = {}
s = perf_counter()
# for symbol in scanner.pricerecs.keys():
for symbol in lib.list_symbols():
    # df_one = scanner.pricerecs[symbol]['1']
    # df_one = df_one[columns]

    # df_day = scanner.pricerecs[symbol]['D']
    # df_day = df_day[columns]

    # print(df_one.tail())
    # print(df_day.tail())

    # lib.write(f'{symbol}:1', df_one)
    # lib.write(f'{symbol}:D', df_day)
    # lib.write(f'{symbol}', df_one)

    #     # load from library
    df = lib.read(f'{symbol}').data

    #     df.reset_index(inplace=True)
    #     # df = stratify_df(df)
    #     print(df.tail())
    all_dfs[symbol] = df
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')