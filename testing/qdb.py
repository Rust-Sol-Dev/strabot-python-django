import sys
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
from django.conf import settings
django.setup()

from time import perf_counter
import requests
from io import StringIO

import psycopg2
import pandas as pd
from questdb.ingress import Sender, IngressError
from stratbot.scanner.models.symbols import SymbolRec
# from django.core.cache import caches

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 25)
pd.set_option('display.expand_frame_repr', False)


# cache = caches['markets']

def create_df_from_timescaledb(symbolrec: SymbolRec):
    df = symbolrec.one
    df.rename(
        columns={
            'time': 't',
            'symbol': 's',
            'open': 'o',
            'high': 'h',
            'low': 'l',
            'close': 'c',
            'volume': 'v',
        },
        inplace=True
    )

    df['t'] = df['timestamp'].astype(int) // 1000 * 10**6
    del df['timestamp']
    return df


symbolrecs = SymbolRec.objects.filter(symbol_type='crypto')
for symbolrec in symbolrecs:
    df = create_df_from_timescaledb(symbolrec)


# df = pd.DataFrame({
#     'id': pd.Categorical(['toronto1', 'paris3']),
#     'temperature': [20.0, 21.0],
#     'humidity': [0.5, 0.6],
#     'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})
#
# with Sender('localhost', 9009) as sender:
#     sender.dataframe(df, table_name='sensors')


# df = pd.DataFrame({
#         'pair': ['USDGBP', 'EURJPY'],
#         'traded_price': [0.83, 142.62],
#         'qty': [100, 400],
#         'limit_price': [0.84, None],
#         'timestamp': [
#             pd.Timestamp('2022-08-06 07:35:23.189062', tz='UTC'),
#             pd.Timestamp('2022-08-06 07:35:23.189062', tz='UTC')]})
# try:
#     with Sender('localhost', 9009) as sender:
#         sender.dataframe(
#             df,
#             table_name='trades',  # Table name to insert into.
#             symbols=['pair'],  # Columns to be inserted as SYMBOL types.
#             at='timestamp')  # Column containing the designated timestamps.
#
# except IngressError as e:
#     sys.stderr.write(f'Got error: {e}\n')




from time import perf_counter

df = pd.read_csv('../all_dfs.csv')
df['time'] = pd.to_datetime(df['time'])
df.rename(columns={'time': 'ts', 'symbol': 'sym'}, inplace=True)
s = perf_counter()
with Sender('100.79.194.66', 9009) as sender:
    sender.dataframe(df, table_name='stock_ohlcv', at='ts')
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


def query_questdb(sql: str, url: str = "http://100.79.194.66:9000/exp") -> pd.DataFrame:
    params = {"query": sql}
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return pd.read_csv(StringIO(response.text))


# s = perf_counter()
# sql_query = "SELECT * FROM stock_ohlcv_15 WHERE sym = 'SPY'"
# df = query_questdb(sql_query)
# print(df.tail())
# elapsed = perf_counter() - s
# print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


def query_questdb_psycopg(sql: str, host: str = "localhost", port: int = 8812, dbname: str = "qdb", user: str = "admin", password: str = "quest") -> pd.DataFrame:
    conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    with psycopg2.connect(conn_string) as conn:
        # return pd.read_sql(sql, conn)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)
            return df


# s = perf_counter()
# sql_query = "SELECT * FROM stock_ohlcv WHERE sym = 'SPY'"
# df = query_questdb_psycopg(sql_query)
# print(df.tail())
# elapsed = perf_counter() - s
# print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
