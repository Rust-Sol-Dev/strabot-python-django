import os
import pickle
from dataclasses import dataclass
from datetime import datetime, timedelta

import bytewax.operators as op
import pandas as pd
import pytz
import redis
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from bytewax.connectors.stdio import StdOutSink
from confluent_kafka import OFFSET_STORED


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec, SymbolType

from dataflows.bars import Bar
from dataflows.timeframe_ops import make_crypto_time_buckets, make_stock_time_buckets
from dataflows.serializers import deserialize


cache = caches['markets']
r = cache.client.get_client(write=True)
# r = redis.Redis(host='127.0.0.1', db=0, port=6379)

symbols = SymbolRec.objects.all().values_list('symbol', 'symbol_type')
symbol_type_map = {symbol: symbol_type for symbol, symbol_type in symbols}
allowed_symbols = symbol_type_map.keys()


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'minute-bar-consumer',
    'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.closed_kline1m', 'ALPACA.bars'],
    starting_offset=OFFSET_STORED,
    add_config=kafka_conf,
    batch_size=5000,
)

# ======================================================================================================================


def initialize_dfs_from_redis(symbol: str, symbol_type: SymbolType):
    dfs = {}
    if symbol_type_map[symbol] == SymbolType.STOCK:
        timeframes = ['1', '15', '30', '60', '4H', 'D', 'W', 'M', 'Q', 'Y']
    else:
        timeframes = ['1', '15', '30', '60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y']

    pipe = r.pipeline()
    for tf in timeframes:
        pipe.get(f'df:{symbol_type}:{symbol}:{tf}')
    results = pipe.execute()

    for tf, df_bytes in zip(timeframes, results):
        df = pickle.loads(df_bytes)
        dfs[tf] = df
    return dfs


def create_minute_bar(symbol__minute_bar):
    symbol, minute_bar = symbol__minute_bar

    if symbol_type_map[symbol] == SymbolType.STOCK:
        timestamp = datetime.fromisoformat(minute_bar.get('t')).timestamp()
        open_ = minute_bar.get('o')
        high = minute_bar.get('h')
        low = minute_bar.get('l')
        close = minute_bar.get('c')
        volume = minute_bar.get('v')
    else:
        timestamp = minute_bar.get('t') / 1000.0
        open_ = float(minute_bar.get('o'))
        high = float(minute_bar.get('h'))
        low = float(minute_bar.get('l'))
        close = float(minute_bar.get('c'))
        volume = float(minute_bar.get('q'))

    bar = Bar(
        ts=timestamp,
        o=float(open_),
        h=float(high),
        l=float(low),
        c=float(close),
        v=float(volume),
    )
    return symbol, bar


@dataclass
class DataFrameState:
    dfs: dict[str, pd.DataFrame]
    ts: datetime


def update_timeframes_stateful(dfs, symbol__bar):
    symbol, bar = symbol__bar
    symbol_type = symbol_type_map[symbol]

    if dfs is None:
        dfs = initialize_dfs_from_redis(symbol, symbol_type)
    # if data is None:
    #     data = DataFrameState(
    #         dfs=initialize_dfs_from_redis(symbol, symbol_type),
    #         ts=datetime.now(tz=pytz.UTC) - timedelta(minutes=5),
    #     )

    dt = datetime.fromtimestamp(bar.ts, tz=pytz.UTC)
    # if data.ts > dt:
    #     print('skipping', symbol, data.ts, dt)
    #     return data, bar

    bucket_func = make_crypto_time_buckets if symbol_type == SymbolType.CRYPTO else make_stock_time_buckets
    time_buckets = bucket_func(dt)

    for tf, bucket in time_buckets.items():
        tf_df = dfs[tf]
        if bucket not in tf_df.index:
            new_bar_df = pd.DataFrame(bar.as_dict, index=pd.Index([bucket], name='time'))
            del new_bar_df['ts']
            del new_bar_df['sid']
            tf_df = pd.concat([tf_df, new_bar_df]).drop_duplicates(keep='first')
        else:
            tf_df.loc[bucket, 'h'] = max(tf_df.loc[bucket, 'h'], bar.h)
            tf_df.loc[bucket, 'l'] = min(tf_df.loc[bucket, 'l'], bar.l)
            tf_df.loc[bucket, 'c'] = bar.c
            tf_df.loc[bucket, 'v'] += bar.v
        dfs[tf] = tf_df.tail(10_000)
        # data.ts = dt

    pipe = r.pipeline()
    for tf_, df in dfs.items():
        serialized_df = pickle.dumps(df)
        pipe.set(f'df:{symbol_type}:{symbol}:{tf_}', serialized_df)
    pipe.execute()

    return dfs, bar


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('dataflow_df')

s = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.map, 'to_bar', create_minute_bar)
    # .then(op.map, 'update_timeframes', update_timeframes)
    .then(op.map, 'add_symbol_key', lambda data: (data[0], (data[0], data[1])))
    .then(op.stateful_map, 'update_timeframes_stateful', update_timeframes_stateful)
    .then(op.map, 'convert_timestamp', lambda data: (data[0], (data[1], datetime.fromtimestamp(data[1].ts).isoformat())))
)
op.output('stdout_sink', s, StdOutSink())
