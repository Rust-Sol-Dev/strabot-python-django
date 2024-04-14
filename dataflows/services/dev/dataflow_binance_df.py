import os
from datetime import timedelta, datetime, timezone
import math
from collections import defaultdict

import bytewax.operators as op
import pandas as pd
from bytewax.operators.window import EventClockConfig, TumblingWindow, fold_window
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from confluent_kafka import OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED
from orjson import orjson

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings

from dataflows import bars
from dataflows.sinks import PostgresqlSink, PsqlTradeSink, CSVSink
from dataflows.timeframe_ops import historical_resample

BAR_DEQUE_MAXLEN = 14

bar_history = defaultdict(lambda: defaultdict(bars.BarSeries))
bars_df = defaultdict(lambda: defaultdict(pd.DataFrame))
trade_df = pd.DataFrame()


brokers = ['redpanda-01.stratalerts.com:9093']

kafka_conf = {
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 't3',
    'sasl.password': 'NME7EvUFwposBpgfJGJZpyCuxkPdZkfJ',
    'group.id': 'bytewax-binance-trade-consumer',
    # 'group.id': 'demo-consumer',
    # TODO: this might need to be removed. Can lead to duplicate processing of messages if the consumer fails
    #  between processing a message and the next automatic commit. See:
    #  https://learning.oreilly.com/library/view/kafka-the-definitive/9781492043072/ch04.html#idm45351109403456
    'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=brokers,
    topics=['BINANCE.aggTrade', 'dispatch'],
    starting_offset=OFFSET_BEGINNING,
    add_config=kafka_conf,
    batch_size=5000,
)

kafka_sink = KafkaSink(
    brokers=brokers,
    topic='BINANCE.bars_resampled',
    add_config=kafka_conf,
)

# ======================================================================================================================


def deserialize(data):
    key, value = data
    key = key.decode('utf-8')
    value = orjson.loads(value)
    return key, value


def serialize(data):
    key, value = data
    value = orjson.dumps(value)
    return key, value


def backfill(symbol__value):
    """
    Filter timescale events, which backfill historical data. Create BarSeries for each symbol/tf pair.
    """
    symbol, value = symbol__value

    if value.get('e') == 'timescale_crypto':
        tf = value['tf']
        bar_series = bar_history.get(symbol, {}).get(tf, bars.BarSeries(symbol, tf, maxlen=BAR_DEQUE_MAXLEN))

        if not bar_series.bars:
            previous_bar = None
            for bar in value['bars']:
                ts = bar['ts']
                bar = bars.Bar(ts=ts, o=bar['o'], h=bar['h'], l=bar['l'], c=bar['c'], v=bar['v'])
                if not previous_bar:
                    sid = None
                else:
                    sid = bars.strat_id(previous_bar, bar)
                previous_bar = bar
                bar.sid = sid
                bar_series.add_bar(bar)
            bar_history[symbol][tf] = bar_series

    elif value.get('e') == 'aggTrade':
        return symbol, value


def accumulate(acc, x):
    """
    Accumulate trade events into a list of (price, volume) tuples. Track volume in USDT.
    """
    price = float(x['p'])
    volume = float(x['q']) * price
    acc.append((price, volume))
    return acc


def ohlc(symbol__metadata__prices_volumes):
    symbol, (metadata, prices_volumes) = symbol__metadata__prices_volumes
    prices, volumes = zip(*prices_volumes)

    ts = metadata.open_time.timestamp()
    open_ = prices[0]
    close = prices[-1]
    high = max(prices)
    low = min(prices)
    volume = sum(volumes)

    return symbol, {
        'time': ts,
        'symbol': symbol,
        'exchange': 'BINANCE',
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    }


def to_df(symbol__trades):
    symbol, trades = symbol__trades

    df = pd.DataFrame(data=trades)

    # df['T'] = pd.to_datetime(df['T'], unit='ms', utc=True)
    # df.set_index('T', inplace=True)

    # Convert price 'p' and quantity 'q' to numeric
    # df['p'] = pd.to_numeric(df['p'])
    # df['q'] = pd.to_numeric(df['q'])

    # for tf in ['15', '30', '60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y']:
    # new_df = pd.concat([bars_df[symbol][tf], df])
    global trade_df

    new_df = pd.concat([trade_df, df])
    trade_df = new_df
    print(len(trade_df))
    trade_df.to_parquet('data/trades.parquet')
    # resampled_df = historical_resample('crypto', tf, new_df)
    # print(resampled_df.tail(5))
    # bars_df[symbol][tf] = resampled_df.tail(5)
    # ohlcv = df['p'].resample('15T').ohlc()
    # ohlcv['volume'] = df['q'].resample('15T').sum()
    return symbol


def resample(symbol__df):
    symbol, df = symbol__df
    """
             e              E         a        s          p     q          f          l      timestamp      m
    time                                                                                                                                     
    2023-12-12 16:42:30.584000+00:00  aggTrade  1702399350586  64660131  INJUSDT  25.210000  11.4  197117280  197117280  1702399350584  False
    """


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('dataflow_binance_bars')

clock_config = EventClockConfig(
    lambda data: datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc),
    wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))


s = (
    op.input('in', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    # .then(fold_window, 'window', clock_config, window_config, list, accumulate)
    # .then(op.map, 'ohlc', ohlc)
    .then(op.batch, 'batch', timedelta(seconds=5), 5000)
    .then(op.map, 'to_df', to_df)
    # .then(op.map, 'resample', resample)
)

# s = (
#
#     .then(op.filter_map, 'backfill', backfill)
#     .then(fold_window, 'window', clock_config, window_config, list, accumulate)
#     .then(op.map, 'agg', ohlc)
#     .then(op.map, 'downsample', downsample)
#     .then(op.filter, 'filter_null', lambda data: data[1] != {})
# )
#
# s_serialized = op.map('serialize', s, serialize)
# op.output('kafka_sink', s_serialized, kafka_sink)
# op.output('postgres_sink', s, PostgresqlSink())
#
# s = op.filter('filter_btcusdt', s, lambda data: data[0] == 'BTCUSDT')
# s = op.filter_map('clean', s, clean)
op.output('stdout_sink', s, StdOutSink())
