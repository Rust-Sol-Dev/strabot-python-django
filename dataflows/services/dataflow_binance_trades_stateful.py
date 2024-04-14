import os
import copy
from datetime import datetime, timedelta, timezone
from typing import Tuple, Callable

import bytewax.operators as op
import bytewax.operators.window as window_op
import redis.exceptions
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.operators.window import EventClockConfig, TumblingWindow

from dataflows.serializers import deserialize, serialize
from dataflows.sinks.redis import RedisSink

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec

from dataflows.bars import Bar, BarSeries, to_ohlc, strat_id, parse_tfc, clean
from dataflows.timeframe_ops import floor_datetime_fixed, floor_datetime_variable

cache = caches['markets']
r = cache.client.get_client(write=True)
bar_history_key_prefix = 'barHistory:crypto:'


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'binance-stateful-trade-consumer-dev',
    # 'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.aggTrade'],
    add_config=kafka_conf,
    batch_size=5000,
)

kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='BINANCE.bars_resampled',
    add_config=kafka_conf,
)


# def bar_series_from_db(symbol, tf):
#     symbolrec = SymbolRec.objects.get(symbol=symbol)
#
#     df = getattr(symbolrec, symbolrec.TF_MAP[tf])
#     df = df.tail(5).reset_index()
#     df['time'] = (df['time'].astype('int64') // 1e9)
#     df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'strat_id']]
#     df.rename(columns={
#         'time': 'ts',
#         'open': 'o',
#         'high': 'h',
#         'low': 'l',
#         'close': 'c',
#         'volume': 'v',
#         'strat_id': 'sid',
#     }, inplace=True)
#     bars_dict = df.to_dict(orient='records')
#
#     bar_series = BarSeries(symbol, tf)
#     for bar_ in bars_dict:
#         bar_series.add_bar(Bar(**bar_))
#
#     return bar_series


def bar_series_from_cache(symbol, tf):
    bar_series = BarSeries(symbol, tf)
    try:
        bars = r.json().get(f'{bar_history_key_prefix}{symbol}', tf)
        for bar in bars:
            bar_series.add_bar(Bar(**bar))
    except redis.exceptions.ResponseError:
        pass
    return bar_series


def update_bar_series(bar_series, symbol__tf__bucket__bar):
    symbol, tf, bucket, bar = symbol__tf__bucket__bar

    if bar_series is None:
        print(f'load historical data for {symbol} [{tf}]')
        bar_series = bar_series_from_cache(symbol, tf)
        # bar_series = bar_series_from_db(symbol, tf)

    bucket_ts = bucket.timestamp()
    current_bar = bar_series.get_newest()

    if current_bar is None:
        current_bar = Bar(ts=bucket_ts, o=bar.o, h=bar.h, l=bar.l, c=bar.c, v=bar.v)
        bar_series.add_bar(current_bar)

    # if the bar is older than the current bar in the series, such as when reading from the
    # beginning of a redpanda topic when no state is available, skip updating the bar_series
    # because the bar_series is "current".
    if bar.ts < current_bar.ts:
        bar_ts = datetime.fromtimestamp(bar.ts, tz=timezone.utc)
        current_bar_ts = datetime.fromtimestamp(current_bar.ts, tz=timezone.utc)
        print(f'skipping {symbol} [{tf}] {bar_ts} < {current_bar_ts}')
        return bar_series, copy.deepcopy(bar_series)

    if bucket_ts == current_bar.ts:
        current_bar.h = max(bar.h, current_bar.h, float('-inf'))
        current_bar.l = min(bar.l, current_bar.l, float('inf'))
        current_bar.c = bar.c
        current_bar.v = current_bar.v + bar.v

        if previous_bar := bar_series.get_previous():
            current_bar.sid = strat_id(previous_bar, current_bar)
        bar_series.replace_newest(current_bar)
    else:
        current_bar = Bar(ts=bucket_ts, o=bar.o, h=bar.h, l=bar.l, c=bar.c, v=bar.v)
        if previous_bar := bar_series.get_previous():
            current_bar.sid = strat_id(previous_bar, current_bar)
        bar_series.add_bar(current_bar)

    return bar_series, copy.deepcopy(bar_series)


def accumulate(acc, trade):
    price = float(trade["p"])
    dollar_volume = float(trade["q"]) * price
    acc.append((price, dollar_volume))
    return acc


def build_window_assigner(
    tf: str,
    delta: timedelta | None,
) -> Callable[[Bar], Tuple[str, Tuple[str, str, datetime, Bar]]]:

    def window_assigner(symbol__bar):
        symbol, bar = symbol__bar
        dt = datetime.fromtimestamp(bar.ts, tz=timezone.utc)
        if tf in ['M', 'Q', 'Y']:
            bucket = floor_datetime_variable(dt, interval=tf)
        else:
            bucket = floor_datetime_fixed(dt, delta=delta)
        return symbol, (symbol, tf, bucket, bar)

    return window_assigner


def group_by_tf(symbol__bar_series_streams):
    symbol, bar_series_streams = symbol__bar_series_streams

    by_tf = {}
    for bar_series in bar_series_streams:
        by_tf[bar_series.tf] = bar_series.as_dict()

    return symbol, by_tf


def calculate_typical_price_and_square(symbol__bar):
    symbol, bar = symbol__bar
    typical_price = (bar.h + bar.l + bar.c) / 3
    print(typical_price * bar.v, bar.v, typical_price, typical_price**2)
    return symbol, (typical_price * bar.v, bar.v, typical_price, typical_price**2)


def accumulate_vwap(acc, vwap_data):
    acc.append((vwap_data[0], vwap_data[1]))


flow = Dataflow("binance_trades_stateful")
trades = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
)

clock_config = EventClockConfig(
    lambda data: datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
    wait_for_system_duration=timedelta(seconds=1),
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))

one_second_window = window_op.fold_window(
    "window", trades, clock_config, window_config, list, accumulate
).then(op.map, "agg", to_ohlc)

# timeframe_1 = op.map(
#     "assign_1m_timeframe", one_second_window, calculate_typical_price_and_square
# )
# op.output('stdout_sink_vwap', timeframe_1, StdOutSink())

# vwap_1 = op.stateful_map(
#     "process_1m_trades", timeframe_1, lambda: None, update_bar_series
# )

# vwap_daily =

timeframe_15 = op.map(
    "assign_15m_timeframe", one_second_window, build_window_assigner('15', timedelta(minutes=15))
)
trades_15 = op.stateful_map(
    "process_15m_trades", timeframe_15, lambda: None, update_bar_series
)

timeframe_30 = op.map(
    "assign_30m_timeframe", one_second_window, build_window_assigner('30', timedelta(minutes=30))
)
trades_30 = op.stateful_map(
    "process_30m_trades", timeframe_30, lambda: None, update_bar_series
)

timeframe_60 = op.map(
    "assign_60m_timeframe", one_second_window, build_window_assigner('60', timedelta(minutes=60))
)
trades_60 = op.stateful_map(
    "process_60m_trades", timeframe_60, lambda: None, update_bar_series
)

timeframe_4h = op.map(
    "assign_4H_timeframe", one_second_window, build_window_assigner('4H', timedelta(hours=4))
)
trades_4h = op.stateful_map(
    "process_4H_trades", timeframe_4h, lambda: None, update_bar_series
)

timeframe_6h = op.map(
    "assign_6H_timeframe", one_second_window, build_window_assigner('6H', timedelta(hours=6))
)
trades_6h = op.stateful_map(
    "process_6H_trades", timeframe_6h, lambda: None, update_bar_series
)

timeframe_12h = op.map(
    "assign_12H_timeframe", one_second_window, build_window_assigner('12H', timedelta(hours=12))
)
trades_12h = op.stateful_map(
    "process_12H_trades", timeframe_12h, lambda: None, update_bar_series
)

timeframe_d = op.map(
    "assign_D_timeframe", one_second_window, build_window_assigner('D', timedelta(days=1))
)
trades_d = op.stateful_map(
    "process_D_trades", timeframe_d, lambda: None, update_bar_series
)

timeframe_w = op.map(
    "assign_W_timeframe", one_second_window, build_window_assigner('W', timedelta(weeks=1))
)
trades_w = op.stateful_map(
    "process_W_trades", timeframe_w, lambda: None, update_bar_series
)

timeframe_m = op.map(
    "assign_M_timeframe", one_second_window, build_window_assigner('M', None)
)
trades_m = op.stateful_map(
    "process_M_trades", timeframe_m, lambda: None, update_bar_series
)

timeframe_q = op.map(
    "assign_Q_timeframe", one_second_window, build_window_assigner('Q', None)
)
trades_q = op.stateful_map(
    "process_Q_trades", timeframe_q, lambda: None, update_bar_series
)

timeframe_y = op.map(
    "assign_Y_timeframe", one_second_window, build_window_assigner('Y', None)
)
trades_y = op.stateful_map(
    "process_Y_trades", timeframe_y, lambda: None, update_bar_series
)

tf_streams = op.join(
    "join_tf_streams",
    trades_15,
    trades_30,
    trades_60,
    trades_4h,
    trades_6h,
    trades_12h,
    trades_d,
    trades_w,
    trades_m,
    trades_q,
    trades_y
)
tf_streams = op.map('group_by_tf', tf_streams, group_by_tf)
# op.output('redis_sink', tf_streams, RedisSink(r, bar_history_key_prefix))

s_serialized = op.map('kafka_serialize', tf_streams, serialize)
# op.output('kafka_sink', s_serialized, kafka_sink)

s_tfc = op.map('tfc', tf_streams, parse_tfc)
# op.output('redis_sink_tfc', s_tfc, RedisSink(r, f'TFC:crypto:'))

s_display = op.filter('filter_btcusdt', tf_streams, lambda data: data[0] == 'BTCUSDT')
s_display = op.filter_map('clean', s_display, clean)
op.output('stdout_sink', s_display, StdOutSink())
