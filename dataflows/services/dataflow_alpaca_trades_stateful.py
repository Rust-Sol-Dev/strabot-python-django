import os
import copy
from datetime import datetime, timedelta, timezone, time
from typing import Tuple, Callable

import bytewax.operators as op
import bytewax.operators.window as window_op
import pytz
import redis.exceptions
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.operators.window import EventClockConfig, TumblingWindow

from dataflows.serializers import deserialize, serialize
from dataflows.sinks.redis import RedisSink

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec

from dataflows.bars import Bar, to_ohlc, strat_id, parse_tfc, clean, bar_series_from_cache
from dataflows.timeframe_ops import floor_datetime_variable, floor_datetime_mixed

cache = caches['markets']
r = cache.client.get_client(write=True)
bar_history_key_prefix = 'barHistory:stock:'


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-stateful-trade-consumer',
    # 'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['ALPACA.trades'],
    add_config=kafka_conf,
    batch_size=5000,
)

kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.bars_resampled',
    add_config=kafka_conf,
)


def filter_timestamp(symbol__value):
    """
    Filter trades that are outside of regular trading hours
    """
    symbol, value = symbol__value
    dt = datetime.fromisoformat(value['t'])
    dt = dt.astimezone(pytz.timezone('US/Eastern'))
    return time(9, 30) <= dt.time() < time(16, 0)


def filter_conditions(symbol__value):
    """
    This needs work. The list of conditions and their meanings is here:
        https://www.utpplan.com/DOC/UtpBinaryOutputSpec.pdf#page=43
    """
    exclude_conditions = [
        "C",  # Cash Sale
        # "G",  # Bunched Sold Trade (* update last: no)
        "H",  # Price Variation Trade
        "I",  # Odd Lot Trade
        "M",  # Market Center Official Close
        "N",  # Next Day
        "P",  # Prior Reference Price (* update last: no)
        "Q",  # Market Center Official Open
        "R",  # Seller
        "T",  # Form T
        "U",  # Extended Trading Hours (Sold Out of Sequence)
        "V",  # Contingent Trade
        "W",  # Average Price Trade
        # "Z",  # Sold (out of Sequence) (* update last: no)
        # "4",  # Derivatively Priced (* update last: no)
        "7",  # Qualified Contingent Trade (QCT)
    ]
    _, value = symbol__value
    conditions = value.get('c', [])
    return not any(condition in conditions for condition in exclude_conditions)


def filter_extended_hours(symbol__value):
    _, value = symbol__value
    conditions = value.get('c', [])
    return 'T' in conditions


def accumulate(acc, x):
    price = float(x['p'])
    volume = float(x['s'])
    acc.append((price, volume))
    return acc


def update_bar_series(bar_series, symbol__tf__bucket__bar):
    symbol, tf, bucket, bar = symbol__tf__bucket__bar

    if bar_series is None:
        print(f'load historical data for {symbol} [{tf}]')
        bar_series = bar_series_from_cache(r, bar_history_key_prefix, symbol, tf)
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


def build_window_assigner(
    tf: str,
    delta: timedelta | None,
    offset: timedelta | None = timedelta(0),
) -> Callable[[Bar], Tuple[str, Tuple[str, str, datetime, Bar]]]:

    def window_assigner(symbol__bar):
        symbol, bar = symbol__bar
        dt = datetime.fromtimestamp(bar.ts, tz=timezone.utc)
        if tf in ['M', 'Q', 'Y']:
            bucket = floor_datetime_variable(dt, interval=tf)
        else:
            bucket = floor_datetime_mixed(dt, delta=delta, offset=offset)
        return symbol, (symbol, tf, bucket, bar)

    return window_assigner


def group_by_tf(symbol__bar_series_streams):
    symbol, bar_series_streams = symbol__bar_series_streams

    by_tf = {}
    for bar_series in bar_series_streams:
        by_tf[bar_series.tf] = bar_series.as_dict()

    return symbol, by_tf


flow = Dataflow("alpaca_trades_stateful")
trades = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_timestamp', filter_timestamp)
    .then(op.filter, 'filter_conditions', filter_conditions)
)

clock_config = EventClockConfig(
    lambda data: datetime.fromisoformat(data['t']),
    wait_for_system_duration=timedelta(seconds=1),
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))

one_second_window = window_op.fold_window(
    "window", trades, clock_config, window_config, list, accumulate
).then(op.map, "agg", to_ohlc)

timeframe_15 = op.map(
    "assign_15m_timeframe", one_second_window, build_window_assigner('15', timedelta(minutes=15))
)
trades_15 = op.stateful_map("process_15m_trades", timeframe_15, update_bar_series)

timeframe_30 = op.map(
    "assign_30m_timeframe", one_second_window, build_window_assigner('30', timedelta(minutes=30))
)
trades_30 = op.stateful_map("process_30m_trades", timeframe_30, update_bar_series)

timeframe_60 = op.map(
    "assign_60m_timeframe", one_second_window, build_window_assigner('60', timedelta(minutes=60), offset=timedelta(minutes=30))
)
trades_60 = op.stateful_map("process_60m_trades", timeframe_60, update_bar_series)

timeframe_4h = op.map(
    "assign_4H_timeframe", one_second_window, build_window_assigner('4H', timedelta(hours=4), offset=timedelta(hours=9, minutes=30))
)
trades_4h = op.stateful_map("process_4H_trades", timeframe_4h, update_bar_series)

timeframe_d = op.map(
    "assign_D_timeframe", one_second_window, build_window_assigner('D', timedelta(days=1))
)
trades_d = op.stateful_map("process_D_trades", timeframe_d, update_bar_series)

timeframe_w = op.map(
    "assign_W_timeframe", one_second_window, build_window_assigner('W', timedelta(weeks=1))
)
trades_w = op.stateful_map(
    "process_W_trades", timeframe_w, update_bar_series
)

timeframe_m = op.map(
    "assign_M_timeframe", one_second_window, build_window_assigner('M', None)
)
trades_m = op.stateful_map(
    "process_M_trades", timeframe_m, update_bar_series
)

timeframe_q = op.map(
    "assign_Q_timeframe", one_second_window, build_window_assigner('Q', None)
)
trades_q = op.stateful_map(
    "process_Q_trades", timeframe_q, update_bar_series
)

timeframe_y = op.map(
    "assign_Y_timeframe", one_second_window, build_window_assigner('Y', None)
)
trades_y = op.stateful_map(
    "process_Y_trades", timeframe_y, update_bar_series
)

tf_streams = op.join(
    "join_tf_streams",
    trades_15,
    trades_30,
    trades_60,
    trades_4h,
    trades_d,
    trades_w,
    trades_m,
    trades_q,
    trades_y
)
tf_streams = op.map('group_by_tf', tf_streams, group_by_tf)
op.output('redis_sink', tf_streams, RedisSink(r, bar_history_key_prefix))

s_serialized = op.map('kafka_serialize', tf_streams, serialize)
op.output('kafka_sink', s_serialized, kafka_sink)

s_tfc = op.map('tfc', tf_streams, parse_tfc)
op.output('redis_sink_tfc', s_tfc, RedisSink(r, f'TFC:stock:'))

s_spy = op.filter('filter_spy', tf_streams, lambda data: data[0] == 'SPY')
s_btc = op.filter_map('clean', s_spy, clean)
op.output('stdout_sink', s_btc, StdOutSink())
