import statistics
import time
from time import perf_counter
from datetime import timedelta, datetime, timezone, time
import math
import os
from collections import defaultdict

import pandas as pd
import pytz
from bytewax.bytewax import SlidingWindow
import bytewax.operators as op
from bytewax.operators.window import SystemClockConfig, EventClockConfig, TumblingWindow, fold_window
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax.connectors.stdio import StdOutSink
from confluent_kafka import OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED

from dataflows import bars
from dataflows.serializers import deserialize, serialize

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from stratbot.scanner.models.symbols import SymbolRec


BAR_DEQUE_MAXLEN = 5

bar_history = defaultdict(lambda: defaultdict())
bar_df = defaultdict(lambda: defaultdict(pd.DataFrame))
bar_pricerec = defaultdict(lambda: defaultdict(list))


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-trade-consumer',
    'enable.auto.commit': True,
}


kafka_input = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['ALPACA.trades', 'dispatch'],
    # starting_offset=OFFSET_STORED,
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=10_000,
)

kafka_output = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.bars_resampled',
    add_config=kafka_conf,
)


extended_hours_output = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.extended_hours',
    add_config=kafka_conf,
)

watchlist = SymbolRec.objects.filter(symbol_type='stock').values_list('symbol', flat=True)


# ======================================================================================================================


def _floor_to_start_of_period(dt, tz, period='day'):
    """
    Floor the datetime to the start of the day or week in a given timezone.
    """
    dt_tz = dt.astimezone(tz)
    if period == 'week':
        dt_tz -= timedelta(days=dt_tz.weekday())
    return tz.localize(datetime(dt_tz.year, dt_tz.month, dt_tz.day))


def _floor_datetime_mixed(dt, delta, offset=timedelta(0)):
    """
    Floor the datetime for intervals with a fixed duration, considering an offset.
    For daily or weekly intervals, floor to UTC time, otherwise floor to EST.
    """
    market_tz = pytz.timezone("America/New_York")
    utc_tz = pytz.utc

    period = 'day' if delta < timedelta(days=7) else 'week'
    start_of_period = _floor_to_start_of_period(dt, utc_tz, period) if delta >= timedelta(days=1) else _floor_to_start_of_period(dt, market_tz)

    start_of_period_with_offset = start_of_period + offset
    intervals_since_start = (dt.astimezone(utc_tz) - start_of_period_with_offset) // delta
    floored_time = start_of_period_with_offset + intervals_since_start * delta

    return floored_time.astimezone(dt.tzinfo)


def _floor_datetime_variable(dt: datetime, interval: str) -> datetime:
    """
    Floor the datetime for intervals with variable durations, such as months (M), quarters (Q), and years (Y).
    These intervals can't be represented as a fixed duration due to varying lengths (like number of days per month,
    leap years).
    """
    floor_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if interval == 'Q':
        floor_dt = floor_dt.replace(month=3 * ((dt.month - 1) // 3) + 1)
    elif interval == 'Y':
        floor_dt = floor_dt.replace(month=1)
    return floor_dt


def make_time_buckets(dt):
    """
    Create time buckets for datetime downsample.
    """
    return {
        '15': _floor_datetime_mixed(dt, delta=timedelta(minutes=15)),
        '30': _floor_datetime_mixed(dt, delta=timedelta(minutes=30)),
        '60': _floor_datetime_mixed(dt, delta=timedelta(minutes=60), offset=timedelta(minutes=30)),
        '4H': _floor_datetime_mixed(dt, delta=timedelta(hours=4), offset=timedelta(hours=9, minutes=30)),
        'D': _floor_datetime_mixed(dt, delta=timedelta(days=1)),
        'W': _floor_datetime_mixed(dt, delta=timedelta(weeks=1)),
        'M': _floor_datetime_variable(dt, interval='M'),
        'Q': _floor_datetime_variable(dt, interval='Q'),
        'Y': _floor_datetime_variable(dt, interval='Y'),
    }


def backfill(symbol__value):
    """
    Filter timescale events, which backfill historical data. Create BarSeries for each symbol/tf pair.
    """
    symbol, value = symbol__value

    if value.get('e') == 'timescale_stock':
        tf = value['tf']
        bar_series = bars.BarSeries(symbol, tf)

        for bar in value['bars']:
            ts = bar['ts']
            new_bar = bars.Bar(ts=ts, o=bar['o'], h=bar['h'], l=bar['l'], c=bar['c'], v=bar['v'], sid=bar['sid'])
            bar_series.add_bar(new_bar)
        bar_history[symbol][tf] = bar_series

    elif value.get('T') == 't':
        return symbol, value


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
    """
    This needs work. The list of conditions and their meanings is here:
        https://www.utpplan.com/DOC/UtpBinaryOutputSpec.pdf#page=43
    """
    _, value = symbol__value
    conditions = value.get('c', [])
    return 'I' not in conditions and 'T' in conditions


def accumulate(acc, x):
    """
    Accumulate trade events into a list of (price, volume) tuples. Track volume in USDT.
    """
    price = float(x['p'])
    volume = float(x['s'])
    acc.append((price, volume))
    return acc


# def downsample(symbol__ohlcv):
#     """
#     Downsample trades (time and sales) into time buckets. Until a symbol is backfilled by filter_timescale(), the
#     dataflow will ignore events for that symbol.
#     """
#     symbol, ohlcv = symbol__ohlcv
#
#     ts = ohlcv['ts']
#     dt = datetime.fromtimestamp(ts)
#     time_buckets = make_time_buckets(dt)
#
#     open_ = ohlcv['o']
#     high = ohlcv['h']
#     low = ohlcv['l']
#     close = ohlcv['c']
#     volume = ohlcv['v']
#     vwap = ohlcv['vwap']
#
#     bars_by_tf = {}
#     for tf, bucket in time_buckets.items():
#         bar_series = bar_history.get(symbol, {}).get(tf, bars.BarSeries(symbol, tf))
#
#         newest_bar = bar_series.get_newest()
#         if not newest_bar or len(bar_series.bars) < 2:
#             continue
#             # newest_bar = bars.Bar(ts=bucket.timestamp(), o=open_, h=high, l=low, c=close, v=volume)
#
#         bucket_ts = bucket.timestamp()
#         if newest_bar.ts == bucket_ts:
#             newest_bar.h = max(high, newest_bar.h, float('-inf'))
#             newest_bar.l = min(low, newest_bar.l, float('inf'))
#             newest_bar.c = close
#             newest_bar.v = newest_bar.v + volume
#
#             previous_bar = bar_series.get_previous()
#             newest_bar.sid = bars.strat_id(previous_bar, newest_bar)
#             newest_bar.vwap = vwap
#             bar_series.replace_newest(newest_bar)
#         else:
#             newest_bar = bars.Bar(ts=bucket_ts, o=open_, h=high, l=low, c=close, v=volume)
#             if previous_bar := bar_series.get_previous():
#                 newest_bar.sid = bars.strat_id(previous_bar, newest_bar)
#             bar_series.add_bar(newest_bar)
#             bar_history[symbol][tf] = bar_series
#         bars_by_tf[tf] = bar_series.as_dict()
#     return symbol, bars_by_tf

def downsample(symbol__value):
    """
    Downsample trades (time and sales) into time buckets. Until a symbol is backfilled by filter_timescale(), the
    dataflow will ignore events for that symbol.
    """
    symbol, value = symbol__value
    dt = datetime.fromtimestamp(value['ts'], tz=timezone.utc)
    time_buckets = make_time_buckets(dt)

    open_ = value['o']
    high = value['h']
    low = value['l']
    close = value['c']
    volume = value['v']

    bars_by_tf = {}
    for tf, bucket in time_buckets.items():
        bar_series = bar_history.get(symbol, {}).get(tf, bars.BarSeries(symbol, tf))

        newest_bar = bar_series.get_newest()
        if not newest_bar or len(bar_series.bars) < 2:
            continue
            # newest_bar = bars.Bar(ts=bucket.timestamp(), o=open_, h=high, l=low, c=close, v=volume)

        bucket_ts = bucket.timestamp()
        if bucket_ts == newest_bar.ts:
            newest_bar.h = max(high, newest_bar.h, float('-inf'))
            newest_bar.l = min(low, newest_bar.l, float('inf'))
            newest_bar.c = close
            newest_bar.v = newest_bar.v + volume

            previous_bar = bar_series.get_previous()
            newest_bar.sid = bars.strat_id(previous_bar, newest_bar)
            bar_series.replace_newest(newest_bar)
        else:
            # bar_series.add_bar(bars.Bar(ts=bucket_ts, o=open_, h=high, l=low, c=close, v=volume))
            # bar_history[symbol][tf] = bar_series

            # TODO: test this code vs using the code above
            newest_bar = bars.Bar(ts=bucket_ts, o=open_, h=high, l=low, c=close, v=volume)
            if previous_bar := bar_series.get_previous():
                newest_bar.sid = bars.strat_id(previous_bar, newest_bar)
            bar_series.add_bar(newest_bar)
            bar_history[symbol][tf] = bar_series
        bars_by_tf[tf] = bar_series.as_dict()

    return symbol, bars_by_tf


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('alpaca_trades_resampled')

clock_config = EventClockConfig(
    lambda data: datetime.fromisoformat(data['t']),
    wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))
# sliding_window = SlidingWindow(length=timedelta(seconds=5), offset=timedelta(seconds=0), align_to=align_to)


s = (
    op.input('input', flow, kafka_input)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_watchlist', lambda data: data[0] in watchlist)
    .then(op.filter_map, 'backfill', backfill)
    # .then(op.filter, 'filter_timestamp', filter_timestamp)
    # .then(op.filter, 'filter_conditions', filter_conditions)
    # .then(fold_window, 'window', clock_config, window_config, list, accumulate)
    # .then(op.map, 'agg', bars.ohlc)
    # .then(op.map, 'downsample', downsample)
    # .then(op.filter, 'filter_null', lambda data: data[1] != {})
)
# s_serialized = op.map('serialize', s, serialize)
# op.output('kafka_out', s_serialized, kafka_output)

s_branch = op.branch('branch', s, filter_timestamp)
s_eth = op.filter('filter_extended_hours', s_branch.falses, filter_extended_hours)
s_eth = op.map_value('map_value', s_eth, lambda data: data['p'])
s_eth_serialized = op.map('serialize', s_eth, serialize)
op.output('extended_hours_output', s_eth_serialized, extended_hours_output)

# s_rth = op.filter('filter_conditions', s_branch.trues, filter_conditions)
# s_rth = op.filter('filter_conditions', s_rth, filter_conditions)
# s_rth = fold_window('window', s_rth, clock_config, window_config, list, accumulate)
# s_rth = op.map('agg', s_rth, bars.ohlc)
# s_rth = op.map('downsample', s_rth, downsample)
# s_rth = op.filter('filter_null', s_rth, lambda data: data[1] != {})


# s2 = op.flat_map('flatten', s, lambda data: [(data[0], tf, value) for tf, value in data[1].items()])
# s2 = op.map('key_by_symbol_tf', s2, lambda data: (f'{data[0]}_{data[1]}', data[2]))
# s2_serialized = op.map('s2_serialize', s2, serialize)
# op.output('redis_sink', s2_serialized, RedisSink())
# op.output('std_out', s_eth, StdOutSink())


# s = op.filter('spy', s, lambda data: data[0] == 'SPY')
# s = op.filter_map('clean', s, clean)
