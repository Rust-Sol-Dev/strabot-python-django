import time
from datetime import timedelta, datetime, timezone, time
import os
from collections import defaultdict

import pytz
import bytewax.operators as op
import redis
from bytewax.operators.window import EventClockConfig, TumblingWindow, fold_window
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from confluent_kafka import OFFSET_STORED

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec

from dataflows import bars
from dataflows.serializers import deserialize, serialize
from dataflows.sinks.redis import RedisSink
from dataflows.timeframe_ops import make_stock_time_buckets


BAR_DEQUE_MAXLEN = 5

cache = caches['markets']
r = cache.client.get_client(write=True)
history_key_prefix = 'barHistory:stock:'

bar_history = defaultdict(lambda: defaultdict())


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
    # topics=['dispatch'],
    topics=['ALPACA.trades', 'dispatch'],
    starting_offset=OFFSET_STORED,
    # starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=10_000,
)

kafka_output = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.bars_resampled',
    add_config=kafka_conf,
)


# ======================================================================================================================

def backfill_bars():
    symbols = SymbolRec.objects.filter(symbol_type='stock').values_list('symbol', flat=True)

    with r.pipeline() as pipe:
        for symbol in symbols:
            pipe.json().get(f'{history_key_prefix}{symbol}')

        for symbol, data in zip(symbols, pipe.execute()):
            for tf, bars_ in data.items():
                bar_series = bars.BarSeries(symbol, tf)

                for bar in bars_:
                    ts = bar['ts']
                    new_bar = bars.Bar(s=symbol, ts=ts, o=bar['o'], h=bar['h'], l=bar['l'], c=bar['c'], v=bar['v'],
                                       sid=bar['sid'])
                    bar_series.add_bar(new_bar)
                bar_history[symbol][tf] = bar_series


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


def accumulate(acc, x):
    """
    Accumulate trade events into a list of (price, volume) tuples.
    """
    price = float(x['p'])
    volume = float(x['s'])
    acc.append((price, volume))
    return acc


def downsample(symbol__bar):
    symbol, bar = symbol__bar
    dt = datetime.fromtimestamp(bar.ts, tz=timezone.utc)
    time_buckets = make_stock_time_buckets(dt)

    bars_by_tf: dict[str, bars.Bar] = {}
    for tf, bucket in time_buckets.items():
        bar_series = bar_history.get(symbol, {}).get(tf, bars.BarSeries(symbol, tf))

        current_bar = bar_series.get_newest()
        if not current_bar or len(bar_series.bars) < 2:
            continue

        bucket_ts = bucket.timestamp()
        if bucket_ts == current_bar.ts:
            current_bar.h = max(bar.h, current_bar.h, float('-inf'))
            current_bar.l = min(bar.l, current_bar.l, float('inf'))
            current_bar.c = bar.c
            current_bar.v = current_bar.v + bar.v

            previous_bar = bar_series.get_previous()
            current_bar.sid = bars.strat_id(previous_bar, current_bar)
            bar_series.replace_newest(current_bar)
        else:
            current_bar = bars.Bar(ts=bucket_ts, o=bar.o, h=bar.h, l=bar.l, c=bar.c, v=bar.v)
            if previous_bar := bar_series.get_previous():
                current_bar.sid = bars.strat_id(previous_bar, current_bar)
            bar_series.add_bar(current_bar)
            bar_history[symbol][tf] = bar_series
        bars_by_tf[tf] = bar_series.as_dict()

    return symbol, bars_by_tf


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

# backfill_bars()

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
    .then(op.filter_map, 'backfill', backfill)
    # .then(op.filter, 'filter_timestamp', filter_timestamp)
    # .then(op.filter, 'filter_conditions', filter_conditions)
    # .then(fold_window, 'window', clock_config, window_config, list, accumulate)
    # .then(op.map, 'agg', bars.to_ohlc)
    # .then(op.map, 'downsample', downsample)
    # .then(op.filter, 'filter_null', lambda data: data[1] != {})
)
# op.output('redis_sink', s, RedisSink(history_key_prefix))
# s_serialized = op.map('serialize', s, serialize)
# op.output('kafka_sink', s_serialized, kafka_output)

# s_tfc = op.map('tfc', s, bars.parse_tfc)
# op.output('redis_sink_tfc', s_tfc, RedisSink(f'TFC:stock:'))

op.output('stdout_sink', s, StdOutSink())
# s2 = op.flat_map('flatten', s, lambda data: [(data[0], tf, value) for tf, value in data[1].items()])
# s2 = op.map('key_by_symbol_tf', s2, lambda data: (f'{data[0]}_{data[1]}', data[2]))
# s = op.filter('spy', s, lambda data: data[0] == 'SPY')
# s = op.filter_map('clean', s, clean)
