import os
from datetime import timedelta, datetime, timezone
import math
from collections import defaultdict
from decimal import Decimal

import bytewax.operators as op
import redis
from bytewax.operators.window import EventClockConfig, TumblingWindow, fold_window
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from confluent_kafka import OFFSET_END, OFFSET_STORED

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec

from dataflows import bars
from dataflows.serializers import deserialize, serialize
from dataflows.sinks.redis import RedisSink
from dataflows.timeframe_ops import make_crypto_time_buckets


BAR_DEQUE_MAXLEN = 5

cache = caches['markets']
r = cache.client.get_client(write=True)
bar_history_key_prefix = 'barHistory:crypto:'

bar_history = defaultdict(lambda: defaultdict())


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-binance-trade-consumer',
    'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.aggTrade', 'dispatch'],
    starting_offset=OFFSET_STORED,
    add_config=kafka_conf,
    batch_size=5000,
)

kafka_sink_bars = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='BINANCE.bars_resampled',
    add_config=kafka_conf,
)


# ======================================================================================================================

def backfill_from_redis():
    symbols = SymbolRec.objects.filter(symbol_type='crypto').values_list('symbol', flat=True)

    with r.pipeline() as pipe:
        for symbol in symbols:
            pipe.json().get(f'{bar_history_key_prefix}{symbol}')

        for symbol, tf__bars in zip(symbols, pipe.execute()):
            for tf, bars_ in tf__bars.items():
                bar_series = bars.BarSeries(symbol, tf)
                for bar in bars_:
                    bar_series.add_bar(bars.Bar(**bar))
                bar_history[symbol][tf] = bar_series


def backfill_from_redpanda(symbol__msg):
    symbol, msg = symbol__msg

    if msg.get('e') == 'timescale_crypto':
        tf = msg['tf']
        bar_series = bars.BarSeries(symbol, tf)
        for bar in msg['bars']:
            bar_series.add_bar(bars.Bar(**bar))
        bar_history[symbol][tf] = bar_series

    elif msg.get('e') == 'aggTrade':
        return symbol, msg


def accumulate(acc, x):
    price = float(x['p'])
    dollar_volume = float(x['q']) * price
    acc.append((price, dollar_volume))
    return acc


def downsample(symbol__bar):
    symbol, bar = symbol__bar
    dt = datetime.fromtimestamp(bar.ts, tz=timezone.utc)
    time_buckets = make_crypto_time_buckets(dt)

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

backfill_from_redis()

flow = Dataflow('dataflow_binance_trades')

clock_config = EventClockConfig(
    lambda data: datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc),
    wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))

s = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    # .then(op.filter_map, 'backfill', backfill_from_redpanda)
    .then(fold_window, 'window', clock_config, window_config, list, accumulate)
    .then(op.map, 'agg', bars.to_ohlc)
    .then(op.map, 'downsample', downsample)
    .then(op.filter, 'filter_null', lambda data: data[1] != {})
)
op.output('redis_sink', s, RedisSink(r, bar_history_key_prefix))
s_serialized = op.map('kafka_serialize', s, serialize)
op.output('kafka_sink', s_serialized, kafka_sink_bars)

# s_advance_decline = op.map('advance_decline', s, bars.advance_decline)
# op.output('redis_sink_advance_decline', s_advance_decline, RedisSink('advance_decline:crypto:'))
# op.output('stdout_sink_advance_decline', s_advance_decline, StdOutSink())

s_tfc = op.map('tfc', s, bars.parse_tfc)
op.output('redis_sink_tfc', s_tfc, RedisSink(r, f'TFC:crypto:'))
# op.output('stdout_tfc_sink', s_tfc, StdOutSink())

s_btc = op.filter('filter_btcusdt', s, lambda data: data[0] == 'BTCUSDT')
s_btc = op.filter_map('clean', s_btc, bars.clean)
op.output('stdout_sink', s_btc, StdOutSink())
