import os
import copy
from datetime import datetime, timedelta, timezone, time
from typing import Tuple, Callable

import bytewax.operators as op
import bytewax.operators.window as window_op
import pytz
import redis.exceptions
from bytewax.bytewax import SystemClockConfig
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


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-extended-trade-consumer',
    # 'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['ALPACA.trades'],
    add_config=kafka_conf,
    batch_size=5000,
)

# kafka_sink = KafkaSink(
#     brokers=settings.REDPANDA_BROKERS,
#     topic='ALPACA.bars_resampled',
#     add_config=kafka_conf,
# )


def filter_timestamp(symbol__value):
    """
    Filter trades that are outside of regular trading hours
    """
    symbol, value = symbol__value
    dt = datetime.fromisoformat(value['t'])
    dt = dt.astimezone(pytz.timezone('US/Eastern'))
    return not time(9, 30) <= dt.time() < time(16, 0)


def filter_conditions(symbol__value):
    _, value = symbol__value
    conditions = value.get('c', [])
    return 'T' in conditions


def accumulate(acc, x):
    volume = float(x['s'])
    price = float(x['p'])
    volume_usd = volume * price
    acc.append(volume_usd)
    return acc


def update_volume(volume_history, volume):
    volume_history = volume_history + volume
    return volume_history, copy.deepcopy(volume_history)


def sum_volume(symbol__metadata__volumes):
    symbol, (metadata, volumes) = symbol__metadata__volumes
    return symbol, sum(volumes)


def reducer(x, y):
    # print(x, y)
    return y


flow = Dataflow("alpaca_trades_extended")
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

reducer_window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=5))

one_second_window = (
    window_op.fold_window("window", trades, clock_config, window_config, list, accumulate)
    .then(op.map, "sum_volume", sum_volume)
    .then(op.stateful_map, 'update_volume', lambda: 0, update_volume)
    # .then(op.collect, 'collect', timeout=timedelta(seconds=5), max_size=1000)
    .then(window_op.reduce_window, 'reducer', SystemClockConfig(), reducer_window_config, reducer)
)
op.output('stdout_sink', one_second_window, StdOutSink())

