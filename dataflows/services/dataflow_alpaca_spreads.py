import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import bytewax.operators as op
import redis
from bytewax.operators.window import EventClockConfig, TumblingWindow
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import fold_window
from confluent_kafka import OFFSET_STORED, OFFSET_END

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches

from dataflows.serializers import deserialize, serialize
from dataflows.sinks.redis import RedisSink


cache = caches['markets']
r = cache.client.get_client(write=True)
history_key_prefix = 'barHistory:stock:'


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-spread-consumer',
    'enable.auto.commit': True,
}


kafka_input = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['ALPACA.quotes'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=5_000,
)


kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.spreads',
    add_config=kafka_conf,
)


# ======================================================================================================================


def accumulate(acc, x):
    acc.append((x['bp'], x['ap']))
    return acc


def calculate_spread(symbol__meta_values):
    symbol, (meta, values) = symbol__meta_values
    bid, ask = values[-1]
    bid = Decimal(str(bid))
    ask = Decimal(str(ask))
    spread = ask - bid
    price = (bid + ask) / 2
    spread_percentage = (spread / price) * 100
    return symbol, {'bid': float(bid), 'ask': float(ask), 'spread_usd': float(spread), 'spread_percentage': float(spread_percentage)}


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

clock_config = EventClockConfig(
    lambda data: datetime.fromisoformat(data['t']),
    wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))

flow = Dataflow('alpaca_spreads')
s = (
    op.input('input', flow, kafka_input)
    .then(op.map, 'deserialize', deserialize)
    .then(fold_window, 'window', clock_config, window_config, list, accumulate)
    .then(op.map, 'calculate_spread', calculate_spread)
)

op.output('redis_sink', s, RedisSink(r, f'spread:stock:'))
op.output('stdout_sink', s, StdOutSink())

s = op.map('serialize', s, serialize)
op.output('kafka_sink', s, kafka_sink)
