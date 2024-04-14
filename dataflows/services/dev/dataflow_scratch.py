import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import EventClockConfig, TumblingWindow, fold_window
from bytewax.connectors.kafka import KafkaSource
from bytewax.testing import TestingSource
from bytewax.dataflow import Dataflow
from confluent_kafka import OFFSET_END

from dataflows.serializers import deserialize

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-trade-price-consumer',
    'enable.auto.commit': True,
}


kafka_input = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.bars_resampled'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=10_000,
)


# flow = Dataflow("test")
# inp = [
#     'a',
#     'b',
#     'b',
#     'b',
#     'c',
#     'd',
#     'e',
#     'e',
#     'e',
#     'f',
# ]
#
# def check_state(running_count, _item):
#     running_count += 1
#     return running_count, running_count
#
#
# s = op.input('testing_source', Dataflow('test'), TestingSource(inp))
# s = op.key_on('self_as_key', s, lambda x: x)
# s = op.stateful_map('testing_state', s, lambda: 0, check_state)


def accumulate(acc, x):
    acc = [x]
    return acc


flow = Dataflow('alpaca_prices')

clock_config = EventClockConfig(
    lambda data: datetime.now(tz=timezone.utc),
    wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=30))

s = (
    op.input('input', flow, kafka_input)
    .then(op.map, 'deserialize', deserialize)
    .then(fold_window, 'window', clock_config, window_config, list, accumulate)
)
op.output('std_out', s, StdOutSink())
