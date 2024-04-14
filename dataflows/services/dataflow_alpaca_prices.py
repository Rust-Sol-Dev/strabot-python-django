import time
from datetime import timedelta, datetime, timezone, time
import os

import pytz
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import EventClockConfig, TumblingWindow, fold_window
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from confluent_kafka import OFFSET_END

from dataflows.serializers import deserialize, serialize

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
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
    topics=['ALPACA.trades'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=10_000,
)

kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.prices',
    add_config=kafka_conf,
)


def filter_timestamp(symbol__value):
    symbol, value = symbol__value
    dt = datetime.fromisoformat(value['t'])
    dt = dt.astimezone(pytz.timezone('US/Eastern'))
    return dt.time() < time(9, 30) or dt.time() >= time(16, 0)


def filter_conditions(symbol__value):
    exclude_conditions = [
        "C",  # Cash Sale
        "G",  # Bunched Sold Trade (* update last: no)
        "H",  # Price Variation Trade
        "I",  # Odd Lot Trade
        "M",  # Market Center Official Close
        "N",  # Next Day
        "P",  # Prior Reference Price (* update last: no)
        "Q",  # Market Center Official Open
        "R",  # Seller
        # "T",  # Form T
        "U",  # Extended Trading Hours (Sold Out of Sequence)
        "V",  # Contingent Trade
        "W",  # Average Price Trade
        "Z",  # Sold (out of Sequence) (* update last: no)
        "4",  # Derivatively Priced (* update last: no)
        "7",  # Qualified Contingent Trade (QCT)
    ]
    _, value = symbol__value

    conditions = value.get('c', [])
    return not any(condition in conditions for condition in exclude_conditions)


def accumulate(acc, x):
    price = float(x['p'])
    acc.append(price)
    return acc


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('alpaca_prices')

clock_config = EventClockConfig(
    lambda data: datetime.fromisoformat(data['t']),
    wait_for_system_duration=timedelta(seconds=1)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
window_config = TumblingWindow(align_to=align_to, length=timedelta(seconds=1))

s = (
    op.input('input', flow, kafka_input)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_conditions', filter_conditions)
    .then(fold_window, 'window', clock_config, window_config, list, accumulate)
    .then(op.map, 'latest_trade', lambda data: (data[0], data[1][1][-1]))
    .then(op.map, 'serialize', serialize)
)
op.output('kafka_sink', s, kafka_sink)
# op.output('std_out', s, StdOutSink())
