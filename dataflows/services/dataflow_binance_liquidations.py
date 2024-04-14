import os

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from confluent_kafka import OFFSET_END, OFFSET_STORED

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches

from dataflows.serializers import deserialize


cache = caches['markets']
r = cache.client.get_client(write=True)


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'binance-liquidation-consumer',
    'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.forceOrder'],
    starting_offset=OFFSET_STORED,
    add_config=kafka_conf,
    batch_size=5000,
)


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================


flow = Dataflow('binance_liquidations')

s = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.map, 'extract_order', lambda x: (x[0], x[1]['o']))
    .then(op.map, 'calculate_usd_volume', lambda x: (x[0], float(x[1]['p']) * float(x[1]['q'])))
    .then(op.filter, 'filter', lambda x: x[1] > 25000)
)
op.output('stdout_sink', s, StdOutSink())
