from datetime import timedelta, datetime, timezone
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings

from bytewax.outputs import StatelessSinkPartition, DynamicSink
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from confluent_kafka import OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED
from questdb.ingress import Sender, TimestampNanos

from dataflows.serializers import deserialize


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-binance-trade-qdb-consumer',
    'enable.auto.commit': True,
}


kafka_input = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.aggTrade'],
    starting_offset=OFFSET_STORED,
    add_config=kafka_conf,
    batch_size=5000,
)

# ======================================================================================================================


class QuestDBPartition(StatelessSinkPartition):
    def __init__(self):
        self.client = Sender('100.79.194.66', 9009)
        self.client.connect()

    def write_batch(self, items: list) -> None:
        for symbol, rows in items:
            for rec in rows:
                timestamp = rec['T']
                price = float(rec['p'])
                size = float(rec['q'])
                market_maker = rec['m']

                self.client.row(
                    'binance_aggtrade',
                    symbols={'s': symbol},
                    columns={
                        'p': price,
                        'q': size,
                        'm': market_maker,
                    },
                    at=TimestampNanos(timestamp * 10**6),
                )
        self.client.flush()


class QuestDBSink(DynamicSink):
    def build(self, worker_index, worker_count) -> QuestDBPartition:
        return QuestDBPartition()


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('binance_trades_to_qdb')
s = (
    op.input('input', flow, kafka_input)
    .then(op.map, 'deserialize', deserialize)
    .then(op.collect, 'collect', timedelta(seconds=1), 1000)
)
op.output('questdb_sink', s, QuestDBSink())
