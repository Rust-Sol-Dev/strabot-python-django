from datetime import timedelta, datetime, timezone
import os

from bytewax.outputs import StatelessSinkPartition, DynamicSink

from dataflows.serializers import deserialize

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings

from orjson import orjson
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from confluent_kafka import OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED

from stratbot.scanner.models.pricerecs import CryptoTrade

kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-binance-trade-psql-consumer',
    'enable.auto.commit': True,
}


kafka_input = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.aggTrade'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=2000,
)

# ======================================================================================================================

class PostgresqlSinkPartition(StatelessSinkPartition):

    def write_batch(self, items: list) -> None:
        new_recs = []
        for symbol, rows in items:
            for rec in rows:
                timestamp = datetime.fromtimestamp(rec['T'] / 1000.0, tz=timezone.utc)
                trade_id = rec['a']
                price = float(rec['p'])
                size = float(rec['q'])
                market_maker = rec['m']

                new_recs.append(
                    CryptoTrade(
                        time=timestamp,
                        symbol=symbol,
                        trade_id=trade_id,
                        price=price,
                        size=size,
                        market_maker=market_maker,
                    )
                )
        CryptoTrade.objects.bulk_create(new_recs, ignore_conflicts=True)


class PostgresqlSink(DynamicSink):
    def build(self, worker_index, worker_count) -> PostgresqlSinkPartition:
        return PostgresqlSinkPartition()


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('alpaca_trades_to_psql')
s = (
    op.input('input', flow, kafka_input)
    .then(op.map, 'deserialize', deserialize)
    .then(op.collect, 'collect', timedelta(seconds=1), 1000)
)
op.output('postgresql_sink', s, PostgresqlSink())
