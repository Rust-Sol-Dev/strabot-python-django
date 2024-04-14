from datetime import timedelta, datetime
import os

from bytewax.outputs import StatelessSinkPartition, DynamicSink


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings

from orjson import orjson
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from confluent_kafka import OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED

from stratbot.scanner.models.pricerecs import StockTrade


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-trade-psql-consumer',
    'enable.auto.commit': True,
}


kafka_input = KafkaSource(
    brokers=[settings.REDPANDA_BROKERS],
    topics=['ALPACA.trades'],
    starting_offset=OFFSET_BEGINNING,
    add_config=kafka_conf,
    batch_size=2000,
)

# ======================================================================================================================


def deserialize(data):
    key, value = data
    key = key.decode('utf-8')
    value = orjson.loads(value)
    return key, value


def serialize(data):
    key, value = data
    value = orjson.dumps(value)
    return key, value


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


class PostgresqlSinkPartition(StatelessSinkPartition):

    def write_batch(self, items: list) -> None:
        new_recs = []
        for symbol, rows in items:
            for rec in rows:
                timestamp = datetime.fromisoformat(rec['t'])
                trade_id = rec['i']
                exchange_code = rec['x']
                price = rec['p']
                size = rec['s']
                conditions = rec['c']
                tape = rec['z']
                new_recs.append(
                    StockTrade(
                        time=timestamp,
                        symbol=symbol,
                        trade_id=trade_id,
                        exchange_code=exchange_code,
                        price=price,
                        size=size,
                        conditions=conditions,
                        tape=tape
                    )
                )
        StockTrade.objects.bulk_create(new_recs, ignore_conflicts=True)


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
    .then(op.filter, 'filter_conditions', filter_conditions)
    .then(op.batch, 'batch', timedelta(seconds=5), 5000)
)

op.output('postgresql_sink', s, PostgresqlSink())
