import copy
import os
from datetime import datetime
from decimal import Decimal

import msgspec
import pytz

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from confluent_kafka import OFFSET_END
from discord_webhook import DiscordWebhook, DiscordEmbed
# from rich.console import Console
from rich import print
from rich.table import Table

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from stratbot.scanner.models.symbols import SymbolRec

from dataflows.serializers import deserialize
from dataflows.setups import SetupMsg
from dataflows.bars import to_bar_series_by_tf, potential_outside_bar, add_key_to_value, opening_prices, tfc_state, \
    timeframes_colored, bar_shape

# console = Console()

kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-p3-reader-consumer',
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['potential_outside_bars'],
    add_config=kafka_conf,
    batch_size=5000,
    starting_offset=OFFSET_END,
)


flow = Dataflow("potential_outside_bars")
bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
)
op.output('stdout_sink', bar_stream, StdOutSink())
