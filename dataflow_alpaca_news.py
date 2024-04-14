import json
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from time import perf_counter

import bytewax.operators as op
import pytz
from bytewax.inputs import StatelessSourcePartition, DynamicSource
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from confluent_kafka import OFFSET_END
from websocket import create_connection

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings

from dataflows.serializers import deserialize, serialize

started_on = datetime.now(tz=pytz.utc)
already_alerted = dict()


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-alpaca-news-consumer',
    'enable.auto.commit': True,
}

kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='ALPACA.news',
    add_config=kafka_conf,
)


class WebsocketSourcePartition(StatelessSourcePartition):
    def __init__(self):
        self.ws = create_connection("wss://stream.data.alpaca.markets/v1beta1/news")
        self.ws.send(json.dumps({"action": "auth", "key": settings.ALPACA_API_KEY, "secret": settings.ALPACA_API_SECRET}))
        self.ws.send(json.dumps({"action": "subscribe", "news": ["*"]}))
        print(self.ws.recv())
        print(self.ws.recv())
        print(self.ws.recv())

    def next_batch(self, sched: datetime):
        return [self.ws.recv()]

    def next_awake(self):
        pass

    def close(self):
        self.ws.close()


class WebsocketSource(DynamicSource):

    def build(self, now: datetime, worker_index: int, worker_count: int) -> WebsocketSourcePartition:
        return WebsocketSourcePartition()


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('dataflow_alpaca_news')

news_source = (
    op.input('news_source', flow, WebsocketSource())
    # .then(op.flat_map, 'flatten', split_news)
    # .then(op.map, 'serialize', serialize)
)
# op.output('kafka_sink', news_source, kafka_sink)
op.output('kafka_sink', news_source, StdOutSink())
