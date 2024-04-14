import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from time import perf_counter

import bytewax.operators as op
import pytz
from bytewax.inputs import StatelessSourcePartition, DynamicSource
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from confluent_kafka import OFFSET_END
from orjson import orjson

from dataflows.bars import to_bar_series_by_tf
from dataflows.serializers import deserialize
from dataflows.setups import flat_map_setups
from dataflows.sinks.null import NullSink

# from dataflows.setups import (
#     flat_map_setups,
#     drop_unused_timeframes,
#     is_potential_outside_bar, flat_map_setups_single_tf, flat_map_setups_testing
# )
# from dataflows.sinks.postgresql import PostgresqlSetupSink

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings

from stratbot.scanner.models.symbols import Setup
from stratbot.alerts import tasks


started_on = datetime.now(tz=pytz.utc)
already_alerted = dict()


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    # 'group.id': 'bytewax-binance-in-force-consumer',
    # 'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.bars_resampled'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=2500,
)


class PostgresqlSourcePartition(StatelessSourcePartition):

    def next_batch(self):
        setups_by_symbol = defaultdict(list)

        setups = (Setup.objects
                  .prefetch_related('symbol_rec')
                  .filter(expires__gt=datetime.now(tz=pytz.utc))
                  .filter(symbol_rec__symbol_type='crypto')
                  .filter(negated=False)
                  )

        for setup in setups:
            setups_by_symbol[setup.symbol_rec.symbol].append(setup)

        return [(symbol, setups) for symbol, setups in setups_by_symbol.items()]

    def next_awake(self):
        pass

    def close(self):
        pass


class PostgresqlSource(DynamicSource):

    def build(self, step_id: str, worker_index: int, worker_count: int) -> PostgresqlSourcePartition:
        return PostgresqlSourcePartition()


class PostgresqlSetupSinkPartition(StatelessSinkPartition):
    fields_to_update = [
        'negated',
        'has_triggered',
        'in_force',
        'in_force_alerted',
        'in_force_last_alerted',
        'hit_magnitude',
        'magnitude_alerted',
        'magnitude_last_alerted',
        'initial_trigger',
        'last_triggered',
        'in_force_alerted',
        'discord_alerted',
        'potential_outside',
    ]

    def write_batch(self, batch: list) -> None:
        print('===============================================================')
        print('write batch')
        print('===============================================================')
        # print(batch)

        to_update = []
        s = perf_counter()
        for symbol_setup_pk, setups in batch:
            to_update.append(setups[-1])

        # Setup.objects.bulk_update(to_update, self.fields_to_update)
        elapsed = perf_counter() - s
        print(f'done writing batch of {len(to_update)} in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

        # for symbol, setup in items:
        #     s = perf_counter()
        #     print(symbol, setup)
        #     setup.save()
        #     elapsed = perf_counter() - s
        #     print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        #     pass


class PostgresqlSetupSink(DynamicSink):
    def build(self, step_id: str, worker_index: int, worker_count: int) -> PostgresqlSetupSinkPartition:
        return PostgresqlSetupSinkPartition()


# def clean_expired_items(data_dict):
#     current_time = datetime.now(tz=pytz.utc)
#     expiration_cutoff = current_time - timedelta(minutes=5)
#     sorted_items = sorted(data_dict.items(), key=lambda x: x[1])
#     return {key: timestamp for key, timestamp in sorted_items if timestamp > expiration_cutoff}


def check_in_force(symbol__setup__tf_bar_series):
    symbol, setup, tf_bar_series = symbol__setup__tf_bar_series

    bar_series = tf_bar_series[setup.tf]
    close = bar_series.get_newest().c
    triggered_lower = close < setup.trigger and setup.direction == -1
    triggered_higher = close > setup.trigger and setup.direction == 1
    setup.in_force = triggered_lower or triggered_higher
    if setup.in_force:
        setup.has_triggered = True
        setup.last_triggered = datetime.now(tz=pytz.utc)

    return symbol, (setup, bar_series)


def is_against_tfc(symbol__setups__tf_bar_series):
    symbol, (setups, tf_bar_series) = symbol__setups__tf_bar_series

    daily_open = tf_bar_series['D'].get_newest().o
    for setup in setups:
        if setup.tf in ["15", "30"]:
            if (setup.direction == -1 and setup.trigger > daily_open) or (
                setup.direction == 1 and setup.trigger < daily_open
            ):
                print(f"TFC mismatch (daily): {symbol}, {setup}")
                setup.negated = True

    return symbol, (setups, tf_bar_series)


def send_alerts(symbol__setups):
    symbol, setups = symbol__setups

    # if already_alerted.get(setup.pk) or setup.negated or setup.expires < datetime.now(tz=pytz.utc):
    #     return None

    for setup in setups:
        if not setup.initial_trigger and setup.in_force:
            print(f"initial trigger @ {datetime.now()}: {symbol}, {setup.pk}, {setup}")
            setup.initial_trigger = datetime.now(tz=pytz.utc)
            # tasks.send_discord_alert.delay(setup.symbol_rec.pk, setup.pk)
            setup.discord_alerted = True
            # already_alerted[setup.pk] = datetime.now(tz=pytz.utc)
            # clean_expired_items(already_alerted)

    return symbol, setups
    # setup.in_force_last_alerted = datetime.now(tz=pytz.utc)


def scan_setups(historical_setups, symbol__setups__tf_bar_series):
    if historical_setups is None:
        historical_setups = {}
    symbol, (setups, tf_bar_series) = symbol__setups__tf_bar_series

    # for tf, setup in tf_setups.items():
    #     if historical_setups.get(tf) is None or historical_setups[tf].timestamp < setup.timestamp:
    #         historical_setups[tf] = setup

    for setup in setups:
        print(setup)
    return symbol, (setups, tf_bar_series)


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('dataflow_dev')

setup_source = op.input('postgresql_source', flow, PostgresqlSource())
bar_source = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
)
stream = (
    op.join('join', setup_source, bar_source)
    # .then(op.flat_map, 'flat_map_setups', flat_map_setups)
    # .then(op.map, 'check_in_force', check_in_force)
    # .then(op.map, 'is_against_tfc', is_against_tfc)
    .then(op.map, 'add_symbol_key', lambda data: (data[0], (data[0], data[1])))
    .then(op.stateful_map, 'scan_setups', scan_setups)
)

# stream = op.map('check_in_force', stream, check_in_force)
# stream = op.filter_map('is_against_tfc', stream, is_against_tfc)

# stream = op.map('drop_bars', stream, lambda x: (x[0], x[1][0]))
# stream = op.filter_map('send_alerts', stream, send_alerts)
# stream = op.flat_map('flat_map_setups', stream, flat_map_setups)
# stream = op.flat_map('flat_map_setups', stream, flat_map_setups_single_tf)
# stream = op.flat_map('flat_map_setups', setup_source, flat_map_setups_testing)

# stream = op.map('drop_unused_timeframes', stream, drop_unused_timeframes)
# stream = op.map('potential_outside_bar', stream, is_potential_outside_bar)
# stream = op.filter_map('is_in_force', stream, is_in_force)
# stream = op.filter_map('send_alerts', stream, send_alerts)
# stream = op.collect('collect', stream, timeout=timedelta(seconds=1), max_size=500)

# op.output('psql_sink', stream, PostgresqlSetupSink())
# op.output('stdout_sink', stream, StdOutSink())
# stream = op.filter_map('filter_none', stream, lambda x: x is not None)
op.output('stdout_sink', stream, StdOutSink())
# op.output('stdout_sink', stream, NullSink())
