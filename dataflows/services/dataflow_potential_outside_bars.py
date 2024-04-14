import copy
import os
from datetime import datetime
from decimal import Decimal

import msgspec.json
import pytz

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from confluent_kafka import OFFSET_END
from rich import print

from dataflows.sinks.null import NullSink

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from stratbot.scanner.models.symbols import SymbolRec
from stratbot.alerts.tasks import send_discord_alert_from_dataflow

from dataflows.setups import SetupMsg
from dataflows.alerts import create_table, add_row
from dataflows.serializers import deserialize, serialize
from dataflows.bars import (
    to_bar_series_by_tf, potential_outside_bar, add_key_to_value, opening_prices, tfc_state, bar_shape
)


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'outside-bar-consumer',
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.bars_resampled', 'ALPACA.bars_resampled'],
    add_config=kafka_conf,
    batch_size=5000,
    starting_offset=OFFSET_END,
)


symbols = SymbolRec.objects.filter(skip_discord_alerts=False).values_list('symbol', 'symbol_type')
symbols_map = {symbol: symbol_type for symbol, symbol_type in symbols}
allowed_symbols = symbols_map.keys()


def scan_bars(symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series

    price = Decimal(str(tf_bar_series['15'].get_newest().c))
    opens = opening_prices(tf_bar_series)
    tfc_table = tfc_state(opens, price)

    setups_by_tf = {}
    for tf, bar_series in tf_bar_series.items():
        previous_bar = bar_series.get_previous()
        current_bar = bar_series.get_newest()
        is_potential_outside, direction = potential_outside_bar(previous_bar, current_bar)

        if is_potential_outside:
            hit_magnitude = current_bar.sid == '3'
            outside_trigger = (previous_bar.h + previous_bar.l) / 2
            outside_target = previous_bar.h if direction == 1 else previous_bar.l

            setups_by_tf[tf] = SetupMsg(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(current_bar.ts, tz=pytz.utc),
                tf=tf,
                direction=direction,
                trigger=outside_trigger,
                target=outside_target,
                trigger_bar=previous_bar,
                target_bar=previous_bar,
                current_bar=current_bar,
                pattern=[previous_bar.sid, 'P3'],
                priority=3,
                shape=bar_shape(previous_bar),
                hit_magnitude=hit_magnitude,
                # tfc_table=tfc_table,
                potential_outside=True,
            )

    return symbol, setups_by_tf


def send_alerts(historical_alerts, symbol__setups):
    symbol, setups = symbol__setups

    if historical_alerts is None:
        historical_alerts = {}

    total_setups = len(setups.items())
    # bull_setups = [setup.tf for setup in setups.values() if setup.direction == 1]
    # bear_setups = [setup.tf for setup in setups.values() if setup.direction == -1]
    # is_outside = [setup.tf for setup in setups.values() if setup.current_bar.sid == 3]

    table = create_table()
    for tf, setup in setups.items():
        ts = setup.current_bar.ts
        hit_magnitude = setup.hit_magnitude

        if historical_alerts.get((tf, 'p3')) is None or ts > historical_alerts[(tf, 'p3')]:
            historical_alerts[(tf, 'p3')] = ts
            table = add_row(table, setup)
            if tf in ['15', '30']:
                continue

            # setup.notes = ''
            # if len(is_outside) > 1:
            #     setup.notes += f'OUTSIDE: ' + ' / '.join(is_outside) + '\n'
            #
            # if len(bull_setups) > 1 or len(bear_setups) > 1:
            #     setup.notes += f'P3s: ' + ' / '.join(bull_setups or bear_setups) + '\n'

            if total_setups > 1:
                setup.notes = f'MULTIPLE P3s: ' + ' / '.join(setups.keys())

            setup_msg = msgspec.json.encode(setup)
            send_discord_alert_from_dataflow.delay(symbol, setup_msg)

        if hit_magnitude and (historical_alerts.get((tf, 'magnitude')) is None or ts > historical_alerts[(tf, 'magnitude')]):
            historical_alerts[(tf, 'magnitude')] = ts
            table = add_row(table, setup)

    if len(table.rows) > 0:
        print(table)

    return historical_alerts, copy.deepcopy(historical_alerts)


flow = Dataflow("potential_outside_bars")
bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_symbols', lambda data: data[0] in allowed_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
    .then(op.map, 'scan_bars', scan_bars)
)
s = op.map('add_key_to_value', bar_stream, add_key_to_value)
s = op.stateful_map('send_alerts', s, send_alerts)
op.output('stdout_sink', bar_stream, NullSink())
