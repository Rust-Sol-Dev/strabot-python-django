import copy
import os
from datetime import datetime, timezone
from decimal import Decimal

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from confluent_kafka import OFFSET_END
from rich import print


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from stratbot.scanner.models.symbols import SymbolRec

from dataflows.alerts import DiscordMsgAlert
from dataflows.bars import to_bar_series_by_tf, opening_prices, potential_outside_bar
from dataflows.serializers import deserialize
from dataflows.setups import find_targets, create_setups_from_bar_series
from dataflows.sinks.null import NullSink


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    # topics=['ALPACA.bars_resampled'],
    topics=['BINANCE.bars_resampled'],
    add_config=kafka_conf,
    batch_size=5000,
    starting_offset=OFFSET_END,
)


symbols = SymbolRec.objects.filter(skip_discord_alerts=False).values_list('symbol', 'symbol_type')
symbol_type_map = {symbol: symbol_type for symbol, symbol_type in symbols}
allowed_symbols = symbol_type_map.keys()


def filter_ftfc(symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series
    opens = opening_prices(tf_bar_series)
    price = Decimal(str(tf_bar_series['15'].get_newest().c))
    if all([price > o for tf, o in opens.items() if tf in ['60', 'D', 'W', 'M', 'Q', 'Y']]):
        return True
    elif all([price < o for tf, o in opens.items() if tf in ['60', 'D', 'W', 'M', 'Q', 'Y']]):
        return True
    return False


def scan_setups(historical_setups, tf_bar_series__tf_setups):
    tf_bar_series, tf_setups = tf_bar_series__tf_setups

    for tf, setup in tf_setups.items():
        if historical_setups.get(tf) is None or historical_setups[tf].timestamp < setup.timestamp:
            historical_setups[tf] = setup

    for tf, setup in historical_setups.items():
        bar_series = tf_bar_series[tf]
        if bar_series is None or setup is None:
            continue
        trigger_bar = setup.trigger_bar
        current_bar = bar_series.get_newest()

        potential_outside, direction = potential_outside_bar(setup.trigger_bar, current_bar)
        if potential_outside:
            setup.potential_outside = True
            setup.direction = direction
            setup.negated = True
            continue

        continuation = trigger_bar.sid == current_bar.sid # or (trigger_bar.sid == '3' and current_bar.sid != '1')
        if continuation:
            setup.negated = True
            setup.negated_reasons.add('CONTINUATION')
            continue

        in_force_bear = current_bar.c < trigger_bar.l
        in_force_bull = current_bar.c > trigger_bar.h
        setup.in_force = in_force_bear or in_force_bull

        if setup.in_force:
            if not setup.initial_trigger:
                setup.initial_trigger = datetime.now(tz=timezone.utc)

            setup.trigger = setup.bear_trigger if in_force_bear else setup.bull_trigger
            if setup.trigger_bar.sid == '3':
                setup.target = None
            else:
                setup.target = find_targets(setup, bar_series)

            msg = 'IN FORCE'
            if not setup.in_force_alerted and tf not in ['15', '30']:
                bull_or_bear = 'BULL' if setup.direction == 1 else 'BEAR'
                print(f'{datetime.now()}: {msg} - {bull_or_bear}: {setup.symbol} [{tf}] {setup.pattern} {current_bar.sid}')

                symbolrec = SymbolRec.objects.get(symbol=setup.symbol)

                # alert = DiscordMsgAlert(symbolrec, setup)
                # if setup.trigger_bar.sid == '3':
                #     channel = f'{symbolrec.symbol_type}-expando'
                # else:
                #     channel = f'{symbolrec.symbol_type}-ftfc'
                # alert.send_msg(channel=channel)

                setup.in_force_alerted = True
            setup.in_force_last_alerted = datetime.now(tz=timezone.utc)

    return historical_setups, copy.deepcopy(historical_setups)


flow = Dataflow("dataflow_ftfc")

bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_symbols', lambda data: data[0] in allowed_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
    .then(op.filter, 'filter_ftfc', filter_ftfc)
)

setup_stream = (
    op.stateful_map('create_setups', bar_stream, lambda: {}, create_setups_from_bar_series)
    .then(op.filter, 'filter_null', lambda data: data[1] is not None)
)


s_joined = (
    op.join('join', bar_stream, setup_stream)
    # .then(op.map, 'filter_ftfc', filter_ftfc)
    # .then(op.map, 'filter_continuation', filter_continuation)
    .then(op.stateful_map, 'scan_setups', lambda: {}, scan_setups)
)

op.output('stdout_sink', s_joined, NullSink())
# op.output('stdout_sink', s_joined, StdOutSink())
