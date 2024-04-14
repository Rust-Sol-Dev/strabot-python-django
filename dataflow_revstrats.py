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
    topics=['ALPACA.bars_resampled', 'BINANCE.bars_resampled'],
    add_config=kafka_conf,
    batch_size=5000,
    starting_offset=OFFSET_END,
)

symbols = SymbolRec.objects.filter(skip_discord_alerts=False).values_list('symbol', 'symbol_type')
symbol_type_map = {symbol: symbol_type for symbol, symbol_type in symbols}
allowed_symbols = symbol_type_map.keys()


"""
Rev Strats
Any 1-2-2 / 1-3-2 [2 bar rev strat]
After Bar Close Scanning
3 = Hammer / Shooter
Any 1-2-going3 [1 bar rev strat]
Live Bar Scanning

    Logic B) :
        Stock is a 1-2u or 1-2d
            If yes
        Is 2d green [long] or 2u red [short]
            Yes
        Has it taken out the motherbar Hi/Lo
    Yes                 No
Mag = Potential 3 [T1]          Mag = Potential 3 [T1]
    MotherBar Hi/Lo [T2]

[ALERT = Continuity change]

    Logic A):
        Stock is a 1–2u/3/2d [shoot | hammer/shooter | hammer]
            If yes
        Create Setup
        ALERT = The Break
"""


def scan_setups(historical_setups, tf_bar_series__setup):
    tf_bar_series, setup = tf_bar_series__setup

    if historical_setups.get(setup.tf) is None or historical_setups[setup.tf].timestamp < setup.timestamp:
        historical_setups[setup.tf] = setup

    for tf, setup in historical_setups.items():
        bar_series = tf_bar_series[tf]
        if bar_series is None or setup is None:
            continue

        current_bar = bar_series.get_newest()

        # in_force_bear = current_bar.c < trigger_bar.l
        # in_force_bull = current_bar.c > trigger_bar.h
        # setup.in_force = in_force_bear or in_force_bull
        #
        # if setup.in_force:
        #     if not setup.initial_trigger:
        #         setup.initial_trigger = datetime.now(tz=timezone.utc)
        #
        #     setup.trigger = setup.bear_trigger if in_force_bear else setup.bull_trigger
        #     if setup.trigger_bar.sid == '3':
        #         setup.target = None
        #     else:
        #         setup.target = find_targets(setup, bar_series)
        #
        #     msg = 'IN FORCE'
        #     if not setup.in_force_alerted and tf not in ['15', '30']:
        #         bull_or_bear = 'BULL' if setup.direction == 1 else 'BEAR'
        #         print(
        #             f'{datetime.now()}: {msg} - {bull_or_bear}: {setup.symbol} [{tf}] {setup.pattern} {current_bar.sid}')
        #
        #         symbolrec = SymbolRec.objects.get(symbol=setup.symbol)
        #
        #         alert = DiscordMsgAlert(symbolrec, setup)
        #         if setup.trigger_bar.sid == '3':
        #             channel = f'{symbolrec.symbol_type}-expando'
        #         else:
        #             channel = f'{symbolrec.symbol_type}-ftfc'
        #         alert.send_msg(channel=channel)
        #
        #         setup.in_force_alerted = True
        #     setup.in_force_last_alerted = datetime.now(tz=timezone.utc)

    return historical_setups, copy.deepcopy(historical_setups)


def filter_revstrats(symbol__setup):
    symbol, setup = symbol__setup

    match setup.target_bar.as_tuple, setup.trigger_bar.as_tuple:
        case ('1', _, _, _), ('2U', _, False, True):
            return True
        case ('1', _, _, _), ('2D', _, True, False):
            return True
        case('1', _, _, _), ('3', _, _, _):
            return True
    return False


flow = Dataflow("dataflow_revstrats")

bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_symbols', lambda data: data[0] in allowed_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
)

setup_stream = (
    op.stateful_map('create_setups', bar_stream, lambda: {}, create_setups_from_bar_series)
    .then(op.flat_map, 'flat_map', lambda data: [(data[0], setup) for _, setup in data[1].items()])
    .then(op.filter, 'filter_null', lambda data: data[1] is not None)
    .then(op.filter, 'filter_revstrats', filter_revstrats)
)

s_joined = (
    op.join('join', bar_stream, setup_stream)
    .then(op.stateful_map, 'scan_setups', lambda: {}, scan_setups)
)
#
# op.output('stdout_sink', s_joined, NullSink())
# op.output('stdout_sink', setup_stream, StdOutSink())
op.output('stdout_sink', s_joined, StdOutSink())
