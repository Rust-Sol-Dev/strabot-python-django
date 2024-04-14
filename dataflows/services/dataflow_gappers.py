import copy
import os
from datetime import datetime
from decimal import Decimal
import pytz

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from confluent_kafka import OFFSET_END
from rich import print
from rich.table import Table

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.conf import settings
from stratbot.scanner.models.symbols import SymbolRec

from dataflows.alerts import DiscordMsgAlert
from dataflows.setups import SetupMsg, build_setup
from dataflows.serializers import deserialize, serialize
from dataflows.bars import to_bar_series_by_tf, add_key_to_value, opening_prices


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-gapper-consumer',
}

bar_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['ALPACA.bars_resampled'],
    add_config=kafka_conf,
    batch_size=5000,
    starting_offset=OFFSET_END,
)

# price_source = KafkaSource(
#     brokers=settings.REDPANDA_BROKERS,
#     topics=['ALPACA.prices'],
#     add_config=kafka_conf,
#     batch_size=5000,
#     starting_offset=OFFSET_END,
# )

# spread_source = KafkaSource(
#     brokers=settings.REDPANDA_BROKERS,
#     topics=['ALPACA.spreads'],
#     add_config=kafka_conf,
#     batch_size=5000,
#     starting_offset=OFFSET_END,
# )


allowed_symbols = SymbolRec.objects.filter(symbol_type='stock').values_list('symbol', flat=True)


def filter_tfc(symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series
    opens = opening_prices(tf_bar_series)
    price = Decimal(str(tf_bar_series['15'].get_newest().c))
    if price > opens['D'] or price < opens['D']:
        return True
    return False


def is_gapper(symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series

    daily = tf_bar_series['D']
    previous_bar = daily.get_previous()
    current_bar = daily.get_newest()

    if previous_bar is None or current_bar is None:
        return

    gapping_up = current_bar.o > previous_bar.h
    gapping_down = current_bar.o < previous_bar.l
    if not gapping_up and not gapping_down:
        return

    gap_up_percentage = ((current_bar.o - previous_bar.h) / previous_bar.h) * 100 if gapping_up else 0
    gap_down_percentage = ((previous_bar.l - current_bar.o) / previous_bar.l) * 100 if gapping_down else 0

    if (gapping_up and gap_up_percentage < 2) or (gapping_down and gap_down_percentage < 2):
        return

    setups_by_tf = {}
    for tf, bar_series in tf_bar_series.items():
        if tf not in ['30', '60']:
            continue

        bar_series = tf_bar_series[tf]
        target_bar, trigger_bar = bar_series.strat_candles()
        if trigger_bar is None or target_bar is None:
            continue

        # today_ts = datetime.now(tz=pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        # if target_bar.ts < today_ts:
        #     continue

        current_bar = bar_series.get_newest()
        bar_conditions = (
                trigger_bar.sid in ['1', '2U', '2D']
                and current_bar.sid != trigger_bar.sid
                and current_bar.sid != '3'
        )
        actionable_bear = gapping_up and current_bar.c < trigger_bar.l and bar_conditions
        actionable_bull = gapping_down and current_bar.c > trigger_bar.h and bar_conditions

        setup = build_setup(symbol, bar_series)

        if actionable_bear:
            setup.direction = -1
            setup.trigger = trigger_bar.l
            setup.target = target_bar.l
        elif actionable_bull:
            setup.direction = 1
            setup.trigger = trigger_bar.h
            setup.target = target_bar.h

        if actionable_bear or actionable_bull:
            setups_by_tf[tf] = setup

    return symbol, setups_by_tf


def send_alerts(historical_alerts, symbol_setups):
    symbol, setups = symbol_setups

    if historical_alerts is None:
        historical_alerts = {}

    # if symbol not in allowed_symbols:
    #     return historical_alerts, copy.deepcopy(historical_alerts)

    # if spread['spread_percentage'] > 0.10:
    #     print(f'spread: {spread["spread_percentage"]}')
    #     return historical_alerts, copy.deepcopy(historical_alerts)

    for tf, setup in setups.items():
        ts = setup.timestamp
        if historical_alerts.get(tf) is None or ts > historical_alerts[tf]:
            direction_msg = 'BULL' if setup.direction == 1 else 'BEAR' if setup.direction == -1 else 'NEUTRAL'
            print(datetime.now(), f'actionable: [yellow]{setup.symbol}[/yellow] [[white]{tf}[/white]] - {direction_msg}')

            symbolrec = SymbolRec.objects.get(symbol=setup.symbol)
            alert = DiscordMsgAlert(symbolrec, setup)
            alert.send_msg(channel='gappers')

            historical_alerts[tf] = ts
    return historical_alerts, copy.deepcopy(historical_alerts)


flow = Dataflow("gappers")
bar_stream = (
    op.input('bar_source', flow, bar_source)
    .then(op.map, 'deserialize_bars', deserialize)
    .then(op.filter, 'filter_symbols', lambda data: data[0] in allowed_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
    .then(op.filter, 'filter_tfc', filter_tfc)
    .then(op.filter_map, 'is_gapper', is_gapper)
    .then(op.filter, 'filter_null', lambda x: x[1] != {})
)

# spread_stream = (
#     op.input('spread_source', flow, spread_source)
#     .then(op.map, 'deserialize_spreads', deserialize)
#     .then(op.filter, 'filter_spread_symbols', lambda data: data[0] in allowed_symbols)
# )

# s_joined = op.join('join', bar_stream, spread_stream)

s = op.map('add_key_to_value', bar_stream, add_key_to_value)
s = op.stateful_map('send_alerts', s, send_alerts)
s = op.filter('filter_null_alerts', s, lambda x: x[1] != {})
# s_serialized = op.map('serialize', bar_stream, serialize)

# op.output('kafka_sink', s_serialized, kafka_sink)
op.output('stdout_sink', s, StdOutSink())
