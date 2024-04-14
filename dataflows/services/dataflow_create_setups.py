import copy
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from time import perf_counter

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
from stratbot.scanner.models.pricerecs import STOCK_TF_PRICEREC_MODEL_MAP, CRYPTO_TF_PRICEREC_MODEL_MAP

from dataflows.setups import find_targets, create_setups_from_bar_series
from dataflows.serializers import deserialize, serialize
from dataflows.bars import to_bar_series_by_tf, potential_outside_bar, opening_prices, tfc_state
from dataflows.alerts import DiscordMsgAlert


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    # 'group.id': 'bytewax-dev-consumer',
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    # topics=['ALPACA.bars_resampled'],
    topics=['BINANCE.bars_resampled'],
    add_config=kafka_conf,
    batch_size=5000,
    starting_offset=OFFSET_END,
)

kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='setups',
    add_config=kafka_conf,
)

symbols = SymbolRec.objects.filter(skip_discord_alerts=False).values_list('symbol', 'symbol_type')
symbol_type_map = {symbol: symbol_type for symbol, symbol_type in symbols}
allowed_symbols = symbol_type_map.keys()


def scan_setups(historical_setups, symbol__tf_bar_series__tf_setups):
    symbol, (tf_bar_series, tf_setups) = symbol__tf_bar_series__tf_setups

    for tf, setup in tf_setups.items():
        if historical_setups.get(tf) is None or historical_setups[tf].timestamp < setup.timestamp:
            historical_setups[tf] = setup

    # opens = opening_prices(tf_bar_series)
    price = Decimal(str(tf_bar_series['15'].get_newest().c))
    # tfc_table = tfc_state(opens, price)

    inside_bars = []
    for tf, bar_series in tf_bar_series.items():
        current_bar = bar_series.get_newest()
        if current_bar.sid == '1':
            inside_bars.append(tf)

    in_force_setups = []
    setup_triggers = {}
    for tf, setup in historical_setups.items():
        bar_series = tf_bar_series[tf]
        if bar_series is None or setup is None:
            continue
        trigger_bar = setup.trigger_bar

        if setup.negated:
            continue
        # if setup.negated or setup.hit_magnitude or setup.potential_outside:
        #     continue

        current_bar = bar_series.get_newest()

        continuation = trigger_bar.sid == current_bar.sid or (trigger_bar.sid == '3' and current_bar.sid != '1')
        if continuation:
            setup.negated = True
            setup.negated_reasons.add('CONTINUATION')
            continue

        setup.check_potential_outside(current_bar)
        if setup.potential_outside is False:
            setup.check_in_force(current_bar)

        pattern = '-'.join(setup.pattern) + '-' + current_bar.sid
        if setup.trigger:
            setup_triggers[tf] = setup.trigger

        if setup.in_force:
            if not setup.initial_trigger:
                setup.initial_trigger = datetime.now(tz=timezone.utc)

            in_force_setups.append((tf, setup.bull_or_bear, setup.potential_outside))
            setup.target = find_targets(setup, bar_series)

            if symbol_type_map[setup.symbol] == 'stock':
                model_map = STOCK_TF_PRICEREC_MODEL_MAP
            else:
                model_map = CRYPTO_TF_PRICEREC_MODEL_MAP
            model = model_map[tf]

            if setup.targets is None and setup.target is not None:
                s = perf_counter()
                targets = []
                if setup.direction == 1:
                    additional_targets = (
                        model.objects
                        .filter(symbol=setup.symbol, high__gt=setup.target)
                        .order_by('-bucket')
                        .values_list('high', flat=True)
                    )
                    current_high = setup.target
                    for target in additional_targets:
                        if target > current_high:
                            targets.append(target)
                            current_high = target
                else:
                    additional_targets = (
                        model.objects
                        .filter(symbol=setup.symbol, low__lt=setup.target)
                        .order_by('-bucket')
                        .values_list('low', flat=True)
                    )
                    current_low = setup.target
                    for target in additional_targets:
                        if target < current_low:
                            targets.append(target)
                            current_low = target

                setup.targets = targets[0:5]
                print(f'additional targets [{symbol}]: ', setup.targets)
                elapsed = perf_counter() - s
                print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

            msg = 'IN FORCE'
            if not setup.in_force_alerted and tf not in ['15', '30']:
                # tfc_output = ' '.join([f"[[white]{tf}[/white]] {tfc.distance_percent}%" for tf, tfc in tfc_table.items()])
                print(f'{datetime.now()}: {msg} - {setup.bull_or_bear}: {setup.symbol} [{tf}] {pattern} {setup.priority} {setup.shape}')
                # if tfc_direction == setup.direction:
                    # print(f'*** FTFC DIRECTION MATCH')
                if len(inside_bars) > 5 or (tf in ['60', '4H', '6H', '12H'] and 'D' in inside_bars and 'W' in inside_bars):
                    setup.notes = f'INSIDE: ' + ' / '.join(inside_bars)
                    print(f'*** INSIDE BAR WARNING: {inside_bars}')
                # console.print(f'* TFC: {tfc_output}')

                symbolrec = SymbolRec.objects.get(symbol=setup.symbol)
                alert = DiscordMsgAlert(symbolrec, setup)
                # alert.send_msg(channel=symbolrec.symbol_type)

                setup.in_force_alerted = True
                setup.in_force_last_alerted = datetime.now(tz=timezone.utc)

        if not setup.hit_magnitude:
            hit_magnitude_bull = setup.direction == 1 and (price >= setup.target or current_bar.h >= setup.target)
            hit_magnitude_bear = setup.direction == -1 and (price <= setup.target or current_bar.l <= setup.target)
            if hit_magnitude_bull or hit_magnitude_bear:
                setup.hit_magnitude = True

                if tf not in ['15', '30'] and not setup.magnitude_alerted:
                    msg = 'HIT MAGNITUDE'
                    print(f'{datetime.now()}: {msg} - {setup.bull_or_bear}: {setup.symbol} [{tf}] {pattern} {setup.priority} {setup.shape}')

                    # symbolrec = SymbolRec.objects.get(symbol=setup.symbol)
                    # alert = DiscordAlert(symbolrec, setup)
                    # alert.send_msg(channel=symbolrec.symbol_type)

                    setup.magnitude_alerted = True
                    setup.magnitude_last_alerted = datetime.now(tz=timezone.utc)

    # print(symbol, setup_triggers)
    # if len(in_force_setups) >= 3:
    #     print(f'*** IN FORCE SETUPS: {symbol} {in_force_setups}')

    return historical_setups, copy.deepcopy(historical_setups)


flow = Dataflow("create_setups_dataflow")

bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_symbols', lambda data: data[0] in allowed_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
)

setup_stream = (
    op.stateful_map('create_setups', bar_stream, lambda: {}, create_setups_from_bar_series)
    .then(op.filter, 'filter_null', lambda data: data[1] is not None)
)

s_joined = (
    op.join('join', bar_stream, setup_stream)
    .then(op.map, 'add_symbol_key', lambda data: (data[0], (data[0], data[1])))
    # .then(op.map, 'filter_continuation', filter_continuation)
    # .then(op.map, 'check_potential_outside', check_potential_outside)
    .then(op.stateful_map, 'scan_setups', lambda: {}, scan_setups)
)

op.output('stdout_sink', s_joined, NullSink())
