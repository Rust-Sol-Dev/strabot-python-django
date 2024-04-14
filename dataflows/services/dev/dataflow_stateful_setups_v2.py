import os
import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging

from confluent_kafka import OFFSET_END
from rich.logging import RichHandler
from rich.console import Console
from rich.text import Text

import bytewax.operators as op
import msgspec
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.window import EventClockConfig, TumblingWindow, fold_window
from bytewax.connectors.kafka import KafkaSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec
from stratbot.scanner.models.pricerecs import CRYPTO_TF_PRICEREC_MODEL_MAP

from dataflows.alerts import DiscordMsgAlert
from dataflows.serializers import deserialize
from dataflows.setups import build_setup
from dataflows.bars import to_bar_series_by_tf, add_key_to_value, opening_prices, tfc_state, potential_outside_bar, Bar

# cache = caches['markets']
# r = cache.client.get_client(write=True)
# bar_history_key_prefix = 'barHistory:crypto:'
console = Console()

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)]
)

kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-stateful-consumer',
    # 'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.bars_resampled', 'ALPACA.bars_resampled'],
    # topics=['ALPACA.bars_resampled'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=5000,
)


symbols = SymbolRec.objects.filter(skip_discord_alerts=False).values_list('symbol', 'symbol_type')
symbols_map = {symbol: symbol_type for symbol, symbol_type in symbols}
allowed_symbols = symbols_map.keys()


def find_targets(setup, bar_series):
    direction = setup.direction
    h_or_l = 'h' if direction == 1 else 'l'
    high_or_low = 'high' if direction == 1 else 'low'
    column = high_or_low

    # if setup.pattern in [['1', '2U'], ['1', '1'], ['1', '2D']]:
    #     try:
    #         bar = bar_series.get_by_index(-4)
    #         print(setup.symbol, bar)
    #         target = getattr(bar, h_or_l)
    #     except IndexError:
    #         target = None
    # else:
    target = getattr(setup.target_bar, h_or_l)

    # model = CRYPTO_TF_PRICEREC_MODEL_MAP[setup.tf]
    # if setup.direction == 1:
    #     df = model.df.filter(symbol=setup.symbol, high__gt=setup.trigger).to_timeseries(index='bucket')
    # else:
    #     df = model.df.filter(symbol=setup.symbol, low__lt=setup.trigger).to_timeseries(index='bucket')
    #
    # condition = (df[column] >= target) if setup.direction == 1 else (df[column] <= target)
    # filtered = df.loc[condition, column].sort_index(ascending=False)
    #
    # potential_targets = filtered[abs((filtered - float(setup.trigger)) / float(setup.trigger)) >= 0.001]
    #
    # targets = []
    # for value in potential_targets:
    #     if targets:
    #         if (direction == 1 and value <= targets[-1]) or (direction == -1 and value >= targets[-1]):
    #             continue
    #     targets.append(value)
    #
    #     if len(targets) == 5:
    #         break
    # print(setup.symbol, setup.tf, setup.pattern, setup.trigger)
    # return targets
    return target

def filter_conditions(symbol__tf_bar_series__opens):
    symbol, (tf_bar_series, opens) = symbol__tf_bar_series__opens
    price = tf_bar_series['15'].get_newest().c

    new_tf_bar_series = {}
    for tf, bar_series in tf_bar_series.items():
        previous_bar = bar_series.get_previous()
        current_bar = bar_series.get_newest()
        is_potential_outside, _ = potential_outside_bar(previous_bar, current_bar)
        if is_potential_outside:
            continue

        target_bar, trigger_bar = bar_series.strat_candles()
        if trigger_bar is None or target_bar is None:
            continue

        continuation = trigger_bar.sid == current_bar.sid
        if continuation or trigger_bar.sid == '3':
            continue

        new_tf_bar_series[tf] = bar_series

    return symbol, (new_tf_bar_series, opens, price)


# def scan_bars(historical_setups, symbol__tf_bar_series__opens__price):
    # symbol, (tf_bar_series, opens, price) = symbol__tf_bar_series__opens__price
def scan_bars(historical_setups, symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series

    if historical_setups is None:
        historical_setups = {}
        for tf, bar_series in tf_bar_series.items():
            historical_setups[tf] = build_setup(symbol, bar_series)

    opens = opening_prices(tf_bar_series)
    price = Decimal(str(tf_bar_series['15'].get_newest().c))
    tfc_table = tfc_state(opens, price)

    for tf, setup in historical_setups.items():
        bar_series = tf_bar_series[tf]
        target_bar, trigger_bar = bar_series.strat_candles()
        if trigger_bar is None or target_bar is None:
            continue

        trigger_bar_ts = datetime.fromtimestamp(trigger_bar.ts, tz=timezone.utc)
        if trigger_bar_ts != setup.timestamp:
            # setup = build_setup(symbol, bar_series)
            historical_setups[tf] = build_setup(symbol, bar_series)
            continue

        if setup.negated:
            continue

        if setup.hit_magnitude or setup.potential_outside:
        # if setup.hit_magnitude:
            continue

        current_bar = bar_series.get_newest()

        continuation = trigger_bar.sid == current_bar.sid
        if continuation or trigger_bar.sid == '3':
            setup.negated = True
            setup.negated_reasons.add('CONTINUATION')
            # print(f'CONTINUATION: {symbol} [{tf}] {setup.pattern} {current_bar.sid} {setup.priority}')
            continue

        # check if potential outside bar
        if setup.potential_outside is False and tf not in ['15', '30']:
            potential_outside, direction = potential_outside_bar(trigger_bar, current_bar)
            if potential_outside:
                setup.potential_outside = True
                # print(f'POTENTIAL OUTSIDE: {symbol} [{tf}] {setup.pattern} {current_bar.sid} {setup.priority}')
                continue

        tradingview_url = 'https://stratalerts.com/tvr/?symbol={}&interval={}'
        url_symbol = f'BINANCE:{symbol}.P' if symbol.endswith('USDT') else symbol
        match tf:
            case 'Q':
                url_tf = '3M'
            case 'Y':
                url_tf = '12M'
            case _:
                url_tf = tf
        url = tradingview_url.format(url_symbol, url_tf)

        in_force_bear = current_bar.c < trigger_bar.l
        in_force_bull = current_bar.c > trigger_bar.h
        is_in_force = in_force_bear or in_force_bull

        if is_in_force:
            setup.in_force = True
            if not setup.initial_trigger:
                setup.initial_trigger = datetime.now(tz=timezone.utc)

            setup.target = find_targets(setup, bar_series)
            if not setup.target:
                print('NO TARGET', setup.symbol, setup.tf, setup.pattern, setup.trigger)

            if in_force_bear:
                setup.direction = -1
                setup.trigger = setup.bear_trigger
                setup.target = setup.bear_target

            elif in_force_bull:
                setup.direction = 1
                setup.trigger = setup.bull_trigger
                setup.target = setup.bull_target

            msg = 'IN FORCE'
            # if not continuation and not setup.in_force_alerted:
            if not setup.in_force_alerted:
                if tf not in ['15', '30']:
                    bull_or_bear = 'BULL' if setup.direction == 1 else 'BEAR'
                    tfc_output = ' '.join([f"[[white]{tf}[/white]] {tfc.distance_percent}%" for tf, tfc in tfc_table.items()])
                    console.print(f'{datetime.now()}: {msg} - {bull_or_bear}: {symbol} [{tf}] {setup.pattern} {current_bar.sid} {setup.priority} {setup.shape} {setup.potential_outside}')
                    console.print(url)
                    # console.print(f'* TFC: {tfc_output}')

                    symbolrec = SymbolRec.objects.get(symbol=symbol)
                    alert = DiscordMsgAlert(symbolrec, setup)
                    alert.send_msg(channel=symbolrec.symbol_type)

                setup.in_force_alerted = True
                setup.in_force_last_alerted = datetime.now(tz=timezone.utc)
        else:
            setup.in_force = False

        if setup.target and not setup.hit_magnitude:
            hit_magnitude_bull = setup.direction == 1 and (price >= setup.target or current_bar.h >= setup.target)
            hit_magnitude_bear = setup.direction == -1 and (price <= setup.target or current_bar.l <= setup.target)
            if (hit_magnitude_bull or hit_magnitude_bear) and not setup.magnitude_alerted:
                setup.hit_magnitude = True
                bull_or_bear = 'BULL' if setup.direction == 1 else 'BEAR'

                if tf not in ['15', '30']:
                    msg = 'HIT MAGNITUDE'
                    # tfc_output = ' '.join([f"[[white]{tf}[/white]] {tfc.distance_percent}%" for tf, tfc in tfc_table.items()])
                    console.print(f'{datetime.now()}: {msg} - {bull_or_bear}: {symbol} [{tf}] {setup.pattern} {current_bar.sid} {setup.priority} {setup.shape} {setup.potential_outside}')
                    console.print(url)
                    # console.print(f'* TFC: {tfc_output}')

                    symbolrec = SymbolRec.objects.get(symbol=symbol)
                    alert = DiscordMsgAlert(symbolrec, setup)
                    alert.send_msg(channel=symbolrec.symbol_type)

                setup.magnitude_alerted = True
                setup.magnitude_last_alerted = datetime.now(tz=timezone.utc)

    return historical_setups, copy.deepcopy(historical_setups)


def create_setup(symbol__meta__tf_bar_series):
    symbol, (meta, tf_bar_series) = symbol__meta__tf_bar_series
    tf_bar_series = tf_bar_series[0]
    setups = {}
    for tf, bar_series in tf_bar_series.items():
        setups[tf] = build_setup(symbol, bar_series)
    return symbol, setups


class _SinkPartition(StatelessSinkPartition):
    def write_batch(self, items: list) -> None:
        # output = {}
        # for symbol, tf_setupmsgs in items:
        #     for tf, setupmsg in tf_setupmsgs.items():
        #         output[tf] = msgspec.json.encode(setupmsg)
        # print(output)
        # print(items)
        pass


class TestSink(DynamicSink):
    def build(self, worker_index: int, worker_count: int) -> _SinkPartition:
        return _SinkPartition()


def fetch_opens(symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series
    opens = {}
    for tf, bar_series in tf_bar_series.items():
        current_bar = bar_series.get_newest()
        opens[tf] = current_bar.o
    return symbol, (tf_bar_series, opens)


def accumulate(acc, x):
    acc = [x]
    return acc


clock_config = EventClockConfig(
    lambda data: datetime.now(tz=timezone.utc),
    wait_for_system_duration=timedelta(0)
)
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)

flow = Dataflow("stateful_setups_v2")
bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    .then(op.filter, 'filter_symbols', lambda data: data[0] in allowed_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
    # .then(op.map, 'fetch_opens', fetch_opens)
    # .then(op.map, 'filter_conditions', filter_conditions)
    # .then(op.filter, 'filter_empty', lambda data: data[1][0] != {})
    # .then(op.map, 'add_key_to_value', add_key_to_value)
)
# op.inspect('inspect', bar_stream)

# window_config = TumblingWindow(align_to=align_to, length=timedelta(minutes=1))
# window_15 = fold_window(
#     "window15", bar_stream, clock_config, window_config, list, accumulate
# ).then(op.map, 'scan_15', create_setup)
# # ).then(op.stateful_map, 'scan_bars', lambda: None, scan_bars)
#
# window_config = TumblingWindow(align_to=align_to, length=timedelta(minutes=5))
# window_30 = fold_window(
#     "window30", bar_stream, clock_config, window_config, list, accumulate
# ).then(op.map, 'scan_30', create_setup)
# # ).then(op.stateful_map, 'scan_bars', lambda: None, scan_bars)

# window_config = TumblingWindow(align_to=align_to, length=timedelta(minutes=60))
# sixty_min_window = fold_window(
#     "window60", bar_stream, clock_config, window_config, list, accumulate
# ).then(op.stateful_map, 'scan_bars', lambda: None, scan_bars)
#
# window_config = TumblingWindow(align_to=align_to, length=timedelta(hours=4))
# four_hour_window = fold_window(
#     "window4h", bar_stream, clock_config, window_config, list, accumulate
# ).then(op.stateful_map, 'scan_bars', lambda: None, scan_bars)
#
# window_config = TumblingWindow(align_to=align_to, length=timedelta(hours=6))
# six_hour_window = fold_window(
#     "window6h", bar_stream, clock_config, window_config, list, accumulate
# ).then(op.stateful_map, 'scan_bars', lambda: None, scan_bars)

# op.output('test_sink', s, TestSink())
op.output('stdout_sink', bar_stream, StdOutSink())
# op.output('stdout_sink1', window_15, StdOutSink())
# op.output('stdout_sink2', window_30, StdOutSink())
